"""node_monitor.cli.main -- Phase 0 CLI surface.

Design: PHASE0_DAEMON_DESIGN.md + PHASE0_DAEMON_IMPLEMENTATION_PLAN.md
Task 7 ("Add CLI commands: `daemon dry-run`, `daemon smoke`, and
`validate-run`. Status output must print config and output paths.").
PLANNING.md section 15 ("CLI surface"):
``node-monitor daemon dry-run --out FILE  # Phase 0: JSON to file, no DB``.

This module is intentionally thin: it resolves a strict
``node_monitor.config.Phase0Config``, wires the real transport
(``node_monitor.collector.transport.run_local_probe``/
``run_remote_probe``, dispatched per node by role) and a real
``node_monitor.output.jsonl.Phase0Sink`` into a
``node_monitor.daemon.Daemon``, runs it to completion, and maps its
exit code straight through to the process exit code. No orchestration,
retry, or scheduling policy of its own -- every one of those already
lives in ``daemon.py``/``collector/scheduler.py``, and this module must
never invent a parallel path around them (card: "Clarify command
behavior from the authoritative design/plan and existing APIs rather
than inventing parallel orchestration").

Mixed local/remote dispatch (review round 1 fix, kanban task
t_3d9d7387): every configured local node (``role: local``) is polled
via the existing ``collector.transport.run_local_probe``; every
configured remote node (``role: remote``) is polled via the existing
``collector.transport.run_remote_probe`` over a project-owned SSH
config/control directory (``collector.transport.write_ssh_config`` --
never the operator's interactive ``~/.ssh/config``), with
``BatchMode=yes``/``ConnectTimeout`` already baked into that generated
config and ``expected_fqdn=None`` passed for every poll (kanban task
t_88d97d8e: the per-call alias-equals-FQDN check this used to drive
was itself a bug -- a legitimate ssh_target/alias never equals the
probe's self-reported FQDN; the real per-node consistency and
cross-node uniqueness checks now live in ``Daemon`` itself, design:
"SSH aliases are transport identifiers only, never provenance").
Which transport a given ``(node, loop)`` poll uses is decided purely
by ``node.is_local`` inside ``_make_transport_fn`` -- no separate
code path, and no upfront rejection of remote nodes.

``daemon dry-run`` / ``daemon smoke`` share one internal implementation
(``_run_daemon``): ``smoke`` is exactly ``dry-run`` with one additional,
required ``--duration-sec`` override that REPLACES the loaded config's
own ``duration_sec`` for this invocation only (the loaded
``Phase0Config`` is an immutable frozen dataclass -- overriding must
happen by passing an overridden raw mapping through
``node_monitor.config.load_config`` again, never by mutating the
dataclass in place) -- design: "short smoke/preflight" is explicitly a
bounded-duration variant of the same run, not a different code path.
"""

import asyncio
import dataclasses
import getpass
import os
import re
import socket
import subprocess
import sys

import click
import yaml
from sqlalchemy import create_engine, text

from node_monitor import __version__
from node_monitor.collector import transport
from node_monitor.config import (
   ConfigError,
   NodeMonitorConfig,
   Phase0Config,
   discover_config_path,
   load_config,
   load_config_file_any,
)
from node_monitor.daemon import EXIT_OK, EXIT_SINK_FATAL, Daemon
from node_monitor.daemon_control import (
   ControlFile,
   DaemonControlError,
   DaemonState,
   STATE_DIFFERENT_HOST,
   STATE_EXITED,
   STATE_NOT_RUNNING,
   STATE_RUNNING,
   STATE_STALE,
   STATE_STOPPING,
   current_process_start_ticks,
)
from node_monitor.database.connection import NodeMonitorDB
from node_monitor.database.migration import MigrationError, MigrationRunner
from node_monitor.database.writer import DatabaseWriter
from node_monitor.output.jsonl import (
   Phase0Sink,
   Phase0SinkDiskFullError,
   Phase0SinkError,
   RecoveryFinalizeError,
   finalize_orphaned_run,
   validate_jsonl_artifact,
)
from node_monitor.output.postgres import PostgresDaemonSink

# node_monitor/collector/remote_probe.py -- the ONLY probe binary this CLI
# ever invokes, for both local nodes (by path, see _make_transport_fn) and
# remote nodes (this same file piped over SSH stdin). This module must
# never import that file as a Python module: README.md's "never imported
# by the daemon" is exactly this CLI's own runtime import graph, not just
# a future daemon process's -- the probe is quarantined at Python 3.6 and
# is only ever imported by its own test suite. The expected probe_version
# transport.validate_probe_payload() enforces per poll is instead obtained
# by actually executing the shipped probe's own ``--version`` contract as
# a subprocess (see _resolve_probe_version) -- never by reading
# node_monitor.collector.remote_probe.PROBE_VERSION off an imported
# module object.
_REMOTE_PROBE_SCRIPT_PATH = os.path.join(
   os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
   "collector", "remote_probe.py")

# Design: PHASE0_DAEMON_DESIGN.md "Failure handling" -- "hard subprocess
# deadlines" per poll. The daemon's own counter/census cadence already
# bounds how often a poll is dispatched; this CLI's own hard per-
# subprocess timeout only needs enough headroom above each loop's own
# configured *_timeout_sec (itself the probe's own --max-seconds self-
# abort budget) for the probe's clean self-abort (exit 4) to normally
# fire before this outer deadline ever does -- see
# collector.transport.run_local_probe's own docstring for that
# ordering contract.
_HARD_TIMEOUT_GRACE_SEC = 5.0
_UNSET = object()


def _default_control_file_path():
   return os.path.join(os.path.expanduser("~"), ".node_monitor_daemon.pid")


async def _watch_control_file(control_file, stoppable, *, poll_interval=5.0,
                              heartbeat_interval=30.0):
   """Watch local control state; control loss fails closed."""
   loop = asyncio.get_running_loop()
   last_heartbeat = loop.time()
   while True:
      await asyncio.sleep(poll_interval)
      try:
         state = control_file.read()
         if state.stop_requested:
            stoppable.request_stop()
            return
         if loop.time() - last_heartbeat >= heartbeat_interval:
            control_file.heartbeat()
            last_heartbeat = loop.time()
      except DaemonControlError:
         stoppable.request_stop()
         return


def _exit(code, message=None, err=False):
   if message is not None:
      click.echo(message, err=err)
   sys.exit(code)


# ``config check`` validates a candidate ``database.url``/
# ``NODE_MONITOR_DB_URL`` BEFORE any masking exists to render it safely
# (``NodeMonitorDB.mask_url`` only applies to an already-accepted URL) --
# a rejected URL's own ``ConfigError`` message therefore embeds the raw,
# credential-bearing value verbatim (e.g. "database.url must be
# PostgreSQL-only, got backend 'sqlite' ('sqlite://user:PASSWORD@host/db')").
# This regex redacts the password segment of any embedded
# ``scheme://user:password@`` URL in a diagnostic string before it is ever
# echoed, so a config/env validation failure can never leak the very
# credential ``config check`` exists to keep off stdout/stderr.
_URL_PASSWORD_RE = re.compile(
   r"([a-zA-Z][a-zA-Z0-9+.\-]*://[^:/@\s'\"]+):([^@/\s'\"]+)@")


def _sanitize_config_error(exc):
   return _URL_PASSWORD_RE.sub(r"\1:***@", str(exc))


def _load_config_or_exit(config_path, home):
   if not os.path.exists(config_path):
      _exit(1, "config file not found: %s" % (config_path,), err=True)
   try:
      with open(config_path, "r") as handle:
         raw = yaml.safe_load(handle)
   except yaml.YAMLError as exc:
      _exit(1, "config file is not valid YAML: %s" % (exc,), err=True)
      return  # pragma: no cover -- _exit always raises SystemExit
   if raw is None:
      _exit(1, "config file %r is empty" % (config_path,), err=True)
      return  # pragma: no cover
   try:
      return load_config(raw, home=home)
   except ConfigError as exc:
      _exit(1, "invalid configuration: %s" % (exc,), err=True)


def _resolve_probe_version(probe_python):
   """Obtain the shipped probe's expected ``probe_version`` WITHOUT
   ever importing ``node_monitor.collector.remote_probe`` as a Python
   module (README.md: "never imported by the daemon"; the probe is
   quarantined at Python 3.6 and this CLI must never pull it into its
   own 3.9+ runtime import graph). Instead, this executes the shipped
   probe script's own documented ``--version`` contract
   (``remote_probe.py``'s ``_parse_args``: writes the integer
   ``PROBE_VERSION`` to stdout and exits 0) as a real, short-lived,
   argument-only subprocess -- using ``config.probe_python`` itself so
   the very same interpreter that will run every real poll is what
   answers this version query, and a genuinely broken/incompatible
   interpreter fails loudly here rather than 24 hours into a canary.
   """
   try:
      result = subprocess.run(
         [probe_python, _REMOTE_PROBE_SCRIPT_PATH, "--version"],
         capture_output=True, text=True, timeout=30)
   except OSError as exc:
      _exit(1, "could not execute probe_python %r to resolve the "
                "expected probe version: %s" % (probe_python, exc),
            err=True)
      return  # pragma: no cover -- _exit always raises SystemExit
   if result.returncode != 0:
      _exit(1, "probe --version exited %d: %s"
                % (result.returncode, result.stderr.strip()), err=True)
      return  # pragma: no cover
   try:
      return int(result.stdout.strip())
   except ValueError:
      _exit(1, "probe --version did not print an integer: %r"
                % (result.stdout,), err=True)


def _print_status(config, config_path, run_dir):
   click.echo("config: %s" % config_path)
   click.echo("system: %s" % config.system)
   click.echo("output_root: %s" % config.output_root)
   click.echo("run_dir: %s" % run_dir)
   click.echo("nodes: %s" % ", ".join(node.hostname for node in config.nodes))
   click.echo("duration_sec: %s" % config.duration_sec)


def _make_transport_fn(config, probe_version):
   """Build the ``transport_fn(node, loop)`` callable ``Daemon`` expects,
   dispatching each poll to the REAL production transport for that
   node's configured role -- never a test double, never a single
   local-only path. Local nodes (``node.is_local``) go through
   ``collector.transport.run_local_probe`` exactly as before; remote
   nodes go through the existing ``collector.transport.run_remote_probe``
   over a project-owned SSH config/control directory this function
   creates once via ``collector.transport.write_ssh_config`` (never the
   operator's interactive ``~/.ssh/config`` -- design/PLANNING.md 4.3),
   with ``BatchMode=yes``/``ConnectTimeout`` already baked into that
   generated config and passed again explicitly on the ssh command line
   (``build_remote_argv``'s own belt-and-suspenders contract). The
   literal ssh(1) host argument for a remote node is
   ``node.effective_ssh_target`` -- the configured ``ssh_target``
   override when set (e.g. Polaris's ``.head`` login-node fan-out
   alias), else ``node.hostname`` -- never ``node.hostname`` alone
   (kanban task t_88d97d8e: an SSH alias is a transport identifier
   only, never provenance, so it must never be forced to equal the
   node's own bookkeeping hostname). ``expected_fqdn`` is passed as
   ``None`` for BOTH local and remote polls here: the per-call
   alias-equals-FQDN check this parameter used to drive was itself the
   bug (a legitimate ``.head``/any-ssh-alias target never equals the
   probe's self-reported FQDN) -- the correct per-node consistency and
   cross-node uniqueness checks now live in ``Daemon`` itself
   (``daemon.py``'s ``_established_fqdn``/``_established_by_fqdn``
   maps), which sees every node's payload across the whole run,
   something a single per-call ``transport_fn`` invocation never can.

   Returns a ``collector.transport.ProbeResult`` (not a plain payload
   dict) either way, which ``Daemon._poll_fn``'s own
   ``_normalize_probe_result`` boundary already unwraps.
   """
   hard_timeout_sec = max(
      config.counter_timeout_sec, config.census_timeout_sec
   ) + _HARD_TIMEOUT_GRACE_SEC

   ssh_dir = os.path.join(config.output_root, "ssh")
   os.makedirs(ssh_dir, exist_ok=True)
   ssh_config_path = transport.write_ssh_config(
      os.path.join(ssh_dir, "config"),
      os.path.join(ssh_dir, "control"),
      config.ssh_connect_timeout_sec)

   # Read once, up front, so N concurrent remote polls across the run
   # never each re-read the probe source off disk -- mirrors
   # run_remote_probe's own docstring contract for probe_script_source.
   with open(_REMOTE_PROBE_SCRIPT_PATH, "rb") as handle:
      probe_script_source = handle.read()

   def _max_seconds_for(loop):
      if loop == "counter":
         return config.counter_timeout_sec
      if loop == "census":
         return config.census_timeout_sec
      # hwinfo: one-shot pre-scheduler collection (Daemon._collect_
      # hardware) -- reuse the census timeout as a generous-enough
      # one-shot budget rather than inventing a fourth config knob
      # this increment's card does not ask for.
      return config.census_timeout_sec

   async def transport_fn(node, loop):
      max_seconds = _max_seconds_for(loop)
      if node.is_local:
         return await transport.run_local_probe(
            probe_python=config.probe_python,
            probe_script_path=_REMOTE_PROBE_SCRIPT_PATH,
            loop=loop,
            probe_max_seconds=max_seconds,
            hard_timeout_sec=hard_timeout_sec,
            expected_probe_version=probe_version,
            expected_fqdn=None,
            keep_raw_args=config.keep_raw_args,
         )
      return await transport.run_remote_probe(
         ssh_binary="ssh",
         ssh_config_path=ssh_config_path,
         connect_timeout_sec=config.ssh_connect_timeout_sec,
         hostname=node.effective_ssh_target,
         probe_python=config.probe_python,
         probe_script_source=probe_script_source,
         loop=loop,
         probe_max_seconds=max_seconds,
         hard_timeout_sec=hard_timeout_sec,
         expected_probe_version=probe_version,
         expected_fqdn=None,
         keep_raw_args=config.keep_raw_args,
      )

   return transport_fn


def _run_daemon(config, config_path, run_id, probe_version):
   output_root = config.output_root
   os.makedirs(output_root, exist_ok=True)
   sink = Phase0Sink(
      output_root, run_id, metadata={"system": config.system},
      min_free_disk_pct=config.min_free_disk_pct,
      compress_census=config.compress_census,
      keep_raw_args=config.keep_raw_args)
   run_dir = sink.run_dir

   _print_status(config, config_path, run_dir)

   daemon = Daemon(config, sink, _make_transport_fn(config, probe_version))
   exit_code = asyncio.run(daemon.run())
   if exit_code == EXIT_OK:
      click.echo("run complete: %s" % run_dir)
   else:
      click.echo(
         "run ended with a fatal condition (exit %d): %s"
         % (exit_code, run_dir), err=True)
   return exit_code


@click.group()
def cli():
   """node-monitor: Phase 0 login-node observability CLI."""


@cli.group()
def daemon():
   """Run the Phase 0 daemon."""


@daemon.command("dry-run")
@click.option("--config", "config_path", required=True,
              type=click.Path(dir_okay=False),
              help="Path to the strict Phase 0 YAML config file.")
@click.option("--home", "home", default=None,
              help="Override $HOME for output_root resolution "
                   "(internal/test use; defaults to the real $HOME).")
@click.option("--run-id", "run_id", default=None,
              help="Explicit run id (defaults to a UTC timestamp).")
def daemon_dry_run(config_path, home, run_id):
   """Run the full configured Phase 0 daemon: JSON output only, no DB.

   PLANNING.md section 15: "Phase 0: JSON to file, no DB". Runs for
   exactly the loaded config's own ``duration_sec`` -- for the design's
   real 24-hour canary this call is not expected to return quickly; use
   ``daemon smoke`` for a short, duration-overridden exercise of the
   exact same wiring.
   """
   home = home if home is not None else os.path.expanduser("~")
   config = _load_config_or_exit(config_path, home)
   probe_version = _resolve_probe_version(config.probe_python)
   if run_id is None:
      run_id = _default_run_id()
   exit_code = _run_daemon(config, config_path, run_id, probe_version)
   sys.exit(exit_code)


@daemon.command("smoke")
@click.option("--config", "config_path", required=True,
              type=click.Path(dir_okay=False),
              help="Path to the strict Phase 0 YAML config file.")
@click.option("--home", "home", default=None,
              help="Override $HOME for output_root resolution "
                   "(internal/test use; defaults to the real $HOME).")
@click.option("--run-id", "run_id", default=None,
              help="Explicit run id (defaults to a UTC timestamp).")
@click.option("--duration-sec", "duration_sec", required=True, type=float,
              help="Overrides the loaded config's duration_sec for this "
                   "run only, for a short preflight/smoke exercise.")
def daemon_smoke(config_path, home, run_id, duration_sec):
   """Run the Phase 0 daemon for a short, explicitly bounded duration.

   Design: "Polaris preflight ... runs a short multi-cycle smoke."
   Identical wiring to ``daemon dry-run``, with ``duration_sec``
   overridden for exactly this invocation -- every other configured
   value (intervals, timeouts, node list) is unchanged.
   """
   if duration_sec <= 0:
      _exit(1, "--duration-sec must be positive, got %r" % (duration_sec,),
            err=True)
   home = home if home is not None else os.path.expanduser("~")
   config = _load_config_or_exit(config_path, home)
   probe_version = _resolve_probe_version(config.probe_python)
   raw_override = _config_to_raw(config)
   raw_override["duration_sec"] = duration_sec
   config = load_config(raw_override, home=home)
   if run_id is None:
      run_id = _default_run_id()
   exit_code = _run_daemon(config, config_path, run_id, probe_version)
   sys.exit(exit_code)


@daemon.command("run")
@click.option("--config", "config_path", required=True,
              type=click.Path(dir_okay=False),
              help="Path to the strict nested Phase 1 YAML config file.")
@click.option("--home", "home", default=None,
              help="Override $HOME for output_root resolution "
                   "(internal/test use; defaults to the real $HOME).")
@click.option("--run-id", "run_id", default=None,
              help="Explicit run id (defaults to a UTC timestamp).")
@click.option("--duration-sec", "duration_sec", default=None, type=float,
              help="Override the loaded config's duration_sec for this "
                   "invocation only (optional; must be positive).")
def daemon_run(config_path, home, run_id, duration_sec):
   """Run the Phase 1 daemon: PostgreSQL sink + diagnostic JSONL.

   Requires the strict NESTED configuration layout (nested
   output/collection/ssh/safety/database/retention sections).
   Performs a READ-ONLY schema gate via ``database status`` before
   starting -- rejects unless the schema is fully current.  Never
   calls ``database migrate``; run that operator command explicitly
   first.
   """
   if duration_sec is not None and duration_sec <= 0:
      _exit(1, "--duration-sec must be positive, got %r" % (duration_sec,),
            err=True)

   resolved_home = home if home is not None else os.path.expanduser("~")
   database_url_env = os.environ.get("NODE_MONITOR_DB_URL")

   try:
      loaded = load_config_file_any(
         config_path, home=resolved_home,
         database_url_env=database_url_env)
   except ConfigError as exc:
      _exit(1, "invalid configuration: %s" % (_sanitize_config_error(exc),),
            err=True)
      return  # pragma: no cover
   except yaml.YAMLError:
      _exit(1, "config file is not valid YAML", err=True)
      return  # pragma: no cover
   except OSError:
      _exit(1, "could not read configuration file", err=True)
      return  # pragma: no cover

   if not isinstance(loaded, NodeMonitorConfig):
      _exit(1, "daemon run requires the nested configuration layout",
            err=True)
      return  # pragma: no cover

   # Probe version resolved BEFORE engine creation: a bad interpreter
   # fails here rather than after the pool is open.
   probe_version = _resolve_probe_version(loaded.probe_python)

   if run_id is None:
      run_id = _default_run_id()

   duration_sec_override = duration_sec  # None if not given; positive if given

   # Build a Phase0Config that incorporates the duration override (if any)
   # for the nested-to-flat conversion -- done before engine creation so
   # a bad override value fails before any pool is opened.
   raw_flat = _nested_to_phase0_raw(
      loaded, duration_sec_override=duration_sec_override)
   try:
      phase0_config = load_config(raw_flat, home=resolved_home)
   except ConfigError as exc:
      _exit(1, "invalid configuration: %s" % (exc,), err=True)
      return  # pragma: no cover

   exit_code = _run_daemon_postgres(
      loaded, config_path, run_id, probe_version, resolved_home,
      phase0_config=phase0_config)
   sys.exit(exit_code)


def _load_nested_daemon_config(config_path, home):
   database_url_env = os.environ.get("NODE_MONITOR_DB_URL")
   try:
      loaded = load_config_file_any(
         config_path, home=home, database_url_env=database_url_env)
   except (ConfigError, yaml.YAMLError, OSError) as exc:
      if isinstance(exc, ConfigError):
         message = "invalid configuration: %s" % _sanitize_config_error(exc)
      elif isinstance(exc, yaml.YAMLError):
         message = "config file is not valid YAML"
      else:
         message = "could not read configuration file"
      _exit(1, message, err=True)
   if not isinstance(loaded, NodeMonitorConfig):
      _exit(1, "daemon start requires the nested configuration layout",
            err=True)
   return loaded


def _daemon_state(control_file, run_id, run_directory, log_file):
   import datetime
   now = datetime.datetime.now(datetime.timezone.utc).isoformat()
   return DaemonState(
      hostname=socket.gethostname(), pid=os.getpid(),
      process_start_ticks=current_process_start_ticks(),
      start_timestamp=now, working_directory=os.getcwd(),
      user=getpass.getuser(), heartbeat=now,
      stop_requested=False, exited=False, run_id=run_id,
      run_directory=run_directory, log_file=log_file, outcome="running")


def _status_label(status):
   return {
      STATE_RUNNING: "Running", STATE_STOPPING: "Stopping",
      STATE_EXITED: "Exited", STATE_STALE: "Stale",
      STATE_DIFFERENT_HOST: "Running on different host",
      STATE_NOT_RUNNING: "Not running",
   }[status]


@daemon.command("status")
def daemon_status():
   """Show detached daemon state without touching PostgreSQL."""
   control = ControlFile(_default_control_file_path())
   try:
      status = control.classify()
      click.echo("Status: %s" % _status_label(status))
      if status != STATE_NOT_RUNNING:
         state = control.read()
         click.echo("PID: %d" % state.pid)
         click.echo("Hostname: %s" % state.hostname)
         click.echo("Heartbeat: %s" % state.heartbeat)
         click.echo("Run directory: %s" % state.run_directory)
         click.echo("Log file: %s" % state.log_file)
   except DaemonControlError:
      _exit(1, "Status: Invalid control file", err=True)


@daemon.command("stop")
def daemon_stop():
   """Request graceful shutdown through the local control file."""
   control = ControlFile(_default_control_file_path())
   try:
      status = control.classify()
      if status in (STATE_NOT_RUNNING, STATE_EXITED):
         click.echo("Status: %s" % _status_label(status))
         return
      if status == STATE_DIFFERENT_HOST:
         _exit(1, "daemon is managed on a different host", err=True)
      state = control.request_stop()
      click.echo("Stop requested for PID %d" % state.pid)
   except DaemonControlError:
      _exit(1, "could not request daemon stop", err=True)


@daemon.command("start")
@click.option("--config", "config_path", required=True,
              type=click.Path(dir_okay=False))
@click.option("--home", default=None)
@click.option("--run-id", default=None)
@click.option("--foreground", is_flag=True, default=False)
def daemon_start(config_path, home, run_id, foreground):
   """Start the PostgreSQL-backed daemon indefinitely."""
   resolved_home = home if home is not None else os.path.expanduser("~")
   loaded = _load_nested_daemon_config(config_path, resolved_home)
   probe_version = _resolve_probe_version(loaded.probe_python)
   run_id = run_id if run_id is not None else _default_run_id()
   control = ControlFile(_default_control_file_path())
   status = control.classify()
   if status in (STATE_RUNNING, STATE_STOPPING, STATE_DIFFERENT_HOST):
      _exit(1, "daemon already active: %s" % _status_label(status), err=True)

   raw = _nested_to_phase0_raw(loaded, duration_sec_override=None)
   phase0_config = load_config(raw, home=resolved_home)
   log_file = os.path.join(resolved_home, ".node_monitor_daemon.log")

   if foreground:
      def startup_ack(run_directory):
         control.write(_daemon_state(control, run_id, run_directory, log_file))
      exit_code = _run_daemon_postgres(
         loaded, config_path, run_id, probe_version, resolved_home,
         phase0_config=phase0_config, control_file=control,
         startup_ack=startup_ack)
      state = control.read()
      outcome = "fatal" if exit_code == EXIT_SINK_FATAL else "partial"
      control.mark_exited(outcome=outcome, exit_code=exit_code)
      sys.exit(exit_code)

   read_fd, write_fd = os.pipe()
   child_pid = os.fork()
   if child_pid:
      os.close(write_fd)
      ack = os.read(read_fd, 1)
      os.close(read_fd)
      if ack != b"1":
         _exit(1, "daemon start failed during preflight", err=True)
      state = control.read()
      click.echo("Daemon started in background. PID: %d" % state.pid)
      click.echo("PID file: %s" % control.path)
      click.echo("Log file: %s" % state.log_file)
      return

   os.close(read_fd)
   try:
      os.setsid()
      os.chdir("/")
      stdin_fd = os.open(os.devnull, os.O_RDONLY)
      log_fd = os.open(log_file, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
      os.dup2(stdin_fd, 0)
      os.dup2(log_fd, 1)
      os.dup2(log_fd, 2)
      os.close(stdin_fd)
      os.close(log_fd)

      def startup_ack(run_directory):
         control.write(_daemon_state(control, run_id, run_directory, log_file))
         os.write(write_fd, b"1")
         os.close(write_fd)

      exit_code = _run_daemon_postgres(
         loaded, config_path, run_id, probe_version, resolved_home,
         phase0_config=phase0_config, control_file=control,
         startup_ack=startup_ack)
      state = control.read()
      outcome = "fatal" if exit_code == EXIT_SINK_FATAL else "partial"
      control.mark_exited(outcome=outcome, exit_code=exit_code)
      os._exit(exit_code)
   except BaseException:
      try:
         os.close(write_fd)
      except OSError:
         pass
      os._exit(1)


def _load_database_config_or_exit(config_path, home):
   database_url_env = os.environ.get("NODE_MONITOR_DB_URL")
   try:
      loaded = load_config_file_any(
         config_path, home=home, database_url_env=database_url_env)
   except ConfigError as exc:
      _exit(1, "invalid configuration: %s" % (_sanitize_config_error(exc),),
            err=True)
   except yaml.YAMLError as exc:
      _exit(1, "config file is not valid YAML", err=True)
   except OSError:
      _exit(1, "could not read configuration file", err=True)
   if not isinstance(loaded, NodeMonitorConfig):
      _exit(1, "database commands require the nested configuration layout",
            err=True)
   return loaded.database


def _create_migration_engine(database):
   return create_engine(
      database.url,
      pool_size=database.pool_size,
      max_overflow=database.max_overflow,
      echo=database.echo_sql,
      pool_pre_ping=database.pool_pre_ping,
      pool_timeout=database.pool_timeout_sec,
      pool_recycle=database.pool_recycle_sec,
      connect_args=dict(database.connect_args),
   )


def _run_database_command(config_path, home, operation):
   home = home if home is not None else os.path.expanduser("~")
   database_config = _load_database_config_or_exit(config_path, home)
   engine = None
   try:
      engine = _create_migration_engine(database_config)
      runner = MigrationRunner(engine, __version__)
      return operation(runner)
   except MigrationError:
      _exit(1, "database migration failed; inspect operator logs", err=True)
   except Exception:
      _exit(1, "database operation failed", err=True)
   finally:
      if engine is not None:
         engine.dispose()


@cli.group()
def database():
   """Inspect or migrate the node-monitor PostgreSQL schema."""


@database.command("status")
@click.option("--config", "config_path", required=True,
              type=click.Path(dir_okay=False),
              help="Path to the strict nested YAML configuration.")
@click.option("--home", "home", default=None,
              help="Override $HOME for config path expansion (internal/test).")
def database_status(config_path, home):
   """Read migration status without creating schema objects."""
   status = _run_database_command(
      config_path, home, lambda runner: runner.status())
   click.echo("initialized: %s" % status.initialized)
   click.echo("current_version: %s" % status.current_version)
   click.echo("latest_version: %s" % status.latest_version)
   click.echo(
      "pending_versions: %s"
      % (",".join(str(version) for version in status.pending_versions)
         if status.pending_versions else "none"))
   click.echo("drift: %s" % status.drift)


@database.command("migrate")
@click.option("--config", "config_path", required=True,
              type=click.Path(dir_okay=False),
              help="Path to the strict nested YAML configuration.")
@click.option("--home", "home", default=None,
              help="Override $HOME for config path expansion (internal/test).")
def database_migrate(config_path, home):
   """Apply pending migrations under the operator advisory lock."""
   result = _run_database_command(
      config_path, home, lambda runner: runner.migrate())
   click.echo(
      "applied_versions: %s"
      % (",".join(str(version) for version in result.applied_versions)
         if result.applied_versions else "none"))
   click.echo("current_version: %s" % result.current_version)
   click.echo("latest_version: %s" % result.latest_version)


@cli.group()
def config():
   """Inspect and validate node-monitor configuration."""


@config.command("check")
@click.option("--config", "config_path", default=None,
              type=click.Path(dir_okay=False),
              help="Explicit path to the config file. When omitted, "
                   "discovered pbs-monitor-style (--home dotfile, "
                   "--home XDG path, --etc-path, then --cwd).")
@click.option("--home", "home", default=None,
              help="Override $HOME for discovery/output_root resolution "
                   "(internal/test use; defaults to the real $HOME).")
@click.option("--cwd", "cwd", default=None,
              help="Override the current working directory for config "
                   "discovery (internal/test use; defaults to the real cwd).")
@click.option("--etc-path", "etc_path", default=None,
              help="Override the system config path checked during "
                   "discovery (internal/test use; defaults to "
                   "/etc/node_monitor/config.yaml).")
def config_check(config_path, home, cwd, etc_path):
   """Validate the selected configuration and print a sanitized summary.

   Never constructs a database engine or probes connectivity -- only
   ``NodeMonitorDB.mask_url`` renders the database URL. Reads
   ``NODE_MONITOR_DB_URL`` from the real environment exactly once, and
   only as a fallback when the config file supplies no explicit
   ``database.url``; an explicit file URL always wins.
   """
   home = home if home is not None else os.path.expanduser("~")
   cwd = cwd if cwd is not None else os.getcwd()
   etc_kwargs = {}
   if etc_path is not None:
      etc_kwargs["etc_path"] = etc_path

   try:
      resolved_path = discover_config_path(
         explicit_path=config_path, home=home, cwd=cwd, **etc_kwargs)
   except ConfigError as exc:
      _exit(1, "invalid configuration: %s" % (_sanitize_config_error(exc),),
            err=True)
      return  # pragma: no cover -- _exit always raises SystemExit

   database_url_env = os.environ.get("NODE_MONITOR_DB_URL")

   try:
      loaded = load_config_file_any(
         resolved_path, home=home, database_url_env=database_url_env)
   except ConfigError as exc:
      _exit(1, "invalid configuration: %s" % (_sanitize_config_error(exc),),
            err=True)
      return  # pragma: no cover
   except yaml.YAMLError as exc:
      _exit(1, "config file is not valid YAML: %s" % (exc,), err=True)
      return  # pragma: no cover

   click.echo("config: %s" % resolved_path)
   if isinstance(loaded, Phase0Config):
      click.echo("layout: legacy-flat")
      click.echo(
         "warning: flat Phase-0 configuration is deprecated; migrate to "
         "the nested output/collection/ssh/safety/database/retention "
         "sections")
      click.echo("system: %s" % loaded.system)
      click.echo(
         "nodes: %s" % ", ".join(node.hostname for node in loaded.nodes))
      return

   assert isinstance(loaded, NodeMonitorConfig)  # dispatch guarantee
   click.echo("layout: nested")
   click.echo("system: %s" % loaded.system)
   click.echo("nodes: %d" % len(loaded.nodes))
   click.echo("database_url: %s" % NodeMonitorDB.mask_url(loaded.database.url))
   click.echo("schema: %s" % loaded.database.schema)
   click.echo("pool_size: %s" % loaded.database.pool_size)
   click.echo("max_overflow: %s" % loaded.database.max_overflow)
   click.echo("retention_enabled: %s" % loaded.retention.enabled)
   click.echo("retention_dry_run: %s" % loaded.retention.dry_run)
   click.echo("housekeeping_utc: %s" % loaded.retention.housekeeping_utc)
   click.echo(
      "retention_horizons_days: diagnostic_census=%s "
      "counter_diagnostics=%s counter_minute=%s usage_intervals=%s "
      "usage_hourly=%s counter_hourly=%s daily=%s poll_failures=%s "
      "poll_failures_daily=%s collection_log=%s"
      % (loaded.retention.diagnostic_census_days,
         loaded.retention.counter_diagnostics_days,
         loaded.retention.counter_minute_days,
         loaded.retention.usage_intervals_days,
         loaded.retention.usage_hourly_days,
         loaded.retention.counter_hourly_days,
         loaded.retention.daily_days,
         loaded.retention.poll_failures_days,
         loaded.retention.poll_failures_daily_days,
         loaded.retention.collection_log_days))


@cli.command("validate-run")
@click.argument("run_dir", type=click.Path(exists=False))
def validate_run(run_dir):
   """Validate a completed (or partial) Phase 0 run directory's artifacts.

   Checks: the run directory exists, every production/diagnostic JSONL
   file present validates with zero malformed lines (a truncated FINAL
   line is reported and, on its own with no DONE flag present, does
   not by itself fail validation -- but see below), and a ``DONE``
   flag is present (design: "DONE is written only after summary
   finalization" -- its absence means the run never cleanly finished,
   which this command must not silently accept as valid). A DONE flag
   present together with ANY truncated final line also fails: DONE
   implies ``finalize_summary()`` already closed/fsynced every JSONL
   file, so a legitimately DONE-marked run can never contain a
   truncated line (design: DONE + truncated is a corrupted-artifact
   contradiction, not a valid completed run).
   """
   abs_run_dir = os.path.abspath(run_dir)
   if not os.path.isdir(abs_run_dir):
      _exit(1, "run directory not found: %s" % (run_dir,), err=True)
      return  # pragma: no cover

   click.echo("run_dir: %s" % abs_run_dir)

   problems = []
   any_file_checked = False
   any_truncated = False
   for filename in sorted(os.listdir(abs_run_dir)):
      if not (filename.endswith(".jsonl") or filename.endswith(".jsonl.gz")):
         continue
      any_file_checked = True
      path = os.path.join(abs_run_dir, filename)
      result = validate_jsonl_artifact(path)
      click.echo(
         "%s: valid=%d malformed=%d truncated_final_line=%s"
         % (filename, result["valid_count"], result["malformed_count"],
            result["truncated_final_line"]))
      if result["malformed_count"] > 0:
         problems.append(
            "%s has %d malformed line(s)"
            % (filename, result["malformed_count"]))
      if result["truncated_final_line"]:
         any_truncated = True

   done_path = os.path.join(abs_run_dir, "DONE")
   done_present = os.path.exists(done_path)
   click.echo("DONE present: %s" % done_present)
   if not done_present:
      problems.append("DONE flag is missing -- run did not finalize cleanly")
   elif any_truncated:
      # Design: PHASE0_DAEMON_DESIGN.md's own DONE contract -- "DONE is
      # written only after summary finalization", and finalize_summary()
      # closes/fsyncs every open JSONL handle before that write happens
      # (node_monitor.output.jsonl.Phase0Sink.finalize_summary). A run
      # that legitimately reached DONE therefore cannot contain a
      # truncated final line; one that does is proof of a corrupted
      # artifact or a DONE flag left over from an unrelated/earlier
      # run in this same directory, not a clean completion. Reporting
      # this as valid (review round 1 finding #2, kanban task
      # t_3d9d7387) is a false-positive validator result.
      problems.append(
         "DONE is present but at least one .jsonl artifact has a "
         "truncated final line -- a DONE-marked run cannot legitimately "
         "contain a truncated line")

   if not any_file_checked:
      problems.append("no .jsonl artifact files found in run directory")

   if problems:
      for problem in problems:
         click.echo("INVALID: %s" % problem, err=True)
      sys.exit(1)

   click.echo("valid: run directory passed all structural checks")
   sys.exit(0)


@cli.command("finalize-recovered-run")
@click.argument("run_dir", type=click.Path(exists=False))
@click.option("--run-id", "run_id", default=None,
              help="Explicit run id to record in the recovered summary. "
                   "Defaults to the '<id>' parsed out of the run "
                   "directory's own 'phase0-<id>' basename; only needed "
                   "when the directory was renamed away from that "
                   "convention.")
def finalize_recovered_run(run_dir, run_id):
   """Finalize an ORPHANED Phase 0 run directory: one whose daemon
   process died before writing summary.json/DONE (e.g. the old
   whole-file finalization path OOM-killed mid-finalize), leaving a
   complete set of .jsonl artifacts with no terminal summary/DONE.

   This is explicitly a RECOVERY tool, not a substitute for the
   daemon's own clean completion path (``daemon dry-run``/``daemon
   smoke``, whose ``Phase0Sink.finalize_summary``/``write_done`` this
   reuses the exact same streaming scanner from). It:

   \b
   * Never overwrites an existing summary.json or DONE -- a run that
     already reached either state is refused outright, not silently
     re-finalized.
   * Never publishes summary.json/DONE over untrustworthy artifact
     content -- a run directory with no .jsonl artifacts at all, or
     any artifact containing a malformed line or a truncated final
     line, is refused before either output is written.
   * Never fabricates the crashed daemon's own in-memory acceptance
     telemetry (scheduling-delay samples, per-node success totals,
     etc.) -- that state is gone with the dead process. The produced
     summary carries NO "acceptance" key, only a "recovery" section
     marking it explicitly as a recovered artifact, never
     indistinguishable from an ordinary clean daemon completion.
   * Refuses a run directory (or any artifact inside it) that is not a
     real, owned-by-the-invoking-user file/directory -- rejects a
     symlink or a foreign-owned path rather than following it.
   * Never touches PostgreSQL or any remote system.

   Exits nonzero with a diagnostic on every refusal case above, or on
   any I/O failure while finalizing; exits 0 only once summary.json and
   DONE have both actually been written.
   """
   try:
      summary = finalize_orphaned_run(run_dir, run_id=run_id)
   except RecoveryFinalizeError as exc:
      _exit(1, "finalize-recovered-run: %s" % (exc,), err=True)
      return  # pragma: no cover -- _exit always raises SystemExit
   except OSError as exc:
      _exit(1, "finalize-recovered-run: I/O failure: %s" % (exc,), err=True)
      return  # pragma: no cover

   abs_run_dir = os.path.abspath(run_dir)
   click.echo("run_dir: %s" % abs_run_dir)
   click.echo("run_id: %s" % summary["run_id"])
   click.echo("recovered: %s" % summary["recovery"]["recovered"])
   for record_type in sorted(summary["files"]):
      entry = summary["files"][record_type]
      click.echo(
         "  %s: record_count=%d byte_size=%d malformed_count=%d "
         "truncated_final_line=%s"
         % (record_type, entry["record_count"], entry["byte_size"],
            entry["malformed_count"], entry["truncated_final_line"]))
   click.echo(
      "finalized (recovered, not a clean daemon completion): %s"
      % abs_run_dir)
   sys.exit(0)


def _config_to_raw(config):
   """Reconstruct the raw mapping ``node_monitor.config.load_config``
   accepts from an already-loaded, immutable ``Phase0Config`` -- the
   one place this module needs to re-validate an OVERRIDDEN value
   (``daemon smoke``'s ``--duration-sec``) through the exact same
   strict loader every other config value already went through,
   rather than constructing a second, parallel ``Phase0Config`` by
   hand that could drift from what ``load_config`` itself considers
   valid. Each node's ``ssh_target`` is preserved exactly (present iff
   it was configured, i.e. non-None) -- reconstructing only
   ``hostname``/``role`` here would silently discard an explicit
   remote ``ssh_target`` override on every ``daemon smoke`` invocation
   (Polaris canary integration defect found after t_88d97d8e merged at
   41e7c64), causing the daemon to dial the node's bookkeeping
   ``hostname`` instead of the configured transport alias.
   """
   return {
      "system": config.system,
      "nodes": [
         {"hostname": node.hostname, "role": node.role}
         if node.ssh_target is None
         else {
            "hostname": node.hostname,
            "role": node.role,
            "ssh_target": node.ssh_target,
         }
         for node in config.nodes
      ],
      "output_root": config.output_root,
      "probe_python": config.probe_python,
      "counter_interval_sec": config.counter_interval_sec,
      "census_interval_sec": config.census_interval_sec,
      "rollup_interval_sec": config.rollup_interval_sec,
      "usage_interval_sec": config.usage_interval_sec,
      "duration_sec": config.duration_sec,
      "counter_timeout_sec": config.counter_timeout_sec,
      "census_timeout_sec": config.census_timeout_sec,
      "ssh_connect_timeout_sec": config.ssh_connect_timeout_sec,
      "max_parallel_polls": config.max_parallel_polls,
      "min_free_disk_pct": config.min_free_disk_pct,
      "keep_raw_args": config.keep_raw_args,
      "compress_census": config.compress_census,
   }


def _default_run_id():
   import time
   return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def _nested_to_phase0_raw(nested, duration_sec_override=_UNSET):
   """Convert a validated ``NodeMonitorConfig`` into the flat raw mapping
   that ``node_monitor.config.load_config`` accepts.

   This is the ONLY authorised conversion path from the nested layout
   to Phase0Config.  Every field is read from the appropriate nested
   section so no information is lost or silently defaulted:

   * ``collection.counter_rollup_interval_sec`` → ``rollup_interval_sec``
   * ``ssh.connect_timeout_sec`` → ``ssh_connect_timeout_sec``
   * ``ssh.counter_timeout_sec`` → ``counter_timeout_sec``
   * ``ssh.census_timeout_sec`` → ``census_timeout_sec``
   * node ``ssh_target`` is preserved exactly (present iff non-None)

   ``duration_sec_override``, when given, replaces the collection value
   for this invocation only (positive enforcement is the caller's
   responsibility before calling this helper).
   """
   nodes_raw = []
   for node in nested.nodes:
      entry = {"hostname": node.hostname, "role": node.role}
      if node.ssh_target is not None:
         entry["ssh_target"] = node.ssh_target
      nodes_raw.append(entry)

   col = nested.collection
   ssh = nested.ssh
   safety = nested.safety
   out = nested.output

   return {
      "system": nested.system,
      "nodes": nodes_raw,
      "probe_python": nested.probe_python,
      "output_root": out.root,
      "compress_census": out.compress_census,
      "counter_interval_sec": col.counter_interval_sec,
      "census_interval_sec": col.census_interval_sec,
      "rollup_interval_sec": col.counter_rollup_interval_sec,
      "usage_interval_sec": col.usage_interval_sec,
      "duration_sec": (nested.collection.duration_sec
                       if duration_sec_override is _UNSET
                       else duration_sec_override),
      "keep_raw_args": col.keep_raw_args,
      "counter_timeout_sec": ssh.counter_timeout_sec,
      "census_timeout_sec": ssh.census_timeout_sec,
      "ssh_connect_timeout_sec": ssh.connect_timeout_sec,
      "max_parallel_polls": ssh.max_parallel_polls,
      "min_free_disk_pct": safety.min_free_disk_pct,
   }


class _EngineAdapter:
   """Minimal database adapter for ``DatabaseWriter``.

   Exposes ``begin()`` over the shared migration engine so
   ``DatabaseWriter`` can open transactions without holding an
   independent engine reference or creating a second pool.
   The engine (and its pool) is owned and disposed by the caller.
   """

   def __init__(self, engine):
      self._engine = engine

   def begin(self):
      return self._engine.begin()


def _schema_gate_or_exit(engine, app_version):
   """Run ``MigrationRunner.status()`` read-only.

   Returns normally when the schema is fully current:
     initialized=True, pending_versions=(), drift=False,
     current_version == latest_version.

   Calls ``_exit(1, ...)`` for any rejection.  Never calls migrate().
   The engine is NOT disposed here; the caller disposes it in a
   ``finally`` block so disposal is guaranteed on every path.
   """
   try:
      runner = MigrationRunner(engine, app_version)
      status = runner.status()
   except MigrationError:
      _exit(1, "schema gate: database migration error; inspect operator logs",
            err=True)
      return  # pragma: no cover
   except Exception:
      _exit(1, "schema gate: database operation failed", err=True)
      return  # pragma: no cover

   if not status.initialized:
      _exit(1, "schema gate: database schema not initialized; "
               "run 'database migrate' first", err=True)
      return  # pragma: no cover
   if status.pending_versions:
      _exit(1, "schema gate: pending migrations: %s; "
               "run 'database migrate' first"
               % ", ".join(str(v) for v in status.pending_versions),
            err=True)
      return  # pragma: no cover
   if status.drift:
      _exit(1, "schema gate: applied migration checksum drift detected; "
               "inspect database before restarting", err=True)
      return  # pragma: no cover
   if status.current_version != status.latest_version:
      _exit(1, "schema gate: schema at version %d but latest is %d; "
               "run 'database migrate' first"
               % (status.current_version, status.latest_version),
            err=True)
      return  # pragma: no cover


def _run_daemon_postgres(nested, config_path, run_id, probe_version, home,
                         phase0_config=None, control_file=None,
                         startup_ack=None):
   """Wire the PostgreSQL sink and run the daemon.

   Order:
   1. Create engine (one pool, shared with writer adapter).
   2. Schema gate (status read-only; dispose engine and exit on failure).
   3. Create Phase0Sink (diagnostic JSONL only; no artifacts on gate fail).
   4. Create EngineAdapter + DatabaseWriter + PostgresDaemonSink.
   5. Await sink.start().
   6. Run Daemon; propagate exit code.
   7. Dispose engine unconditionally in finally.

   ``phase0_config`` is the already-validated Phase0Config derived from
   ``nested`` (with any duration override applied); it is built by the
   caller before this function is entered so that conversion errors fail
   before any pool is opened.
   """
   database = nested.database
   if phase0_config is None:
      raw = _nested_to_phase0_raw(nested)
      phase0_config = load_config(raw, home=home)

   # Step 1 -- one engine for both gate and writer.
   # create_engine failure is sanitized: no URL/driver detail in output.
   engine = None
   try:
      engine = _create_migration_engine(database)
   except Exception:
      _exit(1, "daemon run: could not initialize database connection",
            err=True)
      return  # pragma: no cover -- _exit always raises SystemExit

   try:
      # Step 2 -- read-only gate; _schema_gate_or_exit calls _exit on failure
      # which raises SystemExit.  Catch Exception (not BaseException) so
      # SystemExit from _exit propagates through the finally for disposal.
      _schema_gate_or_exit(engine, __version__)

      # Step 3 -- Phase0Sink created AFTER successful gate so no run
      # artifacts exist if the gate rejected the start.
      # OSError/Phase0SinkError during construction: sanitized, engine disposed.
      try:
         output_root = phase0_config.output_root
         os.makedirs(output_root, exist_ok=True)
         diagnostic_sink = Phase0Sink(
            output_root, run_id,
            metadata={"system": phase0_config.system},
            min_free_disk_pct=phase0_config.min_free_disk_pct,
            compress_census=phase0_config.compress_census,
            keep_raw_args=phase0_config.keep_raw_args,
         )
      except (OSError, Phase0SinkError):
         _exit(1, "daemon run: could not create diagnostic output directory",
               err=True)
         return  # pragma: no cover

      run_dir = diagnostic_sink.run_dir
      click.echo("run_dir: %s" % run_dir)

      # Step 4 -- one engine adapter; writer uses engine.begin() only.
      # Any construction exception here is sanitized.
      try:
         adapter = _EngineAdapter(engine)
         writer = DatabaseWriter(adapter)
         sink = PostgresDaemonSink(writer, diagnostic_sink)
         transport_fn = _make_transport_fn(phase0_config, probe_version)
         daemon = Daemon(phase0_config, sink, transport_fn)
      except Exception:
         _exit(1, "daemon run: could not initialize daemon components",
               err=True)
         return  # pragma: no cover

      if startup_ack is not None:
         startup_ack(run_dir)

      # Step 5+6 -- start the sink worker, run the daemon.
      # Unexpected exceptions from sink.start() or daemon.run() are
      # sanitized.  If daemon.run() raises after sink.start() succeeded,
      # abort() cancels and awaits the worker so it is not orphaned.
      async def _run():
         await sink.start()
         watcher = None
         if control_file is not None:
            watcher = asyncio.ensure_future(
               _watch_control_file(control_file, daemon))
         try:
            return await daemon.run()
         except Exception:
            # daemon.run() raised unexpectedly; abort the worker task.
            await sink.abort()
            raise
         finally:
            if watcher is not None:
               watcher.cancel()
               try:
                  await watcher
               except asyncio.CancelledError:
                  pass

      try:
         exit_code = asyncio.run(_run())
      except Exception:
         _exit(1, "daemon run: unexpected runtime failure", err=True)
         return  # pragma: no cover

   finally:
      if engine is not None:
         engine.dispose()

   if exit_code == EXIT_OK:
      click.echo("run complete")
   else:
      click.echo(
         "run ended with a fatal condition (exit %d)" % exit_code, err=True)
   return exit_code


def main():
   cli()


if __name__ == "__main__":
   main()
