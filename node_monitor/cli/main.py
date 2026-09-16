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
import os
import subprocess
import sys

import click
import yaml

from node_monitor.collector import transport
from node_monitor.config import ConfigError, load_config
from node_monitor.daemon import EXIT_OK, Daemon
from node_monitor.output.jsonl import (
   Phase0Sink,
   Phase0SinkDiskFullError,
   Phase0SinkError,
   validate_jsonl_artifact,
)

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


def _exit(code, message=None, err=False):
   if message is not None:
      click.echo(message, err=err)
   sys.exit(code)


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
      )

   return transport_fn


def _run_daemon(config, config_path, run_id, probe_version):
   output_root = config.output_root
   os.makedirs(output_root, exist_ok=True)
   sink = Phase0Sink(
      output_root, run_id, metadata={"system": config.system},
      min_free_disk_pct=config.min_free_disk_pct)
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
      if not filename.endswith(".jsonl"):
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
   }


def _default_run_id():
   import time
   return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def main():
   cli()


if __name__ == "__main__":
   main()
