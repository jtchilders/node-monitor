"""node_monitor.cli.main -- Phase 0 CLI surface.

Design: PHASE0_DAEMON_DESIGN.md + PHASE0_DAEMON_IMPLEMENTATION_PLAN.md
Task 7 ("Add CLI commands: `daemon dry-run`, `daemon smoke`, and
`validate-run`. Status output must print config and output paths.").
PLANNING.md section 15 ("CLI surface"):
``node-monitor daemon dry-run --out FILE  # Phase 0: JSON to file, no DB``.

This module is intentionally thin: it resolves a strict
``node_monitor.config.Phase0Config``, wires the real local-invocation
transport (``node_monitor.collector.transport.run_local_probe``) and a
real ``node_monitor.output.jsonl.Phase0Sink`` into a
``node_monitor.daemon.Daemon``, runs it to completion, and maps its
exit code straight through to the process exit code. No orchestration,
retry, or scheduling policy of its own -- every one of those already
lives in ``daemon.py``/``collector/scheduler.py``, and this module must
never invent a parallel path around them (card: "Clarify command
behavior from the authoritative design/plan and existing APIs rather
than inventing parallel orchestration").

Scope for THIS increment (Task 7 CLI split): local-node-only end-to-end
execution. Every configured node must be local (``role: local``) --
remote/SSH fan-out wiring (``collector.transport.run_remote_probe``,
per-node SSH config/control-directory setup) is out of scope here and
is rejected up front with a clear diagnostic rather than silently
skipping remote nodes or half-wiring SSH. This mirrors the design's own
scope note ("this increment adds ... CLI ... wiring") and the plan's
explicit deferral of deploy/docs to Task 8.

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
import sys

import click
import yaml

from node_monitor.collector import transport
from node_monitor.collector.remote_probe import PROBE_VERSION
from node_monitor.config import ConfigError, load_config
from node_monitor.daemon import EXIT_OK, Daemon
from node_monitor.output.jsonl import (
   Phase0Sink,
   Phase0SinkDiskFullError,
   Phase0SinkError,
   validate_jsonl_artifact,
)

# node_monitor/collector/remote_probe.py's own local-invocation path,
# passed to transport.run_local_probe as the interpreter that runs it.
# The daemon never imports remote_probe.py's module body for its own
# behavior (README.md: "never imported by the daemon" -- that
# constraint is about the DAEMON's runtime import graph on a login
# node, not about a CLI-only constant); PROBE_VERSION is read here
# purely to pass the exact same expected version transport.py's own
# validate_probe_payload() already enforces per poll, so a probe/CLI
# version skew fails loudly as a validation error instead of silently
# comparing against a duplicated literal that could drift.
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


def _require_local_only(config):
   """This CLI increment only wires the local-invocation transport
   (see module docstring's scope note) -- reject a config declaring
   any remote node up front with a clear diagnostic, rather than
   silently never polling it or crashing deep inside a missing SSH
   wiring path.
   """
   remote = [node.hostname for node in config.remote_nodes]
   if remote:
      _exit(
         1,
         "this CLI increment only supports local-node execution; "
         "remove remote node(s) from config or run a future release: "
         "%s" % ", ".join(remote),
         err=True)


def _print_status(config, config_path, run_dir):
   click.echo("config: %s" % config_path)
   click.echo("system: %s" % config.system)
   click.echo("output_root: %s" % config.output_root)
   click.echo("run_dir: %s" % run_dir)
   click.echo("nodes: %s" % ", ".join(node.hostname for node in config.nodes))
   click.echo("duration_sec: %s" % config.duration_sec)


def _make_local_transport_fn(config):
   """Build the ``transport_fn(node, loop)`` callable ``Daemon`` expects,
   wired to the REAL ``collector.transport.run_local_probe`` for every
   configured (already validated all-local, see ``_require_local_only``)
   node -- never a test double. Returns a ``collector.transport.
   ProbeResult`` (not a plain payload dict), which ``Daemon._poll_fn``'s
   own ``_normalize_probe_result`` boundary already unwraps.
   """
   hard_timeout_sec = max(
      config.counter_timeout_sec, config.census_timeout_sec
   ) + _HARD_TIMEOUT_GRACE_SEC

   async def transport_fn(node, loop):
      if loop == "counter":
         max_seconds = config.counter_timeout_sec
      elif loop == "census":
         max_seconds = config.census_timeout_sec
      else:
         # hwinfo: one-shot pre-scheduler collection (Daemon._collect_
         # hardware) -- reuse the census timeout as a generous-enough
         # one-shot budget rather than inventing a fourth config knob
         # this increment's card does not ask for.
         max_seconds = config.census_timeout_sec
      return await transport.run_local_probe(
         probe_python=config.probe_python,
         probe_script_path=_REMOTE_PROBE_SCRIPT_PATH,
         loop=loop,
         probe_max_seconds=max_seconds,
         hard_timeout_sec=hard_timeout_sec,
         expected_probe_version=PROBE_VERSION,
         expected_fqdn=None,
      )

   return transport_fn


def _run_daemon(config, config_path, run_id):
   output_root = config.output_root
   os.makedirs(output_root, exist_ok=True)
   sink = Phase0Sink(
      output_root, run_id, metadata={"system": config.system},
      min_free_disk_pct=config.min_free_disk_pct)
   run_dir = sink.run_dir

   _print_status(config, config_path, run_dir)

   daemon = Daemon(config, sink, _make_local_transport_fn(config))
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
   _require_local_only(config)
   if run_id is None:
      run_id = _default_run_id()
   exit_code = _run_daemon(config, config_path, run_id)
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
   _require_local_only(config)
   raw_override = _config_to_raw(config)
   raw_override["duration_sec"] = duration_sec
   config = load_config(raw_override, home=home)
   if run_id is None:
      run_id = _default_run_id()
   exit_code = _run_daemon(config, config_path, run_id)
   sys.exit(exit_code)


@cli.command("validate-run")
@click.argument("run_dir", type=click.Path(exists=False))
def validate_run(run_dir):
   """Validate a completed (or partial) Phase 0 run directory's artifacts.

   Checks: the run directory exists, every production/diagnostic JSONL
   file present validates with zero malformed lines (a truncated FINAL
   line is reported but does not by itself fail validation -- exactly
   ``node_monitor.output.jsonl.validate_jsonl_artifact``'s own
   truncated-vs-malformed distinction), and a ``DONE`` flag is present
   (design: "DONE is written only after summary finalization" -- its
   absence means the run never cleanly finished, which this command
   must not silently accept as valid).
   """
   abs_run_dir = os.path.abspath(run_dir)
   if not os.path.isdir(abs_run_dir):
      _exit(1, "run directory not found: %s" % (run_dir,), err=True)
      return  # pragma: no cover

   click.echo("run_dir: %s" % abs_run_dir)

   problems = []
   any_file_checked = False
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

   done_path = os.path.join(abs_run_dir, "DONE")
   done_present = os.path.exists(done_path)
   click.echo("DONE present: %s" % done_present)
   if not done_present:
      problems.append("DONE flag is missing -- run did not finalize cleanly")

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
   valid.
   """
   return {
      "system": config.system,
      "nodes": [
         {"hostname": node.hostname, "role": node.role}
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
