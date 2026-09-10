"""Tests for node_monitor.daemon -- Phase 0 orchestration.

Design: PHASE0_DAEMON_DESIGN.md + PHASE0_DAEMON_IMPLEMENTATION_PLAN.md
Task 7 ("Daemon orchestration, lifecycle, and summary"). Per the
kanban recovery direction on this card, this increment covers only:

* a clean accelerated run produces a manifest (via the real
  ``Phase0Sink``), at least one ``node_counter_samples`` rollup
  record, a finalized ``summary.json``, and a DONE file;
* a fatal sink condition (disk-full) propagates as a nonzero exit
  code, and the run does NOT finalize/DONE afterward;
* ``node_monitor/daemon.py`` never imports a database driver, an ORM,
  or this project's own ``database``/``db`` packages.

Every other permutation in the Task 7 write-up (census/usage/hardware
wiring, structured poll-failure/collection-log records, signal
handling, acceptance evaluation) is explicitly deferred to a review
follow-up -- see daemon.py's module docstring for the same scope note.

All async behavior uses plain ``asyncio.run()`` inside ordinary (sync)
test functions, matching every other async test module in this
project (no pytest-asyncio plugin installed).
"""

import ast
import asyncio
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "fixtures"))

from node_monitor.config import load_config  # noqa: E402
from node_monitor.daemon import Daemon, EXIT_OK, EXIT_SINK_FATAL  # noqa: E402
from node_monitor.output.jsonl import Phase0Sink  # noqa: E402
from fake_clock import FakeClock  # noqa: E402


def _run(coro, timeout=10.0):
   return asyncio.run(asyncio.wait_for(coro, timeout=timeout))


def _config(tmp_path, **overrides):
   raw = {
      "system": "polaris",
      "nodes": [
         {"hostname": "polaris-login-04.example.org", "role": "local"},
      ],
      "output_root": "~/phase0-runs",
      "probe_python": "/usr/bin/python3.11",
      # Accelerated: 1-second counter interval, 3-second rollup --
      # a rollup fires after exactly 3 fake-clock samples instead of
      # a real 60-second window.
      "counter_interval_sec": 1,
      "rollup_interval_sec": 3,
      "duration_sec": overrides.pop("duration_sec", 10),
   }
   raw.update(overrides)
   home = str(tmp_path)
   os.makedirs(os.path.join(home, "phase0-runs"), exist_ok=True)
   return load_config(raw, home=home)


def _full_disk_usage(_path):
   class _Usage:
      total = 1_000_000_000
      used = 1_000_000
      free = 999_000_000
   return _Usage()


def _low_disk_usage(_path):
   class _Usage:
      total = 1_000_000_000
      used = 999_500_000
      free = 500_000
   return _Usage()


def _counter_payload(uptime_sec):
   """A minimal-but-valid counter-loop probe payload, matching the
   exact shape ``compute_counter_delta``/``CounterWindowAccumulator``
   consume (see tests/test_metrics.py's own fixtures for precedent).
   """
   return {
      "probe_version": 4,
      "loop": "counter",
      "hostname_fqdn": "polaris-login-04.example.org",
      "uptime_sec": uptime_sec,
      "counters": {
         "cpu_jiffies": {
            "user": 100 + int(uptime_sec), "nice": 0,
            "system": 50, "idle": 900, "iowait": 0,
            "irq": 0, "softirq": 0, "steal": 0,
         },
         "net": {},
         "md_ops": {},
         "mem": {"available_kb": 1000, "cached_kb": 200, "shmem_kb": 10},
         "load1": 0.1, "load5": 0.2, "load15": 0.3,
         "procs_running": 1, "procs_total": 100, "socket_count": 5,
      },
   }


# --------------------------------------------------------------------------
# Clean accelerated run: manifest + counter rollup + finalized summary + DONE
# --------------------------------------------------------------------------

class TestCleanRun:
   def test_clean_run_produces_manifest_rollup_summary_and_done(self, tmp_path):
      config = _config(tmp_path)
      output_root = os.path.join(str(tmp_path), "phase0-runs")
      sink = Phase0Sink(
         output_root, "daemon-run-1", metadata={"system": config.system},
         disk_usage_fn=_full_disk_usage)
      clock = FakeClock()

      call_count = {"n": 0}

      async def transport_fn(node, loop):
         call_count["n"] += 1
         return _counter_payload(uptime_sec=float(call_count["n"]))

      daemon = Daemon(config, sink, transport_fn,
                       clock=clock.time, sleep=clock.sleep)

      async def scenario():
         run_task = asyncio.ensure_future(daemon.run())
         await clock.advance(config.duration_sec)
         return await run_task

      exit_code = _run(scenario())

      assert exit_code == EXIT_OK

      manifest_path = os.path.join(sink.run_dir, "manifest.json")
      assert os.path.exists(manifest_path)
      with open(manifest_path) as handle:
         manifest = json.load(handle)
      assert manifest["system"] == "polaris"

      rollup_path = os.path.join(sink.run_dir, "node_counter_samples.jsonl")
      assert os.path.exists(rollup_path)
      with open(rollup_path) as handle:
         rollup_lines = [json.loads(line) for line in handle]
      # Design: "counter rollup every 60 seconds" -- accelerated here to
      # a 3-sample window (rollup_interval_sec=3, counter_interval_sec=1).
      # 10 fake-clock seconds of dispatch at a 1s interval means at least
      # one full 3-sample window closes.
      assert len(rollup_lines) >= 1
      record = rollup_lines[0]
      assert record["system"] == "polaris"
      assert record["source_hostname"] == "polaris-login-04.example.org"
      assert record["sample_count"] >= 1
      assert 0.0 <= record["coverage"] <= 1.0

      summary_path = os.path.join(sink.run_dir, "summary.json")
      assert os.path.exists(summary_path)
      with open(summary_path) as handle:
         summary = json.load(handle)
      assert summary["run_id"] == "daemon-run-1"
      assert summary["files"]["node_counter_samples"]["record_count"] >= 1

      assert os.path.exists(os.path.join(sink.run_dir, "DONE"))


# --------------------------------------------------------------------------
# Fatal sink error propagates nonzero and skips finalize/DONE
# --------------------------------------------------------------------------

class TestFatalSinkError:
   def test_fatal_disk_guard_stops_daemon_nonzero_without_finalizing(self, tmp_path):
      config = _config(tmp_path, duration_sec=20)
      output_root = os.path.join(str(tmp_path), "phase0-runs")
      sink = Phase0Sink(
         output_root, "daemon-run-2", metadata={"system": config.system},
         disk_usage_fn=_low_disk_usage)
      clock = FakeClock()

      call_count = {"n": 0}

      async def transport_fn(node, loop):
         call_count["n"] += 1
         return _counter_payload(uptime_sec=float(call_count["n"]))

      daemon = Daemon(config, sink, transport_fn,
                       clock=clock.time, sleep=clock.sleep)

      async def scenario():
         run_task = asyncio.ensure_future(daemon.run())
         await clock.advance(config.duration_sec)
         return await run_task

      exit_code = _run(scenario())

      assert exit_code == EXIT_SINK_FATAL
      # Never finalized: a fatal sink condition must never produce a
      # DONE-marked artifact that looks complete (design: "Output
      # write/flush failure or low disk is fatal").
      assert not os.path.exists(os.path.join(sink.run_dir, "summary.json"))
      assert not os.path.exists(os.path.join(sink.run_dir, "DONE"))

   def test_raw_oserror_from_write_record_stops_daemon_nonzero_without_finalizing(
         self, tmp_path):
      """Review round 1 finding: a real output write/flush failure
      from ``Phase0Sink.write_record()`` (``handle.write``,
      ``handle.flush``, ``os.fsync`` in node_monitor/output/jsonl.py)
      surfaces as a raw ``OSError``, NOT a ``Phase0SinkError``
      subclass -- only the pre-flight disk-guard check raises the
      dedicated ``Phase0SinkDiskFullError``. Before this fix, the
      daemon's counter-poll boundary only caught the sink's own
      exception types, so this raw OSError was swallowed by the
      Scheduler as an ordinary failed poll and the daemon went on to
      finalize_summary()/write_done() as if the run were clean --
      exactly the reproduction the reviewer reported (exit_code=0,
      finalized=True, done=True). This test drives a sink whose
      ``write_record`` raises ``OSError`` and asserts the run instead
      stops fatal, nonzero, with no summary/DONE.
      """
      config = _config(tmp_path, duration_sec=20)
      output_root = os.path.join(str(tmp_path), "phase0-runs")
      real_sink = Phase0Sink(
         output_root, "daemon-run-3", metadata={"system": config.system},
         disk_usage_fn=_full_disk_usage)

      class _WriteFailsSink:
         run_dir = real_sink.run_dir

         async def write_record(self, record_type, record):
            raise OSError("simulated fsync failure")

         async def finalize_summary(self):
            raise AssertionError(
               "finalize_summary() must never be called after a fatal "
               "write_record() OSError")

         def write_done(self):
            raise AssertionError(
               "write_done() must never be called after a fatal "
               "write_record() OSError")

      clock = FakeClock()
      call_count = {"n": 0}

      async def transport_fn(node, loop):
         call_count["n"] += 1
         return _counter_payload(uptime_sec=float(call_count["n"]))

      daemon = Daemon(config, _WriteFailsSink(), transport_fn,
                       clock=clock.time, sleep=clock.sleep)

      async def scenario():
         run_task = asyncio.ensure_future(daemon.run())
         await clock.advance(config.duration_sec)
         return await run_task

      exit_code = _run(scenario())

      assert exit_code == EXIT_SINK_FATAL
      assert not os.path.exists(os.path.join(real_sink.run_dir, "summary.json"))
      assert not os.path.exists(os.path.join(real_sink.run_dir, "DONE"))

   def test_raw_oserror_from_finalize_summary_stops_daemon_nonzero(self, tmp_path):
      """Review round 1 finding #2: ``finalize_summary()``/
      ``write_done()`` can also raise a raw ``OSError`` during their
      own flush/fsync/open/write calls -- that failure must map to
      ``EXIT_SINK_FATAL`` too, not escape ``Daemon.run()`` uncaught,
      and DONE must never be written when finalize_summary() itself
      failed.
      """
      config = _config(tmp_path, duration_sec=5)
      output_root = os.path.join(str(tmp_path), "phase0-runs")
      real_sink = Phase0Sink(
         output_root, "daemon-run-4", metadata={"system": config.system},
         disk_usage_fn=_full_disk_usage)

      class _FinalizeFailsSink:
         run_dir = real_sink.run_dir

         async def write_record(self, record_type, record):
            return None

         async def finalize_summary(self):
            raise OSError("simulated fsync failure during finalize")

         def write_done(self):
            raise AssertionError(
               "write_done() must never be called when finalize_summary() "
               "itself raised")

      clock = FakeClock()

      async def transport_fn(node, loop):
         return _counter_payload(uptime_sec=1.0)

      daemon = Daemon(config, _FinalizeFailsSink(), transport_fn,
                       clock=clock.time, sleep=clock.sleep)

      async def scenario():
         run_task = asyncio.ensure_future(daemon.run())
         await clock.advance(config.duration_sec)
         return await run_task

      exit_code = _run(scenario())

      assert exit_code == EXIT_SINK_FATAL
      assert not os.path.exists(os.path.join(real_sink.run_dir, "summary.json"))
      assert not os.path.exists(os.path.join(real_sink.run_dir, "DONE"))

   def test_raw_oserror_from_write_done_stops_daemon_nonzero(self, tmp_path):
      """Symmetric case: finalize_summary() itself succeeds but
      write_done() raises a raw OSError -- also fatal/nonzero. A
      summary.json without a DONE flag is the correct "partial, not
      complete" artifact state; the exit code must reflect that, not
      report EXIT_OK for a run whose DONE flag never landed.
      """
      config = _config(tmp_path, duration_sec=5)
      output_root = os.path.join(str(tmp_path), "phase0-runs")
      sink = Phase0Sink(
         output_root, "daemon-run-5", metadata={"system": config.system},
         disk_usage_fn=_full_disk_usage)

      original_write_done = sink.write_done

      def _failing_write_done():
         raise OSError("simulated fsync failure writing DONE")

      sink.write_done = _failing_write_done

      clock = FakeClock()

      async def transport_fn(node, loop):
         return _counter_payload(uptime_sec=1.0)

      daemon = Daemon(config, sink, transport_fn,
                       clock=clock.time, sleep=clock.sleep)

      async def scenario():
         run_task = asyncio.ensure_future(daemon.run())
         await clock.advance(config.duration_sec)
         return await run_task

      exit_code = _run(scenario())

      assert exit_code == EXIT_SINK_FATAL
      # finalize_summary() DID succeed here (only write_done() failed),
      # so summary.json legitimately exists -- but DONE must not.
      assert os.path.exists(os.path.join(sink.run_dir, "summary.json"))
      assert not os.path.exists(os.path.join(sink.run_dir, "DONE"))
      sink.write_done = original_write_done


# --------------------------------------------------------------------------
# No database import/connection reachable from daemon.py
# --------------------------------------------------------------------------

class TestNoDatabaseImports:
   def test_daemon_module_source_has_no_database_imports(self):
      """Static AST check over daemon.py's own import statements --
      catches `import psycopg2`, `import sqlalchemy`, `from
      node_monitor.database import ...` / `from node_monitor.db
      import ...` at the daemon.py source level itself, regardless of
      whether daemon.py happens to also import some OTHER module that
      transitively pulls one of these in (collector.hardware does;
      daemon.py must simply never import collector.hardware, checked
      as its own assertion below).
      """
      import node_monitor.daemon as daemon_mod

      source = open(daemon_mod.__file__).read()
      tree = ast.parse(source)
      forbidden_modules = ("psycopg2", "sqlalchemy",
                            "node_monitor.database", "node_monitor.db")
      forbidden_prefixes = tuple(m + "." for m in forbidden_modules)

      def _is_forbidden(name):
         return name in forbidden_modules or name.startswith(forbidden_prefixes)

      for node in ast.walk(tree):
         if isinstance(node, ast.Import):
            for alias in node.names:
               assert not _is_forbidden(alias.name), (
                  "forbidden database import: %s" % alias.name)
         elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            assert not _is_forbidden(module), (
               "forbidden database import: from %s import ..." % module)

   def test_daemon_module_does_not_import_hardware_collector(self):
      """node_monitor.collector.hardware is the one module in this
      project that DOES import sqlalchemy (it runs on the daemon
      host, not the monitored node, and is not wired into any daemon
      yet per its own module docstring) -- daemon.py must not import
      it, which is what actually keeps sqlalchemy unreachable from
      this module at runtime, not just absent from its own import
      statements.
      """
      import node_monitor.daemon as daemon_mod

      source = open(daemon_mod.__file__).read()
      tree = ast.parse(source)
      for node in ast.walk(tree):
         if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            assert "collector.hardware" not in module
         if isinstance(node, ast.Import):
            for alias in node.names:
               assert "collector.hardware" not in alias.name

   def test_sqlalchemy_not_in_sys_modules_after_importing_daemon(self):
      """Runtime check complementing the static AST checks above:
      importing node_monitor.daemon fresh in a subprocess must never
      cause sqlalchemy (or psycopg2) to land in sys.modules, i.e. no
      import chain reachable from daemon.py pulls in a database
      driver even indirectly.
      """
      import subprocess

      script = (
         "import sys; "
         "import node_monitor.daemon; "
         "bad = [m for m in ('sqlalchemy', 'psycopg2') if m in sys.modules]; "
         "sys.exit(1 if bad else 0)"
      )
      repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
      result = subprocess.run(
         [sys.executable, "-c", script], cwd=repo_root,
         capture_output=True, text=True, timeout=30)
      assert result.returncode == 0, (
         "daemon import pulled in a database module: stdout=%r stderr=%r"
         % (result.stdout, result.stderr))
