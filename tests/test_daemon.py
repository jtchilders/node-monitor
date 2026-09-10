"""Tests for node_monitor.daemon -- Phase 0 orchestration.

Design: PHASE0_DAEMON_DESIGN.md + PHASE0_DAEMON_IMPLEMENTATION_PLAN.md
Task 7 ("Daemon orchestration, lifecycle, and summary"). This second
increment (building on the first increment's counter-loop-only
orchestration core) covers:

* a clean accelerated run produces a manifest (via the real
  ``Phase0Sink``), at least one ``node_counter_samples`` rollup
  record, one ``node_hardware`` record per configured node, at least
  one ``diagnostic_census`` record, at least one
  ``node_usage_intervals`` record, a finalized ``summary.json``, and a
  DONE file;
* one-time hardware collection per node runs before the scheduler
  starts, is not fatal to the run when the probe itself fails (only a
  sink write failure is), and is logged via ``node_collection_log``
  when it fails;
* a census poll produces a ``diagnostic_census`` record every poll and
  a bounded ``node_usage_intervals`` record every
  ``usage_interval_sec // census_interval_sec`` census samples;
* raw argv/cmdline is never persisted end to end, even when a hostile
  probe payload carries it;
* a sink write failure for any of the three record types this
  increment adds (``node_hardware``, ``diagnostic_census``,
  ``node_usage_intervals``) is fatal -- same contract as the first
  increment already proved for ``node_counter_samples``;
* a fatal sink condition (disk-full) propagates as a nonzero exit
  code, and the run does NOT finalize/DONE afterward;
* ``node_monitor/daemon.py`` never imports a database driver, an ORM,
  or this project's own ``database``/``db`` packages.

Every other permutation in the Task 7 write-up (structured
``node_poll_failures`` records for ordinary scheduled-poll failures,
signal handling, the partial-vs-clean acceptance evaluation,
CLI/setup/deploy/docs) is explicitly deferred to a follow-up -- see
daemon.py's module docstring for the same scope note.

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
from node_monitor.output.contracts import (  # noqa: E402
   validate_diagnostic_census,
   validate_node_hardware,
   validate_node_usage_intervals,
)
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
      # Accelerated: 1-second counter/census intervals, 3-sample
      # rollup/usage windows -- a rollup or usage-interval record
      # fires after exactly 3 fake-clock samples instead of a real
      # 60-second/900-second window.
      "counter_interval_sec": 1,
      "rollup_interval_sec": 3,
      "census_interval_sec": 1,
      "usage_interval_sec": 3,
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


def _counter_payload(uptime_sec, hostname="polaris-login-04.example.org"):
   """A minimal-but-valid counter-loop probe payload, matching the
   exact shape ``compute_counter_delta``/``CounterWindowAccumulator``
   consume (see tests/test_metrics.py's own fixtures for precedent).
   """
   return {
      "probe_version": 4,
      "loop": "counter",
      "hostname_fqdn": hostname,
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


def _hwinfo_payload(hostname="polaris-login-04.example.org"):
   """A minimal-but-valid hwinfo-loop probe payload, matching the exact
   shape ``remote_probe.py``'s ``_collect_hardware()`` produces under
   ``payload["hardware"]`` -- see tests/test_hardware.py's own
   ``_hw()`` fixture for the same field set (minus the bookkeeping
   columns ``system``/``source_hostname``/``first_seen_utc`` that only
   the daemon, not the probe, knows).
   """
   return {
      "probe_version": 4,
      "loop": "hwinfo",
      "hostname_fqdn": hostname,
      "wall_clock_utc": "2026-09-09T00:00:00Z",
      "hardware": {
         "cpu_model": "AMD EPYC 7713",
         "cpu_logical": 256,
         "sockets": 2,
         "cores_per_socket": 64,
         "cpu_max_freq_khz": 2000000,
         "numa_nodes": 2,
         "mem_total_kb": 527954112,
         "swap_total_kb": 0,
         "hugepage_size_kb": 2048,
         "kernel_release": "6.4.0",
         "os_pretty_name": "SLES 15 SP7",
         "net_fs_mounts": 7,
         "net_ifaces": {"bond0": 1000},
         "boot_id": "boot-a",
         "btime": 1700000000,
         "gpus": [],
      },
   }


def _census_payload(uptime_sec, pids=(1,), utime_ticks=100,
                     hostname="polaris-login-04.example.org"):
   """A minimal-but-valid census-loop probe payload, matching the exact
   shape ``build_diagnostic_census``/``build_usage_observations``
   consume (see tests/test_usage.py's own ``_census``/``_row`` helpers
   for precedent). Every row uses ``category="other"``/``activity=
   None`` (normalized by ``UsageIntervalAccumulator`` to ``"unknown"``)
   and ``username="jchilders"`` so tests can assert on a single,
   predictable grain.
   """
   return {
      "probe_version": 4,
      "loop": "census",
      "hostname_fqdn": hostname,
      "wall_clock_utc": "2026-09-09T00:00:00Z",
      "uptime_sec": uptime_sec,
      "counters": {"clk_tck": 100},
      "processes": [
         {
            "pid": pid, "start_time_ticks": 5, "utime_ticks": utime_ticks,
            "stime_ticks": 0, "category": "other", "activity": None,
            "username": "jchilders", "rss_kb": 1000, "state": "S",
            "interactive": False,
         }
         for pid in pids
      ],
   }


def _make_transport_fn():
   """Loop-aware default ``transport_fn``: ``hwinfo`` always returns a
   fixed hardware payload keyed off the requested node's own hostname;
   ``counter``/``census`` return accelerated incrementing payloads,
   with independent per-(node, loop) call counters so the two loops'
   own cadences never interfere with each other -- mirrors a real
   daemon where the counter and census loops for one node are
   dispatched as two entirely independent ``Scheduler`` targets.
   """
   call_counts = {}

   async def transport_fn(node, loop):
      if loop == "hwinfo":
         return _hwinfo_payload(hostname=node.hostname)
      key = (node.hostname, loop)
      call_counts[key] = call_counts.get(key, 0) + 1
      n = call_counts[key]
      if loop == "census":
         return _census_payload(uptime_sec=float(n), hostname=node.hostname)
      return _counter_payload(uptime_sec=float(n), hostname=node.hostname)

   return transport_fn


class _SelectiveWriteFailsSink:
   """Wraps a real ``Phase0Sink`` so ``write_record`` raises for
   exactly one chosen ``record_type`` and delegates to the real sink
   for every other type -- lets a test target a fatal-write assertion
   at one specific record type (``node_hardware``,
   ``diagnostic_census``, ``node_usage_intervals``) without a bespoke
   stub class per type, while still mirroring the hand-written
   ``_WriteFailsSink``/``_FinalizeFailsSink`` stubs' own
   ``run_dir``/``finalize_summary``/``write_done`` never-called
   contract below.
   """

   def __init__(self, real_sink, failing_record_type):
      self._real_sink = real_sink
      self._failing_record_type = failing_record_type
      self.run_dir = real_sink.run_dir

   async def write_record(self, record_type, record):
      if record_type == self._failing_record_type:
         raise OSError(
            "simulated fsync failure for %s" % record_type)
      await self._real_sink.write_record(record_type, record)

   async def finalize_summary(self):
      raise AssertionError(
         "finalize_summary() must never be called after a fatal "
         "write_record() OSError")

   def write_done(self):
      raise AssertionError(
         "write_done() must never be called after a fatal "
         "write_record() OSError")


# --------------------------------------------------------------------------
# Clean accelerated run: manifest + hardware + rollup + census + usage +
# finalized summary + DONE
# --------------------------------------------------------------------------

class TestCleanRun:
   def test_clean_run_produces_every_record_type_summary_and_done(self, tmp_path):
      config = _config(tmp_path)
      output_root = os.path.join(str(tmp_path), "phase0-runs")
      sink = Phase0Sink(
         output_root, "daemon-run-1", metadata={"system": config.system},
         disk_usage_fn=_full_disk_usage)
      clock = FakeClock()

      daemon = Daemon(config, sink, _make_transport_fn(),
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

      hw_path = os.path.join(sink.run_dir, "node_hardware.jsonl")
      assert os.path.exists(hw_path)
      with open(hw_path) as handle:
         hw_lines = [json.loads(line) for line in handle]
      # Design: "node_hardware: one record per node when first seen in
      # the run" -- exactly one row for this single-node config, never
      # re-collected on a later poll of the same node.
      assert len(hw_lines) == 1
      validate_node_hardware(hw_lines[0])
      assert hw_lines[0]["system"] == "polaris"
      assert hw_lines[0]["source_hostname"] == "polaris-login-04.example.org"
      assert hw_lines[0]["cpu_model"] == "AMD EPYC 7713"

      census_path = os.path.join(sink.run_dir, "diagnostic_censuses.jsonl")
      assert os.path.exists(census_path)
      with open(census_path) as handle:
         census_lines = [json.loads(line) for line in handle]
      assert len(census_lines) >= 1
      for census_record in census_lines:
         validate_diagnostic_census(census_record)
         assert census_record["system"] == "polaris"
         assert census_record["source_hostname"] == "polaris-login-04.example.org"

      usage_path = os.path.join(sink.run_dir, "node_usage_intervals.jsonl")
      assert os.path.exists(usage_path)
      with open(usage_path) as handle:
         usage_lines = [json.loads(line) for line in handle]
      # Accelerated 3-sample usage window (usage_interval_sec=3,
      # census_interval_sec=1); 10 fake-clock seconds of dispatch means
      # at least one full window closes.
      assert len(usage_lines) >= 1
      for usage_record in usage_lines:
         validate_node_usage_intervals(usage_record)
         assert usage_record["system"] == "polaris"
         assert usage_record["source_hostname"] == "polaris-login-04.example.org"
         assert usage_record["category"] == "other"
         assert usage_record["activity"] == "unknown"
         assert usage_record["username"] == "jchilders"
         assert usage_record["sample_count"] >= 1

      summary_path = os.path.join(sink.run_dir, "summary.json")
      assert os.path.exists(summary_path)
      with open(summary_path) as handle:
         summary = json.load(handle)
      assert summary["run_id"] == "daemon-run-1"
      assert summary["files"]["node_counter_samples"]["record_count"] >= 1
      assert summary["files"]["node_hardware"]["record_count"] == 1
      assert summary["files"]["diagnostic_census"]["record_count"] >= 1
      assert summary["files"]["node_usage_intervals"]["record_count"] >= 1

      assert os.path.exists(os.path.join(sink.run_dir, "DONE"))


# --------------------------------------------------------------------------
# Hardware-once collection: one record per node, non-fatal probe failure,
# fatal sink-write failure.
# --------------------------------------------------------------------------

class TestHardwareOnceCollection:
   def test_hardware_once_produces_one_record_per_configured_node(self, tmp_path):
      config = _config(tmp_path, nodes=[
         {"hostname": "polaris-login-04.example.org", "role": "local"},
         {"hostname": "polaris-login-05.example.org", "role": "remote"},
      ], duration_sec=5)
      output_root = os.path.join(str(tmp_path), "phase0-runs")
      sink = Phase0Sink(
         output_root, "daemon-run-hw-1", metadata={"system": config.system},
         disk_usage_fn=_full_disk_usage)
      clock = FakeClock()

      daemon = Daemon(config, sink, _make_transport_fn(),
                       clock=clock.time, sleep=clock.sleep)

      async def scenario():
         run_task = asyncio.ensure_future(daemon.run())
         await clock.advance(config.duration_sec)
         return await run_task

      exit_code = _run(scenario())

      assert exit_code == EXIT_OK
      hw_path = os.path.join(sink.run_dir, "node_hardware.jsonl")
      with open(hw_path) as handle:
         records = [json.loads(line) for line in handle]
      hostnames = {record["source_hostname"] for record in records}
      assert hostnames == {
         "polaris-login-04.example.org", "polaris-login-05.example.org"}
      for record in records:
         validate_node_hardware(record)

   def test_hardware_collection_failure_is_not_fatal_and_is_logged(self, tmp_path):
      config = _config(tmp_path, duration_sec=5)
      output_root = os.path.join(str(tmp_path), "phase0-runs")
      sink = Phase0Sink(
         output_root, "daemon-run-hw-2", metadata={"system": config.system},
         disk_usage_fn=_full_disk_usage)
      clock = FakeClock()
      call_counts = {"counter": 0, "census": 0}

      async def transport_fn(node, loop):
         if loop == "hwinfo":
            raise RuntimeError("simulated hwinfo probe timeout")
         call_counts[loop] += 1
         if loop == "census":
            return _census_payload(uptime_sec=float(call_counts["census"]))
         return _counter_payload(uptime_sec=float(call_counts["counter"]))

      daemon = Daemon(config, sink, transport_fn,
                       clock=clock.time, sleep=clock.sleep)

      async def scenario():
         run_task = asyncio.ensure_future(daemon.run())
         await clock.advance(config.duration_sec)
         return await run_task

      exit_code = _run(scenario())

      # A hwinfo probe failure is not fatal to the run -- design only
      # calls output write/flush failure or low disk fatal; counter and
      # census collection for the node proceed with no hardware baseline.
      assert exit_code == EXIT_OK
      hw_path = os.path.join(sink.run_dir, "node_hardware.jsonl")
      assert not os.path.exists(hw_path)

      log_path = os.path.join(sink.run_dir, "node_collection_log.jsonl")
      assert os.path.exists(log_path)
      with open(log_path) as handle:
         entries = [json.loads(line) for line in handle]
      failures = [e for e in entries if e["event"] == "hardware_collection_failed"]
      assert len(failures) == 1
      assert failures[0]["system"] == "polaris"
      assert failures[0]["detail"]["source_hostname"] == \
         "polaris-login-04.example.org"

      # Counter/census loops still ran normally despite the hardware
      # collection failure.
      rollup_path = os.path.join(sink.run_dir, "node_counter_samples.jsonl")
      assert os.path.exists(rollup_path)

   def test_hardware_write_failure_is_fatal_without_finalizing(self, tmp_path):
      config = _config(tmp_path, duration_sec=20)
      output_root = os.path.join(str(tmp_path), "phase0-runs")
      real_sink = Phase0Sink(
         output_root, "daemon-run-hw-3", metadata={"system": config.system},
         disk_usage_fn=_full_disk_usage)
      sink = _SelectiveWriteFailsSink(real_sink, "node_hardware")
      clock = FakeClock()

      daemon = Daemon(config, sink, _make_transport_fn(),
                       clock=clock.time, sleep=clock.sleep)

      async def scenario():
         run_task = asyncio.ensure_future(daemon.run())
         await clock.advance(config.duration_sec)
         return await run_task

      exit_code = _run(scenario())

      assert exit_code == EXIT_SINK_FATAL
      assert not os.path.exists(os.path.join(real_sink.run_dir, "summary.json"))
      assert not os.path.exists(os.path.join(real_sink.run_dir, "DONE"))
      # The scheduler must never even have started: a fatal hardware
      # write aborts the run before any counter/census polling begins.
      assert not os.path.exists(
         os.path.join(real_sink.run_dir, "node_counter_samples.jsonl"))


# --------------------------------------------------------------------------
# Census -> diagnostic_census + node_usage_intervals wiring
# --------------------------------------------------------------------------

class TestCensusAndUsageWiring:
   def test_census_poll_produces_a_diagnostic_census_record_each_poll(self, tmp_path):
      config = _config(tmp_path, duration_sec=5)
      output_root = os.path.join(str(tmp_path), "phase0-runs")
      sink = Phase0Sink(
         output_root, "daemon-run-census-1", metadata={"system": config.system},
         disk_usage_fn=_full_disk_usage)
      clock = FakeClock()

      daemon = Daemon(config, sink, _make_transport_fn(),
                       clock=clock.time, sleep=clock.sleep)

      async def scenario():
         run_task = asyncio.ensure_future(daemon.run())
         await clock.advance(config.duration_sec)
         return await run_task

      exit_code = _run(scenario())

      assert exit_code == EXIT_OK
      census_path = os.path.join(sink.run_dir, "diagnostic_censuses.jsonl")
      with open(census_path) as handle:
         census_lines = [json.loads(line) for line in handle]
      assert len(census_lines) >= 1
      for record in census_lines:
         validate_diagnostic_census(record)
         assert record["probe_version"] == 4

   def test_usage_interval_flushes_after_expected_census_samples(self, tmp_path):
      config = _config(tmp_path, duration_sec=10)
      output_root = os.path.join(str(tmp_path), "phase0-runs")
      sink = Phase0Sink(
         output_root, "daemon-run-usage-1", metadata={"system": config.system},
         disk_usage_fn=_full_disk_usage)
      clock = FakeClock()

      daemon = Daemon(config, sink, _make_transport_fn(),
                       clock=clock.time, sleep=clock.sleep)

      async def scenario():
         run_task = asyncio.ensure_future(daemon.run())
         await clock.advance(config.duration_sec)
         return await run_task

      exit_code = _run(scenario())

      assert exit_code == EXIT_OK
      usage_path = os.path.join(sink.run_dir, "node_usage_intervals.jsonl")
      with open(usage_path) as handle:
         usage_lines = [json.loads(line) for line in handle]
      assert len(usage_lines) >= 1
      record = usage_lines[0]
      validate_node_usage_intervals(record)
      assert record["sample_count"] >= 1
      assert record["expected_count"] == 3

   def test_census_write_failure_is_fatal_without_finalizing(self, tmp_path):
      config = _config(tmp_path, duration_sec=20)
      output_root = os.path.join(str(tmp_path), "phase0-runs")
      real_sink = Phase0Sink(
         output_root, "daemon-run-census-2", metadata={"system": config.system},
         disk_usage_fn=_full_disk_usage)
      sink = _SelectiveWriteFailsSink(real_sink, "diagnostic_census")
      clock = FakeClock()

      daemon = Daemon(config, sink, _make_transport_fn(),
                       clock=clock.time, sleep=clock.sleep)

      async def scenario():
         run_task = asyncio.ensure_future(daemon.run())
         await clock.advance(config.duration_sec)
         return await run_task

      exit_code = _run(scenario())

      assert exit_code == EXIT_SINK_FATAL
      assert not os.path.exists(os.path.join(real_sink.run_dir, "summary.json"))
      assert not os.path.exists(os.path.join(real_sink.run_dir, "DONE"))

   def test_usage_interval_write_failure_is_fatal_without_finalizing(self, tmp_path):
      config = _config(tmp_path, duration_sec=20)
      output_root = os.path.join(str(tmp_path), "phase0-runs")
      real_sink = Phase0Sink(
         output_root, "daemon-run-usage-2", metadata={"system": config.system},
         disk_usage_fn=_full_disk_usage)
      sink = _SelectiveWriteFailsSink(real_sink, "node_usage_intervals")
      clock = FakeClock()

      daemon = Daemon(config, sink, _make_transport_fn(),
                       clock=clock.time, sleep=clock.sleep)

      async def scenario():
         run_task = asyncio.ensure_future(daemon.run())
         await clock.advance(config.duration_sec)
         return await run_task

      exit_code = _run(scenario())

      assert exit_code == EXIT_SINK_FATAL
      assert not os.path.exists(os.path.join(real_sink.run_dir, "summary.json"))
      assert not os.path.exists(os.path.join(real_sink.run_dir, "DONE"))


# --------------------------------------------------------------------------
# Raw argv/cmdline is never persisted, even from a hostile probe payload,
# proved through the full daemon wiring (not just build_diagnostic_census
# in isolation, which tests/test_usage.py already covers).
# --------------------------------------------------------------------------

class TestRawArgvNeverPersistedEndToEnd:
   def test_daemon_never_persists_cmdline_from_a_hostile_census_payload(
         self, tmp_path):
      config = _config(tmp_path, duration_sec=5)
      output_root = os.path.join(str(tmp_path), "phase0-runs")
      sink = Phase0Sink(
         output_root, "daemon-run-argv-1", metadata={"system": config.system},
         disk_usage_fn=_full_disk_usage)
      clock = FakeClock()

      async def transport_fn(node, loop):
         if loop == "hwinfo":
            return _hwinfo_payload()
         if loop == "census":
            payload = _census_payload(uptime_sec=1.0)
            payload["processes"][0]["cmdline"] = (
               "/usr/bin/python3 --secret-token abc123")
            return payload
         return _counter_payload(uptime_sec=1.0)

      daemon = Daemon(config, sink, transport_fn,
                       clock=clock.time, sleep=clock.sleep)

      async def scenario():
         run_task = asyncio.ensure_future(daemon.run())
         await clock.advance(config.duration_sec)
         return await run_task

      exit_code = _run(scenario())

      assert exit_code == EXIT_OK
      census_path = os.path.join(sink.run_dir, "diagnostic_censuses.jsonl")
      assert os.path.exists(census_path)
      with open(census_path, "rb") as handle:
         raw_bytes = handle.read()
      assert b"secret-token" not in raw_bytes
      assert b"cmdline" not in raw_bytes


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

      daemon = Daemon(config, sink, _make_transport_fn(),
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
      """Review round 1 finding (first increment): a real output
      write/flush failure from ``Phase0Sink.write_record()``
      (``handle.write``, ``handle.flush``, ``os.fsync`` in
      node_monitor/output/jsonl.py) surfaces as a raw ``OSError``, NOT
      a ``Phase0SinkError`` subclass -- only the pre-flight disk-guard
      check raises the dedicated ``Phase0SinkDiskFullError``. This
      test drives a sink whose ``write_record`` raises ``OSError`` for
      EVERY record type (including the ``node_hardware`` write that
      now happens before the scheduler even starts) and asserts the
      run stops fatal, nonzero, with no summary/DONE.
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

      daemon = Daemon(config, _WriteFailsSink(), _make_transport_fn(),
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
      """Review round 1 finding #2 (first increment): ``finalize_
      summary()``/``write_done()`` can also raise a raw ``OSError``
      during their own flush/fsync/open/write calls -- that failure
      must map to ``EXIT_SINK_FATAL`` too, not escape ``Daemon.run()``
      uncaught, and DONE must never be written when finalize_summary()
      itself failed.
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

      daemon = Daemon(config, _FinalizeFailsSink(), _make_transport_fn(),
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

      daemon = Daemon(config, sink, _make_transport_fn(),
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
      project that DOES import sqlalchemy (it is the future
      production-database upsert path, keyed on this project's own
      SQLAlchemy-based ``node_hardware`` table, not this Phase 0
      JSONL-only daemon's) -- daemon.py must not import it, which is
      what actually keeps sqlalchemy unreachable from this module at
      runtime, not just absent from its own import statements. This
      increment adds its OWN minimal, pure ``node_hardware`` contract
      record builder instead of importing that module.
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
