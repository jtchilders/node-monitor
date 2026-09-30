"""Real-PostgreSQL daemon acceptance tests.

Task 4 -- exercises the full production wiring:
  MigrationRunner.migrate on a disposable UUID-named database,
  actual PostgresDaemonSink + DatabaseWriter + Daemon,
  deterministic fake transport / fake clock,
  all FIVE relational tables receive rows,
  diagnostic_censuses JSONL file exists and has records,
  no relational diagnostic table,
  idempotency (hardware/counter/usage ON CONFLICT), append events,
  schema gate rejects uninitialized DB,
  database outage (invalid URL) fails cleanly with fixed output and nonzero exit.

No production code is modified unless a defect is exposed.
"""

import asyncio
import json
import os
import socket
import sys
import uuid
from datetime import datetime, timezone

import pytest
import yaml
from click.testing import CliRunner
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.pool import NullPool

from node_monitor.collector.scheduler import BREAKER_CLOSED
from node_monitor.config import NodeConfig, Phase0Config
from node_monitor.daemon import EXIT_OK, EXIT_SINK_FATAL, Daemon
from node_monitor.cli.main import cli
from node_monitor.database.migration import MigrationRunner
from node_monitor.database.writer import DatabaseWriter
from node_monitor.output.jsonl import Phase0Sink
from node_monitor.output.postgres import PostgresDaemonSink


# ---------------------------------------------------------------------------
# Module-level skip gate (matches existing postgres test convention)
# ---------------------------------------------------------------------------

pytestmark = pytest.mark.skipif(
   not os.environ.get("NODE_MONITOR_TEST_DATABASE_URL"),
   reason="NODE_MONITOR_TEST_DATABASE_URL is required",
)


# ---------------------------------------------------------------------------
# Shared disposable-database fixture (UUID-named, exact cleanup)
# ---------------------------------------------------------------------------

@pytest.fixture
def disposable_engine():
   """Create a UUID-named disposable PostgreSQL database, yield an engine
   for it, and guarantee exact cleanup regardless of test outcome."""
   admin_url = make_url(os.environ["NODE_MONITOR_TEST_DATABASE_URL"])
   db_name = "node_monitor_test_" + uuid.uuid4().hex
   admin_engine = create_engine(
      admin_url, poolclass=NullPool, isolation_level="AUTOCOMMIT")
   with admin_engine.connect() as conn:
      version = int(conn.exec_driver_sql(
         "SHOW server_version_num").scalar_one())
      if version < 120000:
         pytest.skip("PostgreSQL 12 or newer is required")
      can_create = conn.exec_driver_sql(
         "SELECT rolcreatedb FROM pg_roles WHERE rolname = current_user"
      ).scalar_one()
      if not can_create:
         pytest.skip("test role lacks CREATEDB")
      conn.exec_driver_sql('CREATE DATABASE "%s"' % db_name)

   engine = create_engine(
      admin_url.set(database=db_name), poolclass=NullPool)
   try:
      yield engine, db_name
   finally:
      engine.dispose()
      with admin_engine.connect() as conn:
         # Terminate exact datname sessions only
         conn.exec_driver_sql(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()",
            (db_name,),
         )
         conn.exec_driver_sql(
            'DROP DATABASE IF EXISTS "%s"' % db_name)
      admin_engine.dispose()


def _fresh_query_engine(engine):
   """Return a NullPool engine using the same URL (fresh session)."""
   return create_engine(str(engine.url), poolclass=NullPool)


def _write_nested_config(tmp_path, database_url):
   """Write a strict nested config suitable for real CLI acceptance."""
   output_root = tmp_path / "cli-runs"
   raw = {
      "system": _SYSTEM,
      "nodes": [{"hostname": socket.getfqdn(), "role": "local"}],
      "probe_python": "python3.11",
      "output": {"root": str(output_root)},
      "collection": {},
      "ssh": {},
      "safety": {},
      "database": {
         "url": database_url,
         "schema": "node_monitor",
         "pool_size": 1,
         "max_overflow": 0,
      },
      "retention": {},
   }
   path = tmp_path / "config.yaml"
   path.write_text(yaml.safe_dump(raw))
   return path, output_root


# ---------------------------------------------------------------------------
# Probe payload factories -- minimal, contract-valid
# ---------------------------------------------------------------------------

_SYSTEM = "test-polaris"
_FQDN = "login-04.example.org"
_PROBE_VER = 4


def _hw_payload(fqdn=_FQDN):
   """Minimal hwinfo probe payload."""
   return {
      "loop": "hwinfo",
      "hostname_fqdn": fqdn,
      "probe_version": _PROBE_VER,
      "hardware": {
         "boot_id": "boot-accept",
         "btime": 100,
         "cpu_model": "Zen",
         "cpu_logical": 64,
         "sockets": 2,
         "cores_per_socket": 16,
         "cpu_max_freq_khz": 3500000,
         "numa_nodes": 4,
         "mem_total_kb": 1048576,
         "swap_total_kb": 0,
         "hugepage_size_kb": 2048,
         "kernel_release": "6.1",
         "os_pretty_name": "Linux",
         "net_fs_mounts": 2,
         "net_ifaces": {"hsn0": {"speed_mbps": 100000}},
         "gpus": [],
      },
   }


def _counter_payload(fqdn=_FQDN, uptime_sec=340091.15):
   """Minimal counter probe payload (produces valid counter records)."""
   return {
      "loop": "counter",
      "hostname_fqdn": fqdn,
      "probe_version": _PROBE_VER,
      "sample_count": 1,
      "uptime_sec": uptime_sec,
      "mem": {
         "MemAvailable": 900000,
         "Cached": 80000,
         "Shmem": 5000,
         "MemTotal": 1048576,
         "SwapTotal": 0,
         "MemFree": 200000,
         "Buffers": 3000,
         "SwapFree": 0,
      },
      "load": {"load1": 1.0, "load5": 2.0, "load15": 3.0},
      "procs": {"running": 2, "total": 100},
      "sockets": 8,
      "cpu": {
         "user": 1000, "nice": 0, "system": 500, "idle": 8264041757,
         "iowait": 97248194, "irq": 0, "softirq": 5580295,
         "steal": 0, "guest": 0, "guest_nice": 0,
      },
      "net": {
         "hsn0": {"rx_bytes": 100000, "rx_packets": 50, "rx_errors": 0,
                  "rx_drop": 0, "tx_bytes": 50000, "tx_packets": 25,
                  "tx_errors": 0, "tx_drop": 0},
      },
      "lustre": {},
   }


def _census_payload(fqdn=_FQDN, uptime_sec=340091.15, utime_ticks=200):
   """Minimal census probe payload."""
   return {
      "loop": "census",
      "hostname_fqdn": fqdn,
      "probe_version": _PROBE_VER,
      "sample_count": 1,
      "uptime_sec": uptime_sec,
      "wall_clock_utc": _BASE_UTC,
      "processes": [
         {
            "pid": 100,
            "start_time_ticks": 98765,
            "utime_ticks": utime_ticks,
            "stime_ticks": 100,
            "category": "shell/session",
            "activity": "shell",
            "username": "alice",
            "rss_kb": 4096,
            "state": "S",
            "interactive": True,
            "cmdline": "/bin/bash",
         },
      ],
      "boot_id": "boot-accept",
      "btime": 100,
      "mem": {
         "MemAvailable": 900000, "Cached": 80000, "Shmem": 5000,
         "MemTotal": 1048576, "SwapTotal": 0, "MemFree": 200000,
         "Buffers": 3000, "SwapFree": 0,
      },
   }


# ---------------------------------------------------------------------------
# Fake wall clock
# ---------------------------------------------------------------------------

_BASE_UTC = "2026-09-30T12:00:00Z"


def _make_advancing_clock(start_utc=_BASE_UTC, step_sec=60):
   """Return a callable that advances by step_sec on each call.

   The CHECK constraint on node_counter_minute/node_usage_intervals requires
   window_end > window_start (and interval_end > interval_start). With a
   static clock, window_start_utc == window_end_utc and the insert is
   rejected. An advancing clock guarantees each flush sees a later timestamp
   than the accumulator's birth instant.
   """
   from datetime import datetime, timezone, timedelta
   _state = [datetime.fromisoformat(start_utc.replace("Z", "+00:00"))]

   def _clock():
      current = _state[0]
      _state[0] = current + timedelta(seconds=step_sec)
      return current.strftime("%Y-%m-%dT%H:%M:%SZ")

   return _clock


def _fake_wall_clock():
   # Legacy static clock kept for simple unit tests that don't touch DB
   return _BASE_UTC


# ---------------------------------------------------------------------------
# Config factory
# ---------------------------------------------------------------------------

def _make_phase0_config(tmp_path, include_failing_node=False):
   """Minimal Phase0Config with accelerated intervals for fast tests.

   counter_interval_sec=1, rollup_interval_sec=3 → 3 counter samples per rollup.
   census_interval_sec=1, usage_interval_sec=3 → 3 census samples per usage window.
   duration_sec=999 → scheduler runs until request_stop() is called.
   """
   run_dir = str(tmp_path / "runs")
   os.makedirs(run_dir, exist_ok=True)
   nodes = [NodeConfig(hostname=_FQDN, role="local")]
   if include_failing_node:
      nodes.append(NodeConfig(hostname="unresolved-node", role="local"))
   return Phase0Config(
      system=_SYSTEM,
      nodes=tuple(nodes),
      output_root=run_dir,
      probe_python=sys.executable,
      counter_interval_sec=1,
      census_interval_sec=1,
      rollup_interval_sec=3,    # 3 counter samples per rollup window
      usage_interval_sec=3,     # 3 census samples per usage window
      duration_sec=999,
      counter_timeout_sec=4,
      census_timeout_sec=20,
      ssh_connect_timeout_sec=8,
      max_parallel_polls=1,
      min_free_disk_pct=1,
      keep_raw_args=True,
      compress_census=False,
   )


class _InjectedDB:
   """Minimal begin() wrapper for DatabaseWriter."""
   def __init__(self, engine):
      self._engine = engine

   def begin(self):
      return self._engine.begin()


def _query_count(engine, table):
   """Query row count from a node_monitor table via fresh NullPool engine."""
   qe = _fresh_query_engine(engine)
   try:
      with qe.connect() as conn:
         return conn.exec_driver_sql(
            "SELECT count(*) FROM node_monitor.%s" % table
         ).scalar_one()
   finally:
      qe.dispose()


# ---------------------------------------------------------------------------
# Core daemon runner helper
#
# Strategy: build a transport that returns payloads until we have enough
# records, then calls daemon._scheduler.request_stop() on the NEXT call
# (which also raises to prevent writing more). This way the daemon
# finalizes cleanly (EXIT_OK) with all target records written.
# ---------------------------------------------------------------------------

def _run_daemon_wired(engine, tmp_path, run_id="acceptance-run", *,
                      counter_payloads=4, census_payloads=4,
                      inject_failure_after=2,
                      include_failing_node=False):
   """Wire real components and run daemon with controlled transport.

   The transport:
   1. Returns hwinfo payload once.
   2. Returns counter payloads up to counter_payloads, then stops.
   3. Returns census payloads up to census_payloads, then stops.
   4. After inject_failure_after counter polls, raises TimeoutError once
      (FQDN must already be established) to trigger node_poll_failures.
   5. Calls daemon._scheduler.request_stop() when both limits are hit.

   Returns (exit_code, diag_sink, daemon).
   """
   config = _make_phase0_config(
      tmp_path, include_failing_node=include_failing_node)
   db = _InjectedDB(engine)
   writer = DatabaseWriter(db)
   diag_sink = Phase0Sink(
      config.output_root, run_id,
      metadata={"system": config.system},
      min_free_disk_pct=config.min_free_disk_pct,
      compress_census=config.compress_census,
      keep_raw_args=config.keep_raw_args,
   )
   pg_sink = PostgresDaemonSink(writer, diag_sink)

   # Mutable state for transport closure
   state = {
      "counter_done": 0,
      "census_done": 0,
      "failure_injected": False,
      "stop_requested": False,
      "daemon": None,
   }

   async def transport(node, loop):
      if node.hostname == "unresolved-node":
         raise TimeoutError("simulated pre-FQDN transport failure")
      if loop == "hwinfo":
         return _hw_payload()

      d = state["daemon"]

      if loop == "counter":
         n = state["counter_done"]
         # Inject a failure once, after FQDN is established
         if (n == inject_failure_after and
               not state["failure_injected"]):
            state["failure_injected"] = True
            raise TimeoutError("simulated counter timeout for poll_failures")
         if n < counter_payloads:
            state["counter_done"] += 1
            return _counter_payload(uptime_sec=100.0 + n)
         # Exhausted; stop daemon then succeed quietly
         if d is not None and not state["stop_requested"]:
            state["stop_requested"] = True
            d._scheduler.request_stop()
         return _counter_payload(uptime_sec=200.0 + n)

      if loop == "census":
         n = state["census_done"]
         if n < census_payloads:
            state["census_done"] += 1
            return _census_payload(
               uptime_sec=100.0 + n, utime_ticks=200 + n * 10)
         if d is not None and not state["stop_requested"]:
            state["stop_requested"] = True
            d._scheduler.request_stop()
         return _census_payload(
            uptime_sec=200.0 + n, utime_ticks=200 + n * 10)

      raise RuntimeError("unexpected loop %r" % loop)

   daemon = Daemon(
      config, pg_sink, transport,
      wall_clock_fn=_make_advancing_clock(),
   )
   state["daemon"] = daemon

   async def _run():
      await pg_sink.start()
      return await daemon.run()

   # Use asyncio.run() for proper loop lifecycle management.
   # asyncio.wait_for provides a hard timeout for test safety.
   exit_code = asyncio.run(asyncio.wait_for(_run(), timeout=30))

   return exit_code, diag_sink, daemon


# ===========================================================================
# Test 1: Schema gate -- uninitialized DB reports not-initialized
# ===========================================================================

def test_schema_gate_rejects_uninitialized_db(disposable_engine):
   """Schema gate must refuse when migration has not been run yet."""
   engine, _ = disposable_engine
   runner = MigrationRunner(engine, "test")
   status = runner.status()
   assert not status.initialized, (
      "freshly created DB must report not initialized; got %r" % vars(status))
   assert status.pending_versions, (
      "uninitialized DB must have pending versions")


# ===========================================================================
# Test 2: Schema gate -- migrated DB proceeds
# ===========================================================================

def test_schema_gate_passes_after_migrate(disposable_engine):
   """After explicit migrate(), status() reports fully current."""
   engine, _ = disposable_engine
   runner = MigrationRunner(engine, "test")
   runner.migrate()
   status = runner.status()
   assert status.initialized
   assert not status.pending_versions
   assert not status.drift
   assert status.current_version == status.latest_version


def test_real_cli_rejects_before_migrate_then_runs_after_operator_migrate(
      disposable_engine, tmp_path):
   """Exercise the operator migration gate through the actual Click CLI."""
   engine, _ = disposable_engine
   config_path, output_root = _write_nested_config(tmp_path, str(engine.url))
   runner = CliRunner()

   before = runner.invoke(
      cli,
      ["daemon", "run", "--config", str(config_path),
       "--home", str(tmp_path), "--duration-sec", "0.2",
       "--run-id", "before-migrate"],
   )
   assert before.exit_code != 0
   assert "database schema not initialized" in before.output
   assert not (output_root / "before-migrate" / "DONE").exists()

   migrate = runner.invoke(
      cli, ["database", "migrate", "--config", str(config_path),
            "--home", str(tmp_path)])
   assert migrate.exit_code == 0, migrate.output
   assert "current_version: 1" in migrate.output

   after = runner.invoke(
      cli,
      ["daemon", "run", "--config", str(config_path),
       "--home", str(tmp_path), "--duration-sec", "0.2",
       "--run-id", "after-migrate"],
   )
   assert after.exit_code == EXIT_OK, after.output
   run_dir_lines = [line for line in after.output.splitlines()
                    if line.startswith("run_dir: ")]
   assert len(run_dir_lines) == 1, after.output
   run_dir = run_dir_lines[0].split(": ", 1)[1]
   assert os.path.exists(os.path.join(run_dir, "DONE"))


# ===========================================================================
# Test 3: All five tables receive rows
# ===========================================================================

def test_all_five_tables_receive_rows(disposable_engine, tmp_path):
   """Run the daemon and verify all five relational tables have rows."""
   engine, _ = disposable_engine
   MigrationRunner(engine, "test").migrate()

   exit_code, diag_sink, daemon = _run_daemon_wired(
      engine, tmp_path, run_id="five-tables",
      counter_payloads=4, census_payloads=4,
      inject_failure_after=2,
      include_failing_node=True,
   )
   assert exit_code == EXIT_OK, "daemon must exit OK; got %d" % exit_code

   hw = _query_count(engine, "node_hardware")
   counter = _query_count(engine, "node_counter_minute")
   usage = _query_count(engine, "node_usage_intervals")
   failures = _query_count(engine, "node_poll_failures")
   logs = _query_count(engine, "node_collection_log")

   assert hw >= 1, "node_hardware must have >= 1 row; got %d" % hw
   assert counter >= 1, "node_counter_minute must have >= 1 row; got %d" % counter
   assert usage >= 1, "node_usage_intervals must have >= 1 row; got %d" % usage
   assert failures >= 1, "node_poll_failures must have >= 1 row; got %d" % failures
   assert logs >= 1, "node_collection_log must have >= 1 row; got %d" % logs


# ===========================================================================
# Test 4: Diagnostic JSONL exists and has records; no relational diag table
# ===========================================================================

def test_diagnostic_jsonl_exists_no_relational_table(disposable_engine, tmp_path):
   """diagnostic_censuses.jsonl must exist; no DB table for it."""
   engine, _ = disposable_engine
   MigrationRunner(engine, "test").migrate()

   exit_code, diag_sink, _ = _run_daemon_wired(
      engine, tmp_path, run_id="diag-test",
      counter_payloads=4, census_payloads=4,
   )
   assert exit_code == EXIT_OK

   # JSONL file exists and has records
   run_dir = diag_sink.run_dir
   census_file = os.path.join(run_dir, "diagnostic_censuses.jsonl")
   assert os.path.exists(census_file), (
      "diagnostic_censuses.jsonl must exist at %s" % census_file)

   with open(census_file) as f:
      lines = [l.strip() for l in f if l.strip()]
   assert len(lines) >= 1, "diagnostic_censuses.jsonl must have >= 1 record"
   for line in lines:
      obj = json.loads(line)
      assert "system" in obj

   # No relational diagnostic table in node_monitor schema
   qe = _fresh_query_engine(engine)
   try:
      with qe.connect() as conn:
         tables = conn.exec_driver_sql("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'node_monitor'
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
         """).scalars().all()
   finally:
      qe.dispose()

   assert "diagnostic_census" not in tables, (
      "diagnostic_census must NOT be a relational table")
   assert "diagnostic_censuses" not in tables


# ===========================================================================
# Test 5: Hardware fact idempotency (ON CONFLICT = one row)
# ===========================================================================

def test_hardware_fact_idempotency_under_retry(disposable_engine):
   """Writing the same hardware record twice must yield one row."""
   engine, _ = disposable_engine
   MigrationRunner(engine, "test").migrate()

   db = _InjectedDB(engine)
   writer = DatabaseWriter(db)
   hw = {
      "system": _SYSTEM,
      "source_hostname": _FQDN,
      "first_seen_utc": "2026-09-30T12:00:00Z",
      "probe_version": _PROBE_VER,
      "boot_id": "boot-a",
      "btime": 100,
      "cpu_model": "Zen",
      "cpu_logical": 64,
      "sockets": 2,
      "cores_per_socket": 16,
      "cpu_max_freq_khz": 3500000,
      "numa_nodes": 4,
      "mem_total_kb": 1048576,
      "swap_total_kb": 0,
      "hugepage_size_kb": 2048,
      "kernel_release": "6.1",
      "os_pretty_name": "Linux",
      "net_fs_mounts": 2,
      "net_ifaces": {"hsn0": {"speed_mbps": 100000}},
      "gpus": [],
   }
   writer.write_record("node_hardware", hw)
   writer.write_record("node_hardware", hw)  # retry

   assert _query_count(engine, "node_hardware") == 1, (
      "hardware retry must produce exactly 1 row via ON CONFLICT")


# ===========================================================================
# Test 6: Counter fact idempotency
# ===========================================================================

def test_counter_fact_idempotency_under_retry(disposable_engine):
   """Writing the same counter record twice must yield one row."""
   engine, _ = disposable_engine
   MigrationRunner(engine, "test").migrate()

   from tests.test_database_writer import _counter as _make_counter
   db = _InjectedDB(engine)
   writer = DatabaseWriter(db)
   cr = _make_counter()
   writer.write_record("node_counter_samples", cr)
   writer.write_record("node_counter_samples", cr)  # retry

   assert _query_count(engine, "node_counter_minute") == 1, (
      "counter retry must produce exactly 1 row via ON CONFLICT")


# ===========================================================================
# Test 7: Usage interval idempotency
# ===========================================================================

def test_usage_interval_idempotency_under_retry(disposable_engine):
   """Writing the same usage record twice must yield one row."""
   engine, _ = disposable_engine
   MigrationRunner(engine, "test").migrate()

   from tests.test_database_writer import _usage as _make_usage
   db = _InjectedDB(engine)
   writer = DatabaseWriter(db)
   ur = _make_usage("alice")
   writer.write_record("node_usage_intervals", ur)
   writer.write_record("node_usage_intervals", ur)  # retry

   assert _query_count(engine, "node_usage_intervals") == 1, (
      "usage retry must produce exactly 1 row via ON CONFLICT")


# ===========================================================================
# Test 8: Poll failure is append-only (two inserts = two rows)
# ===========================================================================

def test_poll_failure_is_append_not_upsert(disposable_engine):
   """Poll failures are append-only: two inserts yield two rows."""
   engine, _ = disposable_engine
   MigrationRunner(engine, "test").migrate()

   db = _InjectedDB(engine)
   writer = DatabaseWriter(db)
   pf = {
      "system": _SYSTEM,
      "source_hostname": _FQDN,
      "loop": "counter",
      "timestamp_utc": "2026-09-30T12:00:00Z",
      "failure_type": "timeout",
      "detail": "ordinary poll failure; see failure_type",
      "consecutive_failures": 1,
      "breaker_state": "closed",
   }
   writer.write_record("node_poll_failures", pf)
   writer.write_record("node_poll_failures", pf)

   assert _query_count(engine, "node_poll_failures") == 2, (
      "poll_failures must append; got wrong count")


# ===========================================================================
# Test 9: Collection log is append-only
# ===========================================================================

def test_collection_log_is_append_not_upsert(disposable_engine):
   """Collection log is append-only: two identical inserts yield two rows."""
   engine, _ = disposable_engine
   MigrationRunner(engine, "test").migrate()

   db = _InjectedDB(engine)
   writer = DatabaseWriter(db)
   cl = {
      "system": _SYSTEM,
      "timestamp_utc": "2026-09-30T12:00:00Z",
      "event": "daemon_started",
      "detail": {"version": "0.1.0"},
   }
   writer.write_record("node_collection_log", cl)
   writer.write_record("node_collection_log", cl)

   assert _query_count(engine, "node_collection_log") == 2, (
      "collection_log must append; got wrong count")


# ===========================================================================
# Test 10: Migration idempotency
# ===========================================================================

def test_migration_replay_is_noop(disposable_engine):
   """Running migrate() twice must apply zero new migrations on the second call."""
   engine, _ = disposable_engine
   runner = MigrationRunner(engine, "test")
   first = runner.migrate()
   second = runner.migrate()
   assert second.applied_versions == (), (
      "second migrate() must apply 0 versions; got %r" % (second.applied_versions,))
   assert first.applied_versions, "first migrate() must apply at least one version"


# ===========================================================================
# Test 11: Database outage -- clean failure, no driver detail in error
# ===========================================================================

def test_invalid_database_url_fails_with_bounded_error(tmp_path):
   """An unresolvable database URL must produce a bounded DatabaseWriteError."""
   from node_monitor.database.writer import DatabaseWriteError

   bad_engine = create_engine(
      "postgresql://baduser@127.0.0.1:19999/nonexistent_db_xyzzy",
      poolclass=NullPool,
      connect_args={"connect_timeout": 1},
   )

   db = _InjectedDB(bad_engine)
   writer = DatabaseWriter(db)
   hw = {
      "system": _SYSTEM,
      "source_hostname": _FQDN,
      "first_seen_utc": "2026-09-30T12:00:00Z",
      "probe_version": _PROBE_VER,
      "boot_id": None, "btime": None, "cpu_model": None,
      "cpu_logical": None, "sockets": None, "cores_per_socket": None,
      "cpu_max_freq_khz": None, "numa_nodes": None, "mem_total_kb": None,
      "swap_total_kb": None, "hugepage_size_kb": None, "kernel_release": None,
      "os_pretty_name": None, "net_fs_mounts": None, "net_ifaces": None,
      "gpus": None,
   }

   with pytest.raises(DatabaseWriteError) as exc_info:
      writer.write_record("node_hardware", hw)

   err = exc_info.value
   # Bounded: no raw driver text, no URL fragment, no credential
   assert err.__cause__ is None, "DatabaseWriteError must not chain driver cause"
   msg = str(err)
   assert "baduser" not in msg, "error must not contain credential"
   assert "19999" not in msg, "error must not contain port"
   assert "127.0.0.1" not in msg, "error must not contain host"
   assert "nonexistent" not in msg, "error must not contain db name"


def test_real_cli_outage_is_sanitized_nonzero_and_writes_no_done(tmp_path):
   """The real daemon CLI must fail closed when PostgreSQL is unreachable."""
   bad_url = "postgresql://db_user@127.0.0.1:19999/outage_sentinel"
   config_path, output_root = _write_nested_config(tmp_path, bad_url)
   result = CliRunner().invoke(
      cli,
      ["daemon", "run", "--config", str(config_path),
       "--home", str(tmp_path), "--duration-sec", "0.2",
       "--run-id", "outage"],
   )

   assert result.exit_code != 0
   assert "database migration error" in result.output
   assert "db_user" not in result.output
   assert "127.0.0.1" not in result.output
   assert "19999" not in result.output
   assert "outage_sentinel" not in result.output
   assert "Traceback" not in result.output
   assert not (output_root / "outage" / "DONE").exists()


# ===========================================================================
# Test 12: Daemon never constructs MigrationRunner (no DDL from daemon path)
# ===========================================================================

def test_daemon_source_never_references_migration_runner():
   """daemon.py must not import or reference MigrationRunner."""
   import node_monitor.daemon as daemon_module
   source = open(daemon_module.__file__).read()
   assert "MigrationRunner" not in source, (
      "daemon.py must never reference MigrationRunner; "
      "migration is an operator-only CLI concern")
   assert "database.migration" not in source


# ===========================================================================
# Test 13: Full end-to-end content verification
# ===========================================================================

def test_end_to_end_content_verification(disposable_engine, tmp_path):
   """Full wiring: verify row content in each table, DONE flag, JSONL."""
   engine, _ = disposable_engine
   MigrationRunner(engine, "test").migrate()

   exit_code, diag_sink, daemon = _run_daemon_wired(
      engine, tmp_path, run_id="e2e-content",
      counter_payloads=4, census_payloads=4,
      inject_failure_after=2,
      include_failing_node=True,
   )
   assert exit_code == EXIT_OK, "daemon must exit OK; got %d" % exit_code

   # Verify DONE file
   done_file = os.path.join(diag_sink.run_dir, "DONE")
   assert os.path.exists(done_file), "DONE flag must exist after clean run"

   # Verify hardware row content via fresh NullPool engine
   qe = _fresh_query_engine(engine)
   try:
      with qe.connect() as conn:
         hw_rows = conn.exec_driver_sql(
            "SELECT system, source_hostname, boot_id, probe_version "
            "FROM node_monitor.node_hardware"
         ).mappings().all()
         hw_rows = [dict(r) for r in hw_rows]

         counter_rows = conn.exec_driver_sql(
            "SELECT system, source_hostname, sample_count "
            "FROM node_monitor.node_counter_minute"
         ).mappings().all()
         counter_rows = [dict(r) for r in counter_rows]

         usage_rows = conn.exec_driver_sql(
            "SELECT system, source_hostname, username "
            "FROM node_monitor.node_usage_intervals"
         ).mappings().all()
         usage_rows = [dict(r) for r in usage_rows]

         failure_rows = conn.exec_driver_sql(
            "SELECT system, source_hostname, failure_type "
            "FROM node_monitor.node_poll_failures"
         ).mappings().all()
         failure_rows = [dict(r) for r in failure_rows]

   finally:
      qe.dispose()

   # Hardware
   assert len(hw_rows) >= 1
   assert hw_rows[0]["system"] == _SYSTEM
   assert hw_rows[0]["source_hostname"] == _FQDN
   assert hw_rows[0]["boot_id"] == "boot-accept"
   assert hw_rows[0]["probe_version"] == _PROBE_VER

   # Counter
   assert len(counter_rows) >= 1
   assert counter_rows[0]["system"] == _SYSTEM
   assert counter_rows[0]["source_hostname"] == _FQDN

   # Usage intervals
   assert len(usage_rows) >= 1
   assert usage_rows[0]["system"] == _SYSTEM
   assert usage_rows[0]["source_hostname"] == _FQDN

   # Poll failures
   assert len(failure_rows) >= 1
   assert failure_rows[0]["system"] == _SYSTEM
   assert failure_rows[0]["source_hostname"] == _FQDN
   assert failure_rows[0]["failure_type"] in (
      "timeout", "invariant_violation", "probe_exit", "ssh_transport",
      "ssh_auth", "malformed_json", "probe_version_mismatch",
      "hostname_mismatch", "scheduler_miss",
   )

   # JSONL
   census_file = os.path.join(diag_sink.run_dir, "diagnostic_censuses.jsonl")
   assert os.path.exists(census_file)
   with open(census_file) as f:
      lines = [l.strip() for l in f if l.strip()]
   assert len(lines) >= 1


# ===========================================================================
# Test 16: Catalog cleanup
# ===========================================================================

def test_catalog_has_no_leftover_test_databases():
   """After the test session, no node_monitor_test_% databases remain from
   prior sessions. Current-run fixtures clean up in their finally blocks."""
   admin_url = make_url(os.environ["NODE_MONITOR_TEST_DATABASE_URL"])
   admin_engine = create_engine(
      admin_url, poolclass=NullPool, isolation_level="AUTOCOMMIT")
   try:
      with admin_engine.connect() as conn:
         prefix = "node_monitor_test_"
         rows = conn.exec_driver_sql(
            "SELECT datname FROM pg_database WHERE datname LIKE %s || '%%'",
            (prefix,),
         ).scalars().all()
      assert rows == [], (
         "node_monitor_test_* databases remain after fixture cleanup: %r"
         % list(rows))
   finally:
      admin_engine.dispose()
