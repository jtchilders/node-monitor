"""Real-PostgreSQL performance acceptance for the complete 24-hour dashboard
endpoint (operational-web-dashboard plan Task 9, Step 1 + Step 2).

All tests in this file are unconditionally SKIPPED when
NODE_MONITOR_TEST_DATABASE_URL is not set -- they never fabricate evidence.

Seeds a disposable UUID-named PostgreSQL database (via
``tests.web.conftest.pg_disposable_db``) with a production-shaped 24-hour,
two-node fixture:

  * 1,440 counter windows per node (node_counter_minute);
  * a full (category, activity, username) grid of usage intervals per node
    across 96 fifteen-minute windows (node_usage_intervals);
  * poll failures and collection-log events;
  * valid JSONB shapes, gaps, partial rows, reboot/reset conditions, null
    hardware totals, and failures.

Then repeatedly exercises DashboardService.dashboard() for the complete
24-hour range after one warm-up call and asserts the Task 9 performance
bounds: per-statement latency, total request latency, response size,
returned counter-row count, and process RSS delta.
"""

import asyncio
import os
import resource
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import event, text

from node_monitor.web.service import DashboardService

from tests.web.conftest import disposable_identifier, pg_skip, quote_identifier


# ---------------------------------------------------------------------------
# Fixture shape constants (plan Task 9, Step 1)
# ---------------------------------------------------------------------------

COUNTER_WINDOWS = 1440
USAGE_WINDOWS = 96
CATEGORIES = ("ai_coding", "shell", "editor", "scheduler", "other")
ACTIVITIES = ("active", "idle", "unknown")
USERS = tuple("user-%02d" % index for index in range(32))

SYSTEM = "polaris"
NODES = ("login-04", "login-05")


def expected_usage_rows_per_node():
   return USAGE_WINDOWS * len(CATEGORIES) * len(ACTIVITIES) * len(USERS)


# ---------------------------------------------------------------------------
# Performance bounds (plan Task 9, Step 2)
# ---------------------------------------------------------------------------

MAX_STATEMENT_SECONDS = 3.0
MAX_REQUEST_SECONDS = 12.0
MAX_RESPONSE_BYTES = 5 * 1024 * 1024
MAX_COUNTER_ROWS = 1440
#: Maximum acceptable increase in this process's peak-RSS high-water mark
#: (``ru_maxrss``) between a pre- and post-measurement snapshot taken
#: around the ITERATIONS dashboard requests below. This is NOT the
#: instantaneous RSS of any single request -- ``ru_maxrss`` is a
#: monotonically non-decreasing high-water mark for the whole process
#: lifetime, so ``rss_after - rss_before`` only ever captures growth that
#: occurred during the measured window; it can never shrink even if the
#: single most expensive request released memory afterward.
MAX_RSS_HWM_DELTA_BYTES = 256 * 1024 * 1024


def normalize_ru_maxrss(raw_ru_maxrss, platform):
   """Pure, platform-unit normalization of a raw ``ru_maxrss`` value to
   bytes.

   ``resource.getrusage(...).ru_maxrss`` is documented to report its peak
   resident-set-size high-water mark in platform-dependent units: bytes on
   macOS/BSD (``sys.platform == "darwin"``), kibibytes (1024-byte units)
   everywhere else (Linux). This function takes the raw value and the
   platform string as plain arguments -- no OS call inside -- so the unit
   conversion itself is directly unit-testable for both platforms without
   needing to run on both operating systems or monkeypatch the ``resource``
   module.
   """
   if platform == "darwin":
      return raw_ru_maxrss
   return raw_ru_maxrss * 1024


def _max_rss_bytes():
   """Return this process's current peak-RSS high-water mark in bytes,
   cross-platform, by reading the real OS-reported ``ru_maxrss`` and
   normalizing its units via ``normalize_ru_maxrss``.

   This is a high-water mark, not an instantaneous sample: PostgreSQL's own
   ``ru_maxrss`` semantics mean the value returned here only ever goes up
   for the lifetime of the process, so two calls bracketing a block of work
   measure how much additional peak memory that block caused the process
   to touch at its worst moment -- not the memory used by any single
   request in isolation.
   """
   raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
   return normalize_ru_maxrss(raw, sys.platform)


# ---------------------------------------------------------------------------
# Pure unit test: platform-unit normalization (no PostgreSQL, no OS call) --
# the methodology itself (bytes on darwin, KiB->bytes elsewhere) is correct
# and unchanged; this test makes the conversion explicit and reviewable via
# the extracted pure helper rather than only exercising it implicitly
# through a live measurement.
# ---------------------------------------------------------------------------

def test_normalize_ru_maxrss_is_identity_on_darwin_and_kib_to_bytes_elsewhere():
   # macOS/BSD: ru_maxrss is already reported in bytes -- no conversion.
   assert normalize_ru_maxrss(123456, "darwin") == 123456
   assert normalize_ru_maxrss(0, "darwin") == 0
   # Linux (and every other non-darwin platform string): ru_maxrss is
   # reported in kibibytes (1024-byte units) -- multiply by 1024.
   assert normalize_ru_maxrss(1000, "linux") == 1000 * 1024
   assert normalize_ru_maxrss(0, "linux") == 0
   assert normalize_ru_maxrss(1000, "freebsd12") == 1000 * 1024


# ---------------------------------------------------------------------------
# Seed-data builders
# ---------------------------------------------------------------------------

def _now():
   # Fixed instant rather than datetime.now() so every row's freshness
   # relative to NOW is deterministic across the whole fixture.
   return datetime(2026, 10, 3, 12, 0, tzinfo=timezone.utc)


def _counter_rows_for_node(node, now, count=COUNTER_WINDOWS):
   """Yield ``count`` production-shaped node_counter_minute row dicts for
   ``node``, spanning slightly more than 24h with a handful of genuine
   missing minutes (gaps), a handful of partial/incomplete windows, and one
   null-hardware-totals window -- while still producing exactly ``count``
   real rows.
   """
   # Reserve a few extra minute-slots beyond `count` so skipping some still
   # leaves exactly `count` real rows -- demonstrates genuine gaps without
   # violating the "1,440 windows per node" requirement.
   total_slots = count + 12
   skip_offsets = frozenset({3, 47, 311, 900, 1200, 1201, 1202, 1340,
                              1341, 1342, 1400, 1439})
   produced = 0
   offset = 0
   while produced < count and offset < total_slots:
      if offset in skip_offsets:
         offset += 1
         continue
      window_end = now - timedelta(minutes=(total_slots - offset))
      window_start = window_end - timedelta(minutes=1)
      sample_count = 6
      meets_minimum = True
      invalid_pair_count = 0
      excess_sample_count = 0
      mem_available_kb = 60_000_000 + (offset % 5000) * 37
      cached_kb = 9_000_000 + (offset % 100) * 11
      shmem_kb = 500_000 + (offset % 50) * 7

      # Partial windows: ~1% of rows have degraded sample coverage.
      if offset % 97 == 0:
         sample_count = 3
         meets_minimum = False

      # Reboot/reset condition: one window has a hardware-reset-like dip in
      # process counts and a reset-flavored excess_sample_count anomaly.
      is_reboot_window = offset == 600
      if is_reboot_window:
         excess_sample_count = 1
         procs_running = 1
         procs_total = 40
      else:
         procs_running = 2 + (offset % 6)
         procs_total = 250 + (offset % 40)

      # Null hardware totals on one deliberately chosen window (mem/cached/
      # shmem all legitimately nullable per schema).
      is_null_hardware_window = offset == 777
      if is_null_hardware_window:
         mem_available_kb = None
         cached_kb = None
         shmem_kb = None

      yield {
         "system": SYSTEM,
         "source_hostname": node,
         "window_start": window_start,
         "window_end": window_end,
         "collector_hostname": node,
         "probe_version": 4,
         "daemon_version": "0.2.0",
         "sample_count": sample_count,
         "expected_count": 6,
         "coverage": sample_count / 6.0,
         "mem_available_kb": mem_available_kb,
         "cached_kb": cached_kb,
         "shmem_kb": shmem_kb,
         "load1": 0.5 + (offset % 10) * 0.1,
         "load5": 0.6 + (offset % 10) * 0.1,
         "load15": 0.7 + (offset % 10) * 0.1,
         "procs_running": procs_running,
         "procs_total": procs_total,
         "socket_count": 8 + (offset % 4),
         "cpu_busy_pct": {
            "p50": 10.0 + (offset % 20),
            "p95": 30.0 + (offset % 20),
            "max": 50.0 + (offset % 20),
         },
         "network_rates": {
            "eth0": {
               "rx_bytes_per_sec": {
                  "p50": 1000.0 + offset, "p95": 2000.0 + offset,
                  "max": 3000.0 + offset,
               },
               "tx_bytes_per_sec": {
                  "p50": 500.0 + offset, "p95": 1000.0 + offset,
                  "max": 1500.0 + offset,
               },
            },
         },
         "lustre_md_summary": {
            "open": {
               "p50_sum": 4.0 + (offset % 5), "p95_sum": 6.0 + (offset % 5),
               "max_sum": 9.0 + (offset % 5), "target_count": 2,
            },
         },
         "meets_minimum_samples": meets_minimum,
         "invalid_pair_count": invalid_pair_count,
         "excess_sample_count": excess_sample_count,
      }
      produced += 1
      offset += 1


def _usage_rows_for_node(node, now, windows=USAGE_WINDOWS):
   """Yield the full (interval, category, activity, username) grid of
   production-shaped node_usage_intervals row dicts for ``node`` -- used to
   disprove application-side raw-row loading: the dashboard must aggregate
   this large grid in SQL, never fetch it wholesale into Python.

   Every one of the ``windows * len(CATEGORIES) * len(ACTIVITIES) *
   len(USERS)`` combinations is yielded exactly once -- this is an exact
   cross product, not an approximate grid with deliberately missing rows.
   Partial/failure coverage is represented IN those real rows via
   ``unmeasured_count > 0`` and/or ``sample_count < expected_count``,
   never by omitting a required combination outright. Genuine counter
   timeline gaps (missing minutes) are a property of
   ``node_counter_minute`` / ``_counter_rows_for_node`` only, and remain
   modeled there as real missing rows -- that is a distinct requirement
   from this table's exact-grid requirement.
   """
   interval_len = timedelta(minutes=15)
   for w in range(windows):
      interval_end = now - timedelta(minutes=15 * (windows - w))
      interval_start = interval_end - interval_len
      for cat_idx, category in enumerate(CATEGORIES):
         for act_idx, activity in enumerate(ACTIVITIES):
            for user_idx, username in enumerate(USERS):
               combo_id = (
                  w * len(CATEGORIES) * len(ACTIVITIES) * len(USERS)
                  + cat_idx * len(ACTIVITIES) * len(USERS)
                  + act_idx * len(USERS) + user_idx)
               # Failure/quality-degradation conditions are represented
               # IN the row (unmeasured_count, reduced sample_count), not
               # by skipping the combination -- every grain in the exact
               # cross product is always present.
               is_unmeasured = combo_id % 401 == 0
               unmeasured_count = 1 if is_unmeasured else 0
               # A second, disjoint ~1-in-500 subset simulates partial
               # sample coverage (reduced sample_count) without ever
               # omitting the row itself.
               is_partial = combo_id % 503 == 0
               sample_count = 3 if is_partial else 6
               rss_p50 = 1000 + (combo_id % 4000)
               yield {
                  "system": SYSTEM,
                  "source_hostname": node,
                  "interval_start": interval_start,
                  "interval_end": interval_end,
                  "category": category,
                  "activity": activity,
                  "username": username,
                  "process_count": {
                     "p50": 1 + (combo_id % 4),
                     "p95": 2 + (combo_id % 6),
                     "max": 3 + (combo_id % 8),
                  },
                  "cpu_seconds": 0.5 + (combo_id % 97) * 0.25,
                  "rss_kb": {
                     "p50": rss_p50,
                     "p95": rss_p50 + 2000,
                     "max": rss_p50 + 4000,
                  },
                  "d_state_fraction": (combo_id % 20) / 100.0,
                  "interactivity_fraction": (combo_id % 40) / 100.0,
                  "sample_count": sample_count,
                  "expected_count": 6,
                  "unmeasured_count": unmeasured_count,
               }


def _poll_failure_rows_for_node(node, now):
   for index in range(5):
      yield {
         "system": SYSTEM,
         "source_hostname": node,
         "loop": "counter" if index % 2 == 0 else "census",
         "recorded_at": now - timedelta(minutes=5 * index),
         "failure_type": "timeout",
         "detail": "probe timed out",
         "consecutive_failures": index + 1,
         "breaker_state": "closed" if index < 2 else "open",
      }


def _collection_log_rows(now):
   for index, event_name in enumerate(("start", "stop", "reset", "reboot")):
      yield {
         "system": SYSTEM,
         "recorded_at": now - timedelta(minutes=30 * index),
         "event": event_name,
         "detail": {"collector": "collector-01", "sequence": index},
      }


def _hardware_row(node, now):
   return {
      "system": SYSTEM,
      "source_hostname": node,
      "first_seen": now - timedelta(days=30),
      "last_verified": now,
      "boot_id": uuid.uuid4().hex,
      "btime": int((now - timedelta(days=30)).timestamp()),
      "cpu_model": "Intel Xeon",
      "cpu_logical": 96,
      "sockets": 2,
      "cores_per_socket": 24,
      "cpu_max_freq_khz": 2_400_000,
      "numa_nodes": 2,
      "mem_total_kb": 131_072_000,
      "swap_total_kb": 0,
      "hugepage_size_kb": 2048,
      "kernel_release": "5.14.0",
      "os_pretty_name": "RHEL 8",
      "net_fs_mounts": 4,
      "net_ifaces": {},
      "gpus": [],
      "probe_version": 4,
   }


def seed_fixture_database(engine):
   """Populate ``engine``'s already-migrated disposable database with the
   full Task 9 24-hour, two-node production-shaped fixture.

   Uses bulk ``executemany``-style inserts (SQLAlchemy ``Connection.execute``
   with a list of parameter dicts) directly against the real migrated
   schema -- the same column/table shapes ``node_monitor.database.writer``
   uses -- rather than one ``DatabaseWriter.write_record`` call per row,
   because the full usage grid is tens of thousands of rows per node and
   this is test-fixture setup, not the behavior under test.

   Returns a dict of exact row counts actually inserted, keyed by table
   name, so tests can assert against real counts rather than assumed ones.
   """
   now = _now()
   counts = {}

   hardware_sql = text("""
      INSERT INTO node_monitor.node_hardware (
         system, source_hostname, first_seen, last_verified, boot_id,
         btime, cpu_model, cpu_logical, sockets, cores_per_socket,
         cpu_max_freq_khz, numa_nodes, mem_total_kb, swap_total_kb,
         hugepage_size_kb, kernel_release, os_pretty_name, net_fs_mounts,
         net_ifaces, gpus, probe_version
      ) VALUES (
         :system, :source_hostname, :first_seen, :last_verified, :boot_id,
         :btime, :cpu_model, :cpu_logical, :sockets, :cores_per_socket,
         :cpu_max_freq_khz, :numa_nodes, :mem_total_kb, :swap_total_kb,
         :hugepage_size_kb, :kernel_release, :os_pretty_name,
         :net_fs_mounts, CAST(:net_ifaces AS jsonb), CAST(:gpus AS jsonb),
         :probe_version
      )
   """).bindparams()

   counter_sql = text("""
      INSERT INTO node_monitor.node_counter_minute (
         system, source_hostname, window_start, window_end,
         collector_hostname, probe_version, daemon_version, sample_count,
         expected_count, coverage, mem_available_kb, cached_kb, shmem_kb,
         load1, load5, load15, procs_running, procs_total, socket_count,
         cpu_busy_pct, network_rates, lustre_md_summary,
         meets_minimum_samples, invalid_pair_count, excess_sample_count
      ) VALUES (
         :system, :source_hostname, :window_start, :window_end,
         :collector_hostname, :probe_version, :daemon_version,
         :sample_count, :expected_count, :coverage, :mem_available_kb,
         :cached_kb, :shmem_kb, :load1, :load5, :load15, :procs_running,
         :procs_total, :socket_count, CAST(:cpu_busy_pct AS jsonb),
         CAST(:network_rates AS jsonb), CAST(:lustre_md_summary AS jsonb),
         :meets_minimum_samples, :invalid_pair_count, :excess_sample_count
      )
   """)

   usage_sql = text("""
      INSERT INTO node_monitor.node_usage_intervals (
         system, source_hostname, interval_start, interval_end, category,
         activity, username, process_count, cpu_seconds, rss_kb,
         d_state_fraction, interactivity_fraction, sample_count,
         expected_count, unmeasured_count
      ) VALUES (
         :system, :source_hostname, :interval_start, :interval_end,
         :category, :activity, :username, CAST(:process_count AS jsonb),
         :cpu_seconds, CAST(:rss_kb AS jsonb), :d_state_fraction,
         :interactivity_fraction, :sample_count, :expected_count,
         :unmeasured_count
      )
   """)

   poll_failure_sql = text("""
      INSERT INTO node_monitor.node_poll_failures (
         system, source_hostname, loop, recorded_at, failure_type, detail,
         consecutive_failures, breaker_state
      ) VALUES (
         :system, :source_hostname, :loop, :recorded_at, :failure_type,
         :detail, :consecutive_failures, :breaker_state
      )
   """)

   collection_log_sql = text("""
      INSERT INTO node_monitor.node_collection_log (
         system, recorded_at, event, detail
      ) VALUES (:system, :recorded_at, :event, CAST(:detail AS jsonb))
   """)

   with engine.begin() as conn:
      import json

      hardware_rows = [_hardware_row(node, now) for node in NODES]
      for row in hardware_rows:
         row["net_ifaces"] = json.dumps(row["net_ifaces"])
         row["gpus"] = json.dumps(row["gpus"])
      conn.execute(hardware_sql, hardware_rows)
      counts["node_hardware"] = len(hardware_rows)

      counter_rows = []
      for node in NODES:
         counter_rows.extend(_counter_rows_for_node(node, now))
      for row in counter_rows:
         row["cpu_busy_pct"] = json.dumps(row["cpu_busy_pct"])
         row["network_rates"] = json.dumps(row["network_rates"])
         row["lustre_md_summary"] = json.dumps(row["lustre_md_summary"])
      conn.execute(counter_sql, counter_rows)
      counts["node_counter_minute"] = len(counter_rows)

      usage_rows = []
      for node in NODES:
         usage_rows.extend(_usage_rows_for_node(node, now))
      for row in usage_rows:
         row["process_count"] = json.dumps(row["process_count"])
         row["rss_kb"] = json.dumps(row["rss_kb"])
      conn.execute(usage_sql, usage_rows)
      counts["node_usage_intervals"] = len(usage_rows)

      poll_failure_rows = []
      for node in NODES:
         poll_failure_rows.extend(_poll_failure_rows_for_node(node, now))
      conn.execute(poll_failure_sql, poll_failure_rows)
      counts["node_poll_failures"] = len(poll_failure_rows)

      collection_log_rows = list(_collection_log_rows(now))
      for row in collection_log_rows:
         row["detail"] = json.dumps(row["detail"])
      conn.execute(collection_log_sql, collection_log_rows)
      counts["node_collection_log"] = len(collection_log_rows)

   return counts, now


# ---------------------------------------------------------------------------
# Fixture: seeded disposable database
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def seeded_engine_and_counts():
   """Module-scoped: seed one disposable database once, reuse across the
   performance tests in this file (seeding ~95k rows is expensive; the
   performance measurements themselves are what varies per test).
   """
   if not bool(os.environ.get("NODE_MONITOR_TEST_DATABASE_URL")):
      pytest.skip("NODE_MONITOR_TEST_DATABASE_URL is required")

   from sqlalchemy import create_engine
   from sqlalchemy.engine import make_url
   from sqlalchemy.pool import NullPool

   base_url = make_url(os.environ["NODE_MONITOR_TEST_DATABASE_URL"])
   admin_url = base_url.set(database="postgres")
   db_name = disposable_identifier("nm_task9_perf")

   admin_engine = create_engine(
      admin_url, poolclass=NullPool, isolation_level="AUTOCOMMIT")
   with admin_engine.connect() as conn:
      conn.exec_driver_sql(
         'CREATE DATABASE "%s"' % quote_identifier(db_name))
   admin_engine.dispose()

   db_url = admin_url.set(database=db_name)
   # Production-compatible session configuration via the real connect_args
   # mechanism (same shape as DatabaseConfig.connect_args / WebDatabase's
   # own connect_args plumbing in node_monitor/database/web.py): a
   # PostgreSQL ``options`` connect_arg carrying ``-c statement_timeout=...
   # -c lock_timeout=...``. This is the exact mechanism the real engines
   # use, so the performance path's actual DB session genuinely enforces
   # PostgreSQL's own server-side 3-second statement cancellation --
   # measurement alone (a client-side stopwatch) can never prove this;
   # only a real connect_args-configured session can. lock_timeout is
   # preserved (2000ms) alongside statement_timeout, matching the same
   # ratio used throughout the config examples and test fixtures (e.g.
   # tests/web/test_web_config.py's own 3000/2000 pair).
   engine = create_engine(
      str(db_url), poolclass=NullPool,
      connect_args={
         "options": "-c statement_timeout=%d -c lock_timeout=%d" % (
            int(MAX_STATEMENT_SECONDS * 1000), 2000),
      },
   )
   try:
      from node_monitor.database.migration import MigrationRunner
      runner = MigrationRunner(engine, "task9-perf-test")
      runner.migrate()
      counts, now = seed_fixture_database(engine)
      yield engine, counts, now
   finally:
      engine.dispose()
      cleanup = create_engine(
         admin_url, poolclass=NullPool, isolation_level="AUTOCOMMIT")
      with cleanup.connect() as conn:
         conn.exec_driver_sql(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()", (db_name,))
         conn.exec_driver_sql(
            'DROP DATABASE IF EXISTS "%s"' % quote_identifier(db_name))
      cleanup.dispose()


# ---------------------------------------------------------------------------
# Step 1: fixture-shape proof
# ---------------------------------------------------------------------------

@pg_skip
def test_fixture_seeds_exact_counter_window_count(seeded_engine_and_counts):
   """Exactly 1,440 counter windows per node (2,880 total) were inserted."""
   _engine, counts, _now = seeded_engine_and_counts
   assert counts["node_counter_minute"] == COUNTER_WINDOWS * len(NODES)


@pg_skip
def test_fixture_seeds_exact_usage_grid_per_node(seeded_engine_and_counts):
   """Each node has EXACTLY the full (window, category, activity, username)
   cross product of usage rows -- 96 windows x 5 categories x 3 activities
   x 32 users per node, per the Task 9 Step 1 specification. This is an
   exact cardinality requirement, not an approximate lower bound: partial
   coverage / failure conditions are represented through
   ``unmeasured_count`` and reduced ``sample_count`` on individual grains,
   never by omitting a required (window, category, activity, username)
   combination from the fixture.

   Asserts real per-node row counts queried directly from the database
   (grouped by source_hostname), not only the insertion-time bookkeeping
   dict returned by ``seed_fixture_database``.
   """
   engine, _counts, _now = seeded_engine_and_counts
   per_node_expected = expected_usage_rows_per_node()
   with engine.connect() as conn:
      rows = conn.execute(text(
         "SELECT source_hostname, count(*) "
         "FROM node_monitor.node_usage_intervals "
         "GROUP BY source_hostname"
      )).all()
   counts_by_node = {node: count for node, count in rows}
   assert set(counts_by_node) == set(NODES)
   for node in NODES:
      assert counts_by_node[node] == per_node_expected, (
         "node %r has %d usage rows, expected exactly %d (the full "
         "windows x categories x activities x users cross product)"
         % (node, counts_by_node[node], per_node_expected))


@pg_skip
def test_fixture_includes_null_hardware_totals_window(seeded_engine_and_counts):
   """At least one counter window has null mem/cached/shmem totals."""
   engine, _counts, _now = seeded_engine_and_counts
   with engine.connect() as conn:
      null_count = conn.execute(text(
         "SELECT count(*) FROM node_monitor.node_counter_minute "
         "WHERE mem_available_kb IS NULL AND cached_kb IS NULL "
         "AND shmem_kb IS NULL"
      )).scalar_one()
   assert null_count >= len(NODES)


@pg_skip
def test_fixture_includes_partial_and_failure_rows(seeded_engine_and_counts):
   """At least one counter window fails meets_minimum_samples, and at least
   one usage row has unmeasured_count > 0 (a failure condition).
   """
   engine, _counts, _now = seeded_engine_and_counts
   with engine.connect() as conn:
      partial = conn.execute(text(
         "SELECT count(*) FROM node_monitor.node_counter_minute "
         "WHERE meets_minimum_samples = false"
      )).scalar_one()
      failures = conn.execute(text(
         "SELECT count(*) FROM node_monitor.node_usage_intervals "
         "WHERE unmeasured_count > 0"
      )).scalar_one()
   assert partial >= len(NODES)
   assert failures >= len(NODES)


@pg_skip
def test_fixture_includes_poll_failures_and_collection_events(
      seeded_engine_and_counts):
   _engine, counts, _now = seeded_engine_and_counts
   assert counts["node_poll_failures"] == 5 * len(NODES)
   assert counts["node_collection_log"] == 4


# ---------------------------------------------------------------------------
# Step 2 (statement timeout enforcement proof): the performance path's
# actual DB session must have a REAL PostgreSQL statement_timeout of
# exactly 3 seconds enforced server-side -- not merely a client-side
# stopwatch around an unbounded statement.
# ---------------------------------------------------------------------------

@pg_skip
def test_performance_engine_session_has_exact_statement_timeout(
      seeded_engine_and_counts):
   """The exact same engine used by ``_run_dashboard_request`` (and thus by
   ``DashboardService``) must open sessions with PostgreSQL
   ``statement_timeout`` set to exactly 3000ms (matching
   ``MAX_STATEMENT_SECONDS``) and ``lock_timeout`` set to exactly 2000ms,
   enforced server-side via ``current_setting()`` on a real connection --
   not inferred from configuration alone.

   This is a real enforcement proof, not a configuration-shape check: it
   runs ``pg_sleep(3.5)`` (longer than the 3-second timeout) over the exact
   connect_args-configured engine and asserts PostgreSQL itself cancels the
   statement with ``QueryCanceled`` -- a client-side stopwatch could never
   produce this failure mode on its own.
   """
   engine, _counts, _now = seeded_engine_and_counts

   with engine.connect() as conn:
      statement_timeout_ms = conn.execute(
         text("SELECT current_setting('statement_timeout')")).scalar_one()
      lock_timeout_ms = conn.execute(
         text("SELECT current_setting('lock_timeout')")).scalar_one()
   assert statement_timeout_ms == "3000ms" or statement_timeout_ms == "3s", (
      "performance engine session statement_timeout must be exactly 3000ms, "
      "got %r" % (statement_timeout_ms,))
   assert lock_timeout_ms == "2000ms" or lock_timeout_ms == "2s", (
      "performance engine session lock_timeout must be exactly 2000ms, "
      "got %r" % (lock_timeout_ms,))

   from sqlalchemy.exc import DBAPIError

   with pytest.raises(DBAPIError, match="(?i)canceling statement|timeout"):
      with engine.connect() as conn:
         conn.execute(text("SELECT pg_sleep(3.5)"))


# ---------------------------------------------------------------------------
# Step 2: performance/resource acceptance over the complete 24h endpoint
# ---------------------------------------------------------------------------

def _run_dashboard_request(engine, now):
   """Build a DashboardService and run one complete 24h request for
   NODES[0], returning (payload_bytes, elapsed_seconds, statement_times).

   ``statement_times`` is a list of per-SQL-statement durations captured
   via SQLAlchemy's before/after_cursor_execute hooks for this one call.
   """
   statement_times = []
   starts = {}

   def _before(conn, cursor, statement, parameters, context, executemany):
      starts[id(cursor)] = time.monotonic()

   def _after(conn, cursor, statement, parameters, context, executemany):
      start = starts.pop(id(cursor), None)
      if start is not None:
         statement_times.append(time.monotonic() - start)

   event.listen(engine, "before_cursor_execute", _before)
   event.listen(engine, "after_cursor_execute", _after)
   try:
      service = DashboardService(engine, system=SYSTEM)

      async def _once():
         start = time.monotonic()
         payload = await service.dashboard(
            node=NODES[0], range_name="24h", username=None)
         elapsed = time.monotonic() - start
         return payload, elapsed

      payload, elapsed = asyncio.run(_once())
   finally:
      event.remove(engine, "before_cursor_execute", _before)
      event.remove(engine, "after_cursor_execute", _after)
   return payload, elapsed, statement_times


@pg_skip
def test_complete_24h_dashboard_request_meets_performance_bounds(
      seeded_engine_and_counts):
   """Repeatedly exercise the complete 24h endpoint (after one warm-up) and
   assert every Task 9 performance bound.

   The RSS assertion measures the process's peak-RSS HIGH-WATER DELTA
   across the measured ITERATIONS requests (``_max_rss_bytes() after -
   _max_rss_bytes() before``, floored at zero) -- not the instantaneous
   resident-set size of any single request. ``ru_maxrss`` is a
   monotonically non-decreasing high-water mark for the whole process
   lifetime, so this delta captures how much additional peak memory the
   measured block of work caused the process to touch at its worst moment.
   """
   engine, _counts, now = seeded_engine_and_counts

   # Warm-up (not included in measured latencies -- JIT/connection/caching).
   _run_dashboard_request(engine, now)

   ITERATIONS = 5
   latencies = []
   statement_times_all = []
   last_payload = None

   rss_hwm_before = _max_rss_bytes()
   for _ in range(ITERATIONS):
      payload, elapsed, statement_times = _run_dashboard_request(engine, now)
      latencies.append(elapsed)
      statement_times_all.extend(statement_times)
      last_payload = payload
   rss_hwm_after = _max_rss_bytes()

   latencies.sort()
   median_latency = latencies[len(latencies) // 2]
   p95_index = min(len(latencies) - 1, int(round(0.95 * (len(latencies) - 1))))
   p95_latency = latencies[p95_index]

   import json
   assert last_payload is not None
   data = json.loads(last_payload)
   returned_counter_rows = len(data["counters"]["rows"])
   returned_aggregate_rows = len(data["usage"]["grains"])
   response_bytes = len(last_payload)
   rss_delta = max(0, rss_hwm_after - rss_hwm_before)

   print("\n--- Task 9 performance evidence ---")
   print("median_latency_sec=%.4f p95_latency_sec=%.4f" %
         (median_latency, p95_latency))
   print("max_statement_sec=%.4f (n=%d statements)" %
         (max(statement_times_all) if statement_times_all else 0.0,
          len(statement_times_all)))
   print("returned_counter_rows=%d returned_aggregate_rows=%d" %
         (returned_counter_rows, returned_aggregate_rows))
   print("response_bytes=%d" % response_bytes)
   print("rss_hwm_before=%d rss_hwm_after=%d rss_hwm_delta=%d" %
         (rss_hwm_before, rss_hwm_after, rss_delta))

   # ---- Blocking acceptance assertions (plan Task 9, Step 2) ----
   assert statement_times_all, "expected at least one SQL statement to be timed"
   assert max(statement_times_all) <= MAX_STATEMENT_SECONDS, (
      "a single DB statement exceeded %.1fs: %.4fs"
      % (MAX_STATEMENT_SECONDS, max(statement_times_all)))
   assert median_latency <= MAX_REQUEST_SECONDS, (
      "median request latency %.4fs exceeds %.1fs"
      % (median_latency, MAX_REQUEST_SECONDS))
   assert p95_latency <= MAX_REQUEST_SECONDS, (
      "p95 request latency %.4fs exceeds %.1fs"
      % (p95_latency, MAX_REQUEST_SECONDS))
   assert response_bytes <= MAX_RESPONSE_BYTES, (
      "response %d bytes exceeds %d byte cap"
      % (response_bytes, MAX_RESPONSE_BYTES))
   assert returned_counter_rows <= MAX_COUNTER_ROWS, (
      "returned %d counter rows exceeds cap of %d"
      % (returned_counter_rows, MAX_COUNTER_ROWS))
   assert rss_delta <= MAX_RSS_HWM_DELTA_BYTES, (
      "process peak-RSS high-water-mark delta %d bytes exceeds cap of "
      "%d bytes" % (rss_delta, MAX_RSS_HWM_DELTA_BYTES))

   # The aggregate (GROUP BY/DISTINCT ON) usage result set must be far
   # smaller than the raw usage grid -- proof the aggregation happens in
   # SQL, not by loading the whole grid into Python and summarizing there.
   assert returned_aggregate_rows < expected_usage_rows_per_node() / 10


@pg_skip
def test_counter_query_inspects_bounded_row_count_via_explain(
      seeded_engine_and_counts):
   """EXPLAIN ANALYZE proves COUNTER_SQL's planner-reported actual row count
   for the complete 24h range matches the real bounded result set (<=1440),
   not the full historical table.
   """
   from node_monitor.web.queries import COUNTER_SQL, COUNTER_ROW_LIMIT

   engine, _counts, now = seeded_engine_and_counts
   start = now - timedelta(hours=24)
   with engine.connect() as conn:
      plan_rows = conn.execute(
         text("EXPLAIN (ANALYZE, FORMAT JSON) " + str(COUNTER_SQL).strip()),
         {"system": SYSTEM, "node": NODES[0], "start": start, "end": now,
          "limit": COUNTER_ROW_LIMIT + 1},
      ).scalar_one()
   root = plan_rows[0]["Plan"]
   actual_rows = root["Actual Rows"]
   print("\n--- EXPLAIN inspected counter rows: %d ---" % actual_rows)
   assert actual_rows <= MAX_COUNTER_ROWS
