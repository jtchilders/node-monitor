"""Real-PostgreSQL tests for node_monitor.web.queries.

All tests in this file are unconditionally SKIPPED when
NODE_MONITOR_TEST_DATABASE_URL is not set.  They NEVER fabricate live
evidence: if the environment variable is absent the skip message says so
and no assertion is attempted.

When the variable IS set, the tests:
  1. Spin up a temporary PostgreSQL database via MigrationRunner.
  2. Insert production-shaped rows via DatabaseWriter.
  3. Execute the exact query constants from queries.py against the live DB.
  4. Assert exact result values.
  5. Collect EXPLAIN (ANALYZE, BUFFERS) output for each query (printed to
     stdout as test output artifacts; not committed to the repository).

Schema contract validated: the live schema created by MigrationRunner must
have username_key as a generated column COALESCE(username, '') -- verified
by inserting rows with NULL and non-NULL usernames and checking the column.
"""

import os
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.pool import NullPool

from node_monitor.database.migration import MigrationRunner
from node_monitor.database.writer import DatabaseWriter

from node_monitor.web.queries import (
   COLLECTION_LOG_SQL,
   COUNTER_ROW_LIMIT,
   COUNTER_SQL,
   COUNTER_STALENESS_SECONDS,
   LUSTRE_PEAK_SUM_API_LABEL,
   LUSTRE_PEAK_SUM_SOURCE,
   MAX_POLL_FAILURES,
   POLL_FAILURES_SQL,
   QueryBoundsError,
   QueryValidationError,
   USAGE_CPU_SQL,
   USAGE_D_STATE_HOTSPOT_SQL,
   USAGE_PROCESS_COUNT_P95_HOTSPOT_SQL,
   USAGE_RSS_P95_HOTSPOT_SQL,
   load_collection_log,
   load_counters,
   load_poll_failures,
   load_usage,
)

# ---------------------------------------------------------------------------
# Skip marker
# ---------------------------------------------------------------------------

_DB_URL = os.environ.get("NODE_MONITOR_TEST_DATABASE_URL")
_PG_AVAILABLE = bool(_DB_URL)

pytestmark = pytest.mark.skipif(
   not _PG_AVAILABLE,
   reason="NODE_MONITOR_TEST_DATABASE_URL is required for PostgreSQL tests",
)

# ---------------------------------------------------------------------------
# Base timestamps
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 9, 30, 14, 0, tzinfo=timezone.utc)
_WINDOW_START = _NOW - timedelta(hours=1)
_INVENTORY = frozenset({"login-04", "login-05"})

# ---------------------------------------------------------------------------
# Database engine fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def pg_engine():
   """Create an isolated test database, run all migrations, yield engine."""
   base_url = make_url(_DB_URL)
   admin_url = base_url.set(database="postgres")
   test_db = "nm_web_queries_test_%s" % os.getpid()

   admin_engine = create_engine(admin_url, poolclass=NullPool)
   with admin_engine.connect().execution_options(
         isolation_level="AUTOCOMMIT") as conn:
      conn.execute(text("CREATE DATABASE %s" % test_db))
   admin_engine.dispose()

   test_url = base_url.set(database=test_db)
   engine = create_engine(str(test_url), poolclass=NullPool)
   runner = MigrationRunner(engine, "test")
   runner.migrate()

   yield engine

   engine.dispose()
   cleanup_engine = create_engine(admin_url, poolclass=NullPool)
   with cleanup_engine.connect().execution_options(
         isolation_level="AUTOCOMMIT") as conn:
      conn.execute(text(
         "DROP DATABASE IF EXISTS %s WITH (FORCE)" % test_db))
   cleanup_engine.dispose()


@pytest.fixture()
def db(pg_engine):
   """Per-test engine wrapper for DatabaseWriter; truncates tables before each test."""
   class _DB:
      def begin(self):
         return pg_engine.begin()

   with pg_engine.begin() as conn:
      conn.execute(text(
         "TRUNCATE node_monitor.node_counter_minute, "
         "node_monitor.node_usage_intervals, "
         "node_monitor.node_poll_failures, "
         "node_monitor.node_collection_log "
         "RESTART IDENTITY CASCADE"
      ))
   return _DB()


@pytest.fixture()
def writer(db):
   return DatabaseWriter(db, clock=lambda: _NOW)


@pytest.fixture()
def conn(pg_engine):
   """A single autocommit connection for SELECT-only queries."""
   with pg_engine.connect() as c:
      yield c


# ---------------------------------------------------------------------------
# Helpers: record builders (production-shaped)
# ---------------------------------------------------------------------------

def _counter_record(window_start_utc, window_end_utc, **overrides):
   record = {
      "system": "polaris",
      "source_hostname": "login-04",
      "collector_hostname": "login-04",
      "probe_version": 4,
      "daemon_version": "0.2.0",
      "window_start_utc": window_start_utc,
      "window_end_utc": window_end_utc,
      "sample_count": 6,
      "expected_count": 6,
      "coverage": 1.0,
      "end_of_window": {
         "mem_available_kb": 900000, "cached_kb": 100000, "shmem_kb": 10000,
         "load1": 1.0, "load5": 2.0, "load15": 3.0,
         "procs_running": 2, "procs_total": 100, "socket_count": 8,
      },
      "rates": {
         "cpu_busy_pct": {"p50": 10.0, "p95": 20.0, "max": 25.0},
         "network": {
            "eth0": {
               "rx_bytes_per_sec": {"p50": 1.0, "p95": 2.0, "max": 3.0},
               "tx_bytes_per_sec": {"p50": 4.0, "p95": 5.0, "max": 6.0},
            },
         },
         "lustre_md_ops": {
            "fs-MDT0000": {
               "open": {"p50": 4.0, "p95": 5.0, "max": 6.0},
            },
         },
      },
      "audit": {
         "meets_minimum_samples": True,
         "invalid_pairs": [],
         "excess_sample_count": 0,
         "raw_cumulative": {"window_first": {}, "window_last": {}},
      },
   }
   record.update(overrides)
   return record


def _usage_record(interval_start_utc, interval_end_utc, username=None,
                  cpu_seconds=3.0, rss_kb=None, d_state_fraction=0.0,
                  **overrides):
   record = {
      "system": "polaris",
      "source_hostname": "login-04",
      "interval_start_utc": interval_start_utc,
      "interval_end_utc": interval_end_utc,
      "category": "ai_coding",
      "activity": "active",
      "username": username,
      "process_count": {"p50": 1, "p95": 2, "max": 2},
      "cpu_seconds": cpu_seconds,
      "rss_kb": rss_kb or {"p50": 4000, "p95": 8000, "max": 9000},
      "d_state_fraction": d_state_fraction,
      "interactivity_fraction": 0.80,
      "sample_count": 6,
      "expected_count": 6,
      "unmeasured_count": 0,
   }
   record.update(overrides)
   return record


def _poll_failure_record(timestamp_utc, failure_type="timeout",
                         consecutive_failures=1):
   return {
      "system": "polaris",
      "source_hostname": "login-04",
      "loop": "counter",
      "timestamp_utc": timestamp_utc,
      "failure_type": failure_type,
      "detail": "probe timed out",
      "consecutive_failures": consecutive_failures,
      "breaker_state": "closed",
   }


def _collection_log_record(timestamp_utc, event="start"):
   return {
      "system": "polaris",
      "timestamp_utc": timestamp_utc,
      "event": event,
      "detail": {"host": "collector-01"},
   }


def _ts(offset_minutes):
   return (_NOW + timedelta(minutes=offset_minutes)).strftime(
      "%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Test: username_key generated column (schema contract)
# ---------------------------------------------------------------------------

def test_username_key_is_generated_column_coalesce_username_empty(writer, conn):
   """username_key must equal COALESCE(username, '') -- verified live."""
   writer.write_record("node_usage_intervals", _usage_record(
      _ts(-60), _ts(-45), username="alice"))
   writer.write_record("node_usage_intervals", _usage_record(
      _ts(-60), _ts(-45), username=None, activity="idle"))

   rows = list(conn.execute(text(
      "SELECT username, username_key "
      "FROM node_monitor.node_usage_intervals "
      "WHERE system = 'polaris' AND source_hostname = 'login-04' "
      "ORDER BY username_key"
   )).mappings())

   # NULL username -> username_key = ''
   null_row = next(r for r in rows if r["username"] is None)
   assert null_row["username_key"] == ""
   # Named username -> username_key = username
   named_row = next(r for r in rows if r["username"] == "alice")
   assert named_row["username_key"] == "alice"


# ---------------------------------------------------------------------------
# Test: counter query returns correct JSONB shapes
# ---------------------------------------------------------------------------

def test_counter_query_returns_production_jsonb_shapes(writer, conn):
   """JSONB columns must have the shapes defined by the writer contract."""
   writer.write_record("node_counter_samples", _counter_record(
      _ts(-60), _ts(-59)))

   rows = list(conn.execute(
      COUNTER_SQL,
      {"system": "polaris", "node": "login-04", "start": _WINDOW_START,
       "end": _NOW + timedelta(hours=1), "limit": COUNTER_ROW_LIMIT + 1},
   ).mappings())
   assert len(rows) == 1
   row = rows[0]

   # cpu_busy_pct: {"p50", "p95", "max"}
   assert set(row["cpu_busy_pct"]) == {"p50", "p95", "max"}

   # network_rates: {"eth0": {"rx_bytes_per_sec": {p50/p95/max}, ...}}
   assert "eth0" in row["network_rates"]
   assert set(row["network_rates"]["eth0"]["rx_bytes_per_sec"]) == {"p50", "p95", "max"}
   assert "lo" not in row["network_rates"]  # loopback excluded by writer

   # lustre_md_summary: {"open": {"p50_sum", "p95_sum", "max_sum", "target_count"}}
   assert "open" in row["lustre_md_summary"]
   assert set(row["lustre_md_summary"]["open"]) == {
      "p50_sum", "p95_sum", "max_sum", "target_count"}

   # lustre peak-sum field is present
   assert LUSTRE_PEAK_SUM_SOURCE in row["lustre_md_summary"]["open"]


# ---------------------------------------------------------------------------
# Test: counter freshness and staleness
# ---------------------------------------------------------------------------

def test_counter_freshness_is_evaluated_against_newest_window_end(writer, conn):
   """load_counters sets is_fresh based on newest window_end vs now_utc."""
   writer.write_record("node_counter_samples", _counter_record(
      _ts(-60), _ts(-59)))

   # Simulate now as just after the window_end: within 120s -> fresh.
   fresh_now = _NOW - timedelta(minutes=59) + timedelta(seconds=60)
   result = load_counters(
      conn, "polaris", "login-04", _WINDOW_START, _INVENTORY,
      now_utc=fresh_now)
   assert result.is_fresh is True

   # Simulate now as much later: beyond 120s -> stale.
   stale_now = _NOW + timedelta(hours=1)
   result2 = load_counters(
      conn, "polaris", "login-04", _WINDOW_START, _INVENTORY,
      now_utc=stale_now)
   assert result2.is_fresh is False


# ---------------------------------------------------------------------------
# Test: counter row cap -- 1440 accepted, 1441 rejected
# ---------------------------------------------------------------------------

def test_counter_limit_accepts_1440_rows(writer, conn):
   """Insert exactly 1440 rows and verify they are returned without error."""
   for i in range(1, 1441):
      start = _ts(-1440 - 1 + i)
      end = _ts(-1440 + i)
      writer.write_record("node_counter_samples", _counter_record(start, end))

   result = load_counters(
      conn, "polaris", "login-04",
      _NOW - timedelta(hours=25),
      _INVENTORY, now_utc=_NOW + timedelta(hours=1))
   assert len(result.rows) == 1440


def test_counter_limit_rejects_1441_rows(writer, conn):
   """Insert 1441 rows; load_counters must raise QueryBoundsError."""
   for i in range(1, 1442):
      start = _ts(-1441 - 1 + i)
      end = _ts(-1441 + i)
      writer.write_record("node_counter_samples", _counter_record(start, end))

   with pytest.raises(QueryBoundsError, match="counter row limit exceeded"):
      load_counters(
         conn, "polaris", "login-04",
         _NOW - timedelta(hours=25),
         _INVENTORY, now_utc=_NOW + timedelta(hours=1))


# ---------------------------------------------------------------------------
# Test: usage CPU sum (additive) and hotspot semantics
# ---------------------------------------------------------------------------

def test_usage_cpu_seconds_is_additive_across_username_grains(writer, conn):
   """cpu_seconds must be summed (not averaged) across username grains."""
   writer.write_record("node_usage_intervals", _usage_record(
      _ts(-60), _ts(-45), username="alice", cpu_seconds=3.0))
   writer.write_record("node_usage_intervals", _usage_record(
      _ts(-60), _ts(-45), username="bob", cpu_seconds=6.0))

   rows = list(conn.execute(
      USAGE_CPU_SQL,
      {"system": "polaris", "node": "login-04",
       "start": _WINDOW_START, "end": _NOW + timedelta(hours=1),
       "username_is_null": True, "username": None},
   ).mappings())
   assert len(rows) == 1
   # 3.0 + 6.0 = 9.0 -- additive, not 4.5 (average) or 3.0/6.0 (one row)
   assert abs(rows[0]["cpu_seconds"] - 9.0) < 1e-9


def test_usage_rss_hotspot_selects_highest_p95_username(writer, conn):
   """DISTINCT ON hotspot must return the username with the highest rss p95."""
   writer.write_record("node_usage_intervals", _usage_record(
      _ts(-60), _ts(-45), username="alice",
      rss_kb={"p50": 4000, "p95": 8000, "max": 9000}))
   writer.write_record("node_usage_intervals", _usage_record(
      _ts(-60), _ts(-45), username="biggest-user",
      rss_kb={"p50": 5000, "p95": 12000, "max": 13000}))

   rows = list(conn.execute(
      USAGE_RSS_P95_HOTSPOT_SQL,
      {"system": "polaris", "node": "login-04",
       "start": _WINDOW_START, "end": _NOW + timedelta(hours=1),
       "username_is_null": True, "username": None},
   ).mappings())
   assert len(rows) == 1
   assert rows[0]["username"] == "biggest-user"
   assert abs(rows[0]["rss_p95_kb"] - 12000.0) < 1e-9


def test_usage_d_state_hotspot_selects_highest_fraction_username(writer, conn):
   """D-state hotspot must return the username with highest d_state_fraction."""
   writer.write_record("node_usage_intervals", _usage_record(
      _ts(-60), _ts(-45), username="alice", d_state_fraction=0.10))
   writer.write_record("node_usage_intervals", _usage_record(
      _ts(-60), _ts(-45), username="blocked-user", d_state_fraction=0.40))

   rows = list(conn.execute(
      USAGE_D_STATE_HOTSPOT_SQL,
      {"system": "polaris", "node": "login-04",
       "start": _WINDOW_START, "end": _NOW + timedelta(hours=1),
       "username_is_null": True, "username": None},
   ).mappings())
   assert len(rows) == 1
   assert rows[0]["username"] == "blocked-user"
   assert abs(rows[0]["d_state_fraction"] - 0.40) < 1e-9


def test_usage_username_key_tiebreaker_is_deterministic(writer, conn):
   """When two usernames have equal rss p95, username_key ASC breaks the tie."""
   # "aaa" < "zzz" lexicographically -> "aaa" wins when rss is equal.
   writer.write_record("node_usage_intervals", _usage_record(
      _ts(-60), _ts(-45), username="zzz-user",
      rss_kb={"p50": 5000, "p95": 10000, "max": 11000}))
   writer.write_record("node_usage_intervals", _usage_record(
      _ts(-60), _ts(-45), username="aaa-user",
      rss_kb={"p50": 5000, "p95": 10000, "max": 11000}))

   rows = list(conn.execute(
      USAGE_RSS_P95_HOTSPOT_SQL,
      {"system": "polaris", "node": "login-04",
       "start": _WINDOW_START, "end": _NOW + timedelta(hours=1),
       "username_is_null": True, "username": None},
   ).mappings())
   assert len(rows) == 1
   # "aaa-user" has username_key "aaa-user" < "zzz-user"
   assert rows[0]["username"] == "aaa-user"


def test_usage_username_filter_binds_as_data(writer, conn):
   """Filtering by username must return only that user's rows."""
   writer.write_record("node_usage_intervals", _usage_record(
      _ts(-60), _ts(-45), username="alice", cpu_seconds=3.0))
   writer.write_record("node_usage_intervals", _usage_record(
      _ts(-60), _ts(-45), username="bob", cpu_seconds=6.0))

   rows = list(conn.execute(
      USAGE_CPU_SQL,
      {"system": "polaris", "node": "login-04",
       "start": _WINDOW_START, "end": _NOW + timedelta(hours=1),
       "username_is_null": False, "username": "alice"},
   ).mappings())
   assert len(rows) == 1
   assert abs(rows[0]["cpu_seconds"] - 3.0) < 1e-9


def test_usage_complete_is_false_when_unmeasured_count_nonzero(writer, conn):
   """complete column must be False when any grain has unmeasured_count > 0."""
   writer.write_record("node_usage_intervals", _usage_record(
      _ts(-60), _ts(-45), username="alice", cpu_seconds=3.0,
      unmeasured_count=1))

   rows = list(conn.execute(
      USAGE_CPU_SQL,
      {"system": "polaris", "node": "login-04",
       "start": _WINDOW_START, "end": _NOW + timedelta(hours=1),
       "username_is_null": True, "username": None},
   ).mappings())
   assert len(rows) == 1
   assert rows[0]["complete"] is False


def test_load_usage_aggregates_correctly(writer, conn):
   """load_usage returns a UsageResult with correctly merged hotspot data."""
   writer.write_record("node_usage_intervals", _usage_record(
      _ts(-60), _ts(-45), username="alice",
      cpu_seconds=3.0, rss_kb={"p50": 4000, "p95": 8000, "max": 9000},
      d_state_fraction=0.10))
   writer.write_record("node_usage_intervals", _usage_record(
      _ts(-60), _ts(-45), username="largest-user",
      cpu_seconds=6.0, rss_kb={"p50": 5000, "p95": 12000, "max": 13000},
      d_state_fraction=0.40))

   result = load_usage(
      conn, "polaris", "login-04", _WINDOW_START, _INVENTORY,
      now_utc=_NOW + timedelta(hours=1))

   ie = _NOW - timedelta(minutes=45)
   grain = result.by_key[("ai_coding", "active", ie)]

   # Additive cpu_seconds
   assert abs(grain.cpu_seconds - 9.0) < 1e-9
   # Hotspot: rss_p95 from largest-user
   assert abs(grain.rss_p95_kb - 12000.0) < 1e-9
   assert grain.rss_p95_username == "largest-user"
   # Hotspot: d_state from largest-user (0.40 > 0.10)
   assert abs(grain.d_state_fraction - 0.40) < 1e-9
   assert grain.d_state_username == "largest-user"


def test_gaps_are_not_zero_filled(writer, conn):
   """Two intervals with a time gap must produce two separate grains, not fill
   the gap with zeros."""
   writer.write_record("node_usage_intervals", _usage_record(
      _ts(-120), _ts(-105), cpu_seconds=1.0))
   writer.write_record("node_usage_intervals", _usage_record(
      _ts(-60), _ts(-45), cpu_seconds=2.0))

   result = load_usage(
      conn, "polaris", "login-04",
      _NOW - timedelta(hours=3),
      _INVENTORY, now_utc=_NOW + timedelta(hours=1))

   # Exactly two grains -- no phantom zero-filled row for the gap.
   assert len(result.by_key) == 2


# ---------------------------------------------------------------------------
# Test: poll failures
# ---------------------------------------------------------------------------

def test_poll_failures_capped_at_10(writer, conn):
   """Only 10 most recent failures are returned even when 15 are stored."""
   for i in range(15):
      writer.write_record("node_poll_failures", _poll_failure_record(
         _ts(-15 + i)))

   result = load_poll_failures(conn, "polaris", "login-04", _INVENTORY,
                               start=_WINDOW_START, end=_NOW + timedelta(hours=1))
   assert len(result) == MAX_POLL_FAILURES


def test_poll_failures_most_recent_first(writer, conn):
   """Poll failures must be ordered by recorded_at DESC."""
   for i in range(3):
      writer.write_record("node_poll_failures", _poll_failure_record(
         _ts(-3 + i), consecutive_failures=i + 1))

   result = load_poll_failures(conn, "polaris", "login-04", _INVENTORY,
                               start=_WINDOW_START, end=_NOW + timedelta(hours=1))
   # Highest consecutive_failures = 3 = most recent
   assert result[0]["consecutive_failures"] == 3


# ---------------------------------------------------------------------------
# Test: collection log is system-level
# ---------------------------------------------------------------------------

def test_collection_log_returns_system_events_without_node_filter(writer, conn):
   """Collection log must be queried without source_hostname filter."""
   writer.write_record("node_collection_log", _collection_log_record(
      _ts(-30), event="start"))
   writer.write_record("node_collection_log", _collection_log_record(
      _ts(-20), event="stop"))

   result = load_collection_log(conn, "polaris", _WINDOW_START,
                                end=_NOW + timedelta(hours=1))
   assert len(result) == 2
   events = {r["event"] for r in result}
   assert events == {"start", "stop"}


# ---------------------------------------------------------------------------
# Step 5: EXPLAIN (ANALYZE, BUFFERS) output for planner evidence
# ---------------------------------------------------------------------------

def _explain(conn, sql, params):
   """Run EXPLAIN (ANALYZE, BUFFERS) and return the plan text."""
   plan_sql = "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) " + str(sql).strip()
   rows = conn.execute(text(plan_sql), params).fetchall()
   return "\n".join(r[0] for r in rows)


def test_explain_counter_sql(writer, conn):
   """EXPLAIN counter query -- planner evidence (printed, not asserted)."""
   for i in range(10):
      writer.write_record("node_counter_samples", _counter_record(
         _ts(-10 + i - 1), _ts(-10 + i)))

   plan = _explain(conn, COUNTER_SQL,
                   {"system": "polaris", "node": "login-04",
                    "start": _WINDOW_START, "end": _NOW + timedelta(hours=1),
                    "limit": COUNTER_ROW_LIMIT + 1})
   print("\n--- EXPLAIN COUNTER_SQL ---\n" + plan)
   # Planner evidence: not asserting latency, just verifying plan is returned.
   assert "Seq Scan" in plan or "Index Scan" in plan or "Bitmap" in plan


def test_explain_usage_cpu_sql(writer, conn):
   """EXPLAIN usage CPU query -- planner evidence."""
   writer.write_record("node_usage_intervals", _usage_record(
      _ts(-60), _ts(-45), username="alice"))
   params = {
      "system": "polaris", "node": "login-04", "start": _WINDOW_START,
      "end": _NOW + timedelta(hours=1),
      "username_is_null": True, "username": None,
   }
   plan = _explain(conn, USAGE_CPU_SQL, params)
   print("\n--- EXPLAIN USAGE_CPU_SQL ---\n" + plan)
   assert "Seq Scan" in plan or "Index Scan" in plan or "Bitmap" in plan


def test_explain_rss_hotspot_sql(writer, conn):
   """EXPLAIN RSS hotspot query -- planner evidence."""
   writer.write_record("node_usage_intervals", _usage_record(
      _ts(-60), _ts(-45), username="alice"))
   params = {
      "system": "polaris", "node": "login-04", "start": _WINDOW_START,
      "end": _NOW + timedelta(hours=1),
      "username_is_null": True, "username": None,
   }
   plan = _explain(conn, USAGE_RSS_P95_HOTSPOT_SQL, params)
   print("\n--- EXPLAIN USAGE_RSS_P95_HOTSPOT_SQL ---\n" + plan)
   assert "Seq Scan" in plan or "Index Scan" in plan or "Bitmap" in plan


def test_explain_d_state_hotspot_sql(writer, conn):
   """EXPLAIN D-state hotspot query -- planner evidence."""
   writer.write_record("node_usage_intervals", _usage_record(
      _ts(-60), _ts(-45), username="alice"))
   params = {
      "system": "polaris", "node": "login-04", "start": _WINDOW_START,
      "end": _NOW + timedelta(hours=1),
      "username_is_null": True, "username": None,
   }
   plan = _explain(conn, USAGE_D_STATE_HOTSPOT_SQL, params)
   print("\n--- EXPLAIN USAGE_D_STATE_HOTSPOT_SQL ---\n" + plan)
   assert "Seq Scan" in plan or "Index Scan" in plan or "Bitmap" in plan


def test_explain_poll_failures_sql(writer, conn):
   """EXPLAIN poll failures query -- planner evidence."""
   writer.write_record("node_poll_failures", _poll_failure_record(_ts(-5)))
   plan = _explain(conn, POLL_FAILURES_SQL,
                   {"system": "polaris", "node": "login-04",
                    "start": _WINDOW_START, "end": _NOW + timedelta(hours=1),
                    "limit": MAX_POLL_FAILURES})
   print("\n--- EXPLAIN POLL_FAILURES_SQL ---\n" + plan)
   assert "Seq Scan" in plan or "Index Scan" in plan or "Bitmap" in plan


def test_explain_collection_log_sql(writer, conn):
   """EXPLAIN collection log query -- planner evidence."""
   writer.write_record("node_collection_log", _collection_log_record(_ts(-10)))
   plan = _explain(conn, COLLECTION_LOG_SQL,
                   {"system": "polaris", "start": _WINDOW_START,
                    "end": _NOW + timedelta(hours=1),
                    "limit": 200})
   print("\n--- EXPLAIN COLLECTION_LOG_SQL ---\n" + plan)
   assert "Seq Scan" in plan or "Index Scan" in plan or "Bitmap" in plan
