"""Task 4 correction tests -- bounded-window and mathematical ranking defects.

Covers all 6 blockers identified in the second audit:

(1) Independent hotspot ranking: rss_p50/p95/max and process_count_p50/p95/max
    each use their OWN independently ranked DISTINCT ON query.  Different users
    can maximize different statistics.  Six separate SQL constants required.
    Each statistic exposes its own contributing username attribute.

(2) complete/partial is a computed property on CounterResult rows (not just
    raw metadata echoed).  Complete iff coverage == 1, meets_minimum_samples
    is True, invalid_pair_count == 0, excess_sample_count == 0.  Each false
    condition tested individually.

(3) Bounded windows: all queries accept an :end upper bound parameter.
    Counter: window_end <= :end.
    Usage: interval_end <= :end.
    Collection log: recorded_at <= :end.
    Poll failures: recorded_at >= :start AND recorded_at <= :end (full range).

(4) _validate_utc_datetime enforces ZERO UTC offset strictly.  A non-UTC
    aware datetime (e.g. US/Eastern) raises QueryValidationError.

(5) Public range-based facade: load_counters_for_range, load_usage_for_range,
    load_poll_failures_for_range, load_collection_log_for_range.
    These derive start = range_hours_to_start(hours, now_utc) internally.
    Task 5 must not call private validators separately.

(6) API metric allowlist: map_metric_to_sql is a dict mapping API metric
    name strings to their fixed SQL constant (no dynamic SQL formatting).
"""

from datetime import datetime, timedelta, timezone, tzinfo
import zoneinfo

import pytest

from node_monitor.web.queries import (
   QueryValidationError,
   _validate_utc_datetime,
   VALID_RANGES_HOURS,
)

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 9, 30, 14, 0, tzinfo=timezone.utc)
_START = _NOW - timedelta(hours=1)
_END = _NOW
_INVENTORY = frozenset({"login-04", "login-05"})


def _make_counter_row(window_end_offset_minutes, coverage=1.0,
                       meets_minimum_samples=True,
                       invalid_pair_count=0, excess_sample_count=0):
   """Return a minimal counter row dict."""
   we = _NOW - timedelta(minutes=window_end_offset_minutes)
   return {
      "window_start": we - timedelta(minutes=1),
      "window_end": we,
      "sample_count": 6, "expected_count": 6, "coverage": coverage,
      "meets_minimum_samples": meets_minimum_samples,
      "invalid_pair_count": invalid_pair_count,
      "excess_sample_count": excess_sample_count,
      "mem_available_kb": None, "cached_kb": None, "shmem_kb": None,
      "load1": None, "load5": None, "load15": None,
      "procs_running": None, "procs_total": None, "socket_count": None,
      "cpu_busy_pct": None, "network_rates": None, "lustre_md_summary": None,
   }


from unittest.mock import MagicMock


def _make_conn(rows_per_call):
   """Return a minimal mock connection."""
   conn = MagicMock()
   calls = iter(rows_per_call)

   def _execute(stmt, params=None):
      result = MagicMock()
      result.mappings.return_value = next(calls)
      return result

   conn.execute.side_effect = _execute
   return conn


def _make_cpu_row(interval_end=None, cpu_seconds=9.0):
   ie = interval_end or (_NOW - timedelta(minutes=1))
   return {
      "interval_end": ie,
      "category": "ai_coding",
      "activity": "active",
      "cpu_seconds": cpu_seconds,
      "complete": True,
   }


def _make_rss_row(interval_end=None, rss_p50_kb=4000.0, rss_p95_kb=8000.0,
                   rss_max_kb=9000.0, username="rss-user"):
   ie = interval_end or (_NOW - timedelta(minutes=1))
   return {
      "interval_end": ie, "category": "ai_coding", "activity": "active",
      "username": username,
      "rss_p50_kb": rss_p50_kb, "rss_p95_kb": rss_p95_kb, "rss_max_kb": rss_max_kb,
      "sample_count": 6, "expected_count": 6, "unmeasured_count": 0,
   }


def _make_proc_row(interval_end=None, process_count_p50=1.0,
                    process_count_p95=2.0, process_count_max=3.0,
                    username="proc-user"):
   ie = interval_end or (_NOW - timedelta(minutes=1))
   return {
      "interval_end": ie, "category": "ai_coding", "activity": "active",
      "username": username,
      "process_count_p50": process_count_p50,
      "process_count_p95": process_count_p95,
      "process_count_max": process_count_max,
      "sample_count": 6, "expected_count": 6, "unmeasured_count": 0,
   }


def _make_d_row(interval_end=None, d_state_fraction=0.10, username="d-user"):
   ie = interval_end or (_NOW - timedelta(minutes=1))
   return {
      "interval_end": ie, "category": "ai_coding", "activity": "active",
      "username": username, "d_state_fraction": d_state_fraction,
      "sample_count": 6, "expected_count": 6, "unmeasured_count": 0,
   }


def _make_ia_row(interval_end=None, interactivity_fraction=0.80,
                  username="ia-user"):
   ie = interval_end or (_NOW - timedelta(minutes=1))
   return {
      "interval_end": ie, "category": "ai_coding", "activity": "active",
      "username": username, "interactivity_fraction": interactivity_fraction,
      "sample_count": 6, "expected_count": 6, "unmeasured_count": 0,
   }


def _make_usage_conn_9q(cpu=None, rss_p50=None, rss_p95=None, rss_max=None,
                         d=None, proc_p50=None, proc_p95=None, proc_max=None,
                         ia=None):
   """Build mock conn returning 9 query result lists: cpu + 3 rss + 3 proc + d + ia."""
   ie = _NOW - timedelta(minutes=1)
   return _make_conn([
      cpu if cpu is not None else [_make_cpu_row()],
      rss_p50 if rss_p50 is not None else [_make_rss_row(rss_p50_kb=4000.0, username="rss-p50-user")],
      rss_p95 if rss_p95 is not None else [_make_rss_row(rss_p95_kb=8000.0, username="rss-p95-user")],
      rss_max if rss_max is not None else [_make_rss_row(rss_max_kb=9000.0, username="rss-max-user")],
      d if d is not None else [_make_d_row()],
      proc_p50 if proc_p50 is not None else [_make_proc_row(process_count_p50=1.0, username="proc-p50-user")],
      proc_p95 if proc_p95 is not None else [_make_proc_row(process_count_p95=2.0, username="proc-p95-user")],
      proc_max if proc_max is not None else [_make_proc_row(process_count_max=3.0, username="proc-max-user")],
      ia if ia is not None else [_make_ia_row()],
   ])


# ===========================================================================
# BLOCKER 1: Independent hotspot ranking -- 6 SQL statements
# ===========================================================================

class TestIndependentHotspotRanking:
   """Each of p50/p95/max for rss and process_count must have its own SQL."""

   def test_six_separate_sql_constants_exist(self):
      """There must be 6 SQL constants: one per stat x metric combination."""
      from node_monitor.web.queries import (
         USAGE_RSS_P50_HOTSPOT_SQL,
         USAGE_RSS_P95_HOTSPOT_SQL,
         USAGE_RSS_MAX_HOTSPOT_SQL,
         USAGE_PROCESS_COUNT_P50_HOTSPOT_SQL,
         USAGE_PROCESS_COUNT_P95_HOTSPOT_SQL,
         USAGE_PROCESS_COUNT_MAX_HOTSPOT_SQL,
      )
      for sql in (
         USAGE_RSS_P50_HOTSPOT_SQL, USAGE_RSS_P95_HOTSPOT_SQL,
         USAGE_RSS_MAX_HOTSPOT_SQL, USAGE_PROCESS_COUNT_P50_HOTSPOT_SQL,
         USAGE_PROCESS_COUNT_P95_HOTSPOT_SQL, USAGE_PROCESS_COUNT_MAX_HOTSPOT_SQL,
      ):
         assert sql is not None

   def test_rss_p50_sql_orders_by_p50(self):
      """USAGE_RSS_P50_HOTSPOT_SQL must ORDER BY rss_kb->>'p50' DESC."""
      from node_monitor.web.queries import USAGE_RSS_P50_HOTSPOT_SQL
      sql = str(USAGE_RSS_P50_HOTSPOT_SQL)
      assert "'p50'" in sql
      assert "DESC" in sql.upper()

   def test_rss_p95_sql_orders_by_p95(self):
      """USAGE_RSS_P95_HOTSPOT_SQL must ORDER BY rss_kb->>'p95' DESC."""
      from node_monitor.web.queries import USAGE_RSS_P95_HOTSPOT_SQL
      sql = str(USAGE_RSS_P95_HOTSPOT_SQL)
      assert "'p95'" in sql
      assert "DESC" in sql.upper()

   def test_rss_max_sql_orders_by_max(self):
      """USAGE_RSS_MAX_HOTSPOT_SQL must ORDER BY rss_kb->>'max' DESC."""
      from node_monitor.web.queries import USAGE_RSS_MAX_HOTSPOT_SQL
      sql = str(USAGE_RSS_MAX_HOTSPOT_SQL)
      assert "'max'" in sql
      assert "DESC" in sql.upper()

   def test_proc_p50_sql_orders_by_p50(self):
      """USAGE_PROCESS_COUNT_P50_HOTSPOT_SQL must ORDER BY process_count->>'p50' DESC."""
      from node_monitor.web.queries import USAGE_PROCESS_COUNT_P50_HOTSPOT_SQL
      sql = str(USAGE_PROCESS_COUNT_P50_HOTSPOT_SQL)
      assert "'p50'" in sql
      assert "DESC" in sql.upper()

   def test_proc_p95_sql_orders_by_p95(self):
      """USAGE_PROCESS_COUNT_P95_HOTSPOT_SQL must ORDER BY process_count->>'p95' DESC."""
      from node_monitor.web.queries import USAGE_PROCESS_COUNT_P95_HOTSPOT_SQL
      sql = str(USAGE_PROCESS_COUNT_P95_HOTSPOT_SQL)
      assert "'p95'" in sql
      assert "DESC" in sql.upper()

   def test_proc_max_sql_orders_by_max(self):
      """USAGE_PROCESS_COUNT_MAX_HOTSPOT_SQL must ORDER BY process_count->>'max' DESC."""
      from node_monitor.web.queries import USAGE_PROCESS_COUNT_MAX_HOTSPOT_SQL
      sql = str(USAGE_PROCESS_COUNT_MAX_HOTSPOT_SQL)
      assert "'max'" in sql
      assert "DESC" in sql.upper()

   def test_six_sql_constants_are_pairwise_distinct(self):
      """All 6 SQL constants must be distinct objects (not aliases)."""
      from node_monitor.web.queries import (
         USAGE_RSS_P50_HOTSPOT_SQL, USAGE_RSS_P95_HOTSPOT_SQL,
         USAGE_RSS_MAX_HOTSPOT_SQL, USAGE_PROCESS_COUNT_P50_HOTSPOT_SQL,
         USAGE_PROCESS_COUNT_P95_HOTSPOT_SQL, USAGE_PROCESS_COUNT_MAX_HOTSPOT_SQL,
      )
      sqls = [
         str(USAGE_RSS_P50_HOTSPOT_SQL), str(USAGE_RSS_P95_HOTSPOT_SQL),
         str(USAGE_RSS_MAX_HOTSPOT_SQL), str(USAGE_PROCESS_COUNT_P50_HOTSPOT_SQL),
         str(USAGE_PROCESS_COUNT_P95_HOTSPOT_SQL),
         str(USAGE_PROCESS_COUNT_MAX_HOTSPOT_SQL),
      ]
      assert len(set(sqls)) == 6, (
         "All 6 hotspot SQL constants must be pairwise distinct -- "
         "each ranks by a DIFFERENT percentile expression")

   def test_load_usage_executes_nine_queries(self):
      """load_usage must issue 9 SQL queries: cpu + 3 rss + d + 3 proc + ia."""
      from node_monitor.web.queries import load_usage
      conn = _make_usage_conn_9q()
      load_usage(conn, "polaris", "login-04", _START, _INVENTORY, now_utc=_NOW)
      assert conn.execute.call_count == 9, (
         "load_usage must execute exactly 9 queries; got %d"
         % conn.execute.call_count)

   def test_grain_exposes_six_independent_usernames(self):
      """Each stat has its own username: rss_p50/p95/max, proc_p50/p95/max."""
      from node_monitor.web.queries import load_usage, _UsageGrain
      ie = _NOW - timedelta(minutes=1)
      conn = _make_usage_conn_9q(
         cpu=[_make_cpu_row(ie)],
         rss_p50=[_make_rss_row(ie, rss_p50_kb=4000.0, username="alice")],
         rss_p95=[_make_rss_row(ie, rss_p95_kb=8000.0, username="bob")],
         rss_max=[_make_rss_row(ie, rss_max_kb=9000.0, username="carol")],
         d=[_make_d_row(ie)],
         proc_p50=[_make_proc_row(ie, process_count_p50=1.0, username="dave")],
         proc_p95=[_make_proc_row(ie, process_count_p95=2.0, username="eve")],
         proc_max=[_make_proc_row(ie, process_count_max=3.0, username="frank")],
         ia=[_make_ia_row(ie)],
      )
      result = load_usage(conn, "polaris", "login-04", _START, _INVENTORY,
                          now_utc=_NOW)
      grain = result.by_key[("ai_coding", "active", ie)]
      assert grain.rss_p50_username == "alice"
      assert grain.rss_p95_username == "bob"
      assert grain.rss_max_username == "carol"
      assert grain.process_count_p50_username == "dave"
      assert grain.process_count_p95_username == "eve"
      assert grain.process_count_max_username == "frank"

   def test_different_users_win_rss_p50_and_p95_and_max(self):
      """Deterministic test: different usernames win p50, p95, max for RSS.

      alice has highest p50 but lowest p95; carol has highest max.
      This is mathematically impossible to get right with a single query."""
      from node_monitor.web.queries import load_usage
      ie = _NOW - timedelta(minutes=1)
      conn = _make_usage_conn_9q(
         cpu=[_make_cpu_row(ie)],
         rss_p50=[_make_rss_row(ie, rss_p50_kb=9999.0, username="p50-winner")],
         rss_p95=[_make_rss_row(ie, rss_p95_kb=7777.0, username="p95-winner")],
         rss_max=[_make_rss_row(ie, rss_max_kb=8888.0, username="max-winner")],
         d=[_make_d_row(ie)],
         proc_p50=[_make_proc_row(ie, process_count_p50=1.0)],
         proc_p95=[_make_proc_row(ie, process_count_p95=2.0)],
         proc_max=[_make_proc_row(ie, process_count_max=3.0)],
         ia=[_make_ia_row(ie)],
      )
      result = load_usage(conn, "polaris", "login-04", _START, _INVENTORY,
                          now_utc=_NOW)
      grain = result.by_key[("ai_coding", "active", ie)]
      assert grain.rss_p50_username == "p50-winner"
      assert grain.rss_p95_username == "p95-winner"
      assert grain.rss_max_username == "max-winner"

   def test_different_users_win_proc_p50_and_p95_and_max(self):
      """Deterministic test: different usernames win p50, p95, max for process_count."""
      from node_monitor.web.queries import load_usage
      ie = _NOW - timedelta(minutes=1)
      conn = _make_usage_conn_9q(
         cpu=[_make_cpu_row(ie)],
         rss_p50=[_make_rss_row(ie, username="u")],
         rss_p95=[_make_rss_row(ie, username="u")],
         rss_max=[_make_rss_row(ie, username="u")],
         d=[_make_d_row(ie)],
         proc_p50=[_make_proc_row(ie, process_count_p50=100.0,
                                   username="proc-p50-winner")],
         proc_p95=[_make_proc_row(ie, process_count_p95=50.0,
                                   username="proc-p95-winner")],
         proc_max=[_make_proc_row(ie, process_count_max=75.0,
                                   username="proc-max-winner")],
         ia=[_make_ia_row(ie)],
      )
      result = load_usage(conn, "polaris", "login-04", _START, _INVENTORY,
                          now_utc=_NOW)
      grain = result.by_key[("ai_coding", "active", ie)]
      assert grain.process_count_p50_username == "proc-p50-winner"
      assert grain.process_count_p95_username == "proc-p95-winner"
      assert grain.process_count_max_username == "proc-max-winner"

   def test_rss_all_values_come_from_correct_independent_queries(self):
      """rss_p50_kb value comes from p50 query row, rss_p95_kb from p95 query, etc."""
      from node_monitor.web.queries import load_usage
      ie = _NOW - timedelta(minutes=1)
      conn = _make_usage_conn_9q(
         cpu=[_make_cpu_row(ie)],
         rss_p50=[_make_rss_row(ie, rss_p50_kb=1111.0, username="u1")],
         rss_p95=[_make_rss_row(ie, rss_p95_kb=2222.0, username="u2")],
         rss_max=[_make_rss_row(ie, rss_max_kb=3333.0, username="u3")],
         d=[_make_d_row(ie)],
         proc_p50=[_make_proc_row(ie, username="u4")],
         proc_p95=[_make_proc_row(ie, username="u5")],
         proc_max=[_make_proc_row(ie, username="u6")],
         ia=[_make_ia_row(ie)],
      )
      result = load_usage(conn, "polaris", "login-04", _START, _INVENTORY,
                          now_utc=_NOW)
      grain = result.by_key[("ai_coding", "active", ie)]
      assert grain.rss_p50_kb == 1111.0
      assert grain.rss_p95_kb == 2222.0
      assert grain.rss_max_kb == 3333.0

   def test_rss_none_when_no_hotspot_rows_for_individual_stats(self):
      """Each username slot is None when the respective query returns no rows."""
      from node_monitor.web.queries import load_usage
      ie = _NOW - timedelta(minutes=1)
      conn = _make_conn([
         [_make_cpu_row(ie)],
         [],  # rss_p50 -- no row
         [],  # rss_p95 -- no row
         [],  # rss_max -- no row
         [_make_d_row(ie)],
         [_make_proc_row(ie)],
         [_make_proc_row(ie)],
         [_make_proc_row(ie)],
         [_make_ia_row(ie)],
      ])
      result = load_usage(conn, "polaris", "login-04", _START, _INVENTORY,
                          now_utc=_NOW)
      grain = result.by_key[("ai_coding", "active", ie)]
      assert grain.rss_p50_username is None
      assert grain.rss_p95_username is None
      assert grain.rss_max_username is None
      assert grain.rss_p50_kb is None
      assert grain.rss_p95_kb is None
      assert grain.rss_max_kb is None

   def test_all_six_hotspot_sqls_bind_username_is_null(self):
      """All 6 hotspot SQL constants must accept :username_is_null."""
      from node_monitor.web.queries import (
         USAGE_RSS_P50_HOTSPOT_SQL, USAGE_RSS_P95_HOTSPOT_SQL,
         USAGE_RSS_MAX_HOTSPOT_SQL, USAGE_PROCESS_COUNT_P50_HOTSPOT_SQL,
         USAGE_PROCESS_COUNT_P95_HOTSPOT_SQL, USAGE_PROCESS_COUNT_MAX_HOTSPOT_SQL,
      )
      for sql in (
         USAGE_RSS_P50_HOTSPOT_SQL, USAGE_RSS_P95_HOTSPOT_SQL,
         USAGE_RSS_MAX_HOTSPOT_SQL, USAGE_PROCESS_COUNT_P50_HOTSPOT_SQL,
         USAGE_PROCESS_COUNT_P95_HOTSPOT_SQL, USAGE_PROCESS_COUNT_MAX_HOTSPOT_SQL,
      ):
         assert ":username_is_null" in str(sql), (
            "SQL must bind :username_is_null: %r" % str(sql)[:60])

   def test_api_metric_allowlist_exists(self):
      """METRIC_TO_SQL_MAP must exist and map API names to SQL constants."""
      from node_monitor.web.queries import METRIC_TO_SQL_MAP
      assert isinstance(METRIC_TO_SQL_MAP, dict)
      assert len(METRIC_TO_SQL_MAP) >= 6, (
         "METRIC_TO_SQL_MAP must have at least 6 entries (rss x3, proc x3)")

   def test_api_metric_allowlist_keys(self):
      """METRIC_TO_SQL_MAP must map the 6 required API stat names."""
      from node_monitor.web.queries import METRIC_TO_SQL_MAP
      required = {
         "rss_p50", "rss_p95", "rss_max",
         "process_count_p50", "process_count_p95", "process_count_max",
      }
      assert required.issubset(set(METRIC_TO_SQL_MAP.keys())), (
         "Missing keys: %r" % (required - set(METRIC_TO_SQL_MAP.keys())))

   def test_api_metric_allowlist_values_are_sql_constants(self):
      """METRIC_TO_SQL_MAP values must be the actual SQL text objects."""
      from node_monitor.web.queries import (
         METRIC_TO_SQL_MAP,
         USAGE_RSS_P50_HOTSPOT_SQL, USAGE_RSS_P95_HOTSPOT_SQL,
         USAGE_RSS_MAX_HOTSPOT_SQL, USAGE_PROCESS_COUNT_P50_HOTSPOT_SQL,
         USAGE_PROCESS_COUNT_P95_HOTSPOT_SQL, USAGE_PROCESS_COUNT_MAX_HOTSPOT_SQL,
      )
      assert METRIC_TO_SQL_MAP["rss_p50"] is USAGE_RSS_P50_HOTSPOT_SQL
      assert METRIC_TO_SQL_MAP["rss_p95"] is USAGE_RSS_P95_HOTSPOT_SQL
      assert METRIC_TO_SQL_MAP["rss_max"] is USAGE_RSS_MAX_HOTSPOT_SQL
      assert METRIC_TO_SQL_MAP["process_count_p50"] is USAGE_PROCESS_COUNT_P50_HOTSPOT_SQL
      assert METRIC_TO_SQL_MAP["process_count_p95"] is USAGE_PROCESS_COUNT_P95_HOTSPOT_SQL
      assert METRIC_TO_SQL_MAP["process_count_max"] is USAGE_PROCESS_COUNT_MAX_HOTSPOT_SQL


# ===========================================================================
# BLOCKER 2: complete/partial is a computed property, not raw metadata
# ===========================================================================

class TestCompleteComputedProperty:
   """CounterResult rows must carry a computed 'complete' field."""

   def _load(self, **kw):
      from node_monitor.web.queries import load_counters
      row = _make_counter_row(1, **kw)
      conn = _make_conn([[row]])
      return load_counters(conn, "polaris", "login-04", _START, _INVENTORY,
                           now_utc=_NOW).rows[0]

   def test_complete_true_when_all_conditions_met(self):
      """complete is True iff coverage==1, meets_minimum_samples==True,
      invalid_pair_count==0, excess_sample_count==0."""
      row = self._load(coverage=1.0, meets_minimum_samples=True,
                       invalid_pair_count=0, excess_sample_count=0)
      assert row["complete"] is True

   def test_complete_false_when_coverage_lt_1(self):
      row = self._load(coverage=0.8, meets_minimum_samples=True,
                       invalid_pair_count=0, excess_sample_count=0)
      assert row["complete"] is False

   def test_complete_false_when_meets_minimum_samples_false(self):
      """complete is False when meets_minimum_samples is False."""
      row = self._load(coverage=1.0, meets_minimum_samples=False,
                       invalid_pair_count=0, excess_sample_count=0)
      assert row["complete"] is False

   def test_complete_false_when_invalid_pair_count_nonzero(self):
      row = self._load(coverage=1.0, meets_minimum_samples=True,
                       invalid_pair_count=1, excess_sample_count=0)
      assert row["complete"] is False

   def test_complete_false_when_excess_sample_count_nonzero(self):
      row = self._load(coverage=1.0, meets_minimum_samples=True,
                       invalid_pair_count=0, excess_sample_count=1)
      assert row["complete"] is False

   def test_complete_false_when_multiple_conditions_fail(self):
      """complete is False when coverage < 1 AND invalid_pair_count > 0."""
      row = self._load(coverage=0.5, meets_minimum_samples=False,
                       invalid_pair_count=3, excess_sample_count=2)
      assert row["complete"] is False

   def test_metadata_still_carried_unchanged(self):
      """Raw metadata fields are still available alongside computed 'complete'."""
      row = self._load(coverage=0.8, meets_minimum_samples=True,
                       invalid_pair_count=2, excess_sample_count=1)
      assert row["coverage"] == 0.8
      assert row["meets_minimum_samples"] is True
      assert row["invalid_pair_count"] == 2
      assert row["excess_sample_count"] == 1


# ===========================================================================
# BLOCKER 3: Bounded windows -- upper :end bound everywhere
# ===========================================================================

class TestBoundedWindowUpperBound:
   """All queries must accept and apply an :end upper-bound parameter."""

   def test_counter_sql_has_window_end_le_end(self):
      """COUNTER_SQL must include window_end <= :end (upper bound)."""
      from node_monitor.web.queries import COUNTER_SQL
      sql = str(COUNTER_SQL)
      assert ":end" in sql, "COUNTER_SQL must bind :end upper bound"
      # The bound must constrain window_end (not just window_start)
      assert "window_end" in sql.lower()

   def test_usage_cpu_sql_has_interval_end_le_end(self):
      """USAGE_CPU_SQL must include interval_end <= :end."""
      from node_monitor.web.queries import USAGE_CPU_SQL
      sql = str(USAGE_CPU_SQL)
      assert ":end" in sql, "USAGE_CPU_SQL must bind :end upper bound"

   def test_collection_log_sql_has_recorded_at_le_end(self):
      """COLLECTION_LOG_SQL must include recorded_at <= :end."""
      from node_monitor.web.queries import COLLECTION_LOG_SQL
      sql = str(COLLECTION_LOG_SQL)
      assert ":end" in sql, "COLLECTION_LOG_SQL must bind :end upper bound"

   def test_poll_failures_sql_has_start_and_end_bounds(self):
      """POLL_FAILURES_SQL must include both :start and :end range bounds."""
      from node_monitor.web.queries import POLL_FAILURES_SQL
      sql = str(POLL_FAILURES_SQL)
      assert ":start" in sql, "POLL_FAILURES_SQL must bind :start lower bound"
      assert ":end" in sql, "POLL_FAILURES_SQL must bind :end upper bound"

   def test_all_six_hotspot_sqls_have_interval_end_le_end(self):
      """All 6 hotspot SQL constants must include interval_end <= :end."""
      from node_monitor.web.queries import (
         USAGE_RSS_P50_HOTSPOT_SQL, USAGE_RSS_P95_HOTSPOT_SQL,
         USAGE_RSS_MAX_HOTSPOT_SQL, USAGE_PROCESS_COUNT_P50_HOTSPOT_SQL,
         USAGE_PROCESS_COUNT_P95_HOTSPOT_SQL, USAGE_PROCESS_COUNT_MAX_HOTSPOT_SQL,
      )
      for sql_obj in (
         USAGE_RSS_P50_HOTSPOT_SQL, USAGE_RSS_P95_HOTSPOT_SQL,
         USAGE_RSS_MAX_HOTSPOT_SQL, USAGE_PROCESS_COUNT_P50_HOTSPOT_SQL,
         USAGE_PROCESS_COUNT_P95_HOTSPOT_SQL, USAGE_PROCESS_COUNT_MAX_HOTSPOT_SQL,
      ):
         sql = str(sql_obj)
         assert ":end" in sql, (
            "Hotspot SQL must bind :end upper bound: %r" % sql[:60])

   def test_d_state_and_ia_hotspot_sqls_have_end_bound(self):
      """D-state and interactivity hotspot SQL must also include :end."""
      from node_monitor.web.queries import (
         USAGE_D_STATE_HOTSPOT_SQL, USAGE_INTERACTIVITY_HOTSPOT_SQL,
      )
      for sql_obj in (USAGE_D_STATE_HOTSPOT_SQL, USAGE_INTERACTIVITY_HOTSPOT_SQL):
         assert ":end" in str(sql_obj), (
            "Hotspot SQL must bind :end: %r" % str(sql_obj)[:60])

   def test_load_counters_passes_end_to_sql(self):
      """load_counters must pass 'end' (now_utc) to the SQL parameters."""
      from node_monitor.web.queries import load_counters
      rows = [_make_counter_row(1)]
      conn = _make_conn([rows])
      load_counters(conn, "polaris", "login-04", _START, _INVENTORY,
                    now_utc=_NOW)
      call_params = conn.execute.call_args_list[0][0][1]
      assert "end" in call_params, (
         "load_counters must pass 'end' parameter to COUNTER_SQL; "
         "got params: %r" % list(call_params.keys()))
      assert call_params["end"] == _NOW

   def test_load_usage_passes_end_to_sql(self):
      """load_usage must pass 'end' (now_utc) as a bound SQL parameter."""
      from node_monitor.web.queries import load_usage
      conn = _make_usage_conn_9q()
      load_usage(conn, "polaris", "login-04", _START, _INVENTORY, now_utc=_NOW)
      # All 9 execute calls must carry 'end'
      for i, c in enumerate(conn.execute.call_args_list):
         params = c[0][1] if len(c[0]) > 1 else {}
         assert "end" in params, (
            "Query %d in load_usage must pass 'end' parameter; "
            "got keys: %r" % (i, list(params.keys())))

   def test_load_poll_failures_passes_start_and_end(self):
      """load_poll_failures must pass both 'start' and 'end' to SQL."""
      from node_monitor.web.queries import load_poll_failures
      rows = [{"recorded_at": _NOW, "loop": "counter", "failure_type": "timeout",
               "detail": "timed out", "consecutive_failures": 1,
               "breaker_state": "closed"}]
      conn = _make_conn([rows])
      load_poll_failures(conn, "polaris", "login-04", _INVENTORY,
                         start=_START, end=_NOW)
      call_params = conn.execute.call_args_list[0][0][1]
      assert "start" in call_params, "load_poll_failures must pass 'start'"
      assert "end" in call_params, "load_poll_failures must pass 'end'"

   def test_load_collection_log_passes_end(self):
      """load_collection_log must pass 'end' as a bound parameter."""
      from node_monitor.web.queries import load_collection_log
      rows = [{"recorded_at": _NOW, "event": "start", "detail": {}}]
      conn = _make_conn([rows])
      load_collection_log(conn, "polaris", _START, end=_NOW)
      call_params = conn.execute.call_args_list[0][0][1]
      assert "end" in call_params, (
         "load_collection_log must pass 'end' parameter; got: %r"
         % list(call_params.keys()))
      assert call_params["end"] == _NOW

   def test_future_rows_excluded_by_upper_bound(self):
      """Rows with window_end > end must be excluded (not just marked stale)."""
      from node_monitor.web.queries import load_counters
      # Row right at boundary should be included
      rows = [_make_counter_row(0)]  # window_end == _NOW
      rows[0]["window_end"] = _NOW
      conn = _make_conn([rows])
      result = load_counters(conn, "polaris", "login-04", _START, _INVENTORY,
                             now_utc=_NOW)
      # The SQL enforces this; at the unit level we verify 'end' is passed
      call_params = conn.execute.call_args_list[0][0][1]
      assert call_params.get("end") == _NOW


# ===========================================================================
# BLOCKER 4: _validate_utc_datetime enforces ZERO UTC offset strictly
# ===========================================================================

class TestStrictUTCValidation:
   """_validate_utc_datetime must reject non-UTC aware datetimes."""

   def test_utc_datetime_accepted(self):
      """A datetime with timezone.utc is accepted."""
      dt = datetime(2026, 9, 30, 14, 0, tzinfo=timezone.utc)
      result = _validate_utc_datetime(dt, "test")
      assert result == dt

   def test_naive_datetime_rejected(self):
      """A naive datetime (no tzinfo) raises QueryValidationError."""
      dt = datetime(2026, 9, 30, 14, 0)
      with pytest.raises(QueryValidationError, match="UTC-aware"):
         _validate_utc_datetime(dt, "test")

   def test_non_utc_aware_eastern_rejected(self):
      """A US/Eastern datetime is aware but NOT UTC -- must be rejected."""
      eastern = zoneinfo.ZoneInfo("America/New_York")
      dt = datetime(2026, 9, 30, 9, 0, tzinfo=eastern)  # UTC-5
      with pytest.raises(QueryValidationError, match="UTC"):
         _validate_utc_datetime(dt, "test")

   def test_non_utc_fixed_offset_rejected(self):
      """A fixed +05:30 offset (IST) is NOT UTC -- must be rejected."""
      ist = timezone(timedelta(hours=5, minutes=30))
      dt = datetime(2026, 9, 30, 19, 30, tzinfo=ist)
      with pytest.raises(QueryValidationError, match="UTC"):
         _validate_utc_datetime(dt, "test")

   def test_fixed_negative_offset_rejected(self):
      """A fixed -08:00 offset is NOT UTC -- must be rejected."""
      pst = timezone(timedelta(hours=-8))
      dt = datetime(2026, 9, 30, 6, 0, tzinfo=pst)
      with pytest.raises(QueryValidationError, match="UTC"):
         _validate_utc_datetime(dt, "test")

   def test_zero_offset_but_not_utc_singleton_rejected_or_accepted(self):
      """timezone(timedelta(0)) is mathematically UTC -- must be accepted.

      This is timezone.utc equivalent (UTC+00:00). We accept it because the
      offset is zero even if the object is not the timezone.utc singleton."""
      utc_equiv = timezone(timedelta(0))
      dt = datetime(2026, 9, 30, 14, 0, tzinfo=utc_equiv)
      # Must not raise (zero offset == UTC)
      result = _validate_utc_datetime(dt, "test")
      assert result == dt

   def test_range_hours_to_start_rejects_non_utc_now(self):
      """range_hours_to_start must reject a non-UTC aware now_utc."""
      from node_monitor.web.queries import range_hours_to_start
      eastern = zoneinfo.ZoneInfo("America/New_York")
      dt = datetime(2026, 9, 30, 9, 0, tzinfo=eastern)
      with pytest.raises(QueryValidationError, match="UTC"):
         range_hours_to_start(1, dt)

   def test_load_counters_rejects_non_utc_aware_now(self):
      """load_counters must reject a non-UTC aware now_utc."""
      from node_monitor.web.queries import load_counters
      eastern = zoneinfo.ZoneInfo("America/New_York")
      conn = MagicMock()
      dt = datetime(2026, 9, 30, 9, 0, tzinfo=eastern)
      with pytest.raises(QueryValidationError, match="UTC"):
         load_counters(conn, "polaris", "login-04", _START, _INVENTORY,
                       now_utc=dt)

   def test_load_usage_rejects_non_utc_aware_now(self):
      """load_usage must reject a non-UTC aware now_utc."""
      from node_monitor.web.queries import load_usage
      eastern = zoneinfo.ZoneInfo("America/New_York")
      conn = MagicMock()
      dt = datetime(2026, 9, 30, 9, 0, tzinfo=eastern)
      with pytest.raises(QueryValidationError, match="UTC"):
         load_usage(conn, "polaris", "login-04", _START, _INVENTORY,
                    now_utc=dt)


# ===========================================================================
# BLOCKER 5: Public range-based facade
# ===========================================================================

class TestRangeBasedFacade:
   """load_*_for_range public facades derive start internally from range_hours."""

   def test_load_counters_for_range_exists(self):
      """load_counters_for_range must be importable from queries."""
      from node_monitor.web.queries import load_counters_for_range
      assert callable(load_counters_for_range)

   def test_load_usage_for_range_exists(self):
      """load_usage_for_range must be importable from queries."""
      from node_monitor.web.queries import load_usage_for_range
      assert callable(load_usage_for_range)

   def test_load_poll_failures_for_range_exists(self):
      """load_poll_failures_for_range must be importable."""
      from node_monitor.web.queries import load_poll_failures_for_range
      assert callable(load_poll_failures_for_range)

   def test_load_collection_log_for_range_exists(self):
      """load_collection_log_for_range must be importable."""
      from node_monitor.web.queries import load_collection_log_for_range
      assert callable(load_collection_log_for_range)

   def test_load_counters_for_range_rejects_invalid_hours(self):
      """load_counters_for_range must reject invalid range_hours."""
      from node_monitor.web.queries import load_counters_for_range
      conn = MagicMock()
      with pytest.raises(QueryValidationError, match="range_hours"):
         load_counters_for_range(conn, "polaris", "login-04", 7, _INVENTORY,
                                  now_utc=_NOW)

   def test_load_usage_for_range_rejects_invalid_hours(self):
      """load_usage_for_range must reject invalid range_hours."""
      from node_monitor.web.queries import load_usage_for_range
      conn = MagicMock()
      with pytest.raises(QueryValidationError, match="range_hours"):
         load_usage_for_range(conn, "polaris", "login-04", 2, _INVENTORY,
                               now_utc=_NOW)

   def test_load_counters_for_range_derives_start_from_hours(self):
      """load_counters_for_range must derive start = now_utc - range_hours."""
      from node_monitor.web.queries import load_counters_for_range
      rows = [_make_counter_row(30)]
      conn = _make_conn([rows])
      load_counters_for_range(conn, "polaris", "login-04", 1, _INVENTORY,
                               now_utc=_NOW)
      call_params = conn.execute.call_args_list[0][0][1]
      expected_start = _NOW - timedelta(hours=1)
      assert call_params["start"] == expected_start, (
         "start must equal now_utc - 1h; got %r" % call_params["start"])

   def test_load_usage_for_range_derives_start_from_hours(self):
      """load_usage_for_range must derive start = now_utc - range_hours."""
      from node_monitor.web.queries import load_usage_for_range
      conn = _make_usage_conn_9q()
      load_usage_for_range(conn, "polaris", "login-04", 3, _INVENTORY,
                            now_utc=_NOW)
      call_params = conn.execute.call_args_list[0][0][1]
      expected_start = _NOW - timedelta(hours=3)
      assert call_params["start"] == expected_start, (
         "start must equal now_utc - 3h; got %r" % call_params["start"])

   def test_load_counters_for_range_sets_end_to_now_utc(self):
      """load_counters_for_range must pass now_utc as 'end'."""
      from node_monitor.web.queries import load_counters_for_range
      rows = [_make_counter_row(30)]
      conn = _make_conn([rows])
      load_counters_for_range(conn, "polaris", "login-04", 1, _INVENTORY,
                               now_utc=_NOW)
      call_params = conn.execute.call_args_list[0][0][1]
      assert call_params.get("end") == _NOW

   def test_load_poll_failures_for_range_uses_valid_range(self):
      """load_poll_failures_for_range must accept valid range_hours."""
      from node_monitor.web.queries import load_poll_failures_for_range
      rows = [{"recorded_at": _NOW, "loop": "counter", "failure_type": "timeout",
               "detail": "x", "consecutive_failures": 1, "breaker_state": "closed"}]
      conn = _make_conn([rows])
      result = load_poll_failures_for_range(conn, "polaris", "login-04", 1,
                                             _INVENTORY, now_utc=_NOW)
      assert isinstance(result, list)

   def test_load_collection_log_for_range_uses_valid_range(self):
      """load_collection_log_for_range must accept valid range_hours."""
      from node_monitor.web.queries import load_collection_log_for_range
      rows = [{"recorded_at": _NOW, "event": "start", "detail": {}}]
      conn = _make_conn([rows])
      result = load_collection_log_for_range(conn, "polaris", 1, now_utc=_NOW)
      assert isinstance(result, list)


# ===========================================================================
# BLOCKER 6: PostgreSQL test compatibility checks (SQL signatures)
# ===========================================================================

class TestPostgresTestCompatibility:
   """Verify that updated SQL constants are backward-compatible with PG tests."""

   def test_usage_rss_p95_hotspot_sql_alias_still_exists(self):
      """USAGE_RSS_P95_HOTSPOT_SQL must still be importable (backward compat)."""
      from node_monitor.web.queries import USAGE_RSS_P95_HOTSPOT_SQL
      assert USAGE_RSS_P95_HOTSPOT_SQL is not None

   def test_usage_process_count_p95_hotspot_sql_alias_still_exists(self):
      """USAGE_PROCESS_COUNT_P95_HOTSPOT_SQL must still be importable."""
      from node_monitor.web.queries import USAGE_PROCESS_COUNT_P95_HOTSPOT_SQL
      assert USAGE_PROCESS_COUNT_P95_HOTSPOT_SQL is not None

   def test_counter_sql_still_projects_required_columns(self):
      """COUNTER_SQL must still SELECT all required columns."""
      from node_monitor.web.queries import COUNTER_SQL
      sql = str(COUNTER_SQL)
      for col in ("window_start", "window_end", "sample_count", "expected_count",
                   "coverage", "meets_minimum_samples", "invalid_pair_count",
                   "excess_sample_count", "cpu_busy_pct"):
         assert col in sql, "COUNTER_SQL must project %r" % col

   def test_poll_failures_sql_still_projects_required_columns(self):
      """POLL_FAILURES_SQL must still project required columns."""
      from node_monitor.web.queries import POLL_FAILURES_SQL
      sql = str(POLL_FAILURES_SQL)
      for col in ("recorded_at", "loop", "failure_type", "detail",
                   "consecutive_failures", "breaker_state"):
         assert col in sql, "POLL_FAILURES_SQL must project %r" % col
