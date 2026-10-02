"""Unit tests for node_monitor.web.queries -- no PostgreSQL required.

Tests cover:
  - Production JSONB shapes from writer/collector contracts (Step 1).
  - Range/node/username/quality/aggregation semantics (Step 2).
  - Parameter validation: exact ranges, inventory-based node validation,
    256-byte UTF-8 username boundary.
  - SQL constant properties: no identifier binding, no SELECT *.
  - Counter row cap: 1440 accepted, 1441 rejected with QueryBoundsError.
  - Counter SQL uses LIMIT sentinel (COUNTER_ROW_LIMIT+1) to cap DB output.
  - LUSTRE_PEAK_SUM_SOURCE = "max_sum" constant and API label "peak-sum".
  - UsageResult.by_key key shape and grain attribute semantics.
  - Non-additive hotspots: rss p50/p95/max, process_count p50/p95/max,
    interactivity_fraction with username, d_state_fraction with username.
  - Loopback exclusion is enforced by writer (not re-checked in query layer).
  - Gaps are not zero-filled (two distinct interval_end values tested).
  - Staleness thresholds including future-timestamp rejection.
  - Collection log is system-level (no node filter) with explicit LIMIT bound.
  - Poll failures capped at MAX_POLL_FAILURES = 10.
  - CounterResult carries invalid_pair_count, excess_sample_count.
  - UTC-aware validation: naive datetimes and non-datetimes raise QueryValidationError.
  - Public range_hours_to_start() helper enforces VALID_RANGES_HOURS.
  - SQL aggregation proof: USAGE_CPU_SQL contains SUM and GROUP BY exact grain.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, call

import pytest

from node_monitor.web.queries import (
   COLLECTION_LOG_LIMIT,
   COLLECTION_LOG_SQL,
   COUNTER_ROW_LIMIT,
   COUNTER_STALENESS_SECONDS,
   COUNTER_SQL,
   LUSTRE_PEAK_SUM_API_LABEL,
   LUSTRE_PEAK_SUM_SOURCE,
   MAX_POLL_FAILURES,
   MAX_USERNAME_BYTES,
   USAGE_STALENESS_SECONDS,
   USAGE_CPU_SQL,
   VALID_RANGES_HOURS,
   CounterResult,
   QueryBoundsError,
   QueryValidationError,
   UsageResult,
   _validate_node,
   _validate_range_hours,
   _validate_username,
   load_collection_log,
   load_counters,
   load_poll_failures,
   load_usage,
   range_hours_to_start,
)
from tests.web.fixtures import (
   production_counter_row,
   production_usage_row,
)

# ---------------------------------------------------------------------------
# Shared fixtures / helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 9, 30, 14, 0, tzinfo=timezone.utc)
_START = _NOW - timedelta(hours=1)
_INVENTORY = frozenset({"login-04", "login-05"})
_END = _NOW - timedelta(minutes=1)


def _make_conn(rows_per_call):
   """Return a minimal mock connection whose .execute().mappings() returns
   successive lists from rows_per_call (a list of lists)."""
   conn = MagicMock()
   calls = iter(rows_per_call)

   def _execute(stmt, params=None):
      result = MagicMock()
      result.mappings.return_value = next(calls)
      return result

   conn.execute.side_effect = _execute
   return conn


# ---------------------------------------------------------------------------
# Step 1: production JSONB shape assertions
# ---------------------------------------------------------------------------

def test_production_shaped_counter_metric_keys():
   counter = production_counter_row()
   assert set(counter["cpu_busy_pct"]) == {"p50", "p95", "max"}
   assert set(counter["network_rates"]["eth0"]["rx_bytes_per_sec"]) == {
      "p50", "p95", "max"}
   assert set(counter["network_rates"]["eth0"]["tx_bytes_per_sec"]) == {
      "p50", "p95", "max"}
   assert set(counter["lustre_md_summary"]["open"]) == {
      "p50_sum", "p95_sum", "max_sum", "target_count"}


def test_production_shaped_usage_metric_keys():
   usage = production_usage_row()
   assert set(usage["process_count"]) == {"p50", "p95", "max"}
   assert set(usage["rss_kb"]) == {"p50", "p95", "max"}


def test_production_counter_loopback_excluded():
   """Loopback 'lo' must not appear in stored network_rates (writer contract)."""
   counter = production_counter_row()
   assert "lo" not in counter["network_rates"]


def test_production_lustre_max_sum_is_stored_key():
   """LUSTRE_PEAK_SUM_SOURCE must be a key inside lustre_md_summary values."""
   counter = production_counter_row()
   for op_stats in counter["lustre_md_summary"].values():
      assert LUSTRE_PEAK_SUM_SOURCE in op_stats, (
         "LUSTRE_PEAK_SUM_SOURCE=%r not found in %r" % (LUSTRE_PEAK_SUM_SOURCE, op_stats))


def test_lustre_peak_sum_constants():
   assert LUSTRE_PEAK_SUM_SOURCE == "max_sum"
   assert LUSTRE_PEAK_SUM_API_LABEL == "peak-sum"


# ---------------------------------------------------------------------------
# Step 2a: range validation
# ---------------------------------------------------------------------------

def test_valid_ranges_are_exactly_five():
   assert VALID_RANGES_HOURS == frozenset({1, 3, 6, 12, 24})


@pytest.mark.parametrize("hours", [1, 3, 6, 12, 24])
def test_valid_range_accepted(hours):
   assert _validate_range_hours(hours) == hours


@pytest.mark.parametrize("hours", [0, 2, 7, 13, 25, 48, -1, 1.5, "1h"])
def test_invalid_range_rejected(hours):
   with pytest.raises(QueryValidationError, match="range_hours"):
      _validate_range_hours(hours)


# ---------------------------------------------------------------------------
# Step 2a-ext: range_hours_to_start public helper
# ---------------------------------------------------------------------------

def test_range_hours_to_start_returns_correct_datetime():
   """range_hours_to_start(1, now_utc) returns now_utc - 1h."""
   result = range_hours_to_start(1, _NOW)
   assert result == _NOW - timedelta(hours=1)


@pytest.mark.parametrize("hours", [1, 3, 6, 12, 24])
def test_range_hours_to_start_accepts_valid_ranges(hours):
   result = range_hours_to_start(hours, _NOW)
   assert result == _NOW - timedelta(hours=hours)


@pytest.mark.parametrize("hours", [0, 2, 7, 48])
def test_range_hours_to_start_rejects_invalid_ranges(hours):
   with pytest.raises(QueryValidationError, match="range_hours"):
      range_hours_to_start(hours, _NOW)


def test_range_hours_to_start_rejects_naive_now_utc():
   """now_utc must be UTC-aware; naive raises QueryValidationError."""
   naive = datetime(2026, 9, 30, 14, 0)
   with pytest.raises(QueryValidationError, match="UTC-aware"):
      range_hours_to_start(1, naive)


def test_range_hours_to_start_rejects_non_datetime():
   """now_utc must be a datetime; non-datetime raises QueryValidationError."""
   with pytest.raises(QueryValidationError, match="UTC-aware"):
      range_hours_to_start(1, "2026-09-30T14:00:00Z")


# ---------------------------------------------------------------------------
# Step 2b: inventory-based node validation
# ---------------------------------------------------------------------------

def test_known_node_accepted():
   assert _validate_node("login-04", _INVENTORY) == "login-04"


def test_unknown_node_rejected():
   with pytest.raises(QueryValidationError, match="not in the known inventory"):
      _validate_node("login-99", _INVENTORY)


def test_empty_inventory_rejects_all_nodes():
   with pytest.raises(QueryValidationError):
      _validate_node("login-04", frozenset())


# ---------------------------------------------------------------------------
# Step 2c: username validation -- 256-byte UTF-8 boundary
# ---------------------------------------------------------------------------

def test_username_none_passes():
   assert _validate_username(None) is None


def test_username_ascii_accepted():
   assert _validate_username("alice") == "alice"


def test_username_exactly_256_bytes_accepted():
   # 256 ASCII chars = 256 UTF-8 bytes.
   username = "a" * 256
   assert _validate_username(username) == username


def test_username_257_bytes_rejected():
   username = "a" * 257
   with pytest.raises(QueryValidationError, match="256 UTF-8 bytes"):
      _validate_username(username)


def test_username_multibyte_utf8_boundary():
   # Each '€' is 3 UTF-8 bytes.  85 * 3 = 255 bytes <= 256 OK.
   ok = "€" * 85
   assert len(ok.encode("utf-8")) == 255
   assert _validate_username(ok) == ok
   # 86 * 3 = 258 > 256 -- must reject.
   over = "€" * 86
   with pytest.raises(QueryValidationError, match="256 UTF-8 bytes"):
      _validate_username(over)


def test_empty_username_rejected():
   with pytest.raises(QueryValidationError, match="empty string"):
      _validate_username("")


def test_hostile_username_is_just_bound_data(monkeypatch):
   """A hostile username string is accepted as data (bound value), not SQL."""
   hostile = "x' OR true; --%00\n"
   # Must not raise -- validation only checks length/type, not content.
   result = _validate_username(hostile)
   assert result == hostile


# ---------------------------------------------------------------------------
# Step 2d: counter row cap
# ---------------------------------------------------------------------------

def _make_counter_row(window_end_offset_minutes):
   """Return a minimal counter row dict for the cap test."""
   we = _NOW - timedelta(minutes=window_end_offset_minutes)
   return {
      "window_start": we - timedelta(minutes=1),
      "window_end": we,
      "sample_count": 6, "expected_count": 6, "coverage": 1.0,
      "meets_minimum_samples": True,
      "invalid_pair_count": 0, "excess_sample_count": 0,
      "mem_available_kb": None, "cached_kb": None, "shmem_kb": None,
      "load1": None, "load5": None, "load15": None,
      "procs_running": None, "procs_total": None, "socket_count": None,
      "cpu_busy_pct": None, "network_rates": None, "lustre_md_summary": None,
   }


def test_counter_limit_accepts_1440():
   rows = [_make_counter_row(i) for i in range(1, 1441)]
   conn = _make_conn([rows])
   result = load_counters(conn, "polaris", "login-04", _START, _INVENTORY,
                          now_utc=_NOW)
   assert len(result.rows) == 1440


def test_counter_limit_rejects_1441():
   rows = [_make_counter_row(i) for i in range(1, 1442)]
   conn = _make_conn([rows])
   with pytest.raises(QueryBoundsError, match="counter row limit exceeded"):
      load_counters(conn, "polaris", "login-04", _START, _INVENTORY,
                    now_utc=_NOW)


# ---------------------------------------------------------------------------
# Step 2d-ext: counter SQL uses sentinel LIMIT (COUNTER_ROW_LIMIT+1)
# ---------------------------------------------------------------------------

def test_counter_sql_has_limit_clause():
   """COUNTER_SQL must contain a LIMIT clause to cap rows returned by DB."""
   sql_text = str(COUNTER_SQL).upper()
   assert "LIMIT" in sql_text, "COUNTER_SQL must contain LIMIT to prevent unbounded fetch"


def test_counter_sql_limit_is_parameterized():
   """COUNTER_SQL LIMIT must be a bound parameter, not a literal."""
   sql_text = str(COUNTER_SQL)
   assert ":limit" in sql_text.lower(), (
      "COUNTER_SQL LIMIT must be a bound parameter (e.g. :limit)")


def test_load_counters_passes_sentinel_limit_to_sql():
   """load_counters must pass COUNTER_ROW_LIMIT+1 as the SQL LIMIT sentinel."""
   rows = [_make_counter_row(1)]
   conn = _make_conn([rows])
   load_counters(conn, "polaris", "login-04", _START, _INVENTORY, now_utc=_NOW)
   call_params = conn.execute.call_args_list[0][0][1]
   assert call_params.get("limit") == COUNTER_ROW_LIMIT + 1, (
      "load_counters must pass COUNTER_ROW_LIMIT+1=%d as limit sentinel, got %r"
      % (COUNTER_ROW_LIMIT + 1, call_params.get("limit")))


# ---------------------------------------------------------------------------
# Step 2e: freshness thresholds -- counter
# ---------------------------------------------------------------------------

def test_counter_freshness_within_120s():
   we = _NOW - timedelta(seconds=119)
   row = {**_make_counter_row(0), "window_end": we}
   conn = _make_conn([[row]])
   result = load_counters(conn, "polaris", "login-04", _START, _INVENTORY,
                          now_utc=_NOW)
   assert result.is_fresh is True
   assert result.newest_window_end == we


def test_counter_stale_beyond_120s():
   we = _NOW - timedelta(seconds=121)
   row = {**_make_counter_row(0), "window_end": we}
   conn = _make_conn([[row]])
   result = load_counters(conn, "polaris", "login-04", _START, _INVENTORY,
                          now_utc=_NOW)
   assert result.is_fresh is False


def test_counter_exactly_120s_is_fresh():
   we = _NOW - timedelta(seconds=120)
   row = {**_make_counter_row(0), "window_end": we}
   conn = _make_conn([[row]])
   result = load_counters(conn, "polaris", "login-04", _START, _INVENTORY,
                          now_utc=_NOW)
   assert result.is_fresh is True


def test_counter_no_rows_is_not_fresh():
   conn = _make_conn([[]])
   result = load_counters(conn, "polaris", "login-04", _START, _INVENTORY,
                          now_utc=_NOW)
   assert result.is_fresh is False
   assert result.newest_window_end is None


def test_counter_future_window_end_is_not_fresh():
   """A window_end in the future (age < 0) must not be considered fresh."""
   we = _NOW + timedelta(seconds=1)
   row = {**_make_counter_row(0), "window_end": we}
   conn = _make_conn([[row]])
   result = load_counters(conn, "polaris", "login-04", _START, _INVENTORY,
                          now_utc=_NOW)
   assert result.is_fresh is False, (
      "A future window_end must not be fresh; age must satisfy 0 <= age <= threshold")


# ---------------------------------------------------------------------------
# Step 2e-ext: UTC validation at load_counters boundary
# ---------------------------------------------------------------------------

def test_load_counters_rejects_naive_now_utc():
   """load_counters must raise QueryValidationError for a naive now_utc."""
   conn = _make_conn([[]])
   naive = datetime(2026, 9, 30, 14, 0)
   with pytest.raises(QueryValidationError, match="UTC-aware"):
      load_counters(conn, "polaris", "login-04", _START, _INVENTORY,
                    now_utc=naive)


def test_load_counters_rejects_non_datetime_now_utc():
   """load_counters must raise QueryValidationError for a non-datetime now_utc."""
   conn = _make_conn([[]])
   with pytest.raises(QueryValidationError, match="UTC-aware"):
      load_counters(conn, "polaris", "login-04", _START, _INVENTORY,
                    now_utc="2026-09-30T14:00:00Z")


def test_load_counters_rejects_naive_start():
   """load_counters must raise QueryValidationError for a naive start."""
   conn = _make_conn([[]])
   naive_start = datetime(2026, 9, 30, 13, 0)
   with pytest.raises(QueryValidationError, match="UTC-aware"):
      load_counters(conn, "polaris", "login-04", naive_start, _INVENTORY,
                    now_utc=_NOW)


# ---------------------------------------------------------------------------
# Step 2f: usage staleness (1200 seconds)
# ---------------------------------------------------------------------------

def _make_cpu_row(interval_end=None, cpu_seconds=9.0):
   ie = interval_end or _END
   return {
      "interval_end": ie,
      "category": "ai_coding",
      "activity": "active",
      "cpu_seconds": cpu_seconds,
      "complete": True,
   }


def _make_rss_row(interval_end=None, rss_p50_kb=4000.0, rss_p95_kb=8000.0,
                   rss_max_kb=9000.0, username="largest-user"):
   ie = interval_end or _END
   return {
      "interval_end": ie,
      "category": "ai_coding",
      "activity": "active",
      "username": username,
      "rss_p50_kb": rss_p50_kb,
      "rss_p95_kb": rss_p95_kb,
      "rss_max_kb": rss_max_kb,
      "sample_count": 6, "expected_count": 6, "unmeasured_count": 0,
   }


def _make_d_row(interval_end=None, d_state_fraction=0.40, username="blocked-user"):
   ie = interval_end or _END
   return {
      "interval_end": ie,
      "category": "ai_coding",
      "activity": "active",
      "username": username,
      "d_state_fraction": d_state_fraction,
      "sample_count": 6, "expected_count": 6, "unmeasured_count": 0,
   }


def _make_proc_row(interval_end=None, process_count_p50=1.0,
                    process_count_p95=2.0, process_count_max=3.0,
                    username="largest-user"):
   ie = interval_end or _END
   return {
      "interval_end": ie,
      "category": "ai_coding",
      "activity": "active",
      "username": username,
      "process_count_p50": process_count_p50,
      "process_count_p95": process_count_p95,
      "process_count_max": process_count_max,
      "sample_count": 6, "expected_count": 6, "unmeasured_count": 0,
   }


def _make_interactivity_row(interval_end=None, interactivity_fraction=0.80,
                             username="interactive-user"):
   ie = interval_end or _END
   return {
      "interval_end": ie,
      "category": "ai_coding",
      "activity": "active",
      "username": username,
      "interactivity_fraction": interactivity_fraction,
      "sample_count": 6, "expected_count": 6, "unmeasured_count": 0,
   }


def _make_usage_conn(cpu=None, rss=None, d=None, proc=None, interactivity=None):
   """Build a mock conn returning the five query result lists in order:
   cpu, rss, d_state, process_count, interactivity."""
   return _make_conn([
      cpu if cpu is not None else [_make_cpu_row()],
      rss if rss is not None else [_make_rss_row()],
      d if d is not None else [_make_d_row()],
      proc if proc is not None else [_make_proc_row()],
      interactivity if interactivity is not None else [_make_interactivity_row()],
   ])


def test_usage_freshness_within_1200s():
   ie = _NOW - timedelta(seconds=1199)
   conn = _make_usage_conn(
      cpu=[_make_cpu_row(ie)], rss=[_make_rss_row(ie)],
      d=[_make_d_row(ie)], proc=[_make_proc_row(ie)],
      interactivity=[_make_interactivity_row(ie)],
   )
   result = load_usage(conn, "polaris", "login-04", _START, _INVENTORY,
                       now_utc=_NOW)
   assert result.is_fresh is True


def test_usage_stale_beyond_1200s():
   ie = _NOW - timedelta(seconds=1201)
   conn = _make_usage_conn(
      cpu=[_make_cpu_row(ie)], rss=[_make_rss_row(ie)],
      d=[_make_d_row(ie)], proc=[_make_proc_row(ie)],
      interactivity=[_make_interactivity_row(ie)],
   )
   result = load_usage(conn, "polaris", "login-04", _START, _INVENTORY,
                       now_utc=_NOW)
   assert result.is_fresh is False


def test_usage_no_rows_is_not_fresh():
   conn = _make_conn([[], [], [], [], []])
   result = load_usage(conn, "polaris", "login-04", _START, _INVENTORY,
                       now_utc=_NOW)
   assert result.is_fresh is False
   assert result.newest_interval_end is None


def test_usage_future_interval_end_is_not_fresh():
   """A interval_end in the future (age < 0) must not be considered fresh."""
   ie = _NOW + timedelta(seconds=1)
   conn = _make_usage_conn(
      cpu=[_make_cpu_row(ie)], rss=[_make_rss_row(ie)],
      d=[_make_d_row(ie)], proc=[_make_proc_row(ie)],
      interactivity=[_make_interactivity_row(ie)],
   )
   result = load_usage(conn, "polaris", "login-04", _START, _INVENTORY,
                       now_utc=_NOW)
   assert result.is_fresh is False, (
      "A future interval_end must not be fresh; "
      "age must satisfy 0 <= age <= threshold")


# ---------------------------------------------------------------------------
# Step 2f-ext: UTC validation at load_usage boundary
# ---------------------------------------------------------------------------

def test_load_usage_rejects_naive_now_utc():
   """load_usage must raise QueryValidationError for a naive now_utc."""
   conn = _make_conn([[], [], [], [], []])
   naive = datetime(2026, 9, 30, 14, 0)
   with pytest.raises(QueryValidationError, match="UTC-aware"):
      load_usage(conn, "polaris", "login-04", _START, _INVENTORY,
                 now_utc=naive)


def test_load_usage_rejects_non_datetime_now_utc():
   """load_usage must raise QueryValidationError for a non-datetime now_utc."""
   conn = _make_conn([[], [], [], [], []])
   with pytest.raises(QueryValidationError, match="UTC-aware"):
      load_usage(conn, "polaris", "login-04", _START, _INVENTORY,
                 now_utc="2026-09-30T14:00:00Z")


def test_load_usage_rejects_naive_start():
   """load_usage must raise QueryValidationError for a naive start."""
   conn = _make_conn([[], [], [], [], []])
   naive_start = datetime(2026, 9, 30, 13, 0)
   with pytest.raises(QueryValidationError, match="UTC-aware"):
      load_usage(conn, "polaris", "login-04", naive_start, _INVENTORY,
                 now_utc=_NOW)


# ---------------------------------------------------------------------------
# Step 2g: usage aggregation semantics
# ---------------------------------------------------------------------------

def test_unfiltered_usage_sums_cpu_and_attributes_hotspots():
   """Exact aggregation semantics:
   - cpu_seconds is the SUM across username grains (additive only).
   - rss_kb p95 hotspot and username come from the RSS hotspot query.
   - d_state_fraction and username come from the D-state hotspot query.
   """
   conn = _make_usage_conn(
      cpu=[_make_cpu_row(cpu_seconds=9.0)],
      rss=[_make_rss_row(rss_p95_kb=8000.0, username="largest-user")],
      d=[_make_d_row(d_state_fraction=0.40, username="blocked-user")],
      proc=[_make_proc_row(process_count_p95=2.0, username="largest-user")],
      interactivity=[_make_interactivity_row(interactivity_fraction=0.80,
                                              username="interactive-user")],
   )
   result = load_usage(conn, "polaris", "login-04", _START, _INVENTORY,
                       now_utc=_NOW)
   grain = result.by_key[("ai_coding", "active", _END)]
   assert grain.cpu_seconds == 9.0
   assert grain.rss_p95_kb == 8000.0
   assert grain.rss_p95_username == "largest-user"
   assert grain.d_state_fraction == 0.40
   assert grain.d_state_username == "blocked-user"


def test_usage_username_filter_is_bound_data_not_sql():
   """Username filter must be passed as a bound parameter, not concatenated."""
   hostile = "x' OR true; --%00\n"
   conn = _make_conn([[], [], [], [], []])
   # Must not raise (hostile string is valid data once length is checked).
   result = load_usage(conn, "polaris", "login-04", _START, _INVENTORY,
                       username=hostile, now_utc=_NOW)
   assert result.rows == []
   # Verify the hostile string was passed as a bound value (not embedded in SQL text).
   calls = conn.execute.call_args_list
   for c in calls:
      params = c[0][1] if len(c[0]) > 1 else c[1].get("parameters", {})
      if "username" in params:
         assert params["username"] == hostile, (
            "username must be a bound value, not embedded in SQL")


def test_gaps_are_not_zero_filled():
   """Two intervals with distinct interval_end values produce two grains;
   no phantom zero-filled rows appear between them."""
   ie1 = _NOW - timedelta(hours=2)
   ie2 = _NOW - timedelta(hours=1)
   conn = _make_usage_conn(
      cpu=[_make_cpu_row(ie1, cpu_seconds=1.0),
           _make_cpu_row(ie2, cpu_seconds=2.0)],
      rss=[_make_rss_row(ie1), _make_rss_row(ie2)],
      d=[_make_d_row(ie1), _make_d_row(ie2)],
      proc=[_make_proc_row(ie1), _make_proc_row(ie2)],
      interactivity=[_make_interactivity_row(ie1),
                     _make_interactivity_row(ie2)],
   )
   result = load_usage(conn, "polaris", "login-04", _START, _INVENTORY,
                       now_utc=_NOW)
   assert len(result.by_key) == 2
   g1 = result.by_key[("ai_coding", "active", ie1)]
   g2 = result.by_key[("ai_coding", "active", ie2)]
   assert abs(g1.cpu_seconds - 1.0) < 1e-9
   assert abs(g2.cpu_seconds - 2.0) < 1e-9


def test_usage_complete_true_when_all_grains_complete():
   row = _make_cpu_row()
   row["complete"] = True
   conn = _make_usage_conn(cpu=[row])
   result = load_usage(conn, "polaris", "login-04", _START, _INVENTORY,
                       now_utc=_NOW)
   grain = result.by_key[("ai_coding", "active", _END)]
   assert grain.complete is True


def test_usage_complete_false_when_any_grain_incomplete():
   row = _make_cpu_row()
   row["complete"] = False
   conn = _make_usage_conn(cpu=[row])
   result = load_usage(conn, "polaris", "login-04", _START, _INVENTORY,
                       now_utc=_NOW)
   grain = result.by_key[("ai_coding", "active", _END)]
   assert grain.complete is False


# ---------------------------------------------------------------------------
# Step 2g-ext: non-additive hotspot ALL metrics (p50/p95/max + interactivity)
# ---------------------------------------------------------------------------

def test_usage_grain_carries_rss_p50_and_max():
   """_UsageGrain must expose rss_p50_kb and rss_max_kb (not only rss_p95_kb)."""
   conn = _make_usage_conn(
      rss=[_make_rss_row(rss_p50_kb=4000.0, rss_p95_kb=8000.0, rss_max_kb=9000.0)],
   )
   result = load_usage(conn, "polaris", "login-04", _START, _INVENTORY,
                       now_utc=_NOW)
   grain = result.by_key[("ai_coding", "active", _END)]
   assert grain.rss_p50_kb == 4000.0
   assert grain.rss_p95_kb == 8000.0
   assert grain.rss_max_kb == 9000.0


def test_usage_grain_carries_process_count_p50_and_max():
   """_UsageGrain must expose process_count_p50 and process_count_max."""
   conn = _make_usage_conn(
      proc=[_make_proc_row(process_count_p50=1.0, process_count_p95=2.0,
                            process_count_max=3.0)],
   )
   result = load_usage(conn, "polaris", "login-04", _START, _INVENTORY,
                       now_utc=_NOW)
   grain = result.by_key[("ai_coding", "active", _END)]
   assert grain.process_count_p50 == 1.0
   assert grain.process_count_p95 == 2.0
   assert grain.process_count_max == 3.0


def test_usage_grain_carries_interactivity_fraction_with_username():
   """_UsageGrain must expose interactivity_fraction and interactivity_username."""
   conn = _make_usage_conn(
      interactivity=[_make_interactivity_row(interactivity_fraction=0.75,
                                              username="interactive-user")],
   )
   result = load_usage(conn, "polaris", "login-04", _START, _INVENTORY,
                       now_utc=_NOW)
   grain = result.by_key[("ai_coding", "active", _END)]
   assert abs(grain.interactivity_fraction - 0.75) < 1e-9
   assert grain.interactivity_username == "interactive-user"


def test_usage_grain_interactivity_is_none_when_no_hotspot_row():
   """interactivity_fraction and interactivity_username are None when no row."""
   conn = _make_conn([
      [_make_cpu_row()],
      [_make_rss_row()],
      [_make_d_row()],
      [_make_proc_row()],
      [],   # no interactivity hotspot row
   ])
   result = load_usage(conn, "polaris", "login-04", _START, _INVENTORY,
                       now_utc=_NOW)
   grain = result.by_key[("ai_coding", "active", _END)]
   assert grain.interactivity_fraction is None
   assert grain.interactivity_username is None


def test_usage_grain_rss_is_none_when_no_hotspot_row():
   """rss_p50_kb, rss_p95_kb, rss_max_kb are None when no RSS hotspot row."""
   conn = _make_conn([
      [_make_cpu_row()],
      [],   # no RSS hotspot row
      [_make_d_row()],
      [_make_proc_row()],
      [_make_interactivity_row()],
   ])
   result = load_usage(conn, "polaris", "login-04", _START, _INVENTORY,
                       now_utc=_NOW)
   grain = result.by_key[("ai_coding", "active", _END)]
   assert grain.rss_p50_kb is None
   assert grain.rss_p95_kb is None
   assert grain.rss_max_kb is None


def test_usage_grain_process_count_is_none_when_no_hotspot_row():
   """process_count_p50/p95/max are None when no process_count hotspot row."""
   conn = _make_conn([
      [_make_cpu_row()],
      [_make_rss_row()],
      [_make_d_row()],
      [],   # no process_count hotspot row
      [_make_interactivity_row()],
   ])
   result = load_usage(conn, "polaris", "login-04", _START, _INVENTORY,
                       now_utc=_NOW)
   grain = result.by_key[("ai_coding", "active", _END)]
   assert grain.process_count_p50 is None
   assert grain.process_count_p95 is None
   assert grain.process_count_max is None


def test_load_usage_executes_five_queries():
   """load_usage must issue exactly five SQL queries: cpu + 4 hotspot queries."""
   conn = _make_usage_conn()
   load_usage(conn, "polaris", "login-04", _START, _INVENTORY, now_utc=_NOW)
   assert conn.execute.call_count == 5, (
      "load_usage must execute exactly 5 queries; got %d" % conn.execute.call_count)


# ---------------------------------------------------------------------------
# Step 2g-ext2: SQL constants for new hotspot columns
# ---------------------------------------------------------------------------

def test_usage_rss_hotspot_sql_projects_p50_and_max():
   """USAGE_RSS_HOTSPOT_SQL must project rss_p50_kb, rss_p95_kb, rss_max_kb."""
   from node_monitor.web.queries import USAGE_RSS_HOTSPOT_SQL
   sql_text = str(USAGE_RSS_HOTSPOT_SQL)
   assert "rss_p50_kb" in sql_text
   assert "rss_p95_kb" in sql_text
   assert "rss_max_kb" in sql_text


def test_usage_process_count_hotspot_sql_projects_p50_and_max():
   """USAGE_PROCESS_COUNT_HOTSPOT_SQL must project p50/p95/max."""
   from node_monitor.web.queries import USAGE_PROCESS_COUNT_HOTSPOT_SQL
   sql_text = str(USAGE_PROCESS_COUNT_HOTSPOT_SQL)
   assert "process_count_p50" in sql_text
   assert "process_count_p95" in sql_text
   assert "process_count_max" in sql_text


def test_usage_interactivity_hotspot_sql_exists():
   """USAGE_INTERACTIVITY_HOTSPOT_SQL must exist and project username."""
   from node_monitor.web.queries import USAGE_INTERACTIVITY_HOTSPOT_SQL
   sql_text = str(USAGE_INTERACTIVITY_HOTSPOT_SQL)
   assert "interactivity_fraction" in sql_text
   assert "username" in sql_text


def test_usage_interactivity_hotspot_sql_uses_distinct_on():
   from node_monitor.web.queries import USAGE_INTERACTIVITY_HOTSPOT_SQL
   sql_text = str(USAGE_INTERACTIVITY_HOTSPOT_SQL).upper()
   assert "DISTINCT ON" in sql_text


def test_usage_interactivity_hotspot_sql_orders_by_username_key():
   from node_monitor.web.queries import USAGE_INTERACTIVITY_HOTSPOT_SQL
   sql_text = str(USAGE_INTERACTIVITY_HOTSPOT_SQL)
   assert "username_key" in sql_text


def test_usage_interactivity_hotspot_sql_binds_username_is_null():
   from node_monitor.web.queries import USAGE_INTERACTIVITY_HOTSPOT_SQL
   assert ":username_is_null" in str(USAGE_INTERACTIVITY_HOTSPOT_SQL)


# ---------------------------------------------------------------------------
# Step 2g-ext3: SQL aggregation proof (mutation-style)
# ---------------------------------------------------------------------------

def test_usage_cpu_sql_contains_sum_aggregation():
   """USAGE_CPU_SQL must contain SUM(cpu_seconds) -- additive aggregation proof."""
   sql_text = str(USAGE_CPU_SQL).upper()
   assert "SUM" in sql_text, "USAGE_CPU_SQL must use SUM for cpu_seconds aggregation"


def test_usage_cpu_sql_contains_group_by_grain():
   """USAGE_CPU_SQL must GROUP BY (interval_end, category, activity) -- exact grain."""
   sql_text = str(USAGE_CPU_SQL).upper()
   assert "GROUP BY" in sql_text, "USAGE_CPU_SQL must have GROUP BY clause"
   assert "INTERVAL_END" in sql_text
   assert "CATEGORY" in sql_text
   assert "ACTIVITY" in sql_text


def test_usage_cpu_sql_does_not_group_by_username():
   """USAGE_CPU_SQL must NOT GROUP BY username; that would defeat additive summing
   across username grains and is the re-sum-of-aggregates anti-pattern."""
   sql_text = str(USAGE_CPU_SQL)
   upper = sql_text.upper()
   gb_pos = upper.find("GROUP BY")
   ob_pos = upper.find("ORDER BY", gb_pos)
   group_by_clause = upper[gb_pos:ob_pos] if ob_pos > gb_pos else upper[gb_pos:]
   assert "USERNAME" not in group_by_clause, (
      "USAGE_CPU_SQL GROUP BY must not include USERNAME -- "
      "summation is over username grains, not per username")


# ---------------------------------------------------------------------------
# Step 2h: poll failures cap
# ---------------------------------------------------------------------------

def test_poll_failures_limit_constant():
   assert MAX_POLL_FAILURES == 10


def test_load_poll_failures_passes_limit_to_sql():
   rows = [
      {"recorded_at": _NOW, "loop": "counter", "failure_type": "timeout",
       "detail": "timed out", "consecutive_failures": 1, "breaker_state": "closed"}
   ] * 3
   conn = _make_conn([rows])
   result = load_poll_failures(conn, "polaris", "login-04", _INVENTORY)
   assert len(result) == 3
   # Verify LIMIT was passed as a bound parameter.
   call_params = conn.execute.call_args_list[0][0][1]
   assert call_params["limit"] == MAX_POLL_FAILURES


def test_poll_failures_rejects_unknown_node():
   conn = _make_conn([[]])
   with pytest.raises(QueryValidationError, match="not in the known inventory"):
      load_poll_failures(conn, "polaris", "login-99", _INVENTORY)


# ---------------------------------------------------------------------------
# Step 2i: collection log -- system-level, bounded LIMIT
# ---------------------------------------------------------------------------

def test_collection_log_sql_has_no_source_hostname_filter():
   """The collection log SQL must not contain source_hostname as a filter."""
   sql_text = str(COLLECTION_LOG_SQL)
   assert "source_hostname" not in sql_text, (
      "COLLECTION_LOG_SQL must not filter by source_hostname; "
      "collection log is system-level")


def test_collection_log_limit_constant_exists():
   """COLLECTION_LOG_LIMIT must be defined as a positive integer."""
   assert isinstance(COLLECTION_LOG_LIMIT, int)
   assert COLLECTION_LOG_LIMIT > 0


def test_collection_log_sql_has_limit_clause():
   """COLLECTION_LOG_SQL must contain a LIMIT clause to prevent unbounded fetch."""
   sql_text = str(COLLECTION_LOG_SQL).upper()
   assert "LIMIT" in sql_text, "COLLECTION_LOG_SQL must have a LIMIT clause"


def test_collection_log_sql_limit_is_parameterized():
   """COLLECTION_LOG_SQL LIMIT must be a bound parameter (not a hardcoded literal)."""
   sql_text = str(COLLECTION_LOG_SQL)
   assert ":limit" in sql_text.lower(), (
      "COLLECTION_LOG_SQL LIMIT must be a bound parameter")


def test_load_collection_log_passes_limit_to_sql():
   """load_collection_log must pass COLLECTION_LOG_LIMIT as a bound parameter."""
   rows = [
      {"recorded_at": _NOW, "event": "start", "detail": {"host": "collector-01"}},
   ]
   conn = _make_conn([rows])
   result = load_collection_log(conn, "polaris", _START)
   assert len(result) == 1
   assert result[0]["event"] == "start"
   call_params = conn.execute.call_args_list[0][0][1]
   assert call_params.get("limit") == COLLECTION_LOG_LIMIT, (
      "load_collection_log must pass COLLECTION_LOG_LIMIT=%d; got %r"
      % (COLLECTION_LOG_LIMIT, call_params.get("limit")))


def test_load_collection_log_returns_rows():
   rows = [
      {"recorded_at": _NOW, "event": "start", "detail": {"host": "collector-01"}},
   ]
   conn = _make_conn([rows])
   result = load_collection_log(conn, "polaris", _START)
   assert len(result) == 1
   assert result[0]["event"] == "start"


# ---------------------------------------------------------------------------
# Step 2j: SQL constants -- no identifier binding, no SELECT *
# ---------------------------------------------------------------------------

def test_counter_sql_has_no_star():
   assert "SELECT *" not in str(COUNTER_SQL).upper()


def test_usage_cpu_sql_has_no_star():
   assert "SELECT *" not in str(USAGE_CPU_SQL).upper()


def test_usage_rss_hotspot_sql_uses_jsonb_extraction():
   from node_monitor.web.queries import USAGE_RSS_HOTSPOT_SQL
   sql_text = str(USAGE_RSS_HOTSPOT_SQL)
   assert "->>" in sql_text, "Hotspot SQL must use ->> for JSONB field extraction"


def test_usage_d_state_hotspot_sql_uses_distinct_on():
   from node_monitor.web.queries import USAGE_D_STATE_HOTSPOT_SQL
   sql_text = str(USAGE_D_STATE_HOTSPOT_SQL).upper()
   assert "DISTINCT ON" in sql_text


def test_usage_rss_hotspot_sql_uses_distinct_on():
   from node_monitor.web.queries import USAGE_RSS_HOTSPOT_SQL
   sql_text = str(USAGE_RSS_HOTSPOT_SQL).upper()
   assert "DISTINCT ON" in sql_text


def test_usage_rss_hotspot_sql_orders_by_username_key():
   """username_key tiebreaker must be in ORDER BY of hotspot queries."""
   from node_monitor.web.queries import USAGE_RSS_HOTSPOT_SQL
   sql_text = str(USAGE_RSS_HOTSPOT_SQL)
   assert "username_key" in sql_text


def test_all_hotspot_sqls_bind_username_is_null():
   """All usage queries must accept :username_is_null to allow None username."""
   from node_monitor.web.queries import (
      USAGE_CPU_SQL,
      USAGE_D_STATE_HOTSPOT_SQL,
      USAGE_INTERACTIVITY_HOTSPOT_SQL,
      USAGE_PROCESS_COUNT_HOTSPOT_SQL,
      USAGE_RSS_HOTSPOT_SQL,
   )
   for sql in (USAGE_CPU_SQL, USAGE_RSS_HOTSPOT_SQL,
               USAGE_D_STATE_HOTSPOT_SQL, USAGE_PROCESS_COUNT_HOTSPOT_SQL,
               USAGE_INTERACTIVITY_HOTSPOT_SQL):
      assert ":username_is_null" in str(sql), (
         "SQL %r must use :username_is_null parameter" % str(sql)[:60])


# ---------------------------------------------------------------------------
# Step 2k: load_counters rejects unknown node before issuing SQL
# ---------------------------------------------------------------------------

def test_load_counters_rejects_unknown_node_before_sql():
   conn = MagicMock()
   with pytest.raises(QueryValidationError):
      load_counters(conn, "polaris", "login-99", _START, _INVENTORY,
                    now_utc=_NOW)
   conn.execute.assert_not_called()


def test_load_usage_rejects_unknown_node_before_sql():
   conn = MagicMock()
   with pytest.raises(QueryValidationError):
      load_usage(conn, "polaris", "login-99", _START, _INVENTORY, now_utc=_NOW)
   conn.execute.assert_not_called()


# ---------------------------------------------------------------------------
# Step 2l: constants are correct type / value
# ---------------------------------------------------------------------------

def test_counter_staleness_constant():
   assert COUNTER_STALENESS_SECONDS == 120


def test_usage_staleness_constant():
   assert USAGE_STALENESS_SECONDS == 1200


def test_counter_row_limit_constant():
   assert COUNTER_ROW_LIMIT == 1440


def test_max_username_bytes_constant():
   assert MAX_USERNAME_BYTES == 256


# ---------------------------------------------------------------------------
# Step 2m: CounterResult quality metadata -- invalid_pair_count and
# excess_sample_count must be carried through from the DB row
# ---------------------------------------------------------------------------

def test_counter_result_carries_invalid_pair_count():
   """CounterResult rows must carry invalid_pair_count from the DB row."""
   row = {**_make_counter_row(1), "invalid_pair_count": 3}
   conn = _make_conn([[row]])
   result = load_counters(conn, "polaris", "login-04", _START, _INVENTORY,
                          now_utc=_NOW)
   assert result.rows[0]["invalid_pair_count"] == 3


def test_counter_result_carries_excess_sample_count():
   """CounterResult rows must carry excess_sample_count from the DB row."""
   row = {**_make_counter_row(1), "excess_sample_count": 2}
   conn = _make_conn([[row]])
   result = load_counters(conn, "polaris", "login-04", _START, _INVENTORY,
                          now_utc=_NOW)
   assert result.rows[0]["excess_sample_count"] == 2


def test_counter_sql_projects_invalid_pair_count():
   """COUNTER_SQL must explicitly SELECT invalid_pair_count."""
   sql_text = str(COUNTER_SQL)
   assert "invalid_pair_count" in sql_text, (
      "COUNTER_SQL must project invalid_pair_count")


def test_counter_sql_projects_excess_sample_count():
   """COUNTER_SQL must explicitly SELECT excess_sample_count."""
   sql_text = str(COUNTER_SQL)
   assert "excess_sample_count" in sql_text, (
      "COUNTER_SQL must project excess_sample_count")


def test_counter_result_complete_semantics_all_quality_flags():
   """A row with coverage=1, meets_minimum_samples=True, and both counts zero
   represents a complete/full-quality result."""
   row = {
      **_make_counter_row(1),
      "coverage": 1.0,
      "meets_minimum_samples": True,
      "invalid_pair_count": 0,
      "excess_sample_count": 0,
   }
   conn = _make_conn([[row]])
   result = load_counters(conn, "polaris", "login-04", _START, _INVENTORY,
                          now_utc=_NOW)
   r = result.rows[0]
   assert r.get("coverage") == 1.0
   assert r.get("meets_minimum_samples") is True
   assert r.get("invalid_pair_count") == 0
   assert r.get("excess_sample_count") == 0


def test_counter_result_partial_when_coverage_lt_1():
   """Rows with coverage < 1 are partial (data is incomplete)."""
   row = {
      **_make_counter_row(1),
      "coverage": 0.8,
      "meets_minimum_samples": True,
      "invalid_pair_count": 0,
      "excess_sample_count": 0,
   }
   conn = _make_conn([[row]])
   result = load_counters(conn, "polaris", "login-04", _START, _INVENTORY,
                          now_utc=_NOW)
   assert result.rows[0]["coverage"] < 1.0


def test_counter_result_partial_when_invalid_pair_count_nonzero():
   """Rows with invalid_pair_count > 0 have quality issues."""
   row = {
      **_make_counter_row(1),
      "coverage": 1.0,
      "meets_minimum_samples": True,
      "invalid_pair_count": 2,
      "excess_sample_count": 0,
   }
   conn = _make_conn([[row]])
   result = load_counters(conn, "polaris", "login-04", _START, _INVENTORY,
                          now_utc=_NOW)
   assert result.rows[0]["invalid_pair_count"] == 2


def test_counter_result_partial_when_excess_sample_count_nonzero():
   """Rows with excess_sample_count > 0 have quality issues."""
   row = {
      **_make_counter_row(1),
      "coverage": 1.0,
      "meets_minimum_samples": True,
      "invalid_pair_count": 0,
      "excess_sample_count": 5,
   }
   conn = _make_conn([[row]])
   result = load_counters(conn, "polaris", "login-04", _START, _INVENTORY,
                          now_utc=_NOW)
   assert result.rows[0]["excess_sample_count"] == 5
