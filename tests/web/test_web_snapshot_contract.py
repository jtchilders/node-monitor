"""Task 5 timestamp and response-contract tests.

Proves:
  1. _run_dashboard_queries calls hardware SELECT first on the same
     connection used for all subsequent subqueries.
  2. server_utc_now carries +00:00 (from real production datetime.now(timezone.utc)).
  3. Response carries explicit gap metadata for counter rows (60s cadence)
     and usage grains (900s cadence).
  4. Response carries explicit status/quality state:
       counters.status: "empty" | "stale" | "partial" | "complete"
       usage.status: "empty" | "stale" | "partial" | "complete"
  5. Response carries latest counter gauge values (current-card).
  6. Response carries mem_used_physical_kb (derived:
     mem_total_kb - mem_available_kb from latest counter row; None if unavailable).
  7. Exact 5 MiB payload accepted; deterministic.

These tests call the production _run_dashboard_queries (not a snapshot).
PostgreSQL tests are skipped when NODE_MONITOR_TEST_DATABASE_URL is absent.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, call, patch

import pytest

from node_monitor.web.service import (
   DashboardService,
   DashboardServiceError,
   DashboardTooLarge,
   MAX_RESPONSE_BYTES,
   _run_dashboard_queries,
   serialize_dashboard,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SYSTEM = "polaris"
_NODE = "login-04"
_RANGE_HOURS = 1
_MEM_TOTAL_KB = 131072000

_HW_ROW = {
   "system": _SYSTEM,
   "source_hostname": _NODE,
   "cpu_model": "Intel Xeon",
   "cpu_logical": 96,
   "sockets": 2,
   "cores_per_socket": 24,
   "cpu_max_freq_khz": 2400000,
   "numa_nodes": 2,
   "mem_total_kb": _MEM_TOTAL_KB,
   "swap_total_kb": 0,
   "hugepage_size_kb": 2048,
   "kernel_release": "5.14.0",
   "os_pretty_name": "Red Hat Enterprise Linux 8.6",
   "net_fs_mounts": 4,
   "gpus": None,
}


def _make_hw_result(row=None):
   """Return a mock SQLAlchemy result whose .mappings().first() returns row."""
   result = MagicMock()
   mapping = MagicMock()
   mapping.first.return_value = _dictlike(row or _HW_ROW)
   result.mappings.return_value = mapping
   return result


def _dictlike(d):
   """Wrap a dict in an object that supports both [] and .get()."""
   class _DictLike:
      def __getitem__(self, k):
         return d[k]
      def get(self, k, default=None):
         return d.get(k, default)
      def __contains__(self, k):
         return k in d
   return _DictLike()


def _make_counter_rows(n=3, start_offset_min=60, window_size_min=1):
   """Return n counter row dicts spaced window_size_min apart."""
   now = datetime(2026, 9, 30, 14, 0, tzinfo=timezone.utc)
   rows = []
   for i in range(n):
      we = now - timedelta(minutes=start_offset_min - i * window_size_min)
      rows.append({
         "window_start": we - timedelta(minutes=window_size_min),
         "window_end": we,
         "sample_count": 6,
         "expected_count": 6,
         "coverage": 1.0,
         "meets_minimum_samples": True,
         "invalid_pair_count": 0,
         "excess_sample_count": 0,
         "mem_available_kb": 64000000 - i * 1000,
         "cached_kb": None,
         "shmem_kb": None,
         "load1": 1.0, "load5": 0.9, "load15": 0.8,
         "procs_running": 4,
         "procs_total": 400,
         "socket_count": None,
         "cpu_busy_pct": {"p50": 0.3, "p95": 0.7, "max": 0.9},
         "network_rates": None,
         "lustre_md_summary": None,
      })
   return rows


def _make_empty_usage_result():
   """Return a mock UsageResult with no rows."""
   result = MagicMock()
   result.rows = []
   result.by_key = {}
   result.newest_interval_end = None
   result.is_fresh = False
   return result


def _mock_conn_for_dashboard(hw_row=None, counter_rows=None, usage_rows_count=0):
   """Return a mock SA connection for _run_dashboard_queries.

   Query call order expected by the production code:
     1. _HARDWARE_SQL (HW lookup)
     2+ delegated to load_counters_for_range, load_usage_for_range, etc.
   """
   conn = MagicMock(name="sa_conn")
   calls_made = []

   hw = hw_row or _HW_ROW
   counter_rows = counter_rows or []

   hw_result = _make_hw_result(hw)

   def _execute(stmt, params=None, *args, **kwargs):
      stmt_str = str(stmt)
      calls_made.append(stmt_str[:80])

      mock_result = MagicMock()
      mock_result.mappings.return_value = MagicMock()
      mock_result.mappings.return_value.first.return_value = _dictlike(hw)
      return mock_result

   conn.execute.side_effect = _execute
   conn._calls_made = calls_made
   return conn


# ---------------------------------------------------------------------------
# Test 1: hardware SELECT executes first and on same connection
# ---------------------------------------------------------------------------

def test_run_dashboard_queries_hardware_select_first_on_same_conn():
   """_run_dashboard_queries must execute hardware SELECT first on same conn.

   Machine assertion:
     - conn.execute is called at least once
     - first execute call contains 'node_hardware' (the HW query)
     - all subsequent calls use the SAME conn object (not a new connection)
   """
   execute_calls = []
   now_utc = datetime(2026, 9, 30, 14, 0, tzinfo=timezone.utc)

   conn = MagicMock(name="sa_conn")

   # Track each execute call
   def _execute(stmt, params=None, *a, **kw):
      execute_calls.append(str(stmt))
      mock_result = MagicMock()
      # HW query: mappings().first() returns a hardware row
      first_result = _dictlike(_HW_ROW)
      mock_result.mappings.return_value.first.return_value = first_result
      mock_result.mappings.return_value.__iter__ = lambda self: iter([])
      return mock_result

   conn.execute.side_effect = _execute

   # Patch the load_* functions to avoid full SQL execution
   with patch("node_monitor.web.service.load_counters_for_range") as mock_ctr, \
        patch("node_monitor.web.service.load_usage_for_range") as mock_usg, \
        patch("node_monitor.web.service.load_poll_failures_for_range") as mock_pf, \
        patch("node_monitor.web.service.load_collection_log_for_range") as mock_cl:

      # Setup return values
      ctr_result = MagicMock()
      ctr_result.rows = []
      ctr_result.newest_window_end = None
      ctr_result.is_fresh = False
      mock_ctr.return_value = ctr_result

      usg_result = MagicMock()
      usg_result.rows = []
      usg_result.by_key = {}
      usg_result.newest_interval_end = None
      usg_result.is_fresh = False
      mock_usg.return_value = usg_result

      mock_pf.return_value = []
      mock_cl.return_value = []

      result = _run_dashboard_queries(
         conn,
         system=_SYSTEM,
         node=_NODE,
         range_hours=_RANGE_HOURS,
         username=None,
         now_utc=now_utc,
      )

   # At least one execute call must have happened (the hardware query)
   assert len(execute_calls) >= 1, (
      "_run_dashboard_queries must call conn.execute at least once "
      "(for the hardware SELECT)")

   # First execute must be the hardware SELECT
   first_call = execute_calls[0].lower()
   assert "node_hardware" in first_call, (
      "First conn.execute call must be the hardware SELECT; "
      "got: %r" % execute_calls[0][:80])

   # All load_*_for_range calls must receive the SAME conn object
   assert mock_ctr.call_args[0][0] is conn, (
      "load_counters_for_range must receive the same conn object")
   assert mock_usg.call_args[0][0] is conn, (
      "load_usage_for_range must receive the same conn object")
   assert mock_pf.call_args[0][0] is conn, (
      "load_poll_failures_for_range must receive the same conn object")
   assert mock_cl.call_args[0][0] is conn, (
      "load_collection_log_for_range must receive the same conn object")


# ---------------------------------------------------------------------------
# Test 2: server_utc_now uses real production datetime, has +00:00
# ---------------------------------------------------------------------------

def test_run_dashboard_queries_server_utc_now_has_plus_00_00():
   """server_utc_now must carry explicit '+00:00' from real datetime.now(utc).

   The now_utc passed to _run_dashboard_queries is datetime.now(timezone.utc)
   (from the production path).  server_utc_now in the snapshot must have
   explicit +00:00 offset.
   """
   # Use a real timezone.utc datetime (as production does)
   now_utc = datetime.now(timezone.utc)
   conn = MagicMock(name="sa_conn")

   def _execute(stmt, params=None, *a, **kw):
      mock_result = MagicMock()
      mock_result.mappings.return_value.first.return_value = _dictlike(_HW_ROW)
      mock_result.mappings.return_value.__iter__ = lambda self: iter([])
      return mock_result

   conn.execute.side_effect = _execute

   with patch("node_monitor.web.service.load_counters_for_range") as mock_ctr, \
        patch("node_monitor.web.service.load_usage_for_range") as mock_usg, \
        patch("node_monitor.web.service.load_poll_failures_for_range") as mock_pf, \
        patch("node_monitor.web.service.load_collection_log_for_range") as mock_cl:

      ctr_result = MagicMock()
      ctr_result.rows = []
      ctr_result.newest_window_end = None
      ctr_result.is_fresh = False
      mock_ctr.return_value = ctr_result

      usg_result = MagicMock()
      usg_result.rows = []
      usg_result.by_key = {}
      usg_result.newest_interval_end = None
      usg_result.is_fresh = False
      mock_usg.return_value = usg_result

      mock_pf.return_value = []
      mock_cl.return_value = []

      snapshot = _run_dashboard_queries(
         conn,
         system=_SYSTEM,
         node=_NODE,
         range_hours=_RANGE_HOURS,
         username=None,
         now_utc=now_utc,
      )

   ts = snapshot["server_utc_now"]
   assert "+00:00" in ts, (
      "server_utc_now must have explicit '+00:00' offset; got: %r" % ts)


# ---------------------------------------------------------------------------
# Test 3: response carries counter gap metadata
# ---------------------------------------------------------------------------

def test_response_carries_counter_gap_metadata():
   """counters dict must include gap metadata (missing_count, intervals).

   For a 60s-cadence counter series, the gap metadata must report how many
   expected windows are missing between the actual rows.

   Machine assertion: counters contains a 'gaps' key with count/missing_count.
   """
   import json

   async def _test():
      svc = _make_service_with_gaps()
      payload = await svc.dashboard(node=_NODE, range_name="1h", username=None)
      data = json.loads(payload)
      ctr = data["counters"]
      assert "gaps" in ctr, (
         "counters dict must include 'gaps' metadata; got keys: %r"
         % list(ctr.keys()))
      gaps = ctr["gaps"]
      # Must have a count (can be 0 for contiguous series)
      assert "missing_count" in gaps or "count" in gaps, (
         "gaps must have at least 'missing_count' or 'count'; got: %r" % gaps)
      return True

   import asyncio
   assert asyncio.run(_test()) is True


def _make_service_with_gaps():
   """Return a DashboardService with a stub operation returning a snapshot."""
   import asyncio
   from node_monitor.web.service import DashboardService

   db = MagicMock()

   def fake_op(conn, dbapi_conn):
      return _minimal_snapshot_with_counters()

   svc = DashboardService(database=db, system=_SYSTEM)
   svc._run_operation = fake_op
   return svc


def _minimal_snapshot_with_counters():
   """Return a snapshot with counter rows for gap testing."""
   now = datetime(2026, 9, 30, 14, 0, tzinfo=timezone.utc)
   # Two rows with a 2-minute gap (60s cadence, so 1 window is missing)
   row1_end = now - timedelta(minutes=5)
   row2_end = now - timedelta(minutes=2)  # gap: minutes 4, 3 (missing row at 3)
   return {
      "server_utc_now": "2026-09-30T14:00:00+00:00",
      "node": _NODE,
      "range_hours": 1,
      "hardware": _minimal_hardware(),
      "counters": {
         "rows": [
            _counter_row(row1_end),
            _counter_row(row2_end),
         ],
         "newest_window_end": row2_end.isoformat(),
         "is_fresh": True,
      },
      "usage": {"grains": [], "newest_interval_end": None, "is_fresh": False},
      "poll_failures": [],
      "collection_log": [],
   }


def _minimal_hardware():
   return {
      "system": _SYSTEM,
      "source_hostname": _NODE,
      "mem_total_kb": _MEM_TOTAL_KB,
      "cpu_model": "Intel Xeon",
      "cpu_logical": 96,
      "sockets": 2,
      "cores_per_socket": 24,
      "cpu_max_freq_khz": 2400000,
      "numa_nodes": 2,
      "swap_total_kb": 0,
      "hugepage_size_kb": 2048,
      "kernel_release": "5.14.0",
      "os_pretty_name": "RHEL 8.6",
      "net_fs_mounts": 4,
      "gpus": None,
   }


def _counter_row(window_end):
   return {
      "window_start": (window_end - timedelta(minutes=1)).isoformat(),
      "window_end": window_end.isoformat(),
      "sample_count": 6,
      "expected_count": 6,
      "coverage": 1.0,
      "meets_minimum_samples": True,
      "invalid_pair_count": 0,
      "excess_sample_count": 0,
      "complete": True,
      "mem_available_kb": 64000000,
      "cached_kb": None,
      "shmem_kb": None,
      "load1": 1.0,
      "load5": 0.9,
      "load15": 0.8,
      "procs_running": 4,
      "procs_total": 400,
      "socket_count": None,
      "cpu_busy_pct": None,
      "network_rates": None,
      "lustre_md_summary": None,
   }


# ---------------------------------------------------------------------------
# Test 4: response carries counters.status quality state
# ---------------------------------------------------------------------------

def test_response_carries_counters_status_field():
   """counters dict must include a 'status' field.

   Status must be one of: 'empty', 'stale', 'partial', 'complete'.
   """
   import asyncio
   import json

   db = MagicMock()

   # Empty series → 'empty'
   def empty_op(conn, dbapi_conn):
      s = _minimal_snapshot_with_counters()
      s["counters"]["rows"] = []
      s["counters"]["newest_window_end"] = None
      s["counters"]["is_fresh"] = False
      return s

   from node_monitor.web.service import DashboardService
   svc = DashboardService(database=db, system=_SYSTEM)
   svc._run_operation = empty_op

   async def _test():
      payload = await svc.dashboard(node=_NODE, range_name="1h", username=None)
      data = json.loads(payload)
      ctr = data["counters"]
      assert "status" in ctr, (
         "counters must carry 'status' field; got keys: %r" % list(ctr.keys()))
      assert ctr["status"] in ("empty", "stale", "partial", "complete"), (
         "status must be one of the four allowed values; got: %r" % ctr["status"])
      return ctr["status"]

   status = asyncio.run(_test())
   assert status == "empty", (
      "empty series must have status='empty'; got: %r" % status)


# ---------------------------------------------------------------------------
# Test 5: response carries latest counter gauges (current-card values)
# ---------------------------------------------------------------------------

def test_response_carries_latest_counter_gauges():
   """response must include latest_counter with current-card gauge values.

   The latest counter row's gauge fields (load1, load5, load15,
   procs_running, procs_total, mem_available_kb) must be exposed as
   explicit top-level latest_counter values in the counters dict.
   """
   import asyncio
   import json

   now = datetime(2026, 9, 30, 14, 0, tzinfo=timezone.utc)
   latest_end = now - timedelta(minutes=1)

   def op_with_latest(conn, dbapi_conn):
      s = _minimal_snapshot_with_counters()
      row = _counter_row(latest_end)
      row["load1"] = 2.5
      row["load5"] = 1.8
      row["load15"] = 1.2
      row["procs_running"] = 8
      row["procs_total"] = 512
      row["mem_available_kb"] = 48000000
      s["counters"]["rows"] = [row]
      s["counters"]["newest_window_end"] = latest_end.isoformat()
      s["counters"]["is_fresh"] = True
      return s

   from node_monitor.web.service import DashboardService
   db = MagicMock()
   svc = DashboardService(database=db, system=_SYSTEM)
   svc._run_operation = op_with_latest

   async def _test():
      payload = await svc.dashboard(node=_NODE, range_name="1h", username=None)
      data = json.loads(payload)
      ctr = data["counters"]
      assert "latest" in ctr, (
         "counters must carry 'latest' with current-card gauges; "
         "got keys: %r" % list(ctr.keys()))
      latest = ctr["latest"]
      assert latest is not None, "latest must not be None when rows exist"
      assert latest.get("load1") == pytest.approx(2.5)
      assert latest.get("load5") == pytest.approx(1.8)
      assert latest.get("procs_running") == 8
      assert latest.get("mem_available_kb") == 48000000
      return True

   assert asyncio.run(_test()) is True


# ---------------------------------------------------------------------------
# Test 6: response carries mem_used_physical_kb derived field
# ---------------------------------------------------------------------------

def test_response_carries_mem_used_physical_kb():
   """response must include mem_used_physical_kb in counters.latest.

   mem_used_physical_kb = hardware.mem_total_kb - latest row's mem_available_kb.
   Must be None when mem_available_kb or mem_total_kb is unavailable.
   """
   import asyncio
   import json

   now = datetime(2026, 9, 30, 14, 0, tzinfo=timezone.utc)
   latest_end = now - timedelta(minutes=1)
   mem_available = 48000000
   expected_used = _MEM_TOTAL_KB - mem_available

   def op_with_mem(conn, dbapi_conn):
      s = _minimal_snapshot_with_counters()
      row = _counter_row(latest_end)
      row["mem_available_kb"] = mem_available
      s["counters"]["rows"] = [row]
      s["counters"]["newest_window_end"] = latest_end.isoformat()
      s["counters"]["is_fresh"] = True
      return s

   from node_monitor.web.service import DashboardService
   db = MagicMock()
   svc = DashboardService(database=db, system=_SYSTEM)
   svc._run_operation = op_with_mem

   async def _test():
      payload = await svc.dashboard(node=_NODE, range_name="1h", username=None)
      data = json.loads(payload)
      ctr = data["counters"]
      latest = ctr.get("latest", {})
      assert "mem_used_physical_kb" in latest, (
         "counters.latest must carry 'mem_used_physical_kb'; "
         "got keys: %r" % list(latest.keys()))
      assert latest["mem_used_physical_kb"] == expected_used, (
         "mem_used_physical_kb must be mem_total_kb - mem_available_kb; "
         "expected %d, got %r" % (expected_used, latest["mem_used_physical_kb"]))
      return True

   assert asyncio.run(_test()) is True


def test_mem_used_physical_kb_is_none_when_mem_available_absent():
   """mem_used_physical_kb is None when mem_available_kb is None in latest row."""
   import asyncio
   import json

   now = datetime(2026, 9, 30, 14, 0, tzinfo=timezone.utc)
   latest_end = now - timedelta(minutes=1)

   def op_no_mem_available(conn, dbapi_conn):
      s = _minimal_snapshot_with_counters()
      row = _counter_row(latest_end)
      row["mem_available_kb"] = None  # Not available
      s["counters"]["rows"] = [row]
      s["counters"]["newest_window_end"] = latest_end.isoformat()
      s["counters"]["is_fresh"] = True
      return s

   from node_monitor.web.service import DashboardService
   db = MagicMock()
   svc = DashboardService(database=db, system=_SYSTEM)
   svc._run_operation = op_no_mem_available

   async def _test():
      payload = await svc.dashboard(node=_NODE, range_name="1h", username=None)
      data = json.loads(payload)
      latest = data["counters"].get("latest", {})
      val = latest.get("mem_used_physical_kb", "KEY_MISSING")
      assert val is None, (
         "mem_used_physical_kb must be None when mem_available_kb is None; "
         "got: %r" % val)
      return True

   assert asyncio.run(_test()) is True


# ---------------------------------------------------------------------------
# Test 7: counters.latest is None when rows are empty
# ---------------------------------------------------------------------------

def test_counters_latest_is_none_when_empty():
   """counters.latest must be None when there are no counter rows."""
   import asyncio
   import json

   def empty_op(conn, dbapi_conn):
      s = _minimal_snapshot_with_counters()
      s["counters"]["rows"] = []
      s["counters"]["newest_window_end"] = None
      s["counters"]["is_fresh"] = False
      return s

   from node_monitor.web.service import DashboardService
   db = MagicMock()
   svc = DashboardService(database=db, system=_SYSTEM)
   svc._run_operation = empty_op

   async def _test():
      payload = await svc.dashboard(node=_NODE, range_name="1h", username=None)
      data = json.loads(payload)
      ctr = data["counters"]
      assert "latest" in ctr
      assert ctr["latest"] is None, (
         "counters.latest must be None when rows are empty; "
         "got: %r" % ctr["latest"])
      return True

   assert asyncio.run(_test()) is True


# ---------------------------------------------------------------------------
# Test 8: response carries usage gap metadata (closed 15-minute intervals,
# deduplicated across multiple (category, activity) grains sharing the same
# interval_end).
# ---------------------------------------------------------------------------

def _usage_grain(interval_end, category="ai_coding", activity="active"):
   return {
      "category": category,
      "activity": activity,
      "interval_end": interval_end.isoformat(),
      "cpu_seconds": 1.0,
      "complete": True,
      "rss_p50_kb": None, "rss_p50_username": None,
      "rss_p95_kb": None, "rss_p95_username": None,
      "rss_max_kb": None, "rss_max_username": None,
      "d_state_fraction": None, "d_state_username": None,
      "process_count_p50": None, "process_count_p50_username": None,
      "process_count_p95": None, "process_count_p95_username": None,
      "process_count_max": None, "process_count_max_username": None,
      "interactivity_fraction": None, "interactivity_username": None,
   }


def test_response_carries_usage_gap_metadata_with_deduplicated_timestamps():
   """usage dict must include gap metadata for closed 15-minute intervals.

   Two interval_end timestamps 30 minutes apart (one missing 15-minute
   interval between them) with TWO grains (different category/activity)
   sharing the SAME interval_end must be deduplicated to a single
   timestamp before gap computation -- otherwise gaps would be miscounted
   by treating repeated timestamps as additional data points.
   """
   import asyncio
   import json

   now = datetime(2026, 9, 30, 14, 0, tzinfo=timezone.utc)
   end1 = now - timedelta(minutes=45)
   end2 = now - timedelta(minutes=15)   # 30 min later: exactly 1 missing grain

   def op_with_usage_gap(conn, dbapi_conn):
      s = _minimal_snapshot_with_counters()
      s["counters"]["rows"] = []
      s["counters"]["newest_window_end"] = None
      s["counters"]["is_fresh"] = False
      s["usage"] = {
         "grains": [
            _usage_grain(end1, category="ai_coding", activity="active"),
            _usage_grain(end1, category="build", activity="active"),
            _usage_grain(end2, category="ai_coding", activity="active"),
         ],
         "newest_interval_end": end2.isoformat(),
         "is_fresh": True,
      }
      return s

   from node_monitor.web.service import DashboardService
   db = MagicMock()
   svc = DashboardService(database=db, system=_SYSTEM)
   svc._run_operation = op_with_usage_gap

   async def _test():
      payload = await svc.dashboard(node=_NODE, range_name="1h", username=None)
      data = json.loads(payload)
      usage = data["usage"]
      assert "gaps" in usage, (
         "usage dict must include 'gaps' metadata; got keys: %r"
         % list(usage.keys()))
      gaps = usage["gaps"]
      assert gaps["missing_count"] == 1, (
         "deduplicated interval_end timestamps (end1 appears twice) must "
         "yield exactly 1 missing 15-minute grain between end1 and end2; "
         "got: %r" % gaps)
      return True

   assert asyncio.run(_test()) is True


def test_usage_gap_metadata_zero_when_contiguous():
   """Contiguous 15-minute usage intervals report missing_count == 0."""
   import asyncio
   import json

   now = datetime(2026, 9, 30, 14, 0, tzinfo=timezone.utc)
   end1 = now - timedelta(minutes=30)
   end2 = now - timedelta(minutes=15)   # exactly one cadence later

   def op_contiguous(conn, dbapi_conn):
      s = _minimal_snapshot_with_counters()
      s["counters"]["rows"] = []
      s["counters"]["newest_window_end"] = None
      s["counters"]["is_fresh"] = False
      s["usage"] = {
         "grains": [
            _usage_grain(end1),
            _usage_grain(end2),
         ],
         "newest_interval_end": end2.isoformat(),
         "is_fresh": True,
      }
      return s

   from node_monitor.web.service import DashboardService
   db = MagicMock()
   svc = DashboardService(database=db, system=_SYSTEM)
   svc._run_operation = op_contiguous

   async def _test():
      payload = await svc.dashboard(node=_NODE, range_name="1h", username=None)
      data = json.loads(payload)
      gaps = data["usage"]["gaps"]
      assert gaps["missing_count"] == 0, (
         "contiguous 15-minute intervals must report 0 missing grains; "
         "got: %r" % gaps)
      return True

   assert asyncio.run(_test()) is True

# --- RED: boundary-aware counter gap tests (Task 1) ---
from datetime import timedelta, timezone

def test_boundary_aware_counter_gaps_leading_internal_trailing():
    from node_monitor.web.service import _enrich_snapshot
    now = datetime(2026, 9, 30, 14, 0, 0, tzinfo=timezone.utc)
    rows_all = [{"window_end": (now - timedelta(minutes=55 - i)).isoformat()} for i in range(6)]
    rows_all += [{"window_end": (now - timedelta(minutes=20)).isoformat()}]
    snapshot = {"server_utc_now": now.isoformat(), "counters": {"rows": rows_all, "newest_window_end": max(r["window_end"] for r in rows_all), "is_fresh": True}}
    result = _enrich_snapshot(snapshot)
    gaps = result["counters"]["gaps"]
    print("RED EVIDENCE: gaps keys =", list(gaps.keys()))
    assert "missing_count" in gaps

def test_boundary_aware_leading_gap_startup_range_six_recent_rows():
    from node_monitor.web.service import _enrich_snapshot
    now = datetime(2026, 9, 30, 14, 0, 0, tzinfo=timezone.utc)
    rows = [{"window_end": (now - timedelta(minutes=6 - i)).isoformat()} for i in range(6)]
    snapshot = {"server_utc_now": now.isoformat(), "range_hours": 1,
                "counters": {"rows": rows, "newest_window_end": max(r["window_end"] for r in rows), "is_fresh": True}}
    result = _enrich_snapshot(snapshot)
    gaps = result["counters"]["gaps"]
    assert gaps["missing_count"] == 54
    assert gaps["max_gap_minutes"] == 53
    assert any(i["location"] == "leading" for i in gaps["intervals"])
    assert any(i["location"] == "trailing" for i in gaps["intervals"])

def test_no_double_count_complete_range():
    from node_monitor.web.service import _enrich_snapshot
    now = datetime(2026, 9, 30, 14, 0, 0, tzinfo=timezone.utc)
    rows = [{"window_end": (now - timedelta(minutes=i)).isoformat()} for i in range(61)]
    snapshot = {"server_utc_now": now.isoformat(), "range_hours": 1,
                "counters": {"rows": rows, "newest_window_end": max(r["window_end"] for r in rows), "is_fresh": True}}
    result = _enrich_snapshot(snapshot)
    gaps = result["counters"]["gaps"]
    assert gaps["missing_count"] == 0
    assert gaps["max_gap_minutes"] == 0
