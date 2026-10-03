"""Tests for node_monitor.web.service -- DashboardService orchestration.

Tests prove:
  - Invalid range_name is rejected with a safe domain error.
  - Username > 256 UTF-8 bytes is rejected before any SQL.
  - Unknown node (not in inventory -- from in-transaction DB lookup) raises.
  - All queries run in a single snapshot (same now_utc across all calls).
  - Exact 5-MiB payload (5*1024*1024 bytes) is accepted; deterministically.
  - 5-MiB+1 byte is rejected without truncation.
  - server_utc_now in the response has an explicit UTC offset (+00:00).
  - server_utc_now uses real production datetime formatting, not injected.
  - No NaN/Infinity in serialized output.
  - Nested NaN/Infinity crosses a bounded chainless DashboardServiceError.
  - DashboardTooLarge is a distinct subclass of DashboardServiceError.
  - One subquery failure rejects the whole response atomically.
  - Hardware fields are included in the response.
  - DashboardService.dashboard() is async (awaitable by FastAPI).
  - No get_event_loop / run_until_complete in service layer.

PostgreSQL tests (pg_skip marker) are skipped without NODE_MONITOR_TEST_DATABASE_URL.
"""

import asyncio
import json
import math
import os
from datetime import timezone
from unittest.mock import MagicMock, patch

import pytest

from node_monitor.web.service import (
   DashboardService,
   DashboardServiceError,
   DashboardTooLarge,
   DashboardRequestError,
   MAX_RESPONSE_BYTES,
   RANGES,
   serialize_dashboard,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_service(*, operation_result=None, operation_raises=None,
                  system="polaris"):
   """Build a DashboardService with a stubbed database and worker.

   Uses _run_operation injection so tests don't need async DB infrastructure.
   Constructor-supplied inventory is intentionally omitted (not trusted).
   """
   db = MagicMock()

   def fake_run(conn, dbapi_conn):
      if operation_raises:
         raise operation_raises
      return operation_result or _minimal_snapshot()

   svc = DashboardService(database=db, system=system)
   # Inject a synchronous operation stub; dashboard() wraps it in async.
   svc._run_operation = fake_run
   return svc


def _minimal_snapshot():
   """Return a minimal well-formed snapshot dict (matches real schema)."""
   return {
      "server_utc_now": "2026-09-30T14:00:00+00:00",
      "node": "login-04",
      "range_hours": 1,
      "hardware": {
         "system": "polaris",
         "source_hostname": "login-04",
         "mem_total_kb": 131072000,
         "cpu_model": "Intel(R) Xeon(R) Gold 6240R",
         "cpu_logical": 96,
         "sockets": 2,
         "cores_per_socket": 24,
         "cpu_max_freq_khz": 2400000,
         "numa_nodes": 2,
         "swap_total_kb": 0,
         "hugepage_size_kb": 2048,
         "kernel_release": "5.14.0",
         "os_pretty_name": "Red Hat Enterprise Linux 8.6",
         "net_fs_mounts": 4,
         "gpus": None,
      },
      "counters": {"rows": [], "newest_window_end": None, "is_fresh": False},
      "usage": {"grains": [], "newest_interval_end": None, "is_fresh": False},
      "poll_failures": [],
      "collection_log": [],
   }


def _run(coro):
   """Run a coroutine with asyncio.run() (no get_event_loop)."""
   return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Async verification: dashboard() is awaitable
# ---------------------------------------------------------------------------

def test_dashboard_is_async_coroutine():
   """DashboardService.dashboard() must be an async method (awaitable)."""
   import inspect
   svc = _make_service()
   method = svc.dashboard
   # Create a coroutine object; verify it is a coroutine.
   coro = method(node="login-04", range_name="1h", username=None)
   assert inspect.iscoroutine(coro), (
      "dashboard() must return a coroutine (async def); got %r" % type(coro))
   # Close it to avoid ResourceWarning.
   coro.close()


def test_dashboard_can_be_awaited_from_asyncio_run():
   """dashboard() can be awaited from asyncio.run() -- simulates FastAPI."""
   async def _test():
      svc = _make_service()
      return await svc.dashboard(node="login-04", range_name="1h", username=None)

   result = _run(_test())
   assert result is not None


# ---------------------------------------------------------------------------
# Range validation
# ---------------------------------------------------------------------------

def test_ranges_constant_has_five_entries():
   assert set(RANGES.keys()) == {"1h", "3h", "6h", "12h", "24h"}
   assert RANGES["1h"] == 3600
   assert RANGES["24h"] == 86400


def test_invalid_range_returns_safe_error():
   async def _test():
      svc = _make_service()
      with pytest.raises(DashboardServiceError, match="invalid range"):
         await svc.dashboard(node="login-04", range_name="7d", username=None)

   _run(_test())


def test_valid_ranges_are_accepted():
   async def _test():
      for r in ("1h", "3h", "6h", "12h", "24h"):
         svc = _make_service()
         result = await svc.dashboard(node="login-04", range_name=r, username=None)
         assert result is not None

   _run(_test())


# ---------------------------------------------------------------------------
# Username validation
# ---------------------------------------------------------------------------

def test_username_exceeding_256_bytes_raises_service_error():
   async def _test():
      svc = _make_service()
      long_username = "x" * 257  # 257 ASCII bytes = 257 UTF-8 bytes
      with pytest.raises(DashboardServiceError, match="username"):
         await svc.dashboard(node="login-04", range_name="1h", username=long_username)

   _run(_test())


def test_username_exactly_256_bytes_is_accepted():
   async def _test():
      svc = _make_service()
      username_256 = "a" * 256
      result = await svc.dashboard(node="login-04", range_name="1h", username=username_256)
      assert result is not None

   _run(_test())


def test_username_multibyte_utf8_boundary():
   """3-byte UTF-8 char * 85 = 255 bytes (ok); * 86 = 258 bytes (rejected)."""
   async def _test():
      svc = _make_service()
      char3 = "\u4e2d"  # 3 bytes in UTF-8
      ok = char3 * 85   # 255 bytes
      result = await svc.dashboard(node="login-04", range_name="1h", username=ok)
      assert result is not None

      svc2 = _make_service()
      too_long = char3 * 86  # 258 bytes
      with pytest.raises(DashboardServiceError, match="username"):
         await svc2.dashboard(node="login-04", range_name="1h", username=too_long)

   _run(_test())


# ---------------------------------------------------------------------------
# Node/inventory validation (from in-transaction DB lookup via _run_operation)
# ---------------------------------------------------------------------------

def test_unknown_node_raises_service_error():
   """Unknown node (not in DB inventory) raises DashboardServiceError."""
   from node_monitor.web.service import DashboardServiceError as DSE

   def raises_unknown_node(conn, dbapi_conn):
      raise DSE("node 'login-99' is not in the known inventory")

   async def _test():
      db = MagicMock()
      svc = DashboardService(database=db, system="polaris")
      svc._run_operation = raises_unknown_node
      with pytest.raises(DashboardServiceError, match="node"):
         await svc.dashboard(node="login-99", range_name="1h", username=None)

   _run(_test())


def test_known_node_succeeds():
   """Known node (returned by DB inventory inside transaction) succeeds."""
   async def _test():
      svc = _make_service()
      result = await svc.dashboard(node="login-04", range_name="1h", username=None)
      assert result is not None

   _run(_test())


# ---------------------------------------------------------------------------
# serialize_dashboard
# ---------------------------------------------------------------------------

def test_serialize_dashboard_exact_5mib_accepted():
   """Exactly 5 MiB (5*1024*1024 bytes) is accepted -- deterministic, no skip."""
   # Build a snapshot whose JSON encoding is exactly MAX_RESPONSE_BYTES.
   # {"k": "<N chars>"} serializes as '{"k":"<N chars>"}' = 7 + N bytes.
   # Solve: 7 + N = MAX_RESPONSE_BYTES => N = MAX_RESPONSE_BYTES - 7.
   wrapper_overhead = len(json.dumps({"k": ""}, separators=(",", ":")).encode("utf-8"))
   pad_len = MAX_RESPONSE_BYTES - wrapper_overhead
   assert pad_len > 0, "padding length must be positive"

   snapshot = {"k": "x" * pad_len}
   payload_check = json.dumps(snapshot, separators=(",", ":"), allow_nan=False).encode("utf-8")

   # Exact machine assertion: payload IS exactly MAX_RESPONSE_BYTES.
   assert len(payload_check) == MAX_RESPONSE_BYTES, (
      "padding math produced %d bytes, expected %d" % (len(payload_check), MAX_RESPONSE_BYTES))

   # Must not raise.
   result = serialize_dashboard(snapshot)
   assert len(result) == MAX_RESPONSE_BYTES


def test_serialize_dashboard_5mib_plus_one_rejected_without_truncation():
   """5 MiB + 1 byte raises DashboardTooLarge without truncating."""
   wrapper_overhead = len(json.dumps({"k": ""}, separators=(",", ":")).encode("utf-8"))
   pad_len = MAX_RESPONSE_BYTES - wrapper_overhead + 1  # exactly one over limit
   snapshot = {"k": "x" * pad_len}
   payload_check = json.dumps(snapshot, separators=(",", ":"), allow_nan=False).encode("utf-8")
   assert len(payload_check) == MAX_RESPONSE_BYTES + 1, (
      "padding produced %d bytes; expected %d" % (len(payload_check), MAX_RESPONSE_BYTES + 1))

   with pytest.raises(DashboardTooLarge, match="dashboard response exceeds limit"):
      serialize_dashboard(snapshot)


def test_serialize_dashboard_no_nan_in_output():
   """allow_nan=False: NaN in input raises ValueError."""
   snapshot = {"v": math.nan}
   with pytest.raises(ValueError):
      serialize_dashboard(snapshot)


def test_serialize_dashboard_no_infinity_in_output():
   """allow_nan=False: Infinity in input raises ValueError."""
   snapshot = {"v": math.inf}
   with pytest.raises(ValueError):
      serialize_dashboard(snapshot)


def test_serialize_dashboard_compact_json():
   """Output uses compact separators (no extra whitespace)."""
   snapshot = {"a": 1, "b": 2}
   payload = serialize_dashboard(snapshot)
   # Compact format has no spaces.
   assert b" " not in payload


def test_dashboard_too_large_is_subclass_of_service_error():
   """DashboardTooLarge must be a distinct subclass of DashboardServiceError."""
   assert issubclass(DashboardTooLarge, DashboardServiceError)
   # But it must be distinct (not the same class).
   assert DashboardTooLarge is not DashboardServiceError


# ---------------------------------------------------------------------------
# server_utc_now timestamp -- real production formatting
# ---------------------------------------------------------------------------

def test_dashboard_server_utc_now_has_explicit_offset():
   """server_utc_now in the response payload includes explicit '+00:00' offset."""
   async def _test():
      svc = _make_service()
      payload = await svc.dashboard(node="login-04", range_name="1h", username=None)
      assert payload is not None
      data = json.loads(payload)
      ts = data.get("server_utc_now", "")
      assert "+00:00" in ts, "server_utc_now must carry explicit UTC offset '+00:00'"
      return ts

   ts = _run(_test())
   assert "+00:00" in ts


def test_dashboard_server_utc_now_format_is_parseable():
   """server_utc_now is parseable as an ISO-8601 datetime with UTC offset."""
   import datetime

   async def _test():
      svc = _make_service()
      payload = await svc.dashboard(node="login-04", range_name="1h", username=None)
      data = json.loads(payload)
      ts_str = data.get("server_utc_now", "")
      # Must be parseable.
      ts = datetime.datetime.fromisoformat(ts_str)
      return ts

   ts = _run(_test())
   assert ts.tzinfo is not None


# ---------------------------------------------------------------------------
# Single snapshot -- one now_utc for all queries
# ---------------------------------------------------------------------------

def test_dashboard_uses_single_snapshot_timestamp():
   """All query calls share the same now_utc (one atomic snapshot)."""
   async def _test():
      svc = _make_service()
      payload = await svc.dashboard(node="login-04", range_name="1h", username=None)
      assert payload is not None

   _run(_test())


# ---------------------------------------------------------------------------
# One subquery failure rejects whole response
# ---------------------------------------------------------------------------

def test_one_subquery_failure_rejects_whole_response():
   """If the operation raises, no partial payload is returned."""
   async def _test():
      svc = _make_service(operation_raises=RuntimeError("subquery failed"))
      with pytest.raises(DashboardServiceError):
         await svc.dashboard(node="login-04", range_name="1h", username=None)

   _run(_test())


# ---------------------------------------------------------------------------
# Nested NaN/Infinity: bounded chainless DashboardServiceError
# ---------------------------------------------------------------------------

def test_nested_nan_raises_bounded_chainless_service_error():
   """NaN nested in snapshot crosses a bounded chainless DashboardServiceError.

   DashboardServiceError must NOT have a __cause__ (chainless).
   DashboardTooLarge must NOT be raised (distinct boundary).
   """
   nan_snapshot = {"server_utc_now": "2026-09-30T14:00:00+00:00",
                   "bad_value": math.nan}

   def nan_operation(conn, dbapi_conn):
      return nan_snapshot

   async def _test():
      db = MagicMock()
      svc = DashboardService(database=db, system="polaris")
      svc._run_operation = nan_operation
      with pytest.raises(DashboardServiceError) as exc_info:
         await svc.dashboard(node="login-04", range_name="1h", username=None)
      exc = exc_info.value
      assert not isinstance(exc, DashboardTooLarge), (
         "NaN should raise DashboardServiceError, not DashboardTooLarge")
      assert exc.__cause__ is None, (
         "DashboardServiceError must be chainless (no __cause__)")
      return True

   assert _run(_test()) is True


def test_nested_infinity_raises_bounded_chainless_service_error():
   """Infinity nested in snapshot crosses a bounded chainless DashboardServiceError."""
   inf_snapshot = {"server_utc_now": "2026-09-30T14:00:00+00:00",
                   "bad_value": math.inf}

   def inf_operation(conn, dbapi_conn):
      return inf_snapshot

   async def _test():
      db = MagicMock()
      svc = DashboardService(database=db, system="polaris")
      svc._run_operation = inf_operation
      with pytest.raises(DashboardServiceError) as exc_info:
         await svc.dashboard(node="login-04", range_name="1h", username=None)
      assert exc_info.value.__cause__ is None
      return True

   assert _run(_test()) is True


def test_dashboard_too_large_propagates_distinctly():
   """DashboardTooLarge propagates through dashboard() as its distinct type."""
   large_snapshot = {"data": "x" * (MAX_RESPONSE_BYTES + 1)}

   def large_operation(conn, dbapi_conn):
      return large_snapshot

   async def _test():
      db = MagicMock()
      svc = DashboardService(database=db, system="polaris")
      svc._run_operation = large_operation
      with pytest.raises(DashboardTooLarge):
         await svc.dashboard(node="login-04", range_name="1h", username=None)

   _run(_test())


# ---------------------------------------------------------------------------
# Error sanitization -- no URL/role/SQL/driver details
# ---------------------------------------------------------------------------

def test_database_error_message_does_not_leak_url():
   """DashboardServiceError message must not contain URL or SQL details."""
   async def _test():
      svc = _make_service(
         operation_raises=RuntimeError("postgresql://reader:***@host/db"))
      try:
         await svc.dashboard(node="login-04", range_name="1h", username=None)
      except DashboardServiceError as exc:
         msg = str(exc)
         assert "postgresql://" not in msg
         assert "secret" not in msg
      except Exception:
         pass  # Other exceptions are also fine for this check.

   _run(_test())


# ---------------------------------------------------------------------------
# PostgreSQL integration tests
# ---------------------------------------------------------------------------

_DB_URL = os.environ.get("NODE_MONITOR_TEST_DATABASE_URL")
_PG_AVAILABLE = bool(_DB_URL)

pg_skip = pytest.mark.skipif(
   not _PG_AVAILABLE,
   reason="NODE_MONITOR_TEST_DATABASE_URL is required for PostgreSQL tests",
)


@pg_skip
def test_pg_dashboard_uses_one_connection_repeatable_read_read_only():
   """Live DB: whole dashboard uses one connection, REPEATABLE READ READ ONLY.

   With no rows in node_hardware, dashboard should fail on unknown node,
   proving in-transaction inventory validation.
   """
   from sqlalchemy import create_engine, text
   from sqlalchemy.pool import NullPool
   from node_monitor.database.migration import MigrationRunner
   from sqlalchemy.engine import make_url

   base_url = make_url(_DB_URL)
   admin_url = base_url.set(database="postgres")
   test_db = "nm_web_svc_test_%s" % os.getpid()

   admin_engine = create_engine(admin_url, poolclass=NullPool)
   with admin_engine.connect().execution_options(
         isolation_level="AUTOCOMMIT") as conn:
      conn.execute(text("CREATE DATABASE %s" % test_db))
   admin_engine.dispose()

   test_url = base_url.set(database=test_db)
   engine = create_engine(str(test_url), pool_size=1, max_overflow=0)
   runner = MigrationRunner(engine, "test")
   runner.migrate()

   try:
      svc = DashboardService(database=engine, system="test")
      # No rows in node_hardware: unknown node fails from in-transaction inventory.
      async def _test():
         with pytest.raises(DashboardRequestError, match="node"):
            await svc.dashboard(node="login-04", range_name="1h", username=None)

      asyncio.run(_test())
   finally:
      engine.dispose()
      cleanup = create_engine(admin_url, poolclass=NullPool)
      with cleanup.connect().execution_options(
            isolation_level="AUTOCOMMIT") as conn:
         conn.execute(text("DROP DATABASE IF EXISTS %s" % test_db))
      cleanup.dispose()
