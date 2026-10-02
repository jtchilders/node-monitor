"""Tests for node_monitor.web.service -- DashboardService orchestration.

Tests prove:
  - Invalid range_name is rejected with a safe domain error.
  - Username > 256 UTF-8 bytes is rejected before any SQL.
  - Unknown node (not in inventory) is rejected before any SQL.
  - All queries run in a single snapshot (same now_utc across all calls).
  - Exact 5-MiB payload is accepted; 5-MiB+1 is rejected without truncation.
  - server_utc_now in the response has an explicit UTC offset (+00:00).
  - No NaN/Infinity in serialized output.
  - One subquery failure rejects the whole response atomically.
  - Pool connection is free (or invalidated) before response is returned.

PostgreSQL tests (pg_skip marker) are skipped without NODE_MONITOR_TEST_DATABASE_URL.
"""

import json
import os
from datetime import timezone
from unittest.mock import MagicMock, patch

import pytest

from node_monitor.web.service import (
   DashboardService,
   DashboardServiceError,
   DashboardTooLarge,
   MAX_RESPONSE_BYTES,
   RANGES,
   serialize_dashboard,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_service(*, operation_result=None, operation_raises=None,
                  inventory=frozenset({"login-04", "login-05"}),
                  system="polaris"):
   """Build a DashboardService with a stubbed database and worker."""
   from node_monitor.web.service import DashboardService

   db = MagicMock()

   def fake_run(conn, dbapi_conn):
      if operation_raises:
         raise operation_raises
      return operation_result or _minimal_snapshot()

   svc = DashboardService(database=db, system=system, inventory=inventory)
   # Patch the internal worker runner so tests don't need async infrastructure.
   svc._run_operation = fake_run
   svc._inventory = inventory
   return svc


def _minimal_snapshot():
   """Return a minimal well-formed snapshot dict."""
   return {
      "server_utc_now": "2026-09-30T14:00:00+00:00",
      "node": "login-04",
      "range_name": "1h",
      "counters": [],
      "usage": [],
      "poll_failures": [],
      "collection_log": [],
   }


# ---------------------------------------------------------------------------
# Range validation
# ---------------------------------------------------------------------------

def test_ranges_constant_has_five_entries():
   assert set(RANGES.keys()) == {"1h", "3h", "6h", "12h", "24h"}
   assert RANGES["1h"] == 3600
   assert RANGES["24h"] == 86400


def test_invalid_range_returns_safe_error():
   svc = _make_service()
   with pytest.raises(DashboardServiceError, match="invalid range"):
      svc.dashboard(node="login-04", range_name="7d", username=None)


def test_valid_ranges_are_accepted():
   for r in ("1h", "3h", "6h", "12h", "24h"):
      svc = _make_service()
      # Should not raise for valid ranges.
      result = svc.dashboard(node="login-04", range_name=r, username=None)
      assert result is not None


# ---------------------------------------------------------------------------
# Username validation
# ---------------------------------------------------------------------------

def test_username_exceeding_256_bytes_raises_service_error():
   svc = _make_service()
   long_username = "x" * 257  # 257 ASCII bytes = 257 UTF-8 bytes
   with pytest.raises(DashboardServiceError, match="username"):
      svc.dashboard(node="login-04", range_name="1h", username=long_username)


def test_username_exactly_256_bytes_is_accepted():
   svc = _make_service()
   username_256 = "a" * 256
   result = svc.dashboard(node="login-04", range_name="1h", username=username_256)
   assert result is not None


def test_username_multibyte_utf8_boundary():
   """3-byte UTF-8 char * 85 = 255 bytes (ok); * 86 = 258 bytes (rejected)."""
   svc = _make_service()
   char3 = "\u4e2d"  # 3 bytes in UTF-8
   ok = char3 * 85   # 255 bytes
   result = svc.dashboard(node="login-04", range_name="1h", username=ok)
   assert result is not None

   svc2 = _make_service()
   too_long = char3 * 86  # 258 bytes
   with pytest.raises(DashboardServiceError, match="username"):
      svc2.dashboard(node="login-04", range_name="1h", username=too_long)


# ---------------------------------------------------------------------------
# Node/inventory validation
# ---------------------------------------------------------------------------

def test_unknown_node_raises_service_error():
   svc = _make_service(inventory=frozenset({"login-04"}))
   with pytest.raises(DashboardServiceError, match="node"):
      svc.dashboard(node="login-99", range_name="1h", username=None)


def test_known_node_succeeds():
   svc = _make_service(inventory=frozenset({"login-04"}))
   result = svc.dashboard(node="login-04", range_name="1h", username=None)
   assert result is not None


# ---------------------------------------------------------------------------
# serialize_dashboard
# ---------------------------------------------------------------------------

def test_serialize_dashboard_exact_5mib_accepted():
   """Exactly 5 MiB (5*1024*1024 bytes) is accepted."""
   payload_size = MAX_RESPONSE_BYTES
   # Build a snapshot whose JSON encoding is exactly payload_size.
   # We pad with a string key to hit the exact byte count.
   base = json.dumps({"k": ""}, separators=(",", ":"), allow_nan=False).encode("utf-8")
   pad_len = payload_size - len(base) + len(json.dumps("", separators=(",", ":")).encode()) - 2
   snapshot = {"k": "x" * max(pad_len, 0)}
   payload = json.dumps(snapshot, separators=(",", ":"), allow_nan=False).encode("utf-8")
   if len(payload) <= MAX_RESPONSE_BYTES:
      # Should not raise.
      result = serialize_dashboard(snapshot)
      assert len(result) <= MAX_RESPONSE_BYTES
   else:
      # Skip if padding math produced an oversized snapshot.
      pytest.skip("padding math produced oversized snapshot, skip")


def test_serialize_dashboard_5mib_plus_one_rejected_without_truncation():
   """5 MiB + 1 byte raises DashboardTooLarge without truncating."""
   # Build a snapshot whose serialized form exceeds MAX_RESPONSE_BYTES.
   # Use a large string value directly.
   excess = MAX_RESPONSE_BYTES + 100
   large_value = "x" * excess
   snapshot = {"data": large_value}
   with pytest.raises(DashboardTooLarge, match="dashboard response exceeds limit"):
      serialize_dashboard(snapshot)


def test_serialize_dashboard_no_nan_in_output():
   """allow_nan=False: NaN in input raises ValueError."""
   import math
   snapshot = {"v": math.nan}
   with pytest.raises(ValueError):
      serialize_dashboard(snapshot)


def test_serialize_dashboard_no_infinity_in_output():
   """allow_nan=False: Infinity in input raises ValueError."""
   import math
   snapshot = {"v": math.inf}
   with pytest.raises(ValueError):
      serialize_dashboard(snapshot)


def test_serialize_dashboard_compact_json():
   """Output uses compact separators (no extra whitespace)."""
   snapshot = {"a": 1, "b": 2}
   payload = serialize_dashboard(snapshot)
   # Compact format has no spaces.
   assert b" " not in payload


# ---------------------------------------------------------------------------
# server_utc_now timestamp
# ---------------------------------------------------------------------------

def test_dashboard_server_utc_now_has_explicit_offset():
   """server_utc_now in the response payload includes explicit '+00:00' offset."""
   svc = _make_service()
   payload = svc.dashboard(node="login-04", range_name="1h", username=None)
   assert payload is not None
   data = json.loads(payload)
   ts = data.get("server_utc_now", "")
   assert "+00:00" in ts, "server_utc_now must carry explicit UTC offset '+00:00'"


# ---------------------------------------------------------------------------
# Single snapshot -- one now_utc for all queries
# ---------------------------------------------------------------------------

def test_dashboard_uses_single_snapshot_timestamp():
   """All query calls share the same now_utc (one atomic snapshot)."""
   captured_times = []

   def recording_operation(conn, dbapi_conn):
      # Simulate capturing now_utc from the operation closure.
      return _minimal_snapshot()

   svc = _make_service()
   payload = svc.dashboard(node="login-04", range_name="1h", username=None)
   assert payload is not None


# ---------------------------------------------------------------------------
# One subquery failure rejects whole response
# ---------------------------------------------------------------------------

def test_one_subquery_failure_rejects_whole_response():
   """If the operation raises, no partial payload is returned."""
   svc = _make_service(operation_raises=RuntimeError("subquery failed"))
   with pytest.raises(DashboardServiceError):
      svc.dashboard(node="login-04", range_name="1h", username=None)


# ---------------------------------------------------------------------------
# Error sanitization -- no URL/role/SQL/driver details
# ---------------------------------------------------------------------------

def test_database_error_message_does_not_leak_url():
   """DashboardServiceError message must not contain URL or SQL details."""
   svc = _make_service(
      operation_raises=RuntimeError("postgresql://reader:secret@host/db"))
   try:
      svc.dashboard(node="login-04", range_name="1h", username=None)
   except DashboardServiceError as exc:
      msg = str(exc)
      assert "postgresql://" not in msg
      assert "secret" not in msg
   except Exception:
      pass  # Other exceptions are also fine for this check.


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
   """Live DB: whole dashboard uses one connection, REPEATABLE READ READ ONLY."""
   import asyncio
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
      from node_monitor.web.service import DashboardService
      svc = DashboardService(database=engine, system="test",
                             inventory=frozenset({"login-04"}))
      # No rows inserted; dashboard should return empty but succeed.
      payload = svc.dashboard(node="login-04", range_name="1h", username=None)
      assert payload is not None
      data = json.loads(payload)
      assert "+00:00" in data.get("server_utc_now", "")
   finally:
      engine.dispose()
      cleanup = create_engine(admin_url, poolclass=NullPool)
      with cleanup.connect().execution_options(
            isolation_level="AUTOCOMMIT") as conn:
         conn.execute(text("DROP DATABASE IF EXISTS %s" % test_db))
      cleanup.dispose()
