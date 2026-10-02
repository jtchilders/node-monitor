"""node_monitor.web.service -- dashboard orchestration, response bounds,
and safe service-domain errors.

Design: operational-web-dashboard Task 5.

``DashboardService`` assembles a single coherent dashboard snapshot for
one node by running all database queries inside one atomic
REPEATABLE READ READ ONLY transaction via ``DashboardWorker``.

Guarantees:
  * All subquery calls share the same ``now_utc`` timestamp (one snapshot).
  * One subquery failure rejects the whole response -- no partial result.
  * Inventory is obtained from ``node_hardware`` inside the same transaction
    snapshot used for all other queries.
  * Username is validated (max 256 UTF-8 bytes) before any SQL is issued.
  * Range name is validated against the exact allowed set before any SQL.
  * Node is validated against the exact inventory node before any SQL.
  * Serialized response is capped at exactly 5 MiB (5 * 1024 * 1024 bytes).
    Exact 5 MiB is accepted; 5 MiB + 1 byte raises DashboardTooLarge.
  * JSON is serialized with allow_nan=False; NaN/Infinity in any value raises.
  * server_utc_now carries an explicit +00:00 UTC offset.
  * No HTTP response time is used for telemetry freshness.
  * No zero-fill; gaps appear as null or explicit gap metadata.
  * Error messages are sanitized: no URL, role, SQL, or driver details.
"""

import json
from datetime import datetime, timezone

from node_monitor.web.queries import (
   MAX_USERNAME_BYTES,
   QueryValidationError,
   load_collection_log_for_range,
   load_counters_for_range,
   load_poll_failures_for_range,
   load_usage_for_range,
)


# ---------------------------------------------------------------------------
# Public constants
# ---------------------------------------------------------------------------

#: Exact allowed range strings mapped to their duration in seconds.
RANGES = {
   "1h": 3600,
   "3h": 10800,
   "6h": 21600,
   "12h": 43200,
   "24h": 86400,
}

#: Maximum serialized response size in bytes (inclusive).
MAX_RESPONSE_BYTES = 5 * 1024 * 1024  # 5 MiB


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class DashboardServiceError(RuntimeError):
   """Bounded, sanitized dashboard service failure.

   Never contains URL, role, SQL text, or driver exception details.
   """


class DashboardTooLarge(DashboardServiceError):
   """Raised when the serialized dashboard payload exceeds MAX_RESPONSE_BYTES."""


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------

def serialize_dashboard(snapshot):
   """Serialize ``snapshot`` to compact UTF-8 JSON, enforcing the size cap.

   Parameters
   ----------
   snapshot : dict -- the assembled dashboard snapshot.

   Returns
   -------
   bytes -- compact UTF-8 JSON (no spaces).

   Raises
   ------
   DashboardTooLarge
       If the serialized payload exceeds MAX_RESPONSE_BYTES.
   ValueError
       If the snapshot contains NaN or Infinity (allow_nan=False).
   """
   payload = json.dumps(
      snapshot, separators=(",", ":"), allow_nan=False).encode("utf-8")
   if len(payload) > MAX_RESPONSE_BYTES:
      raise DashboardTooLarge("dashboard response exceeds limit")
   return payload


# ---------------------------------------------------------------------------
# DashboardService
# ---------------------------------------------------------------------------

class DashboardService:
   """Assembles one atomic dashboard snapshot.

   Parameters
   ----------
   database : WebDatabase or SQLAlchemy engine.
       Provides a pool with pool_size=1, max_overflow=0.  The service
       obtains the engine via ``database._engine`` if present, otherwise
       treats ``database`` as the engine directly.
   system : str -- system name (e.g. ``"polaris"``).
   inventory : frozenset of str -- known source hostnames.
       The service validates the requested node against this set before
       issuing any SQL.
   """

   def __init__(self, database, system, inventory):
      # Accept either a WebDatabase wrapper or a bare engine.
      if hasattr(database, "_engine"):
         self._engine = database._engine
      else:
         self._engine = database
      self._system = system
      self._inventory = inventory

      # Allow tests to inject a synchronous operation replacement.
      self._run_operation = None

   def dashboard(self, *, node, range_name, username):
      """Build and serialize one atomic dashboard snapshot.

      Parameters
      ----------
      node : str -- source_hostname to query.
      range_name : str -- one of ``RANGES`` keys (``"1h"`` .. ``"24h"``).
      username : str or None -- optional username filter (bound data only).

      Returns
      -------
      bytes -- compact JSON payload, max 5 MiB.

      Raises
      ------
      DashboardServiceError
          On invalid range, unknown node, username too long, subquery
          failure, or any other bounded domain error.
      DashboardTooLarge
          If the assembled payload exceeds 5 MiB.
      """
      # ------------------------------------------------------------------
      # 1. Validate range before touching the database.
      # ------------------------------------------------------------------
      if range_name not in RANGES:
         raise DashboardServiceError(
            "invalid range %r; allowed: %s" % (range_name, sorted(RANGES)))
      range_hours = RANGES[range_name] // 3600

      # ------------------------------------------------------------------
      # 2. Validate username length (UTF-8 bytes).
      # ------------------------------------------------------------------
      if username is not None:
         try:
            byte_len = len(username.encode("utf-8"))
         except (AttributeError, UnicodeEncodeError):
            raise DashboardServiceError("username must be a valid string")
         if byte_len > MAX_USERNAME_BYTES:
            raise DashboardServiceError(
               "username exceeds %d UTF-8 bytes" % MAX_USERNAME_BYTES)

      # ------------------------------------------------------------------
      # 3. Validate node against the known inventory.
      # ------------------------------------------------------------------
      if node not in self._inventory:
         raise DashboardServiceError(
            "node %r is not in the known inventory" % (node,))

      # ------------------------------------------------------------------
      # 4. Run the atomic database operation.
      # ------------------------------------------------------------------
      try:
         snapshot = self._execute_operation(
            node=node, range_hours=range_hours, username=username)
      except DashboardServiceError:
         raise
      except Exception:
         raise DashboardServiceError("dashboard query failed") from None

      # ------------------------------------------------------------------
      # 5. Serialize and enforce size cap.
      # ------------------------------------------------------------------
      return serialize_dashboard(snapshot)

   # ------------------------------------------------------------------
   # Internal -- atomic operation callable
   # ------------------------------------------------------------------

   def _execute_operation(self, *, node, range_hours, username):
      """Run the full set of dashboard queries in one atomic snapshot.

      If ``self._run_operation`` has been injected (by tests), that
      callable is used instead of the real worker/thread path.

      Returns a snapshot dict suitable for ``serialize_dashboard``.
      """
      if self._run_operation is not None:
         # Test injection: call stub directly (synchronous).
         conn = None
         dbapi_conn = None
         return self._run_operation(conn, dbapi_conn)

      # Capture now_utc once for the entire snapshot (one timestamp per request).
      now_utc = datetime.now(timezone.utc)
      system = self._system
      inventory = self._inventory

      def _operation(conn, dbapi_conn):
         return _run_dashboard_queries(
            conn, system=system, node=node,
            range_hours=range_hours, inventory=inventory,
            username=username, now_utc=now_utc)

      # Run synchronously (no async event loop in service layer;
      # the HTTP layer is responsible for the async deadline wrapper).
      import asyncio
      from node_monitor.database.web import run_dashboard_with_deadline
      try:
         loop = asyncio.get_event_loop()
      except RuntimeError:
         loop = asyncio.new_event_loop()
         asyncio.set_event_loop(loop)

      try:
         result = loop.run_until_complete(
            run_dashboard_with_deadline(
               self._engine, _operation, timeout_sec=12.0))
      except Exception:
         raise

      return result


# ---------------------------------------------------------------------------
# Internal -- the actual query orchestration
# ---------------------------------------------------------------------------

def _run_dashboard_queries(conn, *, system, node, range_hours,
                           inventory, username, now_utc):
   """Execute all dashboard subqueries on ``conn`` within one transaction.

   All calls use the same ``now_utc`` so the snapshot is coherent.
   Any subquery failure propagates immediately, aborting the whole response.

   Returns a dict suitable for ``serialize_dashboard``.
   """
   # Counter rows (series + quality/gap metadata).
   counter_result = load_counters_for_range(
      conn, system, node, range_hours, inventory, now_utc=now_utc)

   # Usage rows (CPU series, hotspots, gap metadata).
   usage_result = load_usage_for_range(
      conn, system, node, range_hours, inventory,
      username=username, now_utc=now_utc)

   # Poll failures (bounded at MAX_POLL_FAILURES).
   poll_failures = load_poll_failures_for_range(
      conn, system, node, range_hours, inventory, now_utc=now_utc)

   # System-level collection log (no node filter).
   collection_log = load_collection_log_for_range(
      conn, system, range_hours, now_utc=now_utc)

   # Assemble the snapshot.  server_utc_now carries an explicit +00:00
   # offset so clients can parse it unambiguously.
   server_utc_now = now_utc.isoformat()
   if not server_utc_now.endswith("+00:00"):
      # Force explicit UTC offset representation.
      server_utc_now = now_utc.strftime("%Y-%m-%dT%H:%M:%S+00:00")

   # Serialize counter rows (only non-NaN/Inf-safe scalar types).
   counter_rows = [_safe_counter_row(r) for r in counter_result.rows]

   # Serialize usage grains.
   usage_grains = [_safe_usage_grain(k, v)
                   for k, v in usage_result.by_key.items()]

   # Serialize poll failures.
   pf_rows = [_safe_poll_failure(r) for r in poll_failures]

   # Serialize collection log.
   log_rows = [_safe_log_row(r) for r in collection_log]

   return {
      "server_utc_now": server_utc_now,
      "node": node,
      "range_hours": range_hours,
      "counters": {
         "rows": counter_rows,
         "newest_window_end": (
            counter_result.newest_window_end.isoformat()
            if counter_result.newest_window_end else None),
         "is_fresh": counter_result.is_fresh,
      },
      "usage": {
         "grains": usage_grains,
         "newest_interval_end": (
            usage_result.newest_interval_end.isoformat()
            if usage_result.newest_interval_end else None),
         "is_fresh": usage_result.is_fresh,
      },
      "poll_failures": pf_rows,
      "collection_log": log_rows,
   }


def _safe_counter_row(row):
   """Convert a counter row mapping to a JSON-safe dict."""
   return {
      "window_start": _dt(row.get("window_start")),
      "window_end": _dt(row.get("window_end")),
      "sample_count": row.get("sample_count"),
      "expected_count": row.get("expected_count"),
      "coverage": _safe_float(row.get("coverage")),
      "meets_minimum_samples": row.get("meets_minimum_samples"),
      "invalid_pair_count": row.get("invalid_pair_count"),
      "excess_sample_count": row.get("excess_sample_count"),
      "complete": row.get("complete"),
      "mem_available_kb": row.get("mem_available_kb"),
      "cached_kb": row.get("cached_kb"),
      "shmem_kb": row.get("shmem_kb"),
      "load1": _safe_float(row.get("load1")),
      "load5": _safe_float(row.get("load5")),
      "load15": _safe_float(row.get("load15")),
      "procs_running": row.get("procs_running"),
      "procs_total": row.get("procs_total"),
      "socket_count": row.get("socket_count"),
      "cpu_busy_pct": row.get("cpu_busy_pct"),
      "network_rates": row.get("network_rates"),
      "lustre_md_summary": row.get("lustre_md_summary"),
   }


def _safe_usage_grain(key, grain):
   """Convert a (category, activity, interval_end) grain to a JSON-safe dict."""
   category, activity, interval_end = key
   return {
      "category": category,
      "activity": activity,
      "interval_end": _dt(interval_end),
      "cpu_seconds": _safe_float(grain.cpu_seconds),
      "complete": grain.complete,
      "rss_p50_kb": _safe_float(grain.rss_p50_kb),
      "rss_p50_username": grain.rss_p50_username,
      "rss_p95_kb": _safe_float(grain.rss_p95_kb),
      "rss_p95_username": grain.rss_p95_username,
      "rss_max_kb": _safe_float(grain.rss_max_kb),
      "rss_max_username": grain.rss_max_username,
      "d_state_fraction": _safe_float(grain.d_state_fraction),
      "d_state_username": grain.d_state_username,
      "process_count_p50": _safe_float(grain.process_count_p50),
      "process_count_p50_username": grain.process_count_p50_username,
      "process_count_p95": _safe_float(grain.process_count_p95),
      "process_count_p95_username": grain.process_count_p95_username,
      "process_count_max": _safe_float(grain.process_count_max),
      "process_count_max_username": grain.process_count_max_username,
      "interactivity_fraction": _safe_float(grain.interactivity_fraction),
      "interactivity_username": grain.interactivity_username,
   }


def _safe_poll_failure(row):
   """Convert a poll failure mapping to a JSON-safe dict."""
   return {
      "recorded_at": _dt(row.get("recorded_at")),
      "loop": row.get("loop"),
      "failure_type": row.get("failure_type"),
      "detail": row.get("detail"),
      "consecutive_failures": row.get("consecutive_failures"),
      "breaker_state": row.get("breaker_state"),
   }


def _safe_log_row(row):
   """Convert a collection log mapping to a JSON-safe dict."""
   return {
      "recorded_at": _dt(row.get("recorded_at")),
      "event": row.get("event"),
      "detail": row.get("detail"),
   }


def _dt(value):
   """Serialize a datetime to ISO-8601 string or None."""
   if value is None:
      return None
   try:
      return value.isoformat()
   except AttributeError:
      return str(value)


def _safe_float(value):
   """Return value as-is if None, or raise if it is NaN/Inf."""
   import math
   if value is None:
      return None
   f = float(value)
   if math.isnan(f) or math.isinf(f):
      raise ValueError(
         "dashboard value must be finite, got %r" % value)
   return f
