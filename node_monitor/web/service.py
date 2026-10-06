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
    snapshot used for all other queries.  Constructor-supplied inventory is
    NOT trusted for node validation.
  * Inside the same REPEATABLE READ READ ONLY transaction, a fixed
    parameterized SELECT against node_monitor.node_hardware for exact
    (system, source_hostname) validates the node and fetches required
    hardware fields (mem_total_kb and other hardware context).
  * Username is validated (max 256 UTF-8 bytes) before any SQL is issued.
  * Range name is validated against the exact allowed set before any SQL.
  * Node validation from in-transaction inventory: unknown node fails from
    inside the transaction.
  * Serialized response is capped at exactly 5 MiB (5 * 1024 * 1024 bytes).
    Exact 5 MiB is accepted; 5 MiB + 1 byte raises DashboardTooLarge.
  * JSON is serialized with allow_nan=False; NaN/Infinity in any value raises.
    Nested NaN/Infinity and serialization ValueError/TypeError cross a
    bounded chainless DashboardServiceError boundary.  DashboardTooLarge
    remains a distinct subclass.
  * server_utc_now carries an explicit +00:00 UTC offset, produced by real
    production datetime formatting (not injected snapshots).
  * No HTTP response time is used for telemetry freshness.
  * No zero-fill; gaps appear as null or explicit gap metadata.
  * Error messages are sanitized: no URL, role, SQL, or driver details.
  * Hardware fields (mem_total_kb and context) are included in the response.
  * DashboardService.dashboard is async; it must be awaited by FastAPI.
    No get_event_loop / run_until_complete logic anywhere in this module.
"""

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

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
MAX_NODE_INVENTORY_BYTES = 64 * 1024
MAX_NODE_INVENTORY_COUNT = 128

# Fixed parameterized SQL for in-transaction node inventory + hardware lookup.
# Runs inside the same REPEATABLE READ READ ONLY snapshot as all other queries.
_HARDWARE_SQL = text(
   "SELECT system, source_hostname, "
   "       cpu_model, cpu_logical, sockets, cores_per_socket, "
   "       cpu_max_freq_khz, numa_nodes, mem_total_kb, swap_total_kb, "
   "       hugepage_size_kb, kernel_release, os_pretty_name, "
   "       net_fs_mounts, gpus "
   "FROM node_monitor.node_hardware "
   "WHERE system = :system AND source_hostname = :node"
)

_NODES_SQL = text(
   "SELECT DISTINCT source_hostname "
   "FROM node_monitor.node_hardware "
   "WHERE system = :system "
   "ORDER BY source_hostname "
   "LIMIT 129"
)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class DashboardServiceError(RuntimeError):
   """Bounded, sanitized dashboard service failure."""


class DashboardRequestError(DashboardServiceError):
   """Bounded, sanitized dashboard service failure.

   Never contains URL, role, SQL text, or driver exception details.
   """


class DashboardTooLarge(DashboardServiceError):
   """Raised when the serialized dashboard payload exceeds MAX_RESPONSE_BYTES."""


class NodesServiceError(DashboardServiceError):
   """Bounded, sanitized node-inventory failure."""


class NodesTooLarge(NodesServiceError):
   """Raised when node inventory exceeds its count or byte bound."""


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
   inventory : frozenset of str, optional -- ignored for node validation.
      Constructor-supplied inventory is NOT trusted.  Node validation
      uses an in-transaction SELECT against node_hardware.  This
      parameter is accepted for API compatibility only.
   config_nodes : tuple of NodeConfig, optional -- display metadata only.
      These entries never establish availability; database rows do.
   """

   def __init__(self, database, system, inventory=None, config_nodes=()):
      # Accept either a WebDatabase wrapper or a bare engine.
      if hasattr(database, "_engine"):
         self._engine = database._engine
      else:
         self._engine = database
      self._system = system
      # inventory is ignored for validation; kept only for API compat.
      self._inventory = inventory
      self._config_nodes = tuple(config_nodes)

      # Allow tests to inject a synchronous operation replacement.
      # When set, dashboard() calls this instead of the real worker.
      self._run_operation = None

   async def dashboard(self, *, node, range_name, username):
      """Build and serialize one atomic dashboard snapshot.

      This is an async method; it must be awaited by FastAPI.
      No get_event_loop / run_until_complete logic is used.

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
         raise DashboardRequestError(
            "invalid range %r; allowed: %s" % (range_name, sorted(RANGES)))
      range_hours = RANGES[range_name] // 3600

      # ------------------------------------------------------------------
      # 2. Validate username length (UTF-8 bytes).
      # ------------------------------------------------------------------
      if username is not None:
         try:
            byte_len = len(username.encode("utf-8"))
         except (AttributeError, UnicodeEncodeError):
            raise DashboardRequestError("username must be a valid string")
         if byte_len > MAX_USERNAME_BYTES:
            raise DashboardRequestError(
               "username exceeds %d UTF-8 bytes" % MAX_USERNAME_BYTES)

      # ------------------------------------------------------------------
      # 3. Run the atomic database operation (includes in-transaction
      #    inventory/hardware lookup and node validation).
      # ------------------------------------------------------------------
      try:
         snapshot = await self._execute_operation(
            node=node, range_hours=range_hours, username=username)
      except DashboardServiceError:
         raise
      except Exception:
         raise DashboardServiceError("dashboard query failed") from None

      # ------------------------------------------------------------------
      # 4. Enrich snapshot with response-contract metadata (gaps, status,
      #    latest gauges, mem_used_physical_kb).
      # ------------------------------------------------------------------
      snapshot = _enrich_snapshot(snapshot)

      # ------------------------------------------------------------------
      # 5. Serialize and enforce size cap.
      #    Nested NaN/Infinity and TypeError cross a bounded chainless
      #    DashboardServiceError boundary; DashboardTooLarge is distinct.
      # ------------------------------------------------------------------
      try:
         return serialize_dashboard(snapshot)
      except DashboardTooLarge:
         raise
      except (ValueError, TypeError):
         raise DashboardServiceError(
            "dashboard serialization failed") from None

   async def nodes(self):
      """Return compact, bounded JSON for exact database-backed node IDs."""
      def _operation(conn, dbapi_conn):
         rows = conn.execute(_NODES_SQL, {"system": self._system}).all()
         hostnames = [row[0] for row in rows]
         if len(hostnames) > MAX_NODE_INVENTORY_COUNT:
            raise NodesTooLarge("node inventory exceeds limit")
         return hostnames

      try:
         from node_monitor.database.web import run_dashboard_with_deadline
         hostnames = await run_dashboard_with_deadline(
            self._engine, _operation, timeout_sec=12.0)
         response = {
            "system": self._system,
            "nodes": _build_node_inventory(
               self._system, hostnames, self._config_nodes),
         }
         payload = json.dumps(
            response, separators=(",", ":"), allow_nan=False).encode("utf-8")
         if len(payload) > MAX_NODE_INVENTORY_BYTES:
            raise NodesTooLarge("node inventory exceeds limit")
         return payload
      except NodesServiceError:
         raise
      except Exception:
         raise NodesServiceError("node inventory query failed") from None

   # ------------------------------------------------------------------
   # Internal -- async operation dispatch
   # ------------------------------------------------------------------

   async def _execute_operation(self, *, node, range_hours, username):
      """Run the full set of dashboard queries in one atomic snapshot.

      If ``self._run_operation`` has been injected (by tests), that
      callable is used instead of the real worker/async path.

      Returns a snapshot dict suitable for ``serialize_dashboard``.
      """
      if self._run_operation is not None:
         # Test injection: call stub directly (synchronous shim).
         conn = None
         dbapi_conn = None
         return self._run_operation(conn, dbapi_conn)

      # Capture now_utc once for the entire snapshot (one timestamp per request).
      now_utc = datetime.now(timezone.utc)
      system = self._system

      def _operation(conn, dbapi_conn):
         return _run_dashboard_queries(
            conn, system=system, node=node,
            range_hours=range_hours,
            username=username, now_utc=now_utc)

      # Import here to avoid circular imports at module load time.
      from node_monitor.database.web import run_dashboard_with_deadline
      return await run_dashboard_with_deadline(
         self._engine, _operation, timeout_sec=12.0)


def _conservative_node_label(system, hostname):
   prefix = "%s-login-" % system
   first_component = hostname.split(".", 1)[0]
   if first_component.startswith(prefix):
      suffix = first_component[len(prefix):]
      if suffix.isdigit():
         return "login-%s" % suffix
   return hostname


def _build_node_inventory(system, hostnames, config_nodes):
   config_by_hostname = {node.hostname: node for node in config_nodes}
   available = set(hostnames)
   ordered = [
      node.hostname for node in config_nodes if node.hostname in available]
   ordered.extend(sorted(available.difference(ordered)))
   result = []
   for hostname in ordered:
      configured = config_by_hostname.get(hostname)
      label = (
         configured.display_name
         if configured is not None and configured.display_name is not None
         else _conservative_node_label(system, hostname))
      result.append({
         "id": hostname,
         "label": label,
         "configured": configured is not None,
         "role": configured.role if configured is not None else None,
      })
   return result


# ---------------------------------------------------------------------------
# Internal -- snapshot enrichment (response-contract metadata)
# ---------------------------------------------------------------------------

#: Expected cadence for counter rows (1 window per minute = 60 seconds).
#: This is the schema-defined counter window width (node_counter_minute).
_COUNTER_CADENCE_SECONDS = 60

#: Expected cadence for usage interval_end timestamps (closed 15-minute
#: intervals).  This matches the collector's usage_interval_sec default
#: (see node_monitor/config.py: usage_interval_sec = 900) and the stored
#: node_usage_intervals grain: multiple (category, activity) rows share
#: the SAME interval_end per 15-minute closing, so timestamps are
#: deduplicated before gap computation to avoid miscounting repeated
#: interval_end values as additional data points.
_USAGE_CADENCE_SECONDS = 900


def _parse_ts(v):
   """Parse a datetime or ISO-8601 string into a datetime, or None."""
   if v is None:
      return None
   if isinstance(v, datetime):
      return v
   try:
      return datetime.fromisoformat(str(v))
   except (ValueError, TypeError):
      return None


def _compute_gap_metadata(timestamps, cadence_seconds, range_start=None, range_end=None):
   """Return canonical gap metadata: missing_count, max_gap_minutes, intervals."""
   distinct = sorted({ts for ts in timestamps if ts is not None})
   intervals = []
   missing_count = 0
   max_gap_minutes = 0

   if range_start is not None and distinct:
      start_ts = _parse_ts(range_start) if not isinstance(range_start, datetime) else range_start
      first = distinct[0]
      if start_ts is not None and first > start_ts:
         delta = (first - start_ts).total_seconds()
         n_missing = int(round(delta / cadence_seconds)) - 1
         if n_missing > 0:
            missing_count += n_missing
            intervals.append({"location":"leading","after":_dt(start_ts),"before":_dt(first),"missing_count":n_missing,"max_gap_minutes":n_missing})
            max_gap_minutes = max(max_gap_minutes, n_missing)

   for i in range(1, len(distinct)):
      delta = (distinct[i] - distinct[i-1]).total_seconds()
      if delta > cadence_seconds * 1.5:
         n_missing = int(round(delta / cadence_seconds)) - 1
         missing_count += n_missing
         intervals.append({"location":"internal","after":_dt(distinct[i-1]),"before":_dt(distinct[i]),"missing_count":n_missing,"max_gap_minutes":n_missing})
         max_gap_minutes = max(max_gap_minutes, n_missing)

   if range_end is not None and distinct:
      end_ts = _parse_ts(range_end) if not isinstance(range_end, datetime) else range_end
      last = distinct[-1]
      if end_ts is not None and last < end_ts:
         delta = (end_ts - last).total_seconds()
         n_missing = int(round(delta / cadence_seconds)) - 1
         if n_missing > 0:
            missing_count += n_missing
            intervals.append({"location":"trailing","after":_dt(last),"before":_dt(end_ts),"missing_count":n_missing,"max_gap_minutes":n_missing})
            max_gap_minutes = max(max_gap_minutes, n_missing)

   return {"missing_count": missing_count, "max_gap_minutes": max_gap_minutes, "intervals": intervals}


def _enrich_snapshot(snapshot):
   """Enrich a raw snapshot dict with response-contract metadata.

   Adds:
     counters.gaps         -- gap metadata (missing_count, sorted interval list)
     counters.status       -- 'empty' | 'stale' | 'partial' | 'complete'
     counters.latest       -- current-card gauge values from newest row, or None
     counters.latest.mem_used_physical_kb -- derived physical memory used, or None
     usage.gaps            -- gap metadata over DEDUPLICATED interval_end
                               timestamps (closed 15-minute cadence)
     usage.status          -- 'empty' | 'stale' | 'partial' | 'complete'

   Does NOT zero-fill gaps; gaps appear as explicit metadata.
   Does NOT modify the rows lists.

   Parameters
   ----------
   snapshot : dict -- assembled snapshot from _run_dashboard_queries or stub.

   Returns
   -------
   dict -- snapshot with enrichment added (same object, mutated in place).
   """
   counters = snapshot.get("counters", {})
   rows = counters.get("rows", [])
   is_fresh = counters.get("is_fresh", False)
   newest_window_end = counters.get("newest_window_end")
   hardware = snapshot.get("hardware", {})
   mem_total_kb = hardware.get("mem_total_kb") if hardware else None

   # ------------------------------------------------------------------
   # Counter gap metadata
   # Compute gaps as intervals between sorted window_end timestamps.
   # Expected cadence: _COUNTER_CADENCE_SECONDS (60s).  Counter rows are
   # one-per-minute per node; no deduplication is expected (but harmless
   # if a duplicate window_end were ever present).
   # ------------------------------------------------------------------
   window_ends = [_parse_ts(row.get("window_end")) for row in rows]
   range_hours = snapshot.get("range_hours")
   server_utc_now = snapshot.get("server_utc_now")
   range_start = None
   range_end = None
   if range_hours is not None and server_utc_now is not None:
      try:
         now_dt = datetime.fromisoformat(str(server_utc_now).replace("Z", "+00:00"))
         if now_dt.tzinfo is None:
            now_dt = now_dt.replace(tzinfo=timezone.utc)
         range_start = now_dt - timedelta(hours=range_hours)
         range_end = now_dt
      except (ValueError, TypeError):
         pass
   counters["gaps"] = _compute_gap_metadata(window_ends, _COUNTER_CADENCE_SECONDS, range_start=range_start, range_end=range_end)

   # ------------------------------------------------------------------
   # Counter status
   # empty  : no rows
   # stale  : rows exist but newest is not fresh
   # partial: fresh but at least one row has complete=False
   # complete: fresh and all rows have complete=True
   # ------------------------------------------------------------------
   if not rows:
      counters["status"] = "empty"
   elif not is_fresh:
      counters["status"] = "stale"
   else:
      all_complete = all(row.get("complete", False) for row in rows)
      counters["status"] = "complete" if all_complete else "partial"

   # ------------------------------------------------------------------
   # Latest counter gauges (current-card values from most-recent row)
   # ------------------------------------------------------------------
   if rows:
      # Find the row with the newest window_end.
      latest_row = max(
         rows,
         key=lambda r: _parse_ts(r.get("window_end")) or datetime.min.replace(
            tzinfo=timezone.utc),
      )
      mem_available_kb = latest_row.get("mem_available_kb")
      if mem_total_kb is not None and mem_available_kb is not None:
         mem_used = mem_total_kb - mem_available_kb
      else:
         mem_used = None

      counters["latest"] = {
         "window_end": _dt(latest_row.get("window_end")),
         "mem_available_kb": mem_available_kb,
         "mem_used_physical_kb": mem_used,
         "cached_kb": latest_row.get("cached_kb"),
         "shmem_kb": latest_row.get("shmem_kb"),
         "load1": _safe_float(latest_row.get("load1")),
         "load5": _safe_float(latest_row.get("load5")),
         "load15": _safe_float(latest_row.get("load15")),
         "procs_running": latest_row.get("procs_running"),
         "procs_total": latest_row.get("procs_total"),
         "socket_count": latest_row.get("socket_count"),
         "cpu_busy_pct": latest_row.get("cpu_busy_pct"),
      }
   else:
      counters["latest"] = None

   snapshot["counters"] = counters

   # ------------------------------------------------------------------
   # Usage gap metadata + status
   # Gap metadata is computed over DEDUPLICATED interval_end timestamps:
   # multiple (category, activity) grains share the same closed-interval
   # interval_end (collector usage_interval_sec default = 900s / 15min;
   # see node_monitor/config.py), so repeated timestamps must not be
   # treated as extra data points when detecting missing cadence windows.
   # Status semantics mirror counters.status but are based on the grains
   # list and usage.is_fresh:
   #   empty  : no grains
   #   stale  : grains exist but newest interval_end is not fresh
   #   partial: fresh but at least one grain has complete=False
   #   complete: fresh and all grains have complete=True
   # ------------------------------------------------------------------
   usage = snapshot.get("usage", {})
   if isinstance(usage, dict):
      usage_grains = usage.get("grains", [])
      usage_is_fresh = usage.get("is_fresh", False)

      interval_ends = [_parse_ts(g.get("interval_end")) for g in usage_grains]
      usage["gaps"] = _compute_gap_metadata(interval_ends, _USAGE_CADENCE_SECONDS)

      if not usage_grains:
         usage["status"] = "empty"
      elif not usage_is_fresh:
         usage["status"] = "stale"
      else:
         all_usage_complete = all(g.get("complete", False) for g in usage_grains)
         usage["status"] = "complete" if all_usage_complete else "partial"
      snapshot["usage"] = usage

   return snapshot


# ---------------------------------------------------------------------------
# Internal -- the actual query orchestration
# ---------------------------------------------------------------------------

def _run_dashboard_queries(conn, *, system, node, range_hours,
                           username, now_utc):
   """Execute all dashboard subqueries on ``conn`` within one transaction.

   First runs a fixed parameterized SELECT against node_monitor.node_hardware
   for exact (system, source_hostname) inventory plus required hardware
   fields.  Unknown node fails from inside the transaction.

   All calls use the same ``now_utc`` so the snapshot is coherent.
   Any subquery failure propagates immediately, aborting the whole response.

   Returns a dict suitable for ``serialize_dashboard``.
   """
   from node_monitor.web.service import DashboardServiceError

   # ------------------------------------------------------------------
   # In-transaction inventory + hardware lookup (same REPEATABLE READ
   # READ ONLY snapshot).  No extra connection; uses the checked-out conn.
   # ------------------------------------------------------------------
   hw_row = conn.execute(
      _HARDWARE_SQL, {"system": system, "node": node}
   ).mappings().first()

   if hw_row is None:
      raise DashboardRequestError(
         "node %r is not in the known inventory" % (node,))

   hardware = {
      "system": hw_row["system"],
      "source_hostname": hw_row["source_hostname"],
      "cpu_model": hw_row.get("cpu_model"),
      "cpu_logical": hw_row.get("cpu_logical"),
      "sockets": hw_row.get("sockets"),
      "cores_per_socket": hw_row.get("cores_per_socket"),
      "cpu_max_freq_khz": hw_row.get("cpu_max_freq_khz"),
      "numa_nodes": hw_row.get("numa_nodes"),
      "mem_total_kb": hw_row.get("mem_total_kb"),
      "swap_total_kb": hw_row.get("swap_total_kb"),
      "hugepage_size_kb": hw_row.get("hugepage_size_kb"),
      "kernel_release": hw_row.get("kernel_release"),
      "os_pretty_name": hw_row.get("os_pretty_name"),
      "net_fs_mounts": hw_row.get("net_fs_mounts"),
      "gpus": hw_row.get("gpus"),
   }

   # We have a valid inventory frozenset for queries.
   inventory = frozenset({node})

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
   # offset so clients can parse it unambiguously.  Use real production
   # datetime formatting (not injected snapshots).
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
      "hardware": hardware,
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
