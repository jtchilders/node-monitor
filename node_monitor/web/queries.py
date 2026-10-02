"""node_monitor.web.queries -- bounded, parameterized dashboard SQL projections.

Design: operational-web-dashboard Task 4.

Strict semantics enforced here:
  * Only additive cpu_seconds sums across username grains; never
    percentile-of-percentiles or unweighted averages.
  * All percentile/fraction hotspot grains use DISTINCT ON deterministic
    selection: (interval_end, category, activity) ordered by the metric
    DESC then username_key ASC as the stable null-safe tiebreaker.
    username_key is the real GENERATED column COALESCE(username, '').
  * Exactly five valid time ranges: 1h, 3h, 6h, 12h, 24h.
  * Node validation is inventory-based: the caller supplies an inventory
    frozenset and any requested node not in the inventory is rejected
    before SQL is issued.
  * Username is bound data only; never used as an identifier.
  * Max 256 UTF-8 bytes for username; rejected before SQL is issued.
  * Counter rows capped at 1440; 1441 raises QueryBoundsError.
  * Newest window_end freshness: 120 seconds.
  * Newest closed interval_end freshness: 1200 seconds.
  * Max 10 poll failures returned.
  * Collection log is system-level (no source_hostname filter).
  * Loopback iface "lo" is excluded in the stored network_rates JSONB
    (enforced by the writer/collector; queries project what is stored).
  * LUSTRE_PEAK_SUM_SOURCE = "max_sum"; API label "peak-sum".
  * No ORM rows; no raw column SELECT *; only projected/aggregated columns.
  * All SQL is a fixed Python constant; only VALUES are bound; no identifier
    binding. JSONB field access uses ->> with explicit ::double precision cast.
"""

from datetime import datetime, timezone

from sqlalchemy import bindparam, text

# ---------------------------------------------------------------------------
# Public constants
# ---------------------------------------------------------------------------

#: Exact allowed query ranges in hours (any other value is rejected).
VALID_RANGES_HOURS = frozenset({1, 3, 6, 12, 24})

#: Maximum UTF-8 bytes for a username parameter.
MAX_USERNAME_BYTES = 256

#: Hard cap on counter rows returned; 1441 triggers QueryBoundsError.
COUNTER_ROW_LIMIT = 1440

#: Freshness threshold for newest counter window_end (seconds).
COUNTER_STALENESS_SECONDS = 120

#: Freshness threshold for newest closed usage interval_end (seconds).
USAGE_STALENESS_SECONDS = 1200

#: Maximum poll failure rows returned per node.
MAX_POLL_FAILURES = 10

#: Field name inside lustre_md_summary JSONB used for peak-sum.
LUSTRE_PEAK_SUM_SOURCE = "max_sum"

#: API label for the lustre peak-sum metric.
LUSTRE_PEAK_SUM_API_LABEL = "peak-sum"


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class QueryBoundsError(ValueError):
   """Raised when a query parameter violates a hard bounds contract."""


class QueryValidationError(ValueError):
   """Raised when a query parameter fails domain validation (range, node,
   username length) before any SQL is issued."""


# ---------------------------------------------------------------------------
# Parameter validation helpers
# ---------------------------------------------------------------------------

def _validate_range_hours(hours):
   """Raise QueryValidationError unless hours is in VALID_RANGES_HOURS."""
   if hours not in VALID_RANGES_HOURS:
      raise QueryValidationError(
         "range_hours %r is not allowed; valid values are %r"
         % (hours, sorted(VALID_RANGES_HOURS)))
   return hours


def _validate_node(node, inventory):
   """Raise QueryValidationError if node is not in the inventory frozenset."""
   if node not in inventory:
      raise QueryValidationError(
         "node %r is not in the known inventory" % (node,))
   return node


def _validate_username(username):
   """Raise QueryValidationError if username exceeds MAX_USERNAME_BYTES UTF-8
   bytes or is an empty string (NULL is allowed and means 'all usernames')."""
   if username is None:
      return None
   if not isinstance(username, str):
      raise QueryValidationError("username must be a str or None")
   if username == "":
      raise QueryValidationError("username must not be an empty string")
   if len(username.encode("utf-8")) > MAX_USERNAME_BYTES:
      raise QueryValidationError(
         "username exceeds %d UTF-8 bytes" % MAX_USERNAME_BYTES)
   return username


# ---------------------------------------------------------------------------
# Fixed SQL constants -- counter queries
# ---------------------------------------------------------------------------

#: Load all counter rows for a node within [start, now].
#: Returns (window_start, window_end, sample_count, expected_count, coverage,
#:          meets_minimum_samples, mem_available_kb, cached_kb, shmem_kb,
#:          load1, load5, load15, procs_running, procs_total, socket_count,
#:          cpu_busy_pct, network_rates, lustre_md_summary)
#: ordered by window_end ASC.
COUNTER_SQL = text("""
SELECT
   window_start, window_end,
   sample_count, expected_count, coverage, meets_minimum_samples,
   mem_available_kb, cached_kb, shmem_kb,
   load1, load5, load15, procs_running, procs_total, socket_count,
   cpu_busy_pct, network_rates, lustre_md_summary
FROM node_monitor.node_counter_minute
WHERE system = :system
  AND source_hostname = :node
  AND window_start >= :start
ORDER BY window_end ASC
""")


# ---------------------------------------------------------------------------
# Fixed SQL constants -- usage CPU (additive sum across username grains)
# ---------------------------------------------------------------------------

#: Sum cpu_seconds per (interval_end, category, activity) grain.
#: complete = True iff EVERY contributing username row has
#:   sample_count = expected_count AND unmeasured_count = 0.
#: Accepts :username_is_null (boolean) and :username (text or NULL).
USAGE_CPU_SQL = text("""
SELECT
   interval_end, category, activity,
   sum(cpu_seconds) AS cpu_seconds,
   bool_and(
      sample_count = expected_count
      AND unmeasured_count = 0
   ) AS complete
FROM node_monitor.node_usage_intervals
WHERE system = :system
  AND source_hostname = :node
  AND interval_start >= :start
  AND (:username_is_null OR username = :username)
GROUP BY interval_end, category, activity
ORDER BY interval_end, category, activity
""")


# ---------------------------------------------------------------------------
# Fixed SQL constants -- hotspot queries (DISTINCT ON pattern)
#
# Each query uses DISTINCT ON (interval_end, category, activity) ordered by
# the metric DESC then username_key ASC.  username_key = COALESCE(username, '')
# is the real generated column (migration 0001) used as the deterministic
# null-safe tiebreaker.
#
# No percentile-of-percentiles: each hotspot picks the single username row
# whose stored per-row percentile is the highest.
# ---------------------------------------------------------------------------

#: Hotspot: username with highest rss_kb ->> 'p95' per grain.
USAGE_RSS_P95_HOTSPOT_SQL = text("""
SELECT DISTINCT ON (interval_end, category, activity)
   interval_end, category, activity, username,
   (rss_kb ->> 'p95')::double precision AS rss_p95_kb,
   sample_count, expected_count, unmeasured_count
FROM node_monitor.node_usage_intervals
WHERE system = :system
  AND source_hostname = :node
  AND interval_start >= :start
  AND (:username_is_null OR username = :username)
ORDER BY
   interval_end, category, activity,
   (rss_kb ->> 'p95')::double precision DESC,
   username_key ASC
""")


#: Hotspot: username with highest d_state_fraction per grain.
USAGE_D_STATE_HOTSPOT_SQL = text("""
SELECT DISTINCT ON (interval_end, category, activity)
   interval_end, category, activity, username,
   d_state_fraction,
   sample_count, expected_count, unmeasured_count
FROM node_monitor.node_usage_intervals
WHERE system = :system
  AND source_hostname = :node
  AND interval_start >= :start
  AND (:username_is_null OR username = :username)
ORDER BY
   interval_end, category, activity,
   d_state_fraction DESC,
   username_key ASC
""")


#: Hotspot: username with highest process_count ->> 'p95' per grain.
USAGE_PROCESS_COUNT_P95_HOTSPOT_SQL = text("""
SELECT DISTINCT ON (interval_end, category, activity)
   interval_end, category, activity, username,
   (process_count ->> 'p95')::double precision AS process_count_p95,
   sample_count, expected_count, unmeasured_count
FROM node_monitor.node_usage_intervals
WHERE system = :system
  AND source_hostname = :node
  AND interval_start >= :start
  AND (:username_is_null OR username = :username)
ORDER BY
   interval_end, category, activity,
   (process_count ->> 'p95')::double precision DESC,
   username_key ASC
""")


# ---------------------------------------------------------------------------
# Fixed SQL constants -- poll failures (per node, max 10 rows)
# ---------------------------------------------------------------------------

#: Most recent poll failures for a node, capped at MAX_POLL_FAILURES.
POLL_FAILURES_SQL = text("""
SELECT
   recorded_at, loop, failure_type, detail,
   consecutive_failures, breaker_state
FROM node_monitor.node_poll_failures
WHERE system = :system
  AND source_hostname = :node
ORDER BY recorded_at DESC
LIMIT :limit
""")


# ---------------------------------------------------------------------------
# Fixed SQL constants -- collection log (system-level, no node filter)
# ---------------------------------------------------------------------------

#: Most recent system-level collection log events.
COLLECTION_LOG_SQL = text("""
SELECT
   recorded_at, event, detail
FROM node_monitor.node_collection_log
WHERE system = :system
  AND recorded_at >= :start
ORDER BY recorded_at DESC
""")


# ---------------------------------------------------------------------------
# Result value types
# ---------------------------------------------------------------------------

class CounterResult:
   """Bounded result from load_counters().

   Attributes:
     rows: list of dicts with JSONB columns already parsed (as returned
           by SQLAlchemy mappings -- psycopg2 decodes jsonb automatically).
     newest_window_end: datetime or None if no rows.
     is_fresh: True iff newest_window_end is within COUNTER_STALENESS_SECONDS
               of the supplied now_utc.
   """

   def __init__(self, rows, now_utc):
      self.rows = rows
      if rows:
         self.newest_window_end = max(r["window_end"] for r in rows)
         age = (now_utc - self.newest_window_end).total_seconds()
         self.is_fresh = age <= COUNTER_STALENESS_SECONDS
      else:
         self.newest_window_end = None
         self.is_fresh = False


class UsageResult:
   """Bounded result from load_usage().

   Attributes:
     by_key: dict mapping (category, activity, interval_end) ->
             UsageGrain namedtuple-like object.
     rows: flat list of dicts (additive cpu only; hotspots merged separately).
     newest_interval_end: datetime or None.
     is_fresh: True iff newest closed interval_end is within
               USAGE_STALENESS_SECONDS of now_utc.
   """

   def __init__(self, cpu_rows, rss_hotspot_rows, d_state_hotspot_rows,
                proc_hotspot_rows, now_utc):
      # Index hotspots by (interval_end, category, activity).
      rss_index = {
         (r["interval_end"], r["category"], r["activity"]): r
         for r in rss_hotspot_rows
      }
      d_index = {
         (r["interval_end"], r["category"], r["activity"]): r
         for r in d_state_hotspot_rows
      }
      proc_index = {
         (r["interval_end"], r["category"], r["activity"]): r
         for r in proc_hotspot_rows
      }

      self.by_key = {}
      for row in cpu_rows:
         key = (row["category"], row["activity"], row["interval_end"])
         t_key = (row["interval_end"], row["category"], row["activity"])
         rss = rss_index.get(t_key)
         d = d_index.get(t_key)
         proc = proc_index.get(t_key)
         self.by_key[key] = _UsageGrain(
            cpu_seconds=row["cpu_seconds"],
            complete=row["complete"],
            rss_p95_kb=rss["rss_p95_kb"] if rss else None,
            rss_p95_username=rss["username"] if rss else None,
            d_state_fraction=d["d_state_fraction"] if d else None,
            d_state_username=d["username"] if d else None,
            process_count_p95=proc["process_count_p95"] if proc else None,
            process_count_p95_username=proc["username"] if proc else None,
         )

      self.rows = cpu_rows
      if cpu_rows:
         self.newest_interval_end = max(
            r["interval_end"] for r in cpu_rows)
         age = (now_utc - self.newest_interval_end).total_seconds()
         self.is_fresh = age <= USAGE_STALENESS_SECONDS
      else:
         self.newest_interval_end = None
         self.is_fresh = False


class _UsageGrain:
   """Merged per-(category, activity, interval_end) grain."""

   __slots__ = (
      "cpu_seconds", "complete",
      "rss_p95_kb", "rss_p95_username",
      "d_state_fraction", "d_state_username",
      "process_count_p95", "process_count_p95_username",
   )

   def __init__(self, cpu_seconds, complete, rss_p95_kb, rss_p95_username,
                d_state_fraction, d_state_username,
                process_count_p95, process_count_p95_username):
      self.cpu_seconds = cpu_seconds
      self.complete = complete
      self.rss_p95_kb = rss_p95_kb
      self.rss_p95_username = rss_p95_username
      self.d_state_fraction = d_state_fraction
      self.d_state_username = d_state_username
      self.process_count_p95 = process_count_p95
      self.process_count_p95_username = process_count_p95_username


# ---------------------------------------------------------------------------
# Public query functions
# ---------------------------------------------------------------------------

def load_counters(connection, system, node, start, inventory, *,
                  now_utc=None):
   """Load and validate counter rows for one node within [start, now].

   Parameters
   ----------
   connection : SQLAlchemy connection (inside a transaction).
   system : str -- system name (e.g. "polaris").
   node : str -- source_hostname to query.
   start : datetime (UTC) -- lower bound on window_start (inclusive).
   inventory : frozenset of str -- known node hostnames for validation.
   now_utc : datetime (UTC) or None; defaults to datetime.now(timezone.utc).

   Returns
   -------
   CounterResult

   Raises
   ------
   QueryValidationError  if node is not in inventory.
   QueryBoundsError      if the row count exceeds COUNTER_ROW_LIMIT.
   """
   _validate_node(node, inventory)
   if now_utc is None:
      now_utc = datetime.now(timezone.utc)

   rows = list(connection.execute(
      COUNTER_SQL,
      {"system": system, "node": node, "start": start},
   ).mappings())

   if len(rows) > COUNTER_ROW_LIMIT:
      raise QueryBoundsError(
         "counter row limit exceeded: got %d rows (limit %d)"
         % (len(rows), COUNTER_ROW_LIMIT))

   return CounterResult(rows, now_utc)


def load_usage(connection, system, node, start, inventory, *,
               username=None, now_utc=None):
   """Load and aggregate usage interval rows for one node.

   Additive: cpu_seconds is summed across username grains per
   (interval_end, category, activity).  Hotspot metrics (rss_kb p95,
   d_state_fraction, process_count p95) use DISTINCT ON to pick the single
   contributing username row with the highest value -- never a
   percentile-of-percentiles and never an unweighted average.

   Gaps are preserved as-is: intervals with no rows produce no output.
   Missing intervals are never zero-filled.

   Parameters
   ----------
   connection : SQLAlchemy connection.
   system : str.
   node : str -- source_hostname.
   start : datetime (UTC).
   inventory : frozenset of str.
   username : str or None -- when not None, rows are filtered to this
              username; bound as a data value, never as an identifier.
   now_utc : datetime (UTC) or None.

   Returns
   -------
   UsageResult

   Raises
   ------
   QueryValidationError  if node is not in inventory or username is invalid.
   """
   _validate_node(node, inventory)
   username = _validate_username(username)
   if now_utc is None:
      now_utc = datetime.now(timezone.utc)

   params = {
      "system": system,
      "node": node,
      "start": start,
      "username_is_null": username is None,
      "username": username,
   }

   cpu_rows = list(
      connection.execute(USAGE_CPU_SQL, params).mappings())
   rss_rows = list(
      connection.execute(USAGE_RSS_P95_HOTSPOT_SQL, params).mappings())
   d_rows = list(
      connection.execute(USAGE_D_STATE_HOTSPOT_SQL, params).mappings())
   proc_rows = list(
      connection.execute(USAGE_PROCESS_COUNT_P95_HOTSPOT_SQL, params).mappings())

   return UsageResult(cpu_rows, rss_rows, d_rows, proc_rows, now_utc)


def load_poll_failures(connection, system, node, inventory):
   """Load the most recent poll failures for one node (max MAX_POLL_FAILURES).

   Parameters
   ----------
   connection : SQLAlchemy connection.
   system : str.
   node : str.
   inventory : frozenset of str.

   Returns
   -------
   list of dicts.

   Raises
   ------
   QueryValidationError  if node is not in inventory.
   """
   _validate_node(node, inventory)
   rows = list(connection.execute(
      POLL_FAILURES_SQL,
      {"system": system, "node": node, "limit": MAX_POLL_FAILURES},
   ).mappings())
   return rows


def load_collection_log(connection, system, start):
   """Load system-level collection log events since start.

   Note: collection log has no source_hostname -- it is system-level.

   Parameters
   ----------
   connection : SQLAlchemy connection.
   system : str.
   start : datetime (UTC).

   Returns
   -------
   list of dicts.
   """
   rows = list(connection.execute(
      COLLECTION_LOG_SQL,
      {"system": system, "start": start},
   ).mappings())
   return rows
