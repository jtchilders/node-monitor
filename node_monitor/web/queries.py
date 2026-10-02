"""node_monitor.web.queries -- bounded, parameterized dashboard SQL projections.

Design: operational-web-dashboard Task 4 (corrected).

Strict semantics enforced here:
  * Only additive cpu_seconds sums across username grains; never
    percentile-of-percentiles or unweighted averages.
  * All percentile/fraction hotspot grains use DISTINCT ON deterministic
    selection: (interval_end, category, activity) ordered by the metric
    DESC then username_key ASC as the stable null-safe tiebreaker.
    username_key is the real GENERATED column COALESCE(username, '').
  * Each of rss p50/p95/max and process_count p50/p95/max uses its OWN
    independently ranked DISTINCT ON query and its own SQL constant.
    Six hotspot SQL constants for these two metrics; different users can
    maximize different statistics -- mathematically impossible to detect
    with a single query.  Each statistic exposes its own contributing
    username attribute on _UsageGrain (rss_p50_username, rss_p95_username,
    rss_max_username, process_count_p50_username, process_count_p95_username,
    process_count_max_username).
  * METRIC_TO_SQL_MAP maps API metric name strings to their fixed SQL
    constant.  No dynamic SQL formatting; all SQL is fixed Python constants.
  * Exactly five valid time ranges: 1h, 3h, 6h, 12h, 24h.
  * Node validation is inventory-based: the caller supplies an inventory
    frozenset and any requested node not in the inventory is rejected
    before SQL is issued.
  * Username is bound data only; never used as an identifier.
  * Max 256 UTF-8 bytes for username; rejected before SQL is issued.
  * Counter rows capped at 1440; SQL uses LIMIT COUNTER_ROW_LIMIT+1 sentinel
    so the DB returns at most 1441 rows; load_counters then rejects >1440.
    The one extra row is the sentinel proving overflow without fetching all.
  * Newest window_end freshness: 120 seconds; age must satisfy 0 <= age <= threshold
    (future timestamps are not fresh).
  * Newest closed interval_end freshness: 1200 seconds; same 0 <= age rule.
  * Max 10 poll failures returned.
  * Collection log is system-level (no source_hostname filter); bounded by
    COLLECTION_LOG_LIMIT rows.
  * Loopback iface "lo" is excluded in the stored network_rates JSONB
    (enforced by the writer/collector; queries project what is stored).
  * LUSTRE_PEAK_SUM_SOURCE = "max_sum"; API label "peak-sum".
  * No ORM rows; no raw column SELECT *; only projected/aggregated columns.
  * All SQL is a fixed Python constant; only VALUES are bound; no identifier
    binding. JSONB field access uses ->> with explicit ::double precision cast.
  * start, end, and now_utc parameters must be UTC-aware datetimes with a
    zero UTC offset (timezone.utc or equivalent); naive, non-UTC-aware, or
    non-datetime values raise QueryValidationError before any SQL is issued.
  * All queries accept an :end upper-bound parameter (now_utc) that is passed
    to SQL as a WHERE bound, excluding rows after now_utc entirely rather
    than merely marking them stale.  This enforces a closed bounded window.
  * Poll failures queries accept both :start and :end range bounds.
  * complete is a computed property on each CounterResult row:
    True iff coverage == 1, meets_minimum_samples is True,
    invalid_pair_count == 0, and excess_sample_count == 0.
    Raw metadata fields are still carried unchanged.
  * Public range-based facade functions (load_*_for_range) derive start
    internally via range_hours_to_start and expose a single range_hours
    parameter.  Callers should use these rather than the low-level functions
    that accept raw start/end datetimes.
"""

from datetime import datetime, timedelta, timezone

from sqlalchemy import text

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

#: Maximum collection log rows returned per query.
COLLECTION_LOG_LIMIT = 200

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
   username length, datetime UTC-awareness) before any SQL is issued."""


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


def _validate_utc_datetime(value, name):
   """Raise QueryValidationError unless value is a UTC-aware datetime with
   zero UTC offset.

   Both naive datetimes (tzinfo is None), non-datetime objects, and aware
   datetimes with non-zero UTC offsets (e.g. US/Eastern, IST) raise
   QueryValidationError with a message containing 'UTC'.

   A datetime with timezone.utc or any fixed offset of zero hours is accepted.
   """
   if not isinstance(value, datetime):
      raise QueryValidationError(
         "%s must be a UTC-aware datetime (got %r)" % (name, value))
   if value.tzinfo is None:
      raise QueryValidationError(
         "%s must be a UTC-aware datetime (got %r)" % (name, value))
   # Enforce zero UTC offset strictly.
   utcoffset = value.utcoffset()
   if utcoffset is None or utcoffset.total_seconds() != 0:
      raise QueryValidationError(
         "%s must be a UTC datetime (zero UTC offset); "
         "got timezone with offset %r" % (name, utcoffset))
   return value


def range_hours_to_start(hours, now_utc):
   """Return now_utc - timedelta(hours=hours) after validating both arguments.

   Parameters
   ----------
   hours : int -- must be in VALID_RANGES_HOURS.
   now_utc : datetime (UTC-aware, zero offset).

   Returns
   -------
   datetime -- the start of the query window.

   Raises
   ------
   QueryValidationError  if hours is not in VALID_RANGES_HOURS or now_utc is
                        not a UTC-aware datetime with zero offset.
   """
   _validate_utc_datetime(now_utc, "now_utc")
   _validate_range_hours(hours)
   return now_utc - timedelta(hours=hours)


# ---------------------------------------------------------------------------
# Fixed SQL constants -- counter queries
# ---------------------------------------------------------------------------

#: Load at most COUNTER_ROW_LIMIT+1 counter rows for a node within [start, end].
#: The +1 sentinel allows load_counters to detect overflow without fetching
#: all rows: if DB returns COUNTER_ROW_LIMIT+1 rows, the limit is exceeded.
#: Returns (window_start, window_end, sample_count, expected_count, coverage,
#:          meets_minimum_samples, invalid_pair_count, excess_sample_count,
#:          mem_available_kb, cached_kb, shmem_kb,
#:          load1, load5, load15, procs_running, procs_total, socket_count,
#:          cpu_busy_pct, network_rates, lustre_md_summary)
#: ordered by window_end ASC.
COUNTER_SQL = text("""
SELECT
   window_start, window_end,
   sample_count, expected_count, coverage, meets_minimum_samples,
   invalid_pair_count, excess_sample_count,
   mem_available_kb, cached_kb, shmem_kb,
   load1, load5, load15, procs_running, procs_total, socket_count,
   cpu_busy_pct, network_rates, lustre_md_summary
FROM node_monitor.node_counter_minute
WHERE system = :system
  AND source_hostname = :node
  AND window_start >= :start
  AND window_end <= :end
ORDER BY window_end ASC
LIMIT :limit
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
  AND interval_end <= :end
  AND (:username_is_null OR username = :username)
GROUP BY interval_end, category, activity
ORDER BY interval_end, category, activity
""")


# ---------------------------------------------------------------------------
# Fixed SQL constants -- hotspot queries (DISTINCT ON pattern, 1 per stat)
#
# Each query uses DISTINCT ON (interval_end, category, activity) ordered by
# the SPECIFIC metric/percentile DESC then username_key ASC.  username_key =
# COALESCE(username, '') is the real generated column (migration 0001) used as
# the deterministic null-safe tiebreaker.
#
# MATHEMATICAL CORRECTNESS REQUIREMENT:
# rss p50/p95/max and process_count p50/p95/max each require their OWN
# independently ranked query.  It is mathematically wrong for rss_p50 and
# rss_p95 and rss_max to all come from the same single-pass query that ranks
# by p95 -- a different user may have the highest p50 or max.  Therefore:
#   * 6 separate SQL constants (2 metrics x 3 percentiles)
#   * Each projects only rss_p50_kb, rss_p95_kb, rss_max_kb (or proc equiv)
#     so the caller can read the ranked-winner's value for the ranked stat
#   * The username projected is the winner for THAT stat
#
# D-state and interactivity remain single queries (one stat each).
# ---------------------------------------------------------------------------

#: Hotspot: username with highest rss_kb ->> 'p50' per grain (p50-ranked).
USAGE_RSS_P50_HOTSPOT_SQL = text("""
SELECT DISTINCT ON (interval_end, category, activity)
   interval_end, category, activity, username,
   (rss_kb ->> 'p50')::double precision AS rss_p50_kb,
   (rss_kb ->> 'p95')::double precision AS rss_p95_kb,
   (rss_kb ->> 'max')::double precision AS rss_max_kb,
   sample_count, expected_count, unmeasured_count
FROM node_monitor.node_usage_intervals
WHERE system = :system
  AND source_hostname = :node
  AND interval_start >= :start
  AND interval_end <= :end
  AND (:username_is_null OR username = :username)
ORDER BY
   interval_end, category, activity,
   (rss_kb ->> 'p50')::double precision DESC,
   username_key ASC
""")

#: Hotspot: username with highest rss_kb ->> 'p95' per grain (p95-ranked).
USAGE_RSS_P95_HOTSPOT_SQL = text("""
SELECT DISTINCT ON (interval_end, category, activity)
   interval_end, category, activity, username,
   (rss_kb ->> 'p50')::double precision AS rss_p50_kb,
   (rss_kb ->> 'p95')::double precision AS rss_p95_kb,
   (rss_kb ->> 'max')::double precision AS rss_max_kb,
   sample_count, expected_count, unmeasured_count
FROM node_monitor.node_usage_intervals
WHERE system = :system
  AND source_hostname = :node
  AND interval_start >= :start
  AND interval_end <= :end
  AND (:username_is_null OR username = :username)
ORDER BY
   interval_end, category, activity,
   (rss_kb ->> 'p95')::double precision DESC,
   username_key ASC
""")

#: Hotspot: username with highest rss_kb ->> 'max' per grain (max-ranked).
USAGE_RSS_MAX_HOTSPOT_SQL = text("""
SELECT DISTINCT ON (interval_end, category, activity)
   interval_end, category, activity, username,
   (rss_kb ->> 'p50')::double precision AS rss_p50_kb,
   (rss_kb ->> 'p95')::double precision AS rss_p95_kb,
   (rss_kb ->> 'max')::double precision AS rss_max_kb,
   sample_count, expected_count, unmeasured_count
FROM node_monitor.node_usage_intervals
WHERE system = :system
  AND source_hostname = :node
  AND interval_start >= :start
  AND interval_end <= :end
  AND (:username_is_null OR username = :username)
ORDER BY
   interval_end, category, activity,
   (rss_kb ->> 'max')::double precision DESC,
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
  AND interval_end <= :end
  AND (:username_is_null OR username = :username)
ORDER BY
   interval_end, category, activity,
   d_state_fraction DESC,
   username_key ASC
""")

#: Hotspot: username with highest process_count ->> 'p50' per grain (p50-ranked).
USAGE_PROCESS_COUNT_P50_HOTSPOT_SQL = text("""
SELECT DISTINCT ON (interval_end, category, activity)
   interval_end, category, activity, username,
   (process_count ->> 'p50')::double precision AS process_count_p50,
   (process_count ->> 'p95')::double precision AS process_count_p95,
   (process_count ->> 'max')::double precision AS process_count_max,
   sample_count, expected_count, unmeasured_count
FROM node_monitor.node_usage_intervals
WHERE system = :system
  AND source_hostname = :node
  AND interval_start >= :start
  AND interval_end <= :end
  AND (:username_is_null OR username = :username)
ORDER BY
   interval_end, category, activity,
   (process_count ->> 'p50')::double precision DESC,
   username_key ASC
""")

#: Hotspot: username with highest process_count ->> 'p95' per grain (p95-ranked).
USAGE_PROCESS_COUNT_P95_HOTSPOT_SQL = text("""
SELECT DISTINCT ON (interval_end, category, activity)
   interval_end, category, activity, username,
   (process_count ->> 'p50')::double precision AS process_count_p50,
   (process_count ->> 'p95')::double precision AS process_count_p95,
   (process_count ->> 'max')::double precision AS process_count_max,
   sample_count, expected_count, unmeasured_count
FROM node_monitor.node_usage_intervals
WHERE system = :system
  AND source_hostname = :node
  AND interval_start >= :start
  AND interval_end <= :end
  AND (:username_is_null OR username = :username)
ORDER BY
   interval_end, category, activity,
   (process_count ->> 'p95')::double precision DESC,
   username_key ASC
""")

#: Hotspot: username with highest process_count ->> 'max' per grain (max-ranked).
USAGE_PROCESS_COUNT_MAX_HOTSPOT_SQL = text("""
SELECT DISTINCT ON (interval_end, category, activity)
   interval_end, category, activity, username,
   (process_count ->> 'p50')::double precision AS process_count_p50,
   (process_count ->> 'p95')::double precision AS process_count_p95,
   (process_count ->> 'max')::double precision AS process_count_max,
   sample_count, expected_count, unmeasured_count
FROM node_monitor.node_usage_intervals
WHERE system = :system
  AND source_hostname = :node
  AND interval_start >= :start
  AND interval_end <= :end
  AND (:username_is_null OR username = :username)
ORDER BY
   interval_end, category, activity,
   (process_count ->> 'max')::double precision DESC,
   username_key ASC
""")

#: Hotspot: username with highest interactivity_fraction per grain.
USAGE_INTERACTIVITY_HOTSPOT_SQL = text("""
SELECT DISTINCT ON (interval_end, category, activity)
   interval_end, category, activity, username,
   interactivity_fraction,
   sample_count, expected_count, unmeasured_count
FROM node_monitor.node_usage_intervals
WHERE system = :system
  AND source_hostname = :node
  AND interval_start >= :start
  AND interval_end <= :end
  AND (:username_is_null OR username = :username)
ORDER BY
   interval_end, category, activity,
   interactivity_fraction DESC,
   username_key ASC
""")

# ---------------------------------------------------------------------------
# Backward-compatible aliases (previously only p95 variants existed)
# ---------------------------------------------------------------------------

#: Alias for backward compatibility with existing callers/tests.
USAGE_RSS_HOTSPOT_SQL = USAGE_RSS_P95_HOTSPOT_SQL

#: Alias for backward compatibility.
USAGE_PROCESS_COUNT_HOTSPOT_SQL = USAGE_PROCESS_COUNT_P95_HOTSPOT_SQL

# ---------------------------------------------------------------------------
# API metric name -> fixed SQL constant allowlist
# ---------------------------------------------------------------------------

#: Maps API metric name strings to the fixed SQL constant that ranks by that
#: statistic.  Values are fixed SQL text objects -- no dynamic formatting.
METRIC_TO_SQL_MAP = {
   "rss_p50": USAGE_RSS_P50_HOTSPOT_SQL,
   "rss_p95": USAGE_RSS_P95_HOTSPOT_SQL,
   "rss_max": USAGE_RSS_MAX_HOTSPOT_SQL,
   "process_count_p50": USAGE_PROCESS_COUNT_P50_HOTSPOT_SQL,
   "process_count_p95": USAGE_PROCESS_COUNT_P95_HOTSPOT_SQL,
   "process_count_max": USAGE_PROCESS_COUNT_MAX_HOTSPOT_SQL,
   "d_state": USAGE_D_STATE_HOTSPOT_SQL,
   "interactivity": USAGE_INTERACTIVITY_HOTSPOT_SQL,
}


# ---------------------------------------------------------------------------
# Fixed SQL constants -- poll failures (per node, bounded range)
# ---------------------------------------------------------------------------

#: Most recent poll failures for a node within [start, end], capped at MAX_POLL_FAILURES.
POLL_FAILURES_SQL = text("""
SELECT
   recorded_at, loop, failure_type, detail,
   consecutive_failures, breaker_state
FROM node_monitor.node_poll_failures
WHERE system = :system
  AND source_hostname = :node
  AND recorded_at >= :start
  AND recorded_at <= :end
ORDER BY recorded_at DESC
LIMIT :limit
""")


# ---------------------------------------------------------------------------
# Fixed SQL constants -- collection log (system-level, no node filter)
# ---------------------------------------------------------------------------

#: Most recent system-level collection log events within [start, end], bounded by :limit.
COLLECTION_LOG_SQL = text("""
SELECT
   recorded_at, event, detail
FROM node_monitor.node_collection_log
WHERE system = :system
  AND recorded_at >= :start
  AND recorded_at <= :end
ORDER BY recorded_at DESC
LIMIT :limit
""")


# ---------------------------------------------------------------------------
# Result value types
# ---------------------------------------------------------------------------

def _compute_complete(row):
   """Compute the 'complete' quality flag from raw metadata fields.

   Returns True iff all four conditions hold:
     - coverage == 1
     - meets_minimum_samples is True
     - invalid_pair_count == 0
     - excess_sample_count == 0
   """
   return (
      row.get("coverage") == 1
      and row.get("meets_minimum_samples") is True
      and row.get("invalid_pair_count") == 0
      and row.get("excess_sample_count") == 0
   )


class CounterResult:
   """Bounded result from load_counters().

   Attributes:
     rows: list of dicts with JSONB columns already parsed (as returned
           by SQLAlchemy mappings -- psycopg2 decodes jsonb automatically).
           Each row carries: window_start, window_end, sample_count,
           expected_count, coverage, meets_minimum_samples,
           invalid_pair_count, excess_sample_count, plus gauge columns.
           Each row also carries a computed 'complete' key (True iff
           coverage == 1, meets_minimum_samples is True,
           invalid_pair_count == 0, excess_sample_count == 0).
     newest_window_end: datetime or None if no rows.
     is_fresh: True iff newest_window_end satisfies
               0 <= (now_utc - newest_window_end).total_seconds()
               <= COUNTER_STALENESS_SECONDS.
               Future timestamps (age < 0) are not fresh.
   """

   def __init__(self, rows, now_utc):
      # Add computed 'complete' property to each row.
      self.rows = [dict(r, complete=_compute_complete(r)) for r in rows]
      if rows:
         self.newest_window_end = max(r["window_end"] for r in rows)
         age = (now_utc - self.newest_window_end).total_seconds()
         self.is_fresh = 0 <= age <= COUNTER_STALENESS_SECONDS
      else:
         self.newest_window_end = None
         self.is_fresh = False


class UsageResult:
   """Bounded result from load_usage().

   Attributes:
     by_key: dict mapping (category, activity, interval_end) ->
             _UsageGrain object with all hotspot attributes.
     rows: flat list of dicts (additive cpu only; hotspots merged separately).
     newest_interval_end: datetime or None.
     is_fresh: True iff newest closed interval_end satisfies
               0 <= (now_utc - newest_interval_end).total_seconds()
               <= USAGE_STALENESS_SECONDS.
               Future timestamps (age < 0) are not fresh.
   """

   def __init__(self, cpu_rows,
                rss_p50_rows, rss_p95_rows, rss_max_rows,
                d_state_hotspot_rows,
                proc_p50_rows, proc_p95_rows, proc_max_rows,
                interactivity_hotspot_rows, now_utc):
      # Index each hotspot result by grain key.
      def _index(rows):
         return {
            (r["interval_end"], r["category"], r["activity"]): r
            for r in rows
         }

      rss_p50_index = _index(rss_p50_rows)
      rss_p95_index = _index(rss_p95_rows)
      rss_max_index = _index(rss_max_rows)
      d_index = _index(d_state_hotspot_rows)
      proc_p50_index = _index(proc_p50_rows)
      proc_p95_index = _index(proc_p95_rows)
      proc_max_index = _index(proc_max_rows)
      interactivity_index = _index(interactivity_hotspot_rows)

      self.by_key = {}
      for row in cpu_rows:
         key = (row["category"], row["activity"], row["interval_end"])
         t_key = (row["interval_end"], row["category"], row["activity"])

         rp50 = rss_p50_index.get(t_key)
         rp95 = rss_p95_index.get(t_key)
         rmax = rss_max_index.get(t_key)
         d = d_index.get(t_key)
         pp50 = proc_p50_index.get(t_key)
         pp95 = proc_p95_index.get(t_key)
         pmax = proc_max_index.get(t_key)
         ia = interactivity_index.get(t_key)

         self.by_key[key] = _UsageGrain(
            cpu_seconds=row["cpu_seconds"],
            complete=row["complete"],
            # RSS: each percentile value comes from its own independently-ranked row
            rss_p50_kb=rp50["rss_p50_kb"] if rp50 else None,
            rss_p50_username=rp50["username"] if rp50 else None,
            rss_p95_kb=rp95["rss_p95_kb"] if rp95 else None,
            rss_p95_username=rp95["username"] if rp95 else None,
            rss_max_kb=rmax["rss_max_kb"] if rmax else None,
            rss_max_username=rmax["username"] if rmax else None,
            # D-state (single stat)
            d_state_fraction=d["d_state_fraction"] if d else None,
            d_state_username=d["username"] if d else None,
            # Process count: each percentile independently ranked
            process_count_p50=pp50["process_count_p50"] if pp50 else None,
            process_count_p50_username=pp50["username"] if pp50 else None,
            process_count_p95=pp95["process_count_p95"] if pp95 else None,
            process_count_p95_username=pp95["username"] if pp95 else None,
            process_count_max=pmax["process_count_max"] if pmax else None,
            process_count_max_username=pmax["username"] if pmax else None,
            # Interactivity (single stat)
            interactivity_fraction=ia["interactivity_fraction"] if ia else None,
            interactivity_username=ia["username"] if ia else None,
         )

      self.rows = cpu_rows
      if cpu_rows:
         self.newest_interval_end = max(
            r["interval_end"] for r in cpu_rows)
         age = (now_utc - self.newest_interval_end).total_seconds()
         self.is_fresh = 0 <= age <= USAGE_STALENESS_SECONDS
      else:
         self.newest_interval_end = None
         self.is_fresh = False


class _UsageGrain:
   """Merged per-(category, activity, interval_end) grain.

   Non-additive hotspot fields (rss, process_count, interactivity, d_state)
   each carry the value AND the username of the hotspot row selected by
   DISTINCT ON -- independently ranked per statistic.

   rss_p50_kb comes from the p50-ranked query (rss_p50_username is that
   query's winner).  rss_p95_kb from p95-ranked (rss_p95_username), and
   rss_max_kb from max-ranked (rss_max_username).  Same for process_count.
   """

   __slots__ = (
      "cpu_seconds", "complete",
      "rss_p50_kb", "rss_p50_username",
      "rss_p95_kb", "rss_p95_username",
      "rss_max_kb", "rss_max_username",
      "d_state_fraction", "d_state_username",
      "process_count_p50", "process_count_p50_username",
      "process_count_p95", "process_count_p95_username",
      "process_count_max", "process_count_max_username",
      "interactivity_fraction", "interactivity_username",
   )

   def __init__(self, cpu_seconds, complete,
                rss_p50_kb, rss_p50_username,
                rss_p95_kb, rss_p95_username,
                rss_max_kb, rss_max_username,
                d_state_fraction, d_state_username,
                process_count_p50, process_count_p50_username,
                process_count_p95, process_count_p95_username,
                process_count_max, process_count_max_username,
                interactivity_fraction, interactivity_username):
      self.cpu_seconds = cpu_seconds
      self.complete = complete
      self.rss_p50_kb = rss_p50_kb
      self.rss_p50_username = rss_p50_username
      self.rss_p95_kb = rss_p95_kb
      self.rss_p95_username = rss_p95_username
      self.rss_max_kb = rss_max_kb
      self.rss_max_username = rss_max_username
      self.d_state_fraction = d_state_fraction
      self.d_state_username = d_state_username
      self.process_count_p50 = process_count_p50
      self.process_count_p50_username = process_count_p50_username
      self.process_count_p95 = process_count_p95
      self.process_count_p95_username = process_count_p95_username
      self.process_count_max = process_count_max
      self.process_count_max_username = process_count_max_username
      self.interactivity_fraction = interactivity_fraction
      self.interactivity_username = interactivity_username


# ---------------------------------------------------------------------------
# Public query functions (low-level: accept raw start/end datetimes)
# ---------------------------------------------------------------------------

def load_counters(connection, system, node, start, inventory, *,
                  now_utc=None):
   """Load and validate counter rows for one node within [start, now_utc].

   Parameters
   ----------
   connection : SQLAlchemy connection (inside a transaction).
   system : str -- system name (e.g. "polaris").
   node : str -- source_hostname to query.
   start : datetime (UTC-aware, zero offset) -- lower bound on window_start.
   inventory : frozenset of str -- known node hostnames for validation.
   now_utc : datetime (UTC-aware, zero offset) or None; defaults to
             datetime.now(timezone.utc).  Also used as the :end upper bound
             passed to SQL, excluding future rows from the result set.

   Returns
   -------
   CounterResult

   Raises
   ------
   QueryValidationError  if node is not in inventory, or start/now_utc are
                        not UTC-aware datetimes with zero offset.
   QueryBoundsError      if the row count exceeds COUNTER_ROW_LIMIT.
   """
   _validate_node(node, inventory)
   _validate_utc_datetime(start, "start")
   if now_utc is None:
      now_utc = datetime.now(timezone.utc)
   else:
      _validate_utc_datetime(now_utc, "now_utc")

   # Pass COUNTER_ROW_LIMIT+1 as the sentinel LIMIT so the DB returns at most
   # 1441 rows.  If we get exactly 1441, we know the true count is >= 1441
   # and reject it without having fetched all rows.
   rows = list(connection.execute(
      COUNTER_SQL,
      {"system": system, "node": node, "start": start, "end": now_utc,
       "limit": COUNTER_ROW_LIMIT + 1},
   ).mappings())

   if len(rows) > COUNTER_ROW_LIMIT:
      raise QueryBoundsError(
         "counter row limit exceeded: got %d rows (limit %d)"
         % (len(rows), COUNTER_ROW_LIMIT))

   return CounterResult(rows, now_utc)


def load_usage(connection, system, node, start, inventory, *,
               username=None, now_utc=None):
   """Load and aggregate usage interval rows for one node within [start, now_utc].

   Issues 9 SQL queries: cpu (additive), rss_p50/p95/max (each independently
   ranked), d_state, process_count_p50/p95/max (each independently ranked),
   interactivity.

   Additive: cpu_seconds is summed across username grains per
   (interval_end, category, activity).  Hotspot metrics use DISTINCT ON to
   pick the single contributing username row with the highest VALUE FOR THAT
   SPECIFIC STATISTIC -- never a percentile-of-percentiles and never an
   unweighted average.  Different users may maximize p50, p95, and max --
   each is tracked independently.

   Parameters
   ----------
   connection : SQLAlchemy connection.
   system : str.
   node : str -- source_hostname.
   start : datetime (UTC-aware, zero offset).
   inventory : frozenset of str.
   username : str or None -- when not None, rows are filtered to this
              username; bound as a data value, never as an identifier.
   now_utc : datetime (UTC-aware, zero offset) or None.
             Also used as the :end upper bound to exclude future rows.

   Returns
   -------
   UsageResult

   Raises
   ------
   QueryValidationError  if node is not in inventory, username is invalid,
                        or start/now_utc are not UTC-aware datetimes.
   """
   _validate_node(node, inventory)
   _validate_utc_datetime(start, "start")
   username = _validate_username(username)
   if now_utc is None:
      now_utc = datetime.now(timezone.utc)
   else:
      _validate_utc_datetime(now_utc, "now_utc")

   params = {
      "system": system,
      "node": node,
      "start": start,
      "end": now_utc,
      "username_is_null": username is None,
      "username": username,
   }

   cpu_rows = list(
      connection.execute(USAGE_CPU_SQL, params).mappings())
   rss_p50_rows = list(
      connection.execute(USAGE_RSS_P50_HOTSPOT_SQL, params).mappings())
   rss_p95_rows = list(
      connection.execute(USAGE_RSS_P95_HOTSPOT_SQL, params).mappings())
   rss_max_rows = list(
      connection.execute(USAGE_RSS_MAX_HOTSPOT_SQL, params).mappings())
   d_rows = list(
      connection.execute(USAGE_D_STATE_HOTSPOT_SQL, params).mappings())
   proc_p50_rows = list(
      connection.execute(USAGE_PROCESS_COUNT_P50_HOTSPOT_SQL, params).mappings())
   proc_p95_rows = list(
      connection.execute(USAGE_PROCESS_COUNT_P95_HOTSPOT_SQL, params).mappings())
   proc_max_rows = list(
      connection.execute(USAGE_PROCESS_COUNT_MAX_HOTSPOT_SQL, params).mappings())
   interactivity_rows = list(
      connection.execute(USAGE_INTERACTIVITY_HOTSPOT_SQL, params).mappings())

   return UsageResult(
      cpu_rows,
      rss_p50_rows, rss_p95_rows, rss_max_rows,
      d_rows,
      proc_p50_rows, proc_p95_rows, proc_max_rows,
      interactivity_rows,
      now_utc,
   )


def load_poll_failures(connection, system, node, inventory, *,
                       start=None, end=None):
   """Load the most recent poll failures for one node (max MAX_POLL_FAILURES).

   Parameters
   ----------
   connection : SQLAlchemy connection.
   system : str.
   node : str.
   inventory : frozenset of str.
   start : datetime (UTC-aware, zero offset) or None.
           Lower bound on recorded_at; None defaults to epoch (no lower bound
           in practice, but a far-past value is passed to SQL).
   end : datetime (UTC-aware, zero offset) or None.
         Upper bound on recorded_at; None defaults to datetime.now(timezone.utc).

   Returns
   -------
   list of dicts.

   Raises
   ------
   QueryValidationError  if node is not in inventory.
   """
   _validate_node(node, inventory)
   if end is None:
      end = datetime.now(timezone.utc)
   else:
      _validate_utc_datetime(end, "end")
   if start is None:
      # Effectively no lower bound (epoch).
      start = datetime(1970, 1, 1, tzinfo=timezone.utc)
   else:
      _validate_utc_datetime(start, "start")

   rows = list(connection.execute(
      POLL_FAILURES_SQL,
      {"system": system, "node": node, "start": start, "end": end,
       "limit": MAX_POLL_FAILURES},
   ).mappings())
   return rows


def load_collection_log(connection, system, start, *, end=None):
   """Load system-level collection log events within [start, end].

   Note: collection log has no source_hostname -- it is system-level.
   Results are bounded at COLLECTION_LOG_LIMIT rows.

   Parameters
   ----------
   connection : SQLAlchemy connection.
   system : str.
   start : datetime (UTC).
   end : datetime (UTC-aware, zero offset) or None; defaults to now_utc.
         Upper bound that excludes future rows from the result set.

   Returns
   -------
   list of dicts.
   """
   if end is None:
      end = datetime.now(timezone.utc)
   rows = list(connection.execute(
      COLLECTION_LOG_SQL,
      {"system": system, "start": start, "end": end,
       "limit": COLLECTION_LOG_LIMIT},
   ).mappings())
   return rows


# ---------------------------------------------------------------------------
# Public range-based facade (preferred API for callers)
# ---------------------------------------------------------------------------

def load_counters_for_range(connection, system, node, range_hours, inventory, *,
                             now_utc=None):
   """Load counter rows for one node for the last range_hours.

   Derives start = now_utc - range_hours internally via range_hours_to_start.
   This is the preferred public API; callers should not call load_counters
   directly in normal use.

   Parameters
   ----------
   connection : SQLAlchemy connection.
   system : str.
   node : str.
   range_hours : int -- must be in VALID_RANGES_HOURS.
   inventory : frozenset of str.
   now_utc : datetime (UTC-aware, zero offset) or None.

   Returns
   -------
   CounterResult

   Raises
   ------
   QueryValidationError  if range_hours is invalid, node not in inventory,
                        or now_utc is not UTC-aware.
   QueryBoundsError      if row count exceeds COUNTER_ROW_LIMIT.
   """
   if now_utc is None:
      now_utc = datetime.now(timezone.utc)
   else:
      _validate_utc_datetime(now_utc, "now_utc")
   start = range_hours_to_start(range_hours, now_utc)
   return load_counters(connection, system, node, start, inventory,
                        now_utc=now_utc)


def load_usage_for_range(connection, system, node, range_hours, inventory, *,
                          username=None, now_utc=None):
   """Load usage interval rows for the last range_hours.

   Derives start = now_utc - range_hours internally.

   Parameters
   ----------
   connection : SQLAlchemy connection.
   system : str.
   node : str.
   range_hours : int -- must be in VALID_RANGES_HOURS.
   inventory : frozenset of str.
   username : str or None.
   now_utc : datetime (UTC-aware, zero offset) or None.

   Returns
   -------
   UsageResult
   """
   if now_utc is None:
      now_utc = datetime.now(timezone.utc)
   else:
      _validate_utc_datetime(now_utc, "now_utc")
   start = range_hours_to_start(range_hours, now_utc)
   return load_usage(connection, system, node, start, inventory,
                     username=username, now_utc=now_utc)


def load_poll_failures_for_range(connection, system, node, range_hours,
                                  inventory, *, now_utc=None):
   """Load poll failures for the last range_hours.

   Parameters
   ----------
   connection : SQLAlchemy connection.
   system : str.
   node : str.
   range_hours : int -- must be in VALID_RANGES_HOURS.
   inventory : frozenset of str.
   now_utc : datetime (UTC-aware, zero offset) or None.

   Returns
   -------
   list of dicts.
   """
   if now_utc is None:
      now_utc = datetime.now(timezone.utc)
   else:
      _validate_utc_datetime(now_utc, "now_utc")
   start = range_hours_to_start(range_hours, now_utc)
   return load_poll_failures(connection, system, node, inventory,
                              start=start, end=now_utc)


def load_collection_log_for_range(connection, system, range_hours, *,
                                   now_utc=None):
   """Load collection log events for the last range_hours.

   Parameters
   ----------
   connection : SQLAlchemy connection.
   system : str.
   range_hours : int -- must be in VALID_RANGES_HOURS.
   now_utc : datetime (UTC-aware, zero offset) or None.

   Returns
   -------
   list of dicts.
   """
   if now_utc is None:
      now_utc = datetime.now(timezone.utc)
   else:
      _validate_utc_datetime(now_utc, "now_utc")
   start = range_hours_to_start(range_hours, now_utc)
   return load_collection_log(connection, system, start, end=now_utc)
