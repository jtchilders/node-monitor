"""Production-shaped in-memory fixtures for Task 4 query tests.

All shapes here are derived directly from writer/collector contracts:

  * counter JSONB: writer._counter() -> rates["cpu_busy_pct"] / rates["network"] /
    writer._lustre_summary(rates["lustre_md_ops"])
  * usage JSONB: usage._percentile_stats() -> {"p50", "p95", "max"}

These fixtures are NOT from a live database; they encode the exact stored
shape so unit tests can assert against it without any PostgreSQL connection.
A mismatch against a live row must be fixed in the adapter/query contract,
never converted to silent nulls.
"""

from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Production-shaped JSONB values (derived from writer/collector contracts)
# ---------------------------------------------------------------------------

def production_counter_row():
   """Return a dict that mirrors the JSONB-column values the writer stores
   for a node_counter_minute row produced from a full counter record.

   Shapes derived from:
     - rates["cpu_busy_pct"] -> _percentile_stats -> {"p50", "p95", "max"}
     - rates["network"][iface][field] -> {"p50", "p95", "max"}
     - writer._lustre_summary -> {"p50_sum", "p95_sum", "max_sum", "target_count"}
   """
   return {
      # cpu_busy_pct: scalar aggregate across all CPUs -> p50/p95/max of
      # per-sample busy fraction over the one-minute window.
      "cpu_busy_pct": {"p50": 10.0, "p95": 20.0, "max": 25.0},

      # network_rates: {iface: {field: {p50, p95, max}}} -- loopback "lo" is
      # excluded by _compute_network_deltas (see metrics._LOOPBACK_IFACE).
      "network_rates": {
         "eth0": {
            "rx_bytes_per_sec": {"p50": 1.0, "p95": 2.0, "max": 3.0},
            "tx_bytes_per_sec": {"p50": 4.0, "p95": 5.0, "max": 6.0},
         },
      },

      # lustre_md_summary: {operation: {p50_sum, p95_sum, max_sum, target_count}}
      # written by writer._lustre_summary() which sums per-target stats.
      "lustre_md_summary": {
         "open": {
            "p50_sum": 4.0, "p95_sum": 5.0, "max_sum": 6.0, "target_count": 1,
         },
      },

      # Scalar gauge columns (not JSONB -- included for completeness)
      "mem_available_kb": 900000,
      "cached_kb": 100000,
      "shmem_kb": 10000,
      "load1": 1.0, "load5": 2.0, "load15": 3.0,
      "procs_running": 2, "procs_total": 100, "socket_count": 8,
      "sample_count": 6, "expected_count": 6,
      "meets_minimum_samples": True,
   }


def production_usage_row(username="alice"):
   """Return a dict that mirrors the JSONB-column values the writer stores
   for a node_usage_intervals row.

   Shapes derived from:
     - process_count: _percentile_stats(grain.process_counts) -> {"p50", "p95", "max"}
     - rss_kb: _percentile_stats(grain.rss_per_sample) -> {"p50", "p95", "max"}
     - d_state_fraction: scalar float [0, 1]
     - interactivity_fraction: scalar float [0, 1]
   """
   return {
      "system": "polaris",
      "source_hostname": "login-04",
      "interval_start": datetime(2026, 9, 30, 13, 0, tzinfo=timezone.utc),
      "interval_end": datetime(2026, 9, 30, 13, 15, tzinfo=timezone.utc),
      "category": "ai_coding",
      "activity": "active",
      "username": username,
      # username_key is COALESCE(username, '') -- a generated column.
      "username_key": username if username is not None else "",
      "process_count": {"p50": 1, "p95": 2, "max": 2},
      "cpu_seconds": 9.0,
      "rss_kb": {"p50": 4000, "p95": 8000, "max": 9000},
      "d_state_fraction": 0.40,
      "interactivity_fraction": 0.80,
      "sample_count": 6,
      "expected_count": 6,
      "unmeasured_count": 0,
   }


# ---------------------------------------------------------------------------
# Allowed time-range constants (exact set; any caller value outside this
# set is rejected at the query layer before SQL is issued).
# ---------------------------------------------------------------------------

VALID_RANGES_HOURS = (1, 3, 6, 12, 24)

# ---------------------------------------------------------------------------
# Username length limit (design: 256 UTF-8 bytes max)
# ---------------------------------------------------------------------------

MAX_USERNAME_BYTES = 256

# ---------------------------------------------------------------------------
# Counter row cap and staleness thresholds
# ---------------------------------------------------------------------------

COUNTER_ROW_LIMIT = 1440           # 24 h * 60 min/h -- hard cap; 1441 is rejected
COUNTER_STALENESS_SECONDS = 120    # newest window_end must be within this many seconds
USAGE_STALENESS_SECONDS = 1200     # newest closed interval_end must be within this many

# ---------------------------------------------------------------------------
# Poll failure cap
# ---------------------------------------------------------------------------

MAX_POLL_FAILURES = 10

# ---------------------------------------------------------------------------
# Lustre peak-sum naming constants (design: API label must be "peak-sum")
# ---------------------------------------------------------------------------

LUSTRE_PEAK_SUM_SOURCE = "max_sum"   # field name inside lustre_md_summary JSONB
LUSTRE_PEAK_SUM_API_LABEL = "peak-sum"
