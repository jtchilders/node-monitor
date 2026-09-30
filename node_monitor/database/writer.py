"""Compact transactional PostgreSQL writer for production-shaped records.

The writer accepts only the five relational record types, validates each
record before opening a transaction, converts the JSONL contract into the
initial source schema, and never performs DDL or database lifecycle work.
"""

from datetime import datetime, timezone

from sqlalchemy import JSON, bindparam, text

from node_monitor.output.contracts import validate_record


class DatabaseWriteError(RuntimeError):
   """A bounded writer-domain failure that contains no driver details."""


_ACCEPTED_TYPES = frozenset((
   "node_hardware",
   "node_counter_samples",
   "node_usage_intervals",
   "node_poll_failures",
   "node_collection_log",
))


def _utc_timestamp(value):
   try:
      parsed = datetime.fromisoformat(value)
   except (TypeError, ValueError):
      raise DatabaseWriteError("invalid UTC timestamp") from None
   if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
      raise DatabaseWriteError("invalid UTC timestamp")
   return parsed.astimezone(timezone.utc)


def _json_statement(sql, *names):
   statement = text(sql)
   if names:
      statement = statement.bindparams(
         *(bindparam(name, type_=JSON) for name in names))
   return statement


_HARDWARE_SQL = _json_statement("""
INSERT INTO node_monitor.node_hardware AS current (
   system, source_hostname, first_seen, last_verified, boot_id, btime,
   cpu_model, cpu_logical, sockets, cores_per_socket, cpu_max_freq_khz,
   numa_nodes, mem_total_kb, swap_total_kb, hugepage_size_kb,
   kernel_release, os_pretty_name, net_fs_mounts, net_ifaces, gpus,
   probe_version
) VALUES (
   :system, :source_hostname, :first_seen, :last_verified, :boot_id, :btime,
   :cpu_model, :cpu_logical, :sockets, :cores_per_socket, :cpu_max_freq_khz,
   :numa_nodes, :mem_total_kb, :swap_total_kb, :hugepage_size_kb,
   :kernel_release, :os_pretty_name, :net_fs_mounts,
   CAST(:net_ifaces AS jsonb), CAST(:gpus AS jsonb), :probe_version
)
ON CONFLICT (system, source_hostname) DO UPDATE SET
   last_verified = EXCLUDED.last_verified,
   boot_id = COALESCE(EXCLUDED.boot_id, current.boot_id),
   btime = COALESCE(EXCLUDED.btime, current.btime),
   cpu_model = EXCLUDED.cpu_model,
   cpu_logical = EXCLUDED.cpu_logical,
   sockets = EXCLUDED.sockets,
   cores_per_socket = EXCLUDED.cores_per_socket,
   cpu_max_freq_khz = EXCLUDED.cpu_max_freq_khz,
   numa_nodes = EXCLUDED.numa_nodes,
   mem_total_kb = EXCLUDED.mem_total_kb,
   swap_total_kb = EXCLUDED.swap_total_kb,
   hugepage_size_kb = EXCLUDED.hugepage_size_kb,
   kernel_release = EXCLUDED.kernel_release,
   os_pretty_name = EXCLUDED.os_pretty_name,
   net_fs_mounts = EXCLUDED.net_fs_mounts,
   net_ifaces = EXCLUDED.net_ifaces,
   gpus = EXCLUDED.gpus,
   probe_version = EXCLUDED.probe_version
""", "net_ifaces", "gpus")


_COUNTER_SQL = _json_statement("""
INSERT INTO node_monitor.node_counter_minute (
   system, source_hostname, window_start, window_end, collector_hostname,
   probe_version, daemon_version, sample_count, expected_count, coverage,
   mem_available_kb, cached_kb, shmem_kb, load1, load5, load15,
   procs_running, procs_total, socket_count, cpu_busy_pct, network_rates,
   lustre_md_summary, meets_minimum_samples, invalid_pair_count,
   excess_sample_count
) VALUES (
   :system, :source_hostname, :window_start, :window_end, :collector_hostname,
   :probe_version, :daemon_version, :sample_count, :expected_count, :coverage,
   :mem_available_kb, :cached_kb, :shmem_kb, :load1, :load5, :load15,
   :procs_running, :procs_total, :socket_count,
   CAST(:cpu_busy_pct AS jsonb), CAST(:network_rates AS jsonb),
   CAST(:lustre_md_summary AS jsonb), :meets_minimum_samples,
   :invalid_pair_count, :excess_sample_count
)
ON CONFLICT (system, source_hostname, window_start) DO UPDATE SET
   window_end = EXCLUDED.window_end,
   collector_hostname = EXCLUDED.collector_hostname,
   probe_version = EXCLUDED.probe_version,
   daemon_version = EXCLUDED.daemon_version,
   sample_count = EXCLUDED.sample_count,
   expected_count = EXCLUDED.expected_count,
   coverage = EXCLUDED.coverage,
   mem_available_kb = EXCLUDED.mem_available_kb,
   cached_kb = EXCLUDED.cached_kb,
   shmem_kb = EXCLUDED.shmem_kb,
   load1 = EXCLUDED.load1,
   load5 = EXCLUDED.load5,
   load15 = EXCLUDED.load15,
   procs_running = EXCLUDED.procs_running,
   procs_total = EXCLUDED.procs_total,
   socket_count = EXCLUDED.socket_count,
   cpu_busy_pct = EXCLUDED.cpu_busy_pct,
   network_rates = EXCLUDED.network_rates,
   lustre_md_summary = EXCLUDED.lustre_md_summary,
   meets_minimum_samples = EXCLUDED.meets_minimum_samples,
   invalid_pair_count = EXCLUDED.invalid_pair_count,
   excess_sample_count = EXCLUDED.excess_sample_count
""", "cpu_busy_pct", "network_rates", "lustre_md_summary")


_USAGE_SQL = _json_statement("""
INSERT INTO node_monitor.node_usage_intervals (
   system, source_hostname, interval_start, interval_end, category, activity,
   username, process_count, cpu_seconds, rss_kb, d_state_fraction,
   interactivity_fraction, sample_count, expected_count, unmeasured_count
) VALUES (
   :system, :source_hostname, :interval_start, :interval_end, :category,
   :activity, :username, CAST(:process_count AS jsonb), :cpu_seconds,
   CAST(:rss_kb AS jsonb), :d_state_fraction, :interactivity_fraction,
   :sample_count, :expected_count, :unmeasured_count
)
ON CONFLICT (system, source_hostname, interval_start, category, activity, username_key)
DO UPDATE SET
   interval_end = EXCLUDED.interval_end,
   username = EXCLUDED.username,
   process_count = EXCLUDED.process_count,
   cpu_seconds = EXCLUDED.cpu_seconds,
   rss_kb = EXCLUDED.rss_kb,
   d_state_fraction = EXCLUDED.d_state_fraction,
   interactivity_fraction = EXCLUDED.interactivity_fraction,
   sample_count = EXCLUDED.sample_count,
   expected_count = EXCLUDED.expected_count,
   unmeasured_count = EXCLUDED.unmeasured_count
""", "process_count", "rss_kb")


_POLL_FAILURE_SQL = text("""
INSERT INTO node_monitor.node_poll_failures (
   system, source_hostname, loop, recorded_at, failure_type, detail,
   consecutive_failures, breaker_state
) VALUES (
   :system, :source_hostname, :loop, :recorded_at, :failure_type, :detail,
   :consecutive_failures, :breaker_state
)
""")


_COLLECTION_LOG_SQL = _json_statement("""
INSERT INTO node_monitor.node_collection_log (
   system, recorded_at, event, detail
) VALUES (:system, :recorded_at, :event, CAST(:detail AS jsonb))
""", "detail")


def _hardware(record, now):
   parameters = dict(record)
   parameters["first_seen"] = _utc_timestamp(parameters.pop("first_seen_utc"))
   parameters["last_verified"] = now
   return _HARDWARE_SQL, parameters


def _lustre_summary(targets):
   summary = {}
   for operations in targets.values():
      for operation, statistics in operations.items():
         if statistics is None:
            continue
         bucket = summary.setdefault(operation, {
            "p50_sum": 0.0, "p95_sum": 0.0, "max_sum": 0.0,
            "target_count": 0,
         })
         bucket["p50_sum"] += statistics["p50"]
         bucket["p95_sum"] += statistics["p95"]
         bucket["max_sum"] += statistics["max"]
         bucket["target_count"] += 1
   return summary


def _counter(record, now):
   del now
   gauges = record["end_of_window"]
   rates = record["rates"]
   audit = record["audit"]
   parameters = {
      "system": record["system"],
      "source_hostname": record["source_hostname"],
      "window_start": _utc_timestamp(record["window_start_utc"]),
      "window_end": _utc_timestamp(record["window_end_utc"]),
      "collector_hostname": record["collector_hostname"],
      "probe_version": record["probe_version"],
      "daemon_version": record["daemon_version"],
      "sample_count": record["sample_count"],
      "expected_count": record["expected_count"],
      "coverage": record["coverage"],
      "cpu_busy_pct": rates["cpu_busy_pct"],
      "network_rates": rates["network"],
      "lustre_md_summary": _lustre_summary(rates["lustre_md_ops"]),
      "meets_minimum_samples": audit["meets_minimum_samples"],
      "invalid_pair_count": len(audit["invalid_pairs"]),
      "excess_sample_count": audit["excess_sample_count"],
   }
   for name in (
      "mem_available_kb", "cached_kb", "shmem_kb", "load1", "load5",
      "load15", "procs_running", "procs_total", "socket_count",
   ):
      parameters[name] = gauges.get(name)
   return _COUNTER_SQL, parameters


def _usage(record, now):
   del now
   parameters = dict(record)
   parameters["interval_start"] = _utc_timestamp(
      parameters.pop("interval_start_utc"))
   parameters["interval_end"] = _utc_timestamp(
      parameters.pop("interval_end_utc"))
   return _USAGE_SQL, parameters


def _poll_failure(record, now):
   del now
   parameters = dict(record)
   parameters["recorded_at"] = _utc_timestamp(parameters.pop("timestamp_utc"))
   return _POLL_FAILURE_SQL, parameters


def _collection_log(record, now):
   del now
   parameters = dict(record)
   parameters["recorded_at"] = _utc_timestamp(parameters.pop("timestamp_utc"))
   return _COLLECTION_LOG_SQL, parameters


_ADAPTERS = {
   "node_hardware": _hardware,
   "node_counter_samples": _counter,
   "node_usage_intervals": _usage,
   "node_poll_failures": _poll_failure,
   "node_collection_log": _collection_log,
}


class DatabaseWriter:
   """Validate, adapt, and transactionally persist source records."""

   def __init__(self, database, clock=None):
      self._database = database
      self._clock = clock or (lambda: datetime.now(timezone.utc))

   def _prepare(self, record_type, record):
      if record_type not in _ACCEPTED_TYPES:
         raise DatabaseWriteError("unsupported record type")
      validate_record(record_type, record)
      return _ADAPTERS[record_type](record, self._clock())

   def write_record(self, record_type, record):
      return self.write_records(((record_type, record),))

   def write_records(self, records):
      prepared = [self._prepare(record_type, record)
                  for record_type, record in records]
      try:
         with self._database.begin() as connection:
            for statement, parameters in prepared:
               connection.execute(statement, parameters)
      except Exception:
         raise DatabaseWriteError("database write failed") from None
      return len(prepared)
