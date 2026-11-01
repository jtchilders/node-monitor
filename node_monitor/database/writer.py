"""Writes pipeline records to PostgreSQL: one batch per transaction, safe to replay.

Every statement is idempotent except the collection_events INSERT (events have no natural
key), so a batch replayed from the spool after a lost acknowledgement changes nothing but
the event log. Live state only ever moves forward in time: an older live snapshot or
live_node row, replayed after newer data was written, is skipped.
"""

from __future__ import annotations

import functools
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import psycopg
from psycopg import sql
from psycopg.pq import TransactionStatus

from node_monitor.config import RetentionConfig
from node_monitor.database.codec import to_db_value
from node_monitor.database.connect import DatabaseUnavailable, connect, is_unavailable
from node_monitor.database.maintenance import ensure_partition, partition_name
from node_monitor.pipeline.enrich import LIVE_PROCESS_COLUMNS
from node_monitor.pipeline.node_metrics import NODE_METRICS_COLUMNS
from node_monitor.pipeline.records import (
   COLLECTION_EVENT,
   LIVE_NODE,
   LIVE_PROCESSES,
   NODE_HARDWARE,
   NODE_METRICS_1M,
   NODE_USAGE_5M,
   USER_USAGE_5M,
   Record,
)
from node_monitor.pipeline.timestamps import require_aware
from node_monitor.pipeline.usage import NODE_USAGE_COLUMNS, USER_USAGE_COLUMNS

_IDENTITY = ("system", "node")

# The table each record kind is written to; also the keys of WriteResult counts.
_TABLE = {
   LIVE_PROCESSES: "live_processes",
   LIVE_NODE: "live_nodes",
   NODE_HARDWARE: "node_hardware",
   NODE_METRICS_1M: "node_metrics_1m",
   USER_USAGE_5M: "user_usage_5m",
   NODE_USAGE_5M: "node_usage_5m",
   COLLECTION_EVENT: "collection_events",
}

# Partitioned table -> (partition key column, RetentionConfig field).
_PARTITIONED = {
   "node_metrics_1m": ("minute", "node_metrics_days"),
   "user_usage_5m": ("interval_start", "usage_summary_days"),
   "node_usage_5m": ("interval_start", "usage_summary_days"),
   "collection_events": ("ts", "collection_events_days"),
}

# Summary table -> (columns after system/node, primary key).
_SUMMARIES = {
   "node_metrics_1m": (NODE_METRICS_COLUMNS, ("system", "node", "minute")),
   "user_usage_5m": (USER_USAGE_COLUMNS, ("system", "node", "username", "interval_start")),
   "node_usage_5m": (NODE_USAGE_COLUMNS, ("system", "node", "interval_start")),
}

# Columns a live_node row may set; other keys are ignored.
_LIVE_NODE_COLUMNS = (
   "label",
   "updated_at",
   "session_state",
   "boot_id",
   "probe_version",
   "load1",
   "mem_used_kb",
   "mem_total_kb",
   "procs_total",
   "last_census_at",
)
# Every live_node row carries these; both are NOT NULL, and updated_at orders the updates.
_LIVE_NODE_REQUIRED = ("updated_at", "session_state")

# The probe's hello may grow keys the schema has no column for; those are ignored.
_HARDWARE_COLUMNS = (
   "boot_id",
   "btime",
   "collected_at",
   "probe_version",
   "python_version",
   "hostname_fqdn",
   "cpu_model",
   "cpu_logical",
   "sockets",
   "cores_per_socket",
   "cpu_max_freq_khz",
   "numa_nodes",
   "mem_total_kb",
   "swap_total_kb",
   "hugepage_size_kb",
   "kernel_release",
   "os_pretty_name",
   "net_fs_mounts",
   "net_ifaces",
   "gpus",
)
# boot_id is part of node_hardware's key, so it cannot be NULL.
_UNKNOWN_BOOT_ID = "unknown"

_EVENT_COLUMNS = ("ts", "system", "node", "event_type", "detail")

_MISSING_PARTITION_SQLSTATE = "23514"
_MISSING_PARTITION_TEXT = "no partition of relation"


def _insert(table, columns):
   return sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
      sql.Identifier(table),
      sql.SQL(", ").join(map(sql.Identifier, columns)),
      sql.SQL(", ").join(sql.Placeholder() * len(columns)),
   )


def _set_excluded(columns):
   return sql.SQL(", ").join(
      sql.SQL("{0} = EXCLUDED.{0}").format(sql.Identifier(c)) for c in columns
   )


def _summary_upsert(table):
   columns, key = _SUMMARIES[table]
   all_columns = _IDENTITY + columns
   return sql.SQL("{} ON CONFLICT ({}) DO UPDATE SET {}").format(
      _insert(table, all_columns),
      sql.SQL(", ").join(map(sql.Identifier, key)),
      _set_excluded([c for c in all_columns if c not in key]),
   )


_SUMMARY_SQL = {table: _summary_upsert(table) for table in _SUMMARIES}

_HARDWARE_SQL = sql.SQL("{} ON CONFLICT (system, node, boot_id) DO NOTHING").format(
   _insert("node_hardware", _IDENTITY + _HARDWARE_COLUMNS)
)

_EVENT_SQL = _insert("collection_events", _EVENT_COLUMNS)

_PROCESS_SQL = _insert("live_processes", _IDENTITY + ("ts",) + LIVE_PROCESS_COLUMNS)

# The upsert both orders snapshots and locks the node's row, so the DELETE and INSERT that
# follow cannot interleave with another writer's snapshot of the same node.
_SNAPSHOT_SQL = """
   INSERT INTO live_snapshots AS s (system, node, ts) VALUES (%s, %s, %s)
   ON CONFLICT (system, node) DO UPDATE SET ts = EXCLUDED.ts
   WHERE s.ts <= EXCLUDED.ts
   RETURNING 1
"""

_DELETE_PROCESSES_SQL = "DELETE FROM live_processes WHERE system = %s AND node = %s"

# label and boot_id follow the newest record, so a replayed older one cannot revert them;
# a record without a label, or without a known boot_id, leaves the stored value alone.
_NODE_SQL = """
   INSERT INTO nodes AS n (system, node, label, first_seen, last_seen, boot_id)
   VALUES (%(system)s, %(node)s, %(label)s, %(seen)s, %(seen)s, %(boot_id)s)
   ON CONFLICT (system, node) DO UPDATE SET
      label = CASE WHEN %(has_label)s AND EXCLUDED.last_seen >= n.last_seen
                   THEN EXCLUDED.label ELSE n.label END,
      boot_id = CASE WHEN EXCLUDED.boot_id IS NOT NULL AND EXCLUDED.last_seen >= n.last_seen
                     THEN EXCLUDED.boot_id ELSE n.boot_id END,
      first_seen = LEAST(n.first_seen, EXCLUDED.first_seen),
      last_seen = GREATEST(n.last_seen, EXCLUDED.last_seen)
"""


@functools.lru_cache(maxsize=64)
def _live_node_upsert(columns):
   all_columns = _IDENTITY + columns
   return sql.SQL(
      "{} ON CONFLICT (system, node) DO UPDATE SET {}"
      " WHERE live_nodes.updated_at <= EXCLUDED.updated_at"
   ).format(_insert("live_nodes", all_columns), _set_excluded(columns))


@dataclass(frozen=True)
class WriteResult:
   # Rows inserted or updated, per table written; a table the batch had rows for is
   # present even when nothing changed (a stale snapshot, a known boot).
   rows_written: dict[str, int]
   # Rows dropped because their time is older than the table's retention.
   rows_beyond_retention: dict[str, int]
   # Partitions this write created.
   partitions_created: list[str]


@dataclass
class _Plan:
   """A batch prepared for writing: filtered, deduplicated and converted to parameters."""

   # (system, node) -> newest live_processes row of the batch.
   snapshots: dict = field(default_factory=dict)
   live_nodes: list = field(default_factory=list)
   hardware: list = field(default_factory=list)
   summaries: dict = field(default_factory=dict)
   events: list = field(default_factory=list)
   # (table, partition name) -> a time in that partition's month.
   months: dict = field(default_factory=dict)
   beyond: Counter = field(default_factory=Counter)


def _node_params(row, seen, *, label_present):
   return {
      "system": row["system"],
      "node": row["node"],
      "label": row.get("label"),
      "has_label": label_present,
      "seen": seen,
      "boot_id": row.get("boot_id"),
   }


def _is_missing_partition(exc):
   message = (exc.diag.message_primary if exc.diag else None) or str(exc)
   return exc.sqlstate == _MISSING_PARTITION_SQLSTATE and _MISSING_PARTITION_TEXT in message


class DatabaseWriter:
   def __init__(self, url: str, *, retention: RetentionConfig, connect_timeout_sec: float = 10):
      # Read on every (re)connect, so it can be changed between writes.
      self.url = url
      self._retention = retention
      self._connect_timeout_sec = connect_timeout_sec
      self._conn = None

   def write(self, records: list[Record], now: datetime) -> WriteResult:
      """Write `records` in one transaction.

      Raises DatabaseUnavailable for an outage (the batch may be retried later) and
      ValueError, before touching the database, for a malformed batch. Any other error
      rolls the batch back and is re-raised unchanged: it would fail again on retry.
      """
      plan = self._plan(records, now)
      try:
         conn = self._connection()
         created = []
         try:
            written = self._apply(conn, plan)
         except psycopg.errors.CheckViolation as exc:
            if not _is_missing_partition(exc):
               raise
            created = self._create_partitions(conn, plan)
            written = self._apply(conn, plan)
      except BaseException as exc:
         outage = is_unavailable(exc)
         # After an outage the session is suspect even if psycopg has not marked it broken.
         self._discard_if_unusable(force=outage)
         if outage:
            raise DatabaseUnavailable(f"database unavailable: {exc}") from exc
         raise
      return WriteResult(
         rows_written=written,
         rows_beyond_retention=dict(plan.beyond),
         partitions_created=created,
      )

   def close(self) -> None:
      conn, self._conn = self._conn, None
      if conn is not None:
         conn.close()

   # --- connection ---------------------------------------------------------------------

   def _connection(self):
      if self._conn is None or self._conn.closed:
         # Autocommit, with explicit transaction blocks: nothing can leave an implicit
         # transaction open that would turn the next batch's BEGIN into a savepoint.
         self._conn = connect(
            self.url, connect_timeout_sec=self._connect_timeout_sec, autocommit=True
         )
      return self._conn

   def _discard_if_unusable(self, *, force=False):
      conn = self._conn
      if conn is None:
         return
      if (
         force
         or conn.closed
         or conn.broken
         or conn.info.transaction_status != TransactionStatus.IDLE
      ):
         self._conn = None
         try:
            conn.close()
         except Exception:
            pass

   # --- preparing a batch --------------------------------------------------------------

   def _plan(self, records, now):
      require_aware(now)
      for rec in records:
         if rec.kind not in _TABLE:
            raise ValueError(f"unknown record kind {rec.kind!r}")
      cutoffs = {
         table: now - timedelta(days=getattr(self._retention, attr))
         for table, (_, attr) in _PARTITIONED.items()
      }
      plan = _Plan()
      for rec in records:
         table = _TABLE[rec.kind]
         row = rec.row
         if table in _PARTITIONED:
            column = _PARTITIONED[table][0]
            ts = row.get(column)
            if ts is None:
               raise ValueError(f"{rec.kind} record without {column}")
            # A naive time would be read in the server's zone: refuse it before comparing.
            require_aware(ts)
            if ts < cutoffs[table]:
               plan.beyond[table] += 1
               continue
            plan.months.setdefault((table, partition_name(table, ts)), ts)
         if rec.kind == LIVE_PROCESSES:
            key = (row["system"], row["node"])
            current = plan.snapshots.get(key)
            # Later in the batch wins a tie, as it would across batches.
            if current is None or row["ts"] >= current["ts"]:
               plan.snapshots[key] = row
         elif rec.kind == LIVE_NODE:
            for column in _LIVE_NODE_REQUIRED:
               if row.get(column) is None:
                  raise ValueError(f"live_node record without {column}")
            plan.live_nodes.append(row)
         elif rec.kind == NODE_HARDWARE:
            plan.hardware.append(row)
         elif rec.kind == COLLECTION_EVENT:
            detail = row.get("detail")
            plan.events.append(
               (
                  row["ts"],
                  row["system"],
                  row.get("node"),
                  row["event_type"],
                  to_db_value({} if detail is None else detail),
               )
            )
         else:
            columns = _SUMMARIES[table][0]
            plan.summaries.setdefault(table, []).append(
               tuple(to_db_value(row.get(c)) for c in _IDENTITY + columns)
            )
      return plan

   # --- writing ------------------------------------------------------------------------

   def _create_partitions(self, conn, plan):
      created = []
      with conn.transaction():
         for (table, name), ts in sorted(plan.months.items()):
            if ensure_partition(conn, table, ts):
               created.append(name)
      return created

   def _apply(self, conn, plan):
      written = Counter()
      with conn.transaction(), conn.cursor() as cur:
         # Partitioned tables first: a missing partition fails the attempt before any
         # other work is done and rolled back.
         for table, params in plan.summaries.items():
            cur.executemany(_SUMMARY_SQL[table], params)
            written[table] += cur.rowcount
         if plan.events:
            cur.executemany(_EVENT_SQL, plan.events)
            written["collection_events"] += cur.rowcount
         for row in plan.hardware:
            written["node_hardware"] += self._write_hardware(cur, row)
         for row in plan.live_nodes:
            written["live_nodes"] += self._write_live_node(cur, row)
         for row in plan.snapshots.values():
            written["live_processes"] += self._write_snapshot(cur, row)
      return dict(written)

   def _write_hardware(self, cur, row):
      values = dict(row)
      if values.get("boot_id") is None:
         values["boot_id"] = _UNKNOWN_BOOT_ID
      cur.execute(
         _HARDWARE_SQL, [to_db_value(values.get(c)) for c in _IDENTITY + _HARDWARE_COLUMNS]
      )
      inserted = cur.rowcount
      cur.execute(_NODE_SQL, _node_params(row, row["collected_at"], label_present=False))
      return inserted

   def _write_live_node(self, cur, row):
      columns = tuple(c for c in _LIVE_NODE_COLUMNS if c in row)
      cur.execute(_live_node_upsert(columns), [to_db_value(row[c]) for c in _IDENTITY + columns])
      updated = cur.rowcount
      cur.execute(_NODE_SQL, _node_params(row, row["updated_at"], label_present="label" in row))
      return updated

   def _write_snapshot(self, cur, row):
      system, node, ts = row["system"], row["node"], row["ts"]
      cur.execute(_SNAPSHOT_SQL, (system, node, ts))
      if cur.fetchone() is None:
         return 0
      cur.execute(_DELETE_PROCESSES_SQL, (system, node))
      # A census cannot list one process twice; if it ever did, the primary key would fail
      # this batch on every replay, so the last entry wins instead.
      procs = {(p.get("pid"), p.get("start_time")): p for p in row["processes"]}
      if not procs:
         return 0
      cur.executemany(
         _PROCESS_SQL,
         [
            [system, node, ts] + [to_db_value(p.get(c)) for c in LIVE_PROCESS_COLUMNS]
            for p in procs.values()
         ],
      )
      return cur.rowcount
