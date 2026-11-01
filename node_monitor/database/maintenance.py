"""Monthly partitions of the time-series tables."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

import psycopg
from psycopg import sql

from node_monitor.config import RetentionConfig
from node_monitor.database.codec import to_db_value
from node_monitor.pipeline.timestamps import require_aware

_SCHEMA = "node_monitor"

# Partitioned table -> the RetentionConfig field that says how long to keep it.
PARTITIONED = {
   "node_metrics_1m": "node_metrics_days",
   "user_usage_5m": "usage_summary_days",
   "node_usage_5m": "usage_summary_days",
   "collection_events": "collection_events_days",
}

# Partitioned table -> its partition key column.
PARTITION_KEY = {
   "node_metrics_1m": "minute",
   "user_usage_5m": "interval_start",
   "node_usage_5m": "interval_start",
   "collection_events": "ts",
}

_PARTITION_SUFFIX = re.compile(r"_y(\d{4})m(\d{2})$")


def _month_floor(ts: datetime) -> datetime:
   """First instant of the UTC month containing `ts`; partitions follow UTC months."""
   require_aware(ts)
   return ts.astimezone(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _next_month(start: datetime) -> datetime:
   if start.month == 12:
      return start.replace(year=start.year + 1, month=1)
   return start.replace(month=start.month + 1)


def partition_name(table: str, month_start: datetime) -> str:
   """`<table>_yYYYYmMM` for the UTC month containing `month_start`."""
   start = _month_floor(month_start)
   return f"{table}_y{start:%Y}m{start:%m}"


def ensure_partition(conn: psycopg.Connection, table: str, month_start: datetime) -> bool:
   """Create `table`'s partition for the UTC month containing `month_start` if it is missing.

   Returns True if this call created it. Runs in the caller's transaction, if any.
   """
   start = _month_floor(month_start)
   name = partition_name(table, start)
   # A transaction block (a savepoint inside the caller's) so the advisory lock is held
   # until the create is visible, even on an autocommit connection. Collectors for other
   # systems share these tables; without the lock two concurrent CREATE ... PARTITION OF
   # can fail with DuplicateTable or a catalog unique violation despite IF NOT EXISTS.
   with conn.transaction():
      conn.execute(
         "SELECT pg_advisory_xact_lock(hashtext('node_monitor.partition:' || %s))", (table,)
      )
      exists = conn.execute(
         "SELECT to_regclass(%s) IS NOT NULL", (f"{_SCHEMA}.{name}",)
      ).fetchone()[0]
      if exists:
         return False
      conn.execute(
         sql.SQL(
            "CREATE TABLE IF NOT EXISTS {} PARTITION OF {} FOR VALUES FROM ({}) TO ({})"
         ).format(
            sql.Identifier(_SCHEMA, name),
            sql.Identifier(_SCHEMA, table),
            sql.Literal(start),
            sql.Literal(_next_month(start)),
         )
      )
   return True


def ensure_partitions(conn: psycopg.Connection, now: datetime, months_ahead: int = 2) -> list[str]:
   """Create partitions for the current UTC month and `months_ahead` more, for every table.

   Returns the names of the partitions this call created.
   """
   month = _month_floor(now)
   months = [month]
   for _ in range(months_ahead):
      months.append(_next_month(months[-1]))
   created = []
   for table in PARTITIONED:
      for start in months:
         if ensure_partition(conn, table, start):
            created.append(partition_name(table, start))
   return created


def _attached_partitions(conn: psycopg.Connection, table: str) -> list[str]:
   rows = conn.execute(
      "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid"
      " WHERE i.inhparent = to_regclass(%s) ORDER BY c.relname",
      (f"{_SCHEMA}.{table}",),
   ).fetchall()
   return [r[0] for r in rows]


def apply_retention(
   conn: psycopg.Connection, now: datetime, retention: RetentionConfig
) -> list[str]:
   """Detach and drop partitions whose whole month is older than the table's retention.

   A partition goes only when its upper bound is <= now - days: a month that still holds
   rows within retention is kept. Only `<table>_yYYYYmMM` children of the known tables are
   considered, and the current month is never dropped. Returns the dropped names.
   """
   require_aware(now)
   current = _month_floor(now)
   dropped = []
   for table, attr in PARTITIONED.items():
      cutoff = now - timedelta(days=getattr(retention, attr))
      prefix = table + "_y"
      for name in _attached_partitions(conn, table):
         match = (
            _PARTITION_SUFFIX.fullmatch(name[len(table) :]) if name.startswith(prefix) else None
         )
         if match is None:
            continue
         start = datetime(int(match[1]), int(match[2]), 1, tzinfo=timezone.utc)
         if start >= current or _next_month(start) > cutoff:
            continue
         with conn.transaction():
            conn.execute(
               sql.SQL("ALTER TABLE {} DETACH PARTITION {}").format(
                  sql.Identifier(_SCHEMA, table), sql.Identifier(_SCHEMA, name)
               )
            )
            conn.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier(_SCHEMA, name)))
         dropped.append(name)
   return dropped


def write_heartbeat(
   conn: psycopg.Connection,
   *,
   system: str,
   collector_host: str,
   version: str,
   started_at: datetime,
   now: datetime,
   status: dict,
) -> None:
   """Upsert this collector's row in collector_heartbeat."""
   require_aware(started_at)
   require_aware(now)
   conn.execute(
      "INSERT INTO collector_heartbeat (system, collector_host, version, started_at,"
      " last_seen, status) VALUES (%s, %s, %s, %s, %s, %s)"
      " ON CONFLICT (system) DO UPDATE SET collector_host = EXCLUDED.collector_host,"
      " version = EXCLUDED.version, started_at = EXCLUDED.started_at,"
      " last_seen = EXCLUDED.last_seen, status = EXCLUDED.status",
      (system, collector_host, version, started_at, now, to_db_value(status)),
   )


def run(conn: psycopg.Connection, now: datetime, retention: RetentionConfig) -> dict:
   """Daily maintenance: create upcoming partitions, then drop expired ones."""
   return {
      "created": ensure_partitions(conn, now),
      "dropped": apply_retention(conn, now, retention),
   }
