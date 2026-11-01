"""Monthly partitions of the time-series tables."""

from __future__ import annotations

from datetime import datetime, timezone

import psycopg
from psycopg import sql

from node_monitor.pipeline.timestamps import require_aware

_SCHEMA = "node_monitor"


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
   exists = conn.execute("SELECT to_regclass(%s) IS NOT NULL", (f"{_SCHEMA}.{name}",)).fetchone()[0]
   if exists:
      return False
   # IF NOT EXISTS still matters: another session may create it between the check and here.
   conn.execute(
      sql.SQL("CREATE TABLE IF NOT EXISTS {} PARTITION OF {} FOR VALUES FROM ({}) TO ({})").format(
         sql.Identifier(_SCHEMA, name),
         sql.Identifier(_SCHEMA, table),
         sql.Literal(start),
         sql.Literal(_next_month(start)),
      )
   )
   return True
