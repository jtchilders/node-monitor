"""Read queries over the node_monitor schema, for the CLI and later the web UI."""

import psycopg
from psycopg.rows import dict_row


def heartbeat(conn: psycopg.Connection, system: str) -> dict | None:
   """The collector_heartbeat row for `system` keyed by column name, or None."""
   with conn.cursor(row_factory=dict_row) as cur:
      cur.execute(
         "SELECT system, collector_host, version, started_at, last_seen, status"
         " FROM collector_heartbeat WHERE system = %s",
         (system,),
      )
      return cur.fetchone()
