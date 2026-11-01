"""Read queries over the node_monitor schema, for the CLI and later the web UI."""

import psycopg
from psycopg import sql
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


# Sort key -> column, for the whitelist. Composed with psycopg.sql so a key never reaches
# the statement as text from the caller.
_PROCESS_SORTS = {
   "cpu": "cpu_seconds",
   "rss": "rss_kb",
   "idle": "idle_for_sec",
   "threads": "threads",
   "gpu": "gpu_mem_mb",
}
_USER_SORTS = {
   "rss": "rss_kb",
   "cpu": "cpu_seconds",
   "procs": "processes",
   "gpu": "gpu_mem_mb",
   "idle": "idle_processes",
}


def _rows(conn, query, params) -> list[dict]:
   with conn.cursor(row_factory=dict_row) as cur:
      cur.execute(query, params)
      return cur.fetchall()


def _order(sorts: dict[str, str], sort: str, reverse: bool) -> sql.Composed:
   if sort not in sorts:
      raise ValueError(f"unknown sort {sort!r}; expected one of {sorted(sorts)}")
   direction = "ASC" if reverse else "DESC"
   return sql.SQL("{} {} NULLS LAST").format(sql.Identifier(sorts[sort]), sql.SQL(direction))


def _limit(limit: int | None) -> tuple[sql.Composable, list]:
   if limit is None:
      return sql.SQL(""), []
   return sql.SQL(" LIMIT %s"), [limit]


# live_processes keeps a node's last snapshot after its session ends or the node leaves the
# config; live_snapshots.ts, compared with the database's clock, says how old it is.
_FRESH = sql.SQL(
   "EXISTS (SELECT 1 FROM live_snapshots s WHERE s.system = p.system AND s.node = p.node"
   " AND s.ts >= now() - make_interval(secs => %s))"
)


def stale_process_nodes(
   conn: psycopg.Connection, system: str, max_age_sec: float, *, node: str | None = None
) -> list[str]:
   """Labels of the nodes whose processes `max_age_sec` leaves out, optionally only `node`
   (a host or a label)."""
   where = "p.system = %s AND NOT {fresh}"
   params: list = [system, max_age_sec]
   if node is not None:
      where += " AND (p.node = %s OR COALESCE(ln.label, n.label) = %s)"
      params += [node, node]
   query = sql.SQL(
      "SELECT DISTINCT COALESCE(ln.label, n.label, p.node) AS label"
      " FROM live_processes p"
      " LEFT JOIN live_nodes ln ON ln.system = p.system AND ln.node = p.node"
      " LEFT JOIN nodes n ON n.system = p.system AND n.node = p.node"
      " WHERE " + where + " ORDER BY label"
   ).format(fresh=_FRESH)
   return [r["label"] for r in _rows(conn, query, params)]


def node_states(conn: psycopg.Connection, system: str) -> list[dict]:
   """One row per live_nodes row, by label, with the node's user process and user counts."""
   return _rows(
      conn,
      "SELECT ln.node, COALESCE(ln.label, n.label, ln.node) AS label, ln.session_state,"
      " ln.updated_at, ln.last_census_at, ln.load1, ln.mem_used_kb, ln.mem_total_kb,"
      " ln.procs_total, ln.probe_version, ln.boot_id,"
      " COALESCE(c.user_processes, 0) AS user_processes, COALESCE(c.users, 0) AS users"
      " FROM live_nodes ln"
      " LEFT JOIN nodes n ON n.system = ln.system AND n.node = ln.node"
      " LEFT JOIN (SELECT system, node, count(*) AS user_processes,"
      "            count(DISTINCT username) AS users"
      "            FROM live_processes WHERE is_user_process GROUP BY system, node) c"
      "   ON c.system = ln.system AND c.node = ln.node"
      " WHERE ln.system = %s ORDER BY label, ln.node",
      (system,),
   )


def processes(
   conn: psycopg.Connection,
   system: str,
   *,
   user: str | None = None,
   node: str | None = None,
   category: str | None = None,
   user_only: bool = True,
   idle_min_sec: float | None = None,
   presence: str | None = None,
   sort: str = "cpu",
   reverse: bool = False,
   limit: int | None = None,
   max_age_sec: float | None = None,
) -> list[dict]:
   """Live processes, by default user processes only, sorted descending with NULLs last.

   `node` matches a host or a label. `idle_min_sec` keeps only processes whose cpu_state is
   idle, since idle_for_sec is meaningful only for those. `max_age_sec` keeps only nodes
   whose snapshot is at most that old; `ts` in each row is the snapshot's time."""
   order = _order(_PROCESS_SORTS, sort, reverse)
   where = [sql.SQL("p.system = %s")]
   params: list = [system]
   if user_only:
      where.append(sql.SQL("p.is_user_process"))
   for text, value in (
      ("p.username = %s", user),
      ("(p.node = %s OR COALESCE(ln.label, n.label) = %s)", node),
      ("p.category = %s", category),
      ("p.owner_presence = %s", presence),
   ):
      if value is not None:
         where.append(sql.SQL(text))
         params += [value] * text.count("%s")
   if idle_min_sec is not None:
      where.append(sql.SQL("p.cpu_state = 'idle' AND p.idle_for_sec >= %s"))
      params.append(idle_min_sec)
   if max_age_sec is not None:
      where.append(_FRESH)
      params.append(max_age_sec)
   limit_sql, limit_params = _limit(limit)
   query = sql.SQL(
      "SELECT p.node, COALESCE(ln.label, n.label, p.node) AS label, p.username, p.pid,"
      " p.category, p.activity, p.cpu_seconds, p.rss_kb, p.gpu_mem_mb, p.threads, p.state,"
      " p.cpu_state, p.idle_for_sec, p.idle_for_is_lower_bound, p.idle_bucket,"
      " p.owner_presence, p.cmdline, p.ts"
      " FROM live_processes p"
      " LEFT JOIN live_nodes ln ON ln.system = p.system AND ln.node = p.node"
      " LEFT JOIN nodes n ON n.system = p.system AND n.node = p.node"
      " WHERE {where} ORDER BY {order}, p.node, p.pid{limit}"
   ).format(where=sql.SQL(" AND ").join(where), order=order, limit=limit_sql)
   return _rows(conn, query, params + limit_params)


def users(
   conn: psycopg.Connection,
   system: str,
   *,
   node: str | None = None,
   category: str | None = None,
   sort: str = "rss",
   reverse: bool = False,
   limit: int | None = None,
   max_age_sec: float | None = None,
) -> list[dict]:
   """Per-user totals over user processes; `top_category` is the category with the most of
   that user's RSS. `max_age_sec` keeps only nodes whose snapshot is at most that old."""
   order = _order(_USER_SORTS, sort, reverse)
   where = [
      sql.SQL("p.system = %s"),
      sql.SQL("p.is_user_process"),
      sql.SQL("p.username IS NOT NULL"),
   ]
   params: list = [system]
   if node is not None:
      where.append(sql.SQL("(p.node = %s OR COALESCE(ln.label, n.label) = %s)"))
      params += [node, node]
   if category is not None:
      where.append(sql.SQL("p.category = %s"))
      params.append(category)
   if max_age_sec is not None:
      where.append(_FRESH)
      params.append(max_age_sec)
   limit_sql, limit_params = _limit(limit)
   query = sql.SQL(
      "WITH p AS (SELECT p.username, p.node, p.category, p.cpu_seconds, p.rss_kb,"
      "                  p.gpu_mem_mb, p.cpu_state"
      "           FROM live_processes p"
      "           LEFT JOIN live_nodes ln ON ln.system = p.system AND ln.node = p.node"
      "           LEFT JOIN nodes n ON n.system = p.system AND n.node = p.node"
      "           WHERE {where}),"
      " cat AS (SELECT DISTINCT ON (username) username, category AS top_category"
      "         FROM (SELECT username, category, sum(COALESCE(rss_kb, 0)) AS rss"
      "               FROM p GROUP BY username, category) s"
      "         ORDER BY username, rss DESC, category NULLS LAST)"
      " SELECT username, count(*) AS processes, count(DISTINCT node) AS nodes,"
      " sum(cpu_seconds) AS cpu_seconds, sum(rss_kb)::bigint AS rss_kb,"
      " sum(gpu_mem_mb) AS gpu_mem_mb,"
      " count(*) FILTER (WHERE cpu_state = 'idle') AS idle_processes,"
      " (SELECT top_category FROM cat WHERE cat.username = p.username) AS top_category"
      " FROM p GROUP BY username ORDER BY {order}, username{limit}"
   ).format(where=sql.SQL(" AND ").join(where), order=order, limit=limit_sql)
   return _rows(conn, query, params + limit_params)
