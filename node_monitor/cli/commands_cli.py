"""`node-monitor commands search` — search command_lines via DB."""

from __future__ import annotations

import json
import re

import click
import psycopg
from psycopg.rows import dict_row

from node_monitor.config import ConfigError, load_config
from node_monitor.database.connect import DatabaseUnavailable, connect


def _load(config_path):
   try:
      return load_config(config_path)
   except ConfigError as exc:
      raise click.ClickException(str(exc)) from exc


def _escape_like(s: str) -> str:
   return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


@click.group("commands")
def commands() -> None:
   """Search collected command lines."""


@commands.command("search")
@click.argument("pattern")
@click.option("--regex", "use_regex", is_flag=True, help="Treat PATTERN as a regex.")
@click.option("--user", default=None, help="Filter by username.")
@click.option("--activity", default=None, help="Filter by activity.")
@click.option("--tag", default=None, help="Filter by tag.")
@click.option("--limit", type=click.IntRange(min=1, max=100), default=50, show_default=True)
@click.option(
   "--format",
   "fmt",
   type=click.Choice(["table", "json", "csv"]),
   default="table",
   show_default=True,
)
@click.option("--config", "config_path", type=click.Path(), default=None)
def search(pattern, use_regex, user, activity, tag, limit, fmt, config_path) -> None:
   """Search command lines for PATTERN."""
   if use_regex:
      try:
         re.compile(pattern)
      except re.error as exc:
         click.echo(f"Error: invalid regex: {exc}", err=True)
         raise SystemExit(2) from None

   config = _load(config_path)
   url = config.web.database_url
   if not url:
      raise click.ClickException(f"web.database_url is not set in {config.source_path}")

   conditions: list[str] = ["system=%s"]
   params: list[object] = [config.system]

   if use_regex:
      conditions.append("cmdline ~ %s")
      params.append(pattern)
   else:
      escaped = _escape_like(pattern)
      conditions.append("cmdline ILIKE %s ESCAPE '\\'")
      params.append("%" + escaped + "%")

   if user:
      conditions.append("username=%s")
      params.append(user)
   if activity:
      conditions.append("activity=%s")
      params.append(activity)
   if tag:
      conditions.append("%s = ANY(tags)")
      params.append(tag)

   where_sql = " AND ".join(conditions)

   def fetch():
      with connect(
         url, connect_timeout_sec=config.database.connect_timeout_sec, autocommit=True
      ) as conn:
         with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
               "SELECT count(*)::bigint AS total FROM command_lines WHERE " + where_sql,
               tuple(params),
            )
            total_row = cur.fetchone()
            total = int(total_row["total"]) if total_row else 0
            cur.execute(
               "SELECT cmdline, username, activity, category, tags,"
               " first_seen, last_seen, nodes, sample_count"
               " FROM command_lines"
               " WHERE " + where_sql + " ORDER BY last_seen DESC, cmdline"
               " LIMIT %s",
               tuple(params) + (limit,),
            )
            rows = cur.fetchall()
            return total, rows

   try:
      total, rows = fetch()
   except (psycopg.Error, DatabaseUnavailable) as exc:
      reason = (str(exc).strip().splitlines() or [type(exc).__name__])[0]
      raise click.ClickException(f"database error: {reason}") from exc

   _output(rows, total, fmt)


def _output(rows: list[dict], total: int, fmt: str) -> None:
   if fmt == "json":
      items = []
      for r in rows:
         d = dict(r)
         for k in ("first_seen", "last_seen"):
            if isinstance(d.get(k), object) and hasattr(d[k], "isoformat"):
               d[k] = d[k].isoformat()  # type: ignore[union-attr]
         items.append(d)
      click.echo(json.dumps({"items": items, "total": total}, indent=2, default=str))
      return
   if fmt == "csv":
      import csv
      import io

      buf = io.StringIO()
      w = csv.writer(buf, lineterminator="\n")
      keys = [
         "cmdline",
         "username",
         "activity",
         "category",
         "tags",
         "first_seen",
         "last_seen",
         "nodes",
         "sample_count",
      ]
      w.writerow(keys)
      for r in rows:

         def _iso(v):
            return v.isoformat() if v is not None and hasattr(v, "isoformat") else v

         w.writerow(
            [
               r.get("cmdline", ""),
               r.get("username", ""),
               r.get("activity", ""),
               r.get("category", ""),
               ",".join(r.get("tags") or []),
               _iso(r.get("first_seen")) or "",
               _iso(r.get("last_seen")) or "",
               ",".join(r.get("nodes") or []),
               r.get("sample_count", ""),
            ]
         )
      click.echo(buf.getvalue(), nl=False)
      return
   # table
   if not rows:
      click.echo(f"(no results, total {total})")
      return
   click.echo(f"total {total}, showing {len(rows)}")
   for r in rows:
      tags_s = ",".join(r.get("tags") or [])
      click.echo(
         f"{r.get('username')}  {r.get('activity')}  {r.get('category')}"
         f"  [{tags_s}]  {r.get('cmdline')}"
      )
