"""`node-monitor history` — prints 5m roll-ups via database/queries.py."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import click
import psycopg

from node_monitor.config import ConfigError, load_config
from node_monitor.database.connect import DatabaseUnavailable, connect


def _parse_ts(s: str) -> datetime:
   """Parse an ISO8601-ish timestamp; naive is treated as UTC."""
   try:
      dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
   except Exception as exc:
      raise click.ClickException(f"invalid timestamp {s!r}: {exc}") from exc
   if dt.tzinfo is None:
      dt = dt.replace(tzinfo=timezone.utc)
   return dt.astimezone(timezone.utc)


def _load(config_path):
   try:
      return load_config(config_path)
   except ConfigError as exc:
      raise click.ClickException(str(exc)) from exc


@click.command("history")
@click.option("--user", "user_", default=None, help="Username to query.")
@click.option("--node", "node_", default=None, help="Node to query (host or label).")
@click.option("--from", "from_", default=None, help="Start of range (ISO8601).")
@click.option("--to", "to_", default=None, help="End of range (ISO8601).")
@click.option(
   "--bucket",
   type=click.Choice(["1m", "5m", "1h", "1d"]),
   default=None,
   help="Roll-up bucket.",
)
@click.option(
   "--format",
   "fmt",
   type=click.Choice(["table", "json", "csv"]),
   default="table",
   show_default=True,
)
@click.option("--config", "config_path", type=click.Path(), default=None)
def history(user_, node_, from_, to_, bucket, fmt, config_path) -> None:
   """Show roll-up history for a user or node."""
   if not user_ and not node_:
      raise click.ClickException("one of --user or --node is required")
   if user_ and node_:
      raise click.ClickException("only one of --user or --node may be given")

   config = _load(config_path)
   url = config.web.database_url
   if not url:
      raise click.ClickException(f"web.database_url is not set in {config.source_path}")

   now = datetime.now(timezone.utc)
   from_ts = _parse_ts(from_) if from_ else now - timedelta(days=7)
   to_ts = _parse_ts(to_) if to_ else now
   if from_ts >= to_ts:
      raise click.ClickException("--from must be before --to")

   # Resolve bucket default: user -> 5m, node -> 1m
   if bucket is None:
      bucket = "5m" if user_ else "1m"

   # Validate bucket matches target
   if user_ and bucket not in ("5m", "1h", "1d"):
      raise click.ClickException(f"bucket {bucket!r} invalid for --user; use 5m, 1h or 1d")
   if node_ and bucket not in ("1m", "1h", "1d"):
      raise click.ClickException(f"bucket {bucket!r} invalid for --node; use 1m, 1h or 1d")

   def fetch():
      with connect(
         url, connect_timeout_sec=config.database.connect_timeout_sec, autocommit=True
      ) as conn:
         from node_monitor.database import queries

         if user_:
            return queries.user_history(conn, config.system, user_, from_ts, to_ts, bucket)
         return queries.node_history(conn, config.system, node_, from_ts, to_ts, bucket)

   try:
      rows = fetch()
   except (psycopg.Error, DatabaseUnavailable) as exc:
      reason = (str(exc).strip().splitlines() or [type(exc).__name__])[0]
      raise click.ClickException(f"database error: {reason}") from exc

   _output(rows, fmt)


def _output(rows: list[dict], fmt: str) -> None:
   if fmt == "json":
      serialised = []
      for r in rows:
         d = {}
         for k, v in r.items():
            d[k] = v.isoformat() if isinstance(v, datetime) else v
         serialised.append(d)
      click.echo(json.dumps(serialised, indent=2))
      return
   if fmt == "csv":
      import csv
      import io

      if not rows:
         click.echo("")
         return
      keys = list(rows[0].keys())

      def _cell(v):
         if v is None:
            return ""
         if isinstance(v, datetime):
            return v.isoformat()
         return v

      buf = io.StringIO()
      w = csv.writer(buf, lineterminator="\n")
      w.writerow(keys)
      for r in rows:
         w.writerow([_cell(r.get(k)) for k in keys])
      click.echo(buf.getvalue(), nl=False)
      return
   # table
   if not rows:
      click.echo("(no rows)")
      return
   keys = list(rows[0].keys())

   def _fmt_val(v):
      if v is None:
         return "-"
      if isinstance(v, datetime):
         return v.isoformat()
      return str(v)

   widths = [len(k) for k in keys]
   str_rows = [[_fmt_val(r.get(k)) for k in keys] for r in rows]
   for sr in str_rows:
      for i, v in enumerate(sr):
         widths[i] = max(widths[i], len(v))
   header = "  ".join(k.ljust(widths[i]) for i, k in enumerate(keys))
   click.echo(header)
   click.echo("  ".join("-" * widths[i] for i in range(len(keys))))
   for sr in str_rows:
      click.echo("  ".join(v.ljust(widths[i]) for i, v in enumerate(sr)))
