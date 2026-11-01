"""Commands that read the live tables: status and nodes."""

import json
from contextlib import contextmanager
from datetime import datetime, timezone

import click
import psycopg

from node_monitor.cli.output import (
   Column,
   fmt_age,
   fmt_kb,
   fmt_pct,
   json_default,
   render,
)
from node_monitor.config import ConfigError, load_config
from node_monitor.database import queries
from node_monitor.database.connect import DatabaseUnavailable, connect

# A node row not refreshed this long no longer reflects the node, whatever session_state says.
STALE_AFTER_SEC = 120
HEARTBEAT_STALE_SEC = 180


def common_options(func):
   func = click.option(
      "--format",
      "fmt",
      type=click.Choice(["table", "json", "csv"]),
      default="table",
      show_default=True,
   )(func)
   return click.option("--config", "config_path", type=click.Path(), default=None)(func)


def _load(config_path):
   try:
      return load_config(config_path)
   except ConfigError as exc:
      raise click.ClickException(str(exc)) from exc


def _reason(exc: BaseException) -> str:
   return (str(exc).strip().splitlines() or [type(exc).__name__])[0]


@contextmanager
def _reader(config):
   """A read connection from web.database_url; an unreachable or failing database is a
   one-line ClickException rather than a traceback."""
   url = config.web.database_url
   if not url:
      raise click.ClickException(f"web.database_url is not set in {config.source_path}")
   try:
      conn = connect(url, connect_timeout_sec=config.database.connect_timeout_sec, autocommit=True)
   except (psycopg.Error, DatabaseUnavailable) as exc:
      raise click.ClickException(f"database unreachable: {_reason(exc)}") from exc
   try:
      with conn:
         yield conn
   except (psycopg.Error, DatabaseUnavailable) as exc:
      raise click.ClickException(f"database error: {_reason(exc)}") from exc


def _node_state(row: dict, now: datetime) -> str:
   age = (now - row["updated_at"]).total_seconds()
   return "stale" if age > STALE_AFTER_SEC else row["session_state"]


def _with_state(rows: list[dict], now: datetime) -> list[dict]:
   return [{**r, "state": _node_state(r, now)} for r in rows]


def _ages(now):
   return lambda ts: fmt_age(ts, now)


def _collector(row: dict | None, now: datetime) -> dict | None:
   if row is None:
      return None
   age = (now - row["last_seen"]).total_seconds()
   status = row.get("status") or {}
   return {
      "host": row["collector_host"],
      "version": row["version"],
      "last_seen": row["last_seen"],
      "age_sec": age,
      "stale": age > HEARTBEAT_STALE_SEC,
      "state": status.get("state"),
      "connected": status.get("connected"),
      "spooled_batches": status.get("spooled_batches"),
      "outage_since": status.get("outage_since"),
   }


@click.command()
@common_options
def status(config_path, fmt) -> None:
   """Collector health and per-node session state; exit 1 if the heartbeat is missing or stale."""
   config = _load(config_path)
   with _reader(config) as conn:
      hb = queries.heartbeat(conn, config.system)
      node_rows = queries.node_states(conn, config.system)
   now = datetime.now(timezone.utc)
   collector = _collector(hb, now)
   nodes = _with_state(node_rows, now)
   columns = [
      Column("label", "Node"),
      Column("state", "State"),
      Column("updated_at", "Updated", _ages(now), "right"),
      Column("last_census_at", "Census", _ages(now), "right"),
      Column("probe_version", "Probe"),
   ]
   if fmt == "json":
      payload = {
         "collector": collector,
         "nodes": [{c.key: n[c.key] for c in columns} for n in nodes],
      }
      click.echo(json.dumps(payload, default=json_default, indent=2))
   else:
      if collector is None:
         click.echo(f"collector: no heartbeat recorded for system {config.system}")
      elif fmt == "table":
         flag = "  STALE" if collector["stale"] else ""
         click.echo(
            f"collector: {collector['host']}  version {collector['version']}  "
            f"state {collector['state'] or '-'}  last seen "
            f"{fmt_age(collector['last_seen'], now)} ago{flag}"
         )
         click.echo(
            f"  connected: {collector['connected']}  spooled batches: "
            f"{collector['spooled_batches']}  outage since: {collector['outage_since'] or '-'}"
         )
      if fmt == "table":
         click.echo("")
      render(nodes, columns, fmt)
   if collector is None or collector["stale"]:
      raise SystemExit(1)


@click.command()
@common_options
def nodes(config_path, fmt) -> None:
   """Per-node load, memory, process counts and census age."""
   config = _load(config_path)
   with _reader(config) as conn:
      rows = queries.node_states(conn, config.system)
   now = datetime.now(timezone.utc)
   rows = _with_state(rows, now)
   for r in rows:
      used, total = r["mem_used_kb"], r["mem_total_kb"]
      r["mem_pct"] = 100 * used / total if used is not None and total else None
   render(
      rows,
      [
         Column("label", "Node"),
         Column("state", "State"),
         Column("load1", "Load", lambda v: "-" if v is None else f"{v:.2f}", "right"),
         Column("mem_used_kb", "Mem used", fmt_kb, "right"),
         Column("mem_total_kb", "Mem total", fmt_kb, "right"),
         Column("mem_pct", "Mem", fmt_pct, "right"),
         Column("procs_total", "Procs", justify="right"),
         Column("user_processes", "User procs", justify="right"),
         Column("users", "Users", justify="right"),
         Column("last_census_at", "Census", _ages(now), "right"),
      ],
      fmt,
   )
