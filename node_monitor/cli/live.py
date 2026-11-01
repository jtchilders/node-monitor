"""Commands that read the live tables: status, nodes and the process views."""

import json
from contextlib import contextmanager
from datetime import datetime, timezone

import click
import psycopg

from node_monitor.cli.output import (
   Column,
   fmt_age,
   fmt_duration,
   fmt_kb,
   fmt_pct,
   json_default,
   render,
   visible,
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
            f"collector: {visible(str(collector['host']))}  "
            f"version {visible(str(collector['version']))}  "
            f"state {visible(str(collector['state'] or '-'))}  last seen "
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


COMMAND_WIDTH = 60
_UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


class IdleDuration(click.ParamType):
   """30m, 2h, 1d, 90s, or plain integer seconds."""

   name = "duration"

   def convert(self, value, param, ctx):
      text = str(value).strip().lower()
      unit = _UNIT_SECONDS.get(text[-1:], None)
      digits = text[:-1] if unit else text
      if not (digits.isascii() and digits.isdigit()):
         self.fail(f"{value!r} is not a duration such as 30m, 2h, 1d, 90s or 45", param, ctx)
      return int(digits) * (unit or 1)


def _truncate(text, width=COMMAND_WIDTH):
   """`text` made visible, then cut to `width` characters, so the cut counts what is shown."""
   if text is None:
      return None
   text = visible(text)
   if len(text) <= width:
      return text
   return text[: width - 1] + "…"


def _display_rows(rows):
   """Adds the table-only `idle` and `command` keys; the raw keys stay for JSON and CSV."""
   return [
      {
         **r,
         "idle": fmt_duration(r["idle_for_sec"], r["idle_for_is_lower_bound"]),
         "command": _truncate(r["cmdline"]),
      }
      for r in rows
   ]


def _cols(fmt, table, raw):
   """Table columns carry the display keys; JSON and CSV use `raw`, which has the full
   command and the idle value with its lower-bound flag."""
   return table if fmt == "table" else raw


def _process_options(func, *, sorts, default_sort):
   func = common_options(func)
   func = click.option(
      "--limit",
      type=click.IntRange(min=0),
      default=20,
      show_default=True,
      help="Rows to show; 0 shows all.",
   )(func)
   func = click.option("--reverse", is_flag=True, help="Sort ascending.")(func)
   func = click.option("--sort", type=click.Choice(sorts), default=default_sort, show_default=True)(
      func
   )
   func = click.option("-c", "--category", default=None, help="Only this category.")(func)
   func = include_stale_option(func)
   return click.option("-n", "--node", default=None, help="Only this node (host or label).")(func)


def _limit(limit):
   return limit or None


def include_stale_option(func):
   return click.option(
      "--include-stale",
      is_flag=True,
      help=f"Include nodes whose process snapshot is older than {STALE_AFTER_SEC}s.",
   )(func)


def _max_age(conn, system, include_stale, node):
   """The snapshot age limit for the process queries, None with --include-stale.

   The nodes it leaves out are named on stderr, so JSON and CSV on stdout stay clean."""
   if include_stale:
      return None
   stale = queries.stale_process_nodes(conn, system, STALE_AFTER_SEC, node=node)
   if stale:
      labels = ", ".join(visible(label) for label in stale)
      click.echo(f"excluded stale nodes: {labels} (use --include-stale)", err=True)
   return STALE_AFTER_SEC


def process_options(default_sort):
   return lambda f: _process_options(
      f, sorts=list(queries._PROCESS_SORTS), default_sort=default_sort
   )


_ID = [Column("label", "Node"), Column("username", "User"), Column("pid", "PID", justify="right")]
_CPU = Column("cpu_seconds", "CPU s/min", lambda v: "-" if v is None else f"{v:.0f}", "right")
_RSS = Column("rss_kb", "RSS", fmt_kb, "right")
_TOP_RAW = [
   *_ID,
   Column("category", "Category"),
   Column("activity", "Activity"),
   _CPU,
   _RSS,
   Column("state", "State"),
   Column("idle_for_sec", "Idle"),
   Column("idle_for_is_lower_bound", "Idle lower bound"),
   Column("owner_presence", "Presence"),
   Column("cmdline", "Command"),
   Column("ts", "Snapshot"),
]
_TOP_TABLE = [
   *_ID,
   Column("category", "Category"),
   Column("activity", "Activity"),
   _CPU,
   _RSS,
   Column("state", "State"),
   Column("idle", "Idle", justify="right"),
   Column("owner_presence", "Presence"),
   Column("command", "Command"),
]
_IDLE_RAW = [
   *_ID,
   Column("category", "Category"),
   Column("activity", "Activity"),
   Column("idle_for_sec", "Idle"),
   Column("idle_for_is_lower_bound", "Idle lower bound"),
   _RSS,
   Column("owner_presence", "Presence"),
   Column("cmdline", "Command"),
   Column("ts", "Snapshot"),
]
_IDLE_TABLE = [
   *_ID,
   Column("category", "Category"),
   Column("activity", "Activity"),
   Column("idle", "Idle", justify="right"),
   _RSS,
   Column("owner_presence", "Presence"),
   Column("command", "Command"),
]


@click.command()
@process_options("cpu")
@click.option("-u", "--user", default=None, help="Only this user.")
def top(config_path, fmt, node, include_stale, category, sort, reverse, limit, user) -> None:
   """User processes, busiest first."""
   config = _load(config_path)
   with _reader(config) as conn:
      max_age = _max_age(conn, config.system, include_stale, node)
      rows = queries.processes(
         conn, config.system, user=user, node=node, category=category,
         sort=sort, reverse=reverse, limit=_limit(limit), max_age_sec=max_age,
      )  # fmt: skip
   render(_display_rows(rows), _cols(fmt, _TOP_TABLE, _TOP_RAW), fmt)


@click.command()
@common_options
@click.option(
   "--limit",
   type=click.IntRange(min=0),
   default=20,
   show_default=True,
   help="Rows to show; 0 shows all.",
)
@click.option("--reverse", is_flag=True, help="Sort ascending.")
@click.option(
   "--sort", type=click.Choice(list(queries._USER_SORTS)), default="rss", show_default=True
)
@click.option("-c", "--category", default=None, help="Only this category.")
@include_stale_option
@click.option("-n", "--node", default=None, help="Only this node (host or label).")
def users(config_path, fmt, node, include_stale, category, sort, reverse, limit) -> None:
   """Per-user totals over user processes."""
   config = _load(config_path)
   with _reader(config) as conn:
      max_age = _max_age(conn, config.system, include_stale, node)
      rows = queries.users(
         conn, config.system, node=node, category=category,
         sort=sort, reverse=reverse, limit=_limit(limit), max_age_sec=max_age,
      )  # fmt: skip
   render(
      rows,
      [
         Column("username", "User"),
         Column("processes", "Procs", justify="right"),
         Column("nodes", "Nodes", justify="right"),
         _CPU,
         _RSS,
         Column("gpu_mem_mb", "GPU MiB", justify="right"),
         Column("idle_processes", "Idle procs", justify="right"),
         Column("top_category", "Top category"),
      ],
      fmt,
   )


@click.command()
@click.argument("username")
@process_options("cpu")
def user(config_path, fmt, node, include_stale, category, sort, reverse, limit, username) -> None:
   """Every process of USERNAME, whatever its category."""
   config = _load(config_path)
   with _reader(config) as conn:
      max_age = _max_age(conn, config.system, include_stale, node)
      rows = queries.processes(
         conn, config.system, user=username, node=node, category=category, user_only=False,
         sort=sort, reverse=reverse, limit=None, max_age_sec=max_age,
      )  # fmt: skip
   if not rows:
      raise click.ClickException(f"no live processes for {username}")
   if fmt == "table":
      total = sum(r["rss_kb"] or 0 for r in rows)
      n_nodes = len({r["node"] for r in rows})
      click.echo(f"{username}: {len(rows)} processes on {n_nodes} nodes, total RSS {fmt_kb(total)}")
   shown = rows[: _limit(limit)]
   render(_display_rows(shown), _cols(fmt, _TOP_TABLE, _TOP_RAW), fmt)


@click.command()
@process_options("rss")
@click.option(
   "--min-idle",
   type=IdleDuration(),
   default="1h",
   show_default=True,
   help="Idle at least this long: 30m, 2h, 1d, 90s or seconds.",
)
@click.option("--any-presence", is_flag=True, help="Include processes whose owner is present.")
def idle(
   config_path, fmt, node, include_stale, category, sort, reverse, limit, min_idle, any_presence
) -> None:
   """User processes idle for a while, by default only those whose owner is absent."""
   config = _load(config_path)
   with _reader(config) as conn:
      max_age = _max_age(conn, config.system, include_stale, node)
      rows = queries.processes(
         conn, config.system, node=node, category=category, idle_min_sec=min_idle,
         presence=None if any_presence else "absent",
         sort=sort, reverse=reverse, limit=_limit(limit), max_age_sec=max_age,
      )  # fmt: skip
   render(_display_rows(rows), _cols(fmt, _IDLE_TABLE, _IDLE_RAW), fmt)
