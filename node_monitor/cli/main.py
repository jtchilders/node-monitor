"""node-monitor command line."""

import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import click
import psycopg

from node_monitor.collector.soak import make_private_dir, run_soak, summarize
from node_monitor.config import ConfigError, load_config
from node_monitor.database.bootstrap import bootstrap
from node_monitor.database.connect import connect, is_unavailable
from node_monitor.database.migrate import MIGRATIONS, REQUIRED_VERSION, current_version, migrate
from node_monitor.pipeline.replay import replay

COLUMNS = (
   "node",
   "hours",
   "coverage%",
   "census%",
   "errors",
   "max_gap_s",
   "sessions",
   "losses",
   "rotations",
   "reboots",
   "cpu_s/h",
   "rss_MB",
   "maxrss_MB",
   "scan_p95_s",
   "procs",
)


@click.group()
def cli() -> None:
   """Login-node monitor."""


@cli.group()
def soak() -> None:
   """M0 soak: record probe streams and report on them."""


@soak.command("run")
@click.option("--config", "config_path", type=click.Path(), default=None)
@click.option("--duration-hours", type=float, default=None, help="Run forever if omitted.")
@click.option("--full-census", is_flag=True, help="Record every process in each census.")
@click.option("--out-dir", type=click.Path(path_type=Path), default=None)
def soak_run(config_path, duration_hours, full_census, out_dir) -> None:
   try:
      config = load_config(config_path)
   except ConfigError as exc:
      raise click.ClickException(str(exc)) from exc
   if out_dir is None:
      stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
      # The soak tree is ours alone, so it is made owner-only even if it already exists.
      out_dir = make_private_dir(config.working_dir / "logs" / "soak") / stamp
   make_private_dir(out_dir)
   click.echo(f"recording to {out_dir}")
   duration = None if duration_hours is None else duration_hours * 3600
   sys.exit(asyncio.run(run_soak(config, out_dir, duration, full_census=full_census)))


def _fmt(value, spec: str) -> str:
   return "-" if value is None else format(value, spec)


def _row(label: str, s: dict) -> list[str]:
   losses = sum(s["losses_by_reason"].values())
   return [
      label,
      _fmt(s["hours"], ".2f"),
      _fmt(s["coverage_pct"], ".1f"),
      _fmt(s["census_coverage_pct"], ".1f"),
      str(sum(s["errors_by_where"].values())),
      _fmt(s["max_gap_sec"], ".0f"),
      str(s["sessions_started"]),
      str(losses),
      str(s["rotations"]),
      str(s["reboots"]),
      _fmt(s["probe_cpu_sec_per_hour"], ".1f"),
      # Exact RSS first; ru_maxrss drifts and is only a secondary check.
      _fmt(None if s["probe_rss_kb_max"] is None else s["probe_rss_kb_max"] / 1024, ".1f"),
      _fmt(s["probe_maxrss_kb"] / 1024, ".1f"),
      _fmt(s["census_scan_p95"], ".2f"),
      _fmt(s["process_count_avg"], ".0f"),
   ]


@soak.command("report")
@click.argument("directory", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option("--format", "fmt", type=click.Choice(["table", "json"]), default="table")
def soak_report(directory, fmt) -> None:
   try:
      result = summarize(directory)
   except (OSError, ValueError, KeyError) as exc:
      raise click.ClickException(f"cannot summarize {directory}: {exc}") from exc
   if fmt == "json":
      click.echo(json.dumps(result, indent=2))
      return
   rows = [list(COLUMNS)] + [_row(label, s) for label, s in result.items()]
   widths = [max(len(r[i]) for r in rows) for i in range(len(COLUMNS))]
   for r in rows:
      click.echo(
         "  ".join(
            c.rjust(w) if i else c.ljust(w) for i, (c, w) in enumerate(zip(r, widths, strict=True))
         )
      )


def _json_default(value):
   if isinstance(value, datetime):
      return value.isoformat()
   raise TypeError(f"not JSON serializable: {type(value).__name__}")


@soak.command("replay")
@click.argument("directory", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option("--config", "config_path", type=click.Path(), default=None)
@click.option("--write-records", type=click.Path(path_type=Path), default=None)
def soak_replay(directory, config_path, write_records) -> None:
   """Replay a soak capture through the pipeline and count the records."""
   try:
      config = load_config(config_path)
   except ConfigError as exc:
      raise click.ClickException(str(exc)) from exc
   sink = None
   if write_records is not None:
      # Rows hold redacted but still sensitive command lines.
      fd = os.open(write_records, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
      os.fchmod(fd, 0o600)
      sink = os.fdopen(fd, "w", encoding="utf-8")
   try:

      def write(record):
         line = json.dumps({"kind": record.kind, "row": record.row}, default=_json_default)
         sink.write(line + "\n")

      def skip(label):
         click.echo(f"{label}: not in config, skipped")

      try:
         result = replay(directory, config, on_record=write if sink else None, on_skip=skip)
      except (OSError, ValueError, KeyError) as exc:
         raise click.ClickException(f"cannot replay {directory}: {exc}") from exc
   finally:
      if sink:
         sink.close()
   for label, counts in result.items():
      detail = " ".join(f"{k}={v}" for k, v in sorted(counts.items()))
      click.echo(f"{label}: {detail}")


@cli.group()
def database() -> None:
   """Create, migrate and inspect the PostgreSQL database."""


def _database_url(config_path) -> tuple[str, float]:
   try:
      config = load_config(config_path)
   except ConfigError as exc:
      raise click.ClickException(str(exc)) from exc
   if not config.database.url:
      raise click.ClickException("database.url is not configured")
   return config.database.url, config.database.connect_timeout_sec


def _run_database(action, *args, **kwargs):
   try:
      return action(*args, **kwargs)
   except (psycopg.Error, ValueError) as exc:
      if isinstance(exc, psycopg.Error) and is_unavailable(exc):
         raise click.ClickException(f"database unavailable: {exc}") from exc
      raise click.ClickException(f"database error: {exc}") from exc


@database.command("bootstrap")
@click.option("--admin-url", required=True, help="Superuser or CREATEROLE/CREATEDB role URL.")
@click.option("--database", "db_name", default="node_monitor", show_default=True)
@click.option("--owner", default="node_monitor_owner", show_default=True)
@click.option("--reader", default="node_monitor_reader", show_default=True)
def database_bootstrap(admin_url, db_name, owner, reader) -> None:
   actions = _run_database(bootstrap, admin_url, database=db_name, owner=owner, reader=reader)
   for action in actions:
      click.echo(action)
   if not actions:
      click.echo("nothing to do")


@database.command("migrate")
@click.option("--config", "config_path", type=click.Path(), default=None)
def database_migrate(config_path) -> None:
   """Apply pending migrations. Use the owner-role URL so new tables get reader access."""
   url, timeout = _database_url(config_path)

   def run():
      with connect(url, connect_timeout_sec=timeout, autocommit=True) as conn:
         return migrate(conn)

   applied = _run_database(run)
   if applied:
      click.echo("applied: " + ", ".join(f"{v:04d}" for v in applied))
   else:
      click.echo("up to date")


_PARTITIONS = """
SELECT p.relname AS parent, c.relname AS child, pg_get_expr(c.relpartbound, c.oid)
FROM pg_inherits i
JOIN pg_class c ON c.oid = i.inhrelid
JOIN pg_class p ON p.oid = i.inhparent
JOIN pg_namespace n ON n.oid = p.relnamespace
WHERE n.nspname = 'node_monitor' AND p.relkind = 'p'
ORDER BY p.relname, c.relname
"""


@database.command("status")
@click.option("--config", "config_path", type=click.Path(), default=None)
def database_status(config_path) -> None:
   """Show schema version and partitions; exit 1 if the schema is behind."""
   url, timeout = _database_url(config_path)

   def run():
      with connect(url, connect_timeout_sec=timeout, autocommit=True) as conn:
         return current_version(conn), conn.execute(_PARTITIONS).fetchall()

   version, partitions = _run_database(run)
   click.echo(f"current version: {version}")
   click.echo(f"required version: {REQUIRED_VERSION}")
   pending = [name for v, name in MIGRATIONS if v > version]
   click.echo("pending: " + (", ".join(pending) if pending else "none"))
   for parent, child, bound in partitions:
      click.echo(f"partition {parent}.{child}: {bound}")
   if pending:
      raise SystemExit(1)
