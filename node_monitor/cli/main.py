"""node-monitor command line."""

import asyncio
import json
import logging
import os
import signal
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from importlib import resources
from pathlib import Path

import click
import psycopg

from node_monitor.collector.daemon import Daemon
from node_monitor.collector.instance import AlreadyRunning, InstanceLock, is_alive, read_info
from node_monitor.collector.soak import make_private_dir, run_soak, summarize
from node_monitor.config import ConfigError, load_config
from node_monitor.database.bootstrap import SCHEMA, bootstrap
from node_monitor.database.buffered import BufferedWriter
from node_monitor.database.connect import DatabaseUnavailable, connect, is_unavailable
from node_monitor.database.maintenance import ensure_partitions
from node_monitor.database.migrate import (
   MIGRATIONS,
   REQUIRED_VERSION,
   SchemaOutOfDate,
   check_schema,
   current_version,
   migrate,
)
from node_monitor.database.queries import heartbeat
from node_monitor.database.spool import Spool
from node_monitor.database.writer import DatabaseWriter
from node_monitor.pipeline.replay import replay

_HEARTBEAT_FRESH_SEC = 180
_START_GRACE_SEC = 2

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


_BATCH_RECORDS = 500
_MAX_EVENT_ROUNDS = 3


class _DatabaseSink:
   """Feeds replayed records to a BufferedWriter in batches."""

   def __init__(self, config, now):
      self.now = now
      self.writer = DatabaseWriter(
         config.database.url,
         retention=config.retention,
         connect_timeout_sec=config.database.connect_timeout_sec,
      )
      self.buffered = BufferedWriter(
         self.writer,
         Spool(config.working_dir / "state" / "spool-replay", max_bytes=config.spool.max_bytes),
         system=config.system,
      )
      self.pending: list = []
      self.events: list = []
      self.beyond: Counter = Counter()

   def add(self, record) -> None:
      self.pending.append(record)
      if len(self.pending) >= _BATCH_RECORDS:
         self.submit()

   def submit(self) -> None:
      # The events of one submit ride along with the next, as BufferedWriter expects.
      # They are tallied here, once handed back, so only submitted events are counted.
      self._tally(self.events)
      batch, self.pending = self.events + self.pending, []
      self.events = self.buffered.submit(batch, self.now)

   def finish(self) -> None:
      self.submit()
      # Each submit may itself report events (a retention drop, an outage), which need
      # one more submit to be written; a few rounds is plenty, so a database that keeps
      # failing cannot loop forever.
      for _ in range(_MAX_EVENT_ROUNDS):
         if not self.events:
            break
         self.submit()

   def close(self) -> None:
      self.writer.close()

   def _tally(self, events) -> None:
      for event in events:
         if event.row.get("event_type") == "rows_beyond_retention":
            self.beyond.update(event.row.get("detail") or {})


def _check_database(config, now) -> None:
   def run():
      with connect(
         config.database.url,
         connect_timeout_sec=config.database.connect_timeout_sec,
         autocommit=True,
      ) as conn:
         check_schema(conn)
         ensure_partitions(conn, now)

   try:
      _run_database(run)
   except SchemaOutOfDate as exc:
      # The exception text already says to run `database migrate`.
      raise click.ClickException(str(exc)) from exc


@soak.command("replay")
@click.argument("directory", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option("--config", "config_path", type=click.Path(), default=None)
@click.option("--write-records", type=click.Path(path_type=Path), default=None)
@click.option("--write-db", is_flag=True, help="Also write the records to PostgreSQL.")
def soak_replay(directory, config_path, write_records, write_db) -> None:
   """Replay a soak capture through the pipeline and count the records."""
   try:
      config = load_config(config_path)
   except ConfigError as exc:
      raise click.ClickException(str(exc)) from exc
   db = None
   if write_db:
      if not config.database.url:
         raise click.ClickException("database.url is not configured")
      # Real time, not the capture's: retention is measured back from today.
      db = _DatabaseSink(config, datetime.now(timezone.utc))
      try:
         _check_database(config, db.now)
      finally:
         db.close()
   sink = None
   if write_records is not None:
      # Rows hold redacted but still sensitive command lines.
      fd = os.open(write_records, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
      os.fchmod(fd, 0o600)
      sink = os.fdopen(fd, "w", encoding="utf-8")
   try:

      def write(record):
         if sink:
            line = json.dumps({"kind": record.kind, "row": record.row}, default=_json_default)
            sink.write(line + "\n")
         if db:
            _run_database(db.add, record)

      def skip(label):
         click.echo(f"{label}: not in config, skipped")

      try:
         result = replay(directory, config, on_record=write if (sink or db) else None, on_skip=skip)
      except (OSError, ValueError, KeyError) as exc:
         raise click.ClickException(f"cannot replay {directory}: {exc}") from exc
      if db:
         _run_database(db.finish)
   finally:
      if sink:
         sink.close()
      if db:
         db.close()
   for label, counts in result.items():
      detail = " ".join(f"{k}={v}" for k, v in sorted(counts.items()))
      click.echo(f"{label}: {detail}")
   if db:
      beyond = " ".join(f"{k}={v}" for k, v in sorted(db.beyond.items())) or "none"
      click.echo(f"rows_beyond_retention: {beyond}")
      status = db.buffered.status()
      click.echo(f"status: {status}")
      if status["spooled_batches"]:
         click.echo(
            f"spool still holds {status['spooled_batches']} batches in "
            f"{config.working_dir / 'state' / 'spool-replay'}: the database became unavailable "
            "and not everything was written",
            err=True,
         )
         raise SystemExit(1)


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
@click.option("--admin-url", required=True, help="Superuser connection URL.")
@click.option("--database", "db_name", default="node_monitor", show_default=True)
@click.option("--owner", default="node_monitor_owner", show_default=True)
@click.option("--reader", default="node_monitor_reader", show_default=True)
def database_bootstrap(admin_url, db_name, owner, reader) -> None:
   actions = _run_database(bootstrap, admin_url, database=db_name, owner=owner, reader=reader)
   for action in actions:
      click.echo(action)
   if not actions:
      click.echo("nothing to do")


def _check_schema_owner(conn) -> None:
   """Refuse unless the session's role owns the schema.

   Tables belong to the role that creates them, and the reader's default privileges cover
   only those the owner creates; migrated by anyone else, even a superuser, they would be
   unreadable to the reader.
   """
   row = conn.execute(
      "SELECT pg_get_userbyid(nspowner), current_user FROM pg_namespace WHERE nspname = %s",
      (SCHEMA,),
   ).fetchone()
   if row is None:
      raise click.ClickException(
         f"schema {SCHEMA} does not exist; run `node-monitor database bootstrap` first"
      )
   owner, user = row
   if owner != user:
      raise click.ClickException(
         f"database migrate must run as the owner of schema {SCHEMA} ({owner}), not {user}:"
         " use the owner-role URL in database.url"
      )


@database.command("migrate")
@click.option("--config", "config_path", type=click.Path(), default=None)
def database_migrate(config_path) -> None:
   """Apply pending migrations. Use the owner-role URL so new tables get reader access."""
   url, timeout = _database_url(config_path)

   def run():
      with connect(url, connect_timeout_sec=timeout, autocommit=True) as conn:
         _check_schema_owner(conn)
         return migrate(conn)

   applied = _run_database(run)
   if applied:
      click.echo("applied: " + ", ".join(f"{v:04d}" for v in applied))
   else:
      click.echo("up to date")


_PARTITIONS = """
SELECT p.relname, c.relname, pg_get_expr(c.relpartbound, c.oid)
FROM pg_class p
JOIN pg_namespace n ON n.oid = p.relnamespace
LEFT JOIN pg_inherits i ON i.inhparent = p.oid
LEFT JOIN pg_class c ON c.oid = i.inhrelid
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
      if child is None:
         # Said outright so a missing partition is not mistaken for broken output.
         click.echo(f"partition {parent}: none")
      else:
         click.echo(f"partition {parent}.{child}: {bound}")
   if pending:
      raise SystemExit(1)


@database.command("wait")
@click.option("--timeout", type=float, default=60, show_default=True, help="Seconds to wait.")
@click.option("--config", "config_path", type=click.Path(), default=None)
def database_wait(timeout, config_path) -> None:
   """Wait until database.url accepts connections; exit 1 at the timeout."""
   url, connect_timeout = _database_url(config_path)
   deadline = time.monotonic() + timeout
   while True:
      try:
         with connect(url, connect_timeout_sec=connect_timeout, autocommit=True):
            return
      except (psycopg.Error, DatabaseUnavailable) as exc:
         reason = str(exc).strip().splitlines()[0] if str(exc).strip() else type(exc).__name__
      remaining = deadline - time.monotonic()
      if remaining <= 0:
         raise click.ClickException(f"database not reachable after {timeout:g}s: {reason}")
      time.sleep(min(2, remaining))


@cli.command("config")
@click.option("--create", is_flag=True, help="Write a commented template config file.")
@click.argument("path", type=click.Path(path_type=Path), required=False)
def config_command(create, path) -> None:
   """Manage the configuration file."""
   if not create:
      raise click.UsageError("nothing to do; use --create [PATH]")
   target = (path or Path("~/.node_monitor.yaml")).expanduser()
   template = resources.files("node_monitor").joinpath("config_template.yaml").read_bytes()
   try:
      # O_EXCL so an existing config is never clobbered, even by a racing run.
      fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
   except FileExistsError:
      raise click.ClickException(f"{target} already exists; not overwriting") from None
   except OSError as exc:
      raise click.ClickException(f"cannot write {target}: {exc}") from exc
   with os.fdopen(fd, "wb") as f:
      f.write(template)
   click.echo(f"wrote {target}; edit it, then run `node-monitor daemon start`")


@cli.group()
def daemon() -> None:
   """Run and inspect the collector daemon."""


def _load(config_path):
   try:
      return load_config(config_path)
   except ConfigError as exc:
      raise click.ClickException(str(exc)) from exc


def _already_running(info: dict) -> click.ClickException:
   return click.ClickException(
      f"node-monitor daemon already running (pid {info.get('pid')} on {info.get('host')})"
   )


def _age(seconds: float) -> str:
   seconds = max(0, int(seconds))
   if seconds < 120:
      return f"{seconds}s"
   if seconds < 7200:
      return f"{seconds // 60}m"
   if seconds < 172800:
      return f"{seconds // 3600}h"
   return f"{seconds // 86400}d"


def _log_tail(path: Path, lines: int = 20) -> str:
   try:
      return "\n".join(path.read_text(errors="replace").splitlines()[-lines:])
   except OSError:
      return ""


class _UtcFormatter(logging.Formatter):
   converter = time.gmtime


@daemon.command("start")
@click.option("--foreground", is_flag=True, help="Run in this process (for systemd).")
@click.option("--config", "config_path", type=click.Path(), default=None)
def daemon_start(foreground, config_path) -> None:
   """Start the collector daemon."""
   config = _load(config_path)
   if foreground:
      handler = logging.StreamHandler(sys.stderr)
      handler.setFormatter(_UtcFormatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
      logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)
      lock = InstanceLock(config.working_dir)
      try:
         lock.acquire()
      except AlreadyRunning as exc:
         raise _already_running(exc.info) from exc
      try:
         code = asyncio.run(Daemon(config).run())
      except ConfigError as exc:
         raise click.ClickException(str(exc)) from exc
      finally:
         lock.release()
      sys.exit(code)
   info = read_info(config.working_dir)
   if info and is_alive(info):
      raise _already_running(info)
   log_dir = make_private_dir(config.working_dir / "logs")
   log_path = log_dir / "daemon.log"
   argv = [sys.executable, "-m", "node_monitor.cli.main", "daemon", "start", "--foreground"]
   if config_path:
      argv += ["--config", str(Path(config_path).expanduser().resolve())]
   with open(log_path, "ab") as log:
      child = subprocess.Popen(
         argv,
         stdin=subprocess.DEVNULL,
         stdout=log,
         stderr=log,
         start_new_session=True,
      )
   time.sleep(_START_GRACE_SEC)
   if child.poll() is not None:
      tail = _log_tail(log_path)
      raise click.ClickException(
         f"daemon exited with code {child.returncode}; last log lines:\n{tail}"
      )
   click.echo(f"daemon started (pid {child.pid}); log: {log_path}")


@daemon.command("stop")
@click.option("--timeout", type=float, default=60, show_default=True, help="Seconds to wait.")
@click.option("--force", is_flag=True, help="Stop even if systemd manages the daemon.")
@click.option("--config", "config_path", type=click.Path(), default=None)
def daemon_stop(timeout, force, config_path) -> None:
   """Send SIGTERM to the running daemon and wait for it to exit."""
   config = _load(config_path)
   info = read_info(config.working_dir)
   if not info:
      click.echo("daemon not running")
      return
   if info.get("host") != os.uname().nodename:
      raise click.ClickException(
         f"daemon is running on {info.get('host')} (pid {info.get('pid')}); stop it there"
      )
   if not is_alive(info):
      click.echo("daemon not running")
      return
   if info.get("systemd") and not force:
      click.echo("managed by systemd: systemctl --user stop node-monitor-daemon")
      raise SystemExit(1)
   pid = info["pid"]
   try:
      os.kill(pid, signal.SIGTERM)
   except ProcessLookupError:
      click.echo("daemon not running")
      return
   except PermissionError as exc:
      raise click.ClickException(f"cannot signal pid {pid}: {exc}") from exc
   deadline = time.monotonic() + timeout
   while is_alive(info):
      if time.monotonic() >= deadline:
         raise click.ClickException(f"daemon (pid {pid}) still running after {timeout:g}s")
      time.sleep(0.2)
   click.echo(f"daemon stopped (pid {pid})")


def _print_heartbeat(row: dict, now: datetime) -> float:
   age = (now - row["last_seen"]).total_seconds()
   status = row.get("status") or {}
   click.echo(f"heartbeat: last seen {_age(age)} ago on {row['collector_host']}")
   click.echo(f"  version: {row['version']}  state: {status.get('state', '-')}")
   click.echo(f"  connected: {status.get('connected', '-')}")
   click.echo(f"  spooled batches: {status.get('spooled_batches', '-')}")
   if status.get("outage_since"):
      click.echo(f"  database outage since: {status['outage_since']}")
   for label, state in sorted((status.get("sessions") or {}).items()):
      click.echo(f"  {label}: {state}")
   return age


@daemon.command("status")
@click.option("--config", "config_path", type=click.Path(), default=None)
def daemon_status(config_path) -> None:
   """Show the local daemon process and the heartbeat; exit 1 if neither is healthy."""
   config = _load(config_path)
   healthy = False
   info = read_info(config.working_dir)
   if info and is_alive(info):
      healthy = True
      started = datetime.fromisoformat(info["started_at"])
      up = (datetime.now(timezone.utc) - started).total_seconds()
      click.echo(
         f"local process: running (pid {info['pid']} on {info['host']}, up {_age(up)}, "
         f"systemd: {'yes' if info.get('systemd') else 'no'})"
      )
   else:
      click.echo("local process: not running")
   url = config.web.database_url
   if not url:
      click.echo("heartbeat: unavailable (web.database_url is not configured)")
   else:

      def fetch():
         with connect(
            url, connect_timeout_sec=config.database.connect_timeout_sec, autocommit=True
         ) as conn:
            return heartbeat(conn, config.system)

      try:
         row = fetch()
      except (psycopg.Error, DatabaseUnavailable) as exc:
         reason = (str(exc).strip().splitlines() or [type(exc).__name__])[0]
         click.echo(f"heartbeat: database unreachable ({reason})")
      else:
         if row is None:
            click.echo(f"heartbeat: none recorded for system {config.system}")
         elif _print_heartbeat(row, datetime.now(timezone.utc)) < _HEARTBEAT_FRESH_SEC:
            healthy = True
   if not healthy:
      raise SystemExit(1)


if __name__ == "__main__":
   cli()
