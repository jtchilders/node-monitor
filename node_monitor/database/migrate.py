"""Schema migrations: numbered plain-SQL files, each applied once, in order, in its own
transaction, with the version recorded in `node_monitor.schema_migrations`."""

import re
from importlib import resources

import psycopg
from psycopg import pq

_PACKAGE = "node_monitor.database.migrations"
_FILENAME = re.compile(r"^(\d{4})_\w+\.sql$")

# A session-level lock, so it spans the per-migration transactions; two `migrate` runs
# (say, a deploy racing a manual run) then apply each migration exactly once.
_LOCK = "SELECT pg_advisory_lock(hashtext('node_monitor.migrate'))"
_UNLOCK = "SELECT pg_advisory_unlock(hashtext('node_monitor.migrate'))"

_CREATE_VERSION_TABLE = """
CREATE TABLE IF NOT EXISTS node_monitor.schema_migrations (
   version integer PRIMARY KEY,
   name text NOT NULL,
   applied_at timestamptz NOT NULL DEFAULT now()
)
"""


class SchemaOutOfDate(Exception):
   pass


def _discover() -> list[tuple[int, str]]:
   found: dict[int, str] = {}
   for entry in resources.files(_PACKAGE).iterdir():
      m = _FILENAME.match(entry.name)
      if not m:
         continue
      version = int(m.group(1))
      # Version 0 is what an unmigrated schema reports, so no migration may claim it.
      if version == 0:
         raise RuntimeError(f"migration {entry.name}: versions start at 0001")
      if version in found:
         raise RuntimeError(f"migrations {found[version]} and {entry.name} share a version")
      found[version] = entry.name
   return sorted(found.items())


MIGRATIONS: list[tuple[int, str]] = _discover()
REQUIRED_VERSION: int = MIGRATIONS[-1][0] if MIGRATIONS else 0


def _read_sql(filename: str) -> str:
   return resources.files(_PACKAGE).joinpath(filename).read_text(encoding="utf-8")


def current_version(conn: psycopg.Connection) -> int:
   exists = conn.execute(
      "SELECT to_regclass('node_monitor.schema_migrations') IS NOT NULL"
   ).fetchone()[0]
   if not exists:
      return 0
   return conn.execute(
      "SELECT coalesce(max(version), 0) FROM node_monitor.schema_migrations"
   ).fetchone()[0]


def check_schema(conn: psycopg.Connection) -> None:
   version = current_version(conn)
   if version < REQUIRED_VERSION:
      raise SchemaOutOfDate(
         f"schema version {version}, need {REQUIRED_VERSION}: run `node-monitor database migrate`"
      )


def migrate(conn: psycopg.Connection) -> list[int]:
   """Apply pending migrations; return the versions applied (empty when up to date).

   `conn` must have no open transaction. A failed migration is rolled back whole and
   raised; the ones before it stay applied.
   """
   if conn.info.transaction_status != pq.TransactionStatus.IDLE:
      raise psycopg.ProgrammingError("migrate needs a connection with no open transaction")
   saved_autocommit = conn.autocommit
   # In autocommit mode each `conn.transaction()` block is a real BEGIN/COMMIT rather than
   # a savepoint inside one long transaction.
   conn.autocommit = True
   try:
      conn.execute(_LOCK)
      try:
         return _apply_pending(conn)
      finally:
         # A lost connection has already released its session locks.
         if not conn.closed:
            conn.execute(_UNLOCK)
   finally:
      if not conn.closed:
         conn.autocommit = saved_autocommit


def _apply_pending(conn: psycopg.Connection) -> list[int]:
   conn.execute(_CREATE_VERSION_TABLE)
   done = {r[0] for r in conn.execute("SELECT version FROM node_monitor.schema_migrations")}
   applied = []
   for version, filename in MIGRATIONS:
      if version in done:
         continue
      with conn.transaction():
         conn.execute(_read_sql(filename))
         conn.execute(
            "INSERT INTO node_monitor.schema_migrations (version, name) VALUES (%s, %s)",
            (version, filename),
         )
      applied.append(version)
   return applied
