"""node_monitor.database.migration -- transactional migration runner for
the node_monitor schema.

The canonical immutable ``Migration`` value type, fail-closed resource
discovery (``discover_migrations``), contiguity validation
(``_validate_contiguous``), and schema catalog constants
(``_SOURCE_SCHEMA_COLUMNS``, ``_REQUIRED_SOURCE_CONSTRAINTS``,
``_REQUIRED_SOURCE_INDEXES``) all live in
``node_monitor.database.schema_contract``.  This module imports and
re-exports them so that existing public imports (``from
node_monitor.database.migration import Migration, discover_migrations``)
remain compatible, while the single implementation lives only in
``schema_contract.py``.

Dependency direction::

    schema_contract.py   (canonical types/discovery/catalog)
         ▲
    migration.py         (imports + re-exports + runner)

Design: node_monitor_planning increment2-schema-migrations-writer.md
Task 1 ("Migration resource model and fail-closed discovery") is now
implemented in schema_contract.py.  Task 2 (this module) builds the
transactional runner on top of the shared contract.  Task 3 replaces
the placeholder SQL body of migration 0001 with reviewed source-table
DDL.
"""

import re
import time

from sqlalchemy.exc import SQLAlchemyError

# ---------------------------------------------------------------------------
# Re-export canonical types and discovery from schema_contract so that
# existing imports of the form:
#   from node_monitor.database.migration import Migration, discover_migrations
# remain compatible.
# ---------------------------------------------------------------------------
from node_monitor.database.schema_contract import (  # noqa: F401
   Migration,
   _FILENAME_PATTERN,
   _MIGRATIONS_PACKAGE,
   _SOURCE_SCHEMA_COLUMNS,
   _REQUIRED_SOURCE_CONSTRAINTS,
   _REQUIRED_SOURCE_INDEXES,
   _SUPPORTED_MODES,
   _build_migration,
   _looks_like_intended_migration,
   _migrations_from_directory,
   _migrations_from_entries,
   _parse_filename,
   _read_mode_marker,
   _validate_contiguous,
   compare_source_schema,
   discover_migrations,
   expected_migration_rows,
)

# Stable signed int64 derived once from the migration-lock namespace. Changing
# this value would split coordination between old and new operators.
MIGRATION_LOCK_KEY = -2976567912318189130

_BOOTSTRAP_SQL = """
CREATE SCHEMA IF NOT EXISTS node_monitor;
CREATE TABLE IF NOT EXISTS node_monitor.schema_migrations (
   version INTEGER PRIMARY KEY CHECK (version > 0),
   name TEXT NOT NULL,
   checksum CHAR(64) NOT NULL CHECK (checksum ~ '^[0-9a-f]{64}$'),
   transactional BOOLEAN NOT NULL,
   applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
   application_version TEXT NOT NULL,
   execution_ms BIGINT NOT NULL CHECK (execution_ms >= 0)
)
"""

_LEDGER_SQL = """
SELECT version, name, checksum, transactional
FROM node_monitor.schema_migrations
ORDER BY version
"""

_INSERT_SQL = """
INSERT INTO node_monitor.schema_migrations
   (version, name, checksum, transactional, application_version, execution_ms)
VALUES (%s, %s, %s, %s, %s, %s)
"""


def _has_executable_sql(sql):
   """Return False only when SQL consists solely of whitespace/comments."""
   without_block_comments = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
   without_line_comments = re.sub(
      r"--[^\r\n]*(?:\r?\n|$)", "", without_block_comments)
   return bool(without_line_comments.strip())


def _validate_initial_source_schema(connection):
   rows = connection.exec_driver_sql("""
      SELECT table_name, column_name, udt_name, is_nullable
      FROM information_schema.columns
      WHERE table_schema = 'node_monitor'
        AND table_name <> 'schema_migrations'
      ORDER BY table_name, ordinal_position
   """).all()
   actual = {}
   for table, column, udt_name, nullable in rows:
      actual.setdefault(table, []).append((column, udt_name, nullable))
   drift = compare_source_schema(actual)
   if drift is not None:
      raise MigrationApplyError(
         "migration 1 source schema columns do not match the required contract")

   constraints = frozenset(connection.exec_driver_sql("""
      SELECT con.conname
      FROM pg_constraint con
      JOIN pg_class c ON c.oid = con.conrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'node_monitor'
        AND c.relname <> 'schema_migrations'
   """).scalars().all())
   if not _REQUIRED_SOURCE_CONSTRAINTS.issubset(constraints):
      raise MigrationApplyError(
         "migration 1 source schema constraints are incomplete")

   indexes = frozenset(connection.exec_driver_sql("""
      SELECT indexname FROM pg_indexes
      WHERE schemaname = 'node_monitor'
        AND tablename <> 'schema_migrations'
   """).scalars().all())
   if not _REQUIRED_SOURCE_INDEXES.issubset(indexes):
      raise MigrationApplyError(
         "migration 1 source schema indexes are incomplete")


class MigrationError(RuntimeError):
   """Base class for bounded, credential-safe migration failures."""


class MigrationLockError(MigrationError):
   """Another operator owns the migration advisory lock."""


class MigrationDriftError(MigrationError):
   """The ledger does not match the packaged migration history."""


class MigrationApplyError(MigrationError):
   """A pending migration could not be applied atomically."""


import dataclasses


@dataclasses.dataclass(frozen=True)
class MigrationStatus:
   initialized: bool
   current_version: int
   latest_version: int
   pending_versions: tuple
   drift: bool = False


@dataclasses.dataclass(frozen=True)
class MigrationResult:
   applied_versions: tuple
   current_version: int
   latest_version: int


class MigrationRunner:
   """Run validated migrations through one explicitly injected engine."""

   def __init__(self, engine, application_version, migrations=None,
                postcondition=None):
      if not application_version or not isinstance(application_version, str):
         raise ValueError("application_version must be a non-empty string")
      self._engine = engine
      self._application_version = application_version
      self._migrations = tuple(
         discover_migrations() if migrations is None else migrations)
      if not self._migrations:
         raise ValueError("at least one migration is required")
      _validate_contiguous(self._migrations)
      self._postcondition = postcondition or (lambda connection, migration: None)

   def status(self):
      try:
         with self._engine.connect() as connection:
            initialized = connection.exec_driver_sql(
               "SELECT to_regclass('node_monitor.schema_migrations') "
               "IS NOT NULL").scalar_one()
            connection.commit()
            if not initialized:
               return MigrationStatus(
                  initialized=False,
                  current_version=0,
                  latest_version=self._migrations[-1].version,
                  pending_versions=tuple(m.version for m in self._migrations),
               )
            rows = self._read_ledger(connection)
            connection.commit()
            self._validate_ledger(rows)
            current = rows[-1]["version"] if rows else 0
            return MigrationStatus(
               initialized=True,
               current_version=current,
               latest_version=self._migrations[-1].version,
               pending_versions=tuple(
                  m.version for m in self._migrations if m.version > current),
            )
      except MigrationError:
         raise
      except Exception:
         raise MigrationError("could not inspect migration status") from None

   def migrate(self):
      applied = []
      connection = None
      lock_acquired = False
      try:
         connection = self._engine.connect()
         connection.exec_driver_sql("SET lock_timeout = '30s'")
         connection.commit()
         lock_acquired = bool(connection.exec_driver_sql(
            "SELECT pg_try_advisory_lock(%s)",
            (MIGRATION_LOCK_KEY,)).scalar_one())
         connection.commit()
         if not lock_acquired:
            raise MigrationLockError(
               "database migration lock is held by another operator")

         with connection.begin():
            connection.exec_driver_sql(_BOOTSTRAP_SQL)

         rows = self._read_ledger(connection)
         connection.commit()
         self._validate_ledger(rows)
         current = rows[-1]["version"] if rows else 0

         for migration in self._migrations:
            if migration.version <= current:
               continue
            try:
               sql = migration.sql.decode("utf-8", errors="strict")
            except UnicodeDecodeError:
               raise MigrationApplyError(
                  "migration %d SQL is not valid UTF-8" % migration.version)
            started = time.monotonic_ns()
            try:
               with connection.begin():
                  if _has_executable_sql(sql):
                     connection.exec_driver_sql(sql)
                  if (migration.version == 1 and
                        migration.name == "initial_source_schema"):
                     _validate_initial_source_schema(connection)
                  self._postcondition(connection, migration)
                  elapsed_ms = max(
                     0, (time.monotonic_ns() - started) // 1_000_000)
                  connection.exec_driver_sql(
                     _INSERT_SQL,
                     (migration.version, migration.name, migration.checksum,
                      migration.mode == "transactional",
                      self._application_version, elapsed_ms),
                  )
            except MigrationError:
               raise
            except Exception:
               raise MigrationApplyError(
                  "migration %d failed and was rolled back"
                  % migration.version) from None
            applied.append(migration.version)
            current = migration.version

         return MigrationResult(
            applied_versions=tuple(applied),
            current_version=current,
            latest_version=self._migrations[-1].version,
         )
      except MigrationError:
         raise
      except Exception:
         raise MigrationError("migration operation failed") from None
      finally:
         if connection is not None:
            if lock_acquired:
               try:
                  connection.exec_driver_sql(
                     "SELECT pg_advisory_unlock(%s)",
                     (MIGRATION_LOCK_KEY,))
                  connection.commit()
               except Exception:
                  try:
                     connection.rollback()
                  except Exception:
                     pass
            connection.close()

   def _read_ledger(self, connection):
      result = connection.exec_driver_sql(_LEDGER_SQL)
      return [dict(row) for row in result.mappings().all()]

   def _validate_ledger(self, rows):
      expected_by_version = {m.version: m for m in self._migrations}
      actual_versions = []
      for row in rows:
         try:
            version = int(row["version"])
            name = row["name"]
            checksum = row["checksum"]
            transactional = row["transactional"]
         except (KeyError, TypeError, ValueError):
            raise MigrationDriftError("migration ledger row is invalid")
         actual_versions.append(version)
         expected = expected_by_version.get(version)
         if expected is None:
            raise MigrationDriftError(
               "migration ledger contains unknown version %d" % version)
         if name != expected.name:
            raise MigrationDriftError(
               "migration %d name does not match packaged history" % version)
         if checksum != expected.checksum:
            raise MigrationDriftError(
               "migration %d checksum does not match packaged history" % version)
         if transactional is not True or expected.mode != "transactional":
            raise MigrationDriftError(
               "migration %d mode does not match packaged history" % version)

      if actual_versions != list(range(1, len(actual_versions) + 1)):
         raise MigrationDriftError(
            "migration ledger versions are not contiguous from 1")
