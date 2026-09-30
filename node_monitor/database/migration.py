"""node_monitor.database.migration -- immutable Migration value type and
fail-closed, deterministic discovery of packaged SQL migration
resources.

Design: node_monitor_planning increment2-schema-migrations-writer.md
Task 1 ("Migration resource model and fail-closed discovery"). This
module implements *discovery only*: no migration runner, no SQL
execution, no CLI, no final tables. Task 2 builds the transactional
runner on top of this; Task 3 replaces the placeholder SQL body of
migration 0001 with reviewed source-table DDL.

``discover_migrations()`` reads the packaged
``node_monitor.database.migrations.versions`` resource package via
``importlib.resources`` -- never the filesystem path of the developer's
working tree -- so behavior is identical whether node_monitor is
installed as a wheel, an sdist, or in editable/development mode.
"""

import dataclasses
import hashlib
import importlib.resources as resources
import re
import time

from sqlalchemy.exc import SQLAlchemyError

_SUPPORTED_MODES = frozenset({"transactional"})

# NNNN_name.sql -- exactly 4 digits, underscore, a name using only
# letters/digits/underscores, then ".sql". Deliberately strict: no
# hyphens, no spaces, no missing segments.
_FILENAME_PATTERN = re.compile(r"^(\d{4})_([A-Za-z0-9_]+)\.sql$")

_MIGRATIONS_PACKAGE = "node_monitor.database.migrations.versions"


@dataclasses.dataclass(frozen=True)
class Migration:
   """One immutable, validated migration resource.

   ``sql`` holds the exact packaged bytes -- never decoded, re-encoded,
   or newline-normalized. ``checksum`` is the lowercase hex SHA-256 of
   those exact bytes, and construction fails closed if the checksum
   passed in does not match. ``mode`` names the execution strategy a
   future runner will use; only ``"transactional"`` is supported in
   this increment.
   """

   version: int
   name: str
   sql: bytes
   checksum: str
   mode: str

   def __post_init__(self):
      if not isinstance(self.version, int) or isinstance(self.version, bool):
         raise TypeError("version must be an int, got %r" % (self.version,))
      if self.version <= 0:
         raise ValueError(
            "version must be a positive integer, got %r" % (self.version,))
      if not isinstance(self.name, str) or not self.name:
         raise ValueError("name must be a non-empty string")
      if not isinstance(self.sql, (bytes, bytearray)):
         raise TypeError("sql must be bytes, got %r" % (type(self.sql),))
      if isinstance(self.sql, bytearray):
         object.__setattr__(self, "sql", bytes(self.sql))
      if self.mode not in _SUPPORTED_MODES:
         raise ValueError(
            "unsupported migration mode %r; only %r is supported this "
            "increment" % (self.mode, sorted(_SUPPORTED_MODES)))
      expected_checksum = hashlib.sha256(self.sql).hexdigest()
      if not isinstance(self.checksum, str) or self.checksum != self.checksum.lower():
         raise ValueError(
            "checksum must be a lowercase hex string, got %r"
            % (self.checksum,))
      if self.checksum != expected_checksum:
         raise ValueError(
            "checksum %r does not match SHA-256 of exact SQL bytes "
            "(expected %r)" % (self.checksum, expected_checksum))


def _parse_filename(filename):
   """Return (version, name) for a well-formed ``NNNN_name.sql``
   filename, or None if it does not match the required shape at all
   (used to silently skip unrelated files such as ``__init__.py``).
   """
   match = _FILENAME_PATTERN.match(filename)
   if match is None:
      return None
   version_str, name = match.group(1), match.group(2)
   return int(version_str), name


def _looks_like_intended_migration(filename):
   """True if a filename is clearly *meant* to be a migration (ends in
   .sql and isn't a dunder file) even if it is malformed -- so a typo'd
   filename fails closed with a clear error instead of being silently
   skipped.
   """
   if filename.startswith("__"):
      return False
   return filename.endswith(".sql")


def _build_migration(directory_or_traversable, filename, version, name,
                      read_bytes):
   sql = read_bytes(filename)
   mode = "transactional"
   mode_marker_name = filename + ".mode"
   marker_mode = _read_mode_marker(directory_or_traversable,
                                    mode_marker_name)
   if marker_mode is not None:
      mode = marker_mode
   checksum = hashlib.sha256(sql).hexdigest()
   return Migration(version=version, name=name, sql=sql, checksum=checksum,
                     mode=mode)


def _read_mode_marker(directory_or_traversable, marker_name):
   """Return the stripped text content of a ``<file>.sql.mode`` sidecar
   if present, else None. This function only reads and strips whatever
   text is present -- it does not itself validate or reject the mode
   value; that check happens in ``Migration.__post_init__`` when the
   returned string is used to construct the ``Migration``, so an
   unsupported mode fails closed there rather than being guessed here.
   """
   try:
      marker = directory_or_traversable / marker_name
      if hasattr(marker, "is_file"):
         if not marker.is_file():
            return None
      else:
         from pathlib import Path
         if not (Path(directory_or_traversable) / marker_name).is_file():
            return None
   except (FileNotFoundError, NotADirectoryError):
      return None
   if hasattr(marker, "read_text"):
      return marker.read_text().strip()
   return (directory_or_traversable / marker_name).read_text().strip()


def _validate_contiguous(migrations):
   versions = [m.version for m in migrations]
   seen = set()
   for version in versions:
      if version in seen:
         raise ValueError("duplicate migration version: %d" % version)
      seen.add(version)
   ordered = sorted(seen)
   expected = list(range(1, len(ordered) + 1))
   if ordered != expected:
      raise ValueError(
         "migration versions must be contiguous starting at 1; found %r"
         % (ordered,))


def _migrations_from_entries(entries, read_bytes, container):
   """Shared assembly logic given an iterable of filenames present in
   a migration source (a directory or a packaged resource
   traversable), a ``read_bytes(filename)`` callable, and the
   container itself (used to look up ``.mode`` sidecar files).
   """
   migrations = []
   for filename in entries:
      if not _looks_like_intended_migration(filename):
         continue
      parsed = _parse_filename(filename)
      if parsed is None:
         raise ValueError(
            "malformed migration filename %r; expected NNNN_name.sql"
            % (filename,))
      version, name = parsed
      migrations.append(
         _build_migration(container, filename, version, name, read_bytes))
   if not migrations:
      raise ValueError(
         "no migrations found; every installation must ship at least "
         "migration 1 (expected files matching NNNN_name.sql)")
   migrations.sort(key=lambda m: m.version)
   _validate_contiguous(migrations)
   return migrations


def _migrations_from_directory(directory):
   """Discover migrations from a plain filesystem directory. Used
   directly by tests to exercise filename-parsing and
   version-contiguity edge cases without needing to rebuild the
   packaged resource tree; production discovery goes through
   ``discover_migrations()`` / ``importlib.resources`` instead.
   """
   from pathlib import Path
   directory = Path(directory)
   entries = sorted(
      entry.name for entry in directory.iterdir() if entry.is_file()
   )

   def read_bytes(filename):
      return (directory / filename).read_bytes()

   return _migrations_from_entries(entries, read_bytes, directory)


def discover_migrations():
   """Return the sorted, validated tuple of packaged ``Migration``
   resources from ``node_monitor.database.migrations.versions``, read
   via ``importlib.resources`` so behavior is identical under a wheel
   install, an sdist install, or editable/development mode.

   Raises ``ValueError`` if any filename is malformed, or if versions
   are non-positive, duplicated, or not contiguous from 1.
   """
   package_root = resources.files(_MIGRATIONS_PACKAGE)
   entries = sorted(
      entry.name for entry in package_root.iterdir() if entry.is_file()
   )

   def read_bytes(filename):
      return package_root.joinpath(filename).read_bytes()

   return tuple(
      _migrations_from_entries(entries, read_bytes, package_root))


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


class MigrationError(RuntimeError):
   """Base class for bounded, credential-safe migration failures."""


class MigrationLockError(MigrationError):
   """Another operator owns the migration advisory lock."""


class MigrationDriftError(MigrationError):
   """The ledger does not match the packaged migration history."""


class MigrationApplyError(MigrationError):
   """A pending migration could not be applied atomically."""


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
      except (SQLAlchemyError, RuntimeError, ValueError, TypeError) as exc:
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
      except (SQLAlchemyError, RuntimeError, ValueError, TypeError):
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
