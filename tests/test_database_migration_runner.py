"""Unit contract tests for the operator-only migration runner."""

import hashlib

import pytest

from node_monitor.database.migration import (
   MIGRATION_LOCK_KEY,
   Migration,
   MigrationApplyError,
   MigrationDriftError,
   MigrationLockError,
   MigrationRunner,
)


def _migration(version=1, name="one", sql=b"-- one\n"):
   return Migration(
      version=version,
      name=name,
      sql=sql,
      checksum=hashlib.sha256(sql).hexdigest(),
      mode="transactional",
   )


class _Result:
   def __init__(self, scalar=None, mappings=()):
      self._scalar = scalar
      self._mappings = tuple(mappings)

   def scalar_one(self):
      return self._scalar

   def mappings(self):
      return self

   def all(self):
      return list(self._mappings)


class _Transaction:
   def __init__(self, connection):
      self.connection = connection

   def __enter__(self):
      self.connection.events.append(("begin",))
      return self

   def __exit__(self, exc_type, exc, traceback):
      self.connection.events.append(("rollback" if exc_type else "commit",))
      return False


class _Connection:
   def __init__(self, lock=True, rows=(), fail_sql=None):
      self.lock = lock
      self.rows = tuple(rows)
      self.fail_sql = fail_sql
      self.events = []
      self.closed = False

   def exec_driver_sql(self, sql, parameters=None):
      self.events.append(("sql", sql, parameters))
      if self.fail_sql and self.fail_sql in sql:
         raise RuntimeError("postgresql://secret-user:secret-pass@example.invalid/db")
      if "pg_try_advisory_lock" in sql:
         return _Result(scalar=self.lock)
      if "pg_advisory_unlock" in sql:
         return _Result(scalar=True)
      if "SELECT version, name, checksum" in sql:
         return _Result(mappings=self.rows)
      if "to_regclass" in sql:
         return _Result(scalar=False)
      return _Result()

   def begin(self):
      return _Transaction(self)

   def commit(self):
      self.events.append(("session_commit",))

   def rollback(self):
      self.events.append(("session_rollback",))

   def close(self):
      self.closed = True
      self.events.append(("close",))

   def __enter__(self):
      return self

   def __exit__(self, exc_type, exc, traceback):
      self.close()


class _Engine:
   def __init__(self, connection):
      self.connection = connection
      self.connect_count = 0

   def connect(self):
      self.connect_count += 1
      return self.connection


def test_runner_uses_injected_engine_and_fixed_signed_64_bit_lock(monkeypatch):
   monkeypatch.setenv("NODE_MONITOR_DB_URL", "postgresql://must:not@be-read.invalid/db")
   connection = _Connection()
   engine = _Engine(connection)
   runner = MigrationRunner(engine, "test-version", migrations=(_migration(),))

   runner.migrate()

   assert engine.connect_count == 1
   assert -(2 ** 63) <= MIGRATION_LOCK_KEY < 2 ** 63
   lock_calls = [event for event in connection.events
                 if event[0] == "sql" and "pg_try_advisory_lock" in event[1]]
   assert lock_calls == [("sql", "SELECT pg_try_advisory_lock(%s)",
                          (MIGRATION_LOCK_KEY,))]


def test_lock_refusal_aborts_before_bootstrap_and_still_closes_connection():
   connection = _Connection(lock=False)
   runner = MigrationRunner(_Engine(connection), "test", migrations=(_migration(),))

   with pytest.raises(MigrationLockError):
      runner.migrate()

   sql = [event[1] for event in connection.events if event[0] == "sql"]
   assert not any("CREATE SCHEMA" in statement for statement in sql)
   assert not any("schema_migrations (" in statement for statement in sql)
   assert connection.closed


def test_applied_ledger_is_validated_before_pending_sql():
   first = _migration(1, "one", b"-- one\n")
   second = _migration(2, "two", b"CREATE TABLE must_not_run (x int);\n")
   row = {
      "version": 1,
      "name": "changed",
      "checksum": first.checksum,
      "transactional": True,
   }
   connection = _Connection(rows=(row,))
   runner = MigrationRunner(_Engine(connection), "test", migrations=(first, second))

   with pytest.raises(MigrationDriftError):
      runner.migrate()

   sql = [event[1] for event in connection.events if event[0] == "sql"]
   assert second.sql.decode() not in sql
   assert any("pg_advisory_unlock" in statement for statement in sql)


@pytest.mark.parametrize("rows", [
   ({"version": 2, "name": "two", "checksum": "0" * 64,
     "transactional": True},),
   ({"version": 1, "name": "one", "checksum": "0" * 64,
     "transactional": True},),
   ({"version": 1, "name": "one", "checksum": hashlib.sha256(b"-- one\n").hexdigest(),
     "transactional": False},),
])
def test_unknown_gapped_changed_or_mode_drift_fails_closed(rows):
   runner = MigrationRunner(
      _Engine(_Connection(rows=rows)), "test", migrations=(_migration(),))
   with pytest.raises(MigrationDriftError):
      runner.migrate()


def test_migration_and_ledger_insert_share_transaction_and_unlock_on_success():
   connection = _Connection()
   migration = _migration(sql=b"CREATE TABLE example (x int);\n")
   checked = []
   runner = MigrationRunner(
      _Engine(connection), "test", migrations=(migration,),
      postcondition=lambda conn, item: checked.append((conn, item)),
   )

   result = runner.migrate()

   assert result.applied_versions == (1,)
   assert checked == [(connection, migration)]
   events = connection.events
   migration_index = events.index(("sql", migration.sql.decode(), None))
   ledger_index = next(i for i, event in enumerate(events)
                       if event[0] == "sql" and "INSERT INTO" in event[1])
   begin_indexes = [i for i, event in enumerate(events) if event == ("begin",)]
   commit_indexes = [i for i, event in enumerate(events) if event == ("commit",)]
   assert any(begin < migration_index < ledger_index < commit
              for begin in begin_indexes for commit in commit_indexes)
   assert any(event[0] == "sql" and "pg_advisory_unlock" in event[1]
              for event in events)


def test_apply_failure_is_sanitized_rolls_back_and_unlocks():
   migration = _migration(sql=b"CREATE TABLE fail_me (x int);\n")
   connection = _Connection(fail_sql="CREATE TABLE fail_me")
   runner = MigrationRunner(_Engine(connection), "test", migrations=(migration,))

   with pytest.raises(MigrationApplyError) as caught:
      runner.migrate()

   rendered = str(caught.value)
   assert "secret-user" not in rendered
   assert "secret-pass" not in rendered
   assert ("rollback",) in connection.events
   assert any(event[0] == "sql" and "pg_advisory_unlock" in event[1]
              for event in connection.events)


def test_invalid_utf8_fails_before_execution_and_unlocks():
   sql = b"\xff\xfe"
   migration = _migration(sql=sql)
   connection = _Connection()
   runner = MigrationRunner(_Engine(connection), "test", migrations=(migration,))

   with pytest.raises(MigrationApplyError, match="not valid UTF-8"):
      runner.migrate()

   assert all(event[1] != sql for event in connection.events
              if event[0] == "sql")
   assert any(event[0] == "sql" and "pg_advisory_unlock" in event[1]
              for event in connection.events)
