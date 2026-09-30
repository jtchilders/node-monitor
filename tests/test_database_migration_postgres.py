"""Real-PostgreSQL acceptance tests for the migration runner."""

import hashlib
import os
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.pool import NullPool

from node_monitor.database.migration import (
   MIGRATION_LOCK_KEY,
   Migration,
   MigrationApplyError,
   MigrationDriftError,
   MigrationLockError,
   MigrationRunner,
)


pytestmark = pytest.mark.skipif(
   not os.environ.get("NODE_MONITOR_TEST_DATABASE_URL"),
   reason="NODE_MONITOR_TEST_DATABASE_URL is required",
)


def _migration(version=1, name="one", sql=b"SELECT 1;\n"):
   return Migration(
      version=version,
      name=name,
      sql=sql,
      checksum=hashlib.sha256(sql).hexdigest(),
      mode="transactional",
   )


@pytest.fixture
def postgres_engine():
   admin_url = make_url(os.environ["NODE_MONITOR_TEST_DATABASE_URL"])
   database_name = "node_monitor_test_" + uuid.uuid4().hex
   admin_engine = create_engine(
      admin_url, poolclass=NullPool, isolation_level="AUTOCOMMIT")
   with admin_engine.connect() as connection:
      version = int(connection.exec_driver_sql(
         "SHOW server_version_num").scalar_one())
      if version < 120000:
         pytest.skip("PostgreSQL 12 or newer is required")
      can_create = connection.exec_driver_sql(
         "SELECT rolcreatedb FROM pg_roles WHERE rolname = current_user"
      ).scalar_one()
      if not can_create:
         pytest.skip("test role lacks CREATEDB")
      connection.exec_driver_sql('CREATE DATABASE "%s"' % database_name)

   engine = create_engine(
      admin_url.set(database=database_name), poolclass=NullPool)
   try:
      yield engine
   finally:
      engine.dispose()
      with admin_engine.connect() as connection:
         connection.exec_driver_sql(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()",
            (database_name,),
         )
         connection.exec_driver_sql(
            'DROP DATABASE IF EXISTS "%s"' % database_name)
      admin_engine.dispose()


def test_fresh_bootstrap_applies_packaged_comment_only_placeholder(postgres_engine):
   runner = MigrationRunner(postgres_engine, "test-version")

   result = runner.migrate()

   assert result.applied_versions == (1,)
   with postgres_engine.connect() as connection:
      assert connection.exec_driver_sql(
         "SELECT count(*) FROM node_monitor.schema_migrations"
      ).scalar_one() == 1


def test_fresh_bootstrap_noop_replay_and_read_only_status(postgres_engine):
   migration = _migration()
   runner = MigrationRunner(
      postgres_engine, "test-version", migrations=(migration,))

   before = runner.status()
   assert not before.initialized
   assert before.pending_versions == (1,)

   first = runner.migrate()
   second = runner.migrate()
   after = runner.status()

   assert first.applied_versions == (1,)
   assert second.applied_versions == ()
   assert after.initialized
   assert after.current_version == 1
   assert after.pending_versions == ()
   with postgres_engine.connect() as connection:
      row = connection.exec_driver_sql(
         "SELECT version, name, checksum, transactional, "
         "application_version, execution_ms "
         "FROM node_monitor.schema_migrations").mappings().one()
   assert dict(row) == {
      "version": 1,
      "name": migration.name,
      "checksum": migration.checksum,
      "transactional": True,
      "application_version": "test-version",
      "execution_ms": row["execution_ms"],
   }
   assert row["execution_ms"] >= 0


def test_distinct_sessions_refuse_lock_before_bootstrap(postgres_engine):
   runner = MigrationRunner(
      postgres_engine, "test", migrations=(_migration(),))
   with postgres_engine.connect() as holder:
      assert holder.exec_driver_sql(
         "SELECT pg_try_advisory_lock(%s)",
         (MIGRATION_LOCK_KEY,)).scalar_one()
      holder.commit()
      with pytest.raises(MigrationLockError):
         runner.migrate()
      with postgres_engine.connect() as observer:
         assert observer.exec_driver_sql(
            "SELECT to_regclass('node_monitor.schema_migrations') IS NULL"
         ).scalar_one()
      assert holder.exec_driver_sql(
         "SELECT pg_advisory_unlock(%s)",
         (MIGRATION_LOCK_KEY,)).scalar_one()
      holder.commit()

   assert runner.migrate().applied_versions == (1,)


def test_postcondition_failure_rolls_back_ddl_and_ledger(postgres_engine):
   migration = _migration(
      sql=b"CREATE TABLE node_monitor.must_roll_back (x integer);\n")

   def fail_postcondition(connection, item):
      raise AssertionError("deliberate postcondition failure")

   runner = MigrationRunner(
      postgres_engine, "test", migrations=(migration,),
      postcondition=fail_postcondition,
   )
   with pytest.raises(MigrationApplyError):
      runner.migrate()

   with postgres_engine.connect() as connection:
      assert connection.exec_driver_sql(
         "SELECT to_regclass('node_monitor.must_roll_back') IS NULL"
      ).scalar_one()
      assert connection.exec_driver_sql(
         "SELECT count(*) FROM node_monitor.schema_migrations"
      ).scalar_one() == 0


def test_changed_checksum_fails_before_pending_sql(postgres_engine):
   first = _migration()
   MigrationRunner(postgres_engine, "test", migrations=(first,)).migrate()
   pending = _migration(
      2, "pending", b"CREATE TABLE node_monitor.must_not_run (x integer);\n")
   changed = _migration(1, "one", b"SELECT 2;\n")

   runner = MigrationRunner(
      postgres_engine, "test", migrations=(changed, pending))
   with pytest.raises(MigrationDriftError):
      runner.migrate()

   with postgres_engine.connect() as connection:
      assert connection.exec_driver_sql(
         "SELECT to_regclass('node_monitor.must_not_run') IS NULL"
      ).scalar_one()
