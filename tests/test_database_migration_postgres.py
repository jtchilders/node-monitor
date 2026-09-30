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


def _schema_signature(connection):
   columns = connection.exec_driver_sql("""
      SELECT table_name, column_name, data_type, udt_name, is_nullable,
             is_identity, is_generated, generation_expression
      FROM information_schema.columns
      WHERE table_schema = 'node_monitor'
        AND table_name <> 'schema_migrations'
      ORDER BY table_name, ordinal_position
   """).all()
   constraints = connection.exec_driver_sql("""
      SELECT c.relname, con.conname, con.contype,
             pg_get_constraintdef(con.oid, true)
      FROM pg_constraint con
      JOIN pg_class c ON c.oid = con.conrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'node_monitor'
        AND c.relname <> 'schema_migrations'
      ORDER BY c.relname, con.conname
   """).all()
   indexes = connection.exec_driver_sql("""
      SELECT tablename, indexname, indexdef
      FROM pg_indexes
      WHERE schemaname = 'node_monitor'
        AND tablename <> 'schema_migrations'
      ORDER BY tablename, indexname
   """).all()
   return tuple(columns), tuple(constraints), tuple(indexes)


def test_initial_source_schema_has_exact_tables_and_key_constraints(postgres_engine):
   MigrationRunner(postgres_engine, "test").migrate()
   with postgres_engine.connect() as connection:
      tables = connection.exec_driver_sql("""
         SELECT table_name FROM information_schema.tables
         WHERE table_schema = 'node_monitor' AND table_type = 'BASE TABLE'
         ORDER BY table_name
      """).scalars().all()
      assert tables == [
         "node_collection_log", "node_counter_minute", "node_hardware",
         "node_poll_failures", "node_usage_intervals", "schema_migrations",
      ]
      columns, constraints, indexes = _schema_signature(connection)
      by_table = {}
      for table, column, data_type, udt_name, nullable, identity, generated, expression in columns:
         by_table.setdefault(table, {})[column] = {
            "data_type": data_type, "udt_name": udt_name,
            "nullable": nullable, "identity": identity,
            "generated": generated, "expression": expression,
         }

      assert set(by_table["node_hardware"]) == {
         "system", "source_hostname", "first_seen", "last_verified",
         "boot_id", "btime", "cpu_model", "cpu_logical", "sockets",
         "cores_per_socket", "cpu_max_freq_khz", "numa_nodes",
         "mem_total_kb", "swap_total_kb", "hugepage_size_kb",
         "kernel_release", "os_pretty_name", "net_fs_mounts",
         "net_ifaces", "gpus", "probe_version",
      }
      assert set(by_table["node_counter_minute"]) == {
         "system", "source_hostname", "window_start", "window_end",
         "collector_hostname", "probe_version", "daemon_version",
         "sample_count", "expected_count", "coverage", "mem_available_kb",
         "cached_kb", "shmem_kb", "load1", "load5", "load15",
         "procs_running", "procs_total", "socket_count", "cpu_busy_pct",
         "network_rates", "lustre_md_summary", "meets_minimum_samples",
         "invalid_pair_count", "excess_sample_count",
      }
      assert set(by_table["node_usage_intervals"]) == {
         "id", "system", "source_hostname", "interval_start", "interval_end",
         "category", "activity", "username", "username_key",
         "process_count", "cpu_seconds", "rss_kb", "d_state_fraction",
         "interactivity_fraction", "sample_count", "expected_count",
         "unmeasured_count",
      }
      assert by_table["node_usage_intervals"]["id"]["identity"] == "YES"
      assert by_table["node_usage_intervals"]["username"]["nullable"] == "YES"
      assert by_table["node_usage_intervals"]["username_key"]["generated"] == "ALWAYS"
      assert "COALESCE" in by_table["node_usage_intervals"]["username_key"]["expression"].upper()
      assert by_table["node_counter_minute"]["cpu_busy_pct"]["udt_name"] == "jsonb"
      assert by_table["node_hardware"]["net_ifaces"]["udt_name"] == "jsonb"
      constraint_text = "\n".join(str(row) for row in constraints)
      index_text = "\n".join(str(row) for row in indexes)
      assert "node_counter_minute_pkey" in constraint_text
      assert "node_usage_intervals_grain_key" in constraint_text
      assert "coverage" in constraint_text
      assert "d_state_fraction" in constraint_text
      assert "node_counter_minute_retention_idx" in index_text
      assert "node_usage_intervals_retention_idx" in index_text
      assert "node_poll_failures_retention_idx" in index_text
      assert "node_collection_log_retention_idx" in index_text


def test_legacy_bootstrap_upgrades_to_same_schema_as_fresh(postgres_engine):
   from pathlib import Path
   legacy_sql = (Path(__file__).parents[1] / "node_monitor" / "db" / "schema.sql").read_text()
   with postgres_engine.begin() as connection:
      connection.exec_driver_sql(legacy_sql)
   MigrationRunner(postgres_engine, "test").migrate()
   with postgres_engine.connect() as connection:
      upgraded = _schema_signature(connection)

   # Recreate only the disposable schema, then compare a fresh migration.
   with postgres_engine.begin() as connection:
      connection.exec_driver_sql("DROP SCHEMA node_monitor CASCADE")
   MigrationRunner(postgres_engine, "test").migrate()
   with postgres_engine.connect() as connection:
      fresh = _schema_signature(connection)
   assert upgraded == fresh


def test_incompatible_legacy_hardware_rolls_back_all_source_tables(postgres_engine):
   with postgres_engine.begin() as connection:
      connection.exec_driver_sql("CREATE SCHEMA node_monitor")
      connection.exec_driver_sql("""
         CREATE TABLE node_monitor.node_hardware (
            system integer NOT NULL,
            source_hostname text NOT NULL,
            PRIMARY KEY (system, source_hostname)
         )
      """)

   with pytest.raises(MigrationApplyError):
      MigrationRunner(postgres_engine, "test").migrate()

   with postgres_engine.connect() as connection:
      tables = connection.exec_driver_sql("""
         SELECT table_name FROM information_schema.tables
         WHERE table_schema = 'node_monitor'
         ORDER BY table_name
      """).scalars().all()
      assert tables == ["node_hardware", "schema_migrations"]
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
