"""Tests for node_monitor.database.web -- read-only preflight and
import-graph isolation from the DDL migration runner.

All real-PostgreSQL tests require NODE_MONITOR_TEST_DATABASE_URL; they
are explicitly skipped (not errored) when that variable is absent.
The import-isolation test runs unconditionally.
"""

import importlib
import os
import sys
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.pool import NullPool

from node_monitor.database.migration import MigrationRunner


_PG_AVAILABLE = bool(os.environ.get("NODE_MONITOR_TEST_DATABASE_URL"))


# ---------------------------------------------------------------------------
# Unit test: import graph isolation (no PostgreSQL needed -- always runs)
# ---------------------------------------------------------------------------

def test_web_database_module_does_not_import_ddl_runner():
   """Importing node_monitor.database.web must NOT trigger import of
   node_monitor.database.migration (which contains MigrationRunner / DDL).
   """
   for key in list(sys.modules):
      if "node_monitor.database.migration" in key:
         del sys.modules[key]
      if "node_monitor.database.web" in key:
         del sys.modules[key]
      if "node_monitor.database.schema_contract" in key:
         del sys.modules[key]

   module = importlib.import_module("node_monitor.database.web")
   assert module is not None
   assert "node_monitor.database.migration" not in sys.modules


# ---------------------------------------------------------------------------
# Disposable-database fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def migrated_reader_engine():
   """Create a disposable PostgreSQL database, run the packaged migration as
   the admin identity, create a UUID-named login role with SELECT-only
   privileges, and yield a namespace object with:

     .admin       -- engine bound to admin URL in the test DB
     ._role_name  -- the UUID-named reader role
     .preflight() -- calls WebDatabase(config).preflight()

   Cleanup drops the database and role in ``finally``.
   """
   if not _PG_AVAILABLE:
      pytest.skip("NODE_MONITOR_TEST_DATABASE_URL is required")

   from node_monitor.database.web import WebDatabase
   from node_monitor.config import DatabaseConfig

   admin_url = make_url(os.environ["NODE_MONITOR_TEST_DATABASE_URL"])
   db_name = "nm_webtest_" + uuid.uuid4().hex[:16]
   role_name = "nm_webreader_" + uuid.uuid4().hex[:16]

   admin_engine = create_engine(
      admin_url, poolclass=NullPool, isolation_level="AUTOCOMMIT")

   with admin_engine.connect() as conn:
      server_version = int(
         conn.exec_driver_sql("SHOW server_version_num").scalar_one())
      if server_version < 120000:
         pytest.skip("PostgreSQL 12 or newer is required")
      can_create = conn.exec_driver_sql(
         "SELECT rolcreatedb FROM pg_roles WHERE rolname = current_user"
      ).scalar_one()
      if not can_create:
         pytest.skip("admin role lacks CREATEDB")
      conn.exec_driver_sql('CREATE DATABASE "%s"' % db_name)
      conn.exec_driver_sql(
         "CREATE ROLE %s LOGIN PASSWORD 'test_only_pw'" % role_name)

   db_admin_engine = create_engine(
      admin_url.set(database=db_name), poolclass=NullPool)

   try:
      runner = MigrationRunner(db_admin_engine, "test-version")
      runner.migrate()

      # Revoke default PUBLIC TEMP on the newly created database so the
      # test database matches the expected production setup (operator
      # explicitly withholds TEMP from all roles including PUBLIC).
      with admin_engine.connect() as conn:
         conn.exec_driver_sql(
            'REVOKE TEMP ON DATABASE "%s" FROM PUBLIC' % db_name)

      with db_admin_engine.connect() as conn:
         conn.exec_driver_sql(
            'GRANT CONNECT ON DATABASE "%s" TO %s' % (db_name, role_name))
         conn.exec_driver_sql(
            "GRANT USAGE ON SCHEMA node_monitor TO %s" % role_name)
         conn.exec_driver_sql(
            "GRANT SELECT ON ALL TABLES IN SCHEMA node_monitor TO %s"
            % role_name)
         conn.commit()

      reader_url = admin_url.set(
         database=db_name,
         username=role_name,
         password="test_only_pw",
      )
      reader_config = DatabaseConfig(
         url=str(reader_url),
         schema="node_monitor",
         pool_size=1,
         max_overflow=0,
         echo_sql=False,
         pool_pre_ping=False,
         pool_timeout_sec=10,
         pool_recycle_sec=3600,
         connect_args=(("connect_timeout", 5),),
      )

      class _Namespace:
         def __init__(self, reader_cfg, admin_eng, role):
            self._reader_cfg = reader_cfg
            self.admin = admin_eng
            self._role_name = role

         def preflight(self):
            db = WebDatabase(self._reader_cfg)
            db.preflight()

      yield _Namespace(reader_config, db_admin_engine, role_name)

   finally:
      db_admin_engine.dispose()
      with admin_engine.connect() as conn:
         conn.exec_driver_sql(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()",
            (db_name,),
         )
         conn.exec_driver_sql('DROP DATABASE IF EXISTS "%s"' % db_name)
         conn.exec_driver_sql("DROP ROLE IF EXISTS %s" % role_name)
      admin_engine.dispose()


# ---------------------------------------------------------------------------
# Preflight GREEN path
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _PG_AVAILABLE,
                    reason="NODE_MONITOR_TEST_DATABASE_URL is required")
def test_preflight_passes_with_correct_grants(migrated_reader_engine):
   migrated_reader_engine.preflight()  # must not raise


# ---------------------------------------------------------------------------
# Preflight RED path: missing SELECT on telemetry tables
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _PG_AVAILABLE,
                    reason="NODE_MONITOR_TEST_DATABASE_URL is required")
def test_preflight_rejects_one_missing_select(migrated_reader_engine):
   with migrated_reader_engine.admin.begin() as conn:
      conn.exec_driver_sql(
         "REVOKE SELECT ON node_monitor.node_usage_intervals FROM %s"
         % migrated_reader_engine._role_name)
   with pytest.raises(Exception, match="web database preflight failed"):
      migrated_reader_engine.preflight()


@pytest.mark.skipif(not _PG_AVAILABLE,
                    reason="NODE_MONITOR_TEST_DATABASE_URL is required")
def test_preflight_rejects_missing_select_on_schema_migrations(
      migrated_reader_engine):
   with migrated_reader_engine.admin.begin() as conn:
      conn.exec_driver_sql(
         "REVOKE SELECT ON node_monitor.schema_migrations FROM %s"
         % migrated_reader_engine._role_name)
   with pytest.raises(Exception, match="web database preflight failed"):
      migrated_reader_engine.preflight()


# ---------------------------------------------------------------------------
# Preflight RED path: forbidden write/create privileges
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _PG_AVAILABLE,
                    reason="NODE_MONITOR_TEST_DATABASE_URL is required")
def test_preflight_rejects_effective_write_or_create(migrated_reader_engine):
   with migrated_reader_engine.admin.begin() as conn:
      conn.exec_driver_sql(
         "GRANT INSERT ON node_monitor.node_collection_log TO %s"
         % migrated_reader_engine._role_name)
   with pytest.raises(Exception, match="web database preflight failed"):
      migrated_reader_engine.preflight()


@pytest.mark.skipif(not _PG_AVAILABLE,
                    reason="NODE_MONITOR_TEST_DATABASE_URL is required")
def test_preflight_rejects_effective_update(migrated_reader_engine):
   with migrated_reader_engine.admin.begin() as conn:
      conn.exec_driver_sql(
         "GRANT UPDATE ON node_monitor.node_hardware TO %s"
         % migrated_reader_engine._role_name)
   with pytest.raises(Exception, match="web database preflight failed"):
      migrated_reader_engine.preflight()


@pytest.mark.skipif(not _PG_AVAILABLE,
                    reason="NODE_MONITOR_TEST_DATABASE_URL is required")
def test_preflight_rejects_effective_delete(migrated_reader_engine):
   with migrated_reader_engine.admin.begin() as conn:
      conn.exec_driver_sql(
         "GRANT DELETE ON node_monitor.node_poll_failures TO %s"
         % migrated_reader_engine._role_name)
   with pytest.raises(Exception, match="web database preflight failed"):
      migrated_reader_engine.preflight()


# ---------------------------------------------------------------------------
# Preflight: uninitialised database (no schema_migrations)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _PG_AVAILABLE,
                    reason="NODE_MONITOR_TEST_DATABASE_URL is required")
def test_preflight_fails_on_uninitialised_database():
   from node_monitor.database.web import WebDatabase
   from node_monitor.config import DatabaseConfig

   admin_url = make_url(os.environ["NODE_MONITOR_TEST_DATABASE_URL"])
   db_name = "nm_webtest_bare_" + uuid.uuid4().hex[:16]
   admin_engine = create_engine(
      admin_url, poolclass=NullPool, isolation_level="AUTOCOMMIT")
   try:
      with admin_engine.connect() as conn:
         conn.exec_driver_sql('CREATE DATABASE "%s"' % db_name)

      bare_config = DatabaseConfig(
         url=str(admin_url.set(database=db_name)),
         schema="node_monitor",
         pool_size=1,
         max_overflow=0,
         echo_sql=False,
         pool_pre_ping=False,
         pool_timeout_sec=10,
         pool_recycle_sec=3600,
         connect_args=(("connect_timeout", 5),),
      )
      db = WebDatabase(bare_config)
      with pytest.raises(Exception, match="web database preflight failed"):
         db.preflight()
   finally:
      with admin_engine.connect() as conn:
         conn.exec_driver_sql(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()",
            (db_name,),
         )
         conn.exec_driver_sql('DROP DATABASE IF EXISTS "%s"' % db_name)
      admin_engine.dispose()


# ---------------------------------------------------------------------------
# Error sanitization: no URL/role/SQL in raised message
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _PG_AVAILABLE,
                    reason="NODE_MONITOR_TEST_DATABASE_URL is required")
def test_preflight_error_omits_url_and_role(migrated_reader_engine):
   with migrated_reader_engine.admin.begin() as conn:
      conn.exec_driver_sql(
         "REVOKE SELECT ON node_monitor.node_counter_minute FROM %s"
         % migrated_reader_engine._role_name)
   try:
      migrated_reader_engine.preflight()
      pytest.fail("preflight should have raised")
   except Exception as exc:
      message = str(exc)
      assert "postgresql" not in message.lower()
      assert migrated_reader_engine._role_name not in message
      assert "SELECT" not in message
