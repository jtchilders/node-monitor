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
# Unit tests: redundant broad exception catch is gone
# ---------------------------------------------------------------------------

def test_web_preflight_catch_is_not_redundant_broad_tuple():
   """The preflight() except clause must not use a broad redundant tuple like
   (SQLAlchemyError, ValueError, RuntimeError, Exception) -- Exception alone
   is sufficient and makes intent clear.
   """
   import inspect
   from node_monitor.database import web as web_module
   src = inspect.getsource(web_module)
   assert "(SQLAlchemyError, ValueError, RuntimeError, Exception)" not in src, (
       "Replace redundant broad tuple with plain `except Exception`"
   )


def test_web_preflight_arbitrary_driver_failure_is_sanitized():
   """An arbitrary driver exception (not SQLAlchemy, not ValueError) raised
   inside preflight must be caught and re-raised as WebDatabaseError with no
   credential detail leaking into the message.
   """
   from node_monitor.database.web import WebDatabase, WebDatabaseError
   from node_monitor.config import DatabaseConfig

   cfg = DatabaseConfig(
      url="postgresql://secret-user:secret-pass@invalid.host.invalid:5432/db",
      schema="node_monitor",
      pool_size=1,
      max_overflow=0,
      echo_sql=False,
      pool_pre_ping=False,
      pool_timeout_sec=1,
      pool_recycle_sec=3600,
      connect_args=(("connect_timeout", 1),),
   )
   db = WebDatabase(cfg)
   try:
      with pytest.raises(WebDatabaseError) as exc_info:
         db.preflight()
      msg = str(exc_info.value)
      assert "secret-user" not in msg
      assert "secret-pass" not in msg
      assert "invalid.host.invalid" not in msg
   finally:
      db.dispose()


# ---------------------------------------------------------------------------
# Unit tests: schema contract exports column/constraint/index catalogs
# ---------------------------------------------------------------------------

def test_schema_contract_exports_source_schema_columns():
   """schema_contract must export _SOURCE_SCHEMA_COLUMNS so the web preflight
   can validate exact column/type/nullability rather than just table existence.
   """
   from node_monitor.database import schema_contract
   assert hasattr(schema_contract, "_SOURCE_SCHEMA_COLUMNS"), (
       "_SOURCE_SCHEMA_COLUMNS must be in schema_contract"
   )
   cols = schema_contract._SOURCE_SCHEMA_COLUMNS
   for table in ("node_hardware", "node_counter_minute", "node_usage_intervals",
                 "node_poll_failures", "node_collection_log"):
      assert table in cols, "Expected table %r in _SOURCE_SCHEMA_COLUMNS" % table


def test_schema_contract_exports_required_constraints():
   """schema_contract must export _REQUIRED_SOURCE_CONSTRAINTS."""
   from node_monitor.database import schema_contract
   assert hasattr(schema_contract, "_REQUIRED_SOURCE_CONSTRAINTS")
   assert isinstance(schema_contract._REQUIRED_SOURCE_CONSTRAINTS, frozenset)


def test_schema_contract_exports_required_indexes():
   """schema_contract must export _REQUIRED_SOURCE_INDEXES."""
   from node_monitor.database import schema_contract
   assert hasattr(schema_contract, "_REQUIRED_SOURCE_INDEXES")
   assert isinstance(schema_contract._REQUIRED_SOURCE_INDEXES, frozenset)


def test_web_preflight_validates_column_signatures_not_just_table_names():
   """web._require_current_schema must validate exact column signatures from
   _SOURCE_SCHEMA_COLUMNS (columns/types/nullability), not just table name
   presence. This is tested by verifying the function references the schema
   contract's column catalog.
   """
   import inspect
   from node_monitor.database import web as web_module
   src = inspect.getsource(web_module)
   assert "_SOURCE_SCHEMA_COLUMNS" in src, (
       "web.py must reference _SOURCE_SCHEMA_COLUMNS for drift detection"
   )


# ---------------------------------------------------------------------------
# Unit tests: compare_source_schema helper
# ---------------------------------------------------------------------------

def test_schema_drift_comparison_rejects_extra_column():
   """Column-level schema drift comparison must reject a catalog where an
   extra column is added.
   """
   from node_monitor.database.schema_contract import (
      _SOURCE_SCHEMA_COLUMNS,
      compare_source_schema,
   )
   actual = {t: list(c) for t, c in _SOURCE_SCHEMA_COLUMNS.items()}
   actual["node_hardware"] = list(actual["node_hardware"]) + [
      ("extra_column", "text", "YES")
   ]
   result = compare_source_schema(actual)
   assert result is not None, (
       "compare_source_schema must return a non-None error description "
       "when actual schema differs from expected"
   )


def test_schema_drift_comparison_accepts_exact_match():
   """compare_source_schema returns None when actual matches expected exactly."""
   from node_monitor.database.schema_contract import (
      _SOURCE_SCHEMA_COLUMNS,
      compare_source_schema,
   )
   actual = {t: list(c) for t, c in _SOURCE_SCHEMA_COLUMNS.items()}
   result = compare_source_schema(actual)
   assert result is None, (
       "compare_source_schema must return None on exact match, got: %r" % result
   )


def test_schema_drift_comparison_rejects_missing_table():
   """compare_source_schema rejects a catalog missing a required table."""
   from node_monitor.database.schema_contract import (
      _SOURCE_SCHEMA_COLUMNS,
      compare_source_schema,
   )
   actual = {
      t: list(c)
      for t, c in _SOURCE_SCHEMA_COLUMNS.items()
      if t != "node_poll_failures"
   }
   result = compare_source_schema(actual)
   assert result is not None


def test_schema_drift_comparison_rejects_wrong_nullability():
   """compare_source_schema rejects a catalog with wrong nullability."""
   from node_monitor.database.schema_contract import (
      _SOURCE_SCHEMA_COLUMNS,
      compare_source_schema,
   )
   actual = {t: list(c) for t, c in _SOURCE_SCHEMA_COLUMNS.items()}
   first = actual["node_hardware"][0]
   wrong_null = "YES" if first[2] == "NO" else "NO"
   actual["node_hardware"][0] = (first[0], first[1], wrong_null)
   result = compare_source_schema(actual)
   assert result is not None


# ---------------------------------------------------------------------------
# Disposable-database fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def migrated_reader_engine():
   """Create a disposable PostgreSQL database, run the packaged migration as
   the admin identity, create a UUID-named login role with SELECT-only
   privileges, and yield a namespace object with:

     .admin       -- engine connected to the test DB (NullPool, autocommit)
     ._role_name  -- the UUID-named reader role
     ._db_name    -- the disposable database name
     ._reader_cfg -- DatabaseConfig for the reader role
     .admin       -- engine for admin operations in the test DB

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
         def __init__(self, reader_cfg, admin_eng, role, db):
            self._reader_cfg = reader_cfg
            self.admin = admin_eng
            self._role_name = role
            self._db_name = db

      yield _Namespace(reader_config, db_admin_engine, role_name, db_name)

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
   from node_monitor.database.web import WebDatabase
   db = WebDatabase(migrated_reader_engine._reader_cfg)
   try:
      db.preflight()  # must not raise
   finally:
      db.dispose()


# ---------------------------------------------------------------------------
# Preflight RED path: missing SELECT on telemetry tables
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _PG_AVAILABLE,
                    reason="NODE_MONITOR_TEST_DATABASE_URL is required")
def test_preflight_rejects_one_missing_select(migrated_reader_engine):
   from node_monitor.database.web import WebDatabase, WebDatabaseError
   with migrated_reader_engine.admin.begin() as conn:
      conn.exec_driver_sql(
         "REVOKE SELECT ON node_monitor.node_usage_intervals FROM %s"
         % migrated_reader_engine._role_name)
   db = WebDatabase(migrated_reader_engine._reader_cfg)
   try:
      with pytest.raises(WebDatabaseError, match="web database preflight failed"):
         db.preflight()
   finally:
      db.dispose()


@pytest.mark.skipif(not _PG_AVAILABLE,
                    reason="NODE_MONITOR_TEST_DATABASE_URL is required")
def test_preflight_rejects_missing_select_on_schema_migrations(
      migrated_reader_engine):
   from node_monitor.database.web import WebDatabase, WebDatabaseError
   with migrated_reader_engine.admin.begin() as conn:
      conn.exec_driver_sql(
         "REVOKE SELECT ON node_monitor.schema_migrations FROM %s"
         % migrated_reader_engine._role_name)
   db = WebDatabase(migrated_reader_engine._reader_cfg)
   try:
      with pytest.raises(WebDatabaseError, match="web database preflight failed"):
         db.preflight()
   finally:
      db.dispose()


# ---------------------------------------------------------------------------
# Preflight RED path: forbidden effective privileges (parameterized)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _PG_AVAILABLE,
                    reason="NODE_MONITOR_TEST_DATABASE_URL is required")
@pytest.mark.parametrize("privilege,table", [
   ("INSERT", "node_collection_log"),
   ("UPDATE", "node_hardware"),
   ("DELETE", "node_poll_failures"),
   ("TRUNCATE", "node_counter_minute"),
   ("REFERENCES", "node_usage_intervals"),
   ("TRIGGER", "node_hardware"),
])
def test_preflight_rejects_forbidden_table_privilege(
      privilege, table, migrated_reader_engine):
   """All six forbidden effective privileges -- INSERT, UPDATE, DELETE,
   TRUNCATE, REFERENCES, TRIGGER -- must individually cause preflight failure.
   """
   from node_monitor.database.web import WebDatabase, WebDatabaseError
   with migrated_reader_engine.admin.begin() as conn:
      conn.exec_driver_sql(
         "GRANT %s ON node_monitor.%s TO %s"
         % (privilege, table, migrated_reader_engine._role_name))
   db = WebDatabase(migrated_reader_engine._reader_cfg)
   try:
      with pytest.raises(WebDatabaseError, match="web database preflight failed"):
         db.preflight()
   finally:
      db.dispose()


@pytest.mark.skipif(not _PG_AVAILABLE,
                    reason="NODE_MONITOR_TEST_DATABASE_URL is required")
def test_preflight_rejects_schema_create_privilege(migrated_reader_engine):
   """Schema-level CREATE must cause preflight failure."""
   from node_monitor.database.web import WebDatabase, WebDatabaseError
   with migrated_reader_engine.admin.begin() as conn:
      conn.exec_driver_sql(
         "GRANT CREATE ON SCHEMA node_monitor TO %s"
         % migrated_reader_engine._role_name)
   db = WebDatabase(migrated_reader_engine._reader_cfg)
   try:
      with pytest.raises(WebDatabaseError, match="web database preflight failed"):
         db.preflight()
   finally:
      db.dispose()


# ---------------------------------------------------------------------------
# Preflight: real-PG schema drift tests
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _PG_AVAILABLE,
                    reason="NODE_MONITOR_TEST_DATABASE_URL is required")
def test_preflight_rejects_schema_drift_extra_column(migrated_reader_engine):
   """Preflight must reject a live database where a table has an extra column
   (schema drift), not just accept any database that has the right table names.
   """
   from node_monitor.database.web import WebDatabase, WebDatabaseError
   with migrated_reader_engine.admin.begin() as conn:
      conn.exec_driver_sql(
         "ALTER TABLE node_monitor.node_hardware "
         "ADD COLUMN _drift_test_col text")
   db = WebDatabase(migrated_reader_engine._reader_cfg)
   try:
      with pytest.raises(WebDatabaseError, match="web database preflight failed"):
         db.preflight()
   finally:
      db.dispose()


@pytest.mark.skipif(not _PG_AVAILABLE,
                    reason="NODE_MONITOR_TEST_DATABASE_URL is required")
def test_preflight_rejects_schema_drift_missing_column(migrated_reader_engine):
   """Preflight must reject a live database where a required column is absent."""
   from node_monitor.database.web import WebDatabase, WebDatabaseError
   with migrated_reader_engine.admin.begin() as conn:
      conn.exec_driver_sql(
         "ALTER TABLE node_monitor.node_hardware "
         "DROP COLUMN IF EXISTS boot_id")
   db = WebDatabase(migrated_reader_engine._reader_cfg)
   try:
      with pytest.raises(WebDatabaseError, match="web database preflight failed"):
         db.preflight()
   finally:
      db.dispose()


# ---------------------------------------------------------------------------
# Preflight: uninitialised database (no schema_migrations)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _PG_AVAILABLE,
                    reason="NODE_MONITOR_TEST_DATABASE_URL is required")
def test_preflight_fails_on_uninitialised_database():
   from node_monitor.database.web import WebDatabase, WebDatabaseError
   from node_monitor.config import DatabaseConfig

   admin_url = make_url(os.environ["NODE_MONITOR_TEST_DATABASE_URL"])
   db_name = "nm_webtest_bare_" + uuid.uuid4().hex[:16]
   admin_engine = create_engine(
      admin_url, poolclass=NullPool, isolation_level="AUTOCOMMIT")
   db = None
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
      with pytest.raises(WebDatabaseError, match="web database preflight failed"):
         db.preflight()
   finally:
      if db is not None:
         db.dispose()
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
   from node_monitor.database.web import WebDatabase, WebDatabaseError
   with migrated_reader_engine.admin.begin() as conn:
      conn.exec_driver_sql(
         "REVOKE SELECT ON node_monitor.node_counter_minute FROM %s"
         % migrated_reader_engine._role_name)
   db = WebDatabase(migrated_reader_engine._reader_cfg)
   try:
      try:
         db.preflight()
         pytest.fail("preflight should have raised")
      except WebDatabaseError as exc:
         message = str(exc)
         assert "postgresql" not in message.lower()
         assert migrated_reader_engine._role_name not in message
         assert "SELECT" not in message
   finally:
      db.dispose()
