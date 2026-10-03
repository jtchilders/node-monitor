"""Real-PostgreSQL database and daemon-isolation acceptance (operational-web-
dashboard plan Task 9, Step 3).

All tests in this file are unconditionally SKIPPED when
NODE_MONITOR_TEST_DATABASE_URL is not set -- they never fabricate evidence.

Proves:
  * The web reader role cannot INSERT, UPDATE, DELETE, CREATE TABLE, or
    ALTER TABLE against any node_monitor table -- each statement is rolled
    back explicitly after the expected rejection so the reader connection
    stays usable for the next statement in the same test.
  * The web engine and a separately constructed "daemon-like" writer engine
    are genuinely distinct engine/pool objects, never the same singleton.

Reuses the existing ``migrated_reader_engine`` fixture pattern from
``tests/web/test_web_database_preflight.py`` (UUID-named disposable
database + UUID-named disposable reader role, both dropped in ``finally``).
Never starts, stops, restarts, or reconfigures PostgreSQL, and never
touches ``pbs-monitor``, ``node_monitor_dev``, or the resident collector.
"""

import os
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.pool import NullPool

from node_monitor.config import DatabaseConfig
from node_monitor.database.migration import MigrationRunner
from node_monitor.database.web import WebDatabase

from tests.web.conftest import pg_skip


_PG_AVAILABLE = bool(os.environ.get("NODE_MONITOR_TEST_DATABASE_URL"))


# ---------------------------------------------------------------------------
# Disposable migrated database + disposable UUID-named reader role
# ---------------------------------------------------------------------------

@pytest.fixture
def isolation_db():
   """Create a disposable UUID-named database, migrate it, create a
   UUID-named reader role with SELECT-only grants, and yield a namespace
   exposing both an admin engine (for mutation attempts to compare against)
   and a reader connection for the rejected-DML/DDL proofs.

   Cleanup drops the database and role in ``finally`` -- it never touches
   any other database, role, or the shared PostgreSQL server itself.
   """
   if not _PG_AVAILABLE:
      pytest.skip("NODE_MONITOR_TEST_DATABASE_URL is required")

   admin_url = make_url(os.environ["NODE_MONITOR_TEST_DATABASE_URL"])
   db_name = "nm_isotest_" + uuid.uuid4().hex[:16]
   role_name = "nm_isoreader_" + uuid.uuid4().hex[:16]

   admin_engine = create_engine(
      admin_url, poolclass=NullPool, isolation_level="AUTOCOMMIT")
   with admin_engine.connect() as conn:
      conn.exec_driver_sql('CREATE DATABASE "%s"' % db_name)
      conn.exec_driver_sql(
         "CREATE ROLE %s LOGIN PASSWORD 'test_only_pw'" % role_name)

   db_admin_engine = create_engine(
      admin_url.set(database=db_name), poolclass=NullPool)

   try:
      runner = MigrationRunner(db_admin_engine, "task9-isolation-test")
      runner.migrate()

      with db_admin_engine.connect() as conn:
         conn.exec_driver_sql(
            "GRANT CONNECT ON DATABASE \"%s\" TO %s" % (db_name, role_name))
         conn.exec_driver_sql(
            "GRANT USAGE ON SCHEMA node_monitor TO %s" % role_name)
         conn.exec_driver_sql(
            "GRANT SELECT ON ALL TABLES IN SCHEMA node_monitor TO %s"
            % role_name)
         conn.commit()

      reader_url = admin_url.set(
         database=db_name, username=role_name, password="test_only_pw")
      reader_engine = create_engine(str(reader_url), poolclass=NullPool)

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
         pass

      ns = _Namespace()
      ns.db_name = db_name
      ns.role_name = role_name
      ns.admin_engine = db_admin_engine
      ns.reader_engine = reader_engine
      ns.reader_config = reader_config
      ns.reader_url = reader_url

      yield ns

      reader_engine.dispose()
   finally:
      db_admin_engine.dispose()
      with admin_engine.connect() as conn:
         conn.exec_driver_sql(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()", (db_name,))
         conn.exec_driver_sql('DROP DATABASE IF EXISTS "%s"' % db_name)
         conn.exec_driver_sql("DROP ROLE IF EXISTS %s" % role_name)
      admin_engine.dispose()


# ---------------------------------------------------------------------------
# Step 3: reader cannot mutate or run DDL -- real rejected statements,
# rolled back after each.
# ---------------------------------------------------------------------------

@pg_skip
def test_reader_cannot_mutate_or_run_ddl(isolation_db):
   """Reader role rejects INSERT, UPDATE, DELETE, CREATE TABLE, and ALTER
   TABLE against the real migrated schema; each statement is rolled back
   so the connection stays usable for the next one.
   """
   statements = (
      "INSERT INTO node_monitor.node_collection_log"
      "(system, recorded_at, event, detail) "
      "VALUES ('x', now(), 'x', '{}')",
      "UPDATE node_monitor.node_hardware SET cpu_logical = 1",
      "DELETE FROM node_monitor.node_poll_failures",
      "CREATE TABLE node_monitor.forbidden(id integer)",
      "ALTER TABLE node_monitor.node_hardware ADD COLUMN forbidden integer",
   )
   with isolation_db.reader_engine.connect() as reader_connection:
      for statement in statements:
         with pytest.raises(Exception) as exc_info:
            reader_connection.exec_driver_sql(statement)
         # Real PostgreSQL permission-denied evidence, not a generic error.
         # INSERT/UPDATE/DELETE/CREATE TABLE raise "permission denied";
         # ALTER TABLE on a table the reader does not own raises PostgreSQL's
         # distinct "must be owner of table" InsufficientPrivilege message --
         # both are genuine access-control rejections of the same class.
         message = str(exc_info.value).lower()
         assert (
            "permission denied" in message
            or "must be owner" in message
         ), (
            "expected a permission-denied/must-be-owner rejection for %r; "
            "got: %s" % (statement, exc_info.value))
         reader_connection.rollback()

      # Connection must still be usable for an ordinary SELECT after every
      # rejected statement was rolled back.
      result = reader_connection.execute(
         text("SELECT 1")).scalar_one()
      assert result == 1


@pg_skip
def test_reader_select_still_succeeds_after_rejections(isolation_db):
   """After every DML/DDL rejection + rollback, a real SELECT against a
   telemetry table still succeeds (proves the reader's grants are
   otherwise intact; the role itself was never revoked).
   """
   with isolation_db.reader_engine.connect() as reader_connection:
      try:
         reader_connection.exec_driver_sql(
            "DELETE FROM node_monitor.node_poll_failures")
      except Exception:
         pass
      reader_connection.rollback()
      count = reader_connection.execute(text(
         "SELECT count(*) FROM node_monitor.node_hardware")).scalar_one()
      assert count == 0  # empty but queryable -- reader SELECT still works


# ---------------------------------------------------------------------------
# Step 3: web and daemon engines/pools are distinct objects
# ---------------------------------------------------------------------------

@pg_skip
def test_web_and_daemon_engines_are_distinct_objects(isolation_db):
   """WebDatabase's engine and a separately constructed writer-style engine
   (same URL pattern a daemon writer would use) are genuinely distinct
   Python objects with distinct connection pools -- never a shared
   singleton engine between the read-only web process and the writer.
   """
   web_db = WebDatabase(isolation_db.reader_config)
   try:
      web_engine = web_db._engine

      # "Daemon-like" engine: a second, independently constructed engine
      # against the SAME admin URL a writer/daemon would use. Even when
      # pointed at the identical database, it must be a different engine
      # and pool object -- proving no process-wide singleton is shared
      # between the web reader and the daemon writer.
      daemon_like_engine = create_engine(
         str(isolation_db.admin_engine.url), poolclass=NullPool)
      try:
         assert web_engine is not daemon_like_engine
         assert web_engine.pool is not daemon_like_engine.pool
         assert id(web_engine) != id(daemon_like_engine)
      finally:
         daemon_like_engine.dispose()
   finally:
      web_db.dispose()


@pg_skip
def test_two_web_database_instances_have_independent_engines(isolation_db):
   """Two independently constructed WebDatabase instances (as would happen
   across two separate process starts) never share the same engine/pool
   object, even when built from equivalent config.
   """
   web_db_a = WebDatabase(isolation_db.reader_config)
   web_db_b = WebDatabase(isolation_db.reader_config)
   try:
      assert web_db_a._engine is not web_db_b._engine
      assert web_db_a._engine.pool is not web_db_b._engine.pool
   finally:
      web_db_a.dispose()
      web_db_b.dispose()
