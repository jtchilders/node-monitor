"""Shared PostgreSQL disposable-database fixture for Task 9 real-PostgreSQL
acceptance tests (performance, database isolation, process isolation).

Mirrors the existing pattern already used by test_web_queries_postgres.py's
``pg_engine`` fixture and test_web_database_preflight.py's
``migrated_reader_engine`` fixture: a UUID-named disposable database is
created via the admin connection named by ``NODE_MONITOR_TEST_DATABASE_URL``,
migrated via the real ``MigrationRunner``, and dropped in ``finally``. This
fixture never starts, stops, restarts, or reconfigures PostgreSQL itself,
and never touches ``pbs-monitor``, ``node_monitor_dev``, or any other
database -- it creates and drops exactly one UUID-named disposable database
per test.

All tests in the three new Task 9 real-PostgreSQL files import this single
shared fixture rather than re-implementing disposable-database bootstrap
three times.
"""

import os
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.pool import NullPool

from node_monitor.database.migration import MigrationRunner


_PG_AVAILABLE = bool(os.environ.get("NODE_MONITOR_TEST_DATABASE_URL"))

pg_skip = pytest.mark.skipif(
   not _PG_AVAILABLE,
   reason="NODE_MONITOR_TEST_DATABASE_URL is required for PostgreSQL tests",
)


class DisposableDatabase:
   """Namespace returned by the ``pg_disposable_db`` fixture."""

   def __init__(self, engine, db_name, admin_url):
      self.engine = engine
      self.db_name = db_name
      self.admin_url = admin_url


@pytest.fixture
def pg_disposable_db():
   """Create one UUID-named disposable PostgreSQL database, migrate it with
   the real ``MigrationRunner``, yield a ``DisposableDatabase`` namespace,
   then drop the database in ``finally``.

   Skips (never fabricates evidence) when ``NODE_MONITOR_TEST_DATABASE_URL``
   is not set.
   """
   if not _PG_AVAILABLE:
      pytest.skip("NODE_MONITOR_TEST_DATABASE_URL is required")

   base_url = make_url(os.environ["NODE_MONITOR_TEST_DATABASE_URL"])
   admin_url = base_url.set(database="postgres")
   db_name = "nm_task9_" + uuid.uuid4().hex[:16]

   admin_engine = create_engine(
      admin_url, poolclass=NullPool, isolation_level="AUTOCOMMIT")
   with admin_engine.connect() as conn:
      conn.exec_driver_sql('CREATE DATABASE "%s"' % db_name)
   admin_engine.dispose()

   db_url = admin_url.set(database=db_name)
   engine = create_engine(str(db_url), poolclass=NullPool)
   try:
      runner = MigrationRunner(engine, "task9-test")
      runner.migrate()
      yield DisposableDatabase(engine, db_name, admin_url)
   finally:
      engine.dispose()
      cleanup = create_engine(
         admin_url, poolclass=NullPool, isolation_level="AUTOCOMMIT")
      with cleanup.connect() as conn:
         conn.exec_driver_sql(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()", (db_name,))
         conn.exec_driver_sql('DROP DATABASE IF EXISTS "%s"' % db_name)
      cleanup.dispose()
