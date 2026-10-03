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

This module also centralizes two hardening primitives required across every
Task 9 fixture that builds disposable SQL identifiers or disposable
credentials:

  * ``disposable_identifier()`` / ``quote_identifier()`` -- every disposable
    database/role name used by these fixtures is generated exclusively by
    ``disposable_identifier()`` (a fixed prefix plus 16 lowercase hex
    characters from ``uuid.uuid4().hex``), and ``quote_identifier()``
    validates that exact grammar, fail-closed, before the name is ever
    interpolated into DDL. This is defense-in-depth: the UUID-derived names
    this test suite actually generates were already a bounded, non-attacker-
    controlled value (there is no exploit being fixed here), but every new
    Task 9 fixture now goes through one strict, reviewable helper instead of
    five independent copies of ad-hoc ``%s``-into-SQL-string interpolation.
  * ``disposable_password()`` -- every disposable reader-role password used
    by these fixtures is freshly generated per fixture invocation via
    ``secrets.token_hex`` rather than the single shared literal
    ``"test_only_pw"``. The generated value is test-only, is passed to
    PostgreSQL exclusively as a bound parameter (never interpolated into a
    SQL string), and is never asserted against, printed, or embedded in any
    URL that a test prints -- only ``str(engine.url)`` renderings, which
    SQLAlchemy already masks (``***``), ever touch output.
"""

import os
import re
import secrets
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


# ---------------------------------------------------------------------------
# Strict disposable-identifier helper
# ---------------------------------------------------------------------------

# Exact grammar every disposable database/role name in these fixtures must
# match: a lowercase-letter-leading prefix (letters, digits, underscores)
# followed by an underscore and exactly 16 lowercase hex characters (the
# truncated form of uuid.uuid4().hex used throughout this file). Anything
# that does not match this exact grammar is rejected fail-closed -- no
# identifier is ever interpolated into SQL text without first passing this
# check.
_IDENTIFIER_GRAMMAR = re.compile(r"^[a-z][a-z0-9_]{0,61}_[0-9a-f]{16}$")


class DisposableIdentifierError(ValueError):
   """Raised when a disposable SQL identifier fails the strict grammar
   check. Fail-closed: no DDL is ever issued with a name that doesn't
   match ``_IDENTIFIER_GRAMMAR`` exactly."""


def disposable_identifier(prefix):
   """Return a new disposable identifier: ``prefix`` + "_" + 16 lowercase
   hex characters from a fresh ``uuid.uuid4()``.

   ``prefix`` itself must already be a safe, fixed, lowercase
   letters/digits/underscore literal (it is never derived from external
   input in this test suite); the generated suffix is what makes each
   identifier unique per test run.
   """
   candidate = "%s_%s" % (prefix, uuid.uuid4().hex[:16])
   return quote_identifier(candidate)


def quote_identifier(name):
   """Validate ``name`` against the exact disposable-identifier grammar and
   return it unquoted (callers still wrap it in double quotes at the SQL
   call site, matching this file's existing ``'"%s"' % name`` DDL style).

   Fails closed with ``DisposableIdentifierError`` before any SQL is
   constructed if ``name`` does not match ``_IDENTIFIER_GRAMMAR`` exactly --
   this rejects embedded quotes, semicolons, whitespace, SQL comments, or
   any other byte sequence outside the fixed prefix+hex shape, regardless
   of where ``name`` originated.
   """
   if not isinstance(name, str) or not _IDENTIFIER_GRAMMAR.match(name):
      raise DisposableIdentifierError(
         "disposable identifier %r does not match the required "
         "prefix+16-hex-character grammar" % (name,))
   return name


def disposable_password():
   """Return a freshly generated, test-only password for a disposable
   PostgreSQL reader role.

   Never the shared literal ``"test_only_pw"`` -- each fixture invocation
   gets its own random value via ``secrets.token_hex``. The caller MUST
   pass this value to PostgreSQL only as a bound parameter (never
   interpolated into SQL text) and must never print, log, or assert against
   it directly.
   """
   return secrets.token_hex(16)


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
   db_name = disposable_identifier("nm_task9")

   admin_engine = create_engine(
      admin_url, poolclass=NullPool, isolation_level="AUTOCOMMIT")
   with admin_engine.connect() as conn:
      conn.exec_driver_sql('CREATE DATABASE "%s"' % quote_identifier(db_name))
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
         conn.exec_driver_sql(
            'DROP DATABASE IF EXISTS "%s"' % quote_identifier(db_name))
      cleanup.dispose()
