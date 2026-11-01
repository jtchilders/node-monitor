"""PostgreSQL fixtures. Each test that needs a database gets its own disposable one, named
`node_monitor_test_<8 hex>`; nothing else on the server is created, changed or dropped.

The tests skip unless NODE_MONITOR_TEST_ADMIN_URL names a server and a role that may create
databases (locally `postgresql://localhost/postgres`; in CI a postgres service).
"""

import os
import re
import secrets
from urllib.parse import urlsplit, urlunsplit

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

ADMIN_URL_ENV = "NODE_MONITOR_TEST_ADMIN_URL"
TEST_DB_PREFIX = "node_monitor_test_"
_TEST_DB_NAME = re.compile(r"^node_monitor_test_[0-9a-f]{8}$")


def with_dbname(url: str, dbname: str) -> str:
   """`url` pointing at database `dbname`, as a URL when given one."""
   parts = urlsplit(url)
   if parts.scheme in ("postgresql", "postgres"):
      return urlunsplit(parts._replace(path="/" + dbname))
   params = conninfo_to_dict(url)
   params["dbname"] = dbname
   return make_conninfo(**params)


def with_user(url: str, user: str) -> str:
   """`url` connecting as `user`, replacing any user and password it already names."""
   parts = urlsplit(url)
   if parts.scheme in ("postgresql", "postgres"):
      host = parts.netloc.rpartition("@")[2]
      return urlunsplit(parts._replace(netloc=f"{user}@{host}"))
   params = conninfo_to_dict(url)
   params.pop("password", None)
   params["user"] = user
   return make_conninfo(**params)


def _admin_connect(admin_url):
   return psycopg.connect(admin_url, autocommit=True, connect_timeout=10)


def _drop_test_db(admin_url, name):
   # The name check is the guard that keeps a fixture bug from dropping a real database.
   if not _TEST_DB_NAME.match(name):
      raise ValueError(f"refusing to drop {name!r}: not a test database name")
   with _admin_connect(admin_url) as conn:
      conn.execute(
         "SELECT pg_terminate_backend(pid) FROM pg_stat_activity"
         " WHERE datname = %s AND pid <> pg_backend_pid()",
         (name,),
      )
      conn.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))


@pytest.fixture(scope="session")
def pg_admin_url():
   url = os.environ.get(ADMIN_URL_ENV)
   if not url:
      pytest.skip(f"{ADMIN_URL_ENV} is not set")
   return url


@pytest.fixture
def pg_db(pg_admin_url):
   """URL of a fresh, empty database containing only the `node_monitor` schema."""
   name = TEST_DB_PREFIX + secrets.token_hex(4)
   with _admin_connect(pg_admin_url) as conn:
      # template0 cannot be busy, unlike template1 on a shared server, and keeps the copy
      # minimal.
      conn.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(sql.Identifier(name)))
   try:
      url = with_dbname(pg_admin_url, name)
      with psycopg.connect(url, autocommit=True, connect_timeout=10) as conn:
         conn.execute("CREATE SCHEMA node_monitor")
      yield url
   finally:
      _drop_test_db(pg_admin_url, name)


@pytest.fixture
def migrated_db(pg_db):
   from node_monitor.database.connect import connect
   from node_monitor.database.migrate import migrate

   with connect(pg_db) as conn:
      migrate(conn)
   return pg_db
