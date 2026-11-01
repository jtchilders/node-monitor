import re
import secrets

import psycopg
import pytest
from psycopg import sql

from node_monitor.database.bootstrap import bootstrap
from node_monitor.database.connect import connect
from node_monitor.database.migrate import migrate
from tests.database.conftest import _admin_connect, _drop_test_db, with_dbname

_ROLE = re.compile(r"^nm_test_(owner|reader)_[0-9a-f]{8}$")


def _drop_test_role(conn, name):
   if not _ROLE.match(name):
      raise ValueError(f"refusing to drop {name!r}: not a test role name")
   conn.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(name)))


@pytest.fixture
def names(pg_admin_url):
   """Unique database and role names; everything is dropped afterwards, database first."""
   hexid = secrets.token_hex(4)
   n = {
      "database": f"node_monitor_test_{hexid}",
      "owner": f"nm_test_owner_{hexid}",
      "reader": f"nm_test_reader_{hexid}",
   }
   try:
      yield n
   finally:
      _drop_test_db(pg_admin_url, n["database"])
      with _admin_connect(pg_admin_url) as conn:
         for role in (n["owner"], n["reader"]):
            _drop_test_role(conn, role)


def _url(admin_url, names, role):
   return with_dbname(admin_url, names["database"]).replace("://", f"://{names[role]}@", 1)


def test_bootstrap_is_idempotent(pg_admin_url, names):
   first = bootstrap(pg_admin_url, **names)
   assert len(first) == 4
   assert bootstrap(pg_admin_url, **names) == []


def test_database_uses_builtin_c_utf8_collation(pg_admin_url, names):
   bootstrap(pg_admin_url, **names)
   with _admin_connect(pg_admin_url) as conn:
      row = conn.execute(
         "SELECT datlocprovider, datlocale, pg_encoding_to_char(encoding)"
         " FROM pg_database WHERE datname = %s",
         (names["database"],),
      ).fetchone()
   assert row == ("b", "C.UTF-8", "UTF8")


def test_database_settings_and_public_privileges(pg_admin_url, names):
   bootstrap(pg_admin_url, **names)
   with connect(_url(pg_admin_url, names, "owner")) as conn:
      migrate(conn)
   # A plain connection, without the session options connect() adds, still finds the
   # tables unqualified and reads times in UTC.
   with psycopg.connect(_url(pg_admin_url, names, "reader")) as conn:
      assert conn.execute("SELECT count(*) FROM live_nodes").fetchone()[0] == 0
      assert conn.execute("SHOW TimeZone").fetchone()[0] == "UTC"
   with _admin_connect(pg_admin_url) as conn:
      for privilege in ("CONNECT", "TEMP"):
         assert not conn.execute(
            "SELECT has_database_privilege('public', %s, %s)", (names["database"], privilege)
         ).fetchone()[0]
      assert conn.execute(
         "SELECT has_database_privilege(%s, %s, 'CONNECT')", (names["owner"], names["database"])
      ).fetchone()[0]


def test_rejects_bad_names(pg_admin_url):
   with pytest.raises(ValueError):
      bootstrap(pg_admin_url, database='x"; DROP DATABASE postgres; --')


def test_reader_can_use_but_not_create(pg_admin_url, names):
   bootstrap(pg_admin_url, **names)
   with connect(_url(pg_admin_url, names, "reader")) as conn:
      assert conn.execute("SELECT has_schema_privilege('node_monitor', 'USAGE')").fetchone()[0]
      with pytest.raises(psycopg.errors.InsufficientPrivilege):
         conn.execute("CREATE TABLE node_monitor.nope (a int)")


def test_reader_reads_but_cannot_write_migrated_tables(pg_admin_url, names):
   bootstrap(pg_admin_url, **names)
   with connect(_url(pg_admin_url, names, "owner")) as conn:
      migrate(conn)
   with connect(_url(pg_admin_url, names, "reader")) as conn:
      conn.execute("SELECT count(*) FROM node_monitor.nodes").fetchone()
      conn.execute("SELECT count(*) FROM node_monitor.collection_events").fetchone()
      with pytest.raises(psycopg.errors.InsufficientPrivilege):
         conn.execute("DELETE FROM node_monitor.nodes")


def test_rerun_grants_select_on_existing_tables(pg_admin_url, names):
   bootstrap(pg_admin_url, **names)
   admin_db = with_dbname(pg_admin_url, names["database"])
   with psycopg.connect(admin_db, autocommit=True) as conn:
      # Created by someone other than the owner, so default privileges do not apply.
      conn.execute("CREATE TABLE node_monitor.early (a int)")
   bootstrap(pg_admin_url, **names)
   with connect(_url(pg_admin_url, names, "reader")) as conn:
      conn.execute("SELECT * FROM node_monitor.early")
