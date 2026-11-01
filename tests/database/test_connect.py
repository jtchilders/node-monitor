import psycopg
import pytest

from node_monitor.database.connect import DatabaseUnavailable, connect, is_unavailable


@pytest.mark.parametrize(
   "exc",
   [
      psycopg.OperationalError("server closed the connection unexpectedly"),
      psycopg.errors.AdminShutdown(),
      psycopg.errors.ConnectionTimeout(),
      psycopg.errors.CannotConnectNow(),
      DatabaseUnavailable("spool it"),
   ],
)
def test_outages_are_unavailable(exc):
   assert is_unavailable(exc)


@pytest.mark.parametrize(
   "exc",
   [
      psycopg.errors.SyntaxError(),
      psycopg.errors.UndefinedTable(),
      psycopg.errors.UniqueViolation(),
      psycopg.errors.NumericValueOutOfRange(),
      psycopg.DataError(),
      psycopg.ProgrammingError(),
      ValueError("not a database error"),
   ],
)
def test_bad_statements_and_data_are_not_unavailable(exc):
   assert not is_unavailable(exc)


def test_unreachable_server_is_unavailable():
   with pytest.raises(psycopg.Error) as info:
      connect("postgresql://localhost:1/x", connect_timeout_sec=2)
   assert is_unavailable(info.value)


def test_syntax_error_is_not_unavailable(pg_admin_url):
   with connect(pg_admin_url) as conn:
      with pytest.raises(psycopg.Error) as info:
         conn.execute("SELEC 1")
   assert not is_unavailable(info.value)


def test_connect_sets_search_path_and_utc(pg_admin_url):
   with connect(pg_admin_url, autocommit=True) as conn:
      assert conn.autocommit is True
      search_path = conn.execute("SHOW search_path").fetchone()[0]
      assert search_path.replace(" ", "") == "node_monitor,public"
      assert conn.execute("SHOW TimeZone").fetchone()[0] == "UTC"


def test_connect_keeps_options_given_in_the_url(pg_admin_url):
   sep = "&" if "?" in pg_admin_url else "?"
   url = pg_admin_url + sep + "options=-c%20application_name%3Dnm_test"
   with connect(url) as conn:
      assert conn.execute("SHOW application_name").fetchone()[0] == "nm_test"
      assert conn.execute("SHOW TimeZone").fetchone()[0] == "UTC"
