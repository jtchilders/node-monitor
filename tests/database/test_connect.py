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
      # OperationalError subclasses that depend on the data: a replay fails the same way.
      psycopg.errors.ProgramLimitExceeded(),
      psycopg.errors.StatementTooComplex(),
      psycopg.errors.TooManyColumns(),
      psycopg.errors.TooManyArguments(),
      psycopg.errors.QueryCanceled(),
      ValueError("not a database error"),
   ],
)
def test_bad_statements_and_data_are_not_unavailable(exc):
   assert not is_unavailable(exc)


@pytest.mark.parametrize(
   "sqlstate, unavailable",
   [
      ("40P01", True),  # deadlock detected
      ("40001", True),  # serialization failure
      ("55P03", True),  # lock not available, e.g. past lock_timeout
      ("25006", True),  # read-only transaction: a standby during failover
      ("54000", False),  # program limit exceeded
      ("54001", False),  # statement too complex
      ("57014", False),  # query canceled, e.g. statement_timeout
   ],
)
def test_classification_by_sqlstate(sqlstate, unavailable):
   exc = psycopg.errors.lookup(sqlstate)()
   assert exc.sqlstate == sqlstate
   assert is_unavailable(exc) is unavailable


def test_read_only_transaction_is_unavailable(pg_admin_url):
   with connect(pg_admin_url) as conn:
      conn.execute("SET TRANSACTION READ ONLY")
      with pytest.raises(psycopg.Error) as info:
         conn.execute("CREATE TEMP TABLE nm_ro (a int)")
   assert isinstance(info.value, psycopg.errors.ReadOnlySqlTransaction)
   assert is_unavailable(info.value)


def test_connect_can_set_a_lock_timeout(pg_admin_url):
   with connect(pg_admin_url, lock_timeout="30s") as conn:
      assert conn.execute("SHOW lock_timeout").fetchone()[0] == "30s"


def test_unreachable_server_is_unavailable():
   with pytest.raises(psycopg.Error) as info:
      connect("postgresql://localhost:1/x", connect_timeout_sec=2)
   assert is_unavailable(info.value)


def test_closed_connection_is_unavailable(pg_admin_url):
   conn = connect(pg_admin_url)
   conn.close()
   with pytest.raises(psycopg.OperationalError) as info:
      conn.execute("SELECT 1")
   assert is_unavailable(info.value)


def test_oversized_index_entry_is_not_unavailable(pg_db):
   # Hex of random md5s compresses only about 2:1, so the entry stays far over btree's
   # ~2.7 kB limit after compression.
   with connect(pg_db) as conn:
      conn.execute("CREATE TABLE node_monitor.big (x text)")
      conn.execute("CREATE INDEX ON node_monitor.big (x)")
      with pytest.raises(psycopg.Error) as info:
         conn.execute(
            "INSERT INTO node_monitor.big"
            " SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 400)"
         )
   assert isinstance(info.value, psycopg.errors.ProgramLimitExceeded)
   assert isinstance(info.value, psycopg.OperationalError)
   assert not is_unavailable(info.value)


def test_statement_timeout_is_not_unavailable(pg_admin_url):
   with connect(pg_admin_url) as conn:
      conn.execute("SET statement_timeout = '1ms'")
      with pytest.raises(psycopg.Error) as info:
         conn.execute("SELECT pg_sleep(1)")
   assert isinstance(info.value, psycopg.errors.QueryCanceled)
   assert not is_unavailable(info.value)


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
