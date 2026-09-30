"""Tests for node_monitor.database.connection -- the injected, PostgreSQL-
only connection factory.

Design: node_monitor_planning tiered-storage-retention-design.md
"Database section" ("explicitly injected, not a mutable global
singleton"; "must not execute CREATE SCHEMA, create_all, drop_all,
VACUUM, or any server lifecycle operation"). Plan:
increment1-nested-config-pg-connection.md Task 4.

Every test here patches ``sqlalchemy.create_engine`` and captures the
registered ``connect`` event listener -- these are RUNTIME tests that
prove exactly what NodeMonitorDB calls, never a source-text scan for
forbidden DDL strings.
"""

from unittest import mock

import pytest
import sqlalchemy

from node_monitor.config import DatabaseConfig
from node_monitor.database.connection import NodeMonitorDB


def _database_config(**overrides):
   values = dict(
      url="postgresql://localhost/pbs_monitor_dev",
      schema="node_monitor",
      pool_size=1,
      max_overflow=0,
      echo_sql=False,
      pool_pre_ping=True,
      pool_timeout_sec=10,
      pool_recycle_sec=3600,
      connect_args=(
         ("connect_timeout", 10),
         ("keepalives", 1),
         ("keepalives_count", 3),
         ("keepalives_idle", 30),
         ("keepalives_interval", 10),
         ("options", "-c statement_timeout=15000 -c lock_timeout=5000"),
      ),
   )
   values.update(overrides)
   return DatabaseConfig(**values)


# --------------------------------------------------------------------------
# Requirement 1: exact engine arguments, including copied connect_args.
# --------------------------------------------------------------------------

class TestEngineConstruction:
   def test_create_engine_called_with_exact_arguments(self):
      config = _database_config()
      with mock.patch(
         "node_monitor.database.connection.create_engine"
      ) as mock_create_engine, mock.patch(
         "node_monitor.database.connection.event"
      ):
         mock_create_engine.return_value = mock.Mock()
         NodeMonitorDB(config)
      assert mock_create_engine.call_count == 1
      args, kwargs = mock_create_engine.call_args
      assert args == (config.url,)
      assert kwargs["pool_size"] == 1
      assert kwargs["max_overflow"] == 0
      assert kwargs["echo"] is False
      assert kwargs["pool_pre_ping"] is True
      assert kwargs["pool_timeout"] == 10
      assert kwargs["pool_recycle"] == 3600
      assert kwargs["connect_args"] == dict(config.connect_args)

   def test_connect_args_dict_is_a_copy_not_the_stored_tuple(self):
      config = _database_config()
      with mock.patch(
         "node_monitor.database.connection.create_engine"
      ) as mock_create_engine, mock.patch(
         "node_monitor.database.connection.event"
      ):
         mock_create_engine.return_value = mock.Mock()
         NodeMonitorDB(config)
      _, kwargs = mock_create_engine.call_args
      passed_connect_args = kwargs["connect_args"]
      passed_connect_args["mutated"] = True
      assert "mutated" not in dict(config.connect_args)


# --------------------------------------------------------------------------
# Requirement 2: construction registers one listener but never connects or
# executes SQL.
# --------------------------------------------------------------------------

class TestConstructionDoesNotConnect:
   def test_construction_registers_exactly_one_listener_and_no_connection(self):
      config = _database_config()
      mock_engine = mock.Mock()
      with mock.patch(
         "node_monitor.database.connection.create_engine",
         return_value=mock_engine,
      ), mock.patch(
         "node_monitor.database.connection.event"
      ) as mock_event:
         NodeMonitorDB(config)
      assert mock_event.listen.call_count == 1
      listen_args = mock_event.listen.call_args[0]
      assert listen_args[0] is mock_engine
      assert listen_args[1] == "connect"
      # The engine itself must never be asked to connect/execute during
      # construction -- create_engine() alone is lazy and opens nothing.
      mock_engine.connect.assert_not_called()
      mock_engine.execute.assert_not_called() if hasattr(
         mock_engine, "execute") else None


# --------------------------------------------------------------------------
# Requirement 3: calling the listener executes exactly literal
# SET search_path TO "node_monitor", closes its cursor, commits nothing.
# --------------------------------------------------------------------------

class TestSearchPathListener:
   def test_listener_executes_literal_search_path_and_closes_cursor(self):
      config = _database_config()
      with mock.patch(
         "node_monitor.database.connection.create_engine"
      ) as mock_create_engine, mock.patch(
         "node_monitor.database.connection.event"
      ) as mock_event:
         mock_create_engine.return_value = mock.Mock()
         NodeMonitorDB(config)
      listener = mock_event.listen.call_args[0][2]

      mock_dbapi_connection = mock.Mock()
      mock_cursor = mock.Mock()
      mock_dbapi_connection.cursor.return_value = mock_cursor

      listener(mock_dbapi_connection, mock.Mock())

      mock_cursor.execute.assert_called_once_with(
         'SET search_path TO "node_monitor"')
      mock_cursor.close.assert_called_once()
      mock_dbapi_connection.commit.assert_not_called()


# --------------------------------------------------------------------------
# Requirement 4: ping() executes text("SELECT 1"); True on success, False
# on SQLAlchemy/DBAPI failure without exposing connection details.
# --------------------------------------------------------------------------

class TestPing:
   def test_ping_success_returns_true(self):
      config = _database_config()
      mock_engine = mock.MagicMock()
      mock_connection = mock.MagicMock()
      mock_engine.connect.return_value.__enter__.return_value = mock_connection
      with mock.patch(
         "node_monitor.database.connection.create_engine",
         return_value=mock_engine,
      ), mock.patch(
         "node_monitor.database.connection.event"
      ):
         db = NodeMonitorDB(config)
      assert db.ping() is True
      executed_args = mock_connection.execute.call_args[0]
      assert str(executed_args[0]) == "SELECT 1"

   def test_ping_failure_returns_false_without_raising(self):
      config = _database_config()
      mock_engine = mock.Mock()
      mock_engine.connect.side_effect = sqlalchemy.exc.OperationalError(
         "SELECT 1", {}, Exception("connection refused"))
      with mock.patch(
         "node_monitor.database.connection.create_engine",
         return_value=mock_engine,
      ), mock.patch(
         "node_monitor.database.connection.event"
      ):
         db = NodeMonitorDB(config)
      assert db.ping() is False


# --------------------------------------------------------------------------
# Requirement 5: close() disposes the engine.
# --------------------------------------------------------------------------

class TestClose:
   def test_close_disposes_engine(self):
      config = _database_config()
      mock_engine = mock.Mock()
      with mock.patch(
         "node_monitor.database.connection.create_engine",
         return_value=mock_engine,
      ), mock.patch(
         "node_monitor.database.connection.event"
      ):
         db = NodeMonitorDB(config)
      db.close()
      mock_engine.dispose.assert_called_once()

   def test_begin_exposes_injected_engine_transaction_boundary(self):
      config = _database_config()
      mock_engine = mock.Mock()
      transaction = mock_engine.begin.return_value
      with mock.patch(
         "node_monitor.database.connection.create_engine",
         return_value=mock_engine,
      ), mock.patch(
         "node_monitor.database.connection.event"
      ):
         db = NodeMonitorDB(config)
      assert db.begin() is transaction
      mock_engine.begin.assert_called_once_with()


# --------------------------------------------------------------------------
# Requirement 6: no module-global singleton or implicit config/environment
# lookup exists.
# --------------------------------------------------------------------------

class TestNoGlobalSingleton:
   def test_module_has_no_global_instance_attribute(self):
      import node_monitor.database.connection as connection_module
      for name in dir(connection_module):
         if name.startswith("_"):
            continue
         value = getattr(connection_module, name)
         assert not isinstance(value, NodeMonitorDB), (
            "found a module-level NodeMonitorDB singleton: %r" % (name,))

   def test_constructor_requires_explicit_config_argument(self):
      import inspect
      sig = inspect.signature(NodeMonitorDB.__init__)
      params = list(sig.parameters.values())
      # self, config -- no defaulted/optional config, no environment lookup.
      assert len(params) == 2
      assert params[1].name == "config"
      assert params[1].default is inspect.Parameter.empty


# --------------------------------------------------------------------------
# Requirements 7-9: mask_url.
# --------------------------------------------------------------------------

class TestMaskUrl:
   def test_masking_a_real_sentinel_password_removes_it(self):
      masked = NodeMonitorDB.mask_url(
         "postgresql://user:supersecret@localhost/db")
      assert "supersecret" not in masked
      assert "***" in masked

   def test_percent_encoded_credentials_handled_via_make_url(self):
      masked = NodeMonitorDB.mask_url(
         "postgresql://user:sup%40ersecret@localhost/db")
      assert "supersecret" not in masked
      assert "sup%40ersecret" not in masked
      assert "***" in masked

   def test_postgresql_psycopg2_driver_url_handled(self):
      masked = NodeMonitorDB.mask_url(
         "postgresql+psycopg2://user:supersecret@localhost:5432/db")
      assert "supersecret" not in masked
      assert masked.startswith("postgresql+psycopg2://")

   def test_uses_sqlalchemy_render_as_string_not_a_regex(self):
      # Prove behavior via SQLAlchemy's own contract rather than a
      # regex-shaped assertion: render_as_string(hide_password=True)
      # keeps username, host, port, and db name, masking only the
      # password segment with the literal "***" sentinel SQLAlchemy uses.
      url = "postgresql://alice:supersecret@db.example.org:5433/mydb"
      masked = NodeMonitorDB.mask_url(url)
      expected = sqlalchemy.engine.make_url(url).render_as_string(
         hide_password=True)
      assert masked == expected

   def test_passwordless_url_remains_semantically_unchanged(self):
      url = "postgresql://localhost/db"
      masked = NodeMonitorDB.mask_url(url)
      assert masked == url
