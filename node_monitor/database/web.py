"""node_monitor.database.web -- read-only web database engine and
startup preflight.

Design: operational-web-dashboard Task 2.

``WebDatabase`` wraps a single SQLAlchemy engine configured for
SELECT-only access.  Its ``preflight()`` method verifies:

  1. The database is reachable via CONNECT.
  2. The schema_migrations ledger exactly matches the packaged migration
     history (version / name / checksum / transactional).
  3. The reader has SELECT on every required table.
  4. The reader has NO INSERT / UPDATE / DELETE / TRUNCATE / REFERENCES /
     TRIGGER on any required table.
  5. The reader has NO database-level TEMP or CREATE privilege.
  6. The reader has NO schema-level CREATE privilege.

All checks use SELECT-only queries against ``information_schema`` and
``pg_catalog``; no DDL is executed.  ``WebDatabaseError`` messages are
bounded: they contain neither the connection URL, role name, SQL text,
nor driver exception details.

This module does NOT import ``node_monitor.database.migration``; all
shared constants come from ``node_monitor.database.schema_contract``.
"""

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from node_monitor.database.schema_contract import (
   REQUIRED_TABLES,
   expected_migration_rows,
)


class WebDatabaseError(RuntimeError):
   """Bounded, credential-safe web database failure."""


# Privileges that must NOT exist on any required table for the reader.
_FORBIDDEN_TABLE_PRIVILEGES = frozenset({
   "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER",
})


def _require_current_schema(connection):
   """Verify the schema_migrations ledger exactly matches the packaged
   migration history.  Raises ``WebDatabaseError`` on any mismatch,
   including an uninitialised database.
   """
   # Check that the schema and ledger table exist.
   table_exists = connection.execute(text(
      "SELECT to_regclass('node_monitor.schema_migrations') IS NOT NULL"
   )).scalar_one()
   if not table_exists:
      raise WebDatabaseError("web database preflight failed")

   # Read the ledger.
   rows = connection.execute(text(
      "SELECT version, name, checksum, transactional "
      "FROM node_monitor.schema_migrations "
      "ORDER BY version"
   )).mappings().all()

   actual = tuple(
      (int(row["version"]), row["name"], row["checksum"], bool(row["transactional"]))
      for row in rows
   )
   expected = expected_migration_rows()

   if actual != expected:
      raise WebDatabaseError("web database preflight failed")

   # Verify catalog: all REQUIRED_TABLES (excluding schema_migrations) must
   # exist in the node_monitor schema.
   telemetry_tables = [t for t in REQUIRED_TABLES if t != "schema_migrations"]
   existing = frozenset(connection.execute(text(
      "SELECT table_name FROM information_schema.tables "
      "WHERE table_schema = 'node_monitor' AND table_type = 'BASE TABLE'"
   )).scalars().all())
   for table in telemetry_tables:
      if table not in existing:
         raise WebDatabaseError("web database preflight failed")


def _require_reader_privileges(connection):
   """Verify effective privilege grants for the current reader session.

   Required: CONNECT on database, USAGE on schema, SELECT on every table
   in REQUIRED_TABLES.

   Forbidden: database TEMP/CREATE, schema CREATE, table
   INSERT/UPDATE/DELETE/TRUNCATE/REFERENCES/TRIGGER.
   """
   # -----------------------------------------------------------------------
   # Database-level: CONNECT required; TEMP and CREATE forbidden.
   # -----------------------------------------------------------------------
   db_privs = connection.execute(text(
      "SELECT "
      "  has_database_privilege(current_database(), 'CONNECT') AS can_connect, "
      "  has_database_privilege(current_database(), 'TEMP') AS can_temp, "
      "  has_database_privilege(current_database(), 'CREATE') AS can_create"
   )).mappings().one()

   if not db_privs["can_connect"]:
      raise WebDatabaseError("web database preflight failed")
   if db_privs["can_temp"] or db_privs["can_create"]:
      raise WebDatabaseError("web database preflight failed")

   # -----------------------------------------------------------------------
   # Schema-level: USAGE required; CREATE forbidden.
   # -----------------------------------------------------------------------
   schema_privs = connection.execute(text(
      "SELECT "
      "  has_schema_privilege('node_monitor', 'USAGE') AS can_use, "
      "  has_schema_privilege('node_monitor', 'CREATE') AS can_create"
   )).mappings().one()

   if not schema_privs["can_use"]:
      raise WebDatabaseError("web database preflight failed")
   if schema_privs["can_create"]:
      raise WebDatabaseError("web database preflight failed")

   # -----------------------------------------------------------------------
   # Table-level: SELECT required; write/control privileges forbidden.
   # -----------------------------------------------------------------------
   for table in REQUIRED_TABLES:
      qualified = "node_monitor.%s" % table

      # SELECT must be granted.
      can_select = connection.execute(text(
         "SELECT has_table_privilege(:t, 'SELECT')"
      ), {"t": qualified}).scalar_one()
      if not can_select:
         raise WebDatabaseError("web database preflight failed")

      # Each forbidden privilege must NOT be granted.
      for priv in sorted(_FORBIDDEN_TABLE_PRIVILEGES):
         has_priv = connection.execute(text(
            "SELECT has_table_privilege(:t, :p)"
         ), {"t": qualified, "p": priv}).scalar_one()
         if has_priv:
            raise WebDatabaseError("web database preflight failed")


class WebDatabase:
   """Read-only SQLAlchemy engine with bounded startup preflight.

   Pool is fixed at size=1, max_overflow=0: one connection at a time,
   matching the web server's single-reader access pattern.
   """

   def __init__(self, config):
      self._engine = create_engine(
         config.url,
         pool_size=1,
         max_overflow=0,
         pool_timeout=config.pool_timeout_sec,
         pool_recycle=config.pool_recycle_sec,
         pool_pre_ping=config.pool_pre_ping,
         echo=False,
         connect_args=dict(config.connect_args),
      )

   def preflight(self):
      """Run all schema and privilege checks.

      Raises ``WebDatabaseError("web database preflight failed")`` on any
      failure.  No URL, role name, SQL text, or driver exception detail
      appears in the raised message.
      """
      try:
         with self._engine.connect() as connection:
            _require_current_schema(connection)
            _require_reader_privileges(connection)
      except WebDatabaseError:
         raise
      except (SQLAlchemyError, ValueError, RuntimeError, Exception):
         raise WebDatabaseError("web database preflight failed") from None

   def dispose(self):
      """Release all pooled connections."""
      self._engine.dispose()
