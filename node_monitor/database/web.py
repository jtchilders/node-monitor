"""node_monitor.database.web -- read-only web database engine and
startup preflight.

Design: operational-web-dashboard Task 2.

``WebDatabase`` wraps a single SQLAlchemy engine configured for
SELECT-only access.  Its ``preflight()`` method verifies:

  1. The database is reachable via CONNECT.
  2. The schema_migrations ledger exactly matches the packaged migration
     history (version / name / checksum / transactional).
  3. The exact column/type/nullability signatures of every telemetry
     table match the canonical catalog in ``schema_contract``.  A
     database that merely has the right table names but wrong columns is
     rejected as schema drift.
  3b. All required constraint names (``_REQUIRED_SOURCE_CONSTRAINTS``) and
     required index names (``_REQUIRED_SOURCE_INDEXES``) are present in the
     live ``node_monitor`` schema (subset check).
  4. The reader has SELECT on every required table.
  5. The reader has NO INSERT / UPDATE / DELETE / TRUNCATE / REFERENCES /
     TRIGGER on any required table.
  6. The reader has NO database-level TEMP or CREATE privilege.
  7. The reader has NO schema-level CREATE privilege.

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
   _SOURCE_SCHEMA_COLUMNS,
   _REQUIRED_SOURCE_CONSTRAINTS,
   _REQUIRED_SOURCE_INDEXES,
   compare_source_schema,
   compare_source_constraints,
   compare_source_indexes,
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
   migration history, and that every telemetry table has the exact expected
   column/type/nullability signatures.

   Raises ``WebDatabaseError`` on any mismatch, including an uninitialised
   database or schema drift (wrong columns, wrong types, wrong nullability).
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

   # Verify exact column/type/nullability signatures for all telemetry tables.
   # This catches schema drift: tables that exist but have wrong columns.
   col_rows = connection.execute(text(
      "SELECT table_name, column_name, udt_name, is_nullable "
      "FROM information_schema.columns "
      "WHERE table_schema = 'node_monitor' "
      "  AND table_name <> 'schema_migrations' "
      "ORDER BY table_name, ordinal_position"
   )).all()
   actual_cols = {}
   for table, column, udt_name, nullable in col_rows:
      actual_cols.setdefault(table, []).append((column, udt_name, nullable))

   drift = compare_source_schema(actual_cols)
   if drift is not None:
      raise WebDatabaseError("web database preflight failed")

   # Verify required constraint names are present (subset check -- extra
   # constraints from triggers, application code, etc. are tolerated).
   constraint_rows = connection.execute(text(
      "SELECT conname "
      "FROM pg_catalog.pg_constraint c "
      "JOIN pg_catalog.pg_namespace n ON n.oid = c.connamespace "
      "WHERE n.nspname = 'node_monitor'"
   )).all()
   actual_constraints = {row[0] for row in constraint_rows}
   constraint_drift = compare_source_constraints(actual_constraints)
   if constraint_drift is not None:
      raise WebDatabaseError("web database preflight failed")

   # Verify required index names are present (subset check -- extra indexes
   # are tolerated).
   index_rows = connection.execute(text(
      "SELECT indexname "
      "FROM pg_catalog.pg_indexes "
      "WHERE schemaname = 'node_monitor'"
   )).all()
   actual_indexes = {row[0] for row in index_rows}
   index_drift = compare_source_indexes(actual_indexes)
   if index_drift is not None:
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
      except Exception:
         raise WebDatabaseError("web database preflight failed") from None

   def dispose(self):
      """Release all pooled connections."""
      self._engine.dispose()
