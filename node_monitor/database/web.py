"""node_monitor.database.web -- read-only web database engine,
startup preflight, and atomic dashboard transaction runner.

Design: operational-web-dashboard Tasks 2 and 5.

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

Task 5 additions -- ``DashboardWorker`` and ``run_dashboard_with_deadline``:

  * One dedicated worker thread per dashboard request.
  * Worker alone checks out the SQLAlchemy connection, begins
    REPEATABLE READ READ ONLY, runs all operation subqueries on the
    same connection/snapshot, and commits/rolls-back/closes.
  * Before executing statements the worker publishes the raw DBAPI
    connection through an event-synchronized handoff so the controlling
    event-loop thread can call psycopg2 ``cancel()`` on the 12-second
    outer timeout without touching cursor state.
  * Controller awaits worker termination (``join``) before returning;
    no HTTP error is returned while the worker thread is still alive.
  * Rollback is attempted before pool release; if rollback fails the
    connection is invalidated so it is never returned to the pool.
  * Cleanup is idempotent under timeout / query-error / client-
    cancellation races (cancel_dbapi and invalidate_connection are safe
    to call more than once).

This module does NOT import ``node_monitor.database.migration``; all
shared constants come from ``node_monitor.database.schema_contract``.
"""

import asyncio
import threading

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


# ---------------------------------------------------------------------------
# Task 5 exceptions
# ---------------------------------------------------------------------------

class DashboardTimeout(RuntimeError):
   """Raised by run_dashboard_with_deadline when the 12-second wall-clock
   deadline expires before the worker thread completes its transaction."""


# ---------------------------------------------------------------------------
# Task 2 exception
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Task 5: DashboardWorker -- one dedicated thread per dashboard request
# ---------------------------------------------------------------------------

class DashboardWorker:
   """Executes one dashboard database operation in a dedicated thread.

   The worker:
     1. Checks out a SQLAlchemy connection from the engine pool.
     2. Publishes the raw DBAPI connection via ``_dbapi_ready`` event
        so the controlling event-loop thread can call psycopg2 cancel()
        without touching cursor state.
     3. Begins REPEATABLE READ READ ONLY transaction.
     4. Calls ``operation(conn, dbapi_conn)`` with the SQLAlchemy connection
        and the raw DBAPI connection.
     5. On success: commits, closes connection.
     6. On any exception: rolls back (recording rollback_succeeded), closes
        connection, stores the exception for re-raising via result().

   The controlling thread must:
     - Await ``result()`` with a deadline via asyncio.wait_for().
     - On TimeoutError: call cancel_dbapi(), await join(), then check
       rollback_succeeded to decide whether to call invalidate_connection().
     - Never return an HTTP response while the worker thread is still alive.

   All cleanup methods (cancel_dbapi, invalidate_connection) are idempotent:
   safe to call multiple times under concurrent timeout/error/cancellation
   races.
   """

   def __init__(self, engine, operation):
      self._engine = engine
      self._operation = operation
      self._thread = None
      self._loop = asyncio.get_event_loop()

      # Synchronization: event published by worker before executing statements.
      self._dbapi_ready = threading.Event()
      self._dbapi_conn = None   # raw psycopg2 connection; set before event fires
      self._sa_conn = None      # SQLAlchemy connection handle

      # Result / error (written by worker thread, read by result()).
      self.result_value = None
      self._exception = None

      # Cleanup state flags (idempotent; protected by _cleanup_lock).
      self._cleanup_lock = threading.Lock()
      self._cancel_attempted = False
      self._invalidate_attempted = False

      # Public: True iff rollback completed without raising.
      self.rollback_succeeded = False

      # asyncio.Future resolved by the worker thread on completion.
      self._future = self._loop.create_future()

   # ------------------------------------------------------------------
   # Thread entry point
   # ------------------------------------------------------------------

   def _run(self):
      """Worker thread body.  Checks out connection, runs operation, cleans up."""
      sa_conn = None
      try:
         sa_conn = self._engine.connect()
         self._sa_conn = sa_conn

         # Obtain the raw DBAPI connection and publish it before any SQL.
         dbapi_conn = sa_conn.connection.dbapi_connection
         self._dbapi_conn = dbapi_conn
         self._dbapi_ready.set()

         # Begin REPEATABLE READ READ ONLY transaction.
         sa_conn.execute(
            text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))

         # Execute the caller-supplied operation.
         result = self._operation(sa_conn, dbapi_conn)
         self.result_value = result

         # Commit and close cleanly.
         sa_conn.commit()
         sa_conn.close()
         sa_conn = None
         self._sa_conn = None

         # Resolve the future on the event loop.
         self._loop.call_soon_threadsafe(self._future.set_result, result)

      except BaseException as exc:
         # Attempt rollback before closing.
         if sa_conn is not None:
            try:
               sa_conn.rollback()
               self.rollback_succeeded = True
            except Exception:
               self.rollback_succeeded = False
            try:
               sa_conn.close()
            except Exception:
               pass
            self._sa_conn = None

         # If DBAPI ready event was not set, set it now so cancel_dbapi()
         # doesn't block forever on the event.
         if not self._dbapi_ready.is_set():
            self._dbapi_ready.set()

         self._exception = exc
         self._loop.call_soon_threadsafe(
            self._future.set_exception,
            exc if isinstance(exc, Exception) else RuntimeError(str(exc)),
         )

   def start(self):
      """Start the worker thread (non-blocking)."""
      self._thread = threading.Thread(target=self._run, daemon=True)
      self._thread.start()

   async def result(self):
      """Await the worker's result.  Re-raises any exception from the worker."""
      return await self._future

   async def join(self):
      """Await worker thread termination (non-blocking for event loop)."""
      if self._thread is not None:
         await asyncio.get_event_loop().run_in_executor(
            None, self._thread.join)

   def cancel_dbapi(self):
      """Call psycopg2 cancel() on the raw DBAPI connection.

      Waits up to 5 seconds for the worker to publish the DBAPI connection
      before attempting cancel.  Idempotent; swallows exceptions so the
      caller can always proceed to join().
      """
      with self._cleanup_lock:
         if self._cancel_attempted:
            return
         self._cancel_attempted = True

      # Wait for worker to publish the DBAPI connection (normally immediate).
      self._dbapi_ready.wait(timeout=5.0)
      dbapi_conn = self._dbapi_conn
      if dbapi_conn is not None:
         try:
            dbapi_conn.cancel()
         except Exception:
            # cancel() raised -- rollback will likely fail too; the caller
            # should check rollback_succeeded and invalidate if needed.
            pass

   def invalidate_connection(self):
      """Invalidate the SQLAlchemy connection so it is not returned to the pool.

      Must be called only after join() to avoid racing the worker.
      Idempotent.
      """
      with self._cleanup_lock:
         if self._invalidate_attempted:
            return
         self._invalidate_attempted = True

      sa_conn = self._sa_conn
      if sa_conn is not None:
         try:
            sa_conn.invalidate()
         except Exception:
            pass
         try:
            sa_conn.close()
         except Exception:
            pass
         self._sa_conn = None


# ---------------------------------------------------------------------------
# Top-level coroutine: run operation with 12-second outer deadline
# ---------------------------------------------------------------------------

async def run_dashboard_with_deadline(engine, operation, timeout_sec=12.0):
   """Run ``operation`` in a dedicated worker thread with a wall-clock deadline.

   Parameters
   ----------
   engine : SQLAlchemy engine with pool_size=1, max_overflow=0.
   operation : callable(conn, dbapi_conn) -> result
       The dashboard database operation.  Called in a worker thread inside
       a REPEATABLE READ READ ONLY transaction on a single connection.
   timeout_sec : float (default 12.0)
       Wall-clock deadline in seconds.

   Returns
   -------
   The value returned by ``operation``.

   Raises
   ------
   DashboardTimeout
       If the worker does not complete within ``timeout_sec`` seconds.
       The worker thread is cancelled (via psycopg2 cancel), joined to
       completion, and the connection is invalidated if rollback failed --
       all before this exception propagates.
   Any exception raised by ``operation``
       Propagated after rollback and connection close by the worker.
   """
   worker = DashboardWorker(engine, operation)
   worker.start()
   try:
      return await asyncio.wait_for(worker.result(), timeout=timeout_sec)
   except asyncio.TimeoutError:
      worker.cancel_dbapi()
      await worker.join()
      if not worker.rollback_succeeded:
         worker.invalidate_connection()
      raise DashboardTimeout("dashboard request timed out") from None
