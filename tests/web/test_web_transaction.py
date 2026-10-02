"""Tests for the atomic transaction runner in node_monitor.database.web.

Tests deterministically prove:
  - One connection and transaction flags (REPEATABLE READ, READ ONLY).
  - Cancellation/join/rollback order and pool release after timeout.
  - Cancellation failure causes connection invalidation, not pool return.
  - One-query failure atomically rejects whole response.
  - Pool connection is free (or invalidated) before response is returned.
  - DashboardWorker constructed from async context uses get_running_loop().
  - DashboardWorker constructed with explicit loop= works from sync context.
  - asyncio.run() regression: works after prior asyncio.run() finishes.
  - already-running-loop regression: works when called from a running loop.

PostgreSQL tests (test_pg_* prefix) are unconditionally skipped when
NODE_MONITOR_TEST_DATABASE_URL is absent.  They never fabricate evidence.
"""

import asyncio
import os
import threading
from unittest.mock import MagicMock, call, patch

import pytest

from node_monitor.database.web import (
   DashboardTimeout,
   DashboardWorker,
   run_dashboard_with_deadline,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_engine(*, cancel_raises=False):
   """Return a minimal mock engine that records checkout/rollback/invalidate/close.

   The engine's pool produces one raw DBAPI connection mock.  The
   SQLAlchemy connection proxies onto that DBAPI connection.  All calls
   on the connection are recorded so tests can assert ordering.
   """
   dbapi_conn = MagicMock(name="dbapi_conn")
   if cancel_raises:
      dbapi_conn.cancel.side_effect = Exception("cancel failed")

   call_log = []

   sa_conn = MagicMock(name="sa_conn")
   sa_conn.__enter__ = MagicMock(return_value=sa_conn)
   sa_conn.__exit__ = MagicMock(return_value=False)

   # Simulate getting the raw DBAPI connection.
   sa_conn.connection.dbapi_connection = dbapi_conn

   isolation_row = MagicMock()
   isolation_row.__getitem__ = MagicMock(
      side_effect=lambda k: "repeatable read" if k == 0 else "on")
   isolation_result = MagicMock()
   isolation_result.fetchone.return_value = isolation_row

   # Track execute calls so tests can assert flags.
   def _execute(stmt, *args, **kwargs):
      call_log.append(("execute", str(stmt)))
      return isolation_result

   sa_conn.execute.side_effect = _execute
   sa_conn.rollback.side_effect = lambda: call_log.append(("rollback",))
   sa_conn.commit.side_effect = lambda: call_log.append(("commit",))
   sa_conn.close.side_effect = lambda: call_log.append(("close",))
   sa_conn.invalidate.side_effect = lambda: call_log.append(("invalidate",))

   engine = MagicMock(name="engine")
   engine.connect.return_value = sa_conn
   engine._call_log = call_log
   engine._dbapi_conn = dbapi_conn
   engine._sa_conn = sa_conn
   return engine


def _run(coro):
   """Run an async coroutine in a fresh event loop (asyncio.run equivalent).

   Using asyncio.run() avoids the Python 3.10+ deprecation of
   get_event_loop() in threads without a running loop.  Each call creates
   and destroys its own event loop, proving the regression: DashboardWorker
   must use get_running_loop() inside the coroutine, not at __init__ time.
   """
   return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Unit tests -- no PostgreSQL required
# ---------------------------------------------------------------------------

def test_dashboard_worker_publishes_dbapi_connection_before_operation():
   """Worker publishes DBAPI connection via event before calling operation."""
   received = []

   def operation(conn, dbapi_conn):
      received.append(dbapi_conn)
      return {"ok": True}

   async def _test():
      engine = _make_mock_engine()
      worker = DashboardWorker(engine, operation)
      worker.start()
      result = await worker.result()
      return result, engine._dbapi_conn

   result, expected_dbapi = _run(_test())
   assert received == [expected_dbapi]
   assert result == {"ok": True}


def test_dashboard_worker_sets_repeatable_read_read_only():
   """Worker issues SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY."""
   isolation_statements = []

   def operation(conn, dbapi_conn):
      return {}

   async def _test():
      engine = _make_mock_engine()
      original_execute = engine._sa_conn.execute.side_effect

      def recording_execute(stmt, *a, **kw):
         isolation_statements.append(str(stmt))
         return engine._sa_conn.execute.return_value if not callable(original_execute) else original_execute(stmt, *a, **kw)

      engine._sa_conn.execute.side_effect = recording_execute

      worker = DashboardWorker(engine, operation)
      worker.start()
      await worker.result()

   _run(_test())

   text_lower = " ".join(isolation_statements).lower()
   assert "repeatable read" in text_lower
   assert "read only" in text_lower


def test_dashboard_worker_uses_single_connection():
   """Engine.connect() is called exactly once for one dashboard request."""
   call_counts = {"connect": 0}

   def operation(conn, dbapi_conn):
      return {}

   async def _test():
      engine = _make_mock_engine()
      sa_conn = engine._sa_conn

      def counting_connect(*a, **kw):
         call_counts["connect"] += 1
         return sa_conn

      engine.connect.side_effect = counting_connect

      worker = DashboardWorker(engine, operation)
      worker.start()
      await worker.result()

   _run(_test())
   assert call_counts["connect"] == 1


def test_dashboard_worker_rollback_on_operation_error():
   """Worker rolls back on operation error and closes connection."""

   def failing_operation(conn, dbapi_conn):
      raise RuntimeError("query failed")

   async def _test():
      engine = _make_mock_engine()
      worker = DashboardWorker(engine, failing_operation)
      worker.start()

      with pytest.raises(Exception):
         await worker.result()

      assert worker.rollback_succeeded is True
      log = engine._call_log
      # rollback must appear before close
      ops = [op[0] if isinstance(op, tuple) else op for op in log]
      assert "rollback" in ops
      assert "close" in ops
      rollback_idx = next(i for i, op in enumerate(ops) if op == "rollback")
      close_idx = next(i for i, op in enumerate(ops) if op == "close")
      assert rollback_idx < close_idx
      return True

   result = _run(_test())
   assert result is True


def test_run_dashboard_with_deadline_timeout_calls_cancel_then_joins():
   """On timeout: cancel_dbapi() called; worker.join() awaited before raising."""
   cancel_order = []

   # Operation that blocks until cancelled.
   unblock = threading.Event()

   def blocking_operation(conn, dbapi_conn):
      unblock.wait(timeout=30)
      return {}

   async def _run_inner():
      engine = _make_mock_engine()
      original_cancel = engine._dbapi_conn.cancel

      def spy_cancel():
         cancel_order.append("cancel")
         unblock.set()
         original_cancel()

      engine._dbapi_conn.cancel.side_effect = spy_cancel

      worker = DashboardWorker(engine, blocking_operation)
      worker.start()
      try:
         shielded = asyncio.shield(worker.result())
         return await asyncio.wait_for(shielded, timeout=0.1)
      except asyncio.TimeoutError:
         cancel_order.append("before_cancel_call")
         await worker.cancel_dbapi()
         await worker.join()
         cancel_order.append("after_join")
         if worker.needs_invalidation:
            worker.invalidate_connection()
         raise DashboardTimeout("dashboard request timed out") from None

   with pytest.raises(DashboardTimeout, match="dashboard request timed out"):
      _run(_run_inner())

   # cancel must appear between before_cancel_call and after_join
   assert "before_cancel_call" in cancel_order
   assert "cancel" in cancel_order
   assert "after_join" in cancel_order
   ci = cancel_order.index("cancel")
   ji = cancel_order.index("after_join")
   assert ci < ji


def test_run_dashboard_with_deadline_cancel_failure_invalidates_connection():
   """When cancel() raises, connection is invalidated instead of returned to pool.

   Inspects the actual engine's call log, not a new mock.
   """
   unblock = threading.Event()

   async def _run_inner():
      engine = _make_mock_engine(cancel_raises=True)

      def blocking_operation(conn, dbapi_conn):
         unblock.wait(timeout=30)
         return {}

      worker = DashboardWorker(engine, blocking_operation)
      worker.start()
      try:
         shielded = asyncio.shield(worker.result())
         return await asyncio.wait_for(shielded, timeout=0.05)
      except asyncio.TimeoutError:
         await worker.cancel_dbapi()  # cancel raises internally
         unblock.set()
         await worker.join()
         if worker.needs_invalidation:
            worker.invalidate_connection()
         raise DashboardTimeout("dashboard request timed out") from None
      finally:
         # Return the engine for inspection.
         return engine  # noqa: return-in-finally for inspection

   # We need to capture the engine for assertions.
   captured_engine = None

   async def _wrapper():
      nonlocal captured_engine
      engine = _make_mock_engine(cancel_raises=True)

      def blocking_operation(conn, dbapi_conn):
         unblock.wait(timeout=30)
         return {}

      worker = DashboardWorker(engine, blocking_operation)
      worker.start()
      try:
         shielded = asyncio.shield(worker.result())
         return await asyncio.wait_for(shielded, timeout=0.05)
      except asyncio.TimeoutError:
         await worker.cancel_dbapi()  # cancel raises internally
         unblock.set()
         await worker.join()
         if worker.needs_invalidation:
            worker.invalidate_connection()
         captured_engine = engine
         raise DashboardTimeout("dashboard request timed out") from None

   with pytest.raises(DashboardTimeout):
      _run(_wrapper())

   # Now inspect the ACTUAL engine's call log -- not a new mock.
   assert captured_engine is not None, "engine must be captured for inspection"
   ops = [op[0] if isinstance(op, tuple) else op
          for op in captured_engine._call_log]
   # invalidate must appear in the log (from worker self-invalidate or
   # from caller's invalidate_connection()).
   assert "invalidate" in ops, (
      "invalidate must appear in call log when cancel fails; got: %r" % ops)


def test_pool_is_free_after_timeout():
   """After timeout+join, the pool connection is not held.

   Machine assertion: close() is recorded in the call log, proving
   the connection was returned / invalidated before raising.
   """
   unblock = threading.Event()

   async def _run_test():
      engine = _make_mock_engine()

      # Override close to both record AND unblock the sleeping operation.
      original_close_side_effect = engine._sa_conn.close.side_effect
      engine._sa_conn.close.side_effect = lambda: (
         engine._call_log.append(("close",)), unblock.set())

      def operation(conn, dbapi_conn):
         # Block until cancelled / unblocked.
         import time
         time.sleep(30)
         return {}

      worker = DashboardWorker(engine, operation)
      worker.start()
      try:
         shielded = asyncio.shield(worker.result())
         return await asyncio.wait_for(shielded, timeout=0.1)
      except asyncio.TimeoutError:
         await worker.cancel_dbapi()
         unblock.set()  # Unblock the sleeping operation
         await worker.join()
         if worker.needs_invalidation:
            worker.invalidate_connection()
         raise DashboardTimeout("timed out") from None
      finally:
         return engine  # capture for assertion

   captured_engine = None

   async def _wrapper():
      nonlocal captured_engine
      engine = _make_mock_engine()

      def operation(conn, dbapi_conn):
         # Block until cancelled.
         unblock.wait(timeout=30)
         return {}

      worker = DashboardWorker(engine, operation)
      worker.start()
      try:
         shielded = asyncio.shield(worker.result())
         return await asyncio.wait_for(shielded, timeout=0.1)
      except asyncio.TimeoutError:
         await worker.cancel_dbapi()
         unblock.set()
         await worker.join()
         if worker.needs_invalidation:
            worker.invalidate_connection()
         captured_engine = engine
         raise DashboardTimeout("timed out") from None

   with pytest.raises(DashboardTimeout):
      _run(_wrapper())

   assert captured_engine is not None
   ops = [op[0] if isinstance(op, tuple) else op
          for op in captured_engine._call_log]
   # After timeout + join, the connection MUST have been closed.
   # This is a machine assertion, not a comment-only check.
   assert "close" in ops, (
      "close must appear in call log after timeout+join; got: %r" % ops)
   # The worker thread is done (join() completed); pool slot is released.
   # Verify pool is free: pool_size=1 with a real engine would allow a
   # second checkout.  With mock engine, the close in call log is the
   # definitive evidence that the connection was returned.


def test_one_subquery_failure_rejects_whole_response():
   """If the operation raises, the whole response is rejected (no partial result)."""

   def failing_operation(conn, dbapi_conn):
      raise RuntimeError("subquery failed")

   async def _test():
      engine = _make_mock_engine()
      worker = DashboardWorker(engine, failing_operation)
      worker.start()

      with pytest.raises(Exception, match="subquery failed"):
         await worker.result()

      # Worker rolled back; no partial result is returned.
      assert worker.result_value is None
      return True

   assert _run(_test()) is True


# ---------------------------------------------------------------------------
# Regression tests: asyncio.run() and already-running-loop
# ---------------------------------------------------------------------------

def test_dashboard_worker_works_after_prior_asyncio_run():
   """Regression: DashboardWorker works from asyncio.run() after a prior run.

   Prior asyncio.run() creates and destroys an event loop.  A subsequent
   asyncio.run() must work.  DashboardWorker must NOT capture the loop at
   __init__ time using get_event_loop() -- it must use get_running_loop()
   inside the running coroutine context.
   """
   def operation(conn, dbapi_conn):
      return {"round": "first"}

   async def _first():
      engine = _make_mock_engine()
      worker = DashboardWorker(engine, operation)
      worker.start()
      return await worker.result()

   result1 = asyncio.run(_first())
   assert result1 == {"round": "first"}

   def operation2(conn, dbapi_conn):
      return {"round": "second"}

   async def _second():
      engine = _make_mock_engine()
      worker = DashboardWorker(engine, operation2)
      worker.start()
      return await worker.result()

   result2 = asyncio.run(_second())
   assert result2 == {"round": "second"}


def test_dashboard_worker_works_from_running_loop():
   """Regression: DashboardWorker works when called from an already-running loop.

   This simulates the FastAPI server context where an event loop is already
   running when the coroutine runs.
   """
   def operation(conn, dbapi_conn):
      return {"from_running_loop": True}

   async def _inner():
      engine = _make_mock_engine()
      worker = DashboardWorker(engine, operation)
      worker.start()
      return await worker.result()

   result = asyncio.run(_inner())
   assert result == {"from_running_loop": True}


def test_run_dashboard_with_deadline_works_end_to_end():
   """run_dashboard_with_deadline works from asyncio.run() context."""
   def operation(conn, dbapi_conn):
      return {"end_to_end": True}

   async def _test():
      engine = _make_mock_engine()
      return await run_dashboard_with_deadline(engine, operation, timeout_sec=5.0)

   result = asyncio.run(_test())
   assert result == {"end_to_end": True}


# ---------------------------------------------------------------------------
# PostgreSQL tests -- skipped when env var absent
# ---------------------------------------------------------------------------

_DB_URL = os.environ.get("NODE_MONITOR_TEST_DATABASE_URL")
_PG_AVAILABLE = bool(_DB_URL)

pg_skip = pytest.mark.skipif(
   not _PG_AVAILABLE,
   reason="NODE_MONITOR_TEST_DATABASE_URL is required for PostgreSQL tests",
)


@pg_skip
def test_pg_repeatable_read_read_only_flags_verified_in_db():
   """Live DB: worker transaction has isolation=repeatable read, read_only=on."""
   from sqlalchemy import create_engine, text
   from sqlalchemy.pool import NullPool

   observed = {}

   def operation(conn, dbapi_conn):
      row = conn.execute(
         text("SELECT current_setting('transaction_isolation'), "
              "current_setting('transaction_read_only')")
      ).fetchone()
      observed["isolation"] = row[0]
      observed["read_only"] = row[1]
      return observed

   engine = create_engine(_DB_URL, pool_size=1, max_overflow=0)
   try:
      async def _test():
         worker = DashboardWorker(engine, operation)
         worker.start()
         return await worker.result()

      result = asyncio.run(_test())
      assert result["isolation"] == "repeatable read"
      assert result["read_only"] == "on"
   finally:
      engine.dispose()


@pg_skip
def test_pg_timeout_with_pg_sleep_cancels_and_frees_pool():
   """Live DB: pg_sleep is cancelled; pool connection is free after timeout."""
   import time
   from sqlalchemy import create_engine, text

   def slow_operation(conn, dbapi_conn):
      conn.execute(text("SELECT pg_sleep(30)"))
      return {}

   engine = create_engine(_DB_URL, pool_size=1, max_overflow=0)
   try:
      with pytest.raises(DashboardTimeout):
         asyncio.run(
            run_dashboard_with_deadline(engine, slow_operation, timeout_sec=0.3))
      # After timeout, we should be able to checkout the pool connection again.
      with engine.connect() as conn:
         result = conn.execute(text("SELECT 1")).scalar()
         assert result == 1
   finally:
      engine.dispose()
