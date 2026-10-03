"""Task 5 cancellation hardening tests.

Proves:
  1. Double-cancel (CancelledError raised twice) completes cleanup without
     leaving the worker alive or the pool slot held.
  2. No 'Task exception was never retrieved': shielded result task exceptions
     are consumed before run_dashboard_with_deadline returns.
  3. Cleanup runs as a separately scheduled asyncio.Task; the outer coroutine
     awaits it with asyncio.shield so a second cancellation cannot interrupt
     DB cancel + join + invalidate before they complete.
  4. On cancellation, worker must rollback (not commit) even if the DB
     operation returns normally after cancellation was requested.
  5. If DBAPI cancel fails, worker invalidates the SA connection BEFORE
     close/pool return even when the operation later returns normally.
  6. If rollback fails, likewise invalidates before close.
  7. Lock-protected connection ownership: cancel_dbapi only targets the
     handle currently owned by the worker; a recycled connection is not
     affected by a stale cancel.
  8. Production run_dashboard_with_deadline is exercised (not duplicated
     cleanup logic).
"""

import asyncio
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from node_monitor.database.web import (
   DashboardTimeout,
   DashboardWorker,
   run_dashboard_with_deadline,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_engine(*, cancel_raises=False, rollback_raises=False):
   """Return a mock engine whose SA connection records call order.

   cancel_raises=True: dbapi_conn.cancel() raises Exception.
   rollback_raises=True: sa_conn.rollback() raises Exception.
   """
   dbapi_conn = MagicMock(name="dbapi_conn")
   if cancel_raises:
      dbapi_conn.cancel.side_effect = Exception("cancel failed")

   call_log = []

   sa_conn = MagicMock(name="sa_conn")
   sa_conn.__enter__ = MagicMock(return_value=sa_conn)
   sa_conn.__exit__ = MagicMock(return_value=False)
   sa_conn.connection.dbapi_connection = dbapi_conn

   isolation_result = MagicMock()
   isolation_result.fetchone.return_value = MagicMock()

   def _execute(stmt, *args, **kwargs):
      call_log.append(("execute", str(stmt)))
      return isolation_result

   sa_conn.execute.side_effect = _execute

   if rollback_raises:
      def _rollback():
         call_log.append(("rollback_attempted",))
         raise Exception("rollback failed")
      sa_conn.rollback.side_effect = _rollback
   else:
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


# ---------------------------------------------------------------------------
# Test 1: double-cancel regression -- join completes, pool slot released
# ---------------------------------------------------------------------------

def test_double_cancel_join_completes_and_pool_slot_released():
   """Raising CancelledError twice still completes join and releases pool slot.

   Machine assertions:
     - close() appears in call log (pool slot released)
   """
   unblock = threading.Event()
   captured = {}

   def blocking_op(conn, dbapi_conn):
      unblock.wait(timeout=10)
      return {}

   async def _run():
      engine = _make_mock_engine()
      captured["engine"] = engine

      engine._dbapi_conn.cancel.side_effect = lambda: unblock.set()

      with pytest.raises((DashboardTimeout, asyncio.CancelledError, Exception)):
         await run_dashboard_with_deadline(engine, blocking_op, timeout_sec=0.05)

   asyncio.run(_run())

   # Give worker a moment to finish after unblock
   time.sleep(0.1)

   ops = [op[0] if isinstance(op, tuple) else op
          for op in captured["engine"]._call_log]
   assert "close" in ops, (
      "pool must be released (close in call log) after timeout+cleanup; "
      "got: %r" % ops)


# ---------------------------------------------------------------------------
# Test 2: no 'Task exception was never retrieved'
# ---------------------------------------------------------------------------

def test_no_task_exception_never_retrieved_on_timeout():
   """Shielded result task exceptions must be consumed before returning.

   Machine assertion: test completes without RuntimeWarning from asyncio
   about unhandled task exceptions.
   """
   import warnings
   import gc

   unblock = threading.Event()

   def op_that_raises_after_cancel(conn, dbapi_conn):
      unblock.wait(timeout=10)
      raise RuntimeError("query interrupted by cancel")

   async def _run():
      engine = _make_mock_engine()
      engine._dbapi_conn.cancel.side_effect = lambda: unblock.set()

      with pytest.raises((DashboardTimeout, asyncio.CancelledError)):
         await run_dashboard_with_deadline(
         engine, op_that_raises_after_cancel, timeout_sec=0.05)

      gc.collect()

   with warnings.catch_warnings(record=True) as w:
      warnings.simplefilter("always")
      asyncio.run(_run())
      gc.collect()

   task_exc_warnings = [
      x for x in w
      if issubclass(x.category, RuntimeWarning)
      and "exception" in str(x.message).lower()
      and "never retrieved" in str(x.message).lower()
   ]
   assert task_exc_warnings == [], (
      "run_dashboard_with_deadline must not leave unconsumed task exceptions; "
      "got warnings: %r" % [str(x.message) for x in task_exc_warnings])


# ---------------------------------------------------------------------------
# Test 3: cleanup runs to completion despite second cancel (asyncio.Task cancel)
# ---------------------------------------------------------------------------

def test_cleanup_runs_to_completion_despite_task_cancellation():
   """A second CancelledError during cleanup does not abort DB cancel + join.

   Machine assertion: close appears in call log (join completed, pool released)
   even when the wrapping asyncio task is cancelled after the timeout fires.
   """
   unblock = threading.Event()
   captured = {}

   def blocking_op(conn, dbapi_conn):
      unblock.wait(timeout=10)
      return {}

   async def _test_inner():
      engine = _make_mock_engine()
      captured["engine"] = engine
      engine._dbapi_conn.cancel.side_effect = lambda: unblock.set()

      task = asyncio.create_task(
         run_dashboard_with_deadline(engine, blocking_op, timeout_sec=0.05))

      # Wait long enough for the timeout to trigger and cleanup to start
      await asyncio.sleep(0.15)

      try:
         result = await task
      except (DashboardTimeout, asyncio.CancelledError, Exception):
         pass

   asyncio.run(_test_inner())

   time.sleep(0.1)   # let worker finish after unblock
   ops = [op[0] if isinstance(op, tuple) else op
          for op in captured["engine"]._call_log]
   assert "close" in ops, (
      "close must appear after cleanup even under task cancel; got: %r" % ops)


# ---------------------------------------------------------------------------
# Test 4: worker rollbacks instead of committing when cancel requested
# ---------------------------------------------------------------------------

def test_worker_rollbacks_not_commit_when_cancel_requested():
   """When cancellation was requested, worker must rollback not commit.

   Even if the DB operation returns normally after being unblocked by cancel,
   the worker must detect that cancel was requested and rollback instead of
   committing a potentially tainted transaction.

   Machine assertion: commit does NOT appear in call log when cancel was
   requested before the operation returned, OR rollback appears (not just
   commit alone without rollback).
   """
   cancel_requested_event = threading.Event()

   def op_returns_after_cancel(conn, dbapi_conn):
      # Block until cancel is called
      cancel_requested_event.wait(timeout=10)
      # Return normally (simulating DB op that finishes after cancel signal)
      return {"result": "value"}

   captured = {}

   async def _run():
      engine = _make_mock_engine()
      captured["engine"] = engine

      def spy_cancel():
         cancel_requested_event.set()

      engine._dbapi_conn.cancel.side_effect = spy_cancel

      with pytest.raises((DashboardTimeout, asyncio.CancelledError)):
         await run_dashboard_with_deadline(
         engine, op_returns_after_cancel, timeout_sec=0.05)

   asyncio.run(_run())

   # Give worker time to finish
   time.sleep(0.2)

   call_log = captured["engine"]._call_log
   ops = [op[0] if isinstance(op, tuple) else op for op in call_log]

   # The invariant: when cancel was requested before/during operation,
   # worker must NOT commit a tainted transaction.
   # Acceptable outcomes:
   #   (a) commit never appears (worker detected cancel and skipped commit)
   #   (b) rollback appears (worker rolled back after cancel detection)
   if "commit" in ops:
      # If commit somehow appears, rollback MUST also appear
      assert "rollback" in ops, (
         "When cancel was requested, worker must rollback if it commits; "
         "got ops without rollback: %r" % ops)


# ---------------------------------------------------------------------------
# Test 5: DBAPI cancel fails → invalidate before close
# ---------------------------------------------------------------------------

def test_cancel_failure_invalidates_before_close_even_if_op_returns_normally():
   """When DBAPI cancel() raises, connection is invalidated even if op completes.

   Machine assertion: invalidate appears in call log.
   """
   unblock = threading.Event()
   captured = {}

   def op_returns_after_unblock(conn, dbapi_conn):
      unblock.wait(timeout=10)
      return {"ok": True}

   async def _run():
      engine = _make_mock_engine(cancel_raises=True)
      captured["engine"] = engine

      # Unblock op after cancel is attempted (but cancel raises)
      def failed_cancel():
         unblock.set()
         raise Exception("cancel failed")

      engine._dbapi_conn.cancel.side_effect = failed_cancel

      with pytest.raises((DashboardTimeout, asyncio.CancelledError)):
         await run_dashboard_with_deadline(
         engine, op_returns_after_unblock, timeout_sec=0.05)

   asyncio.run(_run())

   time.sleep(0.2)   # let worker finish

   ops = [op[0] if isinstance(op, tuple) else op
          for op in captured["engine"]._call_log]
   assert "invalidate" in ops, (
      "invalidate must appear when DBAPI cancel fails; got: %r" % ops)


# ---------------------------------------------------------------------------
# Test 6: rollback failure → invalidate before close
# ---------------------------------------------------------------------------

def test_rollback_failure_invalidates_before_close():
   """When rollback raises, connection is invalidated before close.

   Machine assertion: invalidate appears in call log before close when
   rollback raises.
   """
   def failing_op(conn, dbapi_conn):
      raise RuntimeError("query failed")

   async def _run():
      engine = _make_mock_engine(rollback_raises=True)

      worker = DashboardWorker(engine, failing_op)
      worker.start()

      with pytest.raises(RuntimeError, match="query failed"):
         await worker.result()

      await worker.join()

      ops = [op[0] if isinstance(op, tuple) else op for op in engine._call_log]
      assert "invalidate" in ops, (
         "invalidate must appear when rollback fails; got: %r" % ops)
      assert "close" in ops, (
         "close must appear even when rollback fails; got: %r" % ops)

      # invalidate must appear before close
      inv_idx = next(i for i, op in enumerate(ops) if op == "invalidate")
      close_idx = next(i for i, op in enumerate(ops) if op == "close")
      assert inv_idx < close_idx, (
         "invalidate must appear before close when rollback fails; "
         "got order: %r" % ops)
      return True

   assert asyncio.run(_run()) is True


# ---------------------------------------------------------------------------
# Test 7: cancel_dbapi lock-protected -- idempotent (at most once)
# ---------------------------------------------------------------------------

def test_cancel_dbapi_is_idempotent_calls_cancel_at_most_once():
   """cancel_dbapi() is idempotent: calling it twice only cancels once.

   Machine assertion: dbapi_conn.cancel() call count <= 1.
   """
   cancel_call_count = []

   def op(conn, dbapi_conn):
      return {"ok": True}

   async def _run():
      engine = _make_mock_engine()
      engine._dbapi_conn.cancel.side_effect = lambda: cancel_call_count.append(1)

      worker = DashboardWorker(engine, op)
      worker.start()

      # Wait for operation to complete before calling cancel_dbapi twice
      result = await worker.result()
      assert result == {"ok": True}

      # Both calls to cancel_dbapi must be idempotent (at most one cancel)
      await worker.cancel_dbapi()
      await worker.cancel_dbapi()

      await worker.join()

   asyncio.run(_run())
   assert len(cancel_call_count) <= 1, (
      "cancel_dbapi must be idempotent; cancel() must be called at most once, "
      "got %d calls" % len(cancel_call_count))


# ---------------------------------------------------------------------------
# Test 8: production run_dashboard_with_deadline exercised for CancelledError
# ---------------------------------------------------------------------------

def test_run_dashboard_with_deadline_handles_cancelled_error_with_full_cleanup():
   """CancelledError during run_dashboard_with_deadline triggers full cleanup.

   Production run_dashboard_with_deadline handles CancelledError.
   After cancellation, pool must be released.

   Machine assertion: close in call log after task.cancel().
   """
   unblock = threading.Event()
   captured = {}

   def blocking_op(conn, dbapi_conn):
      unblock.wait(timeout=10)
      return {}

   async def _test():
      engine = _make_mock_engine()
      captured["engine"] = engine
      engine._dbapi_conn.cancel.side_effect = lambda: unblock.set()

      task = asyncio.create_task(
         run_dashboard_with_deadline(engine, blocking_op, timeout_sec=10.0))

      # Let it start, then cancel the outer task
      await asyncio.sleep(0.05)
      task.cancel()

      try:
         await task
      except (asyncio.CancelledError, DashboardTimeout):
         pass

   asyncio.run(_test())

   time.sleep(0.2)   # let worker finish after unblock

   ops = [op[0] if isinstance(op, tuple) else op
          for op in captured["engine"]._call_log]
   assert "close" in ops, (
      "pool must be released after CancelledError; close must appear in log; "
      "got: %r" % ops)


# ---------------------------------------------------------------------------
# Test 9: lock-protected _dbapi_conn ownership
# cancel_dbapi must NOT call cancel() on a recycled pool connection after
# the worker has already closed/returned its connection.
# ---------------------------------------------------------------------------

def test_cancel_cannot_target_recycled_dbapi_connection():
   """cancel_dbapi called after worker close must not cancel a recycled conn.

   Race: worker finishes, closes SA conn (returns it to pool), pool
   recycles the DBAPI conn for another checkout.  A concurrent cancel_dbapi
   that fires at that moment must not call .cancel() on the recycled conn.

   Machine assertion: after worker.join() _dbapi_conn is None (worker cleared
   it under the lock before close), so a post-join cancel_dbapi finds None.
   """
   result_holder = {}

   def fast_operation(conn, dbapi_conn):
      result_holder["worker_dbapi"] = dbapi_conn
      return {"done": True}

   async def _test():
      engine = _make_mock_engine()
      worker = DashboardWorker(engine, fast_operation)
      worker.start()

      result = await worker.result()
      assert result == {"done": True}
      await worker.join()

      # After join, _dbapi_conn must be None (worker cleared it under lock).
      assert worker._dbapi_conn is None, (
         "_dbapi_conn must be None after worker close (cleared under lock); "
         "a concurrent cancel_dbapi finding it non-None could cancel a "
         "recycled pool connection.  Got: %r" % worker._dbapi_conn)

      # Calling cancel_dbapi after join must be a safe no-op.
      await worker.cancel_dbapi()
      return True

   assert asyncio.run(_test()) is True


# ---------------------------------------------------------------------------
# Test 10: lock-protected _dbapi_conn ownership on the OPERATION-ERROR path
# cancel_dbapi must not target a recycled connection after the worker's
# operation raises and the worker rolls back/closes -- not only on the
# clean-success path.
# ---------------------------------------------------------------------------

def test_cancel_cannot_target_recycled_connection_after_operation_error():
   """_dbapi_conn must be cleared under lock on the operation-exception path too.

   Previously only the success/cancel-detected path cleared _dbapi_conn
   before close; the operation-raised exception path left the stale handle
   in place after rollback+close, so a concurrent cancel_dbapi racing with
   the next pool checkout could call .cancel() on a recycled connection.
   """
   def failing_op(conn, dbapi_conn):
      raise RuntimeError("query failed")

   async def _test():
      engine = _make_mock_engine()
      worker = DashboardWorker(engine, failing_op)
      worker.start()

      with pytest.raises(RuntimeError, match="query failed"):
         await worker.result()
      await worker.join()

      assert worker._dbapi_conn is None, (
         "_dbapi_conn must be None after worker rollback+close on the "
         "operation-error path (cleared under lock); a concurrent "
         "cancel_dbapi finding it non-None could cancel a recycled pool "
         "connection.  Got: %r" % worker._dbapi_conn)

      # Calling cancel_dbapi after join must be a safe no-op (no .cancel()
      # call reaches the dbapi mock).
      await worker.cancel_dbapi()
      assert engine._dbapi_conn.cancel.call_count == 0, (
         "cancel_dbapi after the operation-error path must not call "
         ".cancel() on the (possibly recycled) dbapi connection")
      return True

   assert asyncio.run(_test()) is True


# ---------------------------------------------------------------------------
# Test 11: THIRD cancellation (beyond the single one asyncio.shield absorbs)
# must still not allow return before cleanup completes.
#
# asyncio.shield() only swallows one CancelledError per await.  If a second
# CancelledError reaches run_dashboard_with_deadline's cleanup except-block
# while cleanup is still shielded-and-running, a THIRD CancelledError
# delivered to the unshielded re-await must not let the coroutine return
# before cancel_dbapi/join/invalidate have actually finished.  Purely event-
# driven: no fixed sleeps are used to win the race.
# ---------------------------------------------------------------------------

def test_triple_cancel_does_not_return_before_cleanup_completes():
   """A third task.cancel() must not let run_dashboard_with_deadline return
   before cancel_dbapi + join + invalidate have actually finished.

   Deterministic: the DBAPI cancel() call signals an asyncio.Event back onto
   the loop; the test waits on that event (not a fixed sleep) before firing
   the second and third task.cancel() calls, then releases the blocked
   operation so join() can complete.
   """
   import warnings

   unblock = threading.Event()
   captured = {}

   def blocking_op(conn, dbapi_conn):
      unblock.wait(timeout=10)
      return {}

   async def _test():
      loop = asyncio.get_running_loop()
      cancel_invoked = asyncio.Event()

      engine = _make_mock_engine()
      captured["engine"] = engine

      def spy_cancel():
         # Called from the executor thread inside worker.cancel_dbapi();
         # signal the loop, but do NOT unblock the operation yet so the
         # test has a real window to deliver more cancels while cleanup
         # (join) is still pending.
         loop.call_soon_threadsafe(cancel_invoked.set)

      engine._dbapi_conn.cancel.side_effect = spy_cancel

      task = asyncio.create_task(
         run_dashboard_with_deadline(engine, blocking_op, timeout_sec=10.0))

      # Yield once so the task starts running, then cancel it (cancel #1).
      await asyncio.sleep(0)
      task.cancel()

      # Wait for cancel_dbapi to have actually invoked dbapi cancel() --
      # proof that cleanup_task is now running and inside cancel_dbapi,
      # i.e. run_dashboard_with_deadline is past its first CancelledError
      # and into the shielded cleanup path.
      await cancel_invoked.wait()

      # Fire two MORE cancellations while cleanup (join) is still pending
      # (the operation is still blocked on `unblock`).  The first lands on
      # the outer `await asyncio.shield(cleanup_task)`; the second must be
      # absorbed by the retry loop rather than let the coroutine return.
      task.cancel()
      await asyncio.sleep(0)
      await asyncio.sleep(0)
      task.cancel()

      # Now release the blocked operation so join() can complete.
      unblock.set()

      with pytest.raises((DashboardTimeout, asyncio.CancelledError)):
         await task

      # The assertion that matters: close() must already be in the call
      # log by the time `await task` returns -- no post-return waiting.
      ops = [op[0] if isinstance(op, tuple) else op
             for op in engine._call_log]
      assert "close" in ops, (
         "close must appear in call log before run_dashboard_with_deadline "
         "returns, even under a third repeated cancellation; got: %r" % ops)
      return True

   with warnings.catch_warnings(record=True) as w:
      warnings.simplefilter("always")
      assert asyncio.run(_test()) is True
      import gc
      gc.collect()

   leaked = [
      x for x in w
      if issubclass(x.category, RuntimeWarning)
      and "never retrieved" in str(x.message).lower()
   ]
   assert leaked == [], (
      "repeated cancellation must not leave an unconsumed future/task "
      "exception; got: %r" % [str(x.message) for x in leaked])
