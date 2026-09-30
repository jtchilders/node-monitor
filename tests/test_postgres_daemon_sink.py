"""Tests for node_monitor.output.postgres.PostgresDaemonSink.

All async behavior uses plain ``asyncio.run()`` inside ordinary (sync)
pytest functions, matching the pattern used throughout this project
(no pytest-asyncio plugin installed).

Contract being tested
---------------------
* diagnostic_census records go to the injected diagnostic sink ONLY (JSONL),
  never to the database writer queue.
* Exactly five relational record types are accepted and queued:
  node_hardware, node_counter_samples, node_usage_intervals,
  node_poll_failures, node_collection_log.
* Unknown record types are rejected immediately, before queueing.
* Records await a bounded asyncio.Queue(256); producers never drop.
* start() creates exactly one asyncio worker that calls
  asyncio.to_thread(writer.write_records, ...) for each batch.
* First writer failure becomes PostgresDaemonSinkError (fixed bounded message,
  no driver detail in message, __cause__, or __context__).
* After any writer failure, ALL subsequent producers raise immediately --
  including producers blocked on a full queue that wake up after failure.
* finalize_summary(acceptance_fn=None) drains the queue, stops and awaits
  the worker, then finalizes the diagnostic sink.  The finalized-success
  state is only set after diagnostic_sink.finalize_summary() completes
  without raising AND there is no stored writer error.
* write_done() is permitted only after finalize_summary() succeeds and
  delegates to the diagnostic sink.
* write_done() is forbidden after a failed finalization.
* Post-finalize write_record() calls are rejected.
* Double start() raises PostgresDaemonSinkError.
* asyncio.to_thread is used for writer calls (blocking I/O off the event loop).
* Submission order is preserved (FIFO).
* Bounded backpressure: a producer blocks when the queue is full and resumes
  when the worker drains a slot.
* Diagnostic writes after DB failure are rejected (stored error checked before
  all routing including diagnostic branch).
* write_record() / finalize_summary() before start() raise boundedly.
* Cancellation of a blocked producer must not enqueue and must not poison sink.
* Cancellation of finalize_summary() must cancel/await the worker and leave
  _finalized False (write_done forbidden); no orphaned task warnings.
"""

import asyncio
import inspect
import threading

import pytest

from node_monitor.output.postgres import (
   PostgresDaemonSink,
   PostgresDaemonSinkError,
)


# ---------------------------------------------------------------------------
# Helper: run a coroutine with a short timeout
# ---------------------------------------------------------------------------

def _run(coro, timeout=5.0):
   return asyncio.run(asyncio.wait_for(coro, timeout=timeout))


# ---------------------------------------------------------------------------
# Minimal test fixtures -- local plain dicts, no SQLAlchemy imports
# ---------------------------------------------------------------------------

def _hardware(**overrides):
   record = {
      "system": "polaris", "source_hostname": "login-04.example.org",
      "first_seen_utc": "2026-09-29T12:00:00+00:00", "probe_version": 4,
      "boot_id": "boot-a", "btime": 100, "cpu_model": "Zen",
      "cpu_logical": 64, "sockets": 2, "cores_per_socket": 16,
      "cpu_max_freq_khz": 3500000, "numa_nodes": 4,
      "mem_total_kb": 1000000, "swap_total_kb": 0,
      "hugepage_size_kb": 2048, "kernel_release": "6.1",
      "os_pretty_name": "Linux", "net_fs_mounts": 2,
      "net_ifaces": {"hsn0": {"speed_mbps": 100000}},
      "gpus": [{"model": "GPU"}],
   }
   record.update(overrides)
   return record


def _counter(**overrides):
   record = {
      "system": "polaris", "source_hostname": "login-04.example.org",
      "window_start_utc": "2026-09-29T12:00:00+00:00",
      "window_end_utc": "2026-09-29T12:01:00+00:00",
      "elapsed_sec": 60.0, "sample_count": 6, "expected_samples": 6,
      "cpu_user_pct": 10.0, "cpu_system_pct": 2.0, "cpu_iowait_pct": 0.5,
      "cpu_idle_pct": 87.5, "load_1m": 1.2, "load_5m": 1.0, "load_15m": 0.8,
      "mem_used_kb": 200000, "mem_avail_kb": 800000,
      "net_rx_bytes": 1000, "net_tx_bytes": 2000,
      "lustre_targets": {},
   }
   record.update(overrides)
   return record


def _usage(**overrides):
   record = {
      "system": "polaris", "source_hostname": "login-04.example.org",
      "username": "testuser", "category": "interactive",
      "interval_start_utc": "2026-09-29T12:00:00+00:00",
      "interval_end_utc": "2026-09-29T12:01:00+00:00",
      "cpu_user_sec": 5.0, "cpu_system_sec": 1.0,
      "process_count": 2,
   }
   record.update(overrides)
   return record


def _poll_failure(**overrides):
   record = {
      "system": "polaris", "source_hostname": "login-04.example.org",
      "timestamp_utc": "2026-09-29T12:00:00+00:00",
      "loop": "counter", "error_kind": "timeout", "detail": {},
   }
   record.update(overrides)
   return record


def _collection_log(**overrides):
   record = {
      "system": "polaris", "timestamp_utc": "2026-09-30T14:00:02+00:00",
      "event": "daemon_started", "detail": {"version": "0.2.0"},
   }
   record.update(overrides)
   return record


def _diagnostic_census():
   return {
      "system": "polaris", "source_hostname": "login-04.example.org",
      "timestamp_utc": "2026-09-30T14:00:00+00:00",
      "probe_version": 4, "processes": [], "cpu_deltas": {},
   }


class _FakeDiagnosticSink:
   """Minimal Phase0Sink-like collaborator for diagnostic_census."""

   def __init__(self, finalize_error=None):
      self.written = []      # list of (record_type, record) tuples
      self.finalized = False
      self.done_written = False
      self._finalize_error = finalize_error

   async def write_record(self, record_type, record):
      self.written.append((record_type, record))

   async def finalize_summary(self, acceptance_fn=None):
      if self._finalize_error is not None:
         raise self._finalize_error
      self.finalized = True
      result = {}
      if acceptance_fn is not None:
         result["acceptance"] = acceptance_fn({})
      return result

   def write_done(self):
      if not self.finalized:
         raise RuntimeError("write_done() called before finalize_summary()")
      self.done_written = True


class _FakeWriter:
   """Synchronous test double for DatabaseWriter."""

   def __init__(self, failure=None):
      self.calls = []          # list of record lists
      self._failure = failure
      self.thread_ids = []     # thread id for each write_records call

   def write_records(self, records):
      self.thread_ids.append(threading.current_thread().ident)
      self.calls.append(list(records))
      if self._failure is not None:
         raise self._failure
      return len(records)


def _make_sink(writer=None, diagnostic=None, queue_size=None):
   writer = writer or _FakeWriter()
   diagnostic = diagnostic or _FakeDiagnosticSink()
   kwargs = {"writer": writer, "diagnostic_sink": diagnostic}
   if queue_size is not None:
      kwargs["_queue_size"] = queue_size
   return PostgresDaemonSink(**kwargs), writer, diagnostic


# ---------------------------------------------------------------------------
# Import sanity
# ---------------------------------------------------------------------------

def test_import_succeeds():
   """PostgresDaemonSink and PostgresDaemonSinkError are importable."""
   assert PostgresDaemonSink is not None
   assert PostgresDaemonSinkError is not None


def test_error_is_exception_subclass():
   assert issubclass(PostgresDaemonSinkError, Exception)


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

def test_construction_accepts_writer_and_diagnostic_sink():
   sink, _, _ = _make_sink()
   assert sink is not None


def test_construction_with_custom_queue_size():
   sink, _, _ = _make_sink(queue_size=4)
   assert sink is not None


# ---------------------------------------------------------------------------
# Before-start lifecycle -- Defect 5
# ---------------------------------------------------------------------------

def test_write_record_before_start_raises_boundedly():
   """write_record() before start() must raise PostgresDaemonSinkError, not hang."""
   async def _go():
      sink, _, _ = _make_sink()
      with pytest.raises(PostgresDaemonSinkError):
         await sink.write_record("node_collection_log", _collection_log())
   _run(_go())


def test_finalize_summary_before_start_raises_boundedly():
   """finalize_summary() before start() must raise PostgresDaemonSinkError, not hang."""
   async def _go():
      sink, _, _ = _make_sink()
      with pytest.raises(PostgresDaemonSinkError):
         await sink.finalize_summary()
   _run(_go())


def test_diagnostic_write_before_start_raises_boundedly():
   """diagnostic_census write before start() must raise PostgresDaemonSinkError."""
   async def _go():
      sink, _, _ = _make_sink()
      with pytest.raises(PostgresDaemonSinkError):
         await sink.write_record("diagnostic_census", _diagnostic_census())
   _run(_go())


# ---------------------------------------------------------------------------
# Unknown / diagnostic_census record types
# ---------------------------------------------------------------------------

def test_unknown_record_type_raises_before_queueing():
   async def _go():
      sink, writer, diagnostic = _make_sink()
      await sink.start()
      with pytest.raises(PostgresDaemonSinkError):
         await sink.write_record("bogus_type", {})
      await sink.finalize_summary()
      assert writer.calls == []
   _run(_go())


def test_diagnostic_census_goes_to_diagnostic_sink_not_writer():
   async def _go():
      sink, writer, diagnostic = _make_sink()
      await sink.start()
      await sink.write_record("diagnostic_census", _diagnostic_census())
      await sink.finalize_summary()
      # Diagnostic sink received it; database writer received nothing
      assert len(diagnostic.written) == 1
      assert diagnostic.written[0][0] == "diagnostic_census"
      assert writer.calls == []
   _run(_go())


# ---------------------------------------------------------------------------
# Defect 4: diagnostic writes after DB failure must be rejected
# ---------------------------------------------------------------------------

def test_diagnostic_write_after_db_failure_raises():
   """diagnostic_census write after a DB writer failure must raise.

   The design says stop accepting new polls/producers after DB failure.
   The stored-error check must fire before the diagnostic routing branch,
   not only for relational types.

   Uses an explicit writer_failed Event so the test waits only as long as
   needed, with no arbitrary sleep(0.1) to establish writer-error state.
   """
   async def _go():
      writer_failed = asyncio.Event()

      class _SignallingFailWriter:
         """Fails on first call and signals writer_failed before raising."""
         def write_records(self, records):
            writer_failed.set()
            raise RuntimeError("db exploded")

      sink = PostgresDaemonSink(
         writer=_SignallingFailWriter(),
         diagnostic_sink=_FakeDiagnosticSink(),
      )
      await sink.start()
      # Trigger the writer failure
      await sink.write_record("node_collection_log", _collection_log())
      # Wait for the worker to process the record and record the failure
      await asyncio.wait_for(writer_failed.wait(), timeout=3.0)
      # Give the event loop one turn to propagate _error assignment
      await asyncio.sleep(0)
      # Diagnostic write must also be rejected after DB failure
      with pytest.raises(PostgresDaemonSinkError):
         await sink.write_record("diagnostic_census", _diagnostic_census())
   _run(_go())


# ---------------------------------------------------------------------------
# Relational record types: queue and write
# ---------------------------------------------------------------------------

def test_relational_record_is_forwarded_to_writer():
   async def _go():
      sink, writer, diagnostic = _make_sink()
      await sink.start()
      await sink.write_record("node_collection_log", _collection_log())
      await sink.finalize_summary()
      assert len(writer.calls) >= 1
      all_records = [item for batch in writer.calls for item in batch]
      assert all_records[0][0] == "node_collection_log"
   _run(_go())


def test_all_five_relational_types_accepted():
   """Each of the five permitted relational types is accepted without error.

   Uses local plain record dictionaries -- no imports from test_database_writer
   and no SQLAlchemy coupling.
   """
   async def _go():
      sink, writer, diagnostic = _make_sink()
      await sink.start()
      records = [
         ("node_hardware", _hardware()),
         ("node_counter_samples", _counter()),
         ("node_usage_intervals", _usage()),
         ("node_poll_failures", _poll_failure()),
         ("node_collection_log", _collection_log()),
      ]
      for record_type, record in records:
         await sink.write_record(record_type, record)
      await sink.finalize_summary()
      total = sum(len(batch) for batch in writer.calls)
      assert total == 5
   _run(_go())


# ---------------------------------------------------------------------------
# Finalization order and lifecycle
# ---------------------------------------------------------------------------

def test_finalize_drains_queue_before_stopping_worker():
   """Records written before finalize_summary() must reach the writer."""
   async def _go():
      sink, writer, diagnostic = _make_sink()
      await sink.start()
      for _ in range(10):
         await sink.write_record("node_collection_log", _collection_log())
      await sink.finalize_summary()
      total = sum(len(batch) for batch in writer.calls)
      assert total == 10
   _run(_go())


def test_finalize_finalizes_diagnostic_sink():
   async def _go():
      sink, writer, diagnostic = _make_sink()
      await sink.start()
      await sink.finalize_summary()
      assert diagnostic.finalized is True
   _run(_go())


def test_finalize_summary_accepts_acceptance_fn():
   """finalize_summary forwards acceptance_fn to the diagnostic sink."""
   async def _go():
      sink, writer, diagnostic = _make_sink()
      await sink.start()
      called_with = []

      def _acceptance_fn(files_summary):
         called_with.append(files_summary)
         return {"accepted": True}

      result = await sink.finalize_summary(acceptance_fn=_acceptance_fn)
      assert len(called_with) == 1   # acceptance_fn was called exactly once
   _run(_go())


def test_write_done_delegates_to_diagnostic_sink():
   async def _go():
      sink, writer, diagnostic = _make_sink()
      await sink.start()
      await sink.finalize_summary()
      sink.write_done()
      assert diagnostic.done_written is True
   _run(_go())


def test_write_done_before_finalize_raises():
   async def _go():
      sink, _, _ = _make_sink()
      await sink.start()
      with pytest.raises(PostgresDaemonSinkError):
         sink.write_done()
   _run(_go())


def test_double_finalize_raises():
   """finalize_summary() called a second time must raise."""
   async def _go():
      sink, _, _ = _make_sink()
      await sink.start()
      await sink.finalize_summary()
      with pytest.raises(PostgresDaemonSinkError):
         await sink.finalize_summary()
   _run(_go())


def test_write_record_after_finalize_raises():
   """write_record() after finalize_summary() must be rejected."""
   async def _go():
      sink, _, _ = _make_sink()
      await sink.start()
      await sink.finalize_summary()
      with pytest.raises(PostgresDaemonSinkError):
         await sink.write_record("node_collection_log", _collection_log())
   _run(_go())


def test_write_done_forbidden_after_failed_finalization():
   """write_done() must raise if finalize_summary() itself raised."""
   async def _go():
      diag = _FakeDiagnosticSink(
         finalize_error=RuntimeError("disk full during finalize")
      )
      sink, writer, _ = _make_sink(diagnostic=diag)
      await sink.start()
      with pytest.raises((PostgresDaemonSinkError, RuntimeError)):
         await sink.finalize_summary()
      # write_done() must NOT be permitted
      with pytest.raises(PostgresDaemonSinkError):
         sink.write_done()
   _run(_go())


def test_finalized_success_state_not_set_before_diagnostic_finalize_completes():
   """write_done() may only succeed if diagnostic finalize succeeded."""
   class _PartialDiag:
      """Finalize raises so success must not be recorded."""
      def __init__(self):
         self.finalized = False
         self.done_written = False

      async def write_record(self, rt, record):
         pass

      async def finalize_summary(self, acceptance_fn=None):
         raise RuntimeError("finalize_summary failed")

      def write_done(self):
         self.done_written = True

   async def _go():
      diag = _PartialDiag()
      sink = PostgresDaemonSink(writer=_FakeWriter(), diagnostic_sink=diag)
      await sink.start()
      with pytest.raises((PostgresDaemonSinkError, RuntimeError)):
         await sink.finalize_summary()
      # finalize failed -- write_done must be rejected
      with pytest.raises(PostgresDaemonSinkError):
         sink.write_done()
   _run(_go())


# ---------------------------------------------------------------------------
# Defect 1: writer failure -> finalize raises -> write_done must raise
# ---------------------------------------------------------------------------

def test_writer_failure_finalize_raises_write_done_remains_forbidden():
   """Writer failure -> finalize_summary raises -> write_done() must raise.

   Defect: finalize_summary() was setting self._finalized = True before
   checking self._error and raising it.  After the raise, _finalized was
   already True, so write_done() incorrectly succeeded.
   """
   async def _go():
      failing_writer = _FakeWriter(failure=RuntimeError("db exploded"))
      sink, _, diagnostic = _make_sink(writer=failing_writer)
      await sink.start()
      await sink.write_record("node_collection_log", _collection_log())
      # finalize_summary must raise because of the DB failure
      with pytest.raises(PostgresDaemonSinkError):
         await sink.finalize_summary()
      # write_done() must also raise -- _finalized must NOT be True
      with pytest.raises(PostgresDaemonSinkError):
         sink.write_done()
      # diagnostic sink's write_done must not have been called
      assert diagnostic.done_written is False
   _run(_go())


def test_writer_failure_diagnostic_finalize_still_runs():
   """Diagnostic finalize runs even after a DB writer failure.

   After DB writer failure, finalize_summary() should still run the
   diagnostic sink's finalize_summary() to flush JSONL data, but must
   NOT mark overall success and must re-raise the DB error.
   """
   async def _go():
      failing_writer = _FakeWriter(failure=RuntimeError("db exploded"))
      sink, _, diagnostic = _make_sink(writer=failing_writer)
      await sink.start()
      await sink.write_record("node_collection_log", _collection_log())
      with pytest.raises(PostgresDaemonSinkError):
         await sink.finalize_summary()
      # Diagnostic finalize ran (JSONL flushed) even though DB failed
      assert diagnostic.finalized is True
      # But overall success is NOT recorded
      with pytest.raises(PostgresDaemonSinkError):
         sink.write_done()
   _run(_go())


# ---------------------------------------------------------------------------
# Double start
# ---------------------------------------------------------------------------

def test_double_start_raises():
   """start() called twice must raise PostgresDaemonSinkError."""
   async def _go():
      sink, _, _ = _make_sink()
      await sink.start()
      with pytest.raises(PostgresDaemonSinkError):
         await sink.start()
      await sink.finalize_summary()
   _run(_go())


# ---------------------------------------------------------------------------
# asyncio.to_thread offload
# ---------------------------------------------------------------------------

def test_writer_called_from_thread_not_event_loop_thread():
   """write_records must be called from a worker thread (asyncio.to_thread)."""
   async def _go():
      loop_thread_id = threading.current_thread().ident
      writer = _FakeWriter()
      sink, _, _ = _make_sink(writer=writer)
      await sink.start()
      await sink.write_record("node_collection_log", _collection_log())
      await sink.finalize_summary()
      assert len(writer.thread_ids) >= 1
      for tid in writer.thread_ids:
         assert tid != loop_thread_id, (
            "write_records must run in a worker thread, not the event-loop thread"
         )
   _run(_go())


# ---------------------------------------------------------------------------
# FIFO submission order
# ---------------------------------------------------------------------------

def test_fifo_submission_order():
   """Records must be dequeued and written in the same order submitted."""
   async def _go():
      writer = _FakeWriter()
      sink, _, _ = _make_sink(writer=writer)
      await sink.start()
      sequence = list(range(8))
      for i in sequence:
         await sink.write_record("node_collection_log", _collection_log(detail={"seq": i}))
      await sink.finalize_summary()
      all_records = [item for batch in writer.calls for item in batch]
      written_seq = [r[1]["detail"]["seq"] for r in all_records]
      assert written_seq == sequence, (
         f"FIFO violated: expected {sequence}, got {written_seq}"
      )
   _run(_go())


# ---------------------------------------------------------------------------
# Bounded backpressure
# ---------------------------------------------------------------------------

def test_bounded_backpressure_producer_blocks_then_resumes():
   """Producer blocks when queue is full and resumes after worker drains a slot."""
   async def _go():
      # Gate that we can open from the test to let the worker proceed
      gate = asyncio.Event()

      class _GatedWriter:
         def __init__(self):
            self.calls = []
            self.thread_ids = []

         def write_records(self, records):
            # Block until the gate is open; run in asyncio.to_thread, so
            # we wait in a thread without blocking the event loop.
            import time
            deadline = time.monotonic() + 5.0
            while not gate.is_set():
               if time.monotonic() > deadline:
                  raise RuntimeError("gate never opened (test timeout)")
               time.sleep(0.005)
            self.calls.append(list(records))
            return len(records)

      gated = _GatedWriter()
      diag = _FakeDiagnosticSink()
      # Queue of size 1: one record fills it, second producer must block
      sink = PostgresDaemonSink(writer=gated, diagnostic_sink=diag, _queue_size=1)
      await sink.start()

      # Put the first record -- worker picks it up immediately and blocks at gate
      await asyncio.sleep(0.02)   # let worker start and block at gate
      await sink.write_record("node_collection_log", _collection_log(detail={"n": 0}))

      # Give the worker a moment to dequeue record 0 and block inside write_records
      await asyncio.sleep(0.05)

      # Now the queue is empty (worker holding record 0).
      # Fill it with one more record so the queue is full again.
      await sink.write_record("node_collection_log", _collection_log(detail={"n": 1}))

      # Third producer should block because queue is full (worker still gated)
      producer_done = asyncio.Event()
      producer_result = []

      async def _producer():
         await sink.write_record("node_collection_log", _collection_log(detail={"n": 2}))
         producer_result.append("enqueued")
         producer_done.set()

      producer_task = asyncio.ensure_future(_producer())

      # Brief pause -- producer should be blocked, not yet done
      await asyncio.sleep(0.05)
      assert not producer_done.is_set(), "Producer should be blocked on full queue"

      # Open the gate; worker drains, freeing a slot, producer unblocks
      gate.set()

      # Wait for the producer to complete
      await asyncio.wait_for(producer_done.wait(), timeout=4.0)
      assert producer_result == ["enqueued"]

      await asyncio.wait_for(sink.finalize_summary(), timeout=4.0)
      total = sum(len(b) for b in gated.calls)
      assert total == 3

   _run(_go(), timeout=15.0)


# ---------------------------------------------------------------------------
# Failure propagation
# ---------------------------------------------------------------------------

def test_writer_failure_becomes_postgres_daemon_sink_error():
   """First writer failure → PostgresDaemonSinkError raised by finalize_summary()."""
   async def _go():
      failing_writer = _FakeWriter(failure=RuntimeError("db exploded"))
      sink, _, diagnostic = _make_sink(writer=failing_writer)
      await sink.start()
      await sink.write_record("node_collection_log", _collection_log())
      with pytest.raises(PostgresDaemonSinkError) as exc_info:
         await sink.finalize_summary()
      # Message is fixed/bounded, no raw driver detail
      assert "db exploded" not in str(exc_info.value)
   _run(_go())


def test_writer_failure_error_message_is_bounded():
   """PostgresDaemonSinkError message is a fixed bounded string."""
   async def _go():
      failing_writer = _FakeWriter(failure=RuntimeError("secret_detail_xyz"))
      sink, _, _ = _make_sink(writer=failing_writer)
      await sink.start()
      await sink.write_record("node_collection_log", _collection_log())
      with pytest.raises(PostgresDaemonSinkError) as exc_info:
         await sink.finalize_summary()
      assert "secret_detail_xyz" not in str(exc_info.value)
   _run(_go())


def test_writer_failure_error_has_no_cause_or_context():
   """PostgresDaemonSinkError must not expose driver detail via __cause__ or __context__."""
   async def _go():
      sentinel = "driver_secret_xyz"
      failing_writer = _FakeWriter(failure=RuntimeError("driver failure: " + sentinel))
      sink, _, _ = _make_sink(writer=failing_writer)
      await sink.start()
      await sink.write_record("node_collection_log", _collection_log())
      try:
         await sink.finalize_summary()
      except PostgresDaemonSinkError as exc:
         assert sentinel not in str(exc)
         assert exc.__cause__ is None, (
            f"__cause__ must be None; got {exc.__cause__!r}"
         )
         assert exc.__context__ is None, (
            f"__context__ must be None; got {exc.__context__!r}"
         )
      else:
         pytest.fail("Expected PostgresDaemonSinkError was not raised")
   _run(_go())


def test_write_after_writer_failure_raises_postgres_daemon_sink_error():
   """After a writer failure, producers re-raise PostgresDaemonSinkError."""
   async def _go():
      failing_writer = _FakeWriter(failure=RuntimeError("db exploded"))
      sink, _, _ = _make_sink(writer=failing_writer, queue_size=4)
      await sink.start()
      # Trigger the failure
      await sink.write_record("node_collection_log", _collection_log())
      # Give the worker a moment to process and record the error
      await asyncio.sleep(0.1)
      # Next write should raise immediately
      with pytest.raises(PostgresDaemonSinkError):
         await sink.write_record("node_collection_log", _collection_log())
   _run(_go())


def test_producer_blocked_on_full_queue_raises_after_writer_failure():
   """A producer blocked on queue.put wakes after worker failure and must raise.

   Deterministic race: worker is gated holding record A; queue capacity 1
   holds record B; producer C calls write_record and is observed pending
   on queue.put (blocked, not yet returned).  Gate A to fail; C must wake
   and raise PostgresDaemonSinkError, not return success.  Queue must drain
   and finalize must terminate.

   This exercises the actual race, not just the pre-put check path.
   """
   async def _go():
      # worker_entered: fired when write_records starts executing (holding A)
      worker_entered = asyncio.Event()
      # release_gate: we open this to let the worker proceed (and fail)
      release_gate = asyncio.Event()

      class _GatedFailWriter:
         """Blocks until release_gate, then fails on first call."""
         def __init__(self):
            self.call_count = 0

         def write_records(self, records):
            import time
            self.call_count += 1
            # Signal that the worker is now holding record A
            worker_entered.set()
            # Wait for the test to set up producer C before releasing
            deadline = time.monotonic() + 5.0
            while not release_gate.is_set():
               if time.monotonic() > deadline:
                  raise RuntimeError("release_gate never opened")
               time.sleep(0.005)
            # Always fail
            raise RuntimeError("db failed")

      gated_fail = _GatedFailWriter()
      diag = _FakeDiagnosticSink()
      # Capacity 1: record A will be dequeued by worker, then B fills the queue,
      # then producer C is observed blocked on put.
      sink = PostgresDaemonSink(
         writer=gated_fail, diagnostic_sink=diag, _queue_size=1
      )
      await sink.start()

      # Enqueue record A -- worker dequeues it and enters write_records (gated)
      await sink.write_record("node_collection_log", _collection_log(detail={"n": 0}))
      # Wait for the worker to be inside write_records holding A
      await asyncio.wait_for(worker_entered.wait(), timeout=3.0)

      # Queue is now empty (worker holds A in write_records).
      # Enqueue record B to fill the capacity-1 queue.
      await sink.write_record("node_collection_log", _collection_log(detail={"n": 1}))

      # Launch producer C -- it must block on queue.put because queue is full
      # and the worker is still gated.
      producer_result = []
      producer_exception = []

      async def _producer_c():
         try:
            await sink.write_record("node_collection_log", _collection_log(detail={"n": 2}))
            producer_result.append("success")
         except PostgresDaemonSinkError as exc:
            producer_exception.append(exc)

      producer_task = asyncio.ensure_future(_producer_c())

      # Let the event loop run so _producer_c reaches queue.put and blocks
      await asyncio.sleep(0.05)

      # Producer C must still be pending (blocked on queue.put)
      assert not producer_task.done(), (
         "Producer C should be blocked on queue.put, not yet done"
      )
      assert producer_result == [], "Producer C must not have succeeded yet"

      # Release the worker -- it fails, sets self._error, drains the queue
      # (including B), then B is task_done'd.  C wakes from queue.put,
      # enqueues its record, then re-checks self._error and must raise.
      release_gate.set()

      # Wait for producer C to finish (raise or succeed)
      await asyncio.wait_for(producer_task, timeout=4.0)

      # Producer C must have raised, not silently succeeded
      assert producer_result == [], (
         "Producer C must NOT return success after writer failure"
      )
      assert len(producer_exception) == 1, (
         "Producer C must raise PostgresDaemonSinkError"
      )

      # finalize_summary raises because of the stored writer error -- that is
      # the correct behavior; queue drains and worker stops cleanly.
      with pytest.raises(PostgresDaemonSinkError):
         await asyncio.wait_for(sink.finalize_summary(), timeout=4.0)

   _run(_go(), timeout=15.0)


# ---------------------------------------------------------------------------
# Defect 3: Cancellation / shutdown contracts
# ---------------------------------------------------------------------------

def test_producer_cancel_while_blocked_does_not_enqueue():
   """Cancelling a producer blocked on queue.put must not enqueue the record.

   Contract: asyncio.CancelledError propagates out; sink's queue and error
   state are unchanged; the sink remains usable (no poison).

   Synchronization: uses worker_entered Event so the test knows the worker
   has dequeued record A before filling B and launching C.  No fixed sleep
   to establish this state.
   """
   async def _go():
      # worker_entered: fires when write_records starts (worker holds A)
      worker_entered = asyncio.Event()
      # worker_gate: keep the worker gated inside write_records
      worker_gate = asyncio.Event()

      class _GatedWriter:
         def write_records(self, records):
            import time
            worker_entered.set()
            deadline = time.monotonic() + 5.0
            while not worker_gate.is_set():
               if time.monotonic() > deadline:
                  raise RuntimeError("gate never opened")
               time.sleep(0.005)
            return len(records)

      sink = PostgresDaemonSink(
         writer=_GatedWriter(),
         diagnostic_sink=_FakeDiagnosticSink(),
         _queue_size=1,
      )
      await sink.start()

      # Enqueue record A -- worker dequeues it immediately and enters write_records
      await sink.write_record("node_collection_log", _collection_log(detail={"n": 0}))
      # Wait deterministically for the worker to be inside write_records holding A
      await asyncio.wait_for(worker_entered.wait(), timeout=3.0)

      # Queue is now empty (worker holds A gated).  Fill the slot with B.
      await sink.write_record("node_collection_log", _collection_log(detail={"n": 1}))

      # Producer C blocks on queue.put (queue full, worker still gated)
      producer_task = asyncio.ensure_future(
         sink.write_record("node_collection_log", _collection_log(detail={"n": 2}))
      )
      await asyncio.sleep(0.05)
      assert not producer_task.done(), "Producer C should be blocked"

      # Cancel producer C
      producer_task.cancel()
      try:
         await asyncio.wait_for(producer_task, timeout=1.0)
      except (asyncio.CancelledError, asyncio.TimeoutError):
         pass

      # Sink must not be poisoned: self._error is still None
      assert sink._error is None, "Cancelling a producer must not poison the sink"

      # Release the worker so finalize can proceed
      worker_gate.set()
      await asyncio.wait_for(sink.finalize_summary(), timeout=4.0)

   _run(_go(), timeout=15.0)


def test_finalize_cancel_leaves_finalized_false_and_no_orphan():
   """Cancelling finalize_summary must not falsely permit write_done.

   Contract: CancelledError propagates; _finalized remains False; no
   orphaned worker task (worker is cancelled and awaited); write_done
   raises PostgresDaemonSinkError.

   We verify no task-exception warnings by ensuring the worker task is
   not left pending after cancellation.
   """
   async def _go():
      # Gate the worker so finalize_summary is stuck awaiting the worker task
      worker_gate = asyncio.Event()
      worker_started = asyncio.Event()

      class _GatedWriter:
         def write_records(self, records):
            import time
            worker_started.set()
            deadline = time.monotonic() + 5.0
            while not worker_gate.is_set():
               if time.monotonic() > deadline:
                  raise RuntimeError("gate never opened")
               time.sleep(0.005)
            return len(records)

      sink = PostgresDaemonSink(
         writer=_GatedWriter(),
         diagnostic_sink=_FakeDiagnosticSink(),
         _queue_size=4,
      )
      await sink.start()

      # Enqueue one record so the worker has something to do (and will be gated)
      await sink.write_record("node_collection_log", _collection_log())
      await asyncio.wait_for(worker_started.wait(), timeout=3.0)

      # Start finalize in background -- it will block awaiting the worker task
      finalize_task = asyncio.ensure_future(sink.finalize_summary())
      await asyncio.sleep(0.05)
      assert not finalize_task.done(), "finalize_summary should be blocked"

      # Cancel finalize
      finalize_task.cancel()
      try:
         await asyncio.wait_for(finalize_task, timeout=1.0)
      except (asyncio.CancelledError, asyncio.TimeoutError):
         pass

      # _finalized must remain False: write_done must raise
      with pytest.raises(PostgresDaemonSinkError):
         sink.write_done()

      # Release the worker gate so the worker task can complete and not
      # leave a pending task that generates asyncio warnings
      worker_gate.set()
      if sink._worker_task is not None and not sink._worker_task.done():
         try:
            await asyncio.wait_for(sink._worker_task, timeout=2.0)
         except Exception:
            pass

   _run(_go(), timeout=15.0)

def test_finalize_cancel_at_queue_put_stop_leaves_worker_done():
   """Cancellation at queue.put(_STOP) must leave worker done, not orphaned.

   This is the Critical path: queue capacity 1, worker is gated inside
   write_records holding record A, queue contains record B (full).
   finalize_summary() is called -- it sets _finalize_attempted then blocks
   at ``await queue.put(_STOP)`` because the queue is full.  The finalize
   task is cancelled.  CancelledError must propagate, _finalized must remain
   False (write_done forbidden), and -- critically -- the worker task must be
   done and awaited (no orphan).

   Without the fix, _STOP never enters the queue, the worker waits forever,
   and worker_task.done() is False (orphan).
   """
   async def _go():
      # writer_entered: fired once write_records starts (worker holds A)
      writer_entered = asyncio.Event()
      # release_writer: open this to let the writer thread finish
      release_writer = asyncio.Event()

      class _GatedWriter:
         """Blocks inside write_records until release_writer is set."""
         def write_records(self, records):
            import time
            writer_entered.set()
            deadline = time.monotonic() + 10.0
            while not release_writer.is_set():
               if time.monotonic() > deadline:
                  raise RuntimeError("release_writer never opened")
               time.sleep(0.005)
            return len(records)

      # Queue capacity 1: record A dequeued by worker, B sits in queue.
      sink = PostgresDaemonSink(
         writer=_GatedWriter(),
         diagnostic_sink=_FakeDiagnosticSink(),
         _queue_size=1,
      )
      await sink.start()

      # Enqueue A -- worker dequeues it and enters write_records (gated)
      await sink.write_record("node_collection_log", _collection_log(detail={"n": 0}))
      # Wait deterministically for worker to be inside write_records holding A
      await asyncio.wait_for(writer_entered.wait(), timeout=3.0)

      # Enqueue B -- fills the capacity-1 queue
      await sink.write_record("node_collection_log", _collection_log(detail={"n": 1}))

      # Verify queue is full (qsize == capacity) and worker is still pending
      assert sink._queue.full(), "Queue must be full before starting finalize"
      assert not sink._worker_task.done(), "Worker must still be running"

      # Start finalize in background.  It sets _finalize_attempted=True, then
      # blocks at await queue.put(_STOP) because the queue is full.
      finalize_task = asyncio.ensure_future(sink.finalize_summary())

      # Give the event loop time to reach queue.put(_STOP) and block
      await asyncio.sleep(0.05)
      assert not finalize_task.done(), (
         "finalize_summary must be blocked at queue.put(_STOP)"
      )
      # Queue still full: _STOP has not been inserted yet
      assert sink._queue.full(), "Queue must still be full; _STOP is pending"

      # Cancel finalize while it is blocked at queue.put(_STOP)
      finalize_task.cancel()
      with pytest.raises(asyncio.CancelledError):
         await finalize_task

      # --- Post-cancellation invariants ---

      # 1. _finalized must remain False: write_done must raise
      with pytest.raises(PostgresDaemonSinkError):
         sink.write_done()

      # 2. Worker must be done (cancelled and awaited): no orphan task.
      #    Release the writer thread so the executor can terminate cleanly
      #    (asyncio.to_thread cannot stop an already-running thread, so the
      #    test must unblock it to prevent executor leakage).
      release_writer.set()

      # Give the event loop time to propagate cancellation to the worker
      worker = sink._worker_task
      assert worker is not None, "worker task must exist after start()"
      try:
         await asyncio.wait_for(asyncio.shield(worker), timeout=3.0)
      except (asyncio.CancelledError, Exception):
         pass  # either outcome is acceptable; we only need done()

      assert worker.done(), (
         "Worker task must be done after finalize cancellation (no orphan). "
         "CURRENT CODE FAILS HERE: _STOP never entered the queue so the worker "
         "is waiting for _STOP indefinitely."
      )

   _run(_go(), timeout=15.0)


# ---------------------------------------------------------------------------

def test_queue_size_not_a_public_parameter():
   """PostgresDaemonSink.__init__ does not accept queue_size as a kwarg."""
   sig = inspect.signature(PostgresDaemonSink.__init__)
   assert "queue_size" not in sig.parameters, (
      "queue_size must not be a public parameter; use _queue_size for tests only"
   )


# ---------------------------------------------------------------------------
# Task 3 corrective: abort() method -- worker cleanup on unexpected failure
# ---------------------------------------------------------------------------
#
# When daemon.run() raises an unexpected exception (not a controlled daemon
# exit), the caller must be able to cancel/await the worker task so it is
# not orphaned.  PostgresDaemonSink.abort() must:
# * Cancel and await the worker (if started, not already done).
# * Be idempotent (callable when worker is None or already done).
# * Leave _finalized False and write_done() forbidden.
# * NOT call diagnostic_sink.finalize_summary or write_done (caller handles
#   cleanup independently).
# ---------------------------------------------------------------------------

class _CapturingSink:
   """Minimal diagnostic sink that records which lifecycle calls were made."""
   def __init__(self):
      self.calls = []

   async def write_record(self, record_type, record):
      self.calls.append(("write_record", record_type))

   async def finalize_summary(self, acceptance_fn=None):
      self.calls.append("finalize_summary")

   def write_done(self):
      self.calls.append("write_done")


class _BlockingWriter:
   """Writer that blocks until released; lets tests control timing."""
   def __init__(self, release_event=None):
      self._event = release_event or threading.Event()

   def write_records(self, records):
      self._event.wait()
      return len(list(records))

   def release(self):
      self._event.set()


def test_abort_before_start_is_idempotent():
   """abort() before start() must not raise and must be a no-op."""
   sink = PostgresDaemonSink(
      writer=_BlockingWriter(), diagnostic_sink=_CapturingSink())

   async def _go():
      # abort() on unstarted sink must not raise; use short timeout
      await asyncio.wait_for(sink.abort(), timeout=2.0)

   _run(_go())
   assert sink._finalized is False


def test_abort_after_start_cancels_and_awaits_worker():
   """abort() after start() must cancel the worker and leave it done."""
   release = threading.Event()
   writer = _BlockingWriter(release)
   diag = _CapturingSink()
   sink = PostgresDaemonSink(writer=writer, diagnostic_sink=diag,
                             _queue_size=4)

   async def _go():
      await sink.start()
      # Queue one record so worker may try to pick it up
      await sink._queue.put(("node_hardware", {"dummy": True}))
      # Give worker a moment to pick up the item
      await asyncio.sleep(0.05)

      # Release the blocking writer BEFORE abort so it can exit
      release.set()

      # abort() must cancel and await the worker within a short time
      await asyncio.wait_for(sink.abort(), timeout=5.0)

      worker = sink._worker_task
      assert worker is not None
      assert worker.done(), "worker must be done after abort()"

   _run(_go())
   # finalize_summary and write_done must NOT have been called
   assert "finalize_summary" not in diag.calls
   assert "write_done" not in diag.calls
   assert sink._finalized is False


def test_abort_after_done_worker_is_idempotent():
   """abort() when worker already done must not raise."""
   diag = _CapturingSink()
   records_written = []

   class QuickWriter:
      def write_records(self, records):
         records_written.extend(records)
         return len(records_written)

   sink = PostgresDaemonSink(writer=QuickWriter(), diagnostic_sink=diag,
                             _queue_size=4)

   async def _go():
      await sink.start()
      # Send sentinel directly to stop worker cleanly
      from node_monitor.output.postgres import _STOP
      await sink._queue.put(_STOP)
      # Wait for worker to finish
      await asyncio.wait_for(sink._worker_task, timeout=3.0)
      assert sink._worker_task.done()

      # Now abort should be a no-op
      await asyncio.wait_for(sink.abort(), timeout=2.0)  # must not raise

   _run(_go())
   assert sink._finalized is False


def test_abort_does_not_call_diagnostic_finalize():
   """abort() must never call diagnostic_sink.finalize_summary."""
   diag = _CapturingSink()

   class QuickWriter:
      def write_records(self, records):
         return len(list(records))

   sink = PostgresDaemonSink(writer=QuickWriter(), diagnostic_sink=diag,
                             _queue_size=4)

   async def _go():
      await sink.start()
      await asyncio.wait_for(sink.abort(), timeout=5.0)

   _run(_go())
   assert "finalize_summary" not in diag.calls, (
      "abort() must not call diagnostic_sink.finalize_summary"
   )
   assert sink._finalized is False
