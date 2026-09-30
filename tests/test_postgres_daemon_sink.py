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
* First writer failure becomes PostgresDaemonSinkError (fixed bounded message)
  and is re-raised by subsequent producers and by finalize().
* finalize() drains the queue, stops and awaits the worker, then
  finalizes the diagnostic sink.
* write_done() is permitted only after finalize() and delegates to the
  diagnostic sink.
"""

import asyncio
import inspect

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
# Minimal test fixtures
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

   def __init__(self):
      self.written = []      # list of (record_type, record) tuples
      self.finalized = False
      self.done_written = False

   async def write_record(self, record_type, record):
      self.written.append((record_type, record))

   async def finalize_summary(self):
      self.finalized = True
      return {}

   def write_done(self):
      if not self.finalized:
         raise RuntimeError("write_done() called before finalize_summary()")
      self.done_written = True


class _FakeWriter:
   """Synchronous test double for DatabaseWriter."""

   def __init__(self, failure=None):
      self.calls = []          # list of record lists
      self._failure = failure

   def write_records(self, records):
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
# Unknown / diagnostic_census record types
# ---------------------------------------------------------------------------

def test_unknown_record_type_raises_before_queueing():
   async def _go():
      sink, writer, diagnostic = _make_sink()
      await sink.start()
      with pytest.raises(PostgresDaemonSinkError):
         await sink.write_record("bogus_type", {})
      await sink.finalize()
      assert writer.calls == []
   _run(_go())


def test_diagnostic_census_goes_to_diagnostic_sink_not_writer():
   async def _go():
      sink, writer, diagnostic = _make_sink()
      await sink.start()
      await sink.write_record("diagnostic_census", _diagnostic_census())
      await sink.finalize()
      # Diagnostic sink received it; database writer received nothing
      assert len(diagnostic.written) == 1
      assert diagnostic.written[0][0] == "diagnostic_census"
      assert writer.calls == []
   _run(_go())


# ---------------------------------------------------------------------------
# Relational record types: queue and write
# ---------------------------------------------------------------------------

def test_relational_record_is_forwarded_to_writer():
   async def _go():
      sink, writer, diagnostic = _make_sink()
      await sink.start()
      await sink.write_record("node_collection_log", _collection_log())
      await sink.finalize()
      assert len(writer.calls) >= 1
      all_records = [item for batch in writer.calls for item in batch]
      assert all_records[0][0] == "node_collection_log"
   _run(_go())


def test_all_five_relational_types_accepted():
   """Each of the five permitted relational types is accepted without error."""
   from tests.test_database_writer import (
      _hardware as _hw, _counter, _usage, _poll_failure,
      _collection_log as _clog,
   )

   async def _go():
      sink, writer, diagnostic = _make_sink()
      await sink.start()
      records = [
         ("node_hardware", _hw()),
         ("node_counter_samples", _counter()),
         ("node_usage_intervals", _usage()),
         ("node_poll_failures", _poll_failure()),
         ("node_collection_log", _clog()),
      ]
      for record_type, record in records:
         await sink.write_record(record_type, record)
      await sink.finalize()
      total = sum(len(batch) for batch in writer.calls)
      assert total == 5
   _run(_go())


# ---------------------------------------------------------------------------
# Finalization order
# ---------------------------------------------------------------------------

def test_finalize_drains_queue_before_stopping_worker():
   """Records written before finalize() must reach the writer."""
   async def _go():
      sink, writer, diagnostic = _make_sink()
      await sink.start()
      for _ in range(10):
         await sink.write_record("node_collection_log", _collection_log())
      await sink.finalize()
      total = sum(len(batch) for batch in writer.calls)
      assert total == 10
   _run(_go())


def test_finalize_finalizes_diagnostic_sink():
   async def _go():
      sink, writer, diagnostic = _make_sink()
      await sink.start()
      await sink.finalize()
      assert diagnostic.finalized is True
   _run(_go())


def test_write_done_delegates_to_diagnostic_sink():
   async def _go():
      sink, writer, diagnostic = _make_sink()
      await sink.start()
      await sink.finalize()
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


# ---------------------------------------------------------------------------
# Failure propagation
# ---------------------------------------------------------------------------

def test_writer_failure_becomes_postgres_daemon_sink_error():
   """First writer failure → PostgresDaemonSinkError raised by finalize()."""
   async def _go():
      failing_writer = _FakeWriter(failure=RuntimeError("db exploded"))
      sink, _, diagnostic = _make_sink(writer=failing_writer)
      await sink.start()
      await sink.write_record("node_collection_log", _collection_log())
      with pytest.raises(PostgresDaemonSinkError) as exc_info:
         await sink.finalize()
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
         await sink.finalize()
      assert "secret_detail_xyz" not in str(exc_info.value)
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


# ---------------------------------------------------------------------------
# Queue capacity is private / not a public parameter
# ---------------------------------------------------------------------------

def test_queue_size_not_a_public_parameter():
   """PostgresDaemonSink.__init__ does not accept queue_size as a kwarg."""
   sig = inspect.signature(PostgresDaemonSink.__init__)
   assert "queue_size" not in sig.parameters, (
      "queue_size must not be a public parameter; use _queue_size for tests only"
   )
