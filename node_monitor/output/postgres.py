"""node_monitor.output.postgres -- async PostgreSQL daemon sink.

Routes incoming records to two destinations:
* ``diagnostic_census`` records go to the injected ``diagnostic_sink``
  (a Phase0Sink-like collaborator) via its ``write_record`` coroutine,
  keeping the JSONL-only Phase 0 invariant intact.
* The five relational record types (node_hardware, node_counter_samples,
  node_usage_intervals, node_poll_failures, node_collection_log) are
  placed on a private bounded ``asyncio.Queue`` and consumed by exactly
  one asyncio worker task that calls
  ``asyncio.to_thread(writer.write_records, ...)`` so the synchronous
  SQLAlchemy writer never blocks the event loop.

Design rules
------------
* Queue capacity is 256 (private default).  Tests may pass ``_queue_size``
  as a constructor kwarg; it is NOT a public parameter (no ``queue_size``
  in the public signature).
* Producers await a full queue and never drop records.
* Unknown record types are rejected before queueing.
* The first ``DatabaseWriter.write_records`` failure is captured, wrapped in
  a fixed bounded ``PostgresDaemonSinkError`` message (never driver detail),
  and stored; subsequent producers re-raise it immediately; ``finalize()``
  re-raises it after draining whichever records the worker already committed.
* ``finalize()`` sends a sentinel to drain and stop the worker, awaits it,
  then calls ``diagnostic_sink.finalize_summary()``.
* ``write_done()`` is allowed only after ``finalize()`` and delegates to the
  diagnostic sink.
* No DDL, no database-server lifecycle, no pbs-monitor references.
"""

import asyncio


_DEFAULT_QUEUE_SIZE = 256

_RELATIONAL_TYPES = frozenset((
   "node_hardware",
   "node_counter_samples",
   "node_usage_intervals",
   "node_poll_failures",
   "node_collection_log",
))

_DIAGNOSTIC_TYPE = "diagnostic_census"

# Sentinel placed on the queue to signal the worker to stop.
_STOP = object()

# Fixed bounded error message -- never interpolate driver/exception detail
# into this string so no secret or stack-frame can escape.
_WRITER_FAILURE_MSG = "PostgreSQL writer failed; run cannot continue"


class PostgresDaemonSinkError(Exception):
   """Raised for any PostgresDaemonSink-level failure."""


class PostgresDaemonSink:
   """Async sink that routes records to PostgreSQL (five relational types)
   or the injected JSONL diagnostic sink (diagnostic_census only).

   Parameters
   ----------
   writer :
      An instance of ``node_monitor.database.writer.DatabaseWriter`` (or
      any object with a synchronous ``write_records(records)`` method).
      Called from a thread via ``asyncio.to_thread`` so it never blocks
      the event loop.
   diagnostic_sink :
      A Phase0Sink-like collaborator that owns the ``diagnostic_census``
      JSONL artifact.  Must expose an async ``write_record(record_type,
      record)`` coroutine, an async ``finalize_summary()`` coroutine, and
      a synchronous ``write_done()`` method.
   _queue_size :
      Internal escape hatch for tests ONLY; must not be called
      ``queue_size`` so the public signature stays unambiguous.
   """

   def __init__(self, writer, diagnostic_sink, *, _queue_size=_DEFAULT_QUEUE_SIZE):
      self._writer = writer
      self._diagnostic_sink = diagnostic_sink
      self._queue = asyncio.Queue(_queue_size)
      self._worker_task = None
      self._error = None        # PostgresDaemonSinkError once set, immutable
      self._finalized = False

   # ------------------------------------------------------------------
   # Lifecycle
   # ------------------------------------------------------------------

   async def start(self):
      """Create and start exactly one queue-consumer worker task."""
      if self._worker_task is not None:
         raise PostgresDaemonSinkError("start() called more than once")
      self._worker_task = asyncio.ensure_future(self._worker())

   async def finalize(self):
      """Drain the queue, stop the worker, then finalize the diagnostic sink.

      If a writer failure was recorded by the worker, re-raises it after
      the worker is fully stopped and the diagnostic sink is finalized.
      """
      if self._finalized:
         raise PostgresDaemonSinkError("finalize() called more than once")
      self._finalized = True

      # Send the stop sentinel and wait for the worker to drain everything
      # that was queued before this point.
      await self._queue.put(_STOP)
      if self._worker_task is not None:
         await self._worker_task

      await self._diagnostic_sink.finalize_summary()

      if self._error is not None:
         raise self._error

   def write_done(self):
      """Write the DONE flag via the diagnostic sink.

      Must be called only after ``finalize()``.
      """
      if not self._finalized:
         raise PostgresDaemonSinkError(
            "write_done() called before finalize()")
      self._diagnostic_sink.write_done()

   # ------------------------------------------------------------------
   # Record routing
   # ------------------------------------------------------------------

   async def write_record(self, record_type, record):
      """Route one record to the database queue or the diagnostic sink.

      Raises ``PostgresDaemonSinkError`` immediately for:
      * An unknown record type.
      * A previously recorded writer failure (so producers see it promptly).

      For relational types the coroutine awaits ``self._queue.put(...)``
      which will block (not spin, never drop) when the queue is full.
      """
      if record_type == _DIAGNOSTIC_TYPE:
         await self._diagnostic_sink.write_record(record_type, record)
         return

      if record_type not in _RELATIONAL_TYPES:
         raise PostgresDaemonSinkError(
            "unknown record_type %r; must be one of %s or %r"
            % (record_type, sorted(_RELATIONAL_TYPES), _DIAGNOSTIC_TYPE))

      # Re-raise a previously recorded writer failure before queueing so
      # callers learn about it promptly (design: "first writer failure is
      # re-raised by producers").
      if self._error is not None:
         raise self._error

      await self._queue.put((record_type, record))

   # ------------------------------------------------------------------
   # Internal worker
   # ------------------------------------------------------------------

   async def _worker(self):
      """Single consumer task: dequeue records and write to the database.

      Runs until it dequeues the ``_STOP`` sentinel.  Any exception from
      ``writer.write_records`` is captured as a ``PostgresDaemonSinkError``
      (bounded message, no driver detail).  Once an error is recorded the
      worker drains the queue without writing (so ``finalize()``'s join
      completes) and sets ``self._error`` for producers and ``finalize()``
      to discover.
      """
      while True:
         item = await self._queue.get()
         if item is _STOP:
            self._queue.task_done()
            break

         record_type, record = item
         if self._error is None:
            try:
               await asyncio.to_thread(
                  self._writer.write_records,
                  [(record_type, record)],
               )
            except Exception:
               # Capture a fixed bounded error; never interpolate
               # any exception message or type name.
               self._error = PostgresDaemonSinkError(_WRITER_FAILURE_MSG)
         self._queue.task_done()
