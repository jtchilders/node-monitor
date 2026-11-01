"""The single thread that writes to the database.

It drains the RecordQueue into BufferedWriter and runs Upkeep, so every database call
the daemon makes happens here. Durable records go first and live state separately:
live rows are replaceable, and a bad one must not take durable data down with it.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from datetime import datetime

from node_monitor.collector.record_queue import RecordQueue
from node_monitor.collector.upkeep import Upkeep
from node_monitor.database.buffered import BufferedWriter
from node_monitor.pipeline.records import Record

log = logging.getLogger(__name__)

# After shutdown, rounds spent writing the events the last submits produced.
_FINAL_ROUNDS = 3


class WriterThread(threading.Thread):
   def __init__(
      self,
      queue: RecordQueue,
      buffered: BufferedWriter,
      upkeep: Upkeep,
      *,
      status_fn: Callable[[], dict],
      clock: Callable[[], datetime],
      flush_interval_sec: float = 5.0,
   ):
      # Not a daemon thread: the process must wait for the final flush.
      super().__init__(name="writer", daemon=False)
      self._queue = queue
      self._buffered = buffered
      self._upkeep = upkeep
      self._status_fn = status_fn
      self._clock = clock
      self._flush_interval_sec = flush_interval_sec
      self._carried: list[Record] = []
      self._last_write_at: datetime | None = None

   def status(self) -> dict:
      last = self._last_write_at
      return {
         **self._buffered.status(),
         "queue": self._queue.depth(),
         "last_write_at": last.isoformat() if last else None,
      }

   def run(self) -> None:
      try:
         while True:
            durable, live = self._queue.take(self._flush_interval_sec)
            self._round(durable, live, tick=True)
            if self._queue.closed and not any(self._queue.depth().values()):
               break
         for _ in range(_FINAL_ROUNDS):
            if not self._carried:
               break
            self._round([], [], tick=False)
      except BaseException:
         log.exception("writer thread crashed")
         raise
      finally:
         self._shutdown()

   def _round(self, durable: list[Record], live: list[Record], *, tick: bool) -> None:
      carried, self._carried = self._carried, []
      next_carried: list[Record] = []
      first = carried + durable
      if first:
         next_carried += self._submit(first)
      if live:
         next_carried += self._submit(live)
      if tick:
         try:
            next_carried += self._upkeep.tick(self._clock(), self._status_fn())
         except Exception:  # noqa: BLE001
            log.exception("upkeep tick failed")
      self._carried = next_carried

   def _submit(self, records: list[Record]) -> list[Record]:
      try:
         events = self._buffered.submit(records, self._clock())
      except Exception:  # noqa: BLE001 - BufferedWriter carries its own events
         log.exception("writing %d records failed", len(records))
         return []
      self._last_write_at = self._clock()
      return events

   def _shutdown(self) -> None:
      try:
         self._upkeep.close(self._clock(), self._status_fn())
      except Exception:  # noqa: BLE001
         log.exception("upkeep close failed")
      try:
         self._buffered.close()
      except Exception:  # noqa: BLE001
         log.exception("buffered close failed")
