"""Thread-safe hand-off from the asyncio pipelines to the single database writer thread.

Durable records are kept whole and in put order. Live records are coalesced per node while
they wait: only the newest `LIVE_PROCESSES` snapshot matters, and `LIVE_NODE` partial rows
merge in put order (later keys win; a missing key means "unchanged"). A slow write therefore
never builds an unbounded backlog of superseded live state.

`put` after `close()` raises RuntimeError: a producer still running at that point is a
shutdown-ordering bug and must not silently lose records.
"""

import threading
import time

from node_monitor.pipeline.records import LIVE_NODE, LIVE_PROCESSES, Record


class RecordQueue:
   def __init__(self):
      self._cond = threading.Condition()
      self._durable: list[Record] = []
      self._live_processes: dict[str, Record] = {}
      self._live_node: dict[str, Record] = {}
      self._closed = False

   @property
   def closed(self) -> bool:
      with self._cond:
         return self._closed

   def put(self, records: list[Record]) -> None:
      if not records:
         return
      with self._cond:
         if self._closed:
            raise RuntimeError("RecordQueue is closed")
         for record in records:
            if record.kind == LIVE_PROCESSES:
               self._live_processes[record.row["node"]] = record
            elif record.kind == LIVE_NODE:
               node = record.row["node"]
               previous = self._live_node.get(node)
               # A new dict: the pipeline may still hold the caller's rows.
               merged = {**previous.row, **record.row} if previous else dict(record.row)
               self._live_node[node] = Record(LIVE_NODE, merged)
            else:
               self._durable.append(record)
         self._cond.notify_all()

   def take(self, timeout: float) -> tuple[list[Record], list[Record]]:
      deadline = time.monotonic() + timeout
      with self._cond:
         while not self._has_records() and not self._closed:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
               break
            self._cond.wait(remaining)
         durable = self._durable
         live = [*self._live_processes.values(), *self._live_node.values()]
         self._durable = []
         self._live_processes = {}
         self._live_node = {}
         return durable, live

   def close(self) -> None:
      with self._cond:
         self._closed = True
         self._cond.notify_all()

   def depth(self) -> dict:
      with self._cond:
         return {
            "durable": len(self._durable),
            "live": len(self._live_processes) + len(self._live_node),
         }

   def _has_records(self) -> bool:
      return bool(self._durable or self._live_processes or self._live_node)
