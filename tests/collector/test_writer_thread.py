import threading
import time
from datetime import datetime, timezone

import pytest

from node_monitor.collector.record_queue import RecordQueue
from node_monitor.collector.writer_thread import WriterThread
from node_monitor.pipeline.records import COLLECTION_EVENT, LIVE_NODE, NODE_METRICS_1M, Record

NOW = datetime(2026, 11, 15, 12, tzinfo=timezone.utc)


def durable(name):
   return Record(NODE_METRICS_1M, {"node": name})


def live(name):
   return Record(LIVE_NODE, {"node": name})


def event(name):
   return Record(COLLECTION_EVENT, {"event_type": name, "node": None})


class FakeBuffered:
   def __init__(self):
      self.calls = []
      self.returns = []  # queued return values, one per submit
      self.raises = []  # queued exceptions (or None), one per submit
      self.closed = 0
      self.seen = threading.Event()

   def submit(self, records, now):
      self.calls.append(list(records))
      self.seen.set()
      exc = self.raises.pop(0) if self.raises else None
      if exc:
         raise exc
      return self.returns.pop(0) if self.returns else []

   def status(self):
      return {"connected": True}

   def close(self):
      self.closed += 1


class FakeUpkeep:
   def __init__(self, events=()):
      self.events = list(events)
      self.ticks = 0
      self.closes = []
      self.statuses = []

   def tick(self, now, status):
      self.ticks += 1
      self.statuses.append(status)
      out, self.events = self.events, []
      return out

   def close(self, now, status):
      self.closes.append(status)


@pytest.fixture
def parts():
   queue = RecordQueue()
   buffered = FakeBuffered()
   upkeep = FakeUpkeep()
   thread = WriterThread(
      queue,
      buffered,
      upkeep,
      status_fn=lambda: {"state": "running"},
      clock=lambda: NOW,
      flush_interval_sec=0.02,
   )
   yield queue, buffered, upkeep, thread
   queue.close()
   if thread.is_alive():
      thread.join(5)


def stop(queue, thread):
   queue.close()
   thread.join(5)
   assert not thread.is_alive()


def test_durable_and_live_are_separate_submits_durable_first(parts):
   queue, buffered, upkeep, thread = parts
   queue.put([live("n1"), durable("n1")])
   thread.start()
   assert buffered.seen.wait(5)
   stop(queue, thread)
   assert buffered.calls == [[durable("n1")], [live("n1")]]


def test_returned_events_are_submitted_next_round(parts):
   queue, buffered, upkeep, thread = parts
   upkeep.events = [event("from_upkeep")]
   buffered.returns = [[event("from_buffered")]]
   queue.put([durable("a")])
   thread.start()
   deadline = time.monotonic() + 5
   while len(buffered.calls) < 2 and time.monotonic() < deadline:
      time.sleep(0.01)
   stop(queue, thread)
   assert buffered.calls[0] == [durable("a")]
   assert buffered.calls[1] == [event("from_buffered"), event("from_upkeep")]


def test_submit_exception_does_not_stop_the_thread(parts, caplog):
   queue, buffered, upkeep, thread = parts
   buffered.raises = [ValueError("poison")]
   queue.put([durable("a")])
   thread.start()
   assert buffered.seen.wait(5)
   time.sleep(0.1)
   assert thread.is_alive()
   queue.put([durable("b")])
   deadline = time.monotonic() + 5
   while len(buffered.calls) < 2 and time.monotonic() < deadline:
      time.sleep(0.01)
   assert buffered.calls[-1] == [durable("b")]
   assert thread.status()["last_write_at"] == NOW.isoformat()
   stop(queue, thread)
   assert any("poison" in (r.exc_text or "") for r in caplog.records)


def test_close_drains_queue_and_stops_within_5s(parts):
   queue, buffered, upkeep, thread = parts
   queue.put([durable("a"), durable("b")])
   queue.close()
   start = time.monotonic()
   thread.start()
   thread.join(5)
   assert not thread.is_alive()
   assert time.monotonic() - start < 5
   assert buffered.calls == [[durable("a"), durable("b")]]
   assert upkeep.closes == [{"state": "running"}]
   assert buffered.closed == 1
   assert thread.daemon is False


def test_exit_writes_carried_events_in_at_most_3_rounds(parts):
   queue, buffered, upkeep, thread = parts
   # Every submit yields a new event, so carried events never run out.
   buffered.returns = [[event(str(i))] for i in range(20)]
   queue.put([durable("a")])
   queue.close()
   thread.start()
   thread.join(5)
   assert not thread.is_alive()
   assert len(buffered.calls) == 1 + 3
   assert buffered.closed == 1


def test_status_is_left_to_upkeep_to_build_when_needed(parts):
   queue, buffered, upkeep, thread = parts
   queue.put([durable("a")])
   queue.close()
   thread.start()
   thread.join(5)
   assert upkeep.statuses and all(callable(s) for s in upkeep.statuses)
   assert upkeep.statuses[0]() == {"state": "running"}


def test_status_combines_buffered_queue_and_last_write(parts):
   queue, buffered, upkeep, thread = parts
   status = thread.status()
   assert status == {
      "connected": True,
      "queue": {"durable": 0, "live": 0},
      "last_write_at": None,
   }
