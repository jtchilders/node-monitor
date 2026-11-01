import threading
import time

import pytest

from node_monitor.collector.record_queue import RecordQueue
from node_monitor.pipeline.records import (
   COLLECTION_EVENT,
   LIVE_NODE,
   LIVE_PROCESSES,
   NODE_METRICS_1M,
   Record,
)


def test_live_processes_coalesce_to_latest_per_node():
   q = RecordQueue()
   q.put(
      [Record(LIVE_PROCESSES, {"node": "a", "n": 1}), Record(LIVE_PROCESSES, {"node": "b", "n": 1})]
   )
   q.put([Record(LIVE_PROCESSES, {"node": "a", "n": 2})])
   durable, live = q.take(0.1)
   assert durable == []
   by_node = {r.row["node"]: r.row["n"] for r in live if r.kind == LIVE_PROCESSES}
   assert by_node == {"a": 2, "b": 1}
   assert len(live) == 2


def test_live_node_partials_merge_in_order():
   q = RecordQueue()
   full = {"node": "a", "updated_at": "t1", "session_state": "up", "load1": 1.5, "mem": 7}
   lost = {"node": "a", "updated_at": "t2", "session_state": "down"}
   q.put([Record(LIVE_NODE, full)])
   q.put([Record(LIVE_NODE, lost)])
   _, live = q.take(0.1)
   assert len(live) == 1
   assert live[0].kind == LIVE_NODE
   assert live[0].row == {
      "node": "a",
      "updated_at": "t2",
      "session_state": "down",
      "load1": 1.5,
      "mem": 7,
   }
   # Callers' dicts are left alone.
   assert full["session_state"] == "up" and "load1" not in lost


def test_live_node_merge_keeps_nodes_separate():
   q = RecordQueue()
   q.put([Record(LIVE_NODE, {"node": "a", "x": 1}), Record(LIVE_NODE, {"node": "b", "x": 2})])
   _, live = q.take(0.1)
   assert sorted((r.row["node"], r.row["x"]) for r in live) == [("a", 1), ("b", 2)]


def test_durable_records_keep_put_order():
   q = RecordQueue()
   q.put([Record(NODE_METRICS_1M, {"node": "a", "i": 0}), Record(LIVE_NODE, {"node": "a"})])
   q.put(
      [
         Record(COLLECTION_EVENT, {"node": "a", "i": 1}),
         Record(NODE_METRICS_1M, {"node": "a", "i": 2}),
      ]
   )
   durable, live = q.take(0.1)
   assert [r.row["i"] for r in durable] == [0, 1, 2]
   assert len(live) == 1


def test_take_drains_and_depth_reports_waiting():
   q = RecordQueue()
   q.put(
      [
         Record(NODE_METRICS_1M, {"node": "a"}),
         Record(LIVE_PROCESSES, {"node": "a"}),
         Record(LIVE_PROCESSES, {"node": "a"}),
         Record(LIVE_NODE, {"node": "a"}),
      ]
   )
   assert q.depth() == {"durable": 1, "live": 2}
   q.take(0.1)
   assert q.depth() == {"durable": 0, "live": 0}


def test_take_times_out_empty():
   q = RecordQueue()
   start = time.monotonic()
   assert q.take(0.1) == ([], [])
   assert 0.08 <= time.monotonic() - start < 1.0


def test_empty_put_does_not_wake_take():
   q = RecordQueue()
   threading.Timer(0.02, lambda: q.put([])).start()
   start = time.monotonic()
   assert q.take(0.2) == ([], [])
   assert time.monotonic() - start >= 0.15


def test_put_from_another_thread_wakes_take():
   q = RecordQueue()
   threading.Timer(0.05, lambda: q.put([Record(NODE_METRICS_1M, {"node": "a"})])).start()
   start = time.monotonic()
   durable, _ = q.take(5.0)
   assert len(durable) == 1
   assert time.monotonic() - start < 0.5


def test_close_wakes_a_blocked_take_and_drains():
   q = RecordQueue()
   result = []
   t = threading.Thread(target=lambda: result.append(q.take(10.0)))
   t.start()
   time.sleep(0.05)
   q.close()
   t.join(2.0)
   assert not t.is_alive()
   assert result == [([], [])]
   assert q.closed


def test_close_leaves_remaining_records_for_one_more_take():
   q = RecordQueue()
   q.put([Record(NODE_METRICS_1M, {"node": "a"}), Record(LIVE_NODE, {"node": "a"})])
   q.close()
   start = time.monotonic()
   durable, live = q.take(10.0)
   assert len(durable) == 1 and len(live) == 1
   assert q.take(10.0) == ([], [])
   assert time.monotonic() - start < 1.0


def test_put_after_close_raises():
   q = RecordQueue()
   q.close()
   with pytest.raises(RuntimeError):
      q.put([Record(NODE_METRICS_1M, {"node": "a"})])
