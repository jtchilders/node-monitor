from collections import Counter

from node_monitor.config import (
   ClassificationConfig,
   CollectionConfig,
   NodeConfig,
   RedactionConfig,
)
from node_monitor.pipeline.node_pipeline import NodePipeline
from node_monitor.pipeline.records import (
   COLLECTION_EVENT,
   LIVE_NODE,
   LIVE_PROCESSES,
   NODE_HARDWARE,
   NODE_METRICS_1M,
   NODE_USAGE_5M,
   USER_USAGE_5M,
)
from tests.pipeline import at


def make_pipeline():
   return NodePipeline(
      system="polaris",
      node=NodeConfig(host="n1.example", label="N1", ssh_target="n1"),
      collection=CollectionConfig(),
      classification=ClassificationConfig(),
      redaction=RedactionConfig(),
   )


def hello(fqdn="n1.example", boot_id="b1"):
   return {
      "type": "hello",
      "seq": 0,
      "ts": "2026-11-01T12:00:00.000Z",
      "probe_version": "7",
      "python_version": "3.6.15",
      "hostname_fqdn": fqdn,
      "pid": 5,
      "boot_id": boot_id,
      "btime": 1000,
      "hardware": {"cpu_model": "AMD", "cpu_count": 64},
   }


def counters(t, user=0):
   return {
      "type": "counters",
      "seq": t,
      "ts": at(t).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
      "cpu_jiffies": {
         k: 0 for k in ("user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal")
      }
      | {"user": user},
      "load1": 0.5,
      "load5": 0.4,
      "load15": 0.3,
      "procs_running": 1,
      "procs_total": 300,
      "mem": {"total_kb": 1000, "available_kb": 400},
   }


def row(**overrides):
   base = {
      "pid": 1,
      "ppid": 0,
      "start_time": 100,
      "uid": 1001,
      "username": "alice",
      "comm": "bash",
      "exe": "/bin/bash",
      "argv0": "bash",
      "cmdline": "bash",
      "state": "S",
      "threads": 1,
      "rss_kb": 10,
      "interactive": False,
      "parent_comm": "sshd",
      "parent_exe": "/usr/sbin/sshd",
      "parent_kind": "sshd",
      "under_screen_or_tmux": False,
      "gpu_mem_mb": 0,
      "cpu_seconds": 1.0,
      "cpu_state": "idle",
      "idle_for_sec": 10,
      "idle_for_is_lower_bound": False,
   }
   base.update(overrides)
   return base


def census(rows):
   msg = {
      "type": "census",
      "seq": 1,
      "ts": "2026-11-01T12:00:00.000Z",
      "gpus": [],
      "scan_seconds": 0.2,
      "coverage": {},
      "gpu_status": "ok",
      "interval_sec": 60,
   }
   if rows is not None:
      msg["processes"] = rows
   return msg


def test_end_to_end_over_a_scripted_session():
   p = make_pipeline()
   recs = p.on_event("session_started", {"attempt": 1, "pid": 9}, at(0))
   recs += p.on_message(hello(), at(0))
   for i in range(31):
      recs += p.on_message(counters(i * 10), at(i * 10))
      if i % 6 == 0:
         recs += p.on_message(census([row(cmdline="claude --token t1")]), at(i * 10))
   kinds = Counter(r.kind for r in recs)
   assert kinds[NODE_HARDWARE] == 1 and kinds[NODE_METRICS_1M] == 5
   assert kinds[USER_USAGE_5M] == 1 and kinds[NODE_USAGE_5M] == 1 and kinds[LIVE_PROCESSES] == 6
   live = [r for r in recs if r.kind == LIVE_PROCESSES][-1].row
   assert live["system"] == "polaris" and live["node"] == "n1.example"
   assert live["processes"][0]["cmdline"] == "claude --token ***"
   assert all(r.row["system"] == "polaris" and r.row["node"] == "n1.example" for r in recs)
   node = [r.row for r in recs if r.kind == LIVE_NODE][-1]
   assert node["boot_id"] == "b1" and node["probe_version"] == "7"
   assert node["mem_used_kb"] == 600 and node["last_census_at"] == at(240)


def test_hostname_mismatch_event():
   recs = make_pipeline().on_message(hello(fqdn="other.example"), at(0))
   assert [r.kind for r in recs] == [NODE_HARDWARE, COLLECTION_EVENT]
   assert recs[1].row["event_type"] == "hostname_mismatch"
   assert recs[0].row["cpu_model"] == "AMD"


def test_error_message_becomes_probe_error_event():
   (rec,) = make_pipeline().on_message(
      {"type": "error", "seq": 1, "ts": "x", "where": "census", "message": "boom"}, at(0)
   )
   assert rec.row["event_type"] == "probe_error"
   assert rec.row["detail"] == {"where": "census", "message": "boom"}


def test_bye_emits_nothing():
   assert make_pipeline().on_message({"type": "bye", "seq": 1, "ts": "x"}, at(0)) == []


def test_late_census_for_closed_interval_is_dropped_with_event():
   p = make_pipeline()
   p.on_message(census([row()]), at(0))
   p.on_message(census([row()]), at(360))
   recs = p.on_message(census([row()]), at(120))
   assert [r.row["event_type"] for r in recs] == ["late_sample"]
   assert recs[0].row["detail"] == {"type": "census", "recv": at(120).isoformat()}


def test_late_counters_for_closed_minute_is_dropped_with_event():
   p = make_pipeline()
   p.on_message(counters(0), at(0))
   assert any(r.kind == NODE_METRICS_1M for r in p.on_message(counters(70), at(70)))
   recs = p.on_message(counters(30), at(30))
   assert [r.row["event_type"] for r in recs] == ["late_sample"]


def test_metrics_only_census_feeds_minutes_without_process_records():
   p = make_pipeline()
   assert p.on_message(census(None), at(0)) == []
   p.on_message(counters(0), at(0))
   p.on_message(counters(70), at(70))
   recs = p.on_message(census(None), at(10))
   assert [r.row["event_type"] for r in recs] == ["late_sample"]


def test_session_restart_starts_new_delta_baseline():
   p = make_pipeline()
   p.on_event("session_started", {"attempt": 1, "pid": 1}, at(0))
   p.on_message(counters(0, user=10_000), at(0))
   p.on_event("session_started", {"attempt": 2, "pid": 2}, at(5))
   p.on_message(counters(10, user=5), at(10))
   p.on_message(counters(20, user=105), at(20))
   (m,) = [r for r in p.flush() if r.kind == NODE_METRICS_1M]
   assert m.row["cpu_pct"]["user"] == 100.0


def test_session_lost_marks_node_down():
   p = make_pipeline()
   p.on_message(hello(boot_id="b9"), at(0))
   recs = p.on_event("session_lost", {"reason": "eof"}, at(5))
   assert [r.kind for r in recs] == [COLLECTION_EVENT, LIVE_NODE]
   assert recs[0].row["event_type"] == "session_lost"
   assert recs[1].row["session_state"] == "down" and recs[1].row["boot_id"] == "b9"


def test_flush_emits_partial_intervals():
   p = make_pipeline()
   p.on_message(counters(0), at(0))
   p.on_message(census([row()]), at(0))
   recs = p.flush()
   assert Counter(r.kind for r in recs) == {NODE_METRICS_1M: 1, USER_USAGE_5M: 1, NODE_USAGE_5M: 1}
   assert p.flush() == []
