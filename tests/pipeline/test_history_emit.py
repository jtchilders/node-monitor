from datetime import datetime, timezone

from node_monitor.config import ClassificationConfig, CollectionConfig, NodeConfig, RedactionConfig
from node_monitor.pipeline.node_pipeline import NodePipeline
from tests.pipeline.test_node_pipeline import census, row

UTC = timezone.utc


def make_pipeline(**cls_overrides):
   cls = ClassificationConfig(**cls_overrides) if cls_overrides else ClassificationConfig()
   return NodePipeline(
      system="polaris",
      node=NodeConfig(host="n1.example", label="N1", ssh_target="n1"),
      collection=CollectionConfig(),
      classification=cls,
      redaction=RedactionConfig(),
   )


def test_notable_and_hourly_emitted():
   """Only notable processes emitted normally; at :00 every user process is emitted."""
   p = make_pipeline()
   # At 12:00 (hourly) all user processes should be emitted hourly.
   hourly_ts = datetime(2026, 11, 1, 12, 0, 0, tzinfo=UTC)
   rows = [
      row(pid=1, start_time=1, cmdline="bash", rss_kb=10, cpu_seconds=0.01, uid=1001),
      row(pid=2, start_time=2, cmdline="sleep 100", rss_kb=10, cpu_seconds=0.01, uid=1001),
   ]
   recs = p.on_message(census(rows), hourly_ts)
   samples = [r for r in recs if r.kind == "process_samples"]
   # At hourly, all user processes emitted (at least 2)
   assert len(samples) >= 2
   assert all(s.row["hourly"] is True for s in samples)

   # At non-hourly, tiny process not notable -> no samples
   p2 = make_pipeline()
   non_hourly = datetime(2026, 11, 1, 12, 5, 0, tzinfo=UTC)
   recs2 = p2.on_message(census(rows), non_hourly)
   samples2 = [r for r in recs2 if r.kind == "process_samples"]
   assert len(samples2) == 0

   # Notable by cpu
   p3 = make_pipeline()
   recs3 = p3.on_message(census([row(pid=1, start_time=1, cpu_seconds=0.5, rss_kb=10)]), non_hourly)
   assert any(r.kind == "process_samples" for r in recs3)

   # Notable by rss >=1GiB (1048576 kb)
   p4 = make_pipeline()
   recs4 = p4.on_message(
      census([row(pid=1, start_time=1, cpu_seconds=0.01, rss_kb=2 * 1024 * 1024)]), non_hourly
   )
   assert any(r.kind == "process_samples" for r in recs4)

   # Notable by ai_agent category
   p5 = make_pipeline()
   recs5 = p5.on_message(
      census([row(pid=1, start_time=1, cmdline="claude --help", cpu_seconds=0.01, rss_kb=10)]),
      non_hourly,
   )
   assert any(r.kind == "process_samples" for r in recs5)

   # Notable by tags
   p6 = make_pipeline()
   recs6 = p6.on_message(
      census(
         [
            row(
               pid=1,
               start_time=1,
               cmdline="claude --dangerously-skip-permissions",
               cpu_seconds=0.01,
               rss_kb=10,
            )
         ]
      ),
      non_hourly,
   )
   assert any(r.kind == "process_samples" for r in recs6)

   # Verify sample fields include tags, hourly, cmdline_hash, etc.
   sample_rows = [r for r in recs if r.kind == "process_samples"]
   s = sample_rows[0].row
   assert "tags" in s and "hourly" in s and "cmdline_hash" in s
   assert s["system"] == "polaris" and s["node"] == "n1.example" and s["ts"] == hourly_ts


def test_cmdline_dedup_per_census():
   """Per distinct (username, cmdline_hash) in one census, one command_lines record."""
   p = make_pipeline()
   ts = datetime(2026, 11, 1, 12, 5, 0, tzinfo=UTC)
   # Two processes with same cmdline+user -> one sighting
   rows = [
      row(pid=1, start_time=1, username="alice", cmdline="python train.py", uid=1001),
      row(pid=2, start_time=2, username="alice", cmdline="python train.py", uid=1001),
      row(pid=3, start_time=3, username="bob", cmdline="python train.py", uid=1002),
   ]
   recs = p.on_message(census(rows), ts)
   cmdlines = [r for r in recs if r.kind == "command_lines"]
   # alice+hash and bob+hash -> 2 distinct
   assert len(cmdlines) == 2
   keys = {(c.row["username"], c.row["cmdline_hash"]) for c in cmdlines}
   assert len(keys) == 2
   for c in cmdlines:
      assert c.row["system"] == "polaris"
      assert c.row["sample_count"] == 1
      assert c.row["nodes"] == ["n1.example"]
      assert c.row["first_seen"] == ts and c.row["last_seen"] == ts
      assert "cmdline" in c.row and "tags" in c.row
