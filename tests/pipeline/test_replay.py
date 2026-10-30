import json
import os
import stat
from datetime import timedelta
from pathlib import Path

from click.testing import CliRunner

from node_monitor.cli.main import cli
from node_monitor.config import (
   ClassificationConfig,
   CollectionConfig,
   Config,
   NodeConfig,
   RedactionConfig,
   SshConfig,
)
from node_monitor.pipeline.replay import replay
from tests.pipeline import at
from tests.pipeline.test_node_pipeline import census, counters, hello, row


def make_config(tmp_path, labels=("n1", "n2")):
   return Config(
      system="polaris",
      nodes=tuple(NodeConfig(f"{label}.example", label, label) for label in labels),
      probe_python="python3",
      working_dir=tmp_path,
      ssh=SshConfig(),
      collection=CollectionConfig(),
      classification=ClassificationConfig(),
      source_path=tmp_path / "config.yaml",
      redaction=RedactionConfig(),
   )


def line(t, body):
   return json.dumps({"recv": at(t).isoformat(), **body})


def msg_line(t, msg):
   return line(t, {"kind": "msg", "msg": msg})


def write_node(d, label, lines, day="20261101"):
   (Path(d) / f"{label}-{day}.jsonl").write_text("\n".join(lines) + "\n")


def build_soak(tmp_path, nodes=("n1", "n2")):
   d = tmp_path / "soak"
   d.mkdir()
   manifest = {
      "nodes": [{"label": n, "ssh_target": n} for n in nodes],
      "counter_interval_sec": 10,
      "census_interval_sec": 60,
   }
   (d / "manifest.json").write_text(json.dumps(manifest))
   for n in nodes:
      fqdn = f"{n}.example"
      lines = [
         line(0, {"kind": "event", "event": "session_started", "detail": {"attempt": 1}}),
         msg_line(0, hello(fqdn=fqdn)),
      ]
      for i in range(31):
         lines.append(msg_line(i * 10, counters(i * 10)))
         if i % 6 == 0:
            lines.append(msg_line(i * 10, census([row(cmdline="claude --token t1")])))
      lines.insert(5, "{torn line")
      write_node(d, n, lines)
   return d


def test_replay_counts_records_from_a_small_soak_dir(tmp_path):
   d = build_soak(tmp_path)
   result = replay(d, make_config(tmp_path))
   assert set(result) == {"n1", "n2"}
   for counts in result.values():
      assert counts["node_hardware"] == 1
      assert counts["node_metrics_1m"] == 6
      assert counts["live_processes"] == 6
      assert counts["user_usage_5m"] == 2 and counts["node_usage_5m"] == 2
      assert counts["collection_event"] == 1


def test_replay_summary_census_is_metrics_only_and_files_read_in_order(tmp_path):
   d = tmp_path / "soak"
   d.mkdir()
   (d / "manifest.json").write_text(json.dumps({"nodes": [{"label": "n1"}]}))
   late = at(0) + timedelta(days=1)
   write_node(d, "n1", [msg_line(0, hello()), msg_line(0, census(None))])
   next_day = [json.dumps({"recv": late.isoformat(), "kind": "msg", "msg": census(None)})]
   write_node(d, "n1", next_day, day="20261102")
   result = replay(d, make_config(tmp_path))
   assert result["n1"]["live_processes"] == 0 and result["n1"]["node_hardware"] == 1
   assert "collection_event" not in result["n1"]


def test_replay_skips_nodes_missing_from_config(tmp_path):
   d = build_soak(tmp_path)
   skipped = []
   result = replay(d, make_config(tmp_path, labels=("n1",)), on_skip=skipped.append)
   assert set(result) == {"n1"} and skipped == ["n2"]


def test_cli_replay_prints_counts_and_writes_private_records(tmp_path, monkeypatch):
   d = build_soak(tmp_path)
   cfg = tmp_path / "config.yaml"
   cfg.write_text(
      "system: polaris\nprobe_python: python3\nnodes:\n"
      "  - {host: n1.example, label: n1, ssh_target: n1}\n"
   )
   monkeypatch.setenv("NODE_MONITOR_CONFIG", str(cfg))
   out = tmp_path / "records.jsonl"
   res = CliRunner().invoke(cli, ["soak", "replay", str(d), "--write-records", str(out)])
   assert res.exit_code == 0, res.output
   assert "n1" in res.output and "node_metrics_1m=6" in res.output
   assert "n2" in res.output and "not in config" in res.output
   assert stat.S_IMODE(os.stat(out).st_mode) == 0o600
   rows = [json.loads(x) for x in out.read_text().splitlines()]
   assert {r["kind"] for r in rows} >= {"node_metrics_1m", "live_processes"}
   live = next(r for r in rows if r["kind"] == "live_processes")
   assert live["row"]["ts"].endswith("+00:00")
   assert live["row"]["processes"][0]["cmdline"] == "claude --token ***"
