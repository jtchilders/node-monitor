import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from click.testing import CliRunner

from node_monitor.cli.main import cli
from node_monitor.collector.soak import SoakRecorder, run_soak, summarize
from node_monitor.config import (
   ClassificationConfig,
   CollectionConfig,
   Config,
   NodeConfig,
   SshConfig,
)
from tests.probe import fake_proc

FAKE_SSH = str(Path(__file__).resolve().with_name("fake_ssh.py"))
T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
   monkeypatch.delenv("LD_PRELOAD", raising=False)
   path = os.path.dirname(sys.executable) + os.pathsep + os.environ.get("PATH", "/usr/bin:/bin")
   monkeypatch.setenv("PATH", path)


def proc(user):
   return {"pid": 1, "username": user, "cmdline": "x"}


def census_msg(processes):
   return {
      "type": "census",
      "seq": 3,
      "ts": 1.0,
      "interval_sec": 60,
      "scan_seconds": 0.5,
      "coverage": 1.0,
      "gpus": [],
      "processes": processes,
   }


def only_file(d):
   files = list(Path(d).glob("*.jsonl"))
   assert len(files) == 1
   return files[0]


def counters_at(t, cpu):
   msg = {
      "type": "counters",
      "seq": 1,
      "ts": t,
      "probe_self": {"cpu_seconds": cpu, "children_cpu_seconds": 0.0, "maxrss_kb": 1000},
   }
   return t, {"kind": "msg", "msg": msg}


def event_at(t, kind, detail):
   return t, {"kind": "event", "event": kind, "detail": detail}


def write_manifest(d, counter_interval_sec):
   (Path(d) / "manifest.json").write_text(
      json.dumps({"counter_interval_sec": counter_interval_sec})
   )


def write_lines(d, label, items):
   with open(Path(d) / f"{label}-20260101.jsonl", "w") as f:
      for t, body in items:
         f.write(json.dumps({"recv": (T0 + timedelta(seconds=t)).isoformat(), **body}) + "\n")


def config_for_fake_tree(tmp_path, nodes, counter, census):
   return Config(
      system="test",
      nodes=tuple(NodeConfig(f"h{i}", f"n{i}", f"n{i}") for i in range(1, nodes + 1)),
      probe_python=sys.executable,
      working_dir=tmp_path,
      ssh=SshConfig(),
      collection=CollectionConfig(
         counter_interval_sec=counter,
         census_interval_sec=census,
         session_silence_timeout_sec=5,
         session_max_lifetime_sec=3600,
         reconnect_backoff_sec=(0.05, 0.4),
      ),
      classification=ClassificationConfig(),
      source_path=tmp_path / "config.yaml",
   )


def test_recorder_summarizes_census_by_default(tmp_path):
   r = SoakRecorder(tmp_path)
   r.on_message("n1", census_msg([proc("a"), proc("a"), proc("b")]))
   r.close()
   line = json.loads(only_file(tmp_path).read_text().splitlines()[0])
   assert line["msg"]["process_count"] == 3 and line["msg"]["user_count"] == 2
   assert "processes" not in line["msg"] and line["msg"]["bytes"] > 0


def test_recorder_full_census_keeps_processes(tmp_path):
   r = SoakRecorder(tmp_path, full_census=True)
   r.on_message("n1", census_msg([proc("a")]))
   r.on_event("n1", "session_started", {"attempt": 1})
   r.close()
   lines = [json.loads(x) for x in only_file(tmp_path).read_text().splitlines()]
   assert len(lines[0]["msg"]["processes"]) == 1
   assert lines[1]["kind"] == "event" and lines[1]["event"] == "session_started"
   assert lines[1]["detail"] == {"attempt": 1}


def test_recorder_write_errors_never_raise(tmp_path, capsys):
   r = SoakRecorder(tmp_path)
   r._dir = tmp_path / "missing"
   r.on_message("n1", {"type": "hello"})
   r.on_event("n1", "x", {})
   assert r.write_errors == 2
   assert capsys.readouterr().err.count("write error") == 1


def test_summarize_coverage_gaps_losses_and_overhead(tmp_path):
   write_manifest(tmp_path, counter_interval_sec=10)
   write_lines(
      tmp_path,
      "n1",
      [
         counters_at(0, cpu=1.0),
         counters_at(10, cpu=1.1),
         counters_at(20, cpu=1.2),
         event_at(25, "session_lost", {"reason": "silence"}),
         event_at(60, "session_started", {"attempt": 2}),
         counters_at(60, cpu=0.0),
         counters_at(3600, cpu=3.0),
      ],
   )
   s = summarize(tmp_path)["n1"]
   assert s["max_gap_sec"] == 3540 and s["losses_by_reason"] == {"silence": 1}
   assert s["counters_received"] == 5
   assert s["probe_cpu_sec_per_hour"] == pytest.approx((0.2 + 3.0) / 1.0, rel=1e-3)


async def test_run_soak_end_to_end_with_fake_ssh(tmp_path):
   cfg = config_for_fake_tree(tmp_path, nodes=2, counter=0.2, census=0.5)
   roots = fake_proc.write_tree(
      tmp_path / "tree", pids=[{"pid": 4242, "comm": "python", "cmdline": ["python", "x.py"]}]
   )
   overrides = {
      "gpu": False,
      "proc_root": roots["proc"],
      "sys_root": roots["sys"],
      "lock_path": str(tmp_path / "probe.lock"),
   }
   rc = await run_soak(
      cfg, tmp_path / "out", duration_sec=3, ssh_binary=FAKE_SSH, probe_overrides=overrides
   )
   assert rc == 0
   assert json.loads((tmp_path / "out" / "manifest.json").read_text())["system"] == "test"
   s = summarize(tmp_path / "out")
   assert set(s) == {"n1", "n2"} and all(v["counters_received"] >= 5 for v in s.values())


def test_cli_report_table(tmp_path):
   write_manifest(tmp_path, counter_interval_sec=10)
   write_lines(tmp_path, "n1", [counters_at(0, 1.0), counters_at(10, 1.1)])
   result = CliRunner().invoke(cli, ["soak", "report", str(tmp_path)])
   assert result.exit_code == 0, result.output
   lines = result.output.splitlines()
   assert "coverage%" in lines[0] and lines[1].startswith("n1")
