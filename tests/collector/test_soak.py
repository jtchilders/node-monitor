import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from click.testing import CliRunner

from node_monitor.cli.main import cli
from node_monitor.collector import soak
from node_monitor.collector.bootstrap import probe_source
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
   manifest = json.loads((tmp_path / "out" / "manifest.json").read_text())
   assert manifest["system"] == "test"
   assert manifest["probe_sha256"] == hashlib.sha256(probe_source().encode("utf-8")).hexdigest()
   assert manifest["git_describe"] == soak._git_describe(Path(soak.__file__).parent)
   assert manifest["git_describe"] is not None  # the tests run from a checkout
   assert mode(tmp_path / "out") == 0o700
   assert {mode(p) for p in (tmp_path / "out").iterdir()} == {0o600}
   s = summarize(tmp_path / "out")
   assert set(s) == {"n1", "n2"} and all(v["counters_received"] >= 5 for v in s.values())


def test_cli_report_table(tmp_path):
   write_manifest(tmp_path, counter_interval_sec=10)
   write_lines(tmp_path, "n1", [counters_at(0, 1.0), counters_at(10, 1.1)])
   result = CliRunner().invoke(cli, ["soak", "report", str(tmp_path)])
   assert result.exit_code == 0, result.output
   lines = result.output.splitlines()
   assert "coverage%" in lines[0] and lines[1].startswith("n1")


# --------------------------------------------------------------------------
# Recorder: census shaping
# --------------------------------------------------------------------------


def tracked(pid, cpu_seconds, cpu_state, idle_for_sec=None, lower_bound=False):
   return {
      "pid": pid,
      "username": "u",
      "cmdline": "x",
      "cpu_seconds": cpu_seconds,
      "cpu_state": cpu_state,
      "idle_for_sec": idle_for_sec,
      "idle_for_is_lower_bound": lower_bound,
   }


def recorded(tmp_path, msg):
   r = SoakRecorder(tmp_path)
   r.on_message("n1", msg)
   r.close()
   return json.loads(only_file(tmp_path).read_text().splitlines()[0])["msg"]


def test_recorder_census_summary_has_dormancy_and_delta_stats(tmp_path):
   msg = census_msg(
      [
         tracked(1, None, None),
         tracked(2, 0.0, "idle", 120, lower_bound=True),
         tracked(3, 0.01, "idle", 300),
         tracked(4, 2.5, "active", 0),
      ]
   )
   msg["gpu_status"] = "suspended"
   shaped = recorded(tmp_path, msg)
   assert shaped["cpu_state_counts"] == {"active": 1, "idle": 2, "null": 1}
   assert shaped["idle_lower_bound_count"] == 1 and shaped["idle_for_max_sec"] == 300
   assert shaped["cpu_seconds_min"] == 0.0 and shaped["cpu_seconds_max"] == 2.5
   assert shaped["gpu_status"] == "suspended"


def test_recorder_census_summary_stats_are_null_without_deltas(tmp_path):
   shaped = recorded(tmp_path, census_msg([tracked(1, None, None)]))
   assert shaped["cpu_seconds_min"] is None and shaped["cpu_seconds_max"] is None
   assert shaped["idle_for_max_sec"] is None and shaped["idle_lower_bound_count"] == 0


def test_recorder_census_bytes_is_compact_json_size(tmp_path):
   msg = census_msg([proc("a"), proc("b")])
   shaped = recorded(tmp_path, msg)
   assert shaped["bytes"] == len(json.dumps(msg, separators=(",", ":")))


def test_recorder_malformed_census_counts_a_write_error(tmp_path, capsys):
   r = SoakRecorder(tmp_path)
   r.on_message("n1", {"type": "census", "processes": 5})
   r.on_message("n1", {"type": "census", "processes": ["not a dict"]})
   r.on_message("n1", {"type": "hello"})
   r.close()
   assert r.write_errors == 2
   lines = [json.loads(x) for x in only_file(tmp_path).read_text().splitlines()]
   assert [ln["msg"]["type"] for ln in lines] == ["hello"]
   assert capsys.readouterr().err.count("write error for n1") == 1


# --------------------------------------------------------------------------
# File and directory modes
# --------------------------------------------------------------------------


def mode(path):
   return os.stat(path).st_mode & 0o777


def test_recorder_creates_private_dir_and_files(tmp_path):
   old = os.umask(0o022)
   try:
      out = tmp_path / "a" / "out"
      r = SoakRecorder(out)
      r.on_event("n1", "session_started", {"attempt": 1})
      r.close()
   finally:
      os.umask(old)
   assert mode(out) == 0o700 and mode(only_file(out)) == 0o600


def test_recorder_tightens_existing_file_and_dir(tmp_path):
   out = tmp_path / "out"
   out.mkdir(mode=0o755)
   out.chmod(0o755)
   day = datetime.now(timezone.utc).strftime("%Y%m%d")
   existing = out / f"n1-{day}.jsonl"
   existing.write_text("")
   existing.chmod(0o644)
   r = SoakRecorder(out)
   r.on_event("n1", "x", {})
   r.close()
   assert mode(out) == 0o700 and mode(existing) == 0o600


def test_cli_soak_run_creates_private_default_out_dir(tmp_path, monkeypatch):
   cfg = config_for_fake_tree(tmp_path, nodes=1, counter=10, census=60)
   seen = {}

   async def fake_run_soak(config, out_dir, duration, full_census=False):
      seen["out_dir"] = out_dir
      return 0

   monkeypatch.setattr("node_monitor.cli.main.load_config", lambda path: cfg)
   monkeypatch.setattr("node_monitor.cli.main.run_soak", fake_run_soak)
   old = os.umask(0o022)
   try:
      result = CliRunner().invoke(cli, ["soak", "run", "--duration-hours", "0.001"])
   finally:
      os.umask(old)
   assert result.exit_code == 0, result.output
   out_dir = seen["out_dir"]
   assert out_dir.parent == tmp_path / "logs" / "soak"
   assert mode(out_dir) == 0o700 and mode(out_dir.parent) == 0o700


# --------------------------------------------------------------------------
# Manifest
# --------------------------------------------------------------------------


def git(repo, *args):
   subprocess.run(
      ["git", "-c", "user.name=t", "-c", "user.email=t@example.com", *args],
      cwd=repo,
      check=True,
      capture_output=True,
   )


def test_git_describe_reports_dirty_and_missing_repo(tmp_path):
   repo = tmp_path / "repo"
   repo.mkdir()
   git(repo, "init", "-q")
   (repo / "f").write_text("a")
   git(repo, "add", "f")
   git(repo, "commit", "-q", "-m", "init")
   clean = soak._git_describe(repo)
   assert clean and not clean.endswith("-dirty")
   (repo / "f").write_text("b")
   assert soak._git_describe(repo) == clean + "-dirty"
   (tmp_path / "plain").mkdir()
   assert soak._git_describe(tmp_path / "plain") is None


# --------------------------------------------------------------------------
# Report
# --------------------------------------------------------------------------


def census_at(t, gpu_status="error", cpu_min=0.0, cpu_max=1.0, idle_max=60):
   msg = {
      "type": "census",
      "seq": 2,
      "scan_seconds": 0.5,
      "process_count": 10,
      "bytes": 100,
      "gpu_status": gpu_status,
      "cpu_seconds_min": cpu_min,
      "cpu_seconds_max": cpu_max,
      "idle_for_max_sec": idle_max,
   }
   return t, {"kind": "msg", "msg": msg}


def error_at(t, where):
   return t, {"kind": "msg", "msg": {"type": "error", "seq": 3, "where": where, "message": "m"}}


def write_manifest_full(d, counter=10, census=60):
   (Path(d) / "manifest.json").write_text(
      json.dumps({"counter_interval_sec": counter, "census_interval_sec": census})
   )


def test_summarize_reports_go_evidence(tmp_path):
   write_manifest_full(tmp_path, counter=10, census=60)
   write_lines(
      tmp_path,
      "n1",
      [
         counters_at(0, 1.0),
         census_at(0, "error", cpu_min=0.0, cpu_max=0.5, idle_max=10),
         error_at(1, "gpu"),
         census_at(60, "error"),
         error_at(61, "gpu"),
         census_at(120, "suspended", cpu_min=-0.0, cpu_max=3.0, idle_max=600),
         error_at(121, "signal"),
         event_at(122, "probe_stderr", {"line": "warn"}),
         event_at(123, "malformed_line", {"bytes": 3, "sample": "abc"}),
         event_at(
            124,
            "session_lost",
            {"reason": "exit", "exit_code": 143, "uptime_sec": 1, "stderr_tail": "x" * 300},
         ),
         event_at(125, "session_crashed", {"error": "boom"}),
         counters_at(240, 2.0),
      ],
   )
   s = summarize(tmp_path)["n1"]
   assert s["census_received"] == 3 and s["census_expected"] == pytest.approx(4.0)
   assert s["census_coverage_pct"] == pytest.approx(75.0)
   assert s["errors_by_where"] == {"gpu": 2, "signal": 1}
   assert s["session_crashed_count"] == 1
   assert s["malformed_line_count"] == 1 and s["probe_stderr_count"] == 1
   assert s["loss_timeline"] == [
      {
         "recv": (T0 + timedelta(seconds=124)).isoformat(),
         "reason": "exit",
         "exit_code": 143,
         "stderr_tail": "x" * 200,
      }
   ]
   assert s["gpu_status_counts"] == {"error": 2, "suspended": 1}
   assert s["cpu_seconds_min"] == 0.0 and s["cpu_seconds_max"] == 3.0
   assert s["idle_for_max_sec"] == 600


def test_summarize_merges_daily_files_in_receipt_order(tmp_path):
   write_manifest_full(tmp_path, counter=10, census=60)
   day1 = [counters_at(t, cpu=t / 100) for t in (0, 10, 20)]
   day2 = [counters_at(t, cpu=t / 100) for t in (86400, 86410)]
   with open(tmp_path / "n1-20260102.jsonl", "w") as f:
      for t, body in day2:
         f.write(json.dumps({"recv": (T0 + timedelta(seconds=t)).isoformat(), **body}) + "\n")
   write_lines(tmp_path, "n1", day1)
   write_lines(tmp_path, "n-2", [counters_at(0, 0.0)])
   result = summarize(tmp_path)
   assert set(result) == {"n1", "n-2"}
   s = result["n1"]
   assert s["first"] == T0.isoformat()
   assert s["last"] == (T0 + timedelta(seconds=86410)).isoformat()
   assert s["counters_received"] == 5 and s["max_gap_sec"] == 86380
   assert s["probe_cpu_sec_per_hour"] == pytest.approx(864.1 / (86410 / 3600), rel=1e-6)


def test_summarize_skips_torn_and_non_object_lines(tmp_path):
   write_manifest_full(tmp_path)
   write_lines(tmp_path, "n1", [counters_at(0, 1.0), counters_at(10, 1.1)])
   with open(tmp_path / "n1-20260101.jsonl", "a") as f:
      f.write("[1, 2]\n" + '{"recv": "2026-01-01T00:00:2')
   assert summarize(tmp_path)["n1"]["counters_received"] == 2


def test_cli_report_table_shows_census_coverage_and_errors(tmp_path):
   write_manifest_full(tmp_path, counter=10, census=60)
   write_lines(
      tmp_path,
      "n1",
      [counters_at(0, 1.0), census_at(0), error_at(1, "gpu"), census_at(60), counters_at(120, 1.1)],
   )
   result = CliRunner().invoke(cli, ["soak", "report", str(tmp_path)])
   assert result.exit_code == 0, result.output
   header, row = (line.split() for line in result.output.splitlines()[:2])
   assert row[header.index("census%")] == "100.0" and row[header.index("errors")] == "1"
   as_json = CliRunner().invoke(cli, ["soak", "report", str(tmp_path), "--format", "json"])
   assert json.loads(as_json.output)["n1"]["errors_by_where"] == {"gpu": 1}
