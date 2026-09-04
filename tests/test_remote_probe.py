"""Tests for the remote probe.

The probe runs unattended on shared login nodes where a parse bug produces a
plausible-looking wrong number rather than a crash. These tests therefore
focus on the failure modes that are silent: comm-field parsing, permission
denial, coverage accounting, and the JSON contract the daemon depends on.
"""

import json
import os
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "fixtures"))

from node_monitor.collector import remote_probe as probe  # noqa: E402
import fake_proc  # noqa: E402


PROBE_PATH = os.path.join(
   os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
   "node_monitor", "collector", "remote_probe.py")


@pytest.fixture
def proc_root(tmp_path, monkeypatch):
   """Point the probe at a synthetic /proc for the duration of one test."""
   root = str(tmp_path / "proc")

   def _build(pids, **kwargs):
      fake_proc.write_proc(root, pids, **kwargs)
      monkeypatch.setattr(probe, "PROC_ROOT", root)
      return root

   return _build


# --------------------------------------------------------------------------
# /proc/<pid>/stat parsing -- the highest-risk parser in the probe
# --------------------------------------------------------------------------

class TestParseStat:
   def test_plain_comm(self):
      comm, tail = probe._parse_stat(fake_proc.make_stat(42, "bash"))
      assert comm == "bash"
      assert tail[1] == "1"

   def test_comm_containing_spaces(self):
      """Real fleet processes have spaces in comm.

      Splitting /proc/<pid>/stat on whitespace shifts every subsequent field
      by one, so utime lands in the tty slot. The result is not a crash --
      it is a plausible wrong CPU number, which is worse.
      """
      comm, tail = probe._parse_stat(
         fake_proc.make_stat(42, "my proc name", utime=777))
      assert comm == "my proc name"
      assert tail[11] == "777"

   def test_comm_containing_parens(self):
      """'(sd-pam)' is present on every Polaris login node.

      Bounding comm with the FIRST ')' instead of the last truncates the
      name and shifts the fields.
      """
      comm, tail = probe._parse_stat(
         fake_proc.make_stat(42, "(sd-pam)", stime=333))
      assert comm == "(sd-pam)"
      assert tail[12] == "333"

   def test_comm_with_parens_and_spaces(self):
      comm, _ = probe._parse_stat(fake_proc.make_stat(42, "weird (x) name"))
      assert comm == "weird (x) name"

   def test_malformed_returns_none(self):
      assert probe._parse_stat("garbage with no parens") is None
      assert probe._parse_stat("") is None

   def test_truncated_tail_returns_none(self):
      assert probe._parse_stat("42 (bash) S 1 2 3") is None


# --------------------------------------------------------------------------
# Classification
# --------------------------------------------------------------------------

class TestClassification:
   @pytest.mark.parametrize("cmdline,expected", fake_proc.CMDLINE_CORPUS)
   def test_corpus(self, cmdline, expected):
      category, _, _ = probe._classify("x", cmdline)
      assert category == expected, "%r -> %s, expected %s" % (
         cmdline, category, expected)

   def test_unknown_stays_other(self):
      """An unrecognized process must not be forced into a category.

      Every misfiled process inflates one category and deflates 'other',
      which is exactly the number an ALCF-4 sizing argument would rest on.
      """
      category, activity, confidence = probe._classify(
         "mysteryd", "/usr/libexec/mysteryd --serve")
      assert category == "other"
      assert activity is None
      assert confidence == "unknown"

   def test_confidence_path_qualified_vs_heuristic(self):
      _, _, absolute = probe._classify(
         "claude", "/soft/nodejs/bin/claude --resume")
      _, _, bare = probe._classify("claude", "claude")
      assert absolute == "path_qualified"
      assert bare == "argv_heuristic"

   def test_ai_agent_beats_compute_build(self):
      """Ordering hazard: an agent launched via python must stay an agent.

      Both rules match 'python ... claude'; category order decides. If
      compute/build won, the fleet's ai-coding-agent count would silently
      lose every python-launched agent.
      """
      category, _, _ = probe._classify(
         "python", "/soft/python/bin/python -m claude_code.cli")
      assert category == "ai-coding-agent"

   def test_project_path_hint(self):
      assert probe._project_from_path(
         "/lus/eagle/projects/datascience/run.sh") == "datascience"
      assert probe._project_from_path("/home/u/run.sh") is None


class TestBehavior:
   def test_tty_implies_interactive(self):
      assert probe._behavior(34816, 100, "bash") == "interactive"

   def test_no_tty_parent_init_is_daemon(self):
      assert probe._behavior(0, 1, "sshd") == "daemon"

   def test_no_tty_live_parent_is_batch(self):
      assert probe._behavior(0, 4242, "python") == "batch"


# --------------------------------------------------------------------------
# Census over a synthetic /proc
# --------------------------------------------------------------------------

class TestCensus:
   def test_basic_census(self, proc_root):
      proc_root([
         {"pid": 100, "comm": "bash", "cmdline": "-bash", "tty_nr": 34816},
         {"pid": 200, "comm": "node", "tty_nr": 0,
          "cmdline": "/home/u/.vscode-server/bin/a/node server-main.js"},
      ])
      rows, coverage = probe._collect_processes(
         {0: "root"}, drop_raw_args=True, deadline=None)
      assert coverage["pids_seen"] == 2
      by_pid = {row["pid"]: row for row in rows}
      assert by_pid[100]["category"] == "shell/session"
      assert by_pid[100]["interactive"] is True
      assert by_pid[200]["category"] == "ide-remote"
      assert by_pid[200]["behavior"] == "daemon"

   def test_kernel_threads_excluded(self, proc_root):
      """Empty cmdline = kernel thread. Not user behavior; must not appear.

      Counting them would add hundreds of phantom 'other' processes per node.
      """
      proc_root([
         {"pid": 2, "comm": "kthreadd", "cmdline": None},
         {"pid": 100, "comm": "bash", "cmdline": "-bash"},
      ])
      rows, coverage = probe._collect_processes(
         {0: "root"}, drop_raw_args=True, deadline=None)
      assert [row["pid"] for row in rows] == [100]
      assert coverage["cmdline_empty"] == 1
      assert coverage["pids_seen"] == 2

   def test_raw_args_dropped_by_default(self, proc_root):
      proc_root([{"pid": 100, "comm": "git",
                  "cmdline": "git clone https://user:secret@host/r.git"}])
      rows, _ = probe._collect_processes(
         {0: "root"}, drop_raw_args=True, deadline=None)
      assert "cmdline" not in rows[0]
      assert "secret" not in json.dumps(rows[0])

   def test_raw_args_kept_when_requested(self, proc_root):
      proc_root([{"pid": 100, "comm": "git", "cmdline": "git status"}])
      rows, _ = probe._collect_processes(
         {0: "root"}, drop_raw_args=False, deadline=None)
      assert rows[0]["cmdline"] == "git status"

   def test_unreadable_stat_counted_not_fatal(self, proc_root, monkeypatch):
      """Another user's process denies reads. Expected, must be counted."""
      root = proc_root([
         {"pid": 100, "comm": "bash", "cmdline": "-bash"},
         {"pid": 300, "comm": "secret", "cmdline": "secret"},
      ])
      real_read = probe._read_text

      def denying_read(path):
         if path.endswith(os.path.join("300", "stat")):
            return None
         return real_read(path)

      monkeypatch.setattr(probe, "_read_text", denying_read)
      rows, coverage = probe._collect_processes(
         {0: "root"}, drop_raw_args=True, deadline=None)
      assert coverage["stat_unreadable"] == 1
      assert coverage["pids_seen"] == 2
      assert len(rows) == 1

   def test_username_null_when_unresolvable(self, proc_root):
      """An unresolvable uid must stay null, never be invented."""
      proc_root([{"pid": 100, "comm": "bash", "cmdline": "-bash"}])
      rows, coverage = probe._collect_processes(
         {}, drop_raw_args=True, deadline=None)
      assert rows[0]["username"] is None
      assert coverage["owner_unresolved"] == 1

   def test_rss_converted_from_pages(self, proc_root):
      proc_root([{"pid": 100, "comm": "bash", "cmdline": "-bash",
                  "rss_pages": 2048}])
      rows, _ = probe._collect_processes(
         {0: "root"}, drop_raw_args=True, deadline=None)
      page_kb = os.sysconf("SC_PAGESIZE") // 1024
      assert rows[0]["rss_kb"] == 2048 * page_kb


# --------------------------------------------------------------------------
# Node counters
# --------------------------------------------------------------------------

class TestCounters:
   def test_counters_parse(self, proc_root):
      proc_root([])
      counters = probe._collect_node_counters()
      assert counters["load1"] == 53.92
      assert counters["procs_running"] == 42
      assert counters["procs_total"] == 6616
      assert counters["cpu_jiffies"]["user"] == 24771747
      assert counters["cpu_jiffies"]["idle"] == 8264041757
      assert counters["mem"]["total_kb"] == 527954108
      assert counters["mem"]["available_kb"] == 403844552
      assert counters["socket_count"] == 1234

   def test_loopback_excluded_from_net(self, proc_root):
      proc_root([])
      net = probe._collect_node_counters()["net"]
      assert "lo" not in net
      assert net["pub0"]["rx_bytes"] == 5000
      assert net["pub0"]["tx_bytes"] == 7000

   def test_clk_tck_reported_not_assumed(self, proc_root):
      """Ticks-per-second must be reported, never hardcoded to 100.

      Every CPU-seconds figure downstream divides by this.
      """
      proc_root([])
      assert probe._collect_node_counters()["clk_tck"] == os.sysconf("SC_CLK_TCK")

   def test_missing_lustre_is_not_an_error(self, proc_root):
      proc_root([])
      assert probe._collect_node_counters()["md_ops"] == {}


# --------------------------------------------------------------------------
# Argument parsing and exit codes
# --------------------------------------------------------------------------

class TestArgs:
   def test_defaults(self):
      opts = probe._parse_args(["probe"])
      assert opts["loop"] == "census"
      assert opts["drop_raw_args"] is True

   def test_counter_loop(self):
      assert probe._parse_args(["probe", "--loop", "counter"])["loop"] == "counter"

   def test_bad_loop_rejected(self):
      with pytest.raises(ValueError):
         probe._parse_args(["probe", "--loop", "nonsense"])

   def test_unknown_arg_rejected(self):
      with pytest.raises(ValueError):
         probe._parse_args(["probe", "--wat"])


class TestExitCodes:
   def _run(self, args, stdin=""):
      return subprocess.run(
         [sys.executable, PROBE_PATH] + args,
         input=stdin, capture_output=True, text=True, timeout=60)

   def test_success_emits_one_json_line(self):
      result = self._run(["--loop", "counter"])
      assert result.returncode == 0
      lines = [line for line in result.stdout.split("\n") if line.strip()]
      assert len(lines) == 1, "probe must emit exactly one line"
      json.loads(lines[0])

   def test_bad_args_exit_2(self):
      assert self._run(["--loop", "bogus"]).returncode == 2

   def test_version_flag(self):
      result = self._run(["--version"])
      assert result.returncode == 0
      assert int(result.stdout.strip()) == probe.PROBE_VERSION


# --------------------------------------------------------------------------
# The JSON contract the daemon parses. Changing any of this must be a
# deliberate probe_version bump, not an accident.
# --------------------------------------------------------------------------

class TestPayloadContract:
   def _payload(self, loop):
      result = subprocess.run(
         [sys.executable, PROBE_PATH, "--loop", loop],
         capture_output=True, text=True, timeout=60)
      assert result.returncode == 0, result.stderr
      return json.loads(result.stdout)

   def test_counter_payload_shape(self):
      payload = self._payload("counter")
      for key in ("probe_version", "loop", "hostname_fqdn", "uptime_sec",
                  "monotonic_sec", "wall_clock_utc", "probe_self_seconds",
                  "counters", "python_version", "drop_raw_args"):
         assert key in payload, "missing %s" % key
      assert payload["loop"] == "counter"
      assert payload["probe_version"] == probe.PROBE_VERSION

   def test_counter_loop_emits_no_processes(self):
      """The counter loop must stay cheap.

      If it ever starts walking /proc, the two loops cost the same and the
      whole two-cadence design is pointless.
      """
      payload = self._payload("counter")
      assert "processes" not in payload
      assert "census_coverage" not in payload

   def test_census_payload_has_processes_and_coverage(self):
      payload = self._payload("census")
      assert isinstance(payload["processes"], list)
      assert payload["processes"], "census on a live host found no processes"
      coverage = payload["census_coverage"]
      assert coverage["pids_seen"] >= len(payload["processes"])

   def test_process_row_fields(self):
      payload = self._payload("census")
      required = {
         "pid", "ppid", "uid", "username", "comm", "category", "behavior",
         "activity", "activity_confidence", "project_path_hint",
         "utime_ticks", "stime_ticks", "rss_kb", "state",
         "start_time_ticks", "interactive",
      }
      for row in payload["processes"][:20]:
         missing = required - set(row)
         assert not missing, "row missing %s" % missing

   def test_identity_pair_present(self):
      """Delta math keys on (pid, start_time_ticks), never pid alone.

      A reused PID with lower cumulative CPU than its predecessor yields a
      negative delta that gets silently clamped to zero.
      """
      payload = self._payload("census")
      for row in payload["processes"][:20]:
         assert isinstance(row["pid"], int)
         assert isinstance(row["start_time_ticks"], int)

   def test_no_raw_cmdline_by_default(self):
      payload = self._payload("census")
      assert payload["drop_raw_args"] is True
      for row in payload["processes"]:
         assert "cmdline" not in row

   def test_stdout_is_pure_json(self):
      """Anything else on stdout makes the daemon's parse fail.

      Lmod and XALT both print to stdout on some ALCF shells.
      """
      result = subprocess.run(
         [sys.executable, PROBE_PATH, "--loop", "counter"],
         capture_output=True, text=True, timeout=60)
      assert result.stdout.count("\n") == 1
      json.loads(result.stdout)
