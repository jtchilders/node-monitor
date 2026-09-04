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


@pytest.fixture
def fake_proc_env(tmp_path):
   """Build a synthetic /proc tree plus the env var to point a real
   subprocess invocation of the probe at it.

   The in-process `proc_root` fixture above monkeypatches the module
   attribute, which a child process started by `subprocess.run` cannot see.
   `NODE_MONITOR_PROC_ROOT` is the only channel that reaches a subprocess,
   which is exactly why it exists (see remote_probe.py's module docstring).
   This is what lets TestExitCodes and TestPayloadContract run on macOS,
   which has no real /proc, while still exercising the real subprocess exit
   codes and stdout contract -- not a monkeypatched in-process call.

   Populated with enough processes to give TestPayloadContract a non-empty
   `processes` list: an interactive shell, a vscode-server node process (a
   daemon, no tty), an sshd session, and a kernel thread (absent cmdline)
   to prove exclusion survives the real CLI path too.
   """
   root = str(tmp_path / "proc")
   fake_proc.write_proc(root, [
      {"pid": 2, "comm": "kthreadd", "cmdline": None},
      {"pid": 100, "comm": "bash", "cmdline": "-bash", "tty_nr": 34816},
      {"pid": 200, "comm": "node", "tty_nr": 0, "ppid": 1,
       "cmdline": "/home/u/.vscode-server/bin/a/node server-main.js"},
      {"pid": 300, "comm": "sshd", "cmdline": "sshd: someuser@pts/12"},
   ])
   env = dict(os.environ)
   env["NODE_MONITOR_PROC_ROOT"] = root
   return env


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
      """Absent cmdline = kernel thread. Not user behavior; must not appear.

      Counting them would add hundreds of phantom 'other' processes per node.
      """
      proc_root([
         {"pid": 2, "comm": "kthreadd", "cmdline": None},
         {"pid": 100, "comm": "bash", "cmdline": "-bash"},
      ])
      rows, coverage = probe._collect_processes(
         {0: "root"}, drop_raw_args=True, deadline=None)
      assert [row["pid"] for row in rows] == [100]
      assert coverage["kernel_thread"] == 1
      assert coverage["pids_seen"] == 2

   def test_empty_cmdline_file_also_excluded(self, proc_root):
      """cmdline file present but empty is the same signal as file absent.

      On a real Linux node a kernel thread's cmdline file exists and reads
      empty; the test fixture's 'absent' case simulates the same thing a
      different way. Both must be excluded, and this project keeps them in
      separate coverage buckets (kernel_thread vs cmdline_empty) only
      because the fixture can express both shapes -- not because an analyst
      is meant to draw a distinction between them.
      """
      proc_root([
         {"pid": 3, "comm": "kworker", "cmdline": ""},
         {"pid": 100, "comm": "bash", "cmdline": "-bash"},
      ])
      rows, coverage = probe._collect_processes(
         {0: "root"}, drop_raw_args=True, deadline=None)
      assert [row["pid"] for row in rows] == [100]
      assert coverage["cmdline_empty"] == 1
      assert coverage["pids_seen"] == 2

   def test_denied_cmdline_treated_as_kernel_thread(self, proc_root, monkeypatch):
      """EACCES on cmdline is indistinguishable from absent here -- and rare.

      /proc/<pid>/cmdline is normally world-readable on Linux, so a real
      denial would be unusual. _read_text collapses denial and absence into
      the same None; rather than invent a way to tell them apart, the probe
      picks the conservative reading (exclude, count as kernel_thread).
      """
      root = proc_root([
         {"pid": 100, "comm": "bash", "cmdline": "-bash"},
         {"pid": 300, "comm": "secret", "cmdline": "secret"},
      ])
      real_read = probe._read_text

      def denying_read(path):
         if path.endswith(os.path.join("300", "cmdline")):
            return None
         return real_read(path)

      monkeypatch.setattr(probe, "_read_text", denying_read)
      rows, coverage = probe._collect_processes(
         {0: "root"}, drop_raw_args=True, deadline=None)
      assert [row["pid"] for row in rows] == [100]
      assert coverage["kernel_thread"] == 1
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
   def _run(self, args, stdin="", env=None):
      return subprocess.run(
         [sys.executable, PROBE_PATH] + args,
         input=stdin, capture_output=True, text=True, timeout=60, env=env)

   def test_success_emits_one_json_line(self, fake_proc_env):
      result = self._run(["--loop", "counter"], env=fake_proc_env)
      assert result.returncode == 0, result.stderr
      lines = [line for line in result.stdout.split("\n") if line.strip()]
      assert len(lines) == 1, "probe must emit exactly one line"
      json.loads(lines[0])

   def test_bad_args_exit_2(self):
      assert self._run(["--loop", "bogus"]).returncode == 2

   def test_bad_proc_root_exit_3(self, tmp_path):
      """Confirms the exit-3 contract without depending on a real /proc.

      Points the probe at a directory that exists but has no uptime file --
      the same failure mode a denied/unmounted /proc produces on Linux.
      """
      env = dict(os.environ)
      env["NODE_MONITOR_PROC_ROOT"] = str(tmp_path / "empty")
      os.makedirs(env["NODE_MONITOR_PROC_ROOT"])
      result = self._run(["--loop", "counter"], env=env)
      assert result.returncode == 3

   def test_version_flag(self):
      result = self._run(["--version"])
      assert result.returncode == 0
      assert int(result.stdout.strip()) == probe.PROBE_VERSION


# --------------------------------------------------------------------------
# The JSON contract the daemon parses. Changing any of this must be a
# deliberate probe_version bump, not an accident.
# --------------------------------------------------------------------------

class TestPayloadContract:
   def _payload(self, loop, env):
      result = subprocess.run(
         [sys.executable, PROBE_PATH, "--loop", loop],
         capture_output=True, text=True, timeout=60, env=env)
      assert result.returncode == 0, result.stderr
      return json.loads(result.stdout)

   def test_counter_payload_shape(self, fake_proc_env):
      payload = self._payload("counter", fake_proc_env)
      for key in ("probe_version", "loop", "hostname_fqdn", "uptime_sec",
                  "monotonic_sec", "wall_clock_utc", "probe_self_seconds",
                  "counters", "python_version", "drop_raw_args"):
         assert key in payload, "missing %s" % key
      assert payload["loop"] == "counter"
      assert payload["probe_version"] == probe.PROBE_VERSION

   def test_counter_loop_emits_no_processes(self, fake_proc_env):
      """The counter loop must stay cheap.

      If it ever starts walking /proc, the two loops cost the same and the
      whole two-cadence design is pointless.
      """
      payload = self._payload("counter", fake_proc_env)
      assert "processes" not in payload
      assert "census_coverage" not in payload

   def test_census_payload_has_processes_and_coverage(self, fake_proc_env):
      payload = self._payload("census", fake_proc_env)
      assert isinstance(payload["processes"], list)
      assert payload["processes"], "census on the fake proc found no processes"
      coverage = payload["census_coverage"]
      assert coverage["pids_seen"] >= len(payload["processes"])

   def test_process_row_fields(self, fake_proc_env):
      payload = self._payload("census", fake_proc_env)
      required = {
         "pid", "ppid", "uid", "username", "comm", "category", "behavior",
         "activity", "activity_confidence", "project_path_hint",
         "utime_ticks", "stime_ticks", "rss_kb", "state",
         "start_time_ticks", "interactive",
      }
      for row in payload["processes"][:20]:
         missing = required - set(row)
         assert not missing, "row missing %s" % missing

   def test_identity_pair_present(self, fake_proc_env):
      """Delta math keys on (pid, start_time_ticks), never pid alone.

      A reused PID with lower cumulative CPU than its predecessor yields a
      negative delta that gets silently clamped to zero.
      """
      payload = self._payload("census", fake_proc_env)
      for row in payload["processes"][:20]:
         assert isinstance(row["pid"], int)
         assert isinstance(row["start_time_ticks"], int)

   def test_no_raw_cmdline_by_default(self, fake_proc_env):
      payload = self._payload("census", fake_proc_env)
      assert payload["drop_raw_args"] is True
      for row in payload["processes"]:
         assert "cmdline" not in row

   def test_stdout_is_pure_json(self, fake_proc_env):
      """Anything else on stdout makes the daemon's parse fail.

      Lmod and XALT both print to stdout on some ALCF shells.
      """
      result = subprocess.run(
         [sys.executable, PROBE_PATH, "--loop", "counter"],
         capture_output=True, text=True, timeout=60, env=fake_proc_env)
      assert result.stdout.count("\n") == 1
      json.loads(result.stdout)


# --------------------------------------------------------------------------
# hwinfo loop -- the static hardware inventory
# --------------------------------------------------------------------------

class TestHwinfoLoop:
   def _hwinfo_env(self, tmp_path, cpu_max_freq_khz=2000000,
                    nvidia_gpus=None, no_sys=False):
      proc_root = str(tmp_path / "proc")
      sys_root = str(tmp_path / "sys")
      fake_proc.write_proc(proc_root, [])
      fake_proc.write_proc_hwinfo(proc_root, nvidia_gpus=nvidia_gpus)
      env = dict(os.environ)
      env["NODE_MONITOR_PROC_ROOT"] = proc_root
      if not no_sys:
         fake_proc.write_sys_hwinfo(
            sys_root, cpu_max_freq_khz=cpu_max_freq_khz)
         env["NODE_MONITOR_SYS_ROOT"] = sys_root
      else:
         env["NODE_MONITOR_SYS_ROOT"] = str(tmp_path / "sys-does-not-exist")
      return env

   def _run(self, env):
      result = subprocess.run(
         [sys.executable, PROBE_PATH, "--loop", "hwinfo"],
         capture_output=True, text=True, timeout=60, env=env)
      assert result.returncode == 0, result.stderr
      lines = [line for line in result.stdout.split("\n") if line.strip()]
      assert len(lines) == 1, "probe must emit exactly one line"
      return json.loads(lines[0])

   def test_hwinfo_payload_shape(self, tmp_path):
      env = self._hwinfo_env(tmp_path)
      payload = self._run(env)
      for key in ("probe_version", "loop", "hostname_fqdn",
                  "wall_clock_utc", "probe_self_seconds", "python_version"):
         assert key in payload, "missing %s" % key
      assert payload["loop"] == "hwinfo"
      assert "hardware" in payload
      hw = payload["hardware"]
      required = {
         "cpu_model", "cpu_logical", "sockets", "cores_per_socket",
         "cpu_max_freq_khz", "numa_nodes", "mem_total_kb", "swap_total_kb",
         "hugepage_size_kb", "kernel_release", "os_pretty_name",
         "net_fs_mounts", "net_ifaces", "boot_id", "btime", "gpus",
      }
      missing = required - set(hw)
      assert not missing, "hardware payload missing %s" % missing

   def test_hwinfo_loop_has_no_counters_or_processes(self, tmp_path):
      """hwinfo must stay cheap -- no per-60s counters, no process walk."""
      env = self._hwinfo_env(tmp_path)
      payload = self._run(env)
      assert "counters" not in payload
      assert "processes" not in payload
      assert "census_coverage" not in payload

   def test_hwinfo_values(self, tmp_path):
      env = self._hwinfo_env(tmp_path)
      hw = self._run(env)["hardware"]
      assert hw["cpu_model"] == "AMD EPYC 7713 64-Core Processor"
      assert hw["cpu_logical"] == 2
      assert hw["sockets"] == 2
      assert hw["cores_per_socket"] == 64
      assert hw["numa_nodes"] == 2
      assert hw["net_fs_mounts"] == 2
      assert hw["net_ifaces"] == {"bond0": 1000, "ens10f0": 1000}
      assert hw["boot_id"] == "abc-123"
      assert hw["btime"] == 1700000000
      assert hw["kernel_release"] == "6.4.0"

   def test_cpu_max_freq_khz_read_from_sysfs(self, tmp_path):
      env = self._hwinfo_env(tmp_path, cpu_max_freq_khz=2098489)
      hw = self._run(env)["hardware"]
      assert hw["cpu_max_freq_khz"] == 2098489

   def test_cpu_max_freq_khz_null_when_sysfs_absent(self, tmp_path):
      """Absent sysfs file -> NULL. Must NEVER fall back to /proc/cpuinfo
      MHz -- that field is a live DVFS reading, not a hardware fact."""
      env = self._hwinfo_env(tmp_path, no_sys=True)
      hw = self._run(env)["hardware"]
      assert hw["cpu_max_freq_khz"] is None

   def test_missing_nvidia_smi_yields_empty_gpus(self, tmp_path, monkeypatch):
      monkeypatch.setenv("PATH", str(tmp_path / "empty-bin"))
      os.makedirs(str(tmp_path / "empty-bin"), exist_ok=True)
      env = self._hwinfo_env(tmp_path)
      env["PATH"] = str(tmp_path / "empty-bin")
      result = subprocess.run(
         [sys.executable, PROBE_PATH, "--loop", "hwinfo"],
         capture_output=True, text=True, timeout=60, env=env)
      assert result.returncode == 0, result.stderr
      payload = json.loads(result.stdout)
      assert payload["hardware"]["gpus"] == []

   def test_nvidia_gpus_from_proc_driver(self, tmp_path):
      env = self._hwinfo_env(tmp_path, nvidia_gpus=["0000:01:00.0"])
      hw = self._run(env)["hardware"]
      assert hw["gpus"] == ["0000:01:00.0"]

   def test_no_instantaneous_frequency_field_anywhere(self, tmp_path):
      """The specific mistake this task exists to prevent: cpu MHz is a
      live per-core DVFS reading (measured on the real fleet: 133/110/97
      distinct values across 256 cores in samples 2s apart) and must never
      appear in the static hardware payload under any name."""
      env = self._hwinfo_env(tmp_path)
      payload = self._run(env)
      raw = json.dumps(payload).lower()
      for forbidden in ("cpu_mhz", "\"mhz\"", "cur_freq", "curfreq"):
         assert forbidden not in raw, (
            "forbidden instantaneous-frequency field found: %s" % forbidden)
      # Confirm the allowed static field is exactly the one we expect.
      assert "cpu_max_freq_khz" in payload["hardware"]


# --------------------------------------------------------------------------
# In-process hwinfo collectors (proc_root fixture, no subprocess)
# --------------------------------------------------------------------------

class TestHwinfoCollectors:
   def test_collect_cpuinfo(self, proc_root):
      root = proc_root([])
      fake_proc.write_proc_hwinfo(root)
      cpuinfo = probe._collect_cpuinfo()
      assert cpuinfo["cpu_model"] == "AMD EPYC 7713 64-Core Processor"
      assert cpuinfo["cpu_logical"] == 2
      assert cpuinfo["sockets"] == 2
      assert cpuinfo["cores_per_socket"] == 64

   def test_collect_cpuinfo_absent_sockets_is_none(self, proc_root):
      root = proc_root([])
      fake_proc.write_proc_hwinfo(
         root, cpuinfo="processor\t: 0\nmodel name\t: Some CPU\n")
      cpuinfo = probe._collect_cpuinfo()
      assert cpuinfo["sockets"] is None
      assert cpuinfo["cores_per_socket"] is None

   def test_collect_net_fs_mounts_counts_only_network_fs(self, proc_root):
      root = proc_root([])
      fake_proc.write_proc_hwinfo(root, mounts=(
         "/dev/sda1 / ext4 rw 0 0\n"
         "tmpfs /tmp tmpfs rw 0 0\n"
         "srv:/a /a nfs rw 0 0\n"
         "srv:/b /b nfs4 rw 0 0\n"
         "mds@o2ib:/fs /lus lustre rw 0 0\n"))
      assert probe._collect_net_fs_mounts() == 3

   def test_collect_boot_id_strips_newline(self, proc_root):
      root = proc_root([])
      fake_proc.write_proc_hwinfo(root, boot_id="deadbeef-1234\n")
      assert probe._collect_boot_id() == "deadbeef-1234"

   def test_collect_btime(self, proc_root):
      root = proc_root([])
      fake_proc.write_proc_hwinfo(
         root, stat_line="cpu  1 2 3 4\nbtime 1699999999\n")
      assert probe._collect_btime() == 1699999999

   def test_collect_os_pretty_name(self, proc_root, tmp_path, monkeypatch):
      root = proc_root([])
      fake_proc.write_proc_hwinfo(root)
      os_release = tmp_path / "os-release"
      os_release.write_text(
         'NAME="SLES"\nPRETTY_NAME="SUSE Linux Enterprise Server 15 SP7"\n')
      monkeypatch.setattr(probe, "OS_RELEASE_PATH", str(os_release))
      assert probe._collect_os_pretty_name() == (
         "SUSE Linux Enterprise Server 15 SP7")

   def test_collect_os_pretty_name_null_when_absent(
         self, proc_root, tmp_path, monkeypatch):
      root = proc_root([])
      fake_proc.write_proc_hwinfo(root)
      monkeypatch.setattr(
         probe, "OS_RELEASE_PATH", str(tmp_path / "does-not-exist"))
      assert probe._collect_os_pretty_name() is None

   def test_collect_gpus_empty_without_nvidia(self, proc_root, monkeypatch):
      root = proc_root([])
      fake_proc.write_proc_hwinfo(root)
      monkeypatch.setattr(
         probe, "_collect_gpus_nvidia_smi", lambda: [])
      assert probe._collect_gpus() == []

   def test_collect_gpus_nvidia_smi_hang_yields_empty(self, monkeypatch):
      """A hanging/failing nvidia-smi must never propagate -- [] always."""
      import subprocess as sp

      def _raising_run(*args, **kwargs):
         raise sp.TimeoutExpired(cmd="nvidia-smi", timeout=3)

      monkeypatch.setattr("shutil.which", lambda name: "/usr/bin/nvidia-smi")
      monkeypatch.setattr(sp, "run", _raising_run)
      assert probe._collect_gpus_nvidia_smi() == []
