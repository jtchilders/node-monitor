import time

from tests.probe import fake_proc


def row(pid, start, utime, stime):
   return {"pid": pid, "start_time": start, "utime": utime, "stime": stime}


def test_parse_stat_handles_spaces_and_parens(probe):
   comm, tail = probe.parse_stat(fake_proc.make_stat(7, "tmux: server (x)"))
   assert comm == "tmux: server (x)" and tail[0] == "S"


def test_parse_stat_rejects_short_tail(probe):
   assert probe.parse_stat("7 (a) S 1 2\n") is None
   assert probe.parse_stat("garbage") is None


def test_scan_excludes_kernel_threads_and_reads_status(probe, tmp_path):
   roots = fake_proc.write_tree(
      tmp_path,
      pids=[
         {"pid": 2, "comm": "kthreadd", "cmdline": None, "uid": 0},
         {
            "pid": 10,
            "comm": "bash",
            "cmdline": ["-bash"],
            "uid": 31112,
            "threads": 1,
            "tty_nr": 34817,
            "sid": 10,
         },
         {
            "pid": 11,
            "comm": "node",
            "cmdline": ["node", "/home/u/.cursor-server/x.js", "--port", "1"],
            "uid": 31112,
            "threads": 11,
            "ppid": 10,
            "rss_pages": 2048,
            "exe": "/usr/bin/node",
         },
      ],
   )
   rows, cov = probe.scan_processes(roots, {31112: "parton"})
   by_pid = {r["pid"]: r for r in rows}
   assert set(by_pid) == {10, 11} and cov["kernel_thread"] == 1
   assert cov["pids_seen"] == 3
   assert by_pid[11]["cmdline"] == "node /home/u/.cursor-server/x.js --port 1"
   assert by_pid[11]["argv0"] == "node"
   assert by_pid[11]["threads"] == 11 and by_pid[11]["username"] == "parton"
   assert by_pid[11]["uid"] == 31112
   assert by_pid[11]["parent_comm"] == "bash" and by_pid[11]["parent_kind"] == "shell"
   assert by_pid[10]["interactive"] and by_pid[10]["session_leader"]
   assert not by_pid[11]["interactive"] and not by_pid[11]["session_leader"]
   assert by_pid[11]["exe"] == "/usr/bin/node"
   assert by_pid[11]["rss_kb"] == 2048 * (probe.os.sysconf("SC_PAGE_SIZE") // 1024)


def test_unresolvable_uid_is_null_not_invented(probe, tmp_path):
   roots = fake_proc.write_tree(
      tmp_path, pids=[{"pid": 10, "comm": "x", "cmdline": ["x"], "uid": 4000000000}]
   )
   names = {}
   rows, cov = probe.scan_processes(roots, names)
   assert rows[0]["username"] is None and cov["owner_unresolved"] == 1
   assert names == {4000000000: None}


def test_parent_kind_and_direct_parent_resolution(probe, tmp_path):
   roots = fake_proc.write_tree(
      tmp_path,
      pids=[
         {"pid": 1, "comm": "systemd", "cmdline": ["/sbin/init"], "uid": 0},
         {"pid": 20, "comm": "sshd", "cmdline": ["sshd"], "uid": 0, "exe": "/usr/sbin/sshd"},
         {"pid": 21, "comm": "a", "cmdline": ["a"], "uid": 5, "ppid": 20},
         {"pid": 22, "comm": "tmux: server", "cmdline": ["tmux"], "uid": 5},
         {"pid": 23, "comm": "b", "cmdline": ["b"], "uid": 5, "ppid": 22},
         {"pid": 24, "comm": "c", "cmdline": ["c"], "uid": 5, "ppid": 999},
         {"pid": 25, "comm": "d", "cmdline": ["d"], "uid": 5, "ppid": 1},
         {"pid": 26, "comm": "e", "cmdline": ["e"], "uid": 5, "ppid": 25},
      ],
   )
   rows, cov = probe.scan_processes(roots, {0: "root", 5: "u"})
   by_pid = {r["pid"]: r for r in rows}
   assert by_pid[21]["parent_kind"] == "sshd"
   assert by_pid[21]["parent_exe"] == "/usr/sbin/sshd"
   assert by_pid[23]["parent_kind"] == "multiplexer" and by_pid[23]["under_screen_or_tmux"]
   assert by_pid[24]["parent_kind"] == "unknown" and by_pid[24]["parent_comm"] is None
   assert by_pid[25]["parent_kind"] == "init"
   assert by_pid[26]["parent_kind"] == "other"
   assert cov["parent_unresolved"] == 1


def test_tracker_active_idle_and_lower_bound(probe):
   t = probe.ProcessTracker(active_cpu_seconds_per_min=0.1, clk_tck=100)
   r = row(pid=5, start=1, utime=100, stime=0)
   t.update([r], now=0, interval_sec=None)
   assert r["cpu_seconds"] is None and r["cpu_state"] is None and "utime" not in r
   assert r["idle_for_sec"] is None
   r = row(pid=5, start=1, utime=150, stime=0)
   t.update([r], now=60, interval_sec=60)
   assert (r["cpu_seconds"], r["cpu_state"], r["idle_for_sec"]) == (0.5, "active", 0)
   r = row(pid=5, start=1, utime=150, stime=0)
   t.update([r], now=180, interval_sec=120)
   assert (r["cpu_state"], r["idle_for_sec"], r["idle_for_is_lower_bound"]) == ("idle", 120, False)


def test_never_active_process_from_session_start_is_lower_bound(probe):
   t = probe.ProcessTracker(0.1, 100)
   t.update([row(5, 1, 10, 0)], now=0, interval_sec=None)
   r = row(5, 1, 10, 0)
   t.update([r], now=60, interval_sec=60)
   r = row(5, 1, 10, 0)
   t.update([r], now=120, interval_sec=60)
   assert r["idle_for_sec"] == 120 and r["idle_for_is_lower_bound"] is True


def test_process_started_after_tracking_is_not_lower_bound(probe):
   t = probe.ProcessTracker(0.1, 100)
   t.update([row(1, 1, 0, 0)], now=0, interval_sec=None)
   t.update([row(1, 1, 0, 0), row(5, 2, 10, 0)], now=60, interval_sec=60)
   r = row(5, 2, 10, 0)
   t.update([row(1, 1, 0, 0), r], now=120, interval_sec=60)
   assert r["idle_for_sec"] == 60 and r["idle_for_is_lower_bound"] is False


def test_pid_reuse_is_new_process(probe):
   t = probe.ProcessTracker(0.1, 100)
   t.update([row(pid=5, start=1, utime=10_000, stime=0)], now=0, interval_sec=None)
   r = row(pid=5, start=999, utime=3, stime=0)
   t.update([r], now=60, interval_sec=60)
   assert r["cpu_seconds"] is None


def test_vanished_processes_are_pruned(probe):
   t = probe.ProcessTracker(0.1, 100)
   t.update([row(5, 1, 0, 0), row(6, 1, 0, 0)], now=0, interval_sec=None)
   assert len(t._state) == 2
   t.update([], now=60, interval_sec=60)
   assert len(t._state) == 0


def test_gpu_query_parses_output(probe, fake_bin):
   fake_bin(
      "nvidia-smi",
      'case "$1" in\n'
      '--query-gpu*) printf "0, 37, 1024, 40960\\n1, 0, 5, 40960\\n";;\n'
      '--query-compute-apps*) printf "4242, 900\\n4243, 12\\n";;\n'
      "esac",
   )
   gpus, by_pid, err = probe.query_gpus()
   assert err is None
   assert gpus == [
      {"index": 0, "util_pct": 37, "mem_used_mb": 1024, "mem_total_mb": 40960},
      {"index": 1, "util_pct": 0, "mem_used_mb": 5, "mem_total_mb": 40960},
   ]
   assert by_pid == {4242: 900, 4243: 12}


def test_gpu_query_missing_binary_is_silent(probe, monkeypatch, tmp_path):
   monkeypatch.setenv("PATH", str(tmp_path))
   assert probe.query_gpus() == ([], {}, None)


def test_gpu_query_failure_returns_empty(probe, fake_bin):
   fake_bin("nvidia-smi", "echo boom >&2; exit 1")
   assert probe.query_gpus() == ([], {}, "nvidia-smi exit 1")


def test_gpu_query_unparseable_output_is_error(probe, fake_bin):
   fake_bin("nvidia-smi", "echo not,a,table")
   gpus, by_pid, err = probe.query_gpus()
   assert gpus == [] and by_pid == {} and err


def test_gpu_query_timeout_is_bounded(probe, fake_bin):
   fake_bin("nvidia-smi", "sleep 30")
   start = time.monotonic()
   gpus, by_pid, err = probe.query_gpus(timeout=1.0)
   assert gpus == [] and "timeout" in err and time.monotonic() - start < 3
