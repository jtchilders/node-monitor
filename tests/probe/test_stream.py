"""Probe streaming loop, per-node lock, and self-termination (run as subprocesses)."""

import json
import os
import select
import signal
import subprocess
import sys
import time

import pytest

from tests.probe import fake_proc
from tests.probe.conftest import PROBE_PATH

PROBE = str(PROBE_PATH)
# Lets the subprocess tests run the probe under the login nodes' oldest interpreter.
PROBE_PYTHON = os.environ.get("NODE_MONITOR_PROBE_PYTHON", sys.executable)

# Runs the probe's real bootstrap entry point. The marker as the first line of the -c
# code puts it in the probe's cmdline, as the bootstrap does.
STREAM_CODE = (
   "#node-monitor-probe\n"
   "import runpy,sys,json\n"
   "runpy.run_path(%r, run_name='__node_monitor_probe__',\n"
   "   init_globals={'PROBE_CONFIG': json.loads(sys.argv[1])})\n" % PROBE
)

PIDS = [
   {"pid": 10, "comm": "bash", "cmdline": ["-bash"], "ppid": 1, "sid": 10},
   {"pid": 11, "comm": "python", "cmdline": ["python", "train.py"], "ppid": 10},
]


class Probe(object):
   """A probe subprocess plus a select-based line reader (no reader thread, so the
   test can close stdout without racing a blocked read)."""

   def __init__(self, popen):
      self.popen = popen
      self.messages = []
      self._buf = b""

   def __getattr__(self, name):
      return getattr(self.popen, name)

   def read_messages(self, until=None, count=None, timeout=5.0):
      if until is None:
         until = lambda ms: len(ms) >= count  # noqa: E731
      deadline = time.monotonic() + timeout
      fd = self.popen.stdout.fileno()
      while not until(self.messages):
         remaining = deadline - time.monotonic()
         if remaining <= 0:
            raise AssertionError("timed out; got %r" % [m["type"] for m in self.messages])
         ready, _, _ = select.select([fd], [], [], remaining)
         if not ready:
            continue
         chunk = os.read(fd, 1 << 20)
         if not chunk:
            if until(self.messages):
               break
            raise AssertionError("probe stdout EOF; got %r" % self.messages)
         self._buf += chunk
         *lines, self._buf = self._buf.split(b"\n")
         self.messages.extend(json.loads(line) for line in lines if line)
      return self.messages


def read_messages(p, **kwargs):
   return p.read_messages(**kwargs)


def _kill_group(pgid):
   try:
      os.killpg(pgid, signal.SIGKILL)
   except OSError:
      pass


@pytest.fixture
def spawn():
   """Start processes in their own session; kill each whole group at teardown."""
   started = []
   extra_groups = []

   def start(argv, **kwargs):
      kwargs.setdefault("stdin", subprocess.PIPE)
      kwargs.setdefault("stdout", subprocess.PIPE)
      popen = subprocess.Popen(argv, start_new_session=True, **kwargs)
      started.append(popen)
      return popen

   start.extra_groups = extra_groups
   yield start
   for pgid in extra_groups:
      _kill_group(pgid)
   for popen in started:
      _kill_group(popen.pid)
      for pipe in (popen.stdin, popen.stdout, popen.stderr):
         if pipe is not None:
            try:
               pipe.close()
            except OSError:
               pass
      try:
         popen.wait(timeout=5)
      except subprocess.TimeoutExpired:
         pass


@pytest.fixture
def run_stream(spawn):
   def run(config):
      return Probe(spawn([PROBE_PYTHON, "-u", "-c", STREAM_CODE, json.dumps(config)]))

   return run


def cfg(tmp_path, root=None, pids=PIDS, **overrides):
   roots = fake_proc.write_tree(root or (tmp_path / "tree"), pids=pids)
   config = {
      "proc_root": roots["proc"],
      "sys_root": roots["sys"],
      "lock_path": str(tmp_path / "lock"),
      "gpu": False,
      "counter_interval_sec": 0.2,
      "census_interval_sec": 0.5,
   }
   config.update(overrides)
   return config


def of_type(msgs, kind):
   return [m for m in msgs if m["type"] == kind]


def wait_for_pid_exit(pid, timeout):
   """True once pid is gone or a zombie (it is not our child, so we cannot reap it)."""
   deadline = time.monotonic() + timeout
   while time.monotonic() < deadline:
      try:
         with open("/proc/%d/stat" % pid) as handle:
            state = handle.read().rsplit(")", 1)[1].split()[0]
      except (OSError, IndexError):
         return True
      if state in ("Z", "X"):
         return True
      time.sleep(0.05)
   return False


def wait_for_file(path, timeout=5.0):
   deadline = time.monotonic() + timeout
   while time.monotonic() < deadline:
      if path.exists() and path.read_text().strip():
         return path.read_text()
      time.sleep(0.02)
   raise AssertionError("%s never written" % path)


# --------------------------------------------------------------------------
# Messages
# --------------------------------------------------------------------------


def test_stream_emits_hello_then_counters_and_census(tmp_path, run_stream):
   p = run_stream(cfg(tmp_path))
   msgs = read_messages(p, until=lambda ms: any(m["type"] == "census" for m in ms), timeout=5)
   assert msgs[0]["type"] == "hello" and msgs[0]["probe_version"] == 1
   assert [m["seq"] for m in msgs] == list(range(len(msgs)))
   assert any(m["type"] == "counters" and "probe_self" in m for m in msgs)


def test_first_counters_and_census_follow_hello_immediately(tmp_path, run_stream):
   p = run_stream(cfg(tmp_path, counter_interval_sec=30, census_interval_sec=30))
   msgs = read_messages(p, count=3, timeout=5)
   assert [m["type"] for m in msgs] == ["hello", "counters", "census"]


def test_message_envelope_and_hello_fields(tmp_path, run_stream):
   p = run_stream(cfg(tmp_path))
   msgs = read_messages(p, count=3, timeout=5)
   for m in msgs:
      assert isinstance(m["seq"], int)
      time.strptime(m["ts"][:-5], "%Y-%m-%dT%H:%M:%S")
      assert m["ts"][-5] == "." and m["ts"][-4:-1].isdigit() and m["ts"].endswith("Z")
   hello = msgs[0]
   assert hello["pid"] == p.pid and hello["boot_id"] == "boot-a"
   assert hello["btime"] == 1760000000 and hello["hardware"]["cpu_model"] == "Test CPU"
   assert hello["python_version"].startswith("%d." % sys.version_info[0])
   assert hello["hostname_fqdn"]
   probe_self = msgs[1]["probe_self"]
   assert set(probe_self) == {"cpu_seconds", "children_cpu_seconds", "maxrss_kb"}
   assert msgs[1]["load1"] == 53.92


def test_second_census_has_cpu_deltas(tmp_path, run_stream):
   p = run_stream(cfg(tmp_path))
   msgs = read_messages(p, until=lambda ms: len(of_type(ms, "census")) >= 2, timeout=5)
   first, second = of_type(msgs, "census")[:2]
   assert first["interval_sec"] is None
   assert all(r["cpu_seconds"] is None for r in first["processes"])
   assert second["interval_sec"] > 0 and second["scan_seconds"] >= 0
   assert {r["pid"] for r in second["processes"]} == {10, 11}
   assert all(r["cpu_seconds"] is not None for r in second["processes"])
   assert all(r["gpu_mem_mb"] is None for r in second["processes"])
   assert second["gpus"] == [] and second["coverage"]["pids_seen"] == 2


def test_collection_exceptions_are_reported_and_loop_continues(tmp_path, run_stream):
   # A deep root makes the exception text (which carries the path) longer than the cap.
   root = tmp_path / ("r" * 200) / ("s" * 200)
   config = cfg(tmp_path, root=root)
   proc = root / "proc"
   (proc / "loadavg").unlink()
   (proc / "loadavg").mkdir()  # reading a directory raises, unlike a missing file
   (proc / "11" / "stat").unlink()
   (proc / "11" / "stat").mkdir()
   p = run_stream(config)

   def enough(ms):
      errors = of_type(ms, "error")
      return (
         sum(e["where"] == "census" for e in errors) >= 2
         and sum(e["where"] == "counters" for e in errors) >= 3
      )

   msgs = read_messages(p, until=enough, timeout=8)
   assert p.poll() is None
   assert of_type(msgs, "counters") == [] and of_type(msgs, "census") == []
   for err in of_type(msgs, "error"):
      assert 0 < len(err["message"]) <= 500
   assert [m["seq"] for m in msgs] == list(range(len(msgs)))


# --------------------------------------------------------------------------
# Self-termination
# --------------------------------------------------------------------------


def test_stdin_eof_terminates_probe(tmp_path, run_stream):
   p = run_stream(cfg(tmp_path))
   read_messages(p, count=1, timeout=5)
   p.stdin.close()
   assert p.wait(timeout=3) == 0


def test_nonblocking_stdin_does_not_end_probe(tmp_path, spawn):
   config = json.dumps(cfg(tmp_path))
   argv = [PROBE_PYTHON, "-u", "-c", STREAM_CODE, config]
   p = Probe(spawn(argv, preexec_fn=lambda: os.set_blocking(0, False)))
   read_messages(p, count=1, timeout=5)
   time.sleep(1.5)
   assert p.poll() is None
   p.stdin.close()
   assert p.wait(timeout=3) == 0


def test_closed_stdout_terminates_probe(tmp_path, run_stream):
   p = run_stream(cfg(tmp_path))
   read_messages(p, count=1, timeout=5)
   p.stdout.close()
   assert p.wait(timeout=3) == 0


def run_under_shell(spawn, tmp_path, config):
   # A non-interactive sh points a background job's stdin at /dev/null, which would end
   # the probe by EOF. fd 3 keeps it on our pipe, so only the parent check can end it.
   pid_file = tmp_path / "probe.pid"
   script = 'exec 3<&0; "$@" <&3 & echo $! > "$PID_FILE"; wait'
   argv = ["sh", "-c", script, "sh", PROBE_PYTHON, "-u", "-c", STREAM_CODE, json.dumps(config)]
   wrapper = Probe(spawn(argv, env=dict(os.environ, PID_FILE=str(pid_file))))
   probe_pid = int(wait_for_file(pid_file))
   assert read_messages(wrapper, count=1, timeout=5)[0]["pid"] == probe_pid
   with open("/proc/%d/stat" % probe_pid) as handle:
      assert int(handle.read().rsplit(")", 1)[1].split()[1]) == wrapper.pid
   return wrapper, probe_pid


def test_parent_death_terminates_probe(tmp_path, spawn):
   wrapper, probe_pid = run_under_shell(spawn, tmp_path, cfg(tmp_path))
   os.kill(wrapper.pid, signal.SIGKILL)
   assert wait_for_pid_exit(probe_pid, timeout=5)
   assert wrapper.stdin.closed is False  # the pipe was open throughout


def test_parent_death_terminates_probe_blocked_on_write(tmp_path, spawn):
   # Census lines far bigger than a pipe buffer, and nobody reading them: the main loop
   # sits in write() and never reaches its own parent check.
   big = [
      {"pid": 100 + i, "comm": "python", "cmdline": ["python", "x" * 50000], "ppid": 1}
      for i in range(6)
   ]
   wrapper, probe_pid = run_under_shell(spawn, tmp_path, cfg(tmp_path, pids=big))
   time.sleep(0.5)
   os.kill(wrapper.pid, signal.SIGKILL)
   assert wait_for_pid_exit(probe_pid, timeout=5)


def test_probe_disables_core_dumps(tmp_path, run_stream):
   p = run_stream(cfg(tmp_path))
   read_messages(p, count=1, timeout=5)
   with open("/proc/%d/limits" % p.pid) as handle:
      line = [entry for entry in handle if entry.startswith("Max core file size")][0]
   assert line.split()[4:6] == ["0", "0"]


# --------------------------------------------------------------------------
# Lock
# --------------------------------------------------------------------------


def test_new_probe_takes_over_stale_probe(tmp_path, run_stream):
   old = run_stream(cfg(tmp_path))
   read_messages(old, count=1, timeout=5)
   new = run_stream(cfg(tmp_path))
   assert read_messages(new, count=1, timeout=8)[0]["type"] == "hello"
   assert old.wait(timeout=8) is not None  # old one was signalled
   assert (tmp_path / "lock").read_text().strip() == str(new.pid)


HOLD_LOCK_CODE = (
   "import fcntl,os,sys,time\n"
   "f=open(sys.argv[1],'a+')\n"
   "fcntl.flock(f,fcntl.LOCK_EX)\n"
   "f.truncate(0); f.write(sys.argv[2] or str(os.getpid())); f.flush()\n"
   "print('held',flush=True)\n"
   "time.sleep(120)\n"
)


def hold_lock_without_marker(spawn, path, content=""):
   holder = spawn([sys.executable, "-c", HOLD_LOCK_CODE, str(path), content])
   assert holder.stdout.readline().strip() == b"held"
   return holder


def test_lock_refuses_to_signal_non_probe(tmp_path, run_stream, spawn):
   holder = hold_lock_without_marker(spawn, tmp_path / "lock")
   p = run_stream(cfg(tmp_path))
   msgs = read_messages(p, count=1, timeout=5)
   assert msgs[0]["type"] == "error" and msgs[0]["where"] == "lock"
   assert str(holder.pid) in msgs[0]["message"]
   assert p.wait(timeout=3) == 3 and holder.poll() is None  # holder untouched


def test_lock_with_unreadable_holder_pid_is_refused(tmp_path, run_stream, spawn):
   holder = hold_lock_without_marker(spawn, tmp_path / "lock", content="garbage")
   p = run_stream(cfg(tmp_path))
   msgs = read_messages(p, count=1, timeout=8)
   assert msgs[0]["type"] == "error" and msgs[0]["where"] == "lock"
   assert p.wait(timeout=3) == 3 and holder.poll() is None


def test_stale_lock_file_naming_live_unrelated_pid_is_not_signalled(tmp_path, run_stream, spawn):
   # The holder died (no flock held) and its pid now belongs to an unrelated process.
   bystander = spawn(["sleep", "60"])
   (tmp_path / "lock").write_text(str(bystander.pid))
   p = run_stream(cfg(tmp_path))
   assert read_messages(p, count=1, timeout=5)[0]["type"] == "hello"
   assert bystander.poll() is None


def test_lock_never_signals_its_own_process(tmp_path, spawn):
   # Even when the recorded holder carries the marker, the probe must not signal itself.
   code = (
      "#node-monitor-probe\n"
      "import fcntl,os,runpy,sys\n"
      "g=runpy.run_path(%r, run_name='x')\n"
      "f=open(sys.argv[1],'a+'); fcntl.flock(f,fcntl.LOCK_EX)\n"
      "f.truncate(0); f.write(str(os.getpid())); f.flush()\n"
      "try:\n"
      "   g['ProbeLock'](sys.argv[1]).acquire()\n"
      "except g['LockHeldByOther'] as exc:\n"
      "   print('refused', exc.pid)\n" % PROBE
   )
   p = spawn([PROBE_PYTHON, "-c", code, str(tmp_path / "lock")])
   out, _ = p.communicate(timeout=10)
   assert p.returncode == 0 and out.split() == [b"refused", str(p.pid).encode()]


def test_lock_default_path_is_per_uid_in_dev_shm(probe):
   assert probe.default_config()["lock_path"] == "/dev/shm/node-monitor-%d.lock" % os.getuid()


# --------------------------------------------------------------------------
# GPU hygiene
# --------------------------------------------------------------------------

HANGING_SMI = (
   'echo "$$ $1" >> "%s"\ncase "$1" in --query-gpu=name*) echo "Fake GPU";; *) exec sleep 30;; esac'
)


def gpu_queries(log):
   if not log.exists():
      return []
   return [line for line in log.read_text().splitlines() if "--query-gpu=index" in line]


@pytest.fixture
def hanging_smi(tmp_path, fake_bin, spawn):
   log = tmp_path / "smi.log"
   fake_bin("nvidia-smi", HANGING_SMI % log)
   yield log
   # Each fake nvidia-smi runs in its own session; kill any the probe left behind.
   if log.exists():
      for line in log.read_text().splitlines():
         _kill_group(int(line.split()[0]))


def test_gpu_timeout_suspends_queries(tmp_path, run_stream, hanging_smi):
   config = cfg(tmp_path, gpu=True, gpu_timeout_sec=0.3, census_interval_sec=0.3)
   p = run_stream(config)
   msgs = read_messages(p, until=lambda ms: len(of_type(ms, "census")) >= 5, timeout=10)
   gpu_errors = [e for e in of_type(msgs, "error") if e["where"] == "gpu"]
   assert len(gpu_errors) == 1 and "timeout" in gpu_errors[0]["message"]
   assert len(gpu_queries(hanging_smi)) == 1
   assert all(c["gpus"] == [] for c in of_type(msgs, "census"))


def test_gpu_queries_resume_after_suspension(tmp_path, run_stream, hanging_smi):
   config = cfg(
      tmp_path, gpu=True, gpu_timeout_sec=0.3, census_interval_sec=0.3, gpu_suspend_sec=1.0
   )
   p = run_stream(config)

   def two_errors(ms):
      return len([e for e in of_type(ms, "error") if e["where"] == "gpu"]) >= 2

   msgs = read_messages(p, until=two_errors, timeout=10)
   assert len(gpu_queries(hanging_smi)) == 2
   censuses = [m["seq"] for m in of_type(msgs, "census")]
   gpu_error_seqs = [e["seq"] for e in of_type(msgs, "error") if e["where"] == "gpu"]
   # At least one census ran while suspended, between the two queries.
   assert any(gpu_error_seqs[0] < s < gpu_error_seqs[1] for s in censuses)


def test_gpu_disabled_spawns_no_nvidia_smi(tmp_path, run_stream, hanging_smi):
   p = run_stream(cfg(tmp_path, gpu=False))
   read_messages(p, until=lambda ms: len(of_type(ms, "census")) >= 2, timeout=5)
   assert not hanging_smi.exists()


def test_abandoned_gpu_queries_are_reaped(probe):
   child = subprocess.Popen(["true"])
   probe._ABANDONED_GPU_QUERIES.append(child)
   deadline = time.monotonic() + 5
   while probe._ABANDONED_GPU_QUERIES and time.monotonic() < deadline:
      probe.reap_abandoned_gpu_queries()
      time.sleep(0.02)
   assert probe._ABANDONED_GPU_QUERIES == [] and child.returncode == 0


def test_hardware_gpu_name_query_is_bounded(probe, tmp_path, fake_bin, monkeypatch):
   # The grandchild holds the pipes; the whole group must die, not just the shell.
   grandchild = tmp_path / "grandchild.pid"
   fake_bin("nvidia-smi", 'sleep 30 & echo $! > "%s"; wait' % grandchild)
   monkeypatch.setattr(probe, "HARDWARE_GPU_TIMEOUT_SEC", 0.5)
   roots = fake_proc.write_tree(tmp_path)
   start = time.monotonic()
   assert probe.collect_hardware(roots)["gpus"] == []
   assert time.monotonic() - start < 3
   assert wait_for_pid_exit(int(wait_for_file(grandchild)), timeout=2)


def test_hardware_skips_nvidia_smi_when_gpu_disabled(probe, tmp_path, fake_bin):
   log = tmp_path / "smi.log"
   fake_bin("nvidia-smi", 'echo called >> "%s"; echo "Fake GPU"' % log)
   roots = fake_proc.write_tree(tmp_path)
   assert probe.collect_hardware(roots, gpu=False)["gpus"] == []
   assert not log.exists()
   assert probe.collect_hardware(roots)["gpus"] == ["Fake GPU"]


# --------------------------------------------------------------------------
# --once
# --------------------------------------------------------------------------


def test_once_mode_on_real_proc():
   out = subprocess.run(
      [PROBE_PYTHON, PROBE, "--once"], stdout=subprocess.PIPE, check=True, timeout=60
   )
   msgs = [json.loads(line) for line in out.stdout.splitlines()]
   # A node whose nvidia-smi is broken adds a gpu error before the census; nothing else may.
   assert [m["type"] for m in msgs if m["type"] != "error"] == ["hello", "counters", "census"]
   assert all(m["where"] == "gpu" for m in msgs if m["type"] == "error")
   assert msgs[-1]["type"] == "census" and msgs[-1]["processes"]


def test_once_mode_on_fake_tree_attaches_gpu_memory(tmp_path, fake_bin):
   fake_bin(
      "nvidia-smi",
      'case "$1" in\n'
      '--query-gpu=name*) echo "Fake GPU";;\n'
      '--query-gpu*) echo "0, 37, 1024, 40960";;\n'
      '--query-compute-apps*) echo "11, 900";;\n'
      "esac",
   )
   roots = fake_proc.write_tree(tmp_path, pids=PIDS)
   out = subprocess.run(
      [PROBE_PYTHON, PROBE, "--once", "--proc-root", roots["proc"], "--sys-root", roots["sys"]],
      stdout=subprocess.PIPE,
      check=True,
      timeout=60,
   )
   msgs = [json.loads(line) for line in out.stdout.splitlines()]
   assert [(m["type"], m["seq"]) for m in msgs] == [("hello", 0), ("counters", 1), ("census", 2)]
   census = msgs[2]
   assert census["gpus"] == [
      {"index": 0, "util_pct": 37, "mem_used_mb": 1024, "mem_total_mb": 40960}
   ]
   assert {r["pid"]: r["gpu_mem_mb"] for r in census["processes"]} == {10: None, 11: 900}
   assert msgs[0]["hardware"]["gpus"] == ["Fake GPU"]


def test_cli_without_once_is_rejected():
   out = subprocess.run([PROBE_PYTHON, PROBE], stderr=subprocess.PIPE, timeout=30)
   assert out.returncode == 2 and b"--once" in out.stderr
