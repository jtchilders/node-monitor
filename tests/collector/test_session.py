import asyncio
import os
import signal
import sys
from pathlib import Path

import pytest

from node_monitor.collector.bootstrap import build_payload, probe_source, remote_command
from node_monitor.collector.session import NodeSession
from node_monitor.collector.ssh import ssh_argv
from tests.probe import fake_proc

pytestmark = pytest.mark.asyncio

FAKE_SSH = Path(__file__).resolve().with_name("fake_ssh.py")


@pytest.fixture(autouse=True)
def ssh_env(monkeypatch):
   # The site's XALT LD_PRELOAD can abort local interpreters at exit and dump core.
   monkeypatch.delenv("LD_PRELOAD", raising=False)
   monkeypatch.delenv("FAKE_SSH_FAIL", raising=False)
   monkeypatch.delenv("FAKE_SSH_STDERR_BYTES", raising=False)
   # fake_ssh's shebang and the remote command's `env` must both resolve.
   path = os.path.dirname(sys.executable) + os.pathsep + os.environ.get("PATH", "/usr/bin:/bin")
   monkeypatch.setenv("PATH", path)


class Rig:
   """A NodeSession wired to in-memory recorders, run as an async context manager."""

   def __init__(self, roots, **session_kwargs):
      self.roots = roots
      self.msgs = []
      self.events = []
      self._changed = asyncio.Event()
      self.session = NodeSession(
         "node-a", on_message=self._on_message, on_event=self._on_event, **session_kwargs
      )
      self.task = None

   def _on_message(self, label, msg):
      assert label == "node-a"
      self.msgs.append(msg)
      self._changed.set()

   def _on_event(self, label, kind, detail):
      assert label == "node-a"
      self.events.append((label, kind, detail))
      self._changed.set()

   async def __aenter__(self):
      self.task = asyncio.create_task(self.session.run())
      return self

   async def __aexit__(self, *exc):
      self.session.stop()
      await asyncio.wait_for(self.task, 30)

   async def wait(self, predicate, timeout):
      loop = asyncio.get_running_loop()
      deadline = loop.time() + timeout
      while not predicate(self.msgs, self.events):
         if self.task.done():
            self.task.result()
            raise AssertionError("run() returned before stop(); events=%r" % (self.events,))
         remaining = deadline - loop.time()
         if remaining <= 0:
            raise AssertionError("condition not met in %gs; events=%r" % (timeout, self.events))
         self._changed.clear()
         try:
            await asyncio.wait_for(self._changed.wait(), remaining)
         except asyncio.TimeoutError:
            pass


def make_session(
   tmp_path,
   *,
   probe_cfg=None,
   pids=None,
   command_prefix="",
   command=None,
   ssh_binary=None,
   silence_timeout=2,
   max_lifetime=3600.0,
   backoff=(0.05, 0.4),
   healthy_after=1,
):
   if pids is None:
      pids = [{"pid": 4242, "comm": "python", "cmdline": ["python", "x.py"]}]
   roots = fake_proc.write_tree(tmp_path / "tree", pids=pids)
   cfg = {
      "counter_interval_sec": 0.2,
      "census_interval_sec": 0.5,
      "active_cpu_seconds_per_min": 0.1,
      "gpu": False,
      # Per test, so no test's probe can take over another's.
      "lock_path": str(tmp_path / "probe.lock"),
      "proc_root": roots["proc"],
      "sys_root": roots["sys"],
   }
   cfg.update(probe_cfg or {})
   if command is None:
      command = command_prefix + remote_command(sys.executable)
   argv = ssh_argv(ssh_binary or str(FAKE_SSH), Path("/dev/null"), "node-a", command)
   return Rig(
      roots,
      argv=argv,
      payload=build_payload(probe_source(), cfg),
      silence_timeout=silence_timeout,
      max_lifetime=max_lifetime,
      backoff=backoff,
      healthy_after=healthy_after,
   )


async def run_until(rig, predicate, timeout):
   async with rig:
      await rig.wait(predicate, timeout)
   return rig.msgs, rig.events


def count(msgs, kind):
   return sum(1 for m in msgs if m["type"] == kind)


def count_events(events, kind):
   return sum(1 for _, k, _ in events if k == kind)


def details(events, kind):
   return [d for _, k, d in events if k == kind]


def reasons(events, kind):
   return [d["reason"] for d in details(events, kind)]


def first_lost(events):
   return details(events, "session_lost")[0]


def kinds_details(events):
   return [(k, d) for _, k, d in events]


def big_tree(n):
   return [
      {"pid": 1000 + i, "comm": "worker", "cmdline": ["python", "w%d.py" % i, "a" * 1024]}
      for i in range(n)
   ]


def hello_pids(msgs):
   return [m["pid"] for m in msgs if m["type"] == "hello"]


async def wait_gone(pid, timeout):
   loop = asyncio.get_running_loop()
   deadline = loop.time() + timeout
   while True:
      try:
         os.kill(pid, 0)
      except ProcessLookupError:
         return True
      if loop.time() >= deadline:
         return False
      await asyncio.sleep(0.05)


async def test_streams_messages(tmp_path):
   msgs, events = await run_until(make_session(tmp_path), lambda m, e: count(m, "census") >= 2, 10)
   assert msgs[0]["type"] == "hello" and events[0][1] == "session_started"
   assert events[0][2] == {"attempt": 1}
   assert count(msgs, "counters") >= 1
   assert not details(events, "session_lost")


async def test_reconnects_after_ssh_process_is_killed(tmp_path):
   rig = make_session(tmp_path)
   async with rig:
      await rig.wait(lambda m, e: count(m, "census") >= 1, 10)
      os.kill(rig.session.current_pid, signal.SIGKILL)
      await rig.wait(lambda m, e: count_events(e, "session_started") >= 2, 10)
      await rig.wait(lambda m, e: count(m, "hello") >= 2, 10)
   events = rig.events
   assert reasons(events, "session_lost")[0] in ("eof", "exit")
   assert count_events(events, "session_started") >= 2
   assert details(events, "session_started")[1] == {"attempt": 2}
   assert details(events, "reconnect_wait")[0]["failures"] == 1


async def test_silence_timeout_kills_and_reconnects(tmp_path):
   rig = make_session(
      tmp_path,
      probe_cfg={"counter_interval_sec": 100, "census_interval_sec": 100},
      silence_timeout=1,
   )
   async with rig:
      await rig.wait(lambda m, e: count_events(e, "session_lost") >= 1, 10)
      silenced_pid = hello_pids(rig.msgs)[0]
      assert await wait_gone(silenced_pid, 5)
      await rig.wait(lambda m, e: count_events(e, "session_started") >= 2, 10)
   assert "silence" in reasons(rig.events, "session_lost")
   assert first_lost(rig.events)["uptime_sec"] >= 1


# A remote side that ignores stdin EOF: a shell with a backgrounded member, which reports
# the member's pid in a hello line and then goes silent.
SILENT_GROUP = 'sleep 600 & echo "{\\"type\\": \\"hello\\", \\"pid\\": $!}"; wait'


async def test_silence_terminates_whole_process_group(tmp_path):
   rig = make_session(tmp_path, command=SILENT_GROUP, silence_timeout=1, backoff=(30, 30))
   async with rig:
      await rig.wait(lambda m, e: count_events(e, "session_lost") >= 1, 15)
      member = hello_pids(rig.msgs)[0]
      assert await wait_gone(member, 1)
   lost = first_lost(rig.events)
   assert lost["reason"] == "silence" and lost["exit_code"] == -signal.SIGTERM


async def test_group_ignoring_sigterm_is_killed(tmp_path):
   rig = make_session(
      tmp_path, command="trap '' TERM; " + SILENT_GROUP, silence_timeout=1, backoff=(30, 30)
   )
   async with rig:
      await rig.wait(lambda m, e: count_events(e, "session_lost") >= 1, 20)
      member = hello_pids(rig.msgs)[0]
      assert await wait_gone(member, 1)
   lost = first_lost(rig.events)
   assert lost["reason"] == "silence" and lost["exit_code"] == -signal.SIGKILL


async def test_rotation_reconnects_without_backoff(tmp_path):
   rig = make_session(tmp_path, max_lifetime=1.0)
   msgs, events = await run_until(
      rig,
      lambda m, e: count_events(e, "session_rotated") >= 1 and count(m, "hello") >= 2,
      15,
   )
   assert count_events(events, "session_rotated") >= 1
   assert details(events, "session_rotated")[0]["uptime_sec"] >= 1.0
   assert not [e for e in events if e[1] == "reconnect_wait"]
   assert not details(events, "session_lost")
   pids = hello_pids(msgs)
   assert pids[0] != pids[1]


async def test_node_reboot_detected_on_boot_id_change(tmp_path):
   rig = make_session(tmp_path)
   boot_id = Path(rig.roots["proc"]) / "sys" / "kernel" / "random" / "boot_id"
   async with rig:
      await rig.wait(lambda m, e: count(m, "hello") >= 1, 10)
      boot_id.write_text("boot-b\n")
      os.kill(rig.session.current_pid, signal.SIGKILL)
      await rig.wait(lambda m, e: count_events(e, "node_rebooted") >= 1, 10)
   assert ("node_rebooted", {"old_boot_id": "boot-a", "new_boot_id": "boot-b"}) in kinds_details(
      rig.events
   )


async def test_same_boot_id_is_not_a_reboot(tmp_path):
   rig = make_session(tmp_path)
   async with rig:
      await rig.wait(lambda m, e: count(m, "hello") >= 1, 10)
      os.kill(rig.session.current_pid, signal.SIGKILL)
      await rig.wait(lambda m, e: count(m, "hello") >= 2, 10)
   assert not details(rig.events, "node_rebooted")


async def test_spawn_failure_backs_off_with_growing_delay(tmp_path, monkeypatch):
   monkeypatch.setenv("FAKE_SSH_FAIL", "1")
   msgs, events = await run_until(
      make_session(tmp_path), lambda m, e: count_events(e, "reconnect_wait") >= 3, 10
   )
   waits = details(events, "reconnect_wait")
   delays = [d["delay_sec"] for d in waits]
   assert len(delays) >= 3 and max(delays) <= 0.4
   assert [d["failures"] for d in waits[:3]] == [1, 2, 3]
   # Jitter scales each base delay (0.05, 0.1, 0.2) into [base/2, base].
   for delay, base in zip(delays, (0.05, 0.1, 0.2), strict=False):
      assert base / 2 <= delay <= base
   assert reasons(events, "session_lost")[0] == "exit"
   assert first_lost(events)["exit_code"] == 255
   assert "Permission denied" in first_lost(events)["stderr_tail"]
   assert msgs == []


async def test_missing_ssh_binary_is_spawn_failed(tmp_path):
   rig = make_session(tmp_path, ssh_binary=str(tmp_path / "no-such-ssh"))
   _, events = await run_until(rig, lambda m, e: count_events(e, "reconnect_wait") >= 2, 10)
   lost = first_lost(events)
   assert lost["reason"] == "spawn_failed" and lost["exit_code"] is None
   assert not details(events, "session_started")


async def test_failures_reset_after_healthy_session(tmp_path):
   rig = make_session(tmp_path)
   async with rig:
      for n in (1, 2):
         await rig.wait(lambda m, e, n=n: count(m, "hello") >= n, 10)
         await asyncio.sleep(1.2)  # outlive healthy_after=1
         os.kill(rig.session.current_pid, signal.SIGKILL)
         await rig.wait(lambda m, e, n=n: count_events(e, "reconnect_wait") >= n, 10)
   assert [d["failures"] for d in details(rig.events, "reconnect_wait")] == [1, 1]


async def test_large_census_line_is_read(tmp_path):
   # 600 fake processes with 1 KiB cmdlines -> census line > 600 KiB
   msgs, events = await run_until(
      make_session(tmp_path, pids=big_tree(600)), lambda m, e: count(m, "census") >= 1, 15
   )
   assert len(next(m for m in msgs if m["type"] == "census")["processes"]) == 600
   assert not details(events, "malformed_line")


async def test_stderr_flood_does_not_block(tmp_path, monkeypatch):
   monkeypatch.setenv("FAKE_SSH_STDERR_BYTES", str(2 * 1024 * 1024))
   msgs, events = await run_until(make_session(tmp_path), lambda m, e: count(m, "census") >= 1, 15)
   assert count_events(events, "probe_stderr") <= 21
   for d in details(events, "probe_stderr"):
      assert len(d.get("line", "")) <= 500


async def test_stderr_flood_beyond_stream_buffer_does_not_block(tmp_path, monkeypatch):
   # More than asyncio buffers on its own (2 x the 32 MiB limit) before it stops reading.
   monkeypatch.setenv("FAKE_SSH_STDERR_BYTES", str(80 * 1024 * 1024))
   msgs, events = await run_until(make_session(tmp_path), lambda m, e: count(m, "census") >= 1, 30)
   assert count_events(events, "probe_stderr") <= 21


async def test_stderr_lines_are_truncated_and_capped(tmp_path):
   prefix = (
      'i=0; while [ $i -lt 30 ]; do echo "warn $i $(printf "%0600d" 0)" >&2; i=$((i+1)); done; '
   )
   rig = make_session(tmp_path, command_prefix=prefix)
   _, events = await run_until(
      rig,
      lambda m, e: count(m, "census") >= 1 and count_events(e, "probe_stderr") >= 20,
      10,
   )
   stderr = details(events, "probe_stderr")
   lines = [d["line"] for d in stderr if "line" in d]
   assert len(lines) == 20
   assert lines[0].startswith("warn 0 ") and lines[19].startswith("warn 19 ")
   assert all(len(line) == 500 for line in lines)
   assert [d for d in stderr if "suppressed" in d] == [{"suppressed": 10}]


async def test_malformed_line_is_reported_and_stream_continues(tmp_path):
   rig = make_session(tmp_path, command_prefix="echo 'this is not json'; ")
   msgs, events = await run_until(rig, lambda m, e: count(m, "census") >= 2, 10)
   malformed = details(events, "malformed_line")
   assert malformed and malformed[0]["sample"] == "this is not json"
   assert malformed[0]["bytes"] == len("this is not json")
   assert count(msgs, "hello") == 1
   assert not details(events, "session_lost")


async def test_malformed_sample_is_capped(tmp_path):
   rig = make_session(tmp_path, command_prefix="printf '%0900d\\n' 0; ")
   _, events = await run_until(rig, lambda m, e: count_events(e, "malformed_line") >= 1, 10)
   malformed = details(events, "malformed_line")[0]
   assert malformed["bytes"] == 900 and malformed["sample"] == "0" * 200


async def test_stop_terminates_remote_probe(tmp_path):
   rig = make_session(tmp_path)
   loop = asyncio.get_running_loop()
   async with rig:
      await rig.wait(lambda m, e: count(m, "census") >= 1, 10)
      probe_pid = hello_pids(rig.msgs)[0]
      stopped_at = loop.time()
   assert rig.task.done() and rig.task.result() is None
   assert await wait_gone(probe_pid, max(0.0, 5 - (loop.time() - stopped_at)))
   assert rig.session.current_pid is None
   assert not details(rig.events, "session_lost")


async def test_stop_during_backoff_returns_promptly(tmp_path, monkeypatch):
   monkeypatch.setenv("FAKE_SSH_FAIL", "1")
   rig = make_session(tmp_path, backoff=(30, 30))
   loop = asyncio.get_running_loop()
   async with rig:
      await rig.wait(lambda m, e: count_events(e, "reconnect_wait") >= 1, 10)
      stopped_at = loop.time()
   assert loop.time() - stopped_at < 2
