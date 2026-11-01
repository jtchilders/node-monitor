import asyncio
import signal

import pytest

import node_monitor
from node_monitor import version
from node_monitor.collector.fleet import build_sessions, run_sessions
from tests.collector.test_soak import config_for_fake_tree


class FakeSession:
   def __init__(self, crash: BaseException | None = None) -> None:
      self.crash = crash
      self.stopped = asyncio.Event()
      self.stop_calls = 0

   async def run(self) -> None:
      if self.crash is not None:
         raise self.crash
      await self.stopped.wait()

   def stop(self) -> None:
      self.stop_calls += 1
      self.stopped.set()


@pytest.mark.asyncio
async def test_run_sessions_returns_when_stop_is_set():
   sessions = {"n1": FakeSession(), "n2": FakeSession()}
   stop = asyncio.Event()
   before = signal.getsignal(signal.SIGTERM)
   asyncio.get_running_loop().call_later(0.2, stop.set)
   await _run_within(sessions, stop, 2.0)
   assert all(s.stop_calls == 1 for s in sessions.values())
   assert signal.getsignal(signal.SIGTERM) is before


async def _run_within(sessions, stop, limit, crashes=None):
   crashes = crashes if crashes is not None else []
   task = asyncio.create_task(
      run_sessions(sessions, stop=stop, on_crash=lambda label, exc: crashes.append((label, exc)))
   )
   done, _ = await asyncio.wait({task}, timeout=limit)
   assert done, "run_sessions did not return in time"
   task.result()


@pytest.mark.asyncio
async def test_run_sessions_reports_a_crashed_session():
   boom = RuntimeError("boom")
   sessions = {"n1": FakeSession(crash=boom), "n2": FakeSession()}
   stop = asyncio.Event()
   crashes = []
   task = asyncio.create_task(
      run_sessions(sessions, stop=stop, on_crash=lambda label, exc: crashes.append((label, exc)))
   )
   await asyncio.sleep(0.2)
   assert not task.done()
   assert crashes == [("n1", boom)]
   assert not sessions["n2"].stopped.is_set()
   stop.set()
   done, _ = await asyncio.wait({task}, timeout=2)
   assert done
   task.result()
   assert sessions["n2"].stopped.is_set()


@pytest.mark.asyncio
async def test_run_sessions_leaves_crash_reporting_to_on_crash(capsys):
   sessions = {"n1": FakeSession(crash=RuntimeError("boom"))}
   stop = asyncio.Event()
   asyncio.get_running_loop().call_later(0.2, stop.set)
   crashes = []
   await _run_within(sessions, stop, 2.0, crashes)
   assert [label for label, _ in crashes] == ["n1"]
   assert capsys.readouterr().err == ""


class TrackedSession(FakeSession):
   """Records whether its run() has returned, however it ended."""

   def __init__(self) -> None:
      super().__init__()
      self.finished = False

   async def run(self) -> None:
      try:
         await super().run()
      finally:
         self.finished = True


@pytest.mark.asyncio
async def test_cancelling_run_sessions_stops_and_awaits_every_session():
   sessions = {"n1": TrackedSession(), "n2": TrackedSession()}
   before = signal.getsignal(signal.SIGTERM)
   task = asyncio.create_task(run_sessions(sessions, on_crash=lambda label, exc: None))
   await asyncio.sleep(0.1)
   task.cancel()
   done, _ = await asyncio.wait({task}, timeout=2)
   assert done and task.cancelled()
   assert all(s.stop_calls == 1 and s.finished for s in sessions.values())
   assert signal.getsignal(signal.SIGTERM) is before


class UnstoppableSession(TrackedSession):
   """stop() raises without stopping, so only cancelling its task ends it."""

   def stop(self) -> None:
      self.stop_calls += 1
      raise RuntimeError("stop failed")


@pytest.mark.asyncio
async def test_a_failing_stop_does_not_leave_sessions_running():
   sessions = {"n1": UnstoppableSession(), "n2": TrackedSession()}
   stop = asyncio.Event()
   asyncio.get_running_loop().call_later(0.1, stop.set)
   await _run_within(sessions, stop, 2.0)
   assert all(s.stop_calls == 1 and s.finished for s in sessions.values())


def test_build_sessions_suffixes_lock_path_per_node(tmp_path):
   config = config_for_fake_tree(tmp_path, 2, 1, 5)
   sessions = build_sessions(
      config,
      on_message=lambda label, msg: None,
      on_event=lambda label, kind, detail: None,
      probe_overrides={"lock_path": "/tmp/probe.lock"},
   )
   assert list(sessions) == ["n1", "n2"]
   assert (tmp_path / "state" / "ssh_config").exists()
   payloads = {label: s._payload for label, s in sessions.items()}
   assert b"/tmp/probe.lock.n1" in payloads["n1"]
   assert b"/tmp/probe.lock.n2" in payloads["n2"]
   assert b"/tmp/probe.lock.n2" not in payloads["n1"]


def test_describe_outside_checkout_is_bare_version(tmp_path):
   assert version.describe(tmp_path) == node_monitor.__version__


def test_describe_inside_checkout_appends_git_describe(monkeypatch, tmp_path):
   monkeypatch.setattr(version, "_git_describe", lambda cwd: "abc123-dirty")
   assert version.describe(tmp_path) == f"{node_monitor.__version__}+abc123-dirty"
