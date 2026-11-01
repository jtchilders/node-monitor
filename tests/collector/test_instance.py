import os
import subprocess
import sys

import pytest

from node_monitor.collector.instance import AlreadyRunning, InstanceLock, is_alive, read_info

_HOLDER = """
import sys, time
from pathlib import Path
from node_monitor.collector.instance import InstanceLock
InstanceLock(Path(sys.argv[1])).acquire()
print("ready", flush=True)
time.sleep(120)
"""


@pytest.fixture
def holder(tmp_path):
   """A child process holding the lock on tmp_path."""
   procs = []

   def start(env=None):
      proc = subprocess.Popen(
         [sys.executable, "-c", _HOLDER, str(tmp_path)],
         stdout=subprocess.PIPE,
         text=True,
         env={**os.environ, **(env or {})},
      )
      procs.append(proc)
      assert proc.stdout.readline().strip() == "ready"
      return proc

   yield start
   for proc in procs:
      proc.terminate()
      try:
         proc.wait(timeout=10)
      except subprocess.TimeoutExpired:
         proc.kill()
         proc.wait(timeout=10)
      proc.stdout.close()


def test_second_acquire_raises_already_running_with_info(tmp_path, holder):
   proc = holder()
   with pytest.raises(AlreadyRunning) as err:
      InstanceLock(tmp_path).acquire()
   assert err.value.info["pid"] == proc.pid
   assert is_alive(err.value.info)


def test_acquire_writes_private_info_and_release_removes_it(tmp_path):
   lock = InstanceLock(tmp_path)
   lock.acquire()
   info_path = tmp_path / "state" / "daemon.json"
   assert info_path.stat().st_mode & 0o777 == 0o600
   assert (tmp_path / "state").stat().st_mode & 0o777 == 0o700
   info = read_info(tmp_path)
   assert info["pid"] == os.getpid()
   assert info["systemd"] is False
   lock.release()
   assert not info_path.exists()
   assert read_info(tmp_path) is None
   # The lock is free again.
   again = InstanceLock(tmp_path)
   again.acquire()
   again.release()


def test_info_records_systemd_invocation(tmp_path, monkeypatch):
   monkeypatch.setenv("INVOCATION_ID", "abc")
   lock = InstanceLock(tmp_path)
   lock.acquire()
   try:
      assert read_info(tmp_path)["systemd"] is True
   finally:
      lock.release()


def test_is_alive_requires_same_host_and_live_pid():
   mine = {"pid": os.getpid(), "host": __import__("socket").gethostname()}
   assert is_alive(mine)
   assert not is_alive({**mine, "host": "elsewhere"})
   assert not is_alive({**mine, "pid": 2**22 + 12345})
