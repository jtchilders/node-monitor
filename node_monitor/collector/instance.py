"""Single-instance guard for the collector daemon."""

import fcntl
import json
import os
import socket
from datetime import datetime, timezone
from pathlib import Path


class AlreadyRunning(Exception):
   def __init__(self, info: dict):
      super().__init__(f"already running (pid {info.get('pid')} on {info.get('host')})")
      self.info = info


def _paths(working_dir: Path) -> tuple[Path, Path, Path]:
   state = Path(working_dir) / "state"
   return state, state / "daemon.lock", state / "daemon.json"


def read_info(working_dir: Path) -> dict | None:
   try:
      return json.loads(_paths(working_dir)[2].read_text())
   except (OSError, ValueError):
      return None


def is_alive(info: dict) -> bool:
   """True only for this host and a live, non-zombie pid.

   A pid alone proves nothing about the daemon: after a crash the pid may be recycled.
   Use running_info to decide whether the daemon runs; this only answers whether a pid lives.
   """
   if info.get("host") != socket.gethostname():
      return False
   pid = info.get("pid")
   if not isinstance(pid, int) or pid <= 0:
      return False
   try:
      os.kill(pid, 0)
   except ProcessLookupError:
      return False
   except PermissionError:
      return True
   # A child that has exited but is not yet reaped still answers kill(0).
   try:
      with open(f"/proc/{pid}/stat") as f:
         return f.read().rsplit(")", 1)[1].split()[0] != "Z"
   except (OSError, IndexError):
      return True


def _probe(working_dir: Path, on_free=None) -> bool:
   """True if another process holds the lock. Runs `on_free` while we briefly hold it."""
   try:
      fd = os.open(_paths(working_dir)[1], os.O_RDWR)
   except FileNotFoundError:
      # Never locked, so nobody holds it.
      if on_free:
         on_free()
      return False
   except OSError:
      return False
   try:
      try:
         fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
      except BlockingIOError:
         return True
      if on_free:
         on_free()
      return False
   finally:
      os.close(fd)  # closing drops our flock


def lock_held(working_dir: Path) -> bool:
   return _probe(working_dir)


def running_info(working_dir: Path) -> dict | None:
   """daemon.json of the running daemon, or None. The kernel lock is the source of truth:
   a leftover daemon.json after a crash names a pid that may now belong to anyone."""
   if not lock_held(working_dir):
      return None
   return read_info(working_dir)


def clear_stale(working_dir: Path) -> dict | None:
   """Remove a daemon.json left by a dead daemon; returns what it said, or None.

   Done while holding the lock so a daemon starting right now is not clobbered."""
   found: list[dict] = []

   def remove():
      info = read_info(working_dir)
      if info is not None:
         found.append(info)
      _paths(working_dir)[2].unlink(missing_ok=True)

   _probe(working_dir, remove)
   return found[0] if found else None


class InstanceLock:
   """An flock held on state/daemon.lock for the life of the process.

   The kernel drops the lock when the process dies, so a crash never leaves a stale lock;
   daemon.json only tells others who holds it.
   """

   def __init__(self, working_dir: Path):
      self.working_dir = Path(working_dir)
      self._fd: int | None = None

   def acquire(self) -> None:
      state, lock_path, info_path = _paths(self.working_dir)
      state.mkdir(mode=0o700, parents=True, exist_ok=True)
      # state/ holds ssh_config, which ssh executes from: an existing directory shared with
      # a group is tightened, since mkdir leaves its mode alone.
      os.chmod(state, 0o700)
      fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
      try:
         fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
      except BlockingIOError:
         os.close(fd)
         raise AlreadyRunning(read_info(self.working_dir) or {}) from None
      self._fd = fd
      info = {
         "pid": os.getpid(),
         "host": socket.gethostname(),
         "started_at": datetime.now(timezone.utc).isoformat(),
      }
      tmp = info_path.with_name("daemon.json.tmp")
      tmp_fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
      with os.fdopen(tmp_fd, "w") as f:
         f.write(json.dumps(info))
      os.replace(tmp, info_path)

   def release(self) -> None:
      if self._fd is None:
         return
      _paths(self.working_dir)[2].unlink(missing_ok=True)
      fcntl.flock(self._fd, fcntl.LOCK_UN)
      os.close(self._fd)
      self._fd = None
