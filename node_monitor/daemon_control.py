"""Atomic local control state for the detached node-monitor daemon."""

import dataclasses
import datetime
import json
import os
import socket
import tempfile


STATE_RUNNING = "running"
STATE_STOPPING = "stopping"
STATE_EXITED = "exited"
STATE_STALE = "stale"
STATE_DIFFERENT_HOST = "different_host"
STATE_NOT_RUNNING = "not_running"
_SCHEMA_VERSION = 1
_ALLOWED_OUTCOMES = frozenset(("running", "clean", "partial", "fatal"))
_REQUIRED_FIELDS = frozenset((
   "schema_version", "hostname", "pid", "process_start_ticks",
   "start_timestamp", "working_directory", "user", "heartbeat",
   "stop_requested", "exited", "run_id", "run_directory", "log_file",
   "outcome",
))
_ALLOWED_FIELDS = _REQUIRED_FIELDS | frozenset(("exit_code",))


class DaemonControlError(Exception):
   """Raised for malformed or unsafe daemon control state."""


@dataclasses.dataclass(frozen=True)
class DaemonState:
   hostname: str
   pid: int
   process_start_ticks: int
   start_timestamp: str
   working_directory: str
   user: str
   heartbeat: str
   stop_requested: bool
   exited: bool
   run_id: str
   run_directory: str
   log_file: str
   outcome: str
   exit_code: object = None

   def __post_init__(self):
      if not isinstance(self.hostname, str) or not self.hostname:
         raise DaemonControlError("daemon state hostname is invalid")
      if isinstance(self.pid, bool) or not isinstance(self.pid, int) or self.pid <= 0:
         raise DaemonControlError("daemon state pid is invalid")
      if (isinstance(self.process_start_ticks, bool)
            or not isinstance(self.process_start_ticks, int)
            or self.process_start_ticks < 0):
         raise DaemonControlError("daemon state process_start_ticks is invalid")
      for key in ("start_timestamp", "working_directory", "user", "heartbeat",
                  "run_id", "run_directory", "log_file"):
         value = getattr(self, key)
         if not isinstance(value, str) or not value:
            raise DaemonControlError("daemon state %s is invalid" % key)
      if not isinstance(self.stop_requested, bool) or not isinstance(self.exited, bool):
         raise DaemonControlError("daemon state flags are invalid")
      if self.outcome not in _ALLOWED_OUTCOMES:
         raise DaemonControlError("daemon state outcome is invalid")
      if self.exit_code is not None and (
            isinstance(self.exit_code, bool) or not isinstance(self.exit_code, int)):
         raise DaemonControlError("daemon state exit_code is invalid")

   def as_dict(self):
      result = dataclasses.asdict(self)
      result["schema_version"] = _SCHEMA_VERSION
      return result

   @classmethod
   def from_dict(cls, raw):
      if not isinstance(raw, dict):
         raise DaemonControlError("daemon control file is not a JSON object")
      if set(raw) != _REQUIRED_FIELDS and set(raw) != _ALLOWED_FIELDS:
         raise DaemonControlError("daemon control file fields are invalid")
      if raw.get("schema_version") != _SCHEMA_VERSION:
         raise DaemonControlError("daemon control schema version is invalid")
      values = dict(raw)
      values.pop("schema_version")
      return cls(**values)


def _utc_now():
   return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _linux_start_ticks(pid):
   try:
      with open("/proc/%d/stat" % pid, "r") as handle:
         fields = handle.read().rsplit(") ", 1)[1].split()
      return int(fields[19])
   except (OSError, IndexError, ValueError):
      return None


class ControlFile:
   """Private, atomically replaced cooperative daemon-control JSON file."""

   def __init__(self, path):
      self.path = os.path.abspath(path)

   def read(self):
      try:
         with open(self.path, "r") as handle:
            raw = json.load(handle)
      except (OSError, ValueError) as exc:
         raise DaemonControlError("could not read daemon control file") from None
      return DaemonState.from_dict(raw)

   def write(self, state):
      if not isinstance(state, DaemonState):
         raise DaemonControlError("daemon state has invalid type")
      directory = os.path.dirname(self.path)
      os.makedirs(directory, mode=0o700, exist_ok=True)
      fd, temporary_path = tempfile.mkstemp(
         prefix=".%s." % os.path.basename(self.path), suffix=".tmp", dir=directory)
      try:
         os.fchmod(fd, 0o600)
         with os.fdopen(fd, "w") as handle:
            json.dump(state.as_dict(), handle, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
         os.replace(temporary_path, self.path)
         os.chmod(self.path, 0o600)
         directory_fd = os.open(directory, os.O_RDONLY)
         try:
            os.fsync(directory_fd)
         finally:
            os.close(directory_fd)
      except OSError:
         try:
            os.unlink(temporary_path)
         except OSError:
            pass
         raise DaemonControlError("could not write daemon control file") from None

   def classify(self, hostname=None):
      if not os.path.exists(self.path):
         return STATE_NOT_RUNNING
      state = self.read()
      hostname = hostname if hostname is not None else socket.gethostname()
      if state.hostname != hostname:
         return STATE_DIFFERENT_HOST
      if state.exited:
         return STATE_EXITED
      actual_ticks = _linux_start_ticks(state.pid)
      if actual_ticks is None or actual_ticks != state.process_start_ticks:
         return STATE_STALE
      if state.stop_requested:
         return STATE_STOPPING
      return STATE_RUNNING

   def request_stop(self, hostname=None):
      state = self.read()
      hostname = hostname if hostname is not None else socket.gethostname()
      if state.hostname != hostname:
         raise DaemonControlError("daemon is managed on a different host")
      if state.exited:
         return state
      updated = dataclasses.replace(state, stop_requested=True, heartbeat=_utc_now())
      self.write(updated)
      return updated

   def heartbeat(self):
      state = self.read()
      updated = dataclasses.replace(state, heartbeat=_utc_now())
      self.write(updated)
      return updated

   def mark_exited(self, outcome, exit_code):
      state = self.read()
      updated = dataclasses.replace(
         state, exited=True, heartbeat=_utc_now(), outcome=outcome,
         exit_code=exit_code)
      self.write(updated)
      return updated


def current_process_start_ticks():
   ticks = _linux_start_ticks(os.getpid())
   if ticks is None:
      raise DaemonControlError("could not determine daemon process identity")
   return ticks
