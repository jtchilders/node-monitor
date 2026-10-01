"""Tests for node-monitor's local cooperative daemon control file."""

import json
import os
import socket

import pytest

from node_monitor.daemon_control import (
   ControlFile,
   DaemonControlError,
   DaemonState,
   STATE_EXITED,
   STATE_RUNNING,
   STATE_STOPPING,
)


def _state(**overrides):
   values = {
      "hostname": socket.gethostname(),
      "pid": os.getpid(),
      "process_start_ticks": 12345,
      "start_timestamp": "2026-09-30T22:00:00Z",
      "working_directory": "/",
      "user": "operator",
      "heartbeat": "2026-09-30T22:00:00Z",
      "stop_requested": False,
      "exited": False,
      "run_id": "test-run",
      "run_directory": "/runs/test-run",
      "log_file": "/logs/test.log",
      "outcome": "running",
   }
   values.update(overrides)
   return DaemonState(**values)


def test_write_is_private_atomic_json_and_round_trips(tmp_path):
   path = tmp_path / ".node_monitor_daemon.pid"
   control = ControlFile(str(path))
   control.write(_state())

   assert path.stat().st_mode & 0o777 == 0o600
   assert not list(tmp_path.glob(".node_monitor_daemon.pid.*.tmp"))
   assert json.loads(path.read_text())["schema_version"] == 1
   assert control.read() == _state()


def test_request_stop_is_monotonic_and_marks_stopping(tmp_path):
   control = ControlFile(str(tmp_path / "state.json"))
   control.write(_state())

   updated = control.request_stop()

   assert updated.stop_requested is True
   assert updated.exited is False
   assert updated.outcome == "running"
   assert control.read().stop_requested is True


def test_mark_exited_is_distinct_from_stop_request(tmp_path):
   control = ControlFile(str(tmp_path / "state.json"))
   control.write(_state(stop_requested=True))

   control.mark_exited(outcome="partial", exit_code=0)

   saved = control.read()
   assert saved.stop_requested is True
   assert saved.exited is True
   assert saved.outcome == "partial"
   assert saved.exit_code == 0
   assert control.classify(hostname=socket.gethostname()) == STATE_EXITED


def test_heartbeat_preserves_preexisting_stop_request(tmp_path):
   control = ControlFile(str(tmp_path / "state.json"))
   control.write(_state())
   control.request_stop()

   updated = control.heartbeat()

   assert updated.stop_requested is True
   assert control.read().stop_requested is True


def test_invalid_or_unknown_state_fails_closed(tmp_path):
   path = tmp_path / "state.json"
   path.write_text('{"pid": 5, "unexpected": true}')

   with pytest.raises(DaemonControlError):
      ControlFile(str(path)).read()


def test_different_host_is_not_safe_to_control(tmp_path):
   control = ControlFile(str(tmp_path / "state.json"))
   control.write(_state(hostname="other-host"))

   assert control.classify(hostname=socket.gethostname()) == "different_host"
   with pytest.raises(DaemonControlError):
      control.request_stop(hostname=socket.gethostname())
