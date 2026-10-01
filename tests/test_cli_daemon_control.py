"""Focused daemon-control CLI and runtime tests."""

import asyncio
import os
import socket

import pytest
import yaml
from click.testing import CliRunner

import node_monitor.cli.main as cli_module
from node_monitor.cli.main import cli
from node_monitor.config import load_nested_config
from node_monitor.daemon_control import ControlFile, DaemonState


def _run(coro):
   return asyncio.run(asyncio.wait_for(coro, timeout=2))


def _state(**overrides):
   values = {
      "hostname": socket.gethostname(), "pid": os.getpid(),
      "process_start_ticks": 1,
      "start_timestamp": "2026-09-30T22:00:00Z",
      "working_directory": "/", "user": "operator",
      "heartbeat": "2026-09-30T22:00:00Z",
      "stop_requested": False, "exited": False,
      "run_id": "run", "run_directory": "/runs/run",
      "log_file": "/logs/daemon.log", "outcome": "running",
   }
   values.update(overrides)
   return DaemonState(**values)


def _nested_raw():
   return {
      "system": "polaris",
      "nodes": [{"hostname": "login.example.org", "role": "local"}],
      "probe_python": "/usr/bin/python3.11",
      "output": {"root": "~/runs"}, "collection": {"duration_sec": 3600},
      "ssh": {}, "safety": {},
      "database": {"url": "postgresql:///node_monitor"}, "retention": {},
   }


def test_status_and_stop_are_control_file_only(tmp_path, monkeypatch):
   path = tmp_path / "daemon.json"
   control = ControlFile(str(path))
   control.write(_state(exited=True, outcome="clean", exit_code=0))
   monkeypatch.setattr(cli_module, "_default_control_file_path", lambda: str(path))
   monkeypatch.setattr(
      cli_module, "_create_migration_engine",
      lambda *_: (_ for _ in ()).throw(AssertionError("database opened")))

   status = CliRunner().invoke(cli, ["daemon", "status"], catch_exceptions=False)
   stop = CliRunner().invoke(cli, ["daemon", "stop"], catch_exceptions=False)

   assert status.exit_code == 0
   assert "Status: Exited" in status.output
   assert stop.exit_code == 0
   assert control.read().exited is True


def test_stop_sets_request_but_not_exited(tmp_path, monkeypatch):
   path = tmp_path / "daemon.json"
   control = ControlFile(str(path))
   control.write(_state())
   monkeypatch.setattr(cli_module, "_default_control_file_path", lambda: str(path))
   monkeypatch.setattr(
      "node_monitor.daemon_control._linux_start_ticks", lambda unused: 1)

   result = CliRunner().invoke(cli, ["daemon", "stop"], catch_exceptions=False)

   assert result.exit_code == 0
   assert control.read().stop_requested is True
   assert control.read().exited is False


def test_control_watcher_requests_stop_and_exits(tmp_path):
   control = ControlFile(str(tmp_path / "daemon.json"))
   control.write(_state(stop_requested=True))
   calls = []

   class Stoppable:
      def request_stop(self):
         calls.append(True)

   _run(cli_module._watch_control_file(
      control, Stoppable(), poll_interval=0.001, heartbeat_interval=60))
   assert calls == [True]


def test_control_watcher_fails_closed_when_file_disappears(tmp_path):
   control = ControlFile(str(tmp_path / "daemon.json"))
   control.write(_state())
   os.unlink(control.path)
   calls = []

   class Stoppable:
      def request_stop(self):
         calls.append(True)

   _run(cli_module._watch_control_file(
      control, Stoppable(), poll_interval=0.001, heartbeat_interval=60))
   assert calls == [True]


def test_start_foreground_forces_indefinite_and_marks_exited(tmp_path, monkeypatch):
   config = tmp_path / "config.yaml"
   config.write_text(yaml.safe_dump(_nested_raw()))
   path = tmp_path / "daemon.json"
   observed = []
   monkeypatch.setattr(cli_module, "_default_control_file_path", lambda: str(path))
   monkeypatch.setattr(cli_module, "current_process_start_ticks", lambda: 7)
   monkeypatch.setattr(cli_module, "_resolve_probe_version", lambda *_: 4)

   def fake_runtime(nested, config_path, run_id, probe_version, home,
                    phase0_config=None, control_file=None, startup_ack=None):
      observed.append(phase0_config.duration_sec)
      if startup_ack is not None:
         startup_ack("/runs/real")
      return 0

   monkeypatch.setattr(cli_module, "_run_daemon_postgres", fake_runtime)

   result = CliRunner().invoke(
      cli, ["daemon", "start", "--config", str(config), "--home",
            str(tmp_path), "--foreground"], catch_exceptions=False)

   assert result.exit_code == 0
   assert observed == [None]
   state = ControlFile(str(path)).read()
   assert state.exited is True
   assert state.outcome == "partial"


def test_postgres_runtime_ack_occurs_after_schema_gate_and_sink_creation(
      tmp_path, monkeypatch):
   events = []

   class Engine:
      def dispose(self): events.append("dispose")
      def begin(self): raise AssertionError

   class Sink:
      run_dir = "/runs/actual"
      async def start(self): events.append("sink_start")
      async def abort(self): events.append("abort")

   class Daemon:
      def __init__(self, *_): pass
      async def run(self): events.append("run"); return 0

   monkeypatch.setattr(cli_module, "_create_migration_engine", lambda *_: Engine())
   monkeypatch.setattr(cli_module, "_schema_gate_or_exit",
                       lambda *_: events.append("gate"))
   monkeypatch.setattr(cli_module, "Phase0Sink", lambda *a, **k: Sink())
   monkeypatch.setattr(cli_module, "PostgresDaemonSink", lambda *a: Sink())
   monkeypatch.setattr(cli_module, "DatabaseWriter", lambda *_: object())
   monkeypatch.setattr(cli_module, "_make_transport_fn", lambda *a: object())
   monkeypatch.setattr(cli_module, "Daemon", Daemon)
   monkeypatch.setattr(cli_module.os, "makedirs", lambda *a, **k: None)

   nested = load_nested_config(_nested_raw(), home=str(tmp_path))
   raw = cli_module._nested_to_phase0_raw(nested, duration_sec_override=None)
   phase0 = cli_module.load_config(raw, home=str(tmp_path))
   result = cli_module._run_daemon_postgres(
      nested, "config", "run", 4, str(tmp_path), phase0_config=phase0,
      startup_ack=lambda run_dir: events.append(("ack", run_dir)))

   assert result == 0
   assert events.index("gate") < events.index(("ack", "/runs/actual"))
   assert events.index(("ack", "/runs/actual")) < events.index("sink_start")
   assert events.index("sink_start") < events.index("run")


def test_start_rejects_concurrent_start_while_lifetime_lock_is_held(
      tmp_path, monkeypatch):
   config = tmp_path / "config.yaml"
   config.write_text(yaml.safe_dump(_nested_raw()))
   path = tmp_path / "daemon.json"
   lock_path = tmp_path / "daemon.lock"
   monkeypatch.setattr(cli_module, "_default_control_file_path", lambda: str(path))
   monkeypatch.setattr(cli_module, "_default_control_lock_path", lambda: str(lock_path))
   monkeypatch.setattr(cli_module, "_resolve_probe_version", lambda *_: 4)

   first_lock = cli_module._acquire_daemon_lock(str(lock_path))
   try:
      result = CliRunner().invoke(
         cli, ["daemon", "start", "--config", str(config), "--home",
               str(tmp_path), "--foreground"], catch_exceptions=False)
   finally:
      cli_module._release_daemon_lock(first_lock)

   assert result.exit_code == 1
   assert "daemon already active" in result.output


def test_mark_exited_attempted_after_runtime_exception(tmp_path, monkeypatch):
   control = ControlFile(str(tmp_path / "daemon.json"))
   control.write(_state())
   calls = []
   monkeypatch.setattr(control, "mark_exited",
                       lambda outcome, exit_code: calls.append((outcome, exit_code)))

   with pytest.raises(RuntimeError, match="boom"):
      cli_module._run_controlled_daemon(
         control,
         lambda unused: (_ for _ in ()).throw(RuntimeError("boom")),
         lambda unused: None)

   assert calls == [("fatal", 1)]


def test_close_inherited_fds_preserves_only_explicit_descriptors(monkeypatch):
   closed = []
   monkeypatch.setattr(cli_module.resource, "getrlimit", lambda *_: (9, 9))
   monkeypatch.setattr(cli_module.os, "close", lambda fd: closed.append(fd))

   cli_module._close_inherited_fds({4, 7})

   assert closed == [3, 5, 6, 8]
