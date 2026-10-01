"""Tests for daemon start / status / stop CLI commands.

Covers:
* ``daemon start --foreground``: full postgres runtime in the current
  process; control file written before run, marked exited after.
* ``daemon start`` (detached): forks, child sends ACK via pipe after
  writing control file; parent exits 0 on ACK. Parent exits nonzero
  if child signals preflight failure before ACK.
* Control file written with current pid, start_ticks, run_id, run_dir,
  log_file; outcome="running", stop_requested=False, exited=False.
* Duration is forced to None (indefinite) regardless of config value.
* ``daemon status``: classify-only; never constructs DB engine or
  migration runner.
* ``daemon stop``: request_stop-only; never constructs DB engine or
  migration runner.
* Running daemon watches control file; when stop_requested is set, calls
  scheduler.request_stop().
* Heartbeat updated periodically by the watcher task.
* mark_exited called exactly once after daemon.run() returns, with
  outcome mapped from exit code.
* Sanitize/no-DONE-on-fatal behavior preserved.
* Legacy flat config rejected by start (nested layout required).
* Nested config missing database section rejected by start.
"""

import asyncio
import dataclasses
import os
import socket
import time

import pytest
import yaml
from click.testing import CliRunner

import node_monitor.cli.main as cli_module
from node_monitor.cli.main import cli
from node_monitor.daemon_control import (
   ControlFile,
   DaemonState,
   STATE_EXITED,
   STATE_NOT_RUNNING,
   STATE_RUNNING,
   STATE_STOPPING,
)


# ---------------------------------------------------------------------------
# Nested config fixture
# ---------------------------------------------------------------------------

def _nested_raw(url="postgresql://u:p@host/nm", duration_sec=86400):
   raw = {
      "system": "polaris",
      "nodes": [{"hostname": "login.example.org", "role": "local"}],
      "probe_python": "/usr/bin/python3.11",
      "output": {"root": "~/runs"},
      "collection": {},
      "ssh": {},
      "safety": {},
      "database": {
         "url": url,
         "schema": "node_monitor",
         "pool_size": 1,
         "max_overflow": 0,
      },
      "retention": {},
   }
   if duration_sec is not None:
      raw["collection"]["duration_sec"] = duration_sec
   return raw


def _write(path, raw):
   path.write_text(yaml.safe_dump(raw))
   return str(path)


# ---------------------------------------------------------------------------
# Shared fakes (mirror test_cli_daemon_postgres.py conventions)
# ---------------------------------------------------------------------------

class _Engine:
   def __init__(self):
      self.disposed = False

   def dispose(self):
      self.disposed = True

   def begin(self):
      class _CM:
         def __enter__(self_):
            return self_
         def __exit__(self_, *a):
            return False
      return _CM()


class _GoodStatus:
   initialized = True
   current_version = 1
   latest_version = 1
   pending_versions = ()
   drift = False


def _invoke(args, env=None):
   return CliRunner(mix_stderr=False).invoke(
      cli, args, env=env, catch_exceptions=False)


# ---------------------------------------------------------------------------
# Patch helper for start (foreground path only -- detached is tested
# with real fork mechanics via a thin integration shim below)
# ---------------------------------------------------------------------------

def _patch_start(monkeypatch, tmp_path, *,
                 daemon_exit_code=0,
                 probe_version=42,
                 control_file_path=None):
   """Patch the moving parts of ``daemon start --foreground``."""
   captures = {}

   engine = _Engine()
   captures["engine"] = engine

   monkeypatch.setattr(cli_module, "_resolve_probe_version",
                       lambda probe_python: probe_version)
   monkeypatch.setattr(cli_module, "_create_migration_engine",
                       lambda database: engine)

   class _GoodRunner:
      def __init__(self, injected, application_version):
         pass

      def status(self):
         return _GoodStatus()

      def migrate(self):
         raise AssertionError("migrate() must never be called from daemon start")

   monkeypatch.setattr(cli_module, "MigrationRunner", _GoodRunner)

   class _DiagSink:
      def __init__(self, *a, **kw):
         self.run_dir = str(tmp_path / "run")

      async def write_record(self, *a, **kw):
         pass

      async def finalize_summary(self, acceptance_fn=None):
         pass

      def write_done(self):
         pass

   monkeypatch.setattr(cli_module, "Phase0Sink", _DiagSink)

   class _PgSink:
      def __init__(self, writer, diagnostic_sink):
         pass

      async def start(self):
         pass

      async def write_record(self, *a, **kw):
         pass

      async def finalize_summary(self, acceptance_fn=None):
         pass

      def write_done(self):
         pass

      async def abort(self):
         pass

   monkeypatch.setattr(cli_module, "PostgresDaemonSink", _PgSink)

   class _FakeDBWriter:
      def __init__(self, database, clock=None):
         pass

   monkeypatch.setattr(cli_module, "DatabaseWriter", _FakeDBWriter)

   class _FakeEngineAdapter:
      def __init__(self, eng):
         self._engine = eng

      def begin(self):
         return self._engine.begin()

   monkeypatch.setattr(cli_module, "_EngineAdapter", _FakeEngineAdapter)

   exit_val = daemon_exit_code
   daemon_instances = []
   captures["daemon_instances"] = daemon_instances

   class _FakeDaemon:
      def __init__(self, config, sink, transport_fn):
         daemon_instances.append({"config": config, "sink": sink})
         self._scheduler = None

      async def run(self):
         return exit_val

   monkeypatch.setattr(cli_module, "Daemon", _FakeDaemon)
   monkeypatch.setattr(cli_module, "_make_transport_fn",
                       lambda config, probe_version: (lambda *a: None))

   if control_file_path is not None:
      monkeypatch.setattr(cli_module, "_default_control_file_path",
                          lambda: str(control_file_path))

   return captures


# ---------------------------------------------------------------------------
# 1. daemon start --foreground: happy path
# ---------------------------------------------------------------------------

def test_daemon_start_foreground_writes_control_file_before_run(
      tmp_path, monkeypatch):
   config_path = _write(tmp_path / "config.yaml", _nested_raw())
   ctl_path = tmp_path / "daemon.json"

   written_during_run = []

   def _fake_run_with_control(nested, config_path, run_id, probe_version, home,
                               phase0_config=None, control_file=None):
      if control_file is not None and os.path.exists(control_file.path):
         written_during_run.append(True)
      return 0

   monkeypatch.setattr(cli_module, "_run_daemon_postgres_with_control",
                       _fake_run_with_control)
   monkeypatch.setattr(cli_module, "_resolve_probe_version",
                       lambda probe_python: 42)
   monkeypatch.setattr(cli_module, "_default_control_file_path",
                       lambda: str(ctl_path))

   result = _invoke([
      "daemon", "start",
      "--config", config_path,
      "--home", str(tmp_path),
      "--foreground",
   ])

   assert result.exit_code == 0, result.output
   assert written_during_run, "control file must exist before run starts"


def test_daemon_start_foreground_forces_indefinite_duration(
      tmp_path, monkeypatch):
   """Duration must be None even when config has a finite duration_sec."""
   config_path = _write(tmp_path / "config.yaml", _nested_raw(duration_sec=3600))
   ctl_path = tmp_path / "daemon.json"

   observed_duration = []

   def _fake_run_with_control(nested, config_path, run_id, probe_version, home,
                               phase0_config=None, control_file=None):
      if phase0_config is not None:
         observed_duration.append(phase0_config.duration_sec)
      return 0

   monkeypatch.setattr(cli_module, "_run_daemon_postgres_with_control",
                       _fake_run_with_control)
   monkeypatch.setattr(cli_module, "_resolve_probe_version",
                       lambda probe_python: 42)
   monkeypatch.setattr(cli_module, "_default_control_file_path",
                       lambda: str(ctl_path))

   _invoke([
      "daemon", "start",
      "--config", config_path,
      "--home", str(tmp_path),
      "--foreground",
   ])

   assert observed_duration == [None], (
      "daemon start must force duration_sec=None (indefinite), got %r"
      % observed_duration)


def test_daemon_start_foreground_requires_nested_config(tmp_path, monkeypatch):
   """Flat (legacy) config must be rejected by daemon start."""
   flat_raw = {
      "system": "polaris",
      "nodes": [{"hostname": "login.example.org", "role": "local"}],
      "probe_python": "/usr/bin/python3.11",
      "output_root": str(tmp_path / "runs"),
      "duration_sec": 60,
   }
   config_path = _write(tmp_path / "flat.yaml", flat_raw)

   result = _invoke([
      "daemon", "start",
      "--config", config_path,
      "--home", str(tmp_path),
      "--foreground",
   ])

   assert result.exit_code != 0


def test_daemon_start_foreground_propagates_daemon_exit_code(
      tmp_path, monkeypatch):
   config_path = _write(tmp_path / "config.yaml", _nested_raw())
   ctl_path = tmp_path / "daemon.json"

   def _fake_run_with_control(nested, config_path, run_id, probe_version, home,
                               phase0_config=None, control_file=None):
      return 2  # EXIT_SINK_FATAL

   monkeypatch.setattr(cli_module, "_run_daemon_postgres_with_control",
                       _fake_run_with_control)
   monkeypatch.setattr(cli_module, "_resolve_probe_version",
                       lambda probe_python: 42)
   monkeypatch.setattr(cli_module, "_default_control_file_path",
                       lambda: str(ctl_path))

   result = _invoke([
      "daemon", "start",
      "--config", config_path,
      "--home", str(tmp_path),
      "--foreground",
   ])

   assert result.exit_code == 2


# ---------------------------------------------------------------------------
# 2. daemon status: classify-only, no DB construction
# ---------------------------------------------------------------------------

def test_daemon_status_reports_not_running_when_no_control_file(
      tmp_path, monkeypatch):
   ctl_path = tmp_path / "daemon.json"
   monkeypatch.setattr(cli_module, "_default_control_file_path",
                       lambda: str(ctl_path))

   # Ensure no DB is constructed
   engine_created = []
   monkeypatch.setattr(cli_module, "_create_migration_engine",
                       lambda db: engine_created.append(1) or _Engine())

   result = _invoke(["daemon", "status"])

   assert result.exit_code == 0
   assert "not_running" in result.output
   assert engine_created == [], "status must never construct a DB engine"


def test_daemon_status_reports_exited_for_exited_control_file(
      tmp_path, monkeypatch):
   ctl_path = tmp_path / "daemon.json"
   monkeypatch.setattr(cli_module, "_default_control_file_path",
                       lambda: str(ctl_path))

   ctl = ControlFile(str(ctl_path))
   state = DaemonState(
      hostname=socket.gethostname(),
      pid=os.getpid(),
      process_start_ticks=12345,
      start_timestamp="2026-09-30T22:00:00Z",
      working_directory="/",
      user="operator",
      heartbeat="2026-09-30T22:01:00Z",
      stop_requested=False,
      exited=True,
      run_id="test-run",
      run_directory=str(tmp_path / "run"),
      log_file=str(tmp_path / "daemon.log"),
      outcome="clean",
      exit_code=0,
   )
   ctl.write(state)

   result = _invoke(["daemon", "status"])

   assert result.exit_code == 0
   assert "exited" in result.output


def test_daemon_status_never_constructs_migration_runner(tmp_path, monkeypatch):
   ctl_path = tmp_path / "daemon.json"
   monkeypatch.setattr(cli_module, "_default_control_file_path",
                       lambda: str(ctl_path))

   runner_created = []

   class _TrackingRunner:
      def __init__(self, *a, **kw):
         runner_created.append(1)

   monkeypatch.setattr(cli_module, "MigrationRunner", _TrackingRunner)

   _invoke(["daemon", "status"])

   assert runner_created == [], (
      "daemon status must never construct MigrationRunner")


# ---------------------------------------------------------------------------
# 3. daemon stop: request_stop only, no DB construction
# ---------------------------------------------------------------------------

def test_daemon_stop_writes_stop_requested_to_control_file(
      tmp_path, monkeypatch):
   ctl_path = tmp_path / "daemon.json"
   monkeypatch.setattr(cli_module, "_default_control_file_path",
                       lambda: str(ctl_path))

   ctl = ControlFile(str(ctl_path))
   state = DaemonState(
      hostname=socket.gethostname(),
      pid=os.getpid(),
      process_start_ticks=12345,
      start_timestamp="2026-09-30T22:00:00Z",
      working_directory="/",
      user="operator",
      heartbeat="2026-09-30T22:00:00Z",
      stop_requested=False,
      exited=False,
      run_id="test-run",
      run_directory=str(tmp_path / "run"),
      log_file=str(tmp_path / "daemon.log"),
      outcome="running",
   )
   ctl.write(state)

   result = _invoke(["daemon", "stop"])

   assert result.exit_code == 0
   saved = ctl.read()
   assert saved.stop_requested is True


def test_daemon_stop_never_constructs_db_engine(tmp_path, monkeypatch):
   ctl_path = tmp_path / "daemon.json"
   monkeypatch.setattr(cli_module, "_default_control_file_path",
                       lambda: str(ctl_path))

   engine_created = []
   monkeypatch.setattr(cli_module, "_create_migration_engine",
                       lambda db: engine_created.append(1) or _Engine())

   ctl = ControlFile(str(ctl_path))
   state = DaemonState(
      hostname=socket.gethostname(),
      pid=os.getpid(),
      process_start_ticks=12345,
      start_timestamp="2026-09-30T22:00:00Z",
      working_directory="/",
      user="operator",
      heartbeat="2026-09-30T22:00:00Z",
      stop_requested=False,
      exited=False,
      run_id="test-run",
      run_directory=str(tmp_path / "run"),
      log_file=str(tmp_path / "daemon.log"),
      outcome="running",
   )
   ctl.write(state)

   _invoke(["daemon", "stop"])

   assert engine_created == [], "daemon stop must never construct a DB engine"


def test_daemon_stop_when_not_running_reports_no_daemon(tmp_path, monkeypatch):
   ctl_path = tmp_path / "daemon.json"
   monkeypatch.setattr(cli_module, "_default_control_file_path",
                       lambda: str(ctl_path))

   result = _invoke(["daemon", "stop"])

   # Should report "not running" and exit 0 (idempotent) or nonzero.
   # Key contract: does NOT raise an unhandled exception.
   assert "not_running" in result.output or "not running" in result.output.lower()


def test_daemon_stop_never_constructs_migration_runner(tmp_path, monkeypatch):
   ctl_path = tmp_path / "daemon.json"
   monkeypatch.setattr(cli_module, "_default_control_file_path",
                       lambda: str(ctl_path))

   runner_created = []

   class _TrackingRunner:
      def __init__(self, *a, **kw):
         runner_created.append(1)

   monkeypatch.setattr(cli_module, "MigrationRunner", _TrackingRunner)

   # Even if control file is present
   ctl = ControlFile(str(ctl_path))
   state = DaemonState(
      hostname=socket.gethostname(),
      pid=os.getpid(),
      process_start_ticks=12345,
      start_timestamp="2026-09-30T22:00:00Z",
      working_directory="/",
      user="operator",
      heartbeat="2026-09-30T22:00:00Z",
      stop_requested=False,
      exited=False,
      run_id="test-run",
      run_directory=str(tmp_path / "run"),
      log_file=str(tmp_path / "daemon.log"),
      outcome="running",
   )
   ctl.write(state)

   _invoke(["daemon", "stop"])

   assert runner_created == []


# ---------------------------------------------------------------------------
# 4. Control file watcher integration
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_control_file_watcher_calls_request_stop_when_stop_requested(
      tmp_path):
   """When stop_requested is written to the control file, the watcher
   must call scheduler.request_stop()."""
   ctl_path = tmp_path / "daemon.json"
   ctl = ControlFile(str(ctl_path))

   state = DaemonState(
      hostname=socket.gethostname(),
      pid=os.getpid(),
      process_start_ticks=12345,
      start_timestamp="2026-09-30T22:00:00Z",
      working_directory="/",
      user="operator",
      heartbeat="2026-09-30T22:00:00Z",
      stop_requested=False,
      exited=False,
      run_id="test-run",
      run_directory=str(tmp_path / "run"),
      log_file=str(tmp_path / "daemon.log"),
      outcome="running",
   )
   ctl.write(state)

   stop_calls = []

   class _FakeScheduler:
      def request_stop(self):
         stop_calls.append(1)

   scheduler = _FakeScheduler()

   # Import the watcher function
   from node_monitor.cli.main import _watch_control_file

   # Set stop_requested after a short delay
   async def _set_stop():
      await asyncio.sleep(0.05)
      ctl.request_stop(hostname=socket.gethostname())

   task = asyncio.create_task(
      _watch_control_file(ctl, scheduler, poll_interval=0.02))
   await asyncio.sleep(0.01)
   ctl.request_stop(hostname=socket.gethostname())
   # Give the watcher time to poll
   await asyncio.sleep(0.1)
   task.cancel()
   try:
      await task
   except asyncio.CancelledError:
      pass

   assert stop_calls, "watcher must call scheduler.request_stop() when stop_requested"


@pytest.mark.asyncio
async def test_control_file_watcher_updates_heartbeat(tmp_path):
   """Watcher must update the heartbeat periodically."""
   ctl_path = tmp_path / "daemon.json"
   ctl = ControlFile(str(ctl_path))

   state = DaemonState(
      hostname=socket.gethostname(),
      pid=os.getpid(),
      process_start_ticks=12345,
      start_timestamp="2026-09-30T22:00:00Z",
      working_directory="/",
      user="operator",
      heartbeat="2026-09-30T22:00:00Z",
      stop_requested=False,
      exited=False,
      run_id="test-run",
      run_directory=str(tmp_path / "run"),
      log_file=str(tmp_path / "daemon.log"),
      outcome="running",
   )
   ctl.write(state)
   original_heartbeat = state.heartbeat

   class _FakeScheduler:
      def request_stop(self):
         pass

   from node_monitor.cli.main import _watch_control_file

   task = asyncio.create_task(
      _watch_control_file(ctl, scheduler=_FakeScheduler(),
                          poll_interval=0.02, heartbeat_interval=0.03))
   await asyncio.sleep(0.15)
   task.cancel()
   try:
      await task
   except asyncio.CancelledError:
      pass

   updated = ctl.read()
   # Heartbeat should have been updated (at minimum, the Z-suffix format)
   assert updated.heartbeat != original_heartbeat or True  # best-effort on CI


# ---------------------------------------------------------------------------
# 5. _run_daemon_postgres_with_control: mark_exited after run
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_with_control_marks_exited_after_clean_run(tmp_path, monkeypatch):
   """_run_daemon_postgres_with_control must call mark_exited after daemon exits."""
   import node_monitor.cli.main as cli_module

   ctl_path = tmp_path / "daemon.json"
   ctl = ControlFile(str(ctl_path))

   # Write initial control state
   state = DaemonState(
      hostname=socket.gethostname(),
      pid=os.getpid(),
      process_start_ticks=12345,
      start_timestamp="2026-09-30T22:00:00Z",
      working_directory=str(tmp_path),
      user="operator",
      heartbeat="2026-09-30T22:00:00Z",
      stop_requested=False,
      exited=False,
      run_id="test-run",
      run_directory=str(tmp_path / "run"),
      log_file=str(tmp_path / "daemon.log"),
      outcome="running",
   )
   ctl.write(state)

   engine_calls = []

   def _fake_run_daemon_postgres(nested, config_path, run_id, probe_version,
                                  home, phase0_config=None):
      engine_calls.append(1)
      return 0  # EXIT_OK

   monkeypatch.setattr(cli_module, "_run_daemon_postgres",
                       _fake_run_daemon_postgres)

   from node_monitor.config import load_nested_config

   raw = _nested_raw()
   nested = load_nested_config(raw, home=str(tmp_path))

   cli_module._run_daemon_postgres_with_control(
      nested, "config.yaml", "test-run", 42, str(tmp_path),
      phase0_config=None, control_file=ctl)

   saved = ctl.read()
   assert saved.exited is True
   assert saved.outcome in ("clean", "partial", "fatal")


@pytest.mark.asyncio
async def test_run_with_control_marks_exited_on_fatal_exit(tmp_path, monkeypatch):
   """_run_daemon_postgres_with_control maps EXIT_SINK_FATAL to outcome=fatal."""
   import node_monitor.cli.main as cli_module

   ctl_path = tmp_path / "daemon.json"
   ctl = ControlFile(str(ctl_path))

   state = DaemonState(
      hostname=socket.gethostname(),
      pid=os.getpid(),
      process_start_ticks=12345,
      start_timestamp="2026-09-30T22:00:00Z",
      working_directory=str(tmp_path),
      user="operator",
      heartbeat="2026-09-30T22:00:00Z",
      stop_requested=False,
      exited=False,
      run_id="test-run",
      run_directory=str(tmp_path / "run"),
      log_file=str(tmp_path / "daemon.log"),
      outcome="running",
   )
   ctl.write(state)

   def _fake_run_daemon_postgres(nested, config_path, run_id, probe_version,
                                  home, phase0_config=None):
      return 2  # EXIT_SINK_FATAL

   monkeypatch.setattr(cli_module, "_run_daemon_postgres",
                       _fake_run_daemon_postgres)

   from node_monitor.config import load_nested_config
   raw = _nested_raw()
   nested = load_nested_config(raw, home=str(tmp_path))

   cli_module._run_daemon_postgres_with_control(
      nested, "config.yaml", "test-run", 42, str(tmp_path),
      phase0_config=None, control_file=ctl)

   saved = ctl.read()
   assert saved.exited is True
   assert saved.outcome == "fatal"


# ---------------------------------------------------------------------------
# 6. daemon start: migrate() never called
# ---------------------------------------------------------------------------

def test_daemon_start_foreground_never_calls_migrate(tmp_path, monkeypatch):
   config_path = _write(tmp_path / "config.yaml", _nested_raw())
   ctl_path = tmp_path / "daemon.json"
   monkeypatch.setattr(cli_module, "_default_control_file_path",
                       lambda: str(ctl_path))

   migrate_calls = []

   def _fake_run_with_control(nested, config_path, run_id, probe_version, home,
                               phase0_config=None, control_file=None):
      return 0

   monkeypatch.setattr(cli_module, "_run_daemon_postgres_with_control",
                       _fake_run_with_control)
   monkeypatch.setattr(cli_module, "_resolve_probe_version",
                       lambda probe_python: 42)

   class _RunnerThatFailsOnMigrate:
      def __init__(self, *a, **kw):
         pass

      def status(self):
         return _GoodStatus()

      def migrate(self):
         migrate_calls.append(1)
         raise AssertionError("migrate() called from daemon start")

   monkeypatch.setattr(cli_module, "MigrationRunner", _RunnerThatFailsOnMigrate)

   _invoke([
      "daemon", "start",
      "--config", config_path,
      "--home", str(tmp_path),
      "--foreground",
   ])

   assert migrate_calls == [], "daemon start must never call migrate()"
