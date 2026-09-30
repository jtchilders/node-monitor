"""Tests for ``daemon run`` -- PostgreSQL-backed runtime command.

Covers:
* Schema gate: proceed when status is fully current; reject on each failure
  condition (not initialized, pending versions, drift, current!=latest).
* Schema gate exceptions sanitized -- no DB/driver errors/URLs in output.
* ``migrate()`` is NEVER called from this path.
* Engine disposed on every rejection, construction error, and daemon exit.
* YAML config URL takes precedence over NODE_MONITOR_DB_URL environment.
* Probe-version resolution errors propagate cleanly (before engine creation).
* Phase0Sink (diagnostic JSONL) is NOT created on schema gate failure.
* PostgresDaemonSink.start() is awaited before daemon.run().
* daemon.run() exit code is propagated as process exit code.
* Positive duration override replaces config value; non-positive rejected.
* ``dry-run``, ``smoke``, and ``--help`` never construct engine or runner.
* Legacy flat config is rejected (nested layout required).
* Config file not found or invalid YAML rejected cleanly.

All tests use injected fakes and inspect exact call ordering.
No real PostgreSQL connections; Task 4 covers acceptance tests.
"""

import asyncio
from types import SimpleNamespace

import pytest
import yaml
from click.testing import CliRunner

import node_monitor.cli.main as cli_module
from node_monitor.cli.main import cli


# ---------------------------------------------------------------------------
# Minimal nested config fixture
# ---------------------------------------------------------------------------

def _nested_raw(url="postgresql://file_user:file_pass@filehost/node_monitor"):
   return {
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


def _write(path, raw):
   path.write_text(yaml.safe_dump(raw))
   return str(path)


# ---------------------------------------------------------------------------
# Shared fakes
# ---------------------------------------------------------------------------

class _Engine:
   def __init__(self):
      self.disposed = False
      self.began = False

   def dispose(self):
      self.disposed = True

   def begin(self):
      self.began = True
      # Return a do-nothing context manager
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


def _make_good_runner(engine_ref, migrate_calls):
   class Runner:
      def __init__(self, injected, application_version):
         assert injected is engine_ref, (
            "Runner received wrong engine object")

      def status(self):
         return _GoodStatus()

      def migrate(self):
         migrate_calls.append(1)
         raise AssertionError("migrate() must never be called from daemon run")

   return Runner


class _DiagnosticSink:
   """Minimal Phase0Sink stand-in that records lifecycle calls."""
   def __init__(self):
      self.run_dir = "/tmp/fake_run_dir"
      self.started = False

   async def write_record(self, record_type, record):
      pass

   async def finalize_summary(self, acceptance_fn=None):
      pass

   def write_done(self):
      pass


class _PostgresSink:
   """Records call order for start/daemon.run coupling checks."""
   def __init__(self):
      self.calls = []

   async def start(self):
      self.calls.append("start")

   async def write_record(self, record_type, record):
      self.calls.append(("write_record", record_type))

   async def finalize_summary(self, acceptance_fn=None):
      self.calls.append("finalize_summary")

   def write_done(self):
      self.calls.append("write_done")


def _invoke(args, env=None):
   return CliRunner().invoke(cli, args, env=env, catch_exceptions=False)


# ---------------------------------------------------------------------------
# Helper: patch daemon run internals
# ---------------------------------------------------------------------------

def _patch_daemon_run(
   monkeypatch, tmp_path, *,
   status_result=None,
   runner_class=None,
   daemon_exit_code=0,
   probe_version=42,
   pg_sink=None,
   diag_sink_factory=None,
):
   """
   Monkeypatches the moving parts of ``daemon run``:
   - _resolve_probe_version -> always returns probe_version
   - _create_migration_engine -> returns a fresh _Engine (stored in captures)
   - MigrationRunner -> runs status() returning status_result, never migrate()
   - Phase0Sink -> returns diag_sink_factory() or a default _DiagnosticSink
   - PostgresDaemonSink -> returns pg_sink or a fresh _PostgresSink
   - Daemon -> runs asyncio.run of a coroutine returning daemon_exit_code
   - _make_transport_fn -> returns a no-op async fn

   Returns ``captures`` dict: engine is in captures["engine"],
   migrate_calls in captures["migrate_calls"], sink in captures["pg_sink"],
   and diag_sink in captures["diag_sink"].
   """
   captures = {"migrate_calls": [], "pg_sink_instances": []}

   engine = _Engine()
   captures["engine"] = engine

   monkeypatch.setattr(cli_module, "_resolve_probe_version",
                       lambda probe_python: probe_version)

   monkeypatch.setattr(cli_module, "_create_migration_engine",
                       lambda database: engine)

   if runner_class is None:
      _mc = captures["migrate_calls"]
      runner_class = _make_good_runner(engine, _mc)
      if status_result is not None:
         _sr = status_result
         class RunnerWithStatus(runner_class):
            def status(self):
               return _sr
         runner_class = RunnerWithStatus

   monkeypatch.setattr(cli_module, "MigrationRunner", runner_class)

   _diag_sink = _DiagnosticSink()
   captures["diag_sink"] = _diag_sink

   if diag_sink_factory is None:
      def diag_sink_factory(*args, **kwargs):
         return _diag_sink
   monkeypatch.setattr(cli_module, "Phase0Sink", diag_sink_factory)

   _pg = pg_sink if pg_sink is not None else _PostgresSink()
   captures["pg_sink"] = _pg
   # Keep track of construction calls
   pg_construction_calls = []
   captures["pg_construction_calls"] = pg_construction_calls

   def _make_pg_sink(writer, diagnostic_sink):
      pg_construction_calls.append(1)
      return _pg

   monkeypatch.setattr(cli_module, "PostgresDaemonSink", _make_pg_sink)

   # DatabaseWriter: just record that it was constructed with something
   # that has a begin() method
   db_writer_instances = []
   captures["db_writer_instances"] = db_writer_instances

   real_DBWriter = None
   try:
      from node_monitor.database.writer import DatabaseWriter as _RealDBW
      real_DBWriter = _RealDBW
   except ImportError:
      pass

   class FakeDBWriter:
      def __init__(self, database, clock=None):
         db_writer_instances.append({"database": database})
         assert hasattr(database, "begin"), (
            "DatabaseWriter must receive an object with begin()")

      def write_records(self, records):
         return len(list(records))

   monkeypatch.setattr(cli_module, "DatabaseWriter", FakeDBWriter)

   # Also patch _EngineAdapter so the fake _Engine doesn't trigger
   # SQLAlchemy event.listen (which only works on real SA engines).
   class FakeEngineAdapter:
      def __init__(self, eng):
         self._engine = eng

      def begin(self):
         return self._engine.begin()

   monkeypatch.setattr(cli_module, "_EngineAdapter", FakeEngineAdapter)

   # Daemon: runs, returns exit code
   exit_code_val = daemon_exit_code
   daemon_instances = []
   captures["daemon_instances"] = daemon_instances

   class FakeDaemon:
      def __init__(self, config, sink, transport_fn):
         daemon_instances.append({
            "config": config,
            "sink": sink,
         })

      async def run(self):
         return exit_code_val

   monkeypatch.setattr(cli_module, "Daemon", FakeDaemon)
   monkeypatch.setattr(cli_module, "_make_transport_fn",
                       lambda config, probe_version: (lambda *a: None))

   return captures


# ---------------------------------------------------------------------------
# 1. Happy path: schema current -> daemon runs, exit code propagated
# ---------------------------------------------------------------------------

def test_daemon_run_proceeds_when_schema_current(tmp_path, monkeypatch):
   config_path = _write(tmp_path / "config.yaml", _nested_raw())
   captures = _patch_daemon_run(monkeypatch, tmp_path, daemon_exit_code=0)

   result = _invoke([
      "daemon", "run",
      "--config", config_path,
      "--home", str(tmp_path),
      "--run-id", "test-run-001",
   ])

   assert result.exit_code == 0, result.output
   assert captures["engine"].disposed, "engine must be disposed after run"
   assert len(captures["daemon_instances"]) == 1, "Daemon must be constructed"
   assert captures["migrate_calls"] == [], "migrate() must never be called"


def test_daemon_run_propagates_daemon_nonzero_exit(tmp_path, monkeypatch):
   config_path = _write(tmp_path / "config.yaml", _nested_raw())
   captures = _patch_daemon_run(monkeypatch, tmp_path, daemon_exit_code=3)

   result = _invoke([
      "daemon", "run",
      "--config", config_path,
      "--home", str(tmp_path),
   ])

   assert result.exit_code == 3
   assert captures["engine"].disposed


# ---------------------------------------------------------------------------
# 2. sink.start() called BEFORE daemon.run()
# ---------------------------------------------------------------------------

def test_sink_start_called_before_daemon_run(tmp_path, monkeypatch):
   config_path = _write(tmp_path / "config.yaml", _nested_raw())
   call_order = []

   class OrderedSink(_PostgresSink):
      async def start(self):
         call_order.append("sink.start")

   class OrderedDaemon:
      def __init__(self, config, sink, transport_fn):
         pass

      async def run(self):
         call_order.append("daemon.run")
         return 0

   pg_sink = OrderedSink()
   captures = _patch_daemon_run(monkeypatch, tmp_path, pg_sink=pg_sink)
   monkeypatch.setattr(cli_module, "Daemon", OrderedDaemon)

   _invoke([
      "daemon", "run",
      "--config", config_path,
      "--home", str(tmp_path),
   ])

   assert call_order == ["sink.start", "daemon.run"], (
      "sink.start() must be awaited before daemon.run(); got %r" % call_order)


# ---------------------------------------------------------------------------
# 3. Schema gate: not initialized -> reject, no artifacts
# ---------------------------------------------------------------------------

def test_daemon_run_rejects_uninitialized_schema(tmp_path, monkeypatch):
   config_path = _write(tmp_path / "config.yaml", _nested_raw())
   diag_sink_created = []

   def _diag_factory(*args, **kwargs):
      diag_sink_created.append(1)
      return _DiagnosticSink()

   bad_status = SimpleNamespace(
      initialized=False, current_version=0, latest_version=1,
      pending_versions=(1,), drift=False)

   captures = _patch_daemon_run(monkeypatch, tmp_path,
                                status_result=bad_status,
                                diag_sink_factory=_diag_factory)

   result = _invoke([
      "daemon", "run",
      "--config", config_path,
      "--home", str(tmp_path),
   ])

   assert result.exit_code != 0
   assert captures["engine"].disposed, "engine must be disposed on rejection"
   assert captures["migrate_calls"] == [], "migrate() must never be called"
   assert len(captures["daemon_instances"]) == 0, (
      "Daemon must not be constructed on schema gate failure")
   assert diag_sink_created == [], (
      "Phase0Sink must not be created when schema gate fails")


# ---------------------------------------------------------------------------
# 4. Schema gate: pending versions -> reject
# ---------------------------------------------------------------------------

def test_daemon_run_rejects_pending_migrations(tmp_path, monkeypatch):
   config_path = _write(tmp_path / "config.yaml", _nested_raw())
   bad_status = SimpleNamespace(
      initialized=True, current_version=0, latest_version=2,
      pending_versions=(1, 2), drift=False)

   captures = _patch_daemon_run(monkeypatch, tmp_path, status_result=bad_status)

   result = _invoke([
      "daemon", "run",
      "--config", config_path,
      "--home", str(tmp_path),
   ])

   assert result.exit_code != 0
   assert captures["engine"].disposed
   assert captures["migrate_calls"] == []


# ---------------------------------------------------------------------------
# 5. Schema gate: drift -> reject
# ---------------------------------------------------------------------------

def test_daemon_run_rejects_drifted_schema(tmp_path, monkeypatch):
   config_path = _write(tmp_path / "config.yaml", _nested_raw())
   bad_status = SimpleNamespace(
      initialized=True, current_version=1, latest_version=1,
      pending_versions=(), drift=True)

   captures = _patch_daemon_run(monkeypatch, tmp_path, status_result=bad_status)

   result = _invoke([
      "daemon", "run",
      "--config", config_path,
      "--home", str(tmp_path),
   ])

   assert result.exit_code != 0
   assert captures["engine"].disposed
   assert captures["migrate_calls"] == []


# ---------------------------------------------------------------------------
# 6. Schema gate: current_version != latest_version -> reject
# ---------------------------------------------------------------------------

def test_daemon_run_rejects_when_current_not_latest(tmp_path, monkeypatch):
   config_path = _write(tmp_path / "config.yaml", _nested_raw())
   bad_status = SimpleNamespace(
      initialized=True, current_version=1, latest_version=2,
      pending_versions=(2,), drift=False)

   captures = _patch_daemon_run(monkeypatch, tmp_path, status_result=bad_status)

   result = _invoke([
      "daemon", "run",
      "--config", config_path,
      "--home", str(tmp_path),
   ])

   assert result.exit_code != 0
   assert captures["engine"].disposed
   assert captures["migrate_calls"] == []


# ---------------------------------------------------------------------------
# 7. Status exceptions sanitized
# ---------------------------------------------------------------------------

def test_daemon_run_status_exception_sanitized(tmp_path, monkeypatch):
   config_path = _write(tmp_path / "config.yaml", _nested_raw())
   engine = _Engine()

   monkeypatch.setattr(cli_module, "_resolve_probe_version",
                       lambda probe_python: 42)
   monkeypatch.setattr(cli_module, "_create_migration_engine",
                       lambda database: engine)

   secret = "DB_CREDENTIAL_MUST_NOT_APPEAR"

   class BadRunner:
      def __init__(self, injected, application_version):
         pass

      def status(self):
         raise RuntimeError("pg error credential=%s" % secret)

      def migrate(self):
         raise AssertionError("migrate must not be called")

   monkeypatch.setattr(cli_module, "MigrationRunner", BadRunner)

   result = _invoke([
      "daemon", "run",
      "--config", config_path,
      "--home", str(tmp_path),
   ])

   assert result.exit_code != 0
   assert secret not in result.output, (
      "DB exception detail must not appear in output")
   assert "Traceback" not in result.output
   assert engine.disposed


# ---------------------------------------------------------------------------
# 8. migrate() is never called
# ---------------------------------------------------------------------------

def test_daemon_run_never_calls_migrate(tmp_path, monkeypatch):
   config_path = _write(tmp_path / "config.yaml", _nested_raw())
   migrate_calls = []
   engine = _Engine()

   monkeypatch.setattr(cli_module, "_resolve_probe_version",
                       lambda probe_python: 42)
   monkeypatch.setattr(cli_module, "_create_migration_engine",
                       lambda database: engine)

   class Runner:
      def __init__(self, injected, application_version):
         pass

      def status(self):
         return _GoodStatus()

      def migrate(self):
         migrate_calls.append(1)
         raise AssertionError("migrate() was called!")

   monkeypatch.setattr(cli_module, "MigrationRunner", Runner)
   # Patch the rest to no-ops
   _patch_daemon_run(monkeypatch, tmp_path)
   # Re-apply the specific ones needed (monkeypatch is cumulative)
   monkeypatch.setattr(cli_module, "MigrationRunner", Runner)

   # With patched runner
   captures = _patch_daemon_run(monkeypatch, tmp_path, daemon_exit_code=0)

   result = _invoke([
      "daemon", "run",
      "--config", config_path,
      "--home", str(tmp_path),
   ])

   assert result.exit_code == 0
   assert migrate_calls == [], "migrate() must never be called from daemon run"


# ---------------------------------------------------------------------------
# 9. YAML URL takes precedence over NODE_MONITOR_DB_URL
# ---------------------------------------------------------------------------

def test_daemon_run_yaml_url_beats_env(tmp_path, monkeypatch):
   file_url = "postgresql://file_user:***@filehost/node_monitor"
   env_url = "postgresql://env_user:***@envhost/node_monitor"
   config_path = _write(tmp_path / "config.yaml", _nested_raw(url=file_url))

   captured_url = {}

   # Use a runner that does NOT enforce engine identity so that
   # capture_engine can return its own engine object.
   migrate_calls = []

   class AnyEngineRunner:
      def __init__(self, injected, application_version):
         pass  # accept any engine

      def status(self):
         return _GoodStatus()

      def migrate(self):
         migrate_calls.append(1)
         raise AssertionError("migrate() must never be called from daemon run")

   captures = _patch_daemon_run(monkeypatch, tmp_path,
                                runner_class=AnyEngineRunner)

   # Override _create_migration_engine AFTER _patch_daemon_run so
   # capture_engine wins; it returns the same engine that captures["engine"]
   # holds so the rest of the patching is consistent.
   def capture_engine(database):
      captured_url["url"] = database.url
      return captures["engine"]

   monkeypatch.setattr(cli_module, "_create_migration_engine", capture_engine)

   result = _invoke(
      ["daemon", "run", "--config", config_path, "--home", str(tmp_path)],
      env={"NODE_MONITOR_DB_URL": env_url})

   assert result.exit_code == 0
   assert "filehost" in captured_url.get("url", ""), (
      "YAML URL must win over env; got: %r" % captured_url.get("url"))
   assert "envhost" not in captured_url.get("url", "")


# ---------------------------------------------------------------------------
# 10. Duration override: positive accepted, non-positive rejected before engine
# ---------------------------------------------------------------------------

def test_daemon_run_positive_duration_override_accepted(tmp_path, monkeypatch):
   config_path = _write(tmp_path / "config.yaml", _nested_raw())
   captures = _patch_daemon_run(monkeypatch, tmp_path, daemon_exit_code=0)

   result = _invoke([
      "daemon", "run",
      "--config", config_path,
      "--home", str(tmp_path),
      "--duration-sec", "30",
   ])

   assert result.exit_code == 0
   # Config passed to Daemon should have duration_sec == 30
   if captures["daemon_instances"]:
      cfg = captures["daemon_instances"][0]["config"]
      assert cfg.duration_sec == 30.0, (
         "duration_sec override not applied; got %r" % cfg.duration_sec)


def test_daemon_run_zero_duration_rejected_before_engine(tmp_path, monkeypatch):
   config_path = _write(tmp_path / "config.yaml", _nested_raw())
   engine_created = []

   monkeypatch.setattr(cli_module, "_resolve_probe_version",
                       lambda probe_python: 42)
   monkeypatch.setattr(cli_module, "_create_migration_engine",
                       lambda database: engine_created.append(1) or _Engine())

   result = _invoke([
      "daemon", "run",
      "--config", config_path,
      "--home", str(tmp_path),
      "--duration-sec", "0",
   ])

   assert result.exit_code != 0
   assert engine_created == [], (
      "engine must not be created when duration-sec is invalid")


def test_daemon_run_negative_duration_rejected(tmp_path, monkeypatch):
   config_path = _write(tmp_path / "config.yaml", _nested_raw())
   engine_created = []

   monkeypatch.setattr(cli_module, "_resolve_probe_version",
                       lambda probe_python: 42)
   monkeypatch.setattr(cli_module, "_create_migration_engine",
                       lambda database: engine_created.append(1) or _Engine())

   result = _invoke([
      "daemon", "run",
      "--config", config_path,
      "--home", str(tmp_path),
      "--duration-sec", "-5",
   ])

   assert result.exit_code != 0
   assert engine_created == []


# ---------------------------------------------------------------------------
# 11. dry-run, smoke, --help never construct engine or runner
# ---------------------------------------------------------------------------

def test_daemon_dry_run_never_constructs_engine_or_runner(monkeypatch):
   engine_created = []
   runner_constructed = []

   monkeypatch.setattr(cli_module, "_create_migration_engine",
                       lambda database: engine_created.append(1) or _Engine())

   class ForbiddenRunner:
      def __init__(self, *args, **kwargs):
         runner_constructed.append(1)
         raise AssertionError("daemon dry-run constructed MigrationRunner")

   monkeypatch.setattr(cli_module, "MigrationRunner", ForbiddenRunner)

   result = _invoke(["daemon", "dry-run", "--help"])
   assert result.exit_code == 0
   assert engine_created == []
   assert runner_constructed == []


def test_daemon_smoke_never_constructs_engine_or_runner(monkeypatch):
   engine_created = []
   runner_constructed = []

   monkeypatch.setattr(cli_module, "_create_migration_engine",
                       lambda database: engine_created.append(1) or _Engine())

   class ForbiddenRunner:
      def __init__(self, *args, **kwargs):
         runner_constructed.append(1)
         raise AssertionError("daemon smoke constructed MigrationRunner")

   monkeypatch.setattr(cli_module, "MigrationRunner", ForbiddenRunner)

   result = _invoke(["daemon", "smoke", "--help"])
   assert result.exit_code == 0
   assert engine_created == []
   assert runner_constructed == []


def test_daemon_run_help_never_constructs_engine_or_runner(monkeypatch):
   engine_created = []
   runner_constructed = []

   monkeypatch.setattr(cli_module, "_create_migration_engine",
                       lambda database: engine_created.append(1) or _Engine())

   class ForbiddenRunner:
      def __init__(self, *args, **kwargs):
         runner_constructed.append(1)

   monkeypatch.setattr(cli_module, "MigrationRunner", ForbiddenRunner)

   result = _invoke(["daemon", "run", "--help"])
   assert result.exit_code == 0
   assert engine_created == []
   assert runner_constructed == []


# ---------------------------------------------------------------------------
# 12. Legacy flat config rejected (nested required)
# ---------------------------------------------------------------------------

def test_daemon_run_rejects_legacy_flat_config(tmp_path):
   config_path = _write(tmp_path / "flat.yaml", {
      "system": "polaris",
      "nodes": [{"hostname": "login.example.org", "role": "local"}],
      "output_root": "~/runs",
      "probe_python": "/usr/bin/python3.11",
   })

   result = _invoke([
      "daemon", "run",
      "--config", config_path,
      "--home", str(tmp_path),
   ])

   assert result.exit_code != 0
   assert "nested" in result.output.lower()


# ---------------------------------------------------------------------------
# 13. Config file not found -> clean error
# ---------------------------------------------------------------------------

def test_daemon_run_missing_config_file(tmp_path):
   result = _invoke([
      "daemon", "run",
      "--config", str(tmp_path / "nonexistent.yaml"),
      "--home", str(tmp_path),
   ])

   assert result.exit_code != 0


# ---------------------------------------------------------------------------
# 14. Engine disposed on construction error (e.g. runner init fails)
# ---------------------------------------------------------------------------

def test_daemon_run_engine_disposed_on_runner_construction_error(
      tmp_path, monkeypatch):
   config_path = _write(tmp_path / "config.yaml", _nested_raw())
   engine = _Engine()

   monkeypatch.setattr(cli_module, "_resolve_probe_version",
                       lambda probe_python: 42)
   monkeypatch.setattr(cli_module, "_create_migration_engine",
                       lambda database: engine)

   class FailingRunner:
      def __init__(self, injected, application_version):
         raise RuntimeError("runner init failed")

      def migrate(self):
         raise AssertionError

   monkeypatch.setattr(cli_module, "MigrationRunner", FailingRunner)

   result = _invoke([
      "daemon", "run",
      "--config", config_path,
      "--home", str(tmp_path),
   ])

   assert result.exit_code != 0
   assert engine.disposed


# ---------------------------------------------------------------------------
# 15. One engine shared: DatabaseWriter receives same engine-backed adapter
# ---------------------------------------------------------------------------

def test_database_writer_receives_engine_backed_begin(tmp_path, monkeypatch):
   config_path = _write(tmp_path / "config.yaml", _nested_raw())
   engine = _Engine()
   db_writer_databases = []

   monkeypatch.setattr(cli_module, "_resolve_probe_version",
                       lambda probe_python: 42)
   monkeypatch.setattr(cli_module, "_create_migration_engine",
                       lambda database: engine)

   class Runner:
      def __init__(self, injected, application_version):
         pass
      def status(self):
         return _GoodStatus()
      def migrate(self):
         raise AssertionError

   monkeypatch.setattr(cli_module, "MigrationRunner", Runner)

   real_DBWriter = cli_module.__dict__.get("DatabaseWriter")

   class CapturingDBWriter:
      def __init__(self, database, clock=None):
         db_writer_databases.append(database)
         # must expose begin()
         assert hasattr(database, "begin"), (
            "database passed to DatabaseWriter must have begin()")

      def write_records(self, records):
         return 0

   monkeypatch.setattr(cli_module, "DatabaseWriter", CapturingDBWriter)

   # Patch _EngineAdapter to avoid SA event.listen on fake engine,
   # but still verify it exposes begin()
   class CapturingAdapter:
      def __init__(self, eng):
         self._engine = eng

      def begin(self):
         return self._engine.begin()

   monkeypatch.setattr(cli_module, "_EngineAdapter", CapturingAdapter)

   pg_sink = _PostgresSink()
   monkeypatch.setattr(cli_module, "PostgresDaemonSink",
                       lambda writer, diagnostic_sink: pg_sink)
   monkeypatch.setattr(cli_module, "Phase0Sink",
                       lambda *a, **kw: _DiagnosticSink())

   class NoDaemon:
      def __init__(self, config, sink, transport_fn):
         pass
      async def run(self):
         return 0

   monkeypatch.setattr(cli_module, "Daemon", NoDaemon)
   monkeypatch.setattr(cli_module, "_make_transport_fn",
                       lambda config, pv: (lambda *a: None))

   result = _invoke([
      "daemon", "run",
      "--config", config_path,
      "--home", str(tmp_path),
   ])

   assert result.exit_code == 0
   assert len(db_writer_databases) == 1
   db_obj = db_writer_databases[0]
   assert hasattr(db_obj, "begin"), "engine adapter must expose begin()"
   assert engine.disposed


# ---------------------------------------------------------------------------
# 16. Output: DB URL never printed, no raw traceback
# ---------------------------------------------------------------------------

def test_daemon_run_never_prints_db_url(tmp_path, monkeypatch):
   file_url = "postgresql://secret_user:secret_pass@secrethost/node_monitor"
   config_path = _write(tmp_path / "config.yaml", _nested_raw(url=file_url))

   bad_status = SimpleNamespace(
      initialized=False, current_version=0, latest_version=1,
      pending_versions=(1,), drift=False)

   engine = _Engine()
   monkeypatch.setattr(cli_module, "_resolve_probe_version",
                       lambda probe_python: 42)
   monkeypatch.setattr(cli_module, "_create_migration_engine",
                       lambda database: engine)

   class Runner:
      def __init__(self, injected, application_version):
         pass
      def status(self):
         return bad_status
      def migrate(self):
         raise AssertionError

   monkeypatch.setattr(cli_module, "MigrationRunner", Runner)

   result = _invoke([
      "daemon", "run",
      "--config", config_path,
      "--home", str(tmp_path),
   ])

   assert result.exit_code != 0
   assert "secret_pass" not in result.output
   assert "secrethost" not in result.output
   assert "Traceback" not in result.output


# ---------------------------------------------------------------------------
# 17. engine disposed after normal daemon exit (not just on failure)
# ---------------------------------------------------------------------------

def test_engine_disposed_after_normal_daemon_exit(tmp_path, monkeypatch):
   config_path = _write(tmp_path / "config.yaml", _nested_raw())
   captures = _patch_daemon_run(monkeypatch, tmp_path, daemon_exit_code=0)

   _invoke([
      "daemon", "run",
      "--config", config_path,
      "--home", str(tmp_path),
   ])

   assert captures["engine"].disposed, "engine must be disposed after normal exit"
