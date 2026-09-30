"""Tests for operator-only database status/migrate commands."""

from types import SimpleNamespace

import pytest
import yaml
from click.testing import CliRunner

import node_monitor.cli.main as cli_module
from node_monitor.cli.main import cli
from node_monitor.database.migration import (
   MigrationApplyError,
   MigrationDriftError,
   MigrationLockError,
)


def _config(url="postgresql://file_user:file_password@filehost/node_monitor"):
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


class _Engine:
   def __init__(self):
      self.disposed = False

   def dispose(self):
      self.disposed = True


def _invoke(args, env=None):
   return CliRunner().invoke(cli, args, env=env, catch_exceptions=False)


def test_database_status_loads_nested_config_and_is_read_only(tmp_path, monkeypatch):
   config_path = _write(tmp_path / "config.yaml", _config())
   engine = _Engine()
   captured = {}

   def fake_engine(database):
      captured["database"] = database
      return engine

   class Runner:
      def __init__(self, injected, application_version):
         assert injected is engine
         captured["application_version"] = application_version

      def status(self):
         return SimpleNamespace(
            initialized=True, current_version=1, latest_version=1,
            pending_versions=(), drift=False)

      def migrate(self):
         raise AssertionError("status must never migrate")

   monkeypatch.setattr(cli_module, "_create_migration_engine", fake_engine)
   monkeypatch.setattr(cli_module, "MigrationRunner", Runner)
   result = _invoke(["database", "status", "--config", config_path,
                     "--home", str(tmp_path)])

   assert result.exit_code == 0, result.output
   assert "initialized: True" in result.output
   assert "current_version: 1" in result.output
   assert "latest_version: 1" in result.output
   assert "pending_versions: none" in result.output
   assert captured["database"].url.endswith("@filehost/node_monitor")
   assert "file_password" not in result.output
   assert engine.disposed


def test_database_migrate_uses_file_url_over_environment(tmp_path, monkeypatch):
   config_path = _write(tmp_path / "config.yaml", _config())
   engine = _Engine()
   captured = {}

   def fake_engine(database):
      captured["url"] = database.url
      return engine

   class Runner:
      def __init__(self, injected, application_version):
         assert injected is engine

      def migrate(self):
         return SimpleNamespace(
            applied_versions=(1,), current_version=1, latest_version=1)

   monkeypatch.setattr(cli_module, "_create_migration_engine", fake_engine)
   monkeypatch.setattr(cli_module, "MigrationRunner", Runner)
   result = _invoke(
      ["database", "migrate", "--config", config_path,
       "--home", str(tmp_path)],
      env={"NODE_MONITOR_DB_URL":
           "postgresql://env_user:env_password@envhost/node_monitor"})

   assert result.exit_code == 0, result.output
   assert captured["url"].endswith("@filehost/node_monitor")
   assert "applied_versions: 1" in result.output
   assert "file_password" not in result.output
   assert "env_password" not in result.output
   assert engine.disposed


def test_database_uses_environment_url_when_file_omits_it(tmp_path, monkeypatch):
   raw = _config()
   del raw["database"]["url"]
   config_path = _write(tmp_path / "config.yaml", raw)
   engine = _Engine()
   captured = {}

   def fake_engine(database):
      captured["url"] = database.url
      return engine

   class Runner:
      def __init__(self, injected, application_version):
         pass

      def status(self):
         return SimpleNamespace(
            initialized=False, current_version=0, latest_version=1,
            pending_versions=(1,), drift=False)

   monkeypatch.setattr(cli_module, "_create_migration_engine", fake_engine)
   monkeypatch.setattr(cli_module, "MigrationRunner", Runner)
   env_url = "postgresql://env_user:env_password@envhost/node_monitor"
   result = _invoke(
      ["database", "status", "--config", config_path,
       "--home", str(tmp_path)],
      env={"NODE_MONITOR_DB_URL": env_url})

   assert result.exit_code == 0, result.output
   assert captured["url"] == env_url
   assert "env_password" not in result.output


def test_database_commands_reject_legacy_flat_config(tmp_path):
   config_path = _write(tmp_path / "flat.yaml", {
      "system": "polaris",
      "nodes": [{"hostname": "login.example.org", "role": "local"}],
      "output_root": "~/runs",
      "probe_python": "/usr/bin/python3.11",
   })
   result = _invoke(["database", "status", "--config", config_path,
                     "--home", str(tmp_path)])
   assert result.exit_code != 0
   assert "nested" in result.output.lower()


def test_database_failures_are_nonzero_and_sanitized(tmp_path, monkeypatch):
   config_path = _write(tmp_path / "config.yaml", _config())
   secret = "DATABASE_PASSWORD_MUST_NOT_LEAK"

   class Runner:
      def __init__(self, engine, application_version):
         pass

      def migrate(self):
         raise MigrationApplyError(
            "migration failed; rejected credential %s" % secret)

   monkeypatch.setattr(cli_module, "_create_migration_engine", lambda config: _Engine())
   monkeypatch.setattr(cli_module, "MigrationRunner", Runner)
   result = _invoke(["database", "migrate", "--config", config_path,
                     "--home", str(tmp_path)])
   assert result.exit_code != 0
   assert secret not in result.output
   assert "migration failed" in result.output.lower()
   assert "Traceback" not in result.output


@pytest.mark.parametrize("error", [MigrationLockError("busy"),
                                    MigrationDriftError("drift")])
def test_database_lock_and_drift_fail_nonzero(tmp_path, monkeypatch, error):
   config_path = _write(tmp_path / "config.yaml", _config())

   class Runner:
      def __init__(self, engine, application_version):
         pass

      def migrate(self):
         raise error

   monkeypatch.setattr(cli_module, "_create_migration_engine", lambda config: _Engine())
   monkeypatch.setattr(cli_module, "MigrationRunner", Runner)
   result = _invoke(["database", "migrate", "--config", config_path,
                     "--home", str(tmp_path)])
   assert result.exit_code != 0


def test_daemon_group_has_no_migration_command_or_runner_construction(monkeypatch):
   class ForbiddenRunner:
      def __init__(self, *args, **kwargs):
         raise AssertionError("daemon path constructed MigrationRunner")

   monkeypatch.setattr(cli_module, "MigrationRunner", ForbiddenRunner)
   result = _invoke(["daemon", "--help"])
   assert result.exit_code == 0
   assert "migrate" not in result.output.lower()
