"""Tests for ``node-monitor config check``.

Design: node_monitor_planning tiered-storage-retention-design.md
"Configuration contract" ("`node-monitor config check` prints the
selected config path and sanitized effective configuration, never a
credential-bearing URL."). Plan:
increment1-nested-config-pg-connection.md Task 5.

Every discovery-related test here passes explicit ``--home``/``--etc``
overrides and writes only into ``tmp_path`` -- never the real,
invoking-user ``$HOME`` or the real ``/etc``.
"""

import os

import pytest
import yaml
from click.testing import CliRunner

from node_monitor.cli.main import cli


def _invoke(args, env=None):
   runner = CliRunner()
   return runner.invoke(cli, args, env=env, catch_exceptions=False)


def _write_yaml(path, raw):
   with open(path, "w") as handle:
      yaml.safe_dump(raw, handle)
   return str(path)


def _flat_config(**overrides):
   raw = {
      "system": "polaris",
      "nodes": [
         {"hostname": "login-04.example.org", "role": "local"},
      ],
      "output_root": "~/phase0-runs",
      "probe_python": "/usr/bin/python3.11",
   }
   raw.update(overrides)
   return raw


def _nested_config(**overrides):
   raw = {
      "system": "polaris",
      "nodes": [
         {"hostname": "login-04.example.org", "role": "local"},
         {"hostname": "login-01.example.org", "role": "remote",
          "ssh_target": "polaris-login-01.head"},
      ],
      "probe_python": "/usr/bin/python3.11",
      "output": {"root": "~/phase0-runs", "compress_census": True},
      "collection": {
         "counter_interval_sec": 10,
         "census_interval_sec": 60,
         "counter_rollup_interval_sec": 60,
         "usage_interval_sec": 900,
         "duration_sec": 86400,
         "keep_raw_args": True,
      },
      "ssh": {
         "connect_timeout_sec": 8,
         "counter_timeout_sec": 15,
         "census_timeout_sec": 20,
         "max_parallel_polls": 4,
      },
      "safety": {"min_free_disk_pct": 10},
      "database": {
         "url": "postgresql://cli_user:***@localhost/pbs_monitor_dev",
         "schema": "node_monitor",
         "pool_size": 1,
         "max_overflow": 0,
         "echo_sql": False,
         "pool_pre_ping": True,
         "pool_timeout_sec": 10,
         "pool_recycle_sec": 3600,
         "connect_args": {
            "connect_timeout": 10,
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10,
            "keepalives_count": 3,
            "options": "-c statement_timeout=15000 -c lock_timeout=5000",
         },
      },
      "retention": {
         "enabled": False, "dry_run": True,
         "diagnostic_census_days": 7, "counter_diagnostics_days": 7,
         "counter_minute_days": 30, "usage_intervals_days": 180,
         "usage_hourly_days": 730, "counter_hourly_days": 730,
         "daily_days": 0, "poll_failures_days": 90,
         "poll_failures_daily_days": 730, "collection_log_days": 90,
         "delete_batch_rows": 50000, "max_batches_per_run": 20,
         "lag_alert_after_runs": 3, "housekeeping_utc": "04:00",
         "require_complete_rollup": True,
      },
   }
   raw.update(overrides)
   return raw


# --------------------------------------------------------------------------
# Requirement 1: flat config validates, prints "layout: legacy-flat", and
# warns of deprecation without changing daemon commands.
# --------------------------------------------------------------------------

class TestFlatLayout:
   def test_flat_config_validates_and_reports_legacy_layout(self, tmp_path):
      home_dir = tmp_path / "home"
      home_dir.mkdir()
      (home_dir / "phase0-runs").mkdir()
      config_path = _write_yaml(tmp_path / "config.yaml", _flat_config())
      result = _invoke(
         ["config", "check", "--config", config_path, "--home", str(home_dir)])
      assert result.exit_code == 0, result.output
      assert "layout: legacy-flat" in result.output
      assert "deprecat" in result.output.lower()


# --------------------------------------------------------------------------
# Requirements 2, 3, 7: nested config prints sanitized effective
# configuration; the real sentinel password never appears; connect_args
# .options and raw dataclass repr never appear.
# --------------------------------------------------------------------------

class TestNestedLayout:
   def test_nested_config_prints_sanitized_summary(self, tmp_path):
      home_dir = tmp_path / "home"
      home_dir.mkdir()
      (home_dir / "phase0-runs").mkdir()
      config_path = _write_yaml(tmp_path / "config.yaml", _nested_config())
      result = _invoke(
         ["config", "check", "--config", config_path, "--home", str(home_dir)])
      assert result.exit_code == 0, result.output
      assert ("config: %s" % config_path) in result.output
      assert "layout: nested" in result.output
      assert "system: polaris" in result.output
      assert "nodes: 2" in result.output
      assert "schema: node_monitor" in result.output
      assert "pool_size: 1" in result.output
      assert "max_overflow: 0" in result.output
      assert "retention_enabled: False" in result.output
      assert "housekeeping_utc: 04:00" in result.output
      # a horizon value from retention must be surfaced somewhere.
      assert "30" in result.output

   def test_sentinel_password_never_appears(self, tmp_path):
      home_dir = tmp_path / "home"
      home_dir.mkdir()
      (home_dir / "phase0-runs").mkdir()
      raw = _nested_config()
      raw["database"]["url"] = (
         "postgresql://cli_user:supersecret@localhost/pbs_monitor_dev")
      config_path = _write_yaml(tmp_path / "config.yaml", raw)
      result = _invoke(
         ["config", "check", "--config", config_path, "--home", str(home_dir)])
      assert result.exit_code == 0, result.output
      assert "supersecret" not in result.output
      assert "***" in result.output

   def test_connect_args_options_never_printed(self, tmp_path):
      home_dir = tmp_path / "home"
      home_dir.mkdir()
      (home_dir / "phase0-runs").mkdir()
      config_path = _write_yaml(tmp_path / "config.yaml", _nested_config())
      result = _invoke(
         ["config", "check", "--config", config_path, "--home", str(home_dir)])
      assert result.exit_code == 0, result.output
      assert "statement_timeout" not in result.output
      assert "lock_timeout" not in result.output

   def test_raw_dataclass_repr_never_printed(self, tmp_path):
      home_dir = tmp_path / "home"
      home_dir.mkdir()
      (home_dir / "phase0-runs").mkdir()
      config_path = _write_yaml(tmp_path / "config.yaml", _nested_config())
      result = _invoke(
         ["config", "check", "--config", config_path, "--home", str(home_dir)])
      assert result.exit_code == 0, result.output
      assert "NodeMonitorConfig(" not in result.output
      assert "DatabaseConfig(" not in result.output


# --------------------------------------------------------------------------
# Requirement 4: CLI reads NODE_MONITOR_DB_URL only when file URL is
# absent; file URL wins.
# --------------------------------------------------------------------------

class TestDatabaseUrlEnvPriority:
   def test_env_url_used_when_file_url_absent(self, tmp_path):
      home_dir = tmp_path / "home"
      home_dir.mkdir()
      (home_dir / "phase0-runs").mkdir()
      raw = _nested_config()
      del raw["database"]["url"]
      config_path = _write_yaml(tmp_path / "config.yaml", raw)
      result = _invoke(
         ["config", "check", "--config", config_path, "--home", str(home_dir)],
         env={"NODE_MONITOR_DB_URL": "postgresql://env_user:***@dbhost/db"})
      assert result.exit_code == 0, result.output
      assert "dbhost" in result.output

   def test_explicit_file_url_wins_over_env(self, tmp_path):
      home_dir = tmp_path / "home"
      home_dir.mkdir()
      (home_dir / "phase0-runs").mkdir()
      config_path = _write_yaml(tmp_path / "config.yaml", _nested_config())
      result = _invoke(
         ["config", "check", "--config", config_path, "--home", str(home_dir)],
         env={"NODE_MONITOR_DB_URL": "postgresql://env_user:***@envhost/db"})
      assert result.exit_code == 0, result.output
      assert "envhost" not in result.output
      assert "localhost" in result.output


# --------------------------------------------------------------------------
# Requirement 5: invalid env URL/schema/mixed layout/YAML/path/discovery
# all fail nonzero.
# --------------------------------------------------------------------------

class TestFailureModes:
   def test_invalid_env_url_fails_nonzero(self, tmp_path):
      home_dir = tmp_path / "home"
      home_dir.mkdir()
      (home_dir / "phase0-runs").mkdir()
      raw = _nested_config()
      del raw["database"]["url"]
      config_path = _write_yaml(tmp_path / "config.yaml", raw)
      result = _invoke(
         ["config", "check", "--config", config_path, "--home", str(home_dir)],
         env={"NODE_MONITOR_DB_URL": "sqlite:///tmp/db.sqlite"})
      assert result.exit_code != 0

   def test_invalid_env_url_with_credentials_never_echoes_password(
         self, tmp_path):
      """An invalid ``NODE_MONITOR_DB_URL`` is rejected by
      ``config.DatabaseConfig``'s own validator BEFORE any masking
      exists to render it safely -- the raised ``ConfigError``'s own
      message therefore embeds the rejected URL verbatim, including
      its password. This must never reach stdout/stderr unredacted:
      a bad env value is exactly the kind of operator typo/paste error
      this command's whole sanitization contract exists to survive.
      """
      home_dir = tmp_path / "home"
      home_dir.mkdir()
      (home_dir / "phase0-runs").mkdir()
      raw = _nested_config()
      del raw["database"]["url"]
      config_path = _write_yaml(tmp_path / "config.yaml", raw)
      sentinel = "S3cr3t_BadEnvUrl_Pa55w0rd"
      bad_env_url = "sqlite://baduser:%s@host/db" % sentinel
      result = _invoke(
         ["config", "check", "--config", config_path, "--home", str(home_dir)],
         env={"NODE_MONITOR_DB_URL": bad_env_url})
      assert result.exit_code != 0
      assert sentinel not in result.output
      assert sentinel not in result.stderr
      # Invalid URLs are omitted from diagnostics entirely rather than
      # relying on best-effort masking of malformed authority syntax.
      assert bad_env_url not in result.output

   @pytest.mark.parametrize("authority", [
      "INVALID_DB_PASSWORD_TOKEN@host",
      ":INVALID_DB_PASSWORD_TOKEN@host",
   ])
   def test_invalid_env_url_never_echoes_password_without_username(
         self, tmp_path, authority):
      home_dir = tmp_path / "home"
      home_dir.mkdir()
      (home_dir / "phase0-runs").mkdir()
      raw = _nested_config()
      del raw["database"]["url"]
      config_path = _write_yaml(tmp_path / "config.yaml", raw)
      result = _invoke(
         ["config", "check", "--config", config_path, "--home", str(home_dir)],
         env={"NODE_MONITOR_DB_URL": "sqlite://%s/db" % authority})
      assert result.exit_code != 0
      assert "INVALID_DB_PASSWORD_TOKEN" not in result.output
      assert "INVALID_DB_PASSWORD_TOKEN" not in result.stderr

   @pytest.mark.parametrize("url", [
      "postgresql://user@host:INVALID_PORT_TOKEN/db",
      "postgresql://user@[INVALID_BRACKET_TOKEN/db",
   ])
   def test_malformed_postgresql_authority_is_safely_rejected(
         self, tmp_path, url):
      home_dir = tmp_path / "home"
      home_dir.mkdir()
      (home_dir / "phase0-runs").mkdir()
      raw = _nested_config()
      raw["database"]["url"] = url
      config_path = _write_yaml(tmp_path / "config.yaml", raw)
      result = _invoke(
         ["config", "check", "--config", config_path, "--home", str(home_dir)])
      assert result.exit_code != 0
      assert "INVALID_" not in result.output
      assert "INVALID_" not in result.stderr
      assert "Traceback" not in result.output
      assert not isinstance(result.exception, ValueError)

   def test_invalid_schema_fails_nonzero(self, tmp_path):
      home_dir = tmp_path / "home"
      home_dir.mkdir()
      (home_dir / "phase0-runs").mkdir()
      raw = _nested_config()
      raw["database"]["schema"] = "public"
      config_path = _write_yaml(tmp_path / "config.yaml", raw)
      result = _invoke(
         ["config", "check", "--config", config_path, "--home", str(home_dir)])
      assert result.exit_code != 0

   def test_mixed_layout_fails_nonzero(self, tmp_path):
      home_dir = tmp_path / "home"
      home_dir.mkdir()
      (home_dir / "phase0-runs").mkdir()
      raw = _nested_config()
      raw["output_root"] = "~/phase0-runs"
      config_path = _write_yaml(tmp_path / "config.yaml", raw)
      result = _invoke(
         ["config", "check", "--config", config_path, "--home", str(home_dir)])
      assert result.exit_code != 0

   def test_invalid_yaml_fails_nonzero(self, tmp_path):
      home_dir = tmp_path / "home"
      home_dir.mkdir()
      config_path = tmp_path / "config.yaml"
      config_path.write_text("system: [unterminated\n")
      result = _invoke(
         ["config", "check", "--config", str(config_path),
          "--home", str(home_dir)])
      assert result.exit_code != 0

   def test_missing_explicit_path_fails_nonzero(self, tmp_path):
      home_dir = tmp_path / "home"
      home_dir.mkdir()
      result = _invoke(
         ["config", "check", "--config", str(tmp_path / "missing.yaml"),
          "--home", str(home_dir)])
      assert result.exit_code != 0

   def test_no_discoverable_config_fails_nonzero(self, tmp_path):
      home_dir = tmp_path / "home"
      home_dir.mkdir()
      cwd = tmp_path / "cwd"
      cwd.mkdir()
      etc_path = tmp_path / "etc" / "node_monitor" / "config.yaml"
      result = _invoke(
         ["config", "check", "--home", str(home_dir), "--cwd", str(cwd),
          "--etc-path", str(etc_path)])
      assert result.exit_code != 0


# --------------------------------------------------------------------------
# Requirement 6: discovery is isolated from the real home and /etc.
# --------------------------------------------------------------------------

class TestDiscoveryIsolation:
   def test_discovery_uses_injected_home_cwd_etc_not_real_ones(self, tmp_path):
      home_dir = tmp_path / "home"
      home_dir.mkdir()
      (home_dir / "phase0-runs").mkdir()
      cwd = tmp_path / "cwd"
      cwd.mkdir()
      etc_path = tmp_path / "etc" / "node_monitor" / "config.yaml"
      dotfile = home_dir / ".node_monitor.yaml"
      dotfile.write_text(yaml.safe_dump(_flat_config()))
      result = _invoke(
         ["config", "check", "--home", str(home_dir), "--cwd", str(cwd),
          "--etc-path", str(etc_path)])
      assert result.exit_code == 0, result.output
      assert str(dotfile) in result.output
