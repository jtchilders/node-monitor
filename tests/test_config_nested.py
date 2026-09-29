"""Tests for node_monitor.config's Phase-1 strict nested configuration.

Design: node_monitor_planning tiered-storage-retention-design.md
"Configuration contract". Plan: increment1-nested-config-pg-connection.md
Task 1 ("Frozen nested section models and validators"), Task 2 ("Layout
dispatch, environment priority, and discovery"), Task 3 ("Complete nested
example").

This module never touches the legacy Phase0Config/load_config path --
tests/test_config.py already owns that contract and must keep passing
byte-for-byte unchanged.
"""

import copy
import os

import pytest
import yaml

from node_monitor.config import (
   CollectionConfig,
   ConfigError,
   DatabaseConfig,
   NodeMonitorConfig,
   OutputConfig,
   Phase0Config,
   RetentionConfig,
   SafetyConfig,
   SshConfig,
   discover_config_path,
   load_any_config,
   load_config_file_any,
   load_nested_config,
)


HOME = "/home/canary"
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _base_nested(**overrides):
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
         "url": "postgresql://localhost/pbs_monitor_dev",
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
   for key, value in overrides.items():
      raw[key] = value
   return raw


def _load(**overrides):
   return load_nested_config(_base_nested(**overrides), home=HOME)


# --------------------------------------------------------------------------
# Task 1, requirement 1: valid input returns a fully typed NodeMonitorConfig.
# --------------------------------------------------------------------------

class TestValidShape:
   def test_valid_input_returns_typed_sections(self):
      cfg = _load()
      assert isinstance(cfg, NodeMonitorConfig)
      assert isinstance(cfg.output, OutputConfig)
      assert isinstance(cfg.collection, CollectionConfig)
      assert isinstance(cfg.ssh, SshConfig)
      assert isinstance(cfg.safety, SafetyConfig)
      assert isinstance(cfg.database, DatabaseConfig)
      assert isinstance(cfg.retention, RetentionConfig)
      assert cfg.system == "polaris"
      assert cfg.probe_python == "/usr/bin/python3.11"

   def test_every_dataclass_is_frozen(self):
      cfg = _load()
      for obj in (cfg, cfg.output, cfg.collection, cfg.ssh, cfg.safety,
                  cfg.database, cfg.retention):
         with pytest.raises(Exception):
            obj.__setattr__("system" if obj is cfg else
                             next(iter(obj.__dataclass_fields__)), "x")

   def test_does_not_wrap_phase0config(self):
      cfg = _load()
      assert not isinstance(cfg, Phase0Config)
      assert not hasattr(cfg, "output_root")


# --------------------------------------------------------------------------
# Task 1, requirement 2: omitted optional values receive documented
# defaults.
# --------------------------------------------------------------------------

class TestDefaults:
   def test_omitted_sections_use_documented_defaults(self):
      raw = _base_nested()
      del raw["ssh"]
      del raw["safety"]
      cfg = load_nested_config(raw, home=HOME)
      assert cfg.ssh.connect_timeout_sec == 8
      assert cfg.ssh.counter_timeout_sec == 4
      assert cfg.ssh.census_timeout_sec == 20
      assert cfg.ssh.max_parallel_polls == 8
      assert cfg.safety.min_free_disk_pct == 10

   def test_omitted_output_defaults(self):
      raw = _base_nested()
      raw["output"] = {"root": "~/phase0-runs"}
      cfg = load_nested_config(raw, home=HOME)
      assert cfg.output.compress_census is False

   def test_omitted_collection_defaults(self):
      raw = _base_nested()
      raw["collection"] = {}
      cfg = load_nested_config(raw, home=HOME)
      assert cfg.collection.counter_interval_sec == 10
      assert cfg.collection.census_interval_sec == 60
      assert cfg.collection.counter_rollup_interval_sec == 60
      assert cfg.collection.usage_interval_sec == 900
      assert cfg.collection.duration_sec == 86400
      assert cfg.collection.keep_raw_args is True

   def test_omitted_retention_defaults_are_all_safe(self):
      raw = _base_nested()
      del raw["retention"]
      cfg = load_nested_config(raw, home=HOME)
      assert cfg.retention.enabled is False
      assert cfg.retention.dry_run is True

   def test_omitted_database_requires_url_from_env(self):
      raw = _base_nested()
      del raw["database"]
      cfg = load_nested_config(
         raw, home=HOME, database_url_env="postgresql://localhost/db")
      assert cfg.database.schema == "node_monitor"
      assert cfg.database.pool_size == 1
      assert cfg.database.max_overflow == 0
      assert cfg.database.echo_sql is False
      assert cfg.database.pool_pre_ping is True
      assert cfg.database.pool_timeout_sec == 10
      assert cfg.database.pool_recycle_sec == 3600
      assert cfg.database.connect_args == ()


# --------------------------------------------------------------------------
# Task 1, requirement 3: output.root expands under injected home; /tmp and
# outside-home paths fail exactly as legacy validation.
# --------------------------------------------------------------------------

class TestOutputRoot:
   def test_tilde_expands_under_injected_home(self):
      cfg = _load()
      assert cfg.output.root == HOME + "/phase0-runs"

   def test_tmp_path_rejected(self):
      raw = _base_nested()
      raw["output"] = {"root": "/tmp/phase0-runs"}
      with pytest.raises(ConfigError, match="tmp"):
         load_nested_config(raw, home=HOME)

   def test_path_outside_home_rejected(self):
      raw = _base_nested()
      raw["output"] = {"root": "/var/phase0-runs"}
      with pytest.raises(ConfigError):
         load_nested_config(raw, home=HOME)


# --------------------------------------------------------------------------
# Task 1, requirement 4: existing node invariants and remote-only
# ssh_target remain enforced.
# --------------------------------------------------------------------------

class TestNodeInvariants:
   def test_zero_local_nodes_rejected(self):
      raw = _base_nested()
      raw["nodes"] = [{"hostname": "a.example.org", "role": "remote"}]
      with pytest.raises(ConfigError, match="local"):
         load_nested_config(raw, home=HOME)

   def test_ssh_target_on_local_node_rejected(self):
      raw = _base_nested()
      raw["nodes"] = [
         {"hostname": "login-04.example.org", "role": "local",
          "ssh_target": "login-04.head"},
      ]
      with pytest.raises(ConfigError, match="ssh_target"):
         load_nested_config(raw, home=HOME)


# --------------------------------------------------------------------------
# Task 1, requirement 5: every section recursively rejects unknown keys.
# --------------------------------------------------------------------------

class TestUnknownKeys:
   def test_top_level_unknown_key_rejected(self):
      raw = _base_nested()
      raw["bogus"] = 1
      with pytest.raises(ConfigError, match="unknown"):
         load_nested_config(raw, home=HOME)

   @pytest.mark.parametrize("section", [
      "output", "collection", "ssh", "safety", "database", "retention"])
   def test_section_unknown_key_rejected(self, section):
      raw = _base_nested()
      raw[section] = dict(raw[section])
      raw[section]["bogus"] = 1
      with pytest.raises(ConfigError, match="unknown"):
         load_nested_config(raw, home=HOME)

   def test_database_connect_args_unknown_key_rejected(self):
      raw = _base_nested()
      raw["database"] = dict(raw["database"])
      raw["database"]["connect_args"] = dict(raw["database"]["connect_args"])
      raw["database"]["connect_args"]["bogus"] = 1
      with pytest.raises(ConfigError, match="unknown"):
         load_nested_config(raw, home=HOME)


# --------------------------------------------------------------------------
# Task 1, requirement 6: numeric fields enforce finite positive values;
# integer-only fields reject booleans/floats.
# --------------------------------------------------------------------------

class TestNumericStrictness:
   @pytest.mark.parametrize("key", [
      "counter_interval_sec", "census_interval_sec",
      "counter_rollup_interval_sec", "usage_interval_sec", "duration_sec"])
   def test_collection_positive_number_enforced(self, key):
      raw = _base_nested()
      raw["collection"] = dict(raw["collection"])
      raw["collection"][key] = 0
      with pytest.raises(ConfigError):
         load_nested_config(raw, home=HOME)

   def test_collection_keep_raw_args_rejects_int(self):
      raw = _base_nested()
      raw["collection"] = dict(raw["collection"])
      raw["collection"]["keep_raw_args"] = 1
      with pytest.raises(ConfigError):
         load_nested_config(raw, home=HOME)

   def test_ssh_max_parallel_polls_rejects_float(self):
      raw = _base_nested()
      raw["ssh"] = dict(raw["ssh"])
      raw["ssh"]["max_parallel_polls"] = 4.5
      with pytest.raises(ConfigError):
         load_nested_config(raw, home=HOME)

   def test_database_pool_size_rejects_bool(self):
      raw = _base_nested()
      raw["database"] = dict(raw["database"])
      raw["database"]["pool_size"] = True
      with pytest.raises(ConfigError):
         load_nested_config(raw, home=HOME)


# --------------------------------------------------------------------------
# Task 1, requirement 7: database.schema must be node_monitor; public fails.
# --------------------------------------------------------------------------

class TestDatabaseSchema:
   def test_schema_must_be_node_monitor(self):
      raw = _base_nested()
      raw["database"] = dict(raw["database"])
      raw["database"]["schema"] = "public"
      with pytest.raises(ConfigError, match="node_monitor"):
         load_nested_config(raw, home=HOME)

   def test_schema_node_monitor_accepted(self):
      cfg = _load()
      assert cfg.database.schema == "node_monitor"


# --------------------------------------------------------------------------
# Task 1, requirement 8: PostgreSQL and PostgreSQL-driver URLs pass;
# SQLite/MySQL/empty values fail.
# --------------------------------------------------------------------------

class TestDatabaseUrl:
   @pytest.mark.parametrize("url", [
      "postgresql://localhost/db",
      "postgresql+psycopg2://user:pw@localhost:5432/db",
   ])
   def test_postgresql_urls_accepted(self, url):
      raw = _base_nested()
      raw["database"] = dict(raw["database"])
      raw["database"]["url"] = url
      cfg = load_nested_config(raw, home=HOME)
      assert cfg.database.url == url

   @pytest.mark.parametrize("url", [
      "sqlite:///tmp/db.sqlite",
      "mysql://localhost/db",
      "",
   ])
   def test_non_postgresql_urls_rejected(self, url):
      raw = _base_nested()
      raw["database"] = dict(raw["database"])
      raw["database"]["url"] = url
      with pytest.raises(ConfigError):
         load_nested_config(raw, home=HOME)


# --------------------------------------------------------------------------
# Task 1, requirement 9: pool_size positive; max_overflow exactly zero.
# --------------------------------------------------------------------------

class TestPoolLimits:
   def test_pool_size_must_be_positive(self):
      raw = _base_nested()
      raw["database"] = dict(raw["database"])
      raw["database"]["pool_size"] = 0
      with pytest.raises(ConfigError):
         load_nested_config(raw, home=HOME)

   def test_max_overflow_nonzero_rejected(self):
      raw = _base_nested()
      raw["database"] = dict(raw["database"])
      raw["database"]["max_overflow"] = 1
      with pytest.raises(ConfigError, match="max_overflow"):
         load_nested_config(raw, home=HOME)

   def test_max_overflow_zero_accepted(self):
      cfg = _load()
      assert cfg.database.max_overflow == 0


# --------------------------------------------------------------------------
# Task 1, requirement 10: connect_args keys and types are strict.
# --------------------------------------------------------------------------

class TestConnectArgs:
   def test_connect_args_stored_as_sorted_tuple_of_pairs(self):
      cfg = _load()
      assert isinstance(cfg.database.connect_args, tuple)
      keys = [pair[0] for pair in cfg.database.connect_args]
      assert keys == sorted(keys)

   def test_connect_args_wrong_type_rejected(self):
      raw = _base_nested()
      raw["database"] = dict(raw["database"])
      raw["database"]["connect_args"] = dict(raw["database"]["connect_args"])
      raw["database"]["connect_args"]["connect_timeout"] = "soon"
      with pytest.raises(ConfigError):
         load_nested_config(raw, home=HOME)

   def test_connect_args_options_must_be_string(self):
      raw = _base_nested()
      raw["database"] = dict(raw["database"])
      raw["database"]["connect_args"] = dict(raw["database"]["connect_args"])
      raw["database"]["connect_args"]["options"] = 123
      with pytest.raises(ConfigError):
         load_nested_config(raw, home=HOME)


# --------------------------------------------------------------------------
# Task 1, requirement 11: retention booleans are true booleans; day values
# are nonnegative integers; batch/run/alert limits are positive.
# --------------------------------------------------------------------------

class TestRetentionValidation:
   @pytest.mark.parametrize("key", ["enabled", "dry_run", "require_complete_rollup"])
   def test_retention_bool_fields_reject_non_bool(self, key):
      raw = _base_nested()
      raw["retention"] = dict(raw["retention"])
      raw["retention"][key] = 1
      with pytest.raises(ConfigError):
         load_nested_config(raw, home=HOME)

   @pytest.mark.parametrize("key", [
      "diagnostic_census_days", "counter_diagnostics_days",
      "counter_minute_days", "usage_intervals_days", "usage_hourly_days",
      "counter_hourly_days", "daily_days", "poll_failures_days",
      "poll_failures_daily_days", "collection_log_days"])
   def test_retention_day_fields_reject_negative(self, key):
      raw = _base_nested()
      raw["retention"] = dict(raw["retention"])
      raw["retention"][key] = -1
      with pytest.raises(ConfigError):
         load_nested_config(raw, home=HOME)

   def test_retention_day_fields_allow_zero(self):
      raw = _base_nested()
      raw["retention"] = dict(raw["retention"])
      raw["retention"]["daily_days"] = 0
      cfg = load_nested_config(raw, home=HOME)
      assert cfg.retention.daily_days == 0

   @pytest.mark.parametrize("key", [
      "delete_batch_rows", "max_batches_per_run", "lag_alert_after_runs"])
   def test_retention_positive_limit_fields_reject_zero(self, key):
      raw = _base_nested()
      raw["retention"] = dict(raw["retention"])
      raw["retention"][key] = 0
      with pytest.raises(ConfigError):
         load_nested_config(raw, home=HOME)


# --------------------------------------------------------------------------
# Task 1, requirement 12: housekeeping_utc accepts valid HH:MM and rejects
# malformed/out-of-range values.
# --------------------------------------------------------------------------

class TestHousekeepingUtc:
   @pytest.mark.parametrize("value", ["00:00", "04:00", "23:59"])
   def test_valid_hhmm_accepted(self, value):
      raw = _base_nested()
      raw["retention"] = dict(raw["retention"])
      raw["retention"]["housekeeping_utc"] = value
      cfg = load_nested_config(raw, home=HOME)
      assert cfg.retention.housekeeping_utc == value

   @pytest.mark.parametrize("value", [
      "24:00", "04:60", "4:00", "04:0", "garbage", "", "04-00"])
   def test_invalid_hhmm_rejected(self, value):
      raw = _base_nested()
      raw["retention"] = dict(raw["retention"])
      raw["retention"]["housekeeping_utc"] = value
      with pytest.raises(ConfigError):
         load_nested_config(raw, home=HOME)


# --------------------------------------------------------------------------
# Task 1, requirement 13: loading does not mutate caller dictionaries.
# --------------------------------------------------------------------------

class TestNoMutation:
   def test_load_nested_config_does_not_mutate_input(self):
      raw = _base_nested()
      snapshot = copy.deepcopy(raw)
      load_nested_config(raw, home=HOME)
      assert raw == snapshot

   def test_load_nested_config_does_not_mutate_input_with_defaults_applied(self):
      raw = _base_nested()
      del raw["retention"]
      snapshot = copy.deepcopy(raw)
      load_nested_config(raw, home=HOME)
      assert raw == snapshot


# --------------------------------------------------------------------------
# Task 1, requirement 14: DatabaseConfig.url uses dataclasses.field(repr=False).
# --------------------------------------------------------------------------

class TestNoCredentialLeakage:
   def test_database_url_absent_from_repr(self):
      raw = _base_nested()
      raw["database"] = dict(raw["database"])
      raw["database"]["url"] = "postgresql://user:supersecret@localhost/db"
      cfg = load_nested_config(raw, home=HOME)
      assert "supersecret" not in repr(cfg.database)
      assert "supersecret" not in repr(cfg)

   def test_database_url_field_has_repr_false(self):
      field = DatabaseConfig.__dataclass_fields__["url"]
      assert field.repr is False



# ==========================================================================
# Task 2: Layout dispatch, environment priority, and discovery
# ==========================================================================

def _base_flat(**overrides):
   config = {
      "system": "polaris",
      "nodes": [
         {"hostname": "polaris-login-04.example.org", "role": "local"},
         {"hostname": "polaris-login-01.example.org", "role": "remote"},
      ],
      "output_root": "~/phase0-runs",
      "probe_python": "/usr/bin/python3.11",
   }
   config.update(overrides)
   return config


class TestLayoutDispatch:
   def test_flat_returns_phase0config_unchanged(self):
      raw = _base_flat()
      cfg = load_any_config(raw, home=HOME)
      assert isinstance(cfg, Phase0Config)
      assert cfg.system == "polaris"

   def test_nested_returns_nodemonitorconfig(self):
      raw = _base_nested()
      cfg = load_any_config(raw, home=HOME)
      assert isinstance(cfg, NodeMonitorConfig)

   @pytest.mark.parametrize("flat_key,flat_value", [
      ("output_root", "~/phase0-runs"),
      ("counter_interval_sec", 10),
      ("census_interval_sec", 60),
      ("rollup_interval_sec", 60),
      ("usage_interval_sec", 900),
      ("duration_sec", 86400),
      ("counter_timeout_sec", 4),
      ("census_timeout_sec", 20),
      ("ssh_connect_timeout_sec", 8),
      ("max_parallel_polls", 8),
      ("min_free_disk_pct", 10),
      ("keep_raw_args", True),
      ("compress_census", False),
   ])
   def test_mixed_flat_and_nested_rejected(self, flat_key, flat_value):
      raw = _base_nested()
      raw[flat_key] = flat_value
      with pytest.raises(ConfigError, match="mixed"):
         load_any_config(raw, home=HOME)

   def test_missing_database_url_uses_injected_env(self):
      raw = _base_nested()
      del raw["database"]
      cfg = load_any_config(
         raw, home=HOME, database_url_env="postgresql://localhost/db")
      assert cfg.database.url == "postgresql://localhost/db"

   def test_explicit_file_url_wins_over_env(self):
      raw = _base_nested()
      cfg = load_any_config(
         raw, home=HOME, database_url_env="postgresql://other/db")
      assert cfg.database.url == "postgresql://localhost/pbs_monitor_dev"

   def test_injected_sqlite_url_fails_before_connection(self):
      raw = _base_nested()
      del raw["database"]
      with pytest.raises(ConfigError):
         load_any_config(
            raw, home=HOME, database_url_env="sqlite:///tmp/db.sqlite")


class TestDiscoverConfigPath:
   def test_explicit_existing_path_wins(self, tmp_path):
      explicit = tmp_path / "explicit.yaml"
      explicit.write_text("system: polaris\n")
      home_dotfile = tmp_path / "home" / ".node_monitor.yaml"
      home_dotfile.parent.mkdir(parents=True)
      home_dotfile.write_text("system: other\n")
      found = discover_config_path(
         explicit_path=str(explicit), home=str(tmp_path / "home"),
         cwd=str(tmp_path), etc_path=str(tmp_path / "etc.yaml"))
      assert found == str(explicit)

   def test_explicit_missing_path_fails_clearly(self, tmp_path):
      with pytest.raises(ConfigError, match="not found"):
         discover_config_path(
            explicit_path=str(tmp_path / "missing.yaml"),
            home=str(tmp_path / "home"), cwd=str(tmp_path),
            etc_path=str(tmp_path / "etc.yaml"))

   def test_automatic_order_home_dotfile_first(self, tmp_path):
      home = tmp_path / "home"
      home.mkdir()
      dotfile = home / ".node_monitor.yaml"
      dotfile.write_text("system: polaris\n")
      xdg = home / ".config" / "node_monitor" / "config.yaml"
      xdg.parent.mkdir(parents=True)
      xdg.write_text("system: other\n")
      found = discover_config_path(
         home=str(home), cwd=str(tmp_path), etc_path=str(tmp_path / "etc.yaml"))
      assert found == str(dotfile)

   def test_automatic_order_xdg_second(self, tmp_path):
      home = tmp_path / "home"
      home.mkdir()
      xdg = home / ".config" / "node_monitor" / "config.yaml"
      xdg.parent.mkdir(parents=True)
      xdg.write_text("system: polaris\n")
      etc = tmp_path / "etc" / "config.yaml"
      etc.parent.mkdir(parents=True)
      etc.write_text("system: other\n")
      found = discover_config_path(
         home=str(home), cwd=str(tmp_path), etc_path=str(etc))
      assert found == str(xdg)

   def test_automatic_order_etc_third(self, tmp_path):
      home = tmp_path / "home"
      home.mkdir()
      etc = tmp_path / "etc" / "config.yaml"
      etc.parent.mkdir(parents=True)
      etc.write_text("system: polaris\n")
      cwd = tmp_path / "cwd"
      cwd.mkdir()
      cwd_config = cwd / "node_monitor.yaml"
      cwd_config.write_text("system: other\n")
      found = discover_config_path(home=str(home), cwd=str(cwd), etc_path=str(etc))
      assert found == str(etc)

   def test_automatic_order_cwd_last(self, tmp_path):
      home = tmp_path / "home"
      home.mkdir()
      cwd = tmp_path / "cwd"
      cwd.mkdir()
      cwd_config = cwd / "node_monitor.yaml"
      cwd_config.write_text("system: polaris\n")
      found = discover_config_path(
         home=str(home), cwd=str(cwd), etc_path=str(tmp_path / "no-etc.yaml"))
      assert found == str(cwd_config)

   def test_no_config_found_anywhere_fails(self, tmp_path):
      home = tmp_path / "home"
      home.mkdir()
      cwd = tmp_path / "cwd"
      cwd.mkdir()
      with pytest.raises(ConfigError):
         discover_config_path(
            home=str(home), cwd=str(cwd), etc_path=str(tmp_path / "no-etc.yaml"))


class TestLoadConfigFileAny:
   def test_empty_yaml_rejected(self, tmp_path):
      path = tmp_path / "config.yaml"
      path.write_text("")
      with pytest.raises(ConfigError, match="empty"):
         load_config_file_any(str(path), home=str(tmp_path))

   def test_dispatches_flat_layout(self, tmp_path):
      path = tmp_path / "config.yaml"
      path.write_text(yaml.safe_dump(_base_flat()))
      cfg = load_config_file_any(str(path), home=str(tmp_path))
      assert isinstance(cfg, Phase0Config)

   def test_dispatches_nested_layout(self, tmp_path):
      path = tmp_path / "config.yaml"
      path.write_text(yaml.safe_dump(_base_nested()))
      cfg = load_config_file_any(str(path), home=str(tmp_path))
      assert isinstance(cfg, NodeMonitorConfig)



# ==========================================================================
# Task 3: complete nested example config.example.phase1.yaml
# ==========================================================================

class TestExamplePhase1Config:
   def test_example_loads_under_strict_nested_schema(self):
      path = os.path.join(_REPO_ROOT, "config.example.phase1.yaml")
      with open(path, "r") as handle:
         raw = yaml.safe_load(handle)
      cfg = load_nested_config(
         raw, home="/home/example-user",
         database_url_env="postgresql://localhost/pbs_monitor_dev")
      assert isinstance(cfg, NodeMonitorConfig)
      assert cfg.output is not None
      assert cfg.collection is not None
      assert cfg.ssh is not None
      assert cfg.safety is not None
      assert cfg.database is not None
      assert cfg.retention is not None

   def test_example_retention_defaults_are_safe(self):
      path = os.path.join(_REPO_ROOT, "config.example.phase1.yaml")
      with open(path, "r") as handle:
         raw = yaml.safe_load(handle)
      cfg = load_nested_config(
         raw, home="/home/example-user",
         database_url_env="postgresql://localhost/pbs_monitor_dev")
      assert cfg.retention.enabled is False
      assert cfg.retention.dry_run is True

   def test_example_database_schema_and_overflow(self):
      path = os.path.join(_REPO_ROOT, "config.example.phase1.yaml")
      with open(path, "r") as handle:
         raw = yaml.safe_load(handle)
      cfg = load_nested_config(
         raw, home="/home/example-user",
         database_url_env="postgresql://localhost/pbs_monitor_dev")
      assert cfg.database.schema == "node_monitor"
      assert cfg.database.max_overflow == 0

   def test_example_housekeeping_utc(self):
      path = os.path.join(_REPO_ROOT, "config.example.phase1.yaml")
      with open(path, "r") as handle:
         raw = yaml.safe_load(handle)
      cfg = load_nested_config(
         raw, home="/home/example-user",
         database_url_env="postgresql://localhost/pbs_monitor_dev")
      assert cfg.retention.housekeeping_utc == "04:00"
