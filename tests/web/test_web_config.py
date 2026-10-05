"""Tests for the independent web-only configuration boundary.

Design: node-monitor web process loads a web-only configuration that:
- accepts exactly ``system`` and ``web`` top-level keys
- rejects every collector key (``nodes``, ``probe_python``, etc.)
- uses ``NODE_MONITOR_WEB_DB_URL`` env var only when no explicit URL is in YAML
- never reads ``NODE_MONITOR_DB_URL``
- rejects the removed legacy ``socket_path`` key
- reuses ``DatabaseConfig`` validation and defaults
"""

from pathlib import Path

import pytest
import yaml

from node_monitor.config import ConfigError, load_web_config


def _write(path, value):
   path.write_text(yaml.safe_dump(value))
   path.chmod(0o600)
   return str(path)


def _valid(url="postgresql+psycopg2://reader@localhost/node_monitor_dev"):
   return {
      "system": "polaris",
      "nodes": [
         {"hostname": "polaris-login-04.example.org", "role": "local",
          "display_name": "login-04"},
      ],
      "probe_python": "/usr/bin/python3.11",
      "database": {"url": "not-a-writer-url"},
      "web": {
         "database": {
            "url": url,
            "schema": "node_monitor",
            "pool_size": 1,
            "max_overflow": 0,
            "connect_args": {
               "connect_timeout": 3,
               "options": "-c statement_timeout=3000 -c lock_timeout=2000",
            },
         },
      },
   }


def test_web_config_projects_reader_and_typed_nodes(tmp_path):
   path = _write(tmp_path / "web.yaml", _valid())
   config = load_web_config(path, home="/home/operator")
   assert config.system == "polaris"
   assert tuple(config.__dataclass_fields__) == ("system", "nodes", "database")
   assert config.nodes[0].hostname == "polaris-login-04.example.org"
   assert config.nodes[0].display_name == "login-04"
   assert config.database.pool_size == 1
   assert config.database.max_overflow == 0


def test_web_config_accepts_daemon_fields_without_exposing_them(tmp_path):
   raw = _valid()
   raw["output"] = {"root": "~/runs"}
   path = _write(tmp_path / "web.yaml", raw)
   config = load_web_config(path, home="/home/operator")
   assert not hasattr(config, "output")
   assert not hasattr(config, "probe_python")


def test_explicit_web_url_wins_and_writer_env_is_never_read(tmp_path, monkeypatch):
   path = _write(tmp_path / "web.yaml", _valid("postgresql:///explicit"))
   monkeypatch.setenv("NODE_MONITOR_WEB_DB_URL", "postgresql:///web_env")
   monkeypatch.setenv("NODE_MONITOR_DB_URL", "postgresql:///writer_secret")
   config = load_web_config(path, home="/home/operator")
   assert config.database.url == "postgresql:///explicit"


def test_web_env_fills_only_an_omitted_url(tmp_path, monkeypatch):
   raw = _valid()
   del raw["web"]["database"]["url"]
   path = _write(tmp_path / "web.yaml", raw)
   monkeypatch.setenv("NODE_MONITOR_WEB_DB_URL", "postgresql:///reader")
   config = load_web_config(path, home="/home/operator")
   assert config.database.url == "postgresql:///reader"


def test_web_config_ignores_malformed_writer_database(tmp_path):
   raw = _valid()
   raw["database"] = {"url": "definitely-not-postgresql"}
   path = _write(tmp_path / "web.yaml", raw)
   config = load_web_config(path, home="/home/operator")
   assert config.database.url.endswith("node_monitor_dev")


def test_web_config_rejects_unknown_top_level_key(tmp_path):
   raw = _valid()
   raw["surprise"] = True
   path = _write(tmp_path / "web.yaml", raw)
   with pytest.raises(ConfigError, match="unknown key"):
      load_web_config(path, home="/home/operator")


def test_legacy_web_only_shape_has_fixed_migration_error(tmp_path):
   raw = _valid()
   raw.pop("nodes")
   raw.pop("probe_python")
   raw.pop("database")
   path = _write(tmp_path / "legacy.yaml", raw)
   with pytest.raises(ConfigError, match="migrate to the unified configuration"):
      load_web_config(path, home="/home/operator")


def test_literal_reader_url_requires_mode_0600(tmp_path):
   path = tmp_path / "web.yaml"
   path.write_text(yaml.safe_dump(_valid()))
   path.chmod(0o644)
   with pytest.raises(ConfigError, match="mode 0600"):
      load_web_config(str(path), home="/home/operator")


def test_literal_writer_url_also_requires_mode_0600(tmp_path):
   raw = _valid()
   raw["database"] = {"url": "postgresql://writer:WRITER_SENTINEL@host/db"}
   path = tmp_path / "web.yaml"
   path.write_text(yaml.safe_dump(raw))
   path.chmod(0o644)
   with pytest.raises(ConfigError) as caught:
      load_web_config(str(path), home="/home/operator")
   assert "mode 0600" in str(caught.value)
   assert "WRITER_SENTINEL" not in str(caught.value)


def test_environment_only_reader_allows_mode_0644(tmp_path, monkeypatch):
   raw = _valid()
   del raw["web"]["database"]["url"]
   raw["database"] = {"url_env": "NODE_MONITOR_DB_URL"}
   path = tmp_path / "web.yaml"
   path.write_text(yaml.safe_dump(raw))
   path.chmod(0o644)
   monkeypatch.setenv("NODE_MONITOR_WEB_DB_URL", "postgresql:///reader")
   assert load_web_config(str(path), home="/home/operator").database.url == \
      "postgresql:///reader"


def test_web_config_rejects_symlink_without_path_leak(tmp_path):
   target = tmp_path / "PATH_SENTINEL-target.yaml"
   _write(target, _valid())
   link = tmp_path / "PATH_SENTINEL-link.yaml"
   link.symlink_to(target)
   with pytest.raises(ConfigError) as caught:
      load_web_config(str(link), home="/home/operator")
   assert "PATH_SENTINEL" not in str(caught.value)


def test_web_config_rejects_unknown_web_subkey(tmp_path):
   raw = _valid()
   raw["web"]["collector_mode"] = True
   path = _write(tmp_path / "web.yaml", raw)
   with pytest.raises(ConfigError, match="unknown key"):
      load_web_config(path, home="/home/operator")


def test_web_config_rejects_unknown_database_subkey(tmp_path):
   raw = _valid()
   raw["web"]["database"]["extra_setting"] = "bad"
   path = _write(tmp_path / "web.yaml", raw)
   with pytest.raises(ConfigError, match="unknown key"):
      load_web_config(path, home="/home/operator")


def test_web_config_rejects_unknown_connect_args_key(tmp_path):
   raw = _valid()
   raw["web"]["database"]["connect_args"]["bad_arg"] = 1
   path = _write(tmp_path / "web.yaml", raw)
   with pytest.raises(ConfigError, match="unknown key"):
      load_web_config(path, home="/home/operator")


def test_web_config_rejects_non_postgresql_url(tmp_path):
   path = _write(tmp_path / "web.yaml", _valid("mysql://reader@localhost/db"))
   with pytest.raises(ConfigError, match="PostgreSQL"):
      load_web_config(path, home="/home/operator")


def test_web_config_rejects_wrong_schema(tmp_path):
   raw = _valid()
   raw["web"]["database"]["schema"] = "public"
   path = _write(tmp_path / "web.yaml", raw)
   with pytest.raises(ConfigError, match="schema"):
      load_web_config(path, home="/home/operator")


def test_web_config_rejects_pool_size_other_than_1(tmp_path):
   raw = _valid()
   raw["web"]["database"]["pool_size"] = 5
   path = _write(tmp_path / "web.yaml", raw)
   with pytest.raises(ConfigError, match="pool_size"):
      load_web_config(path, home="/home/operator")


def test_web_config_rejects_overflow_other_than_0(tmp_path):
   raw = _valid()
   raw["web"]["database"]["max_overflow"] = 2
   path = _write(tmp_path / "web.yaml", raw)
   with pytest.raises(ConfigError, match="max_overflow"):
      load_web_config(path, home="/home/operator")


def test_web_config_rejects_legacy_socket_path(tmp_path):
   raw = _valid()
   raw["web"]["socket_path"] = "~/.node-monitor/run/web.sock"
   path = _write(tmp_path / "web.yaml", raw)
   with pytest.raises(ConfigError, match="unknown key"):
      load_web_config(path, home="/home/operator")


def test_writer_env_var_is_never_consulted(tmp_path, monkeypatch):
   """NODE_MONITOR_DB_URL must never influence web config even when
   NODE_MONITOR_WEB_DB_URL is absent and no URL is in YAML."""
   raw = _valid()
   del raw["web"]["database"]["url"]
   path = _write(tmp_path / "web.yaml", raw)
   monkeypatch.setenv("NODE_MONITOR_DB_URL", "postgresql:///writer_secret")
   monkeypatch.delenv("NODE_MONITOR_WEB_DB_URL", raising=False)
   with pytest.raises(ConfigError):
      load_web_config(path, home="/home/operator")


def test_database_defaults_are_populated(tmp_path):
   """pool_timeout_sec, pool_recycle_sec, pool_pre_ping use DatabaseConfig defaults."""
   path = _write(tmp_path / "web.yaml", _valid())
   config = load_web_config(path, home="/home/operator")
   # These come from _DATABASE_DEFAULTS; intentionally absent from web YAML shape
   assert config.database.pool_pre_ping is True
   assert config.database.pool_timeout_sec == 10
   assert config.database.pool_recycle_sec == 3600


def test_missing_url_and_no_env_raises(tmp_path, monkeypatch):
   raw = _valid()
   del raw["web"]["database"]["url"]
   path = _write(tmp_path / "web.yaml", raw)
   monkeypatch.delenv("NODE_MONITOR_WEB_DB_URL", raising=False)
   monkeypatch.delenv("NODE_MONITOR_DB_URL", raising=False)
   with pytest.raises(ConfigError, match="url"):
      load_web_config(path, home="/home/operator")


def test_injected_database_url_env_wins_over_env_var(tmp_path, monkeypatch):
   """load_web_config accepts database_url_env kwarg; injected value wins over os.environ."""
   raw = _valid()
   del raw["web"]["database"]["url"]
   path = _write(tmp_path / "web.yaml", raw)
   monkeypatch.setenv("NODE_MONITOR_WEB_DB_URL", "postgresql:///from_env")
   config = load_web_config(
      path, home="/home/operator",
      database_url_env="postgresql:///injected",
   )
   assert config.database.url == "postgresql:///injected"
