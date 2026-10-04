"""Tests for the independent web-only configuration boundary.

Design: node-monitor web process loads a web-only configuration that:
- accepts exactly ``system`` and ``web`` top-level keys
- rejects every collector key (``nodes``, ``probe_python``, etc.)
- uses ``NODE_MONITOR_WEB_DB_URL`` env var only when no explicit URL is in YAML
- never reads ``NODE_MONITOR_DB_URL``
- confines socket_path to ~/.node-monitor/run/ after expansion
- reuses ``DatabaseConfig`` validation and defaults
"""

from pathlib import Path

import pytest
import yaml

from node_monitor.config import ConfigError, load_web_config


def _write(path, value):
   path.write_text(yaml.safe_dump(value))
   return str(path)


def _valid(url="postgresql+psycopg2://reader@localhost/node_monitor_dev"):
   return {
      "system": "polaris",
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


def test_web_config_accepts_only_system_and_web(tmp_path):
   path = _write(tmp_path / "web.yaml", _valid())
   config = load_web_config(path, home="/home/operator")
   assert config.system == "polaris"
   assert config.database.pool_size == 1
   assert config.database.max_overflow == 0


def test_web_config_rejects_collector_fields(tmp_path):
   raw = _valid()
   raw["nodes"] = []
   path = _write(tmp_path / "web.yaml", raw)
   with pytest.raises(ConfigError, match="unknown key"):
      load_web_config(path, home="/home/operator")


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


def test_web_config_rejects_probe_python(tmp_path):
   raw = _valid()
   raw["probe_python"] = "/usr/bin/python3.11"
   path = _write(tmp_path / "web.yaml", raw)
   with pytest.raises(ConfigError, match="unknown key"):
      load_web_config(path, home="/home/operator")


def test_web_config_rejects_output(tmp_path):
   raw = _valid()
   raw["output"] = {"root": "/tmp/out"}
   path = _write(tmp_path / "web.yaml", raw)
   with pytest.raises(ConfigError, match="unknown key"):
      load_web_config(path, home="/home/operator")


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
