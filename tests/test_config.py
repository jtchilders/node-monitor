from pathlib import Path

import pytest
import yaml

from node_monitor.config import ConfigError, find_config_path, load_config

MINIMAL = {
   "system": "polaris",
   "probe_python": "/usr/bin/python3.11",
   "nodes": [{"host": "n1.example", "label": "n1"}],
}


def write_yaml(tmp_path, data, name="config.yaml"):
   path = tmp_path / name
   path.parent.mkdir(parents=True, exist_ok=True)
   path.write_text(yaml.safe_dump(data))
   return path


def test_minimal_config_gets_spec_defaults(tmp_path):
   cfg = load_config(str(write_yaml(tmp_path, MINIMAL)))
   assert cfg.nodes[0].ssh_target == "n1.example"
   assert cfg.collection.counter_interval_sec == 10
   assert cfg.collection.census_interval_sec == 60
   assert cfg.collection.session_silence_timeout_sec == 30
   assert cfg.collection.session_max_lifetime_sec == 21600
   assert cfg.collection.reconnect_backoff_sec == (5, 300)
   assert cfg.classification.active_cpu_seconds_per_min == 0.1
   assert cfg.ssh.connect_timeout_sec == 10
   assert cfg.ssh.identity_file is None
   assert cfg.working_dir == Path("~/node-monitor").expanduser()


def test_spec_sections_not_used_yet_are_accepted(tmp_path):
   extra = {
      "database": {"url": "postgresql://x@h:1/db"},
      "web": {"port": 9998},
      "redaction": {"extra_patterns": []},
      "retention": {"process_samples_days": 30},
      "spool": {"max_bytes": 1},
   }
   cfg = load_config(str(write_yaml(tmp_path, {**MINIMAL, **extra})))
   assert cfg.system == "polaris"


def test_example_config_loads(tmp_path):
   cfg = load_config(str(Path(__file__).parent.parent / "config.example.yaml"))
   assert [n.label for n in cfg.nodes] == ["login-01", "login-02", "login-04"]
   assert cfg.nodes[0].ssh_target == "polaris-login-01.head"
   assert cfg.ssh.identity_file == str(Path("~/.ssh/id_ed25519_polaris").expanduser())


def test_tilde_expanded_in_working_dir_and_identity_file(tmp_path):
   data = {**MINIMAL, "working_dir": "~/wd", "ssh": {"identity_file": "~/.ssh/key"}}
   cfg = load_config(str(write_yaml(tmp_path, data)))
   assert cfg.working_dir == Path("~/wd").expanduser()
   assert cfg.ssh.identity_file == str(Path("~/.ssh/key").expanduser())


def test_collection_overrides_applied(tmp_path):
   data = {**MINIMAL, "collection": {"counter_interval_sec": 5, "reconnect_backoff_sec": [1, 60]}}
   cfg = load_config(str(write_yaml(tmp_path, data)))
   assert cfg.collection.counter_interval_sec == 5
   assert cfg.collection.reconnect_backoff_sec == (1, 60)


def test_unknown_top_level_key_is_rejected(tmp_path):
   with pytest.raises(ConfigError, match="unknown key.*colection"):
      load_config(str(write_yaml(tmp_path, {**MINIMAL, "colection": {}})))


@pytest.mark.parametrize("section", ["ssh", "collection", "classification"])
def test_unknown_key_in_section_is_rejected(tmp_path, section):
   with pytest.raises(ConfigError, match="unknown key.*bogus"):
      load_config(str(write_yaml(tmp_path, {**MINIMAL, section: {"bogus": 1}})))


def test_unknown_key_in_node_is_rejected(tmp_path):
   data = {**MINIMAL, "nodes": [{"host": "h", "label": "l", "bogus": 1}]}
   with pytest.raises(ConfigError, match="unknown key.*bogus"):
      load_config(str(write_yaml(tmp_path, data)))


def test_duplicate_node_labels_rejected(tmp_path):
   nodes = [{"host": "a", "label": "x"}, {"host": "b", "label": "x"}]
   with pytest.raises(ConfigError, match="duplicate.*x"):
      load_config(str(write_yaml(tmp_path, {**MINIMAL, "nodes": nodes})))


def test_empty_nodes_rejected(tmp_path):
   with pytest.raises(ConfigError, match="nodes"):
      load_config(str(write_yaml(tmp_path, {**MINIMAL, "nodes": []})))


@pytest.mark.parametrize("missing", ["system", "probe_python", "nodes"])
def test_missing_required_key_rejected(tmp_path, missing):
   data = {k: v for k, v in MINIMAL.items() if k != missing}
   with pytest.raises(ConfigError, match=missing):
      load_config(str(write_yaml(tmp_path, data)))


def test_backoff_must_be_increasing_pair(tmp_path):
   data = {**MINIMAL, "collection": {"reconnect_backoff_sec": [300, 5]}}
   with pytest.raises(ConfigError, match="reconnect_backoff_sec"):
      load_config(str(write_yaml(tmp_path, data)))


def test_non_mapping_file_rejected(tmp_path):
   path = tmp_path / "c.yaml"
   path.write_text("- just\n- a list\n")
   with pytest.raises(ConfigError):
      load_config(str(path))


def test_search_order(tmp_path, monkeypatch):
   monkeypatch.setenv("HOME", str(tmp_path))
   monkeypatch.delenv("NODE_MONITOR_CONFIG", raising=False)
   xdg = write_yaml(tmp_path, MINIMAL, ".config/node_monitor/config.yaml")
   assert find_config_path() == xdg
   home = write_yaml(tmp_path, MINIMAL, ".node_monitor.yaml")
   assert find_config_path() == home
   env = write_yaml(tmp_path, MINIMAL, "env.yaml")
   monkeypatch.setenv("NODE_MONITOR_CONFIG", str(env))
   assert find_config_path() == env
   assert find_config_path(str(xdg)) == xdg


def test_missing_config_lists_paths_tried(tmp_path, monkeypatch):
   monkeypatch.setenv("HOME", str(tmp_path))
   monkeypatch.delenv("NODE_MONITOR_CONFIG", raising=False)
   with pytest.raises(ConfigError) as exc:
      find_config_path()
   assert str(tmp_path / ".node_monitor.yaml") in str(exc.value)
   assert str(tmp_path / ".config/node_monitor/config.yaml") in str(exc.value)
