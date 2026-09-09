"""Tests for node_monitor.config -- strict Phase 0 configuration loading.

Covers every bullet in the Phase 0 implementation plan Task 1: numeric
defaults, explicit local-vs-remote node declaration (exactly one local,
no duplicates), positive timing validation, an explicit versioned probe
Python interpreter string, output rooted under the (injectable) home
directory and never under /tmp, unknown-key rejection at both the
top level and the per-node level, and required-field enforcement.
"""

import os

import pytest
import yaml

from node_monitor.config import ConfigError, NodeConfig, Phase0Config, load_config


HOME = "/home/canary"
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _base_config(**overrides):
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


def _load(**overrides):
   return load_config(_base_config(**overrides), home=HOME)


# --------------------------------------------------------------------------
# Defaults
# --------------------------------------------------------------------------

class TestDefaults:
   def test_default_intervals_and_duration(self):
      cfg = _load()
      assert cfg.counter_interval_sec == 10
      assert cfg.census_interval_sec == 60
      assert cfg.rollup_interval_sec == 60
      assert cfg.usage_interval_sec == 900
      assert cfg.duration_sec == 86400

   def test_default_timeouts_and_limits(self):
      cfg = _load()
      assert cfg.counter_timeout_sec == 4
      assert cfg.census_timeout_sec == 20
      assert cfg.ssh_connect_timeout_sec == 8
      assert cfg.max_parallel_polls == 8
      assert cfg.min_free_disk_pct == 10

   def test_explicit_value_overrides_default(self):
      cfg = _load(census_interval_sec=30)
      assert cfg.census_interval_sec == 30

   def test_config_is_immutable(self):
      cfg = _load()
      with pytest.raises(Exception):
         cfg.system = "other"
      with pytest.raises(Exception):
         cfg.nodes[0].hostname = "other"


# --------------------------------------------------------------------------
# Local-vs-remote node declaration
# --------------------------------------------------------------------------

class TestNodes:
   def test_exactly_one_local_node_is_valid(self):
      cfg = _load()
      assert isinstance(cfg.nodes, tuple)
      assert all(isinstance(node, NodeConfig) for node in cfg.nodes)
      assert cfg.local_node.hostname == "polaris-login-04.example.org"
      assert [n.hostname for n in cfg.remote_nodes] == [
         "polaris-login-01.example.org"]

   def test_zero_local_nodes_rejected(self):
      with pytest.raises(ConfigError, match="local"):
         _load(nodes=[
            {"hostname": "a.example.org", "role": "remote"},
            {"hostname": "b.example.org", "role": "remote"},
         ])

   def test_two_local_nodes_rejected(self):
      with pytest.raises(ConfigError, match="local"):
         _load(nodes=[
            {"hostname": "a.example.org", "role": "local"},
            {"hostname": "b.example.org", "role": "local"},
         ])

   def test_duplicate_hostname_rejected(self):
      with pytest.raises(ConfigError, match="duplicate"):
         _load(nodes=[
            {"hostname": "a.example.org", "role": "local"},
            {"hostname": "a.example.org", "role": "remote"},
         ])

   def test_invalid_role_rejected(self):
      with pytest.raises(ConfigError):
         _load(nodes=[{"hostname": "a.example.org", "role": "primary"}])

   def test_unknown_node_key_rejected(self):
      with pytest.raises(ConfigError, match="unknown"):
         _load(nodes=[
            {"hostname": "a.example.org", "role": "local", "ip": "1.2.3.4"},
         ])

   def test_node_missing_required_key_rejected(self):
      with pytest.raises(ConfigError):
         _load(nodes=[{"hostname": "a.example.org"}])

   def test_empty_nodes_list_rejected(self):
      with pytest.raises(ConfigError):
         _load(nodes=[])

   def test_nodes_must_be_a_list(self):
      with pytest.raises(ConfigError):
         _load(nodes={"hostname": "a.example.org", "role": "local"})


# --------------------------------------------------------------------------
# Explicit Python >= 3.9 interpreter string
# --------------------------------------------------------------------------

class TestProbePython:
   def test_versioned_path_accepted(self):
      cfg = _load(probe_python="/usr/bin/python3.11")
      assert cfg.probe_python == "/usr/bin/python3.11"

   def test_bare_versioned_name_accepted(self):
      cfg = _load(probe_python="python3.9")
      assert cfg.probe_python == "python3.9"

   def test_unversioned_interpreter_rejected(self):
      with pytest.raises(ConfigError):
         _load(probe_python="/usr/bin/python3")

   def test_below_minimum_version_rejected(self):
      with pytest.raises(ConfigError):
         _load(probe_python="/usr/bin/python3.6")

   def test_empty_probe_python_rejected(self):
      with pytest.raises(ConfigError):
         _load(probe_python="")


# --------------------------------------------------------------------------
# Output root: under home, never under /tmp
# --------------------------------------------------------------------------

class TestOutputRoot:
   def test_tilde_expands_under_injected_home(self):
      cfg = _load(output_root="~/phase0-runs")
      assert cfg.output_root == HOME + "/phase0-runs"

   def test_absolute_path_under_home_accepted(self):
      cfg = _load(output_root=HOME + "/runs")
      assert cfg.output_root == HOME + "/runs"

   def test_tmp_path_rejected(self):
      with pytest.raises(ConfigError, match="tmp"):
         _load(output_root="/tmp/phase0-runs")

   def test_path_outside_home_rejected(self):
      with pytest.raises(ConfigError):
         _load(output_root="/var/phase0-runs")

   def test_empty_output_root_rejected(self):
      with pytest.raises(ConfigError):
         _load(output_root="")


# --------------------------------------------------------------------------
# Positive timing validation
# --------------------------------------------------------------------------

@pytest.mark.parametrize("key", [
   "counter_interval_sec", "census_interval_sec", "rollup_interval_sec",
   "usage_interval_sec", "duration_sec", "counter_timeout_sec",
   "census_timeout_sec", "ssh_connect_timeout_sec", "min_free_disk_pct",
])
class TestPositiveTiming:
   def test_zero_rejected(self, key):
      with pytest.raises(ConfigError):
         _load(**{key: 0})

   def test_negative_rejected(self, key):
      with pytest.raises(ConfigError):
         _load(**{key: -1})

   def test_non_numeric_rejected(self, key):
      with pytest.raises(ConfigError):
         _load(**{key: "soon"})


class TestMaxParallelPolls:
   def test_zero_rejected(self):
      with pytest.raises(ConfigError):
         _load(max_parallel_polls=0)

   def test_non_integer_rejected(self):
      with pytest.raises(ConfigError):
         _load(max_parallel_polls=8.5)

   def test_positive_integer_accepted(self):
      cfg = _load(max_parallel_polls=4)
      assert cfg.max_parallel_polls == 4


# --------------------------------------------------------------------------
# Unknown-key rejection / required fields
# --------------------------------------------------------------------------

class TestSchema:
   def test_unknown_top_level_key_rejected(self):
      with pytest.raises(ConfigError, match="unknown"):
         _load(database={"host": "127.0.0.1"})

   def test_config_must_be_a_mapping(self):
      with pytest.raises(ConfigError):
         load_config(["not", "a", "mapping"], home=HOME)

   @pytest.mark.parametrize("key", ["system", "nodes", "output_root", "probe_python"])
   def test_missing_required_key_rejected(self, key):
      raw = _base_config()
      del raw[key]
      with pytest.raises(ConfigError, match="missing"):
         load_config(raw, home=HOME)

   def test_empty_system_rejected(self):
      with pytest.raises(ConfigError):
         _load(system="")

   def test_non_string_system_rejected(self):
      with pytest.raises(ConfigError):
         _load(system=123)


# --------------------------------------------------------------------------
# config.example.yaml must actually satisfy the strict schema it ships as
# a corrected public example for -- catches drift between the loader and
# the example the moment either one changes.
# --------------------------------------------------------------------------

class TestExampleConfig:
   def test_example_config_loads_under_strict_schema(self):
      path = os.path.join(_REPO_ROOT, "config.example.yaml")
      with open(path, "r") as handle:
         raw = yaml.safe_load(handle)
      cfg = load_config(raw, home="/home/example-user")
      assert cfg.system == "polaris"
      assert cfg.local_node is not None
      assert len(cfg.remote_nodes) >= 1
