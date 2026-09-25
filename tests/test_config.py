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
# keep_raw_args: optional, defaults to True (ALCF is a monitored federal
# system with no in-facility privacy expectation -- full argv is captured
# verbatim by default; kanban task B1).
# --------------------------------------------------------------------------

class TestKeepRawArgs:
   def test_absent_defaults_to_true(self):
      cfg = _load()
      assert cfg.keep_raw_args is True

   def test_explicit_true_accepted(self):
      cfg = _load(keep_raw_args=True)
      assert cfg.keep_raw_args is True

   def test_explicit_false_accepted(self):
      cfg = _load(keep_raw_args=False)
      assert cfg.keep_raw_args is False

   def test_non_bool_rejected(self):
      with pytest.raises(ConfigError):
         _load(keep_raw_args="yes")

   def test_int_rejected(self):
      with pytest.raises(ConfigError):
         _load(keep_raw_args=1)


# --------------------------------------------------------------------------
# compress_census: optional, defaults to False, bool-validated exactly like
# keep_raw_args (kanban task B7). Back-compatible: an old config with no
# compress_census key at all still loads (default False, byte-for-byte
# unaffected).
# --------------------------------------------------------------------------

class TestCompressCensus:
   def test_absent_defaults_to_false(self):
      cfg = _load()
      assert cfg.compress_census is False

   def test_explicit_true_accepted(self):
      cfg = _load(compress_census=True)
      assert cfg.compress_census is True

   def test_explicit_false_accepted(self):
      cfg = _load(compress_census=False)
      assert cfg.compress_census is False

   def test_non_bool_rejected(self):
      with pytest.raises(ConfigError):
         _load(compress_census="yes")

   def test_int_rejected(self):
      with pytest.raises(ConfigError):
         _load(compress_census=1)

   def test_old_config_without_compress_census_still_loads(self):
      """Back-compat: a config dict with no compress_census key at all --
      exactly what every config predating kanban task B7 looks like --
      must still load successfully with the new field defaulted."""
      raw = _base_config()
      assert "compress_census" not in raw
      cfg = load_config(raw, home=HOME)
      assert cfg.compress_census is False


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
# ssh_target: optional per-node transport-only alias (kanban t_88d97d8e).
# Design: "SSH aliases are transport identifiers only, never provenance" --
# hostname stays the bookkeeping identity everywhere; ssh_target, when
# present, is ONLY the literal host argument handed to ssh(1).
# --------------------------------------------------------------------------

class TestSshTarget:
   def test_ssh_target_omitted_defaults_to_hostname(self):
      cfg = _load(nodes=[
         {"hostname": "polaris-login-04.example.org", "role": "local"},
         {"hostname": "polaris-login-01.hsn.cm.polaris.alcf.anl.gov",
          "role": "remote"},
      ])
      remote = cfg.remote_nodes[0]
      assert remote.ssh_target is None
      assert remote.effective_ssh_target == remote.hostname

   def test_explicit_ssh_target_on_remote_node_accepted(self):
      cfg = _load(nodes=[
         {"hostname": "polaris-login-04.example.org", "role": "local"},
         {"hostname": "polaris-login-01.hsn.cm.polaris.alcf.anl.gov",
          "role": "remote", "ssh_target": "polaris-login-01.head"},
      ])
      remote = cfg.remote_nodes[0]
      assert remote.hostname == "polaris-login-01.hsn.cm.polaris.alcf.anl.gov"
      assert remote.ssh_target == "polaris-login-01.head"
      assert remote.effective_ssh_target == "polaris-login-01.head"

   def test_ssh_target_on_local_node_rejected(self):
      with pytest.raises(ConfigError, match="ssh_target"):
         _load(nodes=[
            {"hostname": "polaris-login-04.example.org", "role": "local",
             "ssh_target": "polaris-login-04.head"},
         ])

   def test_ssh_target_empty_string_rejected(self):
      with pytest.raises(ConfigError):
         _load(nodes=[
            {"hostname": "polaris-login-04.example.org", "role": "local"},
            {"hostname": "polaris-login-01.example.org", "role": "remote",
             "ssh_target": ""},
         ])

   def test_ssh_target_non_string_rejected(self):
      with pytest.raises(ConfigError):
         _load(nodes=[
            {"hostname": "polaris-login-04.example.org", "role": "local"},
            {"hostname": "polaris-login-01.example.org", "role": "remote",
             "ssh_target": 12345},
         ])

   def test_unknown_key_still_rejected_alongside_ssh_target(self):
      """ssh_target being a newly-allowed key must not accidentally widen
      the allow-list to anything else -- a genuinely unknown key must
      still be rejected even on a node entry that also sets ssh_target.
      """
      with pytest.raises(ConfigError, match="unknown"):
         _load(nodes=[
            {"hostname": "polaris-login-04.example.org", "role": "local"},
            {"hostname": "polaris-login-01.example.org", "role": "remote",
             "ssh_target": "polaris-login-01.head", "ip": "1.2.3.4"},
         ])


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

   def test_example_config_polaris_remote_nodes_use_head_ssh_target(self):
      path = os.path.join(_REPO_ROOT, "config.example.yaml")
      with open(path, "r") as handle:
         raw = yaml.safe_load(handle)
      cfg = load_config(raw, home="/home/example-user")
      for node in cfg.remote_nodes:
         assert node.ssh_target is not None
         assert node.ssh_target.endswith(".head")
         assert node.effective_ssh_target == node.ssh_target
         # hostname itself stays the descriptive fqdn, never the alias.
         assert not node.hostname.endswith(".head")
