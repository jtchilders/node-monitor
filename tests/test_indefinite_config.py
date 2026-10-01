"""Regression tests for nullable nested runtime duration."""

import pytest

from node_monitor.config import ConfigError, load_nested_config


def _raw(duration):
   return {
      "system": "polaris",
      "nodes": [{"hostname": "login-04", "role": "local"}],
      "probe_python": "/usr/bin/python3.11",
      "output": {"root": "~/runs"},
      "collection": {"duration_sec": duration},
      "ssh": {},
      "safety": {},
      "database": {"url": "postgresql:///node_monitor"},
      "retention": {},
   }


def test_nested_config_accepts_explicit_null_duration_for_indefinite_run(tmp_path):
   config = load_nested_config(_raw(None), home=str(tmp_path))
   assert config.collection.duration_sec is None


@pytest.mark.parametrize("duration", [0, -1, True, "forever"])
def test_nested_config_rejects_invalid_indefinite_duration_values(tmp_path, duration):
   with pytest.raises(ConfigError):
      load_nested_config(_raw(duration), home=str(tmp_path))
