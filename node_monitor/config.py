"""Configuration loading and validation."""

import os
from dataclasses import dataclass, fields
from pathlib import Path

import yaml


class ConfigError(Exception):
   """Raised for a missing, malformed, or invalid configuration."""


@dataclass(frozen=True)
class NodeConfig:
   host: str
   label: str
   ssh_target: str


@dataclass(frozen=True)
class SshConfig:
   identity_file: str | None = None
   connect_timeout_sec: float = 10


@dataclass(frozen=True)
class CollectionConfig:
   counter_interval_sec: float = 10
   census_interval_sec: float = 60
   session_silence_timeout_sec: float = 30
   session_max_lifetime_sec: float = 21600
   reconnect_backoff_sec: tuple[float, float] = (5, 300)


@dataclass(frozen=True)
class ClassificationConfig:
   active_cpu_seconds_per_min: float = 0.1


@dataclass(frozen=True)
class Config:
   system: str
   nodes: tuple[NodeConfig, ...]
   probe_python: str
   working_dir: Path
   ssh: SshConfig
   collection: CollectionConfig
   classification: ClassificationConfig
   source_path: Path


TOP_LEVEL_KEYS = {
   "system",
   "nodes",
   "ssh",
   "probe_python",
   "working_dir",
   "collection",
   "classification",
   "database",
   "web",
   "redaction",
   "retention",
   "spool",
}
# Spec keys that M1+ consumes; accepted so one config file serves every milestone.
_COLLECTION_FUTURE = {"summary_interval_sec", "hourly_census"}
_CLASSIFICATION_FUTURE = {"min_user_uid", "notable_cpu_seconds_per_min", "notable_rss_mb"}
_NODE_KEYS = {"host", "label", "ssh_target"}


def find_config_path(explicit: str | None = None) -> Path:
   """Return the first existing config file in the documented search order."""
   if explicit:
      candidates = [Path(explicit).expanduser()]
   else:
      candidates = []
      env = os.environ.get("NODE_MONITOR_CONFIG")
      if env:
         candidates.append(Path(env).expanduser())
      candidates.append(Path("~/.node_monitor.yaml").expanduser())
      candidates.append(Path("~/.config/node_monitor/config.yaml").expanduser())
   for candidate in candidates:
      if candidate.is_file():
         return candidate
   tried = ", ".join(str(c) for c in candidates)
   raise ConfigError(f"no config file found; tried: {tried}")


def _section(raw: dict, name: str) -> dict:
   value = raw.get(name)
   if value is None:
      return {}
   if not isinstance(value, dict):
      raise ConfigError(f"'{name}' must be a mapping")
   return value


def _check_keys(where: str, data: dict, allowed) -> None:
   for key in data:
      if key not in allowed:
         raise ConfigError(f"unknown key '{key}' in {where}")


def _build(cls, where: str, data: dict, extra_allowed=frozenset()):
   names = {f.name for f in fields(cls)}
   _check_keys(where, data, names | set(extra_allowed))
   return cls(**{k: v for k, v in data.items() if k in names})


def _parse_nodes(raw_nodes) -> tuple[NodeConfig, ...]:
   if not isinstance(raw_nodes, list) or not raw_nodes:
      raise ConfigError("'nodes' must be a non-empty list")
   nodes = []
   seen = set()
   for entry in raw_nodes:
      if not isinstance(entry, dict):
         raise ConfigError("each entry in 'nodes' must be a mapping")
      _check_keys("nodes[]", entry, _NODE_KEYS)
      for required in ("host", "label"):
         if not entry.get(required):
            raise ConfigError(f"node entry is missing '{required}'")
      label = entry["label"]
      if label in seen:
         raise ConfigError(f"duplicate node label '{label}'")
      seen.add(label)
      nodes.append(NodeConfig(entry["host"], label, entry.get("ssh_target") or entry["host"]))
   return tuple(nodes)


def load_config(path: str | None = None) -> Config:
   source = find_config_path(path)
   try:
      raw = yaml.safe_load(source.read_text())
   except yaml.YAMLError as exc:
      raise ConfigError(f"{source}: invalid YAML: {exc}") from exc
   if not isinstance(raw, dict):
      raise ConfigError(f"{source}: top level must be a mapping")
   _check_keys("config", raw, TOP_LEVEL_KEYS)
   for required in ("system", "probe_python", "nodes"):
      if required not in raw:
         raise ConfigError(f"missing required key '{required}'")

   ssh_data = dict(_section(raw, "ssh"))
   if ssh_data.get("identity_file"):
      ssh_data["identity_file"] = str(Path(ssh_data["identity_file"]).expanduser())
   ssh = _build(SshConfig, "ssh", ssh_data)

   coll_data = dict(_section(raw, "collection"))
   if "reconnect_backoff_sec" in coll_data:
      pair = coll_data["reconnect_backoff_sec"]
      if not (isinstance(pair, (list, tuple)) and len(pair) == 2 and 0 < pair[0] < pair[1]):
         raise ConfigError("reconnect_backoff_sec must be [min, max] with 0 < min < max")
      coll_data["reconnect_backoff_sec"] = (pair[0], pair[1])
   collection = _build(CollectionConfig, "collection", coll_data, _COLLECTION_FUTURE)

   classification = _build(
      ClassificationConfig,
      "classification",
      _section(raw, "classification"),
      _CLASSIFICATION_FUTURE,
   )

   return Config(
      system=raw["system"],
      nodes=_parse_nodes(raw["nodes"]),
      probe_python=raw["probe_python"],
      working_dir=Path(raw.get("working_dir") or "~/node-monitor").expanduser(),
      ssh=ssh,
      collection=collection,
      classification=classification,
      source_path=source,
   )
