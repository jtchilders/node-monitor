"""Configuration loading and validation."""

import math
import os
import re
from dataclasses import dataclass, fields, replace
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

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
   summary_interval_sec: float = 300
   session_silence_timeout_sec: float = 30
   session_max_lifetime_sec: float = 21600
   reconnect_backoff_sec: tuple[float, float] = (5, 300)
   probe_rss_limit_mb: float = 128


@dataclass(frozen=True)
class ClassificationConfig:
   active_cpu_seconds_per_min: float = 0.1
   min_user_uid: int = 1000
   notable_cpu_seconds_per_min: float = 0.1
   notable_rss_mb: float = 1024


@dataclass(frozen=True)
class RedactionConfig:
   extra_patterns: tuple[str, ...] = ()


@dataclass(frozen=True)
class DatabaseConfig:
   url: str | None = None
   connect_timeout_sec: float = 10


@dataclass(frozen=True)
class WebConfig:
   database_url: str | None = None
   host: str = "127.0.0.1"
   port: int = 9998


@dataclass(frozen=True)
class RetentionConfig:
   node_metrics_days: int = 730
   usage_summary_days: int = 730
   collection_events_days: int = 180
   process_samples_days: int = 30
   command_lines_days: int = 730


@dataclass(frozen=True)
class SpoolConfig:
   max_bytes: int = 1073741824


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
   redaction: RedactionConfig = RedactionConfig()
   database: DatabaseConfig = DatabaseConfig()
   web: WebConfig = WebConfig()
   retention: RetentionConfig = RetentionConfig()
   spool: SpoolConfig = SpoolConfig()


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
_COLLECTION_FUTURE = {"hourly_census"}
_CLASSIFICATION_FUTURE: set[str] = set()
# Retention for these arrives with the process tables in M3.
_RETENTION_FUTURE: set[str] = set()
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


def _positive(where: str, key: str, value) -> None:
   # bool is an int subclass; `true` is never a sensible interval.
   if isinstance(value, bool) or not isinstance(value, (int, float)) or not value > 0:
      raise ConfigError(f"{where}.{key} must be a positive number, got {value!r}")


def _positive_finite(where: str, key: str, value) -> None:
   # libpq takes whole seconds, and math.ceil(inf) raises.
   _positive(where, key, value)
   if not math.isfinite(value):
      raise ConfigError(f"{where}.{key} must be a finite number, got {value!r}")


def _positive_int(where: str, key: str, value) -> None:
   if (
      isinstance(value, bool)
      or not isinstance(value, int)
      or (isinstance(value, float) and not math.isfinite(value))
      or not value > 0
   ):
      raise ConfigError(f"{where}.{key} must be a positive integer, got {value!r}")


def _check_url(label: str, url) -> None:
   if url is None:
      return
   if not isinstance(url, str) or not url.startswith(("postgresql://", "postgres://")):
      raise ConfigError(f"{label} must be a postgresql:// or postgres:// URL, got {url!r}")


def _has_password(url) -> bool:
   if not isinstance(url, str):
      return False
   try:
      parts = urlsplit(url)
      return bool(parts.password) or "password" in parse_qs(parts.query)
   except ValueError:
      return False


def _near_integer(x: float) -> bool:
   return abs(x - round(x)) < 1e-9


def _validate_collection(c: CollectionConfig) -> None:
   for key in (
      "counter_interval_sec",
      "census_interval_sec",
      "summary_interval_sec",
      "session_silence_timeout_sec",
      "session_max_lifetime_sec",
      "probe_rss_limit_mb",
   ):
      _positive("collection", key, getattr(c, key))
   if not c.session_silence_timeout_sec > c.counter_interval_sec:
      raise ConfigError(
         "collection.session_silence_timeout_sec must exceed counter_interval_sec, "
         "or a healthy session would be declared silent"
      )
   ratio = c.summary_interval_sec / c.census_interval_sec
   if (
      round(ratio) < 1
      or not _near_integer(ratio)
      or not _near_integer(3600 / c.summary_interval_sec)
   ):
      raise ConfigError(
         "collection.summary_interval_sec must be a multiple of census_interval_sec "
         "and divide 3600 so intervals align to the clock"
      )


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
   _validate_collection(collection)

   classification = _build(
      ClassificationConfig,
      "classification",
      _section(raw, "classification"),
      _CLASSIFICATION_FUTURE,
   )

   _positive("ssh", "connect_timeout_sec", ssh.connect_timeout_sec)
   _positive(
      "classification", "active_cpu_seconds_per_min", classification.active_cpu_seconds_per_min
   )
   uid = classification.min_user_uid
   if isinstance(uid, bool) or not isinstance(uid, int) or uid < 0:
      raise ConfigError(f"classification.min_user_uid must be a non-negative integer, got {uid!r}")

   redaction_data = dict(_section(raw, "redaction"))
   patterns = redaction_data.get("extra_patterns") or []
   if not isinstance(patterns, (list, tuple)):
      raise ConfigError("redaction.extra_patterns must be a list of regular expressions")
   for pattern in patterns:
      if not isinstance(pattern, str):
         raise ConfigError(f"redaction.extra_patterns entries must be strings, got {pattern!r}")
      try:
         re.compile(pattern)
      except re.error as exc:
         raise ConfigError(
            f"redaction.extra_patterns: {pattern!r} is not a valid regex: {exc}"
         ) from exc
   redaction_data["extra_patterns"] = tuple(patterns)
   redaction = _build(RedactionConfig, "redaction", redaction_data)

   database_data = _section(raw, "database")
   database = _build(DatabaseConfig, "database", database_data)
   web = _build(WebConfig, "web", _section(raw, "web"))
   retention = _build(RetentionConfig, "retention", _section(raw, "retention"), _RETENTION_FUTURE)
   spool = _build(SpoolConfig, "spool", _section(raw, "spool"))

   _check_url("database.url", database.url)
   _check_url("web.database_url", web.database_url)
   _positive_finite("database", "connect_timeout_sec", database.connect_timeout_sec)
   _positive_int("spool", "max_bytes", spool.max_bytes)
   for key in (
      "node_metrics_days",
      "usage_summary_days",
      "collection_events_days",
      "process_samples_days",
      "command_lines_days",
   ):
      _positive_int("retention", key, getattr(retention, key))
   if not isinstance(web.host, str) or not web.host:
      raise ConfigError(f"web.host must be a non-empty string, got {web.host!r}")
   if isinstance(web.port, bool) or not isinstance(web.port, int) or not 1 <= web.port <= 65535:
      raise ConfigError(f"web.port must be an integer in 1..65535, got {web.port!r}")

   # A password in the file makes the file a credential (spec §3); check what the
   # file holds, before any environment override replaces it.
   if _has_password(database.url) or _has_password(web.database_url):
      mode = source.stat().st_mode & 0o777
      if mode & 0o077:
         raise ConfigError(
            f"{source} contains a database password and must have mode 0600 "
            f"(found {mode:04o}); run: chmod 600 {source}"
         )

   db_override = os.environ.get("NODE_MONITOR_DB_URL")
   if db_override:
      _check_url("NODE_MONITOR_DB_URL", db_override)
      database = replace(database, url=db_override)
   web_override = os.environ.get("NODE_MONITOR_WEB_DB_URL")
   if web_override:
      _check_url("NODE_MONITOR_WEB_DB_URL", web_override)
      web = replace(web, database_url=web_override)

   return Config(
      system=raw["system"],
      nodes=_parse_nodes(raw["nodes"]),
      probe_python=raw["probe_python"],
      working_dir=Path(raw.get("working_dir") or "~/node-monitor").expanduser(),
      ssh=ssh,
      collection=collection,
      classification=classification,
      source_path=source,
      redaction=redaction,
      database=database,
      web=web,
      retention=retention,
      spool=spool,
   )
