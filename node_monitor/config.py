"""node_monitor.config -- strict, immutable Phase 0 configuration.

Design: PHASE0_DAEMON_DESIGN.md "Configuration" section. Strict YAML
rejects unknown keys; the loader below is the enforcement point for that
promise, along with every other invariant called out there:

* exactly one local node, no duplicate hostnames
* an explicit, versioned probe-Python interpreter string (never a bare
  ``python3`` -- see remote_probe.py's MIN_PYTHON == (3, 9) contract)
* an output root that resolves under the daemon's own home directory and
  never under ``/tmp`` (a canary meant to run 24 hours must not write to
  a filesystem that clears on reboot or under tmp-cleanup policies)
* strictly positive timing/limit values

Configuration objects are immutable (frozen dataclasses) once loaded: a
running daemon must never observe a config value change out from under
it mid-run.
"""

import dataclasses
import math
import os
import re
import typing
import warnings
from urllib.parse import urlsplit

import yaml
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError


class ConfigError(Exception):
   """Raised for any malformed, incomplete, or unsafe configuration."""


# --------------------------------------------------------------------------
# Migrated flat Phase-0 keys, by nested section (design: "Configuration
# contract" -- "A file that supplies both representations of any migrated
# setting fails with a migration-specific ConfigError"). ``system``,
# ``nodes``, and ``probe_python`` stay shared top-level identity fields in
# both layouts and are therefore never "migrated" -- they never conflict.
# --------------------------------------------------------------------------

_MIGRATED_FLAT_KEYS = frozenset((
   "output_root", "compress_census",
   "counter_interval_sec", "census_interval_sec", "rollup_interval_sec",
   "usage_interval_sec", "duration_sec", "keep_raw_args",
   "counter_timeout_sec", "census_timeout_sec", "ssh_connect_timeout_sec",
   "max_parallel_polls",
   "min_free_disk_pct",
))


# --------------------------------------------------------------------------
# Defaults (PHASE0_DAEMON_DESIGN.md "Configuration")
# --------------------------------------------------------------------------

_DEFAULTS = {
   "counter_interval_sec": 10,
   "census_interval_sec": 60,
   "rollup_interval_sec": 60,
   "usage_interval_sec": 900,
   "duration_sec": 86400,
   "counter_timeout_sec": 4,
   "census_timeout_sec": 20,
   "ssh_connect_timeout_sec": 8,
   "max_parallel_polls": 8,
   "min_free_disk_pct": 10,
   "keep_raw_args": True,
   # Kanban task B7: opt-in gzip compression for the diagnostic_census
   # artifact only (by far the largest -- a real 24h canary produced a
   # 1.7 GB diagnostic_censuses.jsonl). Defaults to False so every
   # existing config/run/test is byte-for-byte unaffected; flip to True
   # only in a config that wants the on-disk census gzipped
   # (node_monitor/output/jsonl.py's Phase0Sink/scan_jsonl_artifact
   # handle the write/read sides transparently).
   "compress_census": False,
}

# Fields required to be present with no built-in default.
_REQUIRED_KEYS = ("system", "nodes", "output_root", "probe_python")

# Every top-level key this config accepts, required or defaulted.
_ALLOWED_KEYS = frozenset(_REQUIRED_KEYS) | frozenset(_DEFAULTS)

_POSITIVE_NUMBER_KEYS = (
   "counter_interval_sec", "census_interval_sec", "rollup_interval_sec",
   "usage_interval_sec", "duration_sec", "counter_timeout_sec",
   "census_timeout_sec", "ssh_connect_timeout_sec", "min_free_disk_pct",
)

_NODE_REQUIRED_KEYS = ("hostname", "role")
# ``ssh_target`` is OPTIONAL and remote-only (see NodeConfig/_validate_node):
# the exact host argument handed to ssh for a remote node, when it must
# differ from the node's own provenance-named ``hostname`` (e.g. Polaris's
# `.head` login-node SSH fan-out alias). Design: "SSH aliases are transport
# identifiers only, never provenance" -- kanban task t_88d97d8e.
_NODE_ALLOWED_KEYS = frozenset(_NODE_REQUIRED_KEYS) | frozenset(("ssh_target",))
_NODE_ROLES = frozenset(("local", "remote"))

# node_monitor/collector/remote_probe.py: MIN_PYTHON = (3, 9). The probe
# refuses to run below this itself, but we also refuse to *deploy* a
# below-floor interpreter string so a bad config fails at load time on
# the daemon host rather than as a runtime probe exit code 2 hours in.
_MIN_PROBE_PYTHON = (3, 9)

# Matches a trailing explicit "3.9", "3.11", ... version suffix on either
# a bare interpreter name ("python3.9") or a path ("/usr/bin/python3.11").
# Deliberately rejects an unversioned "python3"/"python" -- PLANNING.md
# decision 10 requires the interpreter to be named explicitly per system,
# never left to a $PATH default.
_PROBE_PYTHON_RE = re.compile(r"(?:^|/)python(\d+)\.(\d+)$")


@dataclasses.dataclass(frozen=True)
class NodeConfig:
   """One configured node: its hostname and whether it is local or remote.

   ``polaris-login-04`` is local (the daemon invokes the probe directly);
   every other configured node is remote (the daemon invokes the exact
   same probe over SSH). See PHASE0_DAEMON_DESIGN.md "Architecture".

   ``hostname`` is the stable, per-node bookkeeping/provenance identity
   used everywhere in this project EXCEPT as the literal ssh(1) host
   argument -- design: "SSH aliases are transport identifiers only,
   never provenance". ``ssh_target`` is the OPTIONAL transport-only
   override: the exact host argument handed to ssh for a REMOTE node,
   when it must differ from ``hostname`` (e.g. Polaris's `.head`
   login-node SSH fan-out alias, POLARIS-ONLY -- Aurora does not need
   it). Defaults to ``hostname`` when absent, which is exactly today's
   behavior. Always ``None`` for a local node -- the strict loader
   rejects ``ssh_target`` on a ``role: local`` entry outright (a local
   node never dials ssh at all, so a transport alias for it is
   meaningless and, per the unknown-key-rejection discipline this
   loader already applies everywhere else, fails closed rather than
   silently ignored).
   """

   hostname: str
   role: str
   ssh_target: typing.Optional[str] = None

   @property
   def is_local(self):
      return self.role == "local"

   @property
   def effective_ssh_target(self):
      """The exact host argument to hand to ssh for this (remote) node:
      the explicit ``ssh_target`` override when configured, else
      ``hostname`` -- full backward compat with configs that never set
      ``ssh_target`` at all.
      """
      return self.ssh_target if self.ssh_target is not None else self.hostname


@dataclasses.dataclass(frozen=True)
class Phase0Config:
   """Immutable, fully-validated Phase 0 daemon configuration."""

   system: str
   nodes: tuple
   output_root: str
   probe_python: str
   counter_interval_sec: int
   census_interval_sec: int
   rollup_interval_sec: int
   usage_interval_sec: int
   duration_sec: int
   counter_timeout_sec: float
   census_timeout_sec: float
   ssh_connect_timeout_sec: float
   max_parallel_polls: int
   min_free_disk_pct: float
   keep_raw_args: bool
   compress_census: bool

   @property
   def local_node(self):
      for node in self.nodes:
         if node.is_local:
            return node
      # Unreachable in practice: load_config guarantees exactly one local
      # node before a Phase0Config is ever constructed.
      raise ConfigError("no local node configured")

   @property
   def remote_nodes(self):
      return tuple(node for node in self.nodes if not node.is_local)


def _require_mapping(value, what):
   if not isinstance(value, dict):
      raise ConfigError("%s must be a mapping, got %s" % (what, type(value).__name__))
   return value


def _reject_unknown_keys(mapping, allowed, what):
   unknown = set(mapping) - allowed
   if unknown:
      raise ConfigError(
         "%s has unknown key(s): %s" % (what, ", ".join(sorted(unknown))))


def _require_keys(mapping, required, what):
   missing = [key for key in required if key not in mapping]
   if missing:
      raise ConfigError(
         "%s missing required key(s): %s" % (what, ", ".join(missing)))


def _validate_positive_number(value, key):
   if isinstance(value, bool) or not isinstance(value, (int, float)):
      raise ConfigError("%s must be a positive number, got %r" % (key, value))
   if value <= 0:
      raise ConfigError("%s must be positive, got %r" % (key, value))
   return value


def _validate_max_parallel_polls(value):
   if isinstance(value, bool) or not isinstance(value, int):
      raise ConfigError("max_parallel_polls must be a positive integer, got %r" % (value,))
   if value <= 0:
      raise ConfigError("max_parallel_polls must be positive, got %r" % (value,))
   return value


def _validate_keep_raw_args(value):
   if not isinstance(value, bool):
      raise ConfigError("keep_raw_args must be a bool, got %r" % (value,))
   return value


def _validate_compress_census(value):
   if not isinstance(value, bool):
      raise ConfigError("compress_census must be a bool, got %r" % (value,))
   return value


def _validate_system(value):
   if not isinstance(value, str) or not value:
      raise ConfigError("system must be a non-empty string, got %r" % (value,))
   return value


def _validate_probe_python(value):
   if not isinstance(value, str) or not value:
      raise ConfigError("probe_python must be a non-empty string, got %r" % (value,))
   match = _PROBE_PYTHON_RE.search(value)
   if not match:
      raise ConfigError(
         "probe_python must name an explicit versioned interpreter "
         "(e.g. 'python3.11' or '/usr/bin/python3.11'), got %r" % (value,))
   major, minor = int(match.group(1)), int(match.group(2))
   if (major, minor) < _MIN_PROBE_PYTHON:
      raise ConfigError(
         "probe_python %r is below the minimum required Python %d.%d"
         % (value, _MIN_PROBE_PYTHON[0], _MIN_PROBE_PYTHON[1]))
   return value


def _validate_output_root(value, home):
   if not isinstance(value, str) or not value:
      raise ConfigError("output_root must be a non-empty string, got %r" % (value,))
   if value == "~" or value.startswith("~/"):
      expanded = home.rstrip("/") + value[1:]
   else:
      expanded = value
   expanded = os.path.normpath(expanded)
   if expanded == "/tmp" or expanded.startswith("/tmp/"):
      raise ConfigError(
         "output_root must not be under /tmp (a 24-hour canary must "
         "survive tmp-cleanup policies), got %r" % (value,))
   home_norm = os.path.normpath(home)
   if expanded != home_norm and not expanded.startswith(home_norm + "/"):
      raise ConfigError(
         "output_root must resolve under the daemon home %r, got %r "
         "(resolved to %r)" % (home, value, expanded))
   return expanded


def _validate_node(raw, home_hint=None):
   raw = _require_mapping(raw, "node entry")
   _reject_unknown_keys(raw, _NODE_ALLOWED_KEYS, "node entry")
   _require_keys(raw, _NODE_REQUIRED_KEYS, "node entry")
   hostname = raw["hostname"]
   if not isinstance(hostname, str) or not hostname:
      raise ConfigError("node hostname must be a non-empty string, got %r" % (hostname,))
   role = raw["role"]
   if role not in _NODE_ROLES:
      raise ConfigError(
         "node role must be one of %s, got %r" % (sorted(_NODE_ROLES), role))
   ssh_target = raw.get("ssh_target")
   if "ssh_target" in raw:
      if role != "remote":
         # Fail-closed, same discipline as the unknown-key rejection
         # above: ssh_target is a transport-only concept that only
         # means anything for a node the daemon actually dials over
         # ssh. A local node's probe is invoked in-process by path
         # (collector.transport.run_local_probe) -- it never touches
         # ssh at all, so a configured ssh_target on a local node
         # entry is unreachable/meaningless config, not a harmless
         # extra. Rejecting it here (rather than silently ignoring it)
         # is what keeps a config typo -- e.g. ssh_target meant for a
         # different node entry -- from silently doing nothing.
         raise ConfigError(
            "node entry has unknown key(s) for role %r: ssh_target "
            "(ssh_target is remote-only)" % (role,))
      if not isinstance(ssh_target, str) or not ssh_target:
         raise ConfigError(
            "node ssh_target must be a non-empty string, got %r" % (ssh_target,))
   return NodeConfig(hostname=hostname, role=role, ssh_target=ssh_target)


def _validate_nodes(raw_nodes):
   if not isinstance(raw_nodes, list):
      raise ConfigError("nodes must be a list, got %s" % type(raw_nodes).__name__)
   if not raw_nodes:
      raise ConfigError("nodes must declare at least one node")

   nodes = tuple(_validate_node(entry) for entry in raw_nodes)

   seen = set()
   duplicates = set()
   for node in nodes:
      if node.hostname in seen:
         duplicates.add(node.hostname)
      seen.add(node.hostname)
   if duplicates:
      raise ConfigError(
         "duplicate node hostname(s): %s" % ", ".join(sorted(duplicates)))

   local_count = sum(1 for node in nodes if node.is_local)
   if local_count != 1:
      raise ConfigError(
         "exactly one local node is required, found %d" % local_count)

   return nodes


def load_config(raw, home):
   """Load and strictly validate a Phase 0 configuration mapping.

   ``raw`` is a plain dict (typically produced by ``yaml.safe_load``).
   ``home`` is injected explicitly rather than read from the environment
   so tests never depend on the invoking user's real $HOME, and so a
   caller can point output_root validation at a deployment-specific home
   without touching os.environ.
   """
   raw = _require_mapping(raw, "config")
   _reject_unknown_keys(raw, _ALLOWED_KEYS, "config")
   _require_keys(raw, _REQUIRED_KEYS, "config")

   system = _validate_system(raw["system"])
   nodes = _validate_nodes(raw["nodes"])
   probe_python = _validate_probe_python(raw["probe_python"])
   output_root = _validate_output_root(raw["output_root"], home)

   values = {}
   for key in _POSITIVE_NUMBER_KEYS:
      value = raw.get(key, _DEFAULTS[key])
      values[key] = _validate_positive_number(value, key)
   values["max_parallel_polls"] = _validate_max_parallel_polls(
      raw.get("max_parallel_polls", _DEFAULTS["max_parallel_polls"]))
   values["keep_raw_args"] = _validate_keep_raw_args(
      raw.get("keep_raw_args", _DEFAULTS["keep_raw_args"]))
   values["compress_census"] = _validate_compress_census(
      raw.get("compress_census", _DEFAULTS["compress_census"]))

   return Phase0Config(
      system=system,
      nodes=nodes,
      output_root=output_root,
      probe_python=probe_python,
      **values,
   )


def load_config_file(path, home=None):
   """Read and validate a Phase 0 YAML config file.

   ``home`` defaults to ``os.path.expanduser("~")`` -- overridable for
   tests and for deployments that run as a different effective user.
   """
   if home is None:
      home = os.path.expanduser("~")
   with open(path, "r") as handle:
      raw = yaml.safe_load(handle)
   if raw is None:
      raise ConfigError("config file %r is empty" % (path,))
   return load_config(raw, home=home)


# ==========================================================================
# Phase 1: strict nested configuration.
#
# Design: node_monitor_planning "Tiered Storage, PostgreSQL, and Retention
# Design" -- "Configuration contract". Plan: "Increment 1: Nested
# Configuration and PostgreSQL Connection Foundation".
#
# This nested loader is entirely independent of Phase0Config/load_config
# above: it owns its own top-level identity fields (``system``, ``nodes``,
# ``probe_python``) plus six named sections (``output``, ``collection``,
# ``ssh``, ``safety``, ``database``, ``retention``) directly -- it never
# wraps or forwards to a Phase0Config. ``load_any_config``/
# ``load_config_file_any`` dispatch between the two layouts and reject a
# config file that mixes both representations of any migrated setting.
# ==========================================================================


def _validate_bool(value, key):
   if not isinstance(value, bool):
      raise ConfigError("%s must be a bool, got %r" % (key, value))
   return value


def _validate_finite_positive_number(value, key):
   if isinstance(value, bool) or not isinstance(value, (int, float)):
      raise ConfigError("%s must be a positive number, got %r" % (key, value))
   if not math.isfinite(value):
      raise ConfigError("%s must be finite, got %r" % (key, value))
   if value <= 0:
      raise ConfigError("%s must be positive, got %r" % (key, value))
   return value


def _validate_positive_int(value, key):
   if isinstance(value, bool) or not isinstance(value, int):
      raise ConfigError("%s must be a positive integer, got %r" % (key, value))
   if value <= 0:
      raise ConfigError("%s must be positive, got %r" % (key, value))
   return value


def _validate_nonneg_int(value, key):
   if isinstance(value, bool) or not isinstance(value, int):
      raise ConfigError(
         "%s must be a nonnegative integer, got %r" % (key, value))
   if value < 0:
      raise ConfigError("%s must be nonnegative, got %r" % (key, value))
   return value


# --------------------------------------------------------------------------
# output
# --------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class OutputConfig:
   root: str
   compress_census: bool = False


_OUTPUT_REQUIRED_KEYS = ("root",)
_OUTPUT_DEFAULTS = {"compress_census": False}
_OUTPUT_ALLOWED_KEYS = frozenset(_OUTPUT_REQUIRED_KEYS) | frozenset(_OUTPUT_DEFAULTS)


def _validate_output_section(raw, home):
   raw = _require_mapping(raw, "output")
   _reject_unknown_keys(raw, _OUTPUT_ALLOWED_KEYS, "output")
   _require_keys(raw, _OUTPUT_REQUIRED_KEYS, "output")
   root = _validate_output_root(raw["root"], home)
   compress_census = _validate_compress_census(
      raw.get("compress_census", _OUTPUT_DEFAULTS["compress_census"]))
   return OutputConfig(root=root, compress_census=compress_census)


# --------------------------------------------------------------------------
# collection
# --------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class CollectionConfig:
   counter_interval_sec: float
   census_interval_sec: float
   counter_rollup_interval_sec: float
   usage_interval_sec: float
   duration_sec: float
   keep_raw_args: bool


_COLLECTION_DEFAULTS = {
   "counter_interval_sec": 10,
   "census_interval_sec": 60,
   "counter_rollup_interval_sec": 60,
   "usage_interval_sec": 900,
   "duration_sec": 86400,
   "keep_raw_args": True,
}
_COLLECTION_POSITIVE_NUMBER_KEYS = (
   "counter_interval_sec", "census_interval_sec",
   "counter_rollup_interval_sec", "usage_interval_sec", "duration_sec",
)
_COLLECTION_ALLOWED_KEYS = frozenset(_COLLECTION_DEFAULTS)


def _validate_collection_section(raw):
   raw = _require_mapping(raw, "collection")
   _reject_unknown_keys(raw, _COLLECTION_ALLOWED_KEYS, "collection")
   values = {}
   for key in _COLLECTION_POSITIVE_NUMBER_KEYS:
      values[key] = _validate_finite_positive_number(
         raw.get(key, _COLLECTION_DEFAULTS[key]), "collection.%s" % key)
   values["keep_raw_args"] = _validate_bool(
      raw.get("keep_raw_args", _COLLECTION_DEFAULTS["keep_raw_args"]),
      "collection.keep_raw_args")
   return CollectionConfig(**values)


# --------------------------------------------------------------------------
# ssh
# --------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class SshConfig:
   connect_timeout_sec: float
   counter_timeout_sec: float
   census_timeout_sec: float
   max_parallel_polls: int


_SSH_DEFAULTS = {
   "connect_timeout_sec": 8,
   "counter_timeout_sec": 4,
   "census_timeout_sec": 20,
   "max_parallel_polls": 8,
}
_SSH_POSITIVE_NUMBER_KEYS = (
   "connect_timeout_sec", "counter_timeout_sec", "census_timeout_sec",
)
_SSH_ALLOWED_KEYS = frozenset(_SSH_DEFAULTS)


def _validate_ssh_section(raw):
   raw = _require_mapping(raw, "ssh")
   _reject_unknown_keys(raw, _SSH_ALLOWED_KEYS, "ssh")
   values = {}
   for key in _SSH_POSITIVE_NUMBER_KEYS:
      values[key] = _validate_finite_positive_number(
         raw.get(key, _SSH_DEFAULTS[key]), "ssh.%s" % key)
   values["max_parallel_polls"] = _validate_positive_int(
      raw.get("max_parallel_polls", _SSH_DEFAULTS["max_parallel_polls"]),
      "ssh.max_parallel_polls")
   return SshConfig(**values)


# --------------------------------------------------------------------------
# safety
# --------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class SafetyConfig:
   min_free_disk_pct: float


_SAFETY_DEFAULTS = {"min_free_disk_pct": 10}
_SAFETY_ALLOWED_KEYS = frozenset(_SAFETY_DEFAULTS)


def _validate_safety_section(raw):
   raw = _require_mapping(raw, "safety")
   _reject_unknown_keys(raw, _SAFETY_ALLOWED_KEYS, "safety")
   min_free_disk_pct = _validate_finite_positive_number(
      raw.get("min_free_disk_pct", _SAFETY_DEFAULTS["min_free_disk_pct"]),
      "safety.min_free_disk_pct")
   return SafetyConfig(min_free_disk_pct=min_free_disk_pct)


# --------------------------------------------------------------------------
# database
#
# Design: "node-monitor owns only schema `node_monitor`; it must never use
# `public`". ``url`` is deliberately excluded from the dataclass repr
# (``dataclasses.field(repr=False)``) so an accidental ``repr()``/log of a
# DatabaseConfig -- or of the NodeMonitorConfig that contains it -- never
# prints a credential-bearing URL. Validated with SQLAlchemy's own
# ``make_url``, never a hand-rolled regex.
# --------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class DatabaseConfig:
   url: str = dataclasses.field(repr=False)
   schema: str
   pool_size: int
   max_overflow: int
   echo_sql: bool
   pool_pre_ping: bool
   pool_timeout_sec: float
   pool_recycle_sec: float
   connect_args: tuple


_DATABASE_REQUIRED_SCHEMA = "node_monitor"
_DATABASE_DEFAULTS = {
   "schema": _DATABASE_REQUIRED_SCHEMA,
   "pool_size": 1,
   "max_overflow": 0,
   "echo_sql": False,
   "pool_pre_ping": True,
   "pool_timeout_sec": 10,
   "pool_recycle_sec": 3600,
}
_DATABASE_ALLOWED_KEYS = frozenset(
   {"url", "connect_args"} | frozenset(_DATABASE_DEFAULTS))

_DATABASE_CONNECT_ARGS_INT_KEYS = (
   "connect_timeout", "keepalives", "keepalives_idle",
   "keepalives_interval", "keepalives_count",
)
_DATABASE_CONNECT_ARGS_STR_KEYS = ("options",)
_DATABASE_CONNECT_ARGS_ALLOWED_KEYS = (
   frozenset(_DATABASE_CONNECT_ARGS_INT_KEYS)
   | frozenset(_DATABASE_CONNECT_ARGS_STR_KEYS))


def _validate_database_url(value):
   if not isinstance(value, str) or not value:
      raise ConfigError(
         "database.url must be a non-empty PostgreSQL URL")
   try:
      # ``make_url`` does not reject every malformed authority eagerly
      # (notably unmatched IPv6 brackets), while malformed ports can escape
      # from it as ``ValueError`` rather than ``ArgumentError``. Parse the
      # authority independently and force port validation so every malformed
      # form is converted at this trust boundary.
      split = urlsplit(value)
      split.port
      url = make_url(value)
   except (ArgumentError, ValueError):
      raise ConfigError("database.url is not a valid URL")
   backend = url.get_backend_name()
   if backend != "postgresql":
      raise ConfigError(
         "database.url must be PostgreSQL-only, got backend %r" % (backend,))
   return value


def _validate_database_schema(value):
   if value != _DATABASE_REQUIRED_SCHEMA:
      raise ConfigError(
         "database.schema must be exactly %r, got %r"
         % (_DATABASE_REQUIRED_SCHEMA, value))
   return value


def _validate_database_max_overflow(value):
   if isinstance(value, bool) or not isinstance(value, int):
      raise ConfigError(
         "database.max_overflow must be an integer, got %r" % (value,))
   if value != 0:
      raise ConfigError(
         "database.max_overflow must be exactly 0 (single-writer "
         "node-monitor never opens extra pool connections), got %r"
         % (value,))
   return value


def _validate_database_connect_args(raw):
   raw = _require_mapping(raw, "database.connect_args")
   _reject_unknown_keys(
      raw, _DATABASE_CONNECT_ARGS_ALLOWED_KEYS, "database.connect_args")
   pairs = []
   for key, value in raw.items():
      if key in _DATABASE_CONNECT_ARGS_INT_KEYS:
         if isinstance(value, bool) or not isinstance(value, int):
            raise ConfigError(
               "database.connect_args.%s must be an integer, got %r"
               % (key, value))
      else:
         if not isinstance(value, str):
            raise ConfigError(
               "database.connect_args.%s must be a string, got %r"
               % (key, value))
      pairs.append((key, value))
   return tuple(sorted(pairs, key=lambda pair: pair[0]))


def _validate_database_section(raw, resolved_url):
   raw = _require_mapping(raw, "database")
   _reject_unknown_keys(raw, _DATABASE_ALLOWED_KEYS, "database")
   url = _validate_database_url(resolved_url)
   schema = _validate_database_schema(
      raw.get("schema", _DATABASE_DEFAULTS["schema"]))
   pool_size = _validate_positive_int(
      raw.get("pool_size", _DATABASE_DEFAULTS["pool_size"]),
      "database.pool_size")
   max_overflow = _validate_database_max_overflow(
      raw.get("max_overflow", _DATABASE_DEFAULTS["max_overflow"]))
   echo_sql = _validate_bool(
      raw.get("echo_sql", _DATABASE_DEFAULTS["echo_sql"]), "database.echo_sql")
   pool_pre_ping = _validate_bool(
      raw.get("pool_pre_ping", _DATABASE_DEFAULTS["pool_pre_ping"]),
      "database.pool_pre_ping")
   pool_timeout_sec = _validate_finite_positive_number(
      raw.get("pool_timeout_sec", _DATABASE_DEFAULTS["pool_timeout_sec"]),
      "database.pool_timeout_sec")
   pool_recycle_sec = _validate_finite_positive_number(
      raw.get("pool_recycle_sec", _DATABASE_DEFAULTS["pool_recycle_sec"]),
      "database.pool_recycle_sec")
   connect_args = _validate_database_connect_args(raw.get("connect_args", {}))
   return DatabaseConfig(
      url=url, schema=schema, pool_size=pool_size, max_overflow=max_overflow,
      echo_sql=echo_sql, pool_pre_ping=pool_pre_ping,
      pool_timeout_sec=pool_timeout_sec, pool_recycle_sec=pool_recycle_sec,
      connect_args=connect_args)


# --------------------------------------------------------------------------
# retention
# --------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class RetentionConfig:
   enabled: bool
   dry_run: bool
   diagnostic_census_days: int
   counter_diagnostics_days: int
   counter_minute_days: int
   usage_intervals_days: int
   usage_hourly_days: int
   counter_hourly_days: int
   daily_days: int
   poll_failures_days: int
   poll_failures_daily_days: int
   collection_log_days: int
   delete_batch_rows: int
   max_batches_per_run: int
   lag_alert_after_runs: int
   housekeeping_utc: str
   require_complete_rollup: bool


_RETENTION_DEFAULTS = {
   "enabled": False,
   "dry_run": True,
   "diagnostic_census_days": 7,
   "counter_diagnostics_days": 7,
   "counter_minute_days": 30,
   "usage_intervals_days": 180,
   "usage_hourly_days": 730,
   "counter_hourly_days": 730,
   "daily_days": 0,
   "poll_failures_days": 90,
   "poll_failures_daily_days": 730,
   "collection_log_days": 90,
   "delete_batch_rows": 50000,
   "max_batches_per_run": 20,
   "lag_alert_after_runs": 3,
   "housekeeping_utc": "04:00",
   "require_complete_rollup": True,
}
_RETENTION_BOOL_KEYS = ("enabled", "dry_run", "require_complete_rollup")
_RETENTION_DAY_KEYS = (
   "diagnostic_census_days", "counter_diagnostics_days",
   "counter_minute_days", "usage_intervals_days", "usage_hourly_days",
   "counter_hourly_days", "daily_days", "poll_failures_days",
   "poll_failures_daily_days", "collection_log_days",
)
_RETENTION_POSITIVE_INT_KEYS = (
   "delete_batch_rows", "max_batches_per_run", "lag_alert_after_runs",
)
_RETENTION_ALLOWED_KEYS = frozenset(_RETENTION_DEFAULTS)

# HH:MM, 00-23 hours, 00-59 minutes, always zero-padded (design: "housekeeping_utc").
_HOUSEKEEPING_UTC_RE = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)$")


def _validate_housekeeping_utc(value):
   if not isinstance(value, str) or not _HOUSEKEEPING_UTC_RE.match(value):
      raise ConfigError(
         "retention.housekeeping_utc must be a zero-padded 'HH:MM' UTC "
         "time (00:00-23:59), got %r" % (value,))
   return value


def _validate_retention_section(raw):
   raw = _require_mapping(raw, "retention")
   _reject_unknown_keys(raw, _RETENTION_ALLOWED_KEYS, "retention")
   values = {}
   for key in _RETENTION_BOOL_KEYS:
      values[key] = _validate_bool(
         raw.get(key, _RETENTION_DEFAULTS[key]), "retention.%s" % key)
   for key in _RETENTION_DAY_KEYS:
      values[key] = _validate_nonneg_int(
         raw.get(key, _RETENTION_DEFAULTS[key]), "retention.%s" % key)
   for key in _RETENTION_POSITIVE_INT_KEYS:
      values[key] = _validate_positive_int(
         raw.get(key, _RETENTION_DEFAULTS[key]), "retention.%s" % key)
   values["housekeeping_utc"] = _validate_housekeeping_utc(
      raw.get("housekeeping_utc", _RETENTION_DEFAULTS["housekeeping_utc"]))
   return RetentionConfig(**values)


# --------------------------------------------------------------------------
# NodeMonitorConfig: the nested top-level configuration object.
# --------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class NodeMonitorConfig:
   system: str
   nodes: tuple
   probe_python: str
   output: OutputConfig
   collection: CollectionConfig
   ssh: SshConfig
   safety: SafetyConfig
   database: DatabaseConfig
   retention: RetentionConfig


_NESTED_TOP_REQUIRED_KEYS = ("system", "nodes", "probe_python")
_NESTED_TOP_SECTION_KEYS = (
   "output", "collection", "ssh", "safety", "database", "retention")
_NESTED_TOP_ALLOWED_KEYS = (
   frozenset(_NESTED_TOP_REQUIRED_KEYS) | frozenset(_NESTED_TOP_SECTION_KEYS))


def load_nested_config(raw, home, database_url_env=None):
   """Load and strictly validate a nested Phase-1 configuration mapping.

   ``raw`` is never mutated: every section validator reads values out of
   ``raw``/its sub-mappings with ``.get(...)`` and builds brand-new
   dataclass instances, never writing a resolved default back into the
   caller's own mapping.

   ``database_url_env`` is the (already read by the caller -- this
   function never touches ``os.environ`` itself) value of
   ``NODE_MONITOR_DB_URL``, used only when the config does not supply an
   explicit ``database.url``. An explicit ``database.url`` always wins.
   """
   raw = _require_mapping(raw, "config")
   _reject_unknown_keys(raw, _NESTED_TOP_ALLOWED_KEYS, "config")
   _require_keys(raw, _NESTED_TOP_REQUIRED_KEYS, "config")

   system = _validate_system(raw["system"])
   nodes = _validate_nodes(raw["nodes"])
   probe_python = _validate_probe_python(raw["probe_python"])

   output = _validate_output_section(raw.get("output", {}), home)
   collection = _validate_collection_section(raw.get("collection", {}))
   ssh = _validate_ssh_section(raw.get("ssh", {}))
   safety = _validate_safety_section(raw.get("safety", {}))

   database_raw = raw.get("database", {})
   database_raw_mapping = _require_mapping(database_raw, "database")
   explicit_url = database_raw_mapping.get("url")
   if explicit_url is not None:
      resolved_url = explicit_url
   elif database_url_env is not None:
      resolved_url = database_url_env
   else:
      raise ConfigError(
         "database.url is required: set database.url in the config or "
         "inject NODE_MONITOR_DB_URL")
   database = _validate_database_section(database_raw_mapping, resolved_url)

   retention = _validate_retention_section(raw.get("retention", {}))

   return NodeMonitorConfig(
      system=system,
      nodes=nodes,
      probe_python=probe_python,
      output=output,
      collection=collection,
      ssh=ssh,
      safety=safety,
      database=database,
      retention=retention,
   )


def load_any_config(raw, home, database_url_env=None):
   """Dispatch to the legacy flat loader or the strict nested loader.

   Design: "The loader accepts exactly one representation: 1. legacy flat
   Phase-0 configuration, or 2. nested Phase-1 configuration. A file that
   supplies both representations of any migrated setting fails with a
   migration-specific ConfigError; it is never silently merged."
   """
   raw = _require_mapping(raw, "config")
   present_flat = set(raw) & _MIGRATED_FLAT_KEYS
   present_nested = set(raw) & frozenset(_NESTED_TOP_SECTION_KEYS)
   if present_flat and present_nested:
      raise ConfigError(
         "config has a mixed layout: legacy flat key(s) %s cannot be "
         "combined with nested section(s) %s -- migrate the flat "
         "setting(s) into their nested replacement section instead of "
         "supplying both"
         % (", ".join(sorted(present_flat)), ", ".join(sorted(present_nested))))
   if present_nested:
      return load_nested_config(raw, home=home, database_url_env=database_url_env)
   warnings.warn(
      "flat Phase-0 configuration is deprecated; migrate to the nested "
      "output/collection/ssh/safety/database/retention sections",
      DeprecationWarning, stacklevel=2)
   return load_config(raw, home=home)


def discover_config_path(explicit_path=None, home=None, cwd=None,
                          etc_path="/etc/node_monitor/config.yaml"):
   """Resolve the config file path to load, pbs-monitor-style.

   Priority order: an explicit CLI path (fails clearly if it does not
   exist), then ``~/.node_monitor.yaml``, then
   ``~/.config/node_monitor/config.yaml``, then ``etc_path``
   (``/etc/node_monitor/config.yaml`` by default), then
   ``node_monitor.yaml`` in the current directory.
   """
   if home is None:
      home = os.path.expanduser("~")
   if cwd is None:
      cwd = os.getcwd()
   if explicit_path is not None:
      if os.path.exists(explicit_path):
         return explicit_path
      raise ConfigError("config file not found: %r" % (explicit_path,))

   candidates = (
      os.path.join(home, ".node_monitor.yaml"),
      os.path.join(home, ".config", "node_monitor", "config.yaml"),
      etc_path,
      os.path.join(cwd, "node_monitor.yaml"),
   )
   for candidate in candidates:
      if os.path.exists(candidate):
         return candidate
   raise ConfigError(
      "no configuration file found (checked: %s)" % ", ".join(candidates))


def load_config_file_any(path, home=None, database_url_env=None):
   """Read a YAML config file and dispatch it through ``load_any_config``."""
   if home is None:
      home = os.path.expanduser("~")
   with open(path, "r") as handle:
      raw = yaml.safe_load(handle)
   if raw is None:
      raise ConfigError("config file %r is empty" % (path,))
   return load_any_config(raw, home=home, database_url_env=database_url_env)
