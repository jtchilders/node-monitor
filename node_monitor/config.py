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
import os
import re

import yaml


class ConfigError(Exception):
   """Raised for any malformed, incomplete, or unsafe configuration."""


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
_NODE_ALLOWED_KEYS = frozenset(_NODE_REQUIRED_KEYS)
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
   """

   hostname: str
   role: str

   @property
   def is_local(self):
      return self.role == "local"


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
   return NodeConfig(hostname=hostname, role=role)


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
