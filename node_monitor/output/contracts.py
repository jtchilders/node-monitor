"""node_monitor.output.contracts -- production-shaped record schemas.

Design: PHASE0_DAEMON_DESIGN.md "Output contract" -- the five
production-shaped record types plus the Phase-0-only diagnostic census.
Every validator here enforces the same strict-schema discipline as
``node_monitor.config``: unknown keys are rejected, required keys must
be present (though many may be ``None``), and field types/business rules
from the design doc are checked.

The one rule that is not "just a schema check" is on
``diagnostic_censuses``: PLANNING.md decision 7 and the daemon's own
"raw argv never persists" invariant are enforced here in code, not left
to reviewer vigilance, because this file is the last checkpoint before a
record is written to disk.
"""


class ContractError(Exception):
   """Raised when a record does not match its declared record-type schema."""


# --------------------------------------------------------------------------
# Small schema helpers
#
# A field spec is (name, python_types_or_None_meaning_nullable). Every
# validator below is intentionally hand-written rather than driven by a
# generic schema engine: five record types is few enough that explicit
# code stays more readable than a mini-DSL, and each type has at least
# one bespoke business rule (coverage bounds, activity normalization,
# enumerated failure/breaker values, the argv ban) that a generic engine
# would need escape hatches for anyway.
# --------------------------------------------------------------------------

def _check_type(record, key, expected_types, what):
   value = record[key]
   if value is None:
      return
   if not isinstance(value, expected_types):
      raise ContractError(
         "%s field %r must be %s, got %s" % (
            what, key, expected_types, type(value).__name__))


def _validate_schema(record, required_keys, nullable_keys, what):
   """Shared unknown/missing-key enforcement for every record type.

   ``required_keys``: must be present (value may still be None if the
   key is also in ``nullable_keys``).
   ``nullable_keys``: subset of required_keys allowed to hold None.

   Also enforces the global "raw argv/environment never persists"
   invariant (design: "Raw argv, environment, file descriptors, and
   process I/O never persist") recursively across the WHOLE record, not
   just the fields a given validator happens to type-check explicitly --
   every record type accepts at least one free-form nested container
   (``detail``, ``audit``, ``end_of_window``, ``rates``, ``cpu_deltas``)
   that a caller could stuff a forbidden key into at any depth, and
   Phase0Sink writes whatever validate_record() returns verbatim.
   """
   if not isinstance(record, dict):
      raise ContractError("%s record must be a mapping, got %s" % (
         what, type(record).__name__))
   allowed = set(required_keys)
   unknown = set(record) - allowed
   if unknown:
      raise ContractError(
         "%s record has unknown key(s): %s" % (what, ", ".join(sorted(unknown))))
   missing = [key for key in required_keys if key not in record]
   if missing:
      raise ContractError(
         "%s record missing required key(s): %s" % (what, ", ".join(missing)))
   for key in required_keys:
      if record[key] is None and key not in nullable_keys:
         raise ContractError(
            "%s record field %r may not be null" % (what, key))
   _reject_forbidden_argv_keys(record, "%s record" % what)


# --------------------------------------------------------------------------
# node_hardware
# --------------------------------------------------------------------------

_HARDWARE_REQUIRED = (
   "system", "source_hostname", "first_seen_utc", "probe_version",
   "boot_id", "btime", "cpu_model", "cpu_logical", "sockets",
   "cores_per_socket", "cpu_max_freq_khz", "numa_nodes", "mem_total_kb",
   "swap_total_kb", "hugepage_size_kb", "kernel_release", "os_pretty_name",
   "net_fs_mounts", "net_ifaces", "gpus",
)

# Physical facts are frequently unreadable on a given node (permission,
# missing sysfs entry) and therefore nullable; identity/bookkeeping
# fields are not.
_HARDWARE_NULLABLE = (
   "boot_id", "btime", "cpu_model", "cpu_logical", "sockets",
   "cores_per_socket", "cpu_max_freq_khz", "numa_nodes", "mem_total_kb",
   "swap_total_kb", "hugepage_size_kb", "kernel_release", "os_pretty_name",
   "net_fs_mounts", "net_ifaces", "gpus",
)

_HARDWARE_TYPES = {
   "system": (str,),
   "source_hostname": (str,),
   "first_seen_utc": (str,),
   "probe_version": (int,),
   "boot_id": (str,),
   "btime": (int,),
   "cpu_model": (str,),
   "cpu_logical": (int,),
   "sockets": (int,),
   "cores_per_socket": (int,),
   "cpu_max_freq_khz": (int,),
   "numa_nodes": (int,),
   "mem_total_kb": (int,),
   "swap_total_kb": (int,),
   "hugepage_size_kb": (int,),
   "kernel_release": (str,),
   "os_pretty_name": (str,),
   "net_fs_mounts": (int,),
   "net_ifaces": (dict,),
   "gpus": (list,),
}


def validate_node_hardware(record):
   _validate_schema(record, _HARDWARE_REQUIRED, _HARDWARE_NULLABLE, "node_hardware")
   for key, types in _HARDWARE_TYPES.items():
      _check_type(record, key, types, "node_hardware")
   return record


# --------------------------------------------------------------------------
# node_counter_samples
# --------------------------------------------------------------------------

_COUNTER_REQUIRED = (
   "system", "source_hostname", "collector_hostname", "probe_version",
   "daemon_version", "window_start_utc", "window_end_utc", "sample_count",
   "expected_count", "coverage", "end_of_window", "rates", "audit",
)
_COUNTER_NULLABLE = ()
_COUNTER_TYPES = {
   "system": (str,),
   "source_hostname": (str,),
   "collector_hostname": (str,),
   "probe_version": (int,),
   "daemon_version": (str,),
   "window_start_utc": (str,),
   "window_end_utc": (str,),
   "sample_count": (int,),
   "expected_count": (int,),
   "coverage": (int, float),
   "end_of_window": (dict,),
   "rates": (dict,),
   "audit": (dict,),
}


def validate_node_counter_samples(record):
   _validate_schema(record, _COUNTER_REQUIRED, _COUNTER_NULLABLE, "node_counter_samples")
   for key, types in _COUNTER_TYPES.items():
      _check_type(record, key, types, "node_counter_samples")
   if not (0.0 <= record["coverage"] <= 1.0):
      raise ContractError(
         "node_counter_samples coverage must be within [0, 1], got %r"
         % (record["coverage"],))
   if record["sample_count"] < 0:
      raise ContractError("node_counter_samples sample_count must be >= 0")
   if record["expected_count"] < 0:
      raise ContractError("node_counter_samples expected_count must be >= 0")
   return record


# --------------------------------------------------------------------------
# node_usage_intervals
# --------------------------------------------------------------------------

_USAGE_REQUIRED = (
   "system", "source_hostname", "interval_start_utc", "interval_end_utc",
   "category", "activity", "username", "process_count", "cpu_seconds",
   "rss_kb", "d_state_fraction", "interactivity_fraction", "sample_count",
   "expected_count", "unmeasured_count",
)
# username is the one field the design explicitly allows to be null
# (an unresolvable uid, per remote_probe.py's _resolve_uid_names). activity
# must never be null -- design: "Nullable activity is normalized to
# 'unknown', avoiding nullable-PK semantics" -- so it is deliberately
# absent from this nullable set even though its sibling fields are not.
_USAGE_NULLABLE = ("username",)
_USAGE_TYPES = {
   "system": (str,),
   "source_hostname": (str,),
   "interval_start_utc": (str,),
   "interval_end_utc": (str,),
   "category": (str,),
   "activity": (str,),
   "username": (str,),
   "process_count": (dict,),
   "cpu_seconds": (int, float),
   "rss_kb": (dict,),
   "d_state_fraction": (int, float),
   "interactivity_fraction": (int, float),
   "sample_count": (int,),
   "expected_count": (int,),
   "unmeasured_count": (int,),
}


def validate_node_usage_intervals(record):
   _validate_schema(record, _USAGE_REQUIRED, _USAGE_NULLABLE, "node_usage_intervals")
   for key, types in _USAGE_TYPES.items():
      _check_type(record, key, types, "node_usage_intervals")
   if record["activity"] is None:
      # _validate_schema already forbids None here (activity is required
      # and not nullable); this explicit check exists so the failure
      # message names the actual design rule instead of a generic
      # "may not be null".
      raise ContractError(
         "node_usage_intervals activity must never be null -- "
         "normalize None to 'unknown' before writing")
   if record["unmeasured_count"] < 0:
      raise ContractError("node_usage_intervals unmeasured_count must be >= 0")
   if record["sample_count"] < 0:
      raise ContractError("node_usage_intervals sample_count must be >= 0")
   if record["expected_count"] < 0:
      raise ContractError("node_usage_intervals expected_count must be >= 0")
   return record


# --------------------------------------------------------------------------
# node_poll_failures
# --------------------------------------------------------------------------

_POLL_FAILURES_REQUIRED = (
   "system", "source_hostname", "loop", "timestamp_utc", "failure_type",
   "detail", "consecutive_failures", "breaker_state",
)
_POLL_FAILURES_NULLABLE = ()
_POLL_FAILURES_LOOPS = frozenset(("counter", "census"))
# Design: "Failure types distinguish scheduler miss, timeout, SSH
# authentication/transport, probe exit, malformed JSON, probe-version
# mismatch, hostname mismatch, reboot, reset, and invariant violation."
_POLL_FAILURE_TYPES = frozenset((
   "scheduler_miss", "timeout", "ssh_auth", "ssh_transport", "probe_exit",
   "malformed_json", "probe_version_mismatch", "hostname_mismatch",
   "reboot", "reset", "invariant_violation",
))
_BREAKER_STATES = frozenset(("closed", "open", "half_open"))
_POLL_FAILURES_TYPES = {
   "system": (str,),
   "source_hostname": (str,),
   "loop": (str,),
   "timestamp_utc": (str,),
   "failure_type": (str,),
   "detail": (str,),
   "consecutive_failures": (int,),
   "breaker_state": (str,),
}


def validate_node_poll_failures(record):
   _validate_schema(
      record, _POLL_FAILURES_REQUIRED, _POLL_FAILURES_NULLABLE, "node_poll_failures")
   for key, types in _POLL_FAILURES_TYPES.items():
      _check_type(record, key, types, "node_poll_failures")
   if record["loop"] not in _POLL_FAILURES_LOOPS:
      raise ContractError(
         "node_poll_failures loop must be one of %s, got %r"
         % (sorted(_POLL_FAILURES_LOOPS), record["loop"]))
   if record["failure_type"] not in _POLL_FAILURE_TYPES:
      raise ContractError(
         "node_poll_failures failure_type must be one of %s, got %r"
         % (sorted(_POLL_FAILURE_TYPES), record["failure_type"]))
   if record["breaker_state"] not in _BREAKER_STATES:
      raise ContractError(
         "node_poll_failures breaker_state must be one of %s, got %r"
         % (sorted(_BREAKER_STATES), record["breaker_state"]))
   if record["consecutive_failures"] < 0:
      raise ContractError("node_poll_failures consecutive_failures must be >= 0")
   return record


# --------------------------------------------------------------------------
# node_collection_log
# --------------------------------------------------------------------------

_COLLECTION_LOG_REQUIRED = ("system", "timestamp_utc", "event", "detail")
_COLLECTION_LOG_NULLABLE = ()
_COLLECTION_LOG_TYPES = {
   "system": (str,),
   "timestamp_utc": (str,),
   "event": (str,),
   "detail": (dict,),
}


def validate_node_collection_log(record):
   _validate_schema(
      record, _COLLECTION_LOG_REQUIRED, _COLLECTION_LOG_NULLABLE,
      "node_collection_log")
   for key, types in _COLLECTION_LOG_TYPES.items():
      _check_type(record, key, types, "node_collection_log")
   return record


# --------------------------------------------------------------------------
# diagnostic_census (Phase-0-only, never a production table)
# --------------------------------------------------------------------------

_CENSUS_REQUIRED = (
   "system", "source_hostname", "timestamp_utc", "probe_version",
   "processes", "cpu_deltas",
)
_CENSUS_NULLABLE = ()
_CENSUS_TYPES = {
   "system": (str,),
   "source_hostname": (str,),
   "timestamp_utc": (str,),
   "probe_version": (int,),
   "processes": (list,),
   "cpu_deltas": (dict,),
}

# Design: "Raw argv, environment, file descriptors, and process I/O never
# persist." / PLANNING.md decision 7: "--drop-raw-args ... What leaves
# the node is the classification, not the command line." These keys must
# never appear anywhere in a diagnostic_census record, at any nesting
# level checked below -- top-level and per-process.
_FORBIDDEN_ARGV_KEYS = frozenset((
   "argv", "cmdline", "cmdline_raw", "raw_argv", "raw_cmdline", "environ",
))


def _reject_forbidden_argv_keys(value, where):
   """Recursively scan ``value`` for any forbidden raw-argv/environment
   key, at any nesting depth, through dicts and lists alike.

   Review round 1 finding: the original version only checked the
   diagnostic_census record root and its immediate ``processes[]`` rows.
   Every record type carries at least one free-form nested container
   (``detail`` on node_collection_log/node_poll_failures, ``audit`` on
   node_counter_samples, ``cpu_deltas`` on diagnostic_census, etc.) that
   a caller could stuff a forbidden key into at arbitrary depth, and the
   sink writes whatever validate_record() returns verbatim -- so this is
   the last checkpoint before such a key reaches disk. A plain string
   that happens to contain "argv" as a substring (e.g. a human-written
   failure detail) is correctly left alone: only actual mapping keys are
   checked, never string contents.
   """
   if isinstance(value, dict):
      present = _FORBIDDEN_ARGV_KEYS & set(value)
      if present:
         raise ContractError(
            "%s carries forbidden raw-argv key(s): %s"
            % (where, ", ".join(sorted(present))))
      for key, nested in value.items():
         _reject_forbidden_argv_keys(nested, "%s.%s" % (where, key))
   elif isinstance(value, list):
      for index, item in enumerate(value):
         _reject_forbidden_argv_keys(item, "%s[%d]" % (where, index))


def validate_diagnostic_census(record):
   _validate_schema(record, _CENSUS_REQUIRED, _CENSUS_NULLABLE, "diagnostic_census")
   for key, types in _CENSUS_TYPES.items():
      _check_type(record, key, types, "diagnostic_census")
   return record


# --------------------------------------------------------------------------
# Dispatch
# --------------------------------------------------------------------------

_VALIDATORS = {
   "node_hardware": validate_node_hardware,
   "node_counter_samples": validate_node_counter_samples,
   "node_usage_intervals": validate_node_usage_intervals,
   "node_poll_failures": validate_node_poll_failures,
   "node_collection_log": validate_node_collection_log,
   "diagnostic_census": validate_diagnostic_census,
}


def validate_record(record_type, record):
   """Validate ``record`` against the schema named by ``record_type``.

   Raises ``ContractError`` for an unrecognized ``record_type`` itself,
   not just for a malformed record -- a caller passing a typo'd type
   string should fail loudly rather than silently skip validation.
   """
   validator = _VALIDATORS.get(record_type)
   if validator is None:
      raise ContractError(
         "unknown record_type %r; must be one of %s"
         % (record_type, sorted(_VALIDATORS)))
   return validator(record)
