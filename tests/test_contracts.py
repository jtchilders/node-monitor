"""Tests for node_monitor.output.contracts -- record schema validators.

Each record type gets: acceptance of a valid record, rejection of an
unknown extra key, rejection of a missing required key, rejection of a
wrong-typed field, and any type-specific business rule called out in the
design doc (activity normalization, enumerated failure/breaker values,
and -- above all -- the diagnostic-census guarantee that raw argv never
survives into an artifact).
"""

import pytest

from node_monitor.output.contracts import (
   ContractError,
   validate_diagnostic_census,
   validate_node_collection_log,
   validate_node_counter_samples,
   validate_node_hardware,
   validate_node_poll_failures,
   validate_node_usage_intervals,
   validate_record,
)


# --------------------------------------------------------------------------
# node_hardware
# --------------------------------------------------------------------------

class TestNodeHardware:
   def _record(self, **overrides):
      base = {
         "system": "polaris",
         "source_hostname": "polaris-login-04.example.org",
         "first_seen_utc": "2026-09-09T00:00:00Z",
         "probe_version": 4,
         "boot_id": "boot-a",
         "btime": 1700000000,
         "cpu_model": "AMD EPYC 7713",
         "cpu_logical": 256,
         "sockets": 2,
         "cores_per_socket": 64,
         "cpu_max_freq_khz": 2000000,
         "numa_nodes": 2,
         "mem_total_kb": 527954112,
         "swap_total_kb": 0,
         "hugepage_size_kb": 2048,
         "kernel_release": "6.4.0",
         "os_pretty_name": "SLES 15 SP7",
         "net_fs_mounts": 7,
         "net_ifaces": {"bond0": 1000},
         "gpus": [],
      }
      base.update(overrides)
      return base

   def test_valid_record_accepted(self):
      record = self._record()
      assert validate_node_hardware(record) == record

   def test_nullable_fields_may_be_none(self):
      record = self._record(boot_id=None, gpus=None, net_ifaces=None)
      validate_node_hardware(record)

   def test_unknown_key_rejected(self):
      with pytest.raises(ContractError, match="unknown"):
         validate_node_hardware(self._record(extra_field="nope"))

   def test_missing_required_key_rejected(self):
      record = self._record()
      del record["probe_version"]
      with pytest.raises(ContractError, match="missing"):
         validate_node_hardware(record)

   def test_missing_nullable_key_still_rejected(self):
      """A declared field must be present -- None is fine, absent is not."""
      record = self._record()
      del record["gpus"]
      with pytest.raises(ContractError, match="missing"):
         validate_node_hardware(record)

   def test_wrong_type_rejected(self):
      with pytest.raises(ContractError):
         validate_node_hardware(self._record(cpu_logical="256"))

   def test_dispatch_via_validate_record(self):
      record = self._record()
      assert validate_record("node_hardware", record) == record


# --------------------------------------------------------------------------
# node_counter_samples
# --------------------------------------------------------------------------

class TestNodeCounterSamples:
   def _record(self, **overrides):
      base = {
         "system": "polaris",
         "source_hostname": "polaris-login-04.example.org",
         "collector_hostname": "polaris-login-04.example.org",
         "probe_version": 4,
         "daemon_version": "0.1.0",
         "window_start_utc": "2026-09-09T00:00:00Z",
         "window_end_utc": "2026-09-09T00:01:00Z",
         "sample_count": 6,
         "expected_count": 6,
         "coverage": 1.0,
         "end_of_window": {"mem_available_kb": 1000},
         "rates": {"cpu_busy_pct": {"p50": 1.0, "p95": 2.0, "max": 3.0}},
         "audit": {"raw_cumulative": {}},
      }
      base.update(overrides)
      return base

   def test_valid_record_accepted(self):
      record = self._record()
      assert validate_node_counter_samples(record) == record

   def test_unknown_key_rejected(self):
      with pytest.raises(ContractError, match="unknown"):
         validate_node_counter_samples(self._record(bogus=1))

   def test_missing_required_key_rejected(self):
      record = self._record()
      del record["coverage"]
      with pytest.raises(ContractError, match="missing"):
         validate_node_counter_samples(record)

   def test_coverage_out_of_range_rejected(self):
      with pytest.raises(ContractError):
         validate_node_counter_samples(self._record(coverage=1.5))

   def test_negative_sample_count_rejected(self):
      with pytest.raises(ContractError):
         validate_node_counter_samples(self._record(sample_count=-1))

   def test_end_of_window_must_be_object(self):
      with pytest.raises(ContractError):
         validate_node_counter_samples(self._record(end_of_window=[1, 2]))


# --------------------------------------------------------------------------
# node_usage_intervals
# --------------------------------------------------------------------------

class TestNodeUsageIntervals:
   def _record(self, **overrides):
      base = {
         "system": "polaris",
         "source_hostname": "polaris-login-04.example.org",
         "interval_start_utc": "2026-09-09T00:00:00Z",
         "interval_end_utc": "2026-09-09T00:15:00Z",
         "category": "ai-coding-agent",
         "activity": "claude-code",
         "username": "jchilders",
         "process_count": {"p50": 1, "p95": 2, "max": 3},
         "cpu_seconds": 12.5,
         "rss_kb": {"p50": 1000, "p95": 2000, "max": 3000},
         "d_state_fraction": 0.0,
         "interactivity_fraction": 0.5,
         "sample_count": 15,
         "expected_count": 15,
         "unmeasured_count": 0,
      }
      base.update(overrides)
      return base

   def test_valid_record_accepted(self):
      record = self._record()
      assert validate_node_usage_intervals(record) == record

   def test_null_activity_rejected(self):
      """activity=None must be normalized to 'unknown' upstream; the
      validator enforces the grain never carries a nullable PK component."""
      with pytest.raises(ContractError, match="activity"):
         validate_node_usage_intervals(self._record(activity=None))

   def test_unknown_activity_string_accepted(self):
      validate_node_usage_intervals(self._record(activity="unknown"))

   def test_username_may_be_none(self):
      validate_node_usage_intervals(self._record(username=None))

   def test_unknown_key_rejected(self):
      with pytest.raises(ContractError, match="unknown"):
         validate_node_usage_intervals(self._record(bogus=1))

   def test_missing_required_key_rejected(self):
      record = self._record()
      del record["category"]
      with pytest.raises(ContractError, match="missing"):
         validate_node_usage_intervals(record)

   def test_negative_unmeasured_count_rejected(self):
      with pytest.raises(ContractError):
         validate_node_usage_intervals(self._record(unmeasured_count=-1))


# --------------------------------------------------------------------------
# node_poll_failures
# --------------------------------------------------------------------------

class TestNodePollFailures:
   def _record(self, **overrides):
      base = {
         "system": "polaris",
         "source_hostname": "polaris-login-01.example.org",
         "loop": "counter",
         "timestamp_utc": "2026-09-09T00:00:00Z",
         "failure_type": "timeout",
         "detail": "counter probe exceeded 4.0s",
         "consecutive_failures": 1,
         "breaker_state": "closed",
      }
      base.update(overrides)
      return base

   def test_valid_record_accepted(self):
      record = self._record()
      assert validate_node_poll_failures(record) == record

   def test_invalid_loop_rejected(self):
      with pytest.raises(ContractError):
         validate_node_poll_failures(self._record(loop="hwinfo"))

   def test_invalid_failure_type_rejected(self):
      with pytest.raises(ContractError):
         validate_node_poll_failures(self._record(failure_type="oops"))

   def test_invalid_breaker_state_rejected(self):
      with pytest.raises(ContractError):
         validate_node_poll_failures(self._record(breaker_state="frozen"))

   def test_unknown_key_rejected(self):
      with pytest.raises(ContractError, match="unknown"):
         validate_node_poll_failures(self._record(bogus=1))

   def test_negative_consecutive_failures_rejected(self):
      with pytest.raises(ContractError):
         validate_node_poll_failures(self._record(consecutive_failures=-1))


# --------------------------------------------------------------------------
# node_collection_log
# --------------------------------------------------------------------------

class TestNodeCollectionLog:
   def _record(self, **overrides):
      base = {
         "system": "polaris",
         "timestamp_utc": "2026-09-09T00:00:00Z",
         "event": "daemon_start",
         "detail": {"pid": 1234},
      }
      base.update(overrides)
      return base

   def test_valid_record_accepted(self):
      record = self._record()
      assert validate_node_collection_log(record) == record

   def test_unknown_key_rejected(self):
      with pytest.raises(ContractError, match="unknown"):
         validate_node_collection_log(self._record(bogus=1))

   def test_missing_required_key_rejected(self):
      record = self._record()
      del record["event"]
      with pytest.raises(ContractError, match="missing"):
         validate_node_collection_log(record)

   def test_detail_must_be_object(self):
      with pytest.raises(ContractError):
         validate_node_collection_log(self._record(detail="not an object"))


# --------------------------------------------------------------------------
# diagnostic_census -- the raw-argv guarantee lives here
# --------------------------------------------------------------------------

class TestDiagnosticCensus:
   def _record(self, **overrides):
      base = {
         "system": "polaris",
         "source_hostname": "polaris-login-04.example.org",
         "timestamp_utc": "2026-09-09T00:00:00Z",
         "probe_version": 4,
         "processes": [
            {"pid": 42, "username": "jchilders", "category": "shell/session"},
         ],
         "cpu_deltas": {"deltas": [], "unmeasured": [], "anomalies": []},
      }
      base.update(overrides)
      return base

   def test_valid_record_accepted(self):
      record = self._record()
      assert validate_diagnostic_census(record) == record

   def test_unknown_top_level_key_rejected(self):
      with pytest.raises(ContractError, match="unknown"):
         validate_diagnostic_census(self._record(bogus=1))

   def test_top_level_argv_key_rejected(self):
      record = self._record(argv=["python3", "--secret", "xyz"])
      with pytest.raises(ContractError, match="argv"):
         validate_diagnostic_census(record)

   def test_process_row_with_argv_key_rejected(self):
      record = self._record(processes=[
         {"pid": 1, "username": "u", "category": "other", "argv": ["a"]},
      ])
      with pytest.raises(ContractError, match="argv"):
         validate_diagnostic_census(record)

   def test_process_row_with_cmdline_raw_key_rejected(self):
      record = self._record(processes=[
         {"pid": 1, "username": "u", "category": "other",
          "cmdline_raw": "python3 --secret xyz"},
      ])
      with pytest.raises(ContractError, match="argv"):
         validate_diagnostic_census(record)

   def test_processes_must_be_a_list(self):
      with pytest.raises(ContractError):
         validate_diagnostic_census(self._record(processes={}))

   def test_missing_required_key_rejected(self):
      record = self._record()
      del record["probe_version"]
      with pytest.raises(ContractError, match="missing"):
         validate_diagnostic_census(record)


# --------------------------------------------------------------------------
# validate_record dispatch
# --------------------------------------------------------------------------

class TestValidateRecordDispatch:
   def test_unknown_record_type_rejected(self):
      with pytest.raises(ContractError, match="record_type"):
         validate_record("no_such_type", {})
