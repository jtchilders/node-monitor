"""Tests for node_monitor.output.acceptance -- the pure Phase 0
canary acceptance-summary evaluator.

Design: PHASE0_DAEMON_DESIGN.md "Canary acceptance criteria" (kanban
task t_b0ac4c03, split B of t_90c962e7's Task 7 write-up). This module
is a PURE function over already-collected, already-measured inputs --
it never touches a sink, a scheduler, or the filesystem itself; every
input here is exactly the kind of bounded, already-computed value the
daemon accumulates during a run (configured node list, per-node poll
totals, counter-window completeness stats, bounded scheduling-delay
and probe-wall-time sample sets) or that ``Phase0Sink.
finalize_summary()`` already computes for its own ``files`` section
(record/malformed/truncated-final-line counts).

Every design "Canary acceptance criteria" bullet this evaluator scopes
to is exercised here with a real pass and a real fail case, including
the two probe-wall-time thresholds ("Counter probe p95 <=0.5s; census
probe p95 <=2s") evaluated for real from sample lists, with an empty
list reported unavailable (never fabricated as zero or passing).
"""

import math

import pytest

from node_monitor.output.acceptance import evaluate_acceptance


def _files_summary(**overrides):
   base = {
      "node_hardware": {"record_count": 1, "malformed_count": 0,
                         "truncated_final_line": False},
      "node_counter_samples": {"record_count": 1, "malformed_count": 0,
                                "truncated_final_line": False},
      "node_usage_intervals": {"record_count": 1, "malformed_count": 0,
                                "truncated_final_line": False},
      "node_poll_failures": {"record_count": 0, "malformed_count": 0,
                              "truncated_final_line": False},
      "node_collection_log": {"record_count": 0, "malformed_count": 0,
                               "truncated_final_line": False},
      "diagnostic_census": {"record_count": 1, "malformed_count": 0,
                             "truncated_final_line": False},
   }
   for key, patch in overrides.items():
      base[key] = dict(base[key], **patch)
   return base


def _evaluate(**overrides):
   kwargs = dict(
      completion="clean",
      duration_sec=60.0,
      counter_interval_sec=10.0,
      census_interval_sec=60.0,
      nodes=["a.example.org"],
      counter_totals={"a.example.org": 6},
      census_totals={"a.example.org": 1},
      counter_window_stats={"complete_windows": 1, "complete_windows_meeting_minimum": 1},
      scheduling_delay_samples=[0.01, 0.02, 0.03],
      counter_probe_wall_seconds=[0.1, 0.2, 0.15],
      census_probe_wall_seconds=[0.5, 0.6, 0.4],
      files_summary=_files_summary(),
   )
   kwargs.update(overrides)
   return evaluate_acceptance(**kwargs)


# --------------------------------------------------------------------------
# Completion path: clean finite-duration vs orderly-stop partial
# --------------------------------------------------------------------------

class TestCompletionPath:
   def test_clean_completion_with_all_thresholds_met_reports_clean(self):
      result = _evaluate(completion="clean")
      assert result["completion"] == "clean"
      assert result["status"] == "clean"

   def test_partial_completion_with_all_thresholds_met_reports_partial(self):
      result = _evaluate(completion="partial")
      assert result["completion"] == "partial"
      assert result["status"] == "partial"

   def test_rejects_unknown_completion_value(self):
      with pytest.raises(ValueError):
         _evaluate(completion="bogus")


# --------------------------------------------------------------------------
# Per-node counter/census coverage
# --------------------------------------------------------------------------

class TestCoverage:
   def test_counter_coverage_meets_threshold_at_exactly_99_percent(self):
      # duration=60, interval=10 -> ceil(60/10)=6 expected counter polls.
      result = _evaluate(
         counter_totals={"a.example.org": 6}, duration_sec=60.0,
         counter_interval_sec=10.0)
      node = result["thresholds"]["counter_coverage_per_node"]["a.example.org"]
      assert node["expected_count"] == 6
      assert node["value"] == 1.0
      assert node["required"] == 0.99
      assert node["met"] is True

   def test_counter_coverage_below_threshold_fails_and_degrades_status(self):
      # duration=100, interval=10 -> ceil(100/10)=10 expected; only 5 actual.
      result = _evaluate(
         counter_totals={"a.example.org": 5}, duration_sec=100.0,
         counter_interval_sec=10.0)
      node = result["thresholds"]["counter_coverage_per_node"]["a.example.org"]
      assert node["met"] is False
      assert result["status"] == "degraded"

   def test_census_coverage_below_95_percent_fails(self):
      # duration=600, interval=60 -> ceil(600/60)=10 expected; 9 actual -> 90%.
      result = _evaluate(
         census_totals={"a.example.org": 9}, duration_sec=600.0,
         census_interval_sec=60.0)
      node = result["thresholds"]["census_coverage_per_node"]["a.example.org"]
      assert node["value"] == pytest.approx(0.9)
      assert node["met"] is False

   def test_multiple_nodes_evaluated_independently(self):
      result = _evaluate(
         nodes=["a.example.org", "b.example.org"],
         counter_totals={"a.example.org": 6, "b.example.org": 3},
         duration_sec=60.0, counter_interval_sec=10.0)
      thresholds = result["thresholds"]["counter_coverage_per_node"]
      assert thresholds["a.example.org"]["met"] is True
      assert thresholds["b.example.org"]["met"] is False

   def test_configured_node_with_zero_successful_polls_is_included(self):
      # b.example.org never appears in counter_totals at all -- must
      # still get a coverage entry (0 actual, met False), never be
      # silently absent from the summary.
      result = _evaluate(
         nodes=["a.example.org", "b.example.org"],
         counter_totals={"a.example.org": 6},
         duration_sec=60.0, counter_interval_sec=10.0)
      thresholds = result["thresholds"]["counter_coverage_per_node"]
      assert set(thresholds) == {"a.example.org", "b.example.org"}
      assert thresholds["b.example.org"]["actual_count"] == 0
      assert thresholds["b.example.org"]["met"] is False
      assert result["status"] == "degraded"

   def test_totals_entry_for_node_not_in_nodes_is_rejected(self):
      with pytest.raises(ValueError):
         _evaluate(
            nodes=["a.example.org"],
            counter_totals={"a.example.org": 6, "unknown.example.org": 1})


# --------------------------------------------------------------------------
# Expected-poll denominator matches Scheduler's own t=0 dispatch grid
# --------------------------------------------------------------------------

class TestExpectedCountBoundaries:
   def test_duration_shorter_than_one_interval_still_expects_one_dispatch(self):
      # Scheduler dispatches at t=0 regardless of duration; a run
      # shorter than one interval still gets exactly one honest
      # chance to poll -- ceil(5/10) == 1, never floor's 0.
      result = _evaluate(
         nodes=["a.example.org"], counter_totals={"a.example.org": 0},
         duration_sec=5.0, counter_interval_sec=10.0)
      node = result["thresholds"]["counter_coverage_per_node"]["a.example.org"]
      assert node["expected_count"] == 1
      assert node["met"] is False

   def test_duration_exactly_one_interval_expects_one_dispatch(self):
      result = _evaluate(
         nodes=["a.example.org"], counter_totals={"a.example.org": 1},
         duration_sec=10.0, counter_interval_sec=10.0)
      node = result["thresholds"]["counter_coverage_per_node"]["a.example.org"]
      assert node["expected_count"] == 1
      assert node["met"] is True

   def test_duration_just_over_a_multiple_of_interval_rounds_up(self):
      # duration=31, interval=10 -> Scheduler dispatches at 0,10,20,30 (4),
      # matching ceil(31/10)=4, never floor's 3.
      result = _evaluate(
         nodes=["a.example.org"], counter_totals={"a.example.org": 4},
         duration_sec=31.0, counter_interval_sec=10.0)
      node = result["thresholds"]["counter_coverage_per_node"]["a.example.org"]
      assert node["expected_count"] == 4
      assert node["met"] is True

   def test_duration_exact_multiple_of_interval_does_not_round_up(self):
      # duration=30, interval=10 -> Scheduler dispatches at 0,10,20 (3);
      # a naive ceil(30/10 + epsilon) must not over-count to 4.
      result = _evaluate(
         nodes=["a.example.org"], counter_totals={"a.example.org": 3},
         duration_sec=30.0, counter_interval_sec=10.0)
      node = result["thresholds"]["counter_coverage_per_node"]["a.example.org"]
      assert node["expected_count"] == 3
      assert node["met"] is True


# --------------------------------------------------------------------------
# Minimum five valid samples per complete counter rollup
# --------------------------------------------------------------------------

class TestMinimumSamplesPerCompleteWindow:
   def test_no_complete_windows_yet_is_unavailable_not_a_failure(self):
      result = _evaluate(
         counter_window_stats={"complete_windows": 0,
                                "complete_windows_meeting_minimum": 0})
      entry = result["thresholds"]["counter_window_minimum_samples"]
      assert entry["met"] is None
      assert result["status"] != "degraded"

   def test_every_complete_window_meeting_minimum_passes(self):
      result = _evaluate(
         counter_window_stats={"complete_windows": 4,
                                "complete_windows_meeting_minimum": 4})
      entry = result["thresholds"]["counter_window_minimum_samples"]
      assert entry["met"] is True

   def test_a_single_complete_window_under_minimum_fails(self):
      result = _evaluate(
         counter_window_stats={"complete_windows": 4,
                                "complete_windows_meeting_minimum": 3})
      entry = result["thresholds"]["counter_window_minimum_samples"]
      assert entry["met"] is False
      assert result["status"] == "degraded"

   def test_meeting_minimum_exceeding_complete_is_rejected(self):
      with pytest.raises(ValueError):
         _evaluate(
            counter_window_stats={"complete_windows": 2,
                                   "complete_windows_meeting_minimum": 3})


# --------------------------------------------------------------------------
# Scheduling-delay p95 <= 1s
# --------------------------------------------------------------------------

class TestSchedulingDelay:
   def test_empty_sample_set_is_unavailable(self):
      result = _evaluate(scheduling_delay_samples=[])
      entry = result["thresholds"]["scheduling_delay_p95_sec"]
      assert entry["met"] is None
      assert entry["value"] is None

   def test_p95_within_bound_passes(self):
      result = _evaluate(scheduling_delay_samples=[0.1] * 100)
      entry = result["thresholds"]["scheduling_delay_p95_sec"]
      assert entry["value"] == pytest.approx(0.1)
      assert entry["met"] is True

   def test_p95_over_bound_fails(self):
      samples = [0.1] * 94 + [2.0] * 6
      result = _evaluate(scheduling_delay_samples=samples)
      entry = result["thresholds"]["scheduling_delay_p95_sec"]
      assert entry["met"] is False
      assert result["status"] == "degraded"


# --------------------------------------------------------------------------
# Probe wall-time p95: counter <=0.5s, census <=2s -- evaluated for
# real from sample lists, unavailable only when genuinely empty.
# --------------------------------------------------------------------------

class TestProbeWallTime:
   def test_counter_probe_p95_within_bound_passes(self):
      result = _evaluate(counter_probe_wall_seconds=[0.1] * 100)
      entry = result["thresholds"]["counter_probe_p95_sec"]
      assert entry["value"] == pytest.approx(0.1)
      assert entry["required"] == 0.5
      assert entry["met"] is True

   def test_counter_probe_p95_over_bound_fails_and_degrades(self):
      samples = [0.1] * 94 + [1.0] * 6
      result = _evaluate(counter_probe_wall_seconds=samples)
      entry = result["thresholds"]["counter_probe_p95_sec"]
      assert entry["met"] is False
      assert result["status"] == "degraded"

   def test_census_probe_p95_within_bound_passes(self):
      result = _evaluate(census_probe_wall_seconds=[1.0] * 100)
      entry = result["thresholds"]["census_probe_p95_sec"]
      assert entry["value"] == pytest.approx(1.0)
      assert entry["required"] == 2.0
      assert entry["met"] is True

   def test_census_probe_p95_over_bound_fails_and_degrades(self):
      samples = [1.0] * 94 + [3.0] * 6
      result = _evaluate(census_probe_wall_seconds=samples)
      entry = result["thresholds"]["census_probe_p95_sec"]
      assert entry["met"] is False
      assert result["status"] == "degraded"

   def test_empty_counter_probe_samples_is_unavailable_never_fabricated(self):
      result = _evaluate(counter_probe_wall_seconds=[])
      entry = result["thresholds"]["counter_probe_p95_sec"]
      assert entry["value"] is None
      assert entry["met"] is None

   def test_empty_census_probe_samples_is_unavailable_never_fabricated(self):
      result = _evaluate(census_probe_wall_seconds=[])
      entry = result["thresholds"]["census_probe_p95_sec"]
      assert entry["value"] is None
      assert entry["met"] is None

   def test_unavailable_probe_thresholds_never_degrade_status(self):
      result = _evaluate(
         counter_probe_wall_seconds=[], census_probe_wall_seconds=[])
      assert result["status"] == "clean"


# --------------------------------------------------------------------------
# Malformed / truncated JSONL accounting
# --------------------------------------------------------------------------

class TestArtifactIntegrity:
   def test_no_malformed_or_truncated_lines_passes(self):
      result = _evaluate(files_summary=_files_summary())
      entry = result["thresholds"]["artifact_integrity"]
      assert entry["value"]["malformed_lines_total"] == 0
      assert entry["value"]["files_with_truncated_final_line"] == []
      assert entry["met"] is True

   def test_malformed_line_anywhere_fails_regardless_of_completion(self):
      files = _files_summary(node_hardware={"malformed_count": 1})
      result = _evaluate(completion="partial", files_summary=files)
      entry = result["thresholds"]["artifact_integrity"]
      assert entry["value"]["malformed_lines_total"] == 1
      assert entry["met"] is False
      assert result["status"] == "degraded"

   def test_truncated_final_line_tolerated_on_partial_completion(self):
      files = _files_summary(node_counter_samples={"truncated_final_line": True})
      result = _evaluate(completion="partial", files_summary=files)
      entry = result["thresholds"]["artifact_integrity"]
      assert entry["value"]["files_with_truncated_final_line"] == ["node_counter_samples"]
      assert entry["met"] is True

   def test_truncated_final_line_fails_on_clean_completion(self):
      files = _files_summary(node_counter_samples={"truncated_final_line": True})
      result = _evaluate(completion="clean", files_summary=files)
      entry = result["thresholds"]["artifact_integrity"]
      assert entry["met"] is False
      assert result["status"] == "degraded"


# --------------------------------------------------------------------------
# Input validation -- no fabrication, no silently-accepted garbage
# --------------------------------------------------------------------------

class TestInputValidation:
   def test_nodes_must_be_non_empty(self):
      with pytest.raises(ValueError):
         _evaluate(nodes=[])

   def test_nodes_must_not_contain_duplicates(self):
      with pytest.raises(ValueError):
         _evaluate(nodes=["a.example.org", "a.example.org"])

   def test_duration_must_be_positive(self):
      with pytest.raises(ValueError):
         _evaluate(duration_sec=0.0)

   def test_duration_must_be_finite(self):
      with pytest.raises(ValueError):
         _evaluate(duration_sec=float("inf"))

   def test_duration_rejects_nan(self):
      with pytest.raises(ValueError):
         _evaluate(duration_sec=float("nan"))

   def test_counter_interval_must_be_positive(self):
      with pytest.raises(ValueError):
         _evaluate(counter_interval_sec=0.0)

   def test_census_interval_must_be_positive(self):
      with pytest.raises(ValueError):
         _evaluate(census_interval_sec=-1.0)

   def test_counter_totals_rejects_negative_count(self):
      with pytest.raises(ValueError):
         _evaluate(counter_totals={"a.example.org": -1})

   def test_counter_totals_rejects_non_integer_count(self):
      with pytest.raises(ValueError):
         _evaluate(counter_totals={"a.example.org": 3.5})

   def test_scheduling_delay_samples_rejects_negative_value(self):
      with pytest.raises(ValueError):
         _evaluate(scheduling_delay_samples=[0.1, -0.1])

   def test_scheduling_delay_samples_rejects_nan(self):
      with pytest.raises(ValueError):
         _evaluate(scheduling_delay_samples=[0.1, float("nan")])

   def test_probe_wall_seconds_rejects_negative_value(self):
      with pytest.raises(ValueError):
         _evaluate(counter_probe_wall_seconds=[0.1, -0.2])

   def test_malformed_count_rejects_negative_value(self):
      files = _files_summary(node_hardware={"malformed_count": -1})
      with pytest.raises(ValueError):
         _evaluate(files_summary=files)
