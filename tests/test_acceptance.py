"""Tests for node_monitor.output.acceptance -- the pure Phase 0
canary acceptance-summary evaluator.

Design: PHASE0_DAEMON_DESIGN.md "Canary acceptance criteria" (kanban
task t_b0ac4c03, split B of t_90c962e7's Task 7 write-up). This module
is a PURE function over already-collected, already-measured inputs --
it never touches a sink, a scheduler, or the filesystem itself; every
input here is exactly the kind of bounded, already-computed value the
daemon accumulates during a run (per-node poll totals, counter-window
completeness stats, a bounded scheduling-delay sample set) or that
``Phase0Sink.finalize_summary()`` already computes for its own
``files`` section (record/malformed/truncated-final-line counts).

Two probe-wall-time thresholds from the design ("Counter probe p95
<=0.5s; census probe p95 <=2s") are deliberately reported as
UNAVAILABLE in this increment, never fabricated: today's
``transport_fn`` contract (design: "async callable returning the raw
probe payload dict") discards ``collector.transport.ProbeResult``'s
own ``wall_seconds`` before it ever reaches ``node_monitor.daemon`` --
wiring that through is a transport/CLI-layer contract change this
increment's own scope note excludes ("Do not implement CLI, deploy
scripts, or docs"). Every other threshold in the design's own
enumerated list is genuinely computable from what the daemon already
measures and IS evaluated here.
"""

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
      counter_totals={"a.example.org": 6},
      census_totals={"a.example.org": 1},
      counter_window_stats={"complete_windows": 1, "complete_windows_meeting_minimum": 1},
      scheduling_delay_samples=[0.01, 0.02, 0.03],
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
      # duration=60, interval=10 -> 6 expected counter polls.
      result = _evaluate(
         counter_totals={"a.example.org": 6}, duration_sec=60.0,
         counter_interval_sec=10.0)
      node = result["thresholds"]["counter_coverage_per_node"]["a.example.org"]
      assert node["value"] == 1.0
      assert node["required"] == 0.99
      assert node["met"] is True

   def test_counter_coverage_below_threshold_fails_and_degrades_status(self):
      # duration=100, interval=10 -> 10 expected; only 5 actual -> 50%.
      result = _evaluate(
         counter_totals={"a.example.org": 5}, duration_sec=100.0,
         counter_interval_sec=10.0)
      node = result["thresholds"]["counter_coverage_per_node"]["a.example.org"]
      assert node["met"] is False
      assert result["status"] == "degraded"

   def test_census_coverage_below_95_percent_fails(self):
      # duration=600, interval=60 -> 10 expected; 9 actual -> 90% < 95%.
      result = _evaluate(
         census_totals={"a.example.org": 9}, duration_sec=600.0,
         census_interval_sec=60.0)
      node = result["thresholds"]["census_coverage_per_node"]["a.example.org"]
      assert node["value"] == pytest.approx(0.9)
      assert node["met"] is False

   def test_multiple_nodes_evaluated_independently(self):
      result = _evaluate(
         counter_totals={"a.example.org": 6, "b.example.org": 3},
         duration_sec=60.0, counter_interval_sec=10.0)
      thresholds = result["thresholds"]["counter_coverage_per_node"]
      assert thresholds["a.example.org"]["met"] is True
      assert thresholds["b.example.org"]["met"] is False


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
# Probe wall-time thresholds: explicitly unavailable this increment
# --------------------------------------------------------------------------

class TestProbeWallTimeUnavailable:
   def test_counter_probe_p95_reported_unavailable_never_fabricated(self):
      result = _evaluate()
      entry = result["thresholds"]["counter_probe_p95_sec"]
      assert entry["value"] is None
      assert entry["met"] is None
      assert entry["unavailable_reason"]

   def test_census_probe_p95_reported_unavailable_never_fabricated(self):
      result = _evaluate()
      entry = result["thresholds"]["census_probe_p95_sec"]
      assert entry["value"] is None
      assert entry["met"] is None
      assert entry["unavailable_reason"]

   def test_unavailable_thresholds_never_degrade_status(self):
      result = _evaluate()
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
# No fabrication / input validation
# --------------------------------------------------------------------------

class TestNoFabrication:
   def test_node_with_zero_expected_polls_is_not_evaluated_as_coverage(self):
      # duration shorter than one interval -> zero expected dispatches
      # for that loop; nothing to honestly divide by.
      result = _evaluate(
         counter_totals={"a.example.org": 0}, duration_sec=5.0,
         counter_interval_sec=10.0)
      node = result["thresholds"]["counter_coverage_per_node"]["a.example.org"]
      assert node["met"] is None
