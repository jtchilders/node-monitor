"""Tests for node_monitor.collector.usage -- census transformation and
15-minute usage aggregation.

Design: PHASE0_DAEMON_DESIGN.md "Output contract" (node_usage_intervals,
diagnostic_census) + "Metric semantics" (process identity, CPU
attribution) + Task 5 write-up in PHASE0_DAEMON_IMPLEMENTATION_PLAN.md.

Two things under test, matching the module's own split:

* ``build_diagnostic_census`` -- pure function, one raw census probe
  payload (+ the daemon-known system/source_hostname identity, + the
  optional cpu_delta result against the previous sample) in, one
  ``diagnostic_censuses`` record out. Privacy-filtered: this is the last
  checkpoint before a census-derived record reaches the sink, so it must
  not depend on remote_probe.py's own --drop-raw-args default to keep
  raw argv out.
* ``UsageIntervalAccumulator`` -- bounded per-node state for one
  15-minute window; ``finalize()`` must produce dicts that
  ``node_monitor.output.contracts.validate_node_usage_intervals`` accepts
  outright, so this suite pins the accumulator directly against the real
  contract validator rather than a hand-invented shape.
"""

import pytest

from node_monitor.collector.usage import (
   UsageIntervalAccumulator,
   build_diagnostic_census,
   build_usage_observations,
)
from node_monitor.output.contracts import (
   ContractError,
   validate_diagnostic_census,
   validate_node_usage_intervals,
)


CLK_TCK = 100


def _row(pid, start_time_ticks, utime_ticks=0, stime_ticks=0,
         category="other", activity=None, username="jchilders",
         rss_kb=1000, state="S", interactive=False, cmdline=None):
   row = {
      "pid": pid,
      "ppid": 1,
      "uid": 1000,
      "username": username,
      "comm": "python",
      "category": category,
      "behavior": "batch",
      "activity": activity,
      "activity_confidence": "unknown",
      "project_path_hint": None,
      "utime_ticks": utime_ticks,
      "stime_ticks": stime_ticks,
      "rss_kb": rss_kb,
      "state": state,
      "start_time_ticks": start_time_ticks,
      "interactive": interactive,
   }
   if cmdline is not None:
      row["cmdline"] = cmdline
   return row


def _census(uptime_sec, processes, tools=None, wall_clock_utc="2026-09-09T00:00:00Z",
            probe_version=4, clk_tck=CLK_TCK):
   payload = {
      "probe_version": probe_version,
      "loop": "census",
      "hostname_fqdn": "polaris-login-04.example.org",
      "wall_clock_utc": wall_clock_utc,
      "uptime_sec": uptime_sec,
      "counters": {"clk_tck": clk_tck},
      "processes": processes,
   }
   if tools is not None:
      payload["tools"] = tools
   return payload


# --------------------------------------------------------------------------
# build_diagnostic_census
# --------------------------------------------------------------------------

class TestBuildDiagnosticCensus:
   def test_basic_shape_validates_against_contract(self):
      payload = _census(10.0, [_row(1, 5, utime_ticks=100, stime_ticks=50)])

      record = build_diagnostic_census(
         "polaris", "polaris-login-04.example.org", payload)

      validate_diagnostic_census(record)
      assert record["system"] == "polaris"
      assert record["source_hostname"] == "polaris-login-04.example.org"
      assert record["timestamp_utc"] == "2026-09-09T00:00:00Z"
      assert record["probe_version"] == 4

   def test_first_sample_with_no_prior_cpu_delta_has_empty_deltas(self):
      payload = _census(10.0, [_row(1, 5)])

      record = build_diagnostic_census(
         "polaris", "polaris-login-04.example.org", payload)

      assert record["cpu_deltas"]["deltas"] == []
      assert record["cpu_deltas"]["unmeasured"] == []
      assert record["cpu_deltas"]["anomalies"] == []

   def test_supplied_cpu_delta_is_carried_through(self):
      payload = _census(19.0, [_row(1, 5, utime_ticks=150, stime_ticks=70)])
      cpu_deltas = {
         "deltas": [{"pid": 1, "start_time_ticks": 5,
                     "utime_delta_ticks": 50, "stime_delta_ticks": 20,
                     "cpu_seconds": 0.7}],
         "unmeasured": [],
         "anomalies": [],
      }

      record = build_diagnostic_census(
         "polaris", "polaris-login-04.example.org", payload,
         cpu_deltas=cpu_deltas)

      assert record["cpu_deltas"]["deltas"] == cpu_deltas["deltas"]
      validate_diagnostic_census(record)

   def test_raw_cmdline_stripped_even_if_present_in_payload(self):
      # Defense-in-depth: even if the probe emitted raw cmdline (e.g. a
      # local test run with --keep-raw-args), this module must never let
      # it reach a diagnostic_census record.
      payload = _census(10.0, [
         _row(1, 5, cmdline="/usr/bin/python3 --secret-token abc123"),
      ])

      record = build_diagnostic_census(
         "polaris", "polaris-login-04.example.org", payload)

      assert "cmdline" not in record["processes"][0]
      validate_diagnostic_census(record)

   def test_tool_aggregates_retained_in_cpu_deltas(self):
      tools = [{"tool": "claude-code", "username": "jchilders",
                "proc_count": 2, "tree_root_count": 1, "install_count": None,
                "rss_kb_total": 4000, "nested_in": []}]
      payload = _census(10.0, [_row(1, 5)], tools=tools)

      record = build_diagnostic_census(
         "polaris", "polaris-login-04.example.org", payload)

      assert record["cpu_deltas"]["tools"] == tools
      validate_diagnostic_census(record)

   def test_missing_tools_field_yields_empty_list(self):
      payload = _census(10.0, [_row(1, 5)])
      assert "tools" not in payload

      record = build_diagnostic_census(
         "polaris", "polaris-login-04.example.org", payload)

      assert record["cpu_deltas"]["tools"] == []

   def test_other_process_fields_preserved(self):
      payload = _census(10.0, [_row(
         1, 5, category="ai-coding-agent", activity="claude-code",
         username="jchilders", rss_kb=50000, state="S", interactive=True)])

      record = build_diagnostic_census(
         "polaris", "polaris-login-04.example.org", payload)

      row = record["processes"][0]
      assert row["category"] == "ai-coding-agent"
      assert row["activity"] == "claude-code"
      assert row["username"] == "jchilders"
      assert row["rss_kb"] == 50000
      assert row["state"] == "S"
      assert row["interactive"] is True


# --------------------------------------------------------------------------
# build_usage_observations -- joins cpu_delta results back to census rows
# --------------------------------------------------------------------------

class TestBuildUsageObservationsMeasured:
   def test_continuously_running_process_is_measured(self):
      previous = _census(10.0, [
         _row(42, 500, utime_ticks=100, stime_ticks=50,
              category="ai-coding-agent", activity="claude-code",
              username="alice", rss_kb=5000, state="S", interactive=True),
      ])
      current = _census(19.0, [
         _row(42, 500, utime_ticks=250, stime_ticks=90,
              category="ai-coding-agent", activity="claude-code",
              username="alice", rss_kb=5000, state="S", interactive=True),
      ])

      observations = build_usage_observations(current, previous)

      assert len(observations) == 1
      obs = observations[0]
      assert obs["pid"] == 42
      assert obs["unmeasured"] is False
      assert obs["currently_present"] is True
      assert obs["cpu_seconds"] == pytest.approx(1.9)
      assert obs["category"] == "ai-coding-agent"
      assert obs["activity"] == "claude-code"
      assert obs["username"] == "alice"
      assert obs["rss_kb"] == 5000
      assert obs["state"] == "S"
      assert obs["interactive"] is True

   def test_new_process_started_in_window_is_measured(self):
      previous = _census(10.0, [])
      current = _census(19.0, [
         _row(77, 1500, utime_ticks=30, stime_ticks=10,
              category="compute/build", activity="build-driver"),
      ])

      observations = build_usage_observations(current, previous)

      assert len(observations) == 1
      assert observations[0]["unmeasured"] is False
      assert observations[0]["currently_present"] is True
      assert observations[0]["cpu_seconds"] == pytest.approx(0.4)


class TestBuildUsageObservationsExited:
   def test_exited_process_is_unmeasured_with_category_from_previous_sample(self):
      previous = _census(10.0, [
         _row(55, 200, utime_ticks=400, stime_ticks=100,
              category="jupyter", activity="jupyter-kernel",
              username="bob", rss_kb=8000, state="S", interactive=False),
      ])
      current = _census(19.0, [])

      observations = build_usage_observations(current, previous)

      assert len(observations) == 1
      obs = observations[0]
      assert obs["pid"] == 55
      assert obs["unmeasured"] is True
      assert obs["currently_present"] is False
      assert obs["cpu_seconds"] == 0.0
      assert obs["category"] == "jupyter"
      assert obs["activity"] == "jupyter-kernel"
      assert obs["username"] == "bob"


class TestBuildUsageObservationsStartedBeforeWindow:
   def test_started_before_window_process_is_unmeasured_with_current_category(self):
      previous = _census(10.0, [])
      current = _census(19.0, [
         _row(88, 5, utime_ticks=9000, stime_ticks=4000,
              category="fs-scan", activity="filesystem-scan"),
      ])

      observations = build_usage_observations(current, previous)

      assert len(observations) == 1
      obs = observations[0]
      assert obs["unmeasured"] is True
      # Present in current_payload -- currently resident, unlike "exited" --
      # only its CPU is unattributable, its gauge fields are real.
      assert obs["currently_present"] is True
      assert obs["cpu_seconds"] == 0.0
      assert obs["category"] == "fs-scan"


class TestBuildUsageObservationsAmbiguous:
   def test_negative_delta_anomaly_is_unmeasured_not_fabricated(self):
      previous = _census(10.0, [
         _row(9, 50, utime_ticks=800, stime_ticks=250,
              category="other", activity=None),
      ])
      current = _census(19.0, [
         _row(9, 50, utime_ticks=700, stime_ticks=250,
              category="other", activity=None),
      ])

      observations = build_usage_observations(current, previous)

      assert len(observations) == 1
      obs = observations[0]
      assert obs["unmeasured"] is True
      assert obs["currently_present"] is True
      assert obs["cpu_seconds"] == 0.0


class TestBuildUsageObservationsFirstSample:
   def test_no_previous_payload_marks_every_process_unmeasured(self):
      # First census of a run: no previous sample exists at all, so
      # cpu_delta.compute_cpu_delta is never called (there is nothing to
      # diff against). Every process is present but unmeasurable -- it
      # is nonetheless genuinely resident right now.
      current = _census(10.0, [
         _row(1, 5, category="shell/session", activity="shell"),
         _row(2, 8, category="other", activity=None),
      ])

      observations = build_usage_observations(current, previous_payload=None)

      assert len(observations) == 2
      assert all(obs["unmeasured"] is True for obs in observations)
      assert all(obs["currently_present"] is True for obs in observations)
      assert all(obs["cpu_seconds"] == 0.0 for obs in observations)


class TestBuildUsageObservationsMixed:
   def test_mixed_sample_produces_correct_observation_per_pid(self):
      previous = _census(10.0, [
         _row(1, 10, utime_ticks=100, stime_ticks=50,
              category="shell/session", activity="shell", username="alice"),
         _row(2, 20, utime_ticks=300, stime_ticks=100,
              category="jupyter", activity="jupyter-kernel", username="bob"),
      ])
      current = _census(19.0, [
         _row(1, 10, utime_ticks=150, stime_ticks=70,
              category="shell/session", activity="shell", username="alice"),
         _row(4, 1700, utime_ticks=20, stime_ticks=10,
              category="compute/build", activity="build-driver",
              username="alice"),
      ])

      observations = build_usage_observations(current, previous)

      by_pid = {obs["pid"]: obs for obs in observations}
      assert set(by_pid) == {1, 2, 4}
      assert by_pid[1]["unmeasured"] is False
      assert by_pid[1]["currently_present"] is True
      assert by_pid[1]["cpu_seconds"] == pytest.approx(0.7)
      assert by_pid[4]["unmeasured"] is False
      assert by_pid[4]["currently_present"] is True
      assert by_pid[4]["cpu_seconds"] == pytest.approx(0.3)
      assert by_pid[2]["unmeasured"] is True
      assert by_pid[2]["currently_present"] is False
      assert by_pid[2]["category"] == "jupyter"
      assert by_pid[2]["username"] == "bob"


# --------------------------------------------------------------------------
# UsageIntervalAccumulator -- bounded per-node 15-minute rollup
# --------------------------------------------------------------------------

def _make_accumulator(expected_count=15, system="polaris",
                       source_hostname="polaris-login-04.example.org"):
   return UsageIntervalAccumulator(
      system=system,
      source_hostname=source_hostname,
      interval_start_utc="2026-09-09T00:00:00Z",
      interval_end_utc="2026-09-09T00:15:00Z",
      expected_count=expected_count,
   )


def _process_row(pid, category="other", activity=None, username="jchilders",
                  cpu_seconds=0.0, rss_kb=1000, state="S", interactive=False,
                  unmeasured=False, currently_present=True):
   """One 'observed process' entry as add_sample expects -- the shape the
   daemon builds by joining one census payload's process rows against
   cpu_delta.compute_cpu_delta's per-pid deltas/unmeasured lists for the
   SAME sample. ``unmeasured=True`` means this pid had no measurable CPU
   delta for this sample (new-but-before-window, exited, or the first
   sample of the run with nothing to diff against) -- cpu_seconds is
   irrelevant/ignored in that case. ``currently_present=False`` means
   this pid is NOT part of the current census (it is an "exited" entry
   carried only for CPU attribution) and must never contribute to the
   current sample's process-count/RSS/D-state/interactivity gauges --
   only to unmeasured_count.
   """
   return {
      "pid": pid,
      "category": category,
      "activity": activity,
      "username": username,
      "cpu_seconds": cpu_seconds,
      "rss_kb": rss_kb,
      "state": state,
      "interactive": interactive,
      "unmeasured": unmeasured,
      "currently_present": currently_present,
   }


class TestGrainGrouping:
   def test_separate_rows_per_category_activity_username(self):
      acc = _make_accumulator(expected_count=1)
      acc.add_sample([
         _process_row(1, category="ai-coding-agent", activity="claude-code",
                      username="alice", cpu_seconds=1.0),
         _process_row(2, category="ai-coding-agent", activity="codex-cli",
                      username="alice", cpu_seconds=2.0),
         _process_row(3, category="ai-coding-agent", activity="claude-code",
                      username="bob", cpu_seconds=3.0),
      ])

      records = acc.finalize()

      keys = {(r["category"], r["activity"], r["username"]) for r in records}
      assert keys == {
         ("ai-coding-agent", "claude-code", "alice"),
         ("ai-coding-agent", "codex-cli", "alice"),
         ("ai-coding-agent", "claude-code", "bob"),
      }
      for record in records:
         validate_node_usage_intervals(record)

   def test_same_grain_processes_summed_together(self):
      acc = _make_accumulator(expected_count=1)
      acc.add_sample([
         _process_row(1, category="shell/session", activity="shell",
                      username="alice", cpu_seconds=1.0),
         _process_row(2, category="shell/session", activity="shell",
                      username="alice", cpu_seconds=2.0),
      ])

      records = acc.finalize()

      assert len(records) == 1
      assert records[0]["cpu_seconds"] == pytest.approx(3.0)


class TestActivityNoneNormalizedToUnknown:
   def test_null_activity_becomes_unknown_string(self):
      acc = _make_accumulator(expected_count=1)
      acc.add_sample([_process_row(1, category="other", activity=None)])

      records = acc.finalize()

      assert len(records) == 1
      assert records[0]["activity"] == "unknown"
      validate_node_usage_intervals(records[0])


class TestUsernameMayBeNull:
   def test_null_username_grain_preserved_and_passes_contract(self):
      acc = _make_accumulator(expected_count=1)
      acc.add_sample([_process_row(1, username=None)])

      records = acc.finalize()

      assert len(records) == 1
      assert records[0]["username"] is None
      validate_node_usage_intervals(records[0])


class TestProcessCountPercentiles:
   def test_p50_p95_max_over_samples(self):
      acc = _make_accumulator(expected_count=3)
      # Sample 1: 1 process in this grain; sample 2: 3; sample 3: 5.
      acc.add_sample([_process_row(1, cpu_seconds=0.1)])
      acc.add_sample([_process_row(1, cpu_seconds=0.1),
                       _process_row(2, cpu_seconds=0.1),
                       _process_row(3, cpu_seconds=0.1)])
      acc.add_sample([_process_row(i, cpu_seconds=0.1) for i in range(5)])

      records = acc.finalize()

      assert len(records) == 1
      counts = records[0]["process_count"]
      assert counts["max"] == 5
      assert counts["p50"] <= counts["p95"] <= counts["max"]


class TestCpuSecondsSummedAcrossSamples:
   def test_cpu_seconds_accumulates_across_samples(self):
      acc = _make_accumulator(expected_count=2)
      acc.add_sample([_process_row(1, cpu_seconds=2.0)])
      acc.add_sample([_process_row(1, cpu_seconds=3.5)])

      records = acc.finalize()

      assert records[0]["cpu_seconds"] == pytest.approx(5.5)


class TestRssSummary:
   def test_rss_kb_percentiles_over_all_observations(self):
      acc = _make_accumulator(expected_count=2)
      acc.add_sample([_process_row(1, rss_kb=1000)])
      acc.add_sample([_process_row(1, rss_kb=3000)])

      records = acc.finalize()

      rss = records[0]["rss_kb"]
      assert rss["max"] == 3000
      assert rss["p50"] <= rss["p95"] <= rss["max"]


class TestDStateFraction:
   def test_fraction_of_observations_in_d_state(self):
      acc = _make_accumulator(expected_count=4)
      acc.add_sample([_process_row(1, state="D")])
      acc.add_sample([_process_row(1, state="S")])
      acc.add_sample([_process_row(1, state="D")])
      acc.add_sample([_process_row(1, state="S")])

      records = acc.finalize()

      assert records[0]["d_state_fraction"] == pytest.approx(0.5)


class TestInteractivityFraction:
   def test_fraction_of_observations_interactive(self):
      acc = _make_accumulator(expected_count=4)
      acc.add_sample([_process_row(1, interactive=True)])
      acc.add_sample([_process_row(1, interactive=True)])
      acc.add_sample([_process_row(1, interactive=False)])
      acc.add_sample([_process_row(1, interactive=False)])

      records = acc.finalize()

      assert records[0]["interactivity_fraction"] == pytest.approx(0.5)


class TestExpectedAndSampleCounts:
   def test_full_window_reports_full_expected_and_sample_counts(self):
      acc = _make_accumulator(expected_count=3)
      for _ in range(3):
         acc.add_sample([_process_row(1)])

      records = acc.finalize()

      assert records[0]["sample_count"] == 3
      assert records[0]["expected_count"] == 3

   def test_partial_window_reports_partial_sample_count(self):
      acc = _make_accumulator(expected_count=3)
      acc.add_sample([_process_row(1)])

      records = acc.finalize()

      assert records[0]["sample_count"] == 1
      assert records[0]["expected_count"] == 3


class TestUnmeasuredCount:
   def test_unmeasured_process_observation_counted_separately(self):
      acc = _make_accumulator(expected_count=2)
      acc.add_sample([_process_row(1, cpu_seconds=0.0, unmeasured=True)])
      acc.add_sample([_process_row(1, cpu_seconds=1.0, unmeasured=False)])

      records = acc.finalize()

      assert records[0]["unmeasured_count"] == 1
      # Only the measured observation contributes cpu_seconds.
      assert records[0]["cpu_seconds"] == pytest.approx(1.0)

   def test_unmeasured_only_grain_still_produces_a_valid_row(self):
      acc = _make_accumulator(expected_count=1)
      acc.add_sample([_process_row(1, unmeasured=True)])

      records = acc.finalize()

      assert len(records) == 1
      assert records[0]["unmeasured_count"] == 1
      assert records[0]["cpu_seconds"] == 0.0
      validate_node_usage_intervals(records[0])


class TestEmptyWindow:
   def test_zero_samples_produces_zero_records(self):
      acc = _make_accumulator(expected_count=3)

      records = acc.finalize()

      assert records == []


class TestContractCompliance:
   def test_finalize_output_validates_against_node_usage_intervals_contract(self):
      acc = _make_accumulator(expected_count=2)
      acc.add_sample([_process_row(
         1, category="ai-coding-agent", activity="claude-code",
         username="jchilders", cpu_seconds=1.0, rss_kb=2000, state="S",
         interactive=True)])
      acc.add_sample([_process_row(
         1, category="ai-coding-agent", activity="claude-code",
         username="jchilders", cpu_seconds=1.5, rss_kb=2200, state="D",
         interactive=True)])

      records = acc.finalize()

      assert len(records) == 1
      validated = validate_node_usage_intervals(records[0])
      assert validated["system"] == "polaris"
      assert validated["source_hostname"] == "polaris-login-04.example.org"
      assert validated["interval_start_utc"] == "2026-09-09T00:00:00Z"
      assert validated["interval_end_utc"] == "2026-09-09T00:15:00Z"


# --------------------------------------------------------------------------
# Review round 1 findings -- bounded state, zero-fill, exited-process gauges
# --------------------------------------------------------------------------

class TestBoundedGrainState:
   def test_rss_state_bounded_by_expected_count_not_process_count(self):
      # Reviewer repro: one sample with 10,000 same-grain processes must
      # retain O(expected_count) rss list entries, not O(process count) --
      # a real login node with a busy grain (e.g. thousands of short-lived
      # "shell/session" processes) must not make accumulator memory scale
      # with process count across a whole 15-minute window.
      acc = _make_accumulator(expected_count=15)
      acc.add_sample([_process_row(pid, rss_kb=1000 + pid)
                       for pid in range(10000)])

      assert len(acc._grains[("other", "unknown", "jchilders")].rss_per_sample) == 1

   def test_rss_percentiles_computed_over_per_sample_aggregates(self):
      # The bounded aggregate must still be a meaningful RSS summary: one
      # value per sample (e.g. the sample's total/representative RSS for
      # the grain), not a single collapsed constant.
      acc = _make_accumulator(expected_count=3)
      acc.add_sample([_process_row(1, rss_kb=1000), _process_row(2, rss_kb=2000)])
      acc.add_sample([_process_row(1, rss_kb=1500), _process_row(2, rss_kb=2500)])
      acc.add_sample([_process_row(1, rss_kb=3000), _process_row(2, rss_kb=3000)])

      records = acc.finalize()

      assert len(records) == 1
      rss = records[0]["rss_kb"]
      # Per-sample totals: 3000, 4000, 6000 -- max must be 6000, not a
      # per-process value like 3000.
      assert rss["max"] == 6000


class TestZeroFilledGaugeCounts:
   def test_grain_absence_in_a_later_sample_is_a_zero_not_an_omission(self):
      # Reviewer repro: present, then a sample where the grain's process
      # exited (no longer resident), then an empty census. Once a grain
      # has appeared in the interval, EVERY subsequent successful sample
      # must contribute a count for it -- zero when absent -- so
      # process_count percentiles and sample_count reflect every
      # successful census, not just the samples where the grain happened
      # to have a resident process.
      acc = _make_accumulator(expected_count=3)
      acc.add_sample([_process_row(1, category="other", activity="shell",
                                    username="alice", currently_present=True)])
      acc.add_sample([_process_row(1, category="other", activity="shell",
                                    username="alice", unmeasured=True,
                                    currently_present=False)])
      acc.add_sample([])

      records = acc.finalize()

      assert len(records) == 1
      record = records[0]
      # Per-sample process counts must be [1, 0, 0] -- one entry per
      # sample, zero-filled for the exited/empty samples, not [1]
      # (silently omitting the two absent samples).
      assert acc._grains[("other", "shell", "alice")].process_counts == [1, 0, 0]
      assert record["process_count"]["p50"] == 0.0
      assert record["process_count"]["max"] == 1
      assert record["sample_count"] == 3
      assert record["expected_count"] == 3

   def test_grain_first_appearing_after_earlier_successful_samples_is_backfilled(self):
      # Review round 2 finding 2: a grain that does not exist until
      # sample 3 of an already-2-samples-old interval must be
      # BACKFILLED with zero-counts for samples 1-2 -- those were still
      # successful polls of the interval, and this grain's sample_count/
      # percentiles must reflect every successful poll, not just the
      # ones after its own first appearance (the interval's
      # sample_count is "how many polls succeeded", not "how many polls
      # happened since this grain was born").
      acc = _make_accumulator(expected_count=3)
      acc.add_sample([])
      acc.add_sample([])
      acc.add_sample([_process_row(1, category="other", activity="shell",
                                    username="alice")])

      records = acc.finalize()

      assert len(records) == 1
      record = records[0]
      assert acc._grains[("other", "shell", "alice")].process_counts == [0, 0, 1]
      assert record["sample_count"] == 3
      assert record["expected_count"] == 3
      assert record["process_count"] == {"p50": 0.0, "p95": 0.9, "max": 1}


class TestExitedProcessDoesNotPolluteCurrentGauges:
   def test_exited_observation_contributes_unmeasured_not_gauge_state(self):
      # Reviewer finding 3: an exited process's stale previous-sample row
      # (rss_kb, state, interactive) must not be counted as a CURRENTLY
      # resident process's gauge contribution -- it only affects
      # unmeasured_count. This directly targets add_sample()'s handling
      # of currently_present=False observations.
      acc = _make_accumulator(expected_count=2)
      acc.add_sample([_process_row(
         55, category="jupyter", activity="jupyter-kernel", username="bob",
         rss_kb=999999, state="D", interactive=True,
         unmeasured=True, currently_present=False)])
      acc.add_sample([])

      records = acc.finalize()

      assert len(records) == 1
      record = records[0]
      assert record["unmeasured_count"] == 1
      # The exited process's stale D-state/interactive/huge-RSS values
      # must not leak into these gauges.
      assert record["d_state_fraction"] == 0.0
      assert record["interactivity_fraction"] == 0.0
      assert record["rss_kb"]["max"] < 999999
      # And it must not inflate process_count either.
      assert record["process_count"]["max"] == 0

   def test_exited_process_alongside_a_real_resident_in_same_sample(self):
      acc = _make_accumulator(expected_count=1)
      acc.add_sample([
         _process_row(1, category="other", activity="shell", username="alice",
                       rss_kb=2000, state="S", interactive=False,
                       currently_present=True),
         _process_row(2, category="other", activity="shell", username="alice",
                       rss_kb=999999, state="D", interactive=True,
                       unmeasured=True, currently_present=False),
      ])

      records = acc.finalize()

      assert len(records) == 1
      record = records[0]
      # Only the resident process (pid 1) counts toward this sample's
      # process count and gauges; the exited pid 2 contributes only
      # unmeasured_count.
      assert record["process_count"] == {"p50": 1, "p95": 1, "max": 1}
      assert record["rss_kb"]["max"] == 2000
      assert record["d_state_fraction"] == 0.0
      assert record["interactivity_fraction"] == 0.0
      assert record["unmeasured_count"] == 1


# --------------------------------------------------------------------------
# Review round 2 findings -- unmeasured-but-resident CPU exclusion,
# mid-window backfill, and a genuinely bounded excess-sample interval.
# --------------------------------------------------------------------------

class TestUnmeasuredResidentObservationExcludesCpu:
   def test_unmeasured_currently_present_observation_does_not_contribute_cpu(self):
      # Review round 2 finding 1: a CURRENTLY-RESIDENT but CPU-unmeasured
      # observation (e.g. "started before window") must still count
      # toward process-count/RSS/D-state/interactivity gauges, but its
      # (semantically meaningless, per contract, but nonzero here to
      # prove the boundary is enforced) cpu_seconds must NOT be added to
      # the grain's cpu_seconds total -- only unmeasured_count reflects
      # it. Uses a nonzero cpu_seconds input so this cannot pass by
      # accident just because build_usage_observations() always emits
      # 0.0 for unmeasured entries.
      acc = _make_accumulator(expected_count=1)
      acc.add_sample([_process_row(
         1, category="fs-scan", activity="filesystem-scan",
         username="jchilders", cpu_seconds=9.0, unmeasured=True,
         currently_present=True)])

      records = acc.finalize()

      assert len(records) == 1
      record = records[0]
      assert record["cpu_seconds"] == 0.0
      assert record["unmeasured_count"] == 1
      # Still counts as a resident process for the gauges.
      assert record["process_count"] == {"p50": 1, "p95": 1, "max": 1}

   def test_measured_and_unmeasured_resident_observations_in_same_grain(self):
      # A grain with one measured and one unmeasured-but-resident
      # observation in the same sample: only the measured one's CPU
      # counts, but both count toward process_count/gauges.
      acc = _make_accumulator(expected_count=1)
      acc.add_sample([
         _process_row(1, category="other", activity="shell",
                      username="alice", cpu_seconds=2.0, unmeasured=False,
                      currently_present=True),
         _process_row(2, category="other", activity="shell",
                      username="alice", cpu_seconds=99.0, unmeasured=True,
                      currently_present=True),
      ])

      records = acc.finalize()

      assert len(records) == 1
      record = records[0]
      assert record["cpu_seconds"] == pytest.approx(2.0)
      assert record["unmeasured_count"] == 1
      assert record["process_count"] == {"p50": 2, "p95": 2, "max": 2}


class TestIntervalBoundedByExpectedCount:
   def test_excess_samples_beyond_expected_count_are_rejected(self):
      # Review round 2 finding 3: an accumulator's bound is the
      # INTERVAL's expected_count, not just per-grain per-sample
      # aggregation -- feeding more add_sample calls than expected_count
      # must not grow retained state past what expected_count sized it
      # for, and must not inflate sample_count/process_count percentiles
      # past the interval's actual capacity.
      acc = _make_accumulator(expected_count=1)
      for _ in range(5):
         acc.add_sample([_process_row(1, category="other", activity="shell",
                                       username="alice")])

      records = acc.finalize()

      assert len(records) == 1
      record = records[0]
      grain = acc._grains[("other", "shell", "alice")]
      assert len(grain.process_counts) == 1
      assert len(grain.rss_per_sample) == 1
      assert record["sample_count"] == 1
      assert record["expected_count"] == 1
      assert record["process_count"] == {"p50": 1, "p95": 1, "max": 1}

   def test_excess_sample_does_not_create_a_brand_new_grain(self):
      # An excess sample must be rejected in its entirety -- it must not
      # even be allowed to create grain state for a process that never
      # appeared in any of the expected_count accepted samples.
      acc = _make_accumulator(expected_count=1)
      acc.add_sample([_process_row(1, category="other", activity="shell",
                                    username="alice")])
      acc.add_sample([_process_row(2, category="ai-coding-agent",
                                    activity="claude-code",
                                    username="bob")])

      records = acc.finalize()

      keys = {(r["category"], r["activity"], r["username"]) for r in records}
      assert keys == {("other", "shell", "alice")}
