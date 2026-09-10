"""Tests for node_monitor.collector.metrics -- counter deltas and
60-second rollups.

Design: PHASE0_DAEMON_DESIGN.md "Metric semantics" + "Output contract"
(node_counter_samples), and PLANNING.md 14.2's counter-wrap/reboot rule.
Two layers under test, matching the module's own split:

* ``compute_counter_delta`` -- pure function, one pair of consecutive raw
  probe payloads (the exact JSON shape remote_probe.py's main() prints
  for loop="counter") in, one delta-with-validity dict out.
* ``CounterWindowAccumulator`` -- bounded per-(node) state for one
  60-second window; ``finalize()`` must produce a dict that
  ``node_monitor.output.contracts.validate_node_counter_samples`` accepts
  outright, so this suite pins the accumulator directly against the real
  contract validator rather than a hand-invented shape.
"""

import pytest

from node_monitor.collector.metrics import (
   CounterWindowAccumulator,
   compute_counter_delta,
)
from node_monitor.output.contracts import validate_node_counter_samples


CLK_TCK = 100


def _sample(uptime_sec, cpu_jiffies=None, net=None, md_ops=None,
            mem=None, load1=None, load5=None, load15=None,
            procs_running=None, procs_total=None, socket_count=None,
            probe_version=4, hostname_fqdn="polaris-login-04.example.org"):
   """Build a payload shaped like remote_probe.py's real counter output.

   Only fields compute_counter_delta/CounterWindowAccumulator read are
   populated by default; callers add exactly the fields a given test
   needs (mirrors tests/test_cpu_delta.py's ``_sample`` helper).
   """
   counters = {"clk_tck": CLK_TCK}
   if cpu_jiffies is not None:
      counters["cpu_jiffies"] = cpu_jiffies
   if net is not None:
      counters["net"] = net
   if md_ops is not None:
      counters["md_ops"] = md_ops
   if mem is not None:
      counters["mem"] = mem
   if load1 is not None:
      counters["load1"] = load1
   if load5 is not None:
      counters["load5"] = load5
   if load15 is not None:
      counters["load15"] = load15
   if procs_running is not None:
      counters["procs_running"] = procs_running
   if procs_total is not None:
      counters["procs_total"] = procs_total
   if socket_count is not None:
      counters["socket_count"] = socket_count
   return {
      "probe_version": probe_version,
      "loop": "counter",
      "hostname_fqdn": hostname_fqdn,
      "uptime_sec": uptime_sec,
      "counters": counters,
   }


def _jiffies(user=0, nice=0, system=0, idle=0, iowait=0):
   return {"user": user, "nice": nice, "system": system, "idle": idle,
           "iowait": iowait}


# --------------------------------------------------------------------------
# compute_counter_delta -- pure pair-delta function
# --------------------------------------------------------------------------

class TestUptimeElapsedDenominator:
   def test_rate_uses_uptime_delta_not_wall_clock(self):
      sample_a = _sample(100.0, net={"pub0": {"rx_bytes": 1000, "tx_bytes": 500}})
      sample_b = _sample(110.0, net={"pub0": {"rx_bytes": 3000, "tx_bytes": 1500}})

      delta = compute_counter_delta(sample_a, sample_b)

      assert delta["elapsed_sec"] == 10.0
      assert delta["network"]["pub0"]["rx_bytes_per_sec"] == 200.0
      assert delta["network"]["pub0"]["tx_bytes_per_sec"] == 100.0


class TestRebootOrReset:
   def test_uptime_decrease_invalidates_whole_pair(self):
      # uptime_sec going backwards is the reboot signature: the node's own
      # monotonic-since-boot clock cannot decrease except across a reboot.
      sample_a = _sample(50000.0, cpu_jiffies=_jiffies(user=900, idle=100),
                          net={"pub0": {"rx_bytes": 9000, "tx_bytes": 4000}})
      sample_b = _sample(120.0, cpu_jiffies=_jiffies(user=10, idle=5),
                          net={"pub0": {"rx_bytes": 100, "tx_bytes": 50}})

      delta = compute_counter_delta(sample_a, sample_b)

      assert delta["invalid_reason"] == "reboot_or_reset"
      assert delta["elapsed_sec"] is None
      assert delta["cpu_busy_pct"] is None
      assert delta["network"] == {}
      assert delta["lustre_md_ops"] == {}

   def test_zero_elapsed_is_also_invalid(self):
      # Duplicate/out-of-order sample: no positive denominator exists.
      sample_a = _sample(100.0)
      sample_b = _sample(100.0)

      delta = compute_counter_delta(sample_a, sample_b)

      assert delta["invalid_reason"] == "reboot_or_reset"
      assert delta["elapsed_sec"] is None


class TestCpuJiffyReset:
   def test_normal_cpu_busy_pct(self):
      sample_a = _sample(10.0, cpu_jiffies=_jiffies(user=100, system=50, idle=850))
      sample_b = _sample(20.0, cpu_jiffies=_jiffies(user=150, system=70, idle=880))

      delta = compute_counter_delta(sample_a, sample_b)

      # busy = user+system deltas (50+20=70); total = 70 + idle delta (30) = 100
      assert delta["cpu_busy_pct"] == pytest.approx(70.0)

   def test_iowait_counted_as_not_busy(self):
      # Design: "load average is never called CPU utilization" -- iowait is
      # the CPU sitting idle waiting on I/O, not busy time.
      sample_a = _sample(10.0, cpu_jiffies=_jiffies(user=0, idle=0, iowait=0))
      sample_b = _sample(20.0, cpu_jiffies=_jiffies(user=0, idle=50, iowait=50))

      delta = compute_counter_delta(sample_a, sample_b)

      assert delta["cpu_busy_pct"] == pytest.approx(0.0)

   def test_a_field_decreasing_invalidates_cpu_rate_only(self):
      sample_a = _sample(10.0, cpu_jiffies=_jiffies(user=900, idle=100),
                          net={"pub0": {"rx_bytes": 1000, "tx_bytes": 500}})
      sample_b = _sample(20.0, cpu_jiffies=_jiffies(user=10, idle=200),
                          net={"pub0": {"rx_bytes": 2000, "tx_bytes": 1000}})

      delta = compute_counter_delta(sample_a, sample_b)

      assert delta["cpu_busy_pct"] is None
      # Network is an independent series and must still compute normally --
      # a CPU counter reset must not blind unrelated series.
      assert delta["network"]["pub0"]["rx_bytes_per_sec"] == 100.0


class TestNetworkInterfaceReset:
   def test_normal_two_interfaces(self):
      sample_a = _sample(10.0, net={
         "pub0": {"rx_bytes": 1000, "tx_bytes": 500},
         "hsn0": {"rx_bytes": 2000, "tx_bytes": 900},
      })
      sample_b = _sample(20.0, net={
         "pub0": {"rx_bytes": 1500, "tx_bytes": 600},
         "hsn0": {"rx_bytes": 2500, "tx_bytes": 1100},
      })

      delta = compute_counter_delta(sample_a, sample_b)

      assert delta["network"]["pub0"]["rx_bytes_per_sec"] == 50.0
      assert delta["network"]["hsn0"]["rx_bytes_per_sec"] == 50.0

   def test_one_interface_reset_excludes_only_that_interface(self):
      sample_a = _sample(10.0, net={
         "pub0": {"rx_bytes": 9000, "tx_bytes": 4000},   # will reset
         "hsn0": {"rx_bytes": 2000, "tx_bytes": 900},
      })
      sample_b = _sample(20.0, net={
         "pub0": {"rx_bytes": 100, "tx_bytes": 50},      # counter reset
         "hsn0": {"rx_bytes": 2500, "tx_bytes": 1100},
      })

      delta = compute_counter_delta(sample_a, sample_b)

      assert "pub0" not in delta["network"]
      assert delta["network"]["hsn0"]["rx_bytes_per_sec"] == 50.0

   def test_loopback_excluded(self):
      sample_a = _sample(10.0, net={
         "lo": {"rx_bytes": 100, "tx_bytes": 100},
         "pub0": {"rx_bytes": 1000, "tx_bytes": 500},
      })
      sample_b = _sample(20.0, net={
         "lo": {"rx_bytes": 999999, "tx_bytes": 999999},
         "pub0": {"rx_bytes": 1500, "tx_bytes": 600},
      })

      delta = compute_counter_delta(sample_a, sample_b)

      assert "lo" not in delta["network"]
      assert "pub0" in delta["network"]

   def test_newly_appeared_interface_skipped_not_fabricated(self):
      sample_a = _sample(10.0, net={})
      sample_b = _sample(20.0, net={"pub0": {"rx_bytes": 1000, "tx_bytes": 500}})

      delta = compute_counter_delta(sample_a, sample_b)

      assert delta["network"] == {}


class TestLustreTargetOperationReset:
   def test_normal_delta_per_target_and_operation(self):
      sample_a = _sample(10.0, md_ops={
         "agile-MDT0000": {"intent_lock": 1000, "getattr": 500, "total": 1500},
      })
      sample_b = _sample(20.0, md_ops={
         "agile-MDT0000": {"intent_lock": 1400, "getattr": 600, "total": 2000},
      })

      delta = compute_counter_delta(sample_a, sample_b)

      target = delta["lustre_md_ops"]["agile-MDT0000"]
      assert target["intent_lock"] == 40.0
      assert target["getattr"] == 10.0

   def test_one_operation_reset_excludes_only_that_operation(self):
      sample_a = _sample(10.0, md_ops={
         "agile-MDT0000": {"intent_lock": 9000, "getattr": 500},
      })
      sample_b = _sample(20.0, md_ops={
         "agile-MDT0000": {"intent_lock": 50, "getattr": 600},  # reset
      })

      delta = compute_counter_delta(sample_a, sample_b)

      target = delta["lustre_md_ops"]["agile-MDT0000"]
      assert "intent_lock" not in target
      assert target["getattr"] == 10.0

   def test_unseen_target_skipped_not_fabricated(self):
      sample_a = _sample(10.0, md_ops={})
      sample_b = _sample(20.0, md_ops={
         "agile-MDT0000": {"intent_lock": 100},
      })

      delta = compute_counter_delta(sample_a, sample_b)

      assert delta["lustre_md_ops"] == {}


class TestMissingFields:
   def test_missing_counters_key_entirely(self):
      sample_a = {"uptime_sec": 10.0}
      sample_b = {"uptime_sec": 20.0}

      delta = compute_counter_delta(sample_a, sample_b)

      assert delta["elapsed_sec"] == 10.0
      assert delta["cpu_busy_pct"] is None
      assert delta["network"] == {}
      assert delta["lustre_md_ops"] == {}

   def test_missing_uptime_is_invalid(self):
      sample_a = {"counters": {}}
      sample_b = _sample(20.0)

      delta = compute_counter_delta(sample_a, sample_b)

      assert delta["invalid_reason"] == "missing_uptime"
      assert delta["elapsed_sec"] is None

   def test_partial_network_fields_skipped(self):
      sample_a = _sample(10.0, net={"pub0": {"rx_bytes": 1000}})  # no tx_bytes
      sample_b = _sample(20.0, net={"pub0": {"rx_bytes": 1500, "tx_bytes": 600}})

      delta = compute_counter_delta(sample_a, sample_b)

      assert delta["network"] == {}


# --------------------------------------------------------------------------
# CounterWindowAccumulator -- bounded per-node minute rollup
# --------------------------------------------------------------------------

def _make_accumulator(expected_count=6, probe_version=4):
   return CounterWindowAccumulator(
      system="polaris",
      source_hostname="polaris-login-04.example.org",
      collector_hostname="polaris-login-04.example.org",
      probe_version=probe_version,
      daemon_version="0.1.0",
      window_start_utc="2026-09-09T00:00:00Z",
      window_end_utc="2026-09-09T00:01:00Z",
      expected_count=expected_count,
   )


class TestFirstSampleBaseline:
   def test_single_sample_produces_no_fabricated_rate(self):
      acc = _make_accumulator()
      acc.add_sample(_sample(10.0, cpu_jiffies=_jiffies(user=100, idle=900)))

      record = acc.finalize()

      assert record["sample_count"] == 1
      assert record["rates"]["cpu_busy_pct"] is None
      validate_node_counter_samples(record)

   def test_zero_samples_produces_valid_degraded_record(self):
      acc = _make_accumulator()

      record = acc.finalize()

      assert record["sample_count"] == 0
      assert record["coverage"] == 0.0
      assert record["rates"]["cpu_busy_pct"] is None
      assert record["end_of_window"]["mem_available_kb"] is None
      validate_node_counter_samples(record)


class TestExpectedAndSampleCounts:
   def test_full_window_reports_full_coverage(self):
      acc = _make_accumulator(expected_count=6)
      for i in range(6):
         acc.add_sample(_sample(10.0 * (i + 1)))

      record = acc.finalize()

      assert record["sample_count"] == 6
      assert record["expected_count"] == 6
      assert record["coverage"] == 1.0

   def test_partial_window_reports_partial_coverage(self):
      acc = _make_accumulator(expected_count=6)
      for i in range(3):
         acc.add_sample(_sample(10.0 * (i + 1)))

      record = acc.finalize()

      assert record["sample_count"] == 3
      assert record["coverage"] == pytest.approx(0.5)


class TestMinimumFiveValidity:
   def test_five_samples_meets_minimum(self):
      acc = _make_accumulator(expected_count=6)
      for i in range(5):
         acc.add_sample(_sample(10.0 * (i + 1)))

      record = acc.finalize()

      assert record["audit"]["meets_minimum_samples"] is True

   def test_four_samples_does_not_meet_minimum(self):
      acc = _make_accumulator(expected_count=6)
      for i in range(4):
         acc.add_sample(_sample(10.0 * (i + 1)))

      record = acc.finalize()

      assert record["audit"]["meets_minimum_samples"] is False


class TestEndOfWindowGauges:
   def test_gauges_come_from_last_sample_not_a_delta(self):
      acc = _make_accumulator()
      acc.add_sample(_sample(
         10.0, mem={"total_kb": 1000, "available_kb": 900},
         load1=1.0, procs_running=5))
      acc.add_sample(_sample(
         20.0, mem={"total_kb": 1000, "available_kb": 400},
         load1=9.0, procs_running=50))

      record = acc.finalize()

      assert record["end_of_window"]["mem_available_kb"] == 400
      assert record["end_of_window"]["load1"] == 9.0
      assert record["end_of_window"]["procs_running"] == 50

   def test_missing_mem_field_is_none_not_zero(self):
      acc = _make_accumulator()
      acc.add_sample(_sample(10.0))

      record = acc.finalize()

      assert record["end_of_window"]["mem_available_kb"] is None
      assert record["end_of_window"]["cached_kb"] is None


class TestPercentileRollup:
   def test_p50_p95_max_over_multiple_deltas(self):
      acc = _make_accumulator(expected_count=5)
      # Five samples at 10s cadence, rx_bytes deltas of 100 B/s each pair
      # except one deliberate spike, to give p50/p95/max distinct values.
      rx_values = [0, 1000, 2000, 3500, 9500]
      for i, rx in enumerate(rx_values):
         acc.add_sample(_sample(
            10.0 * (i + 1), net={"pub0": {"rx_bytes": rx, "tx_bytes": 0}}))

      record = acc.finalize()

      rx_stats = record["rates"]["network"]["pub0"]["rx_bytes_per_sec"]
      # Per-pair rx rates: 100, 100, 150, 600 (B/s over 10s windows).
      assert rx_stats["max"] == pytest.approx(600.0)
      assert rx_stats["p50"] == pytest.approx(125.0)
      assert rx_stats["p50"] <= rx_stats["p95"] <= rx_stats["max"]


class TestNoFabricatedRates:
   def test_all_invalid_pairs_yields_null_rates_not_zero(self):
      acc = _make_accumulator(expected_count=2)
      # Every pair is a reboot -- zero valid deltas anywhere.
      acc.add_sample(_sample(50000.0))
      acc.add_sample(_sample(10.0))

      record = acc.finalize()

      assert record["rates"]["cpu_busy_pct"] is None
      assert record["rates"]["network"] == {}
      assert record["rates"]["lustre_md_ops"] == {}
      assert len(record["audit"]["invalid_pairs"]) == 1
      assert record["audit"]["invalid_pairs"][0]["reason"] == "reboot_or_reset"


class TestContractCompliance:
   def test_finalize_output_validates_against_node_counter_samples_contract(self):
      acc = _make_accumulator()
      acc.add_sample(_sample(
         10.0, cpu_jiffies=_jiffies(user=100, idle=900),
         net={"pub0": {"rx_bytes": 1000, "tx_bytes": 500}},
         md_ops={"agile-MDT0000": {"intent_lock": 100}},
         mem={"total_kb": 1000, "available_kb": 900},
         load1=1.0, procs_running=5))
      acc.add_sample(_sample(
         20.0, cpu_jiffies=_jiffies(user=150, idle=950),
         net={"pub0": {"rx_bytes": 1500, "tx_bytes": 600}},
         md_ops={"agile-MDT0000": {"intent_lock": 140}},
         mem={"total_kb": 1000, "available_kb": 850},
         load1=2.0, procs_running=6))

      record = acc.finalize()
      validated = validate_node_counter_samples(record)

      assert validated["system"] == "polaris"
      assert validated["source_hostname"] == "polaris-login-04.example.org"
      assert validated["probe_version"] == 4
      assert validated["daemon_version"] == "0.1.0"
      assert validated["window_start_utc"] == "2026-09-09T00:00:00Z"
      assert validated["window_end_utc"] == "2026-09-09T00:01:00Z"

   def test_probe_version_comes_from_constructor_not_samples(self):
      # A window with zero polls still must produce a valid, contract-
      # compliant record (design: "one row per node per complete 60-second
      # window" -- a totally-failed window is still a row). probe_version
      # cannot be inferred from samples that do not exist, so it is a
      # daemon-known constant supplied at construction time, never
      # defaulted or fabricated from data.
      acc = _make_accumulator(probe_version=4)

      record = acc.finalize()

      assert record["probe_version"] == 4
      validate_node_counter_samples(record)
