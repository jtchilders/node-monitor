import pytest

from node_monitor.pipeline.node_metrics import NODE_METRICS_COLUMNS, MinuteAggregator
from tests.pipeline import at

JIFFY_KEYS = ("user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal")


def counters(t, jiffies, rx=0, md=0, cpu=0.0, ts=None, load1=0.5, tx=0, extra_net=None):
   full = {k: 0 for k in JIFFY_KEYS}
   full.update(jiffies)
   full.update(guest=0, guest_nice=0)
   net = {"hsn0": {"rx_bytes": rx, "tx_bytes": tx}}
   net.update(extra_net or {})
   if ts is None:
      ts = at(t).strftime("%Y-%m-%dT%H:%M:%S.000Z")
   return {
      "type": "counters",
      "seq": t,
      "ts": ts,
      "cpu_jiffies": full,
      "load1": load1,
      "load5": 0.4,
      "load15": 0.3,
      "procs_running": 2,
      "procs_total": 300,
      "socket_count": 77,
      "mem": {
         "total_kb": 1000,
         "available_kb": 400,
         "cached_kb": 100,
         "shmem_kb": 10,
         "free_kb": 50,
         "swap_total_kb": 200,
         "swap_free_kb": 150,
      },
      "net": net,
      "md_ops": {"eagle-MDT0000-mdc-ffff": {"open": 1, "total": md}},
      "probe_self": {
         "cpu_seconds": cpu,
         "children_cpu_seconds": 0.0,
         "maxrss_kb": 5000 + t,
         "rss_kb": 4000,
      },
   }


def test_minute_row_from_six_samples():
   agg = MinuteAggregator(10)
   for i in range(7):
      out = agg.add_counters(
         counters(
            i * 10,
            jiffies={"user": 100 * i, "idle": 300 * i},
            rx=1000 * i,
            md=50 * i,
            cpu=0.1 * i,
         ),
         at(i * 10),
         session_id=1,
      )
   (row,) = out
   assert set(row) == set(NODE_METRICS_COLUMNS)
   assert row["minute"] == at(0) and row["sample_count"] == 6 and row["expected_count"] == 6
   assert row["cpu_pct"]["user"] == 25.0 and row["cpu_pct"]["idle"] == 75.0
   assert row["network_rates"]["hsn0"]["rx_bytes_per_sec"] == 100.0
   assert row["lustre_md_rates"] == {"eagle": 5.0}
   assert row["probe_cpu_seconds"] == pytest.approx(0.5)
   assert row["mem_used_kb"] == 600 and row["swap_used_kb"] == 50
   assert row["probe_maxrss_kb"] == 5050 and row["socket_count"] == 77


def test_counter_reset_skips_pair_without_negative_rates():
   agg = MinuteAggregator(10)
   agg.add_counters(counters(0, {"user": 10_000, "idle": 10_000}, rx=10**9), at(0), 1)
   agg.add_counters(counters(10, {"user": 5, "idle": 5}, rx=10), at(10), 2)
   agg.add_counters(counters(20, {"user": 105, "idle": 305}, rx=1010), at(20), 2)
   (row,) = agg.flush()
   assert row["cpu_pct"]["user"] == 25.0
   assert row["network_rates"]["hsn0"]["rx_bytes_per_sec"] == 100.0
   assert all(v >= 0 for v in row["cpu_pct"].values())


def test_nic_reset_keeps_cpu_and_other_interfaces():
   agg = MinuteAggregator(10)
   other = {"ib0": {"rx_bytes": 0, "tx_bytes": 0}}
   agg.add_counters(counters(0, {"user": 0, "idle": 0}, rx=10**9, extra_net=other), at(0), 1)
   other = {"ib0": {"rx_bytes": 500, "tx_bytes": 0}}
   agg.add_counters(counters(10, {"user": 100, "idle": 300}, rx=5, extra_net=other), at(10), 1)
   (row,) = agg.flush()
   assert row["cpu_pct"]["user"] == 25.0
   assert "hsn0" not in row["network_rates"]
   assert row["network_rates"]["ib0"]["rx_bytes_per_sec"] == 50.0


def test_minute_with_only_skipped_pairs_has_gauges_but_no_rates():
   agg = MinuteAggregator(10)
   agg.add_counters(counters(0, {"user": 100}), at(0), 1)
   agg.add_counters(counters(10, {"user": 5}, load1=3.0), at(10), 2)
   (row,) = agg.flush()
   assert row["cpu_pct"] is None
   assert row["network_rates"] == {} and row["lustre_md_rates"] == {}
   assert row["load1"] == 3.0 and row["sample_count"] == 2


def test_census_gpus_and_scan_seconds_attach_to_minute():
   agg = MinuteAggregator(10)
   agg.add_counters(counters(0, {"user": 0}), at(0), 1)
   agg.add_census({"gpus": [{"idx": 0}], "scan_seconds": 0.2}, at(5))
   agg.add_census({"gpus": [{"idx": 1}], "scan_seconds": 0.7}, at(35))
   agg.add_census({"gpus": [], "scan_seconds": 0.4}, at(45))
   (row,) = agg.add_counters(counters(60, {"user": 0}), at(60), 1)
   assert row["gpus"] == [] and row["sample_latency_sec"] == 0.7
   (row2,) = agg.flush()
   assert row2["gpus"] == [] and row2["sample_latency_sec"] is None


def test_clock_offset_is_median_recv_minus_ts():
   agg = MinuteAggregator(10)
   for t, off in ((0, 1.0), (10, 3.0), (20, 2.0)):
      ts = at(t - off).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
      agg.add_counters(counters(t, {"user": 0}, ts=ts), at(t), 1)
   (row,) = agg.flush()
   assert row["clock_offset_sec"] == 2.0


def test_flush_empty_returns_nothing():
   assert MinuteAggregator(10).flush() == []


def test_session_change_skips_all_families_even_when_counters_increase():
   agg = MinuteAggregator(10)
   agg.add_counters(counters(0, {"user": 0, "idle": 0}, rx=0, md=0, cpu=0.0), at(0), 1)
   agg.add_counters(counters(10, {"user": 100, "idle": 300}, rx=1000, md=50, cpu=1.0), at(10), 2)
   (row,) = agg.flush()
   assert row["cpu_pct"] is None
   assert row["network_rates"] == {} and row["lustre_md_rates"] == {}
   assert row["probe_cpu_seconds"] is None


def test_cpu_only_decrease_keeps_net_and_lustre():
   agg = MinuteAggregator(10)
   agg.add_counters(counters(0, {"user": 100, "idle": 100}, rx=0, md=0), at(0), 1)
   agg.add_counters(counters(10, {"user": 50, "idle": 50}, rx=1000, md=50), at(10), 1)
   (row,) = agg.flush()
   assert row["cpu_pct"] is None
   assert row["network_rates"]["hsn0"]["rx_bytes_per_sec"] == 100.0
   assert row["lustre_md_rates"] == {"eagle": 5.0}


def test_lustre_only_decrease_skips_that_filesystem_only():
   agg = MinuteAggregator(10)
   agg.add_counters(counters(0, {"user": 0, "idle": 0}, rx=0, md=100), at(0), 1)
   agg.add_counters(counters(10, {"user": 100, "idle": 300}, rx=1000, md=50), at(10), 1)
   (row,) = agg.flush()
   assert row["lustre_md_rates"] == {}
   assert row["cpu_pct"]["user"] == 25.0
   assert row["network_rates"]["hsn0"]["rx_bytes_per_sec"] == 100.0


def test_probe_cpu_only_decrease_excludes_that_pair():
   agg = MinuteAggregator(10)
   for t, cpu in ((0, 0.0), (10, 1.0), (20, 0.2), (30, 0.7)):
      agg.add_counters(counters(t, {"user": t * 10, "idle": t * 30}, rx=t * 100, cpu=cpu), at(t), 1)
   (row,) = agg.flush()
   assert row["probe_cpu_seconds"] == pytest.approx(1.5)
   assert row["cpu_pct"]["user"] == 25.0
   assert row["network_rates"]["hsn0"]["rx_bytes_per_sec"] == 100.0


def test_pair_spanning_minutes_counts_toward_later_minute():
   agg = MinuteAggregator(10)
   agg.add_counters(counters(0, {"user": 0, "idle": 0}), at(0), 1)
   agg.add_counters(counters(55, {"user": 100, "idle": 100}), at(55), 1)
   (first,) = agg.add_counters(counters(65, {"user": 400, "idle": 100}), at(65), 1)
   (second,) = agg.flush()
   assert first["minute"] == at(0) and first["cpu_pct"]["user"] == 50.0
   assert second["minute"] == at(60) and second["cpu_pct"]["user"] == 100.0


def test_zero_elapsed_pair_contributes_nothing_and_does_not_crash():
   agg = MinuteAggregator(10)
   agg.add_counters(counters(0, {"user": 0, "idle": 0}, rx=0, md=0), at(0), 1)
   agg.add_counters(counters(0, {"user": 10, "idle": 10}, rx=5000, md=500), at(0), 1)
   agg.add_counters(counters(10, {"user": 110, "idle": 310}, rx=6000, md=550), at(10), 1)
   (row,) = agg.flush()
   assert row["network_rates"]["hsn0"]["rx_bytes_per_sec"] == 100.0
   assert row["lustre_md_rates"] == {"eagle": 5.0}
   assert row["cpu_pct"]["user"] == 25.0
