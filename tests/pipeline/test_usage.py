import pytest

from node_monitor.pipeline.usage import NODE_USAGE_COLUMNS, USER_USAGE_COLUMNS, UsageAggregator
from tests.pipeline import at


def e(**overrides):
   row = {
      "pid": 1,
      "username": "alice",
      "uid": 1001,
      "category": "other",
      "activity": None,
      "state": "S",
      "threads": 1,
      "rss_kb": 100,
      "cpu_seconds": 1.0,
      "gpu_mem_mb": None,
      "interactive": False,
      "cpu_state": "active",
      "idle_bucket": "<5m",
      "owner_presence": "absent",
      "is_user_process": True,
   }
   row.update(overrides)
   return row


def by_kind(out):
   res = {}
   for kind, row in out:
      res.setdefault(kind, []).append(row)
   return res


def test_user_absent_from_some_censuses_averages_zero():
   agg = UsageAggregator(300, 60)
   rows_a = [e(username="alice", rss_kb=1000, cpu_seconds=2.0)]
   for i in range(5):
      agg.add_census(rows_a if i < 2 else [], at(60 * i))
   out = dict((k, r) for k, r in agg.add_census([], at(300)) if k == "user_usage_5m")
   row = out["user_usage_5m"]
   assert row["username"] == "alice" and row["sample_count"] == 5 and row["expected_count"] == 5
   assert row["process_count_avg"] == 0.4 and row["process_count_max"] == 1
   assert row["rss_kb_avg"] == 400 and row["rss_kb_max"] == 1000 and row["cpu_seconds"] == 4.0


def test_maps_and_fractions():
   rows = [
      e(
         username="bob",
         category="ai_agent",
         activity="claude-code",
         cpu_state="idle",
         idle_bucket="1h-1d",
         owner_presence="absent",
         state="S",
         rss_kb=500,
         cpu_seconds=0.0,
      ),
      e(
         username="bob",
         category="python",
         activity=None,
         cpu_state="active",
         idle_bucket="<5m",
         owner_presence="absent",
         state="D",
         interactive=True,
         rss_kb=300,
         cpu_seconds=30.0,
      ),
   ]
   agg = UsageAggregator(300, 60)
   agg.add_census(rows, at(0))
   user = dict(agg.flush())["user_usage_5m"]
   assert user["by_category"]["ai_agent"] == {"count": 1.0, "cpu_s": 0.0, "rss_kb": 500.0}
   assert user["by_activity"]["unknown"]["count"] == 1.0
   assert user["by_dormancy"]["idle|1h-1d|absent"]["rss_kb"] == 500.0
   assert user["by_category_dormancy"]["ai_agent"]["idle|1h-1d|absent"]["count"] == 1.0
   assert user["d_state_fraction"] == 0.5 and user["interactive_fraction"] == 0.5


def test_node_row_counts_users_and_active_users():
   agg = UsageAggregator(300, 60)
   agg.add_census(
      [
         e(username="alice"),
         e(username="alice", cpu_state="idle"),
         e(username="bob", cpu_state="idle"),
      ],
      at(0),
   )
   agg.add_census([e(username="alice", cpu_state="idle")], at(60))
   res = by_kind(agg.flush())
   node = res["node_usage_5m"][0]
   assert "username" not in node
   assert set(node) == set(NODE_USAGE_COLUMNS)
   assert node["user_count_avg"] == 1.5 and node["user_count_max"] == 2
   assert node["active_user_count_avg"] == 0.5
   assert node["process_count_avg"] == 2.0 and node["process_count_max"] == 3
   assert node["sample_count"] == 2
   assert len(res["user_usage_5m"]) == 2
   assert set(res["user_usage_5m"][0]) == set(USER_USAGE_COLUMNS)


def test_system_processes_are_excluded():
   agg = UsageAggregator(300, 60)
   agg.add_census([e(username="root", uid=0, is_user_process=False, rss_kb=9999)], at(0))
   res = by_kind(agg.flush())
   assert "user_usage_5m" not in res
   assert res["node_usage_5m"][0]["process_count_max"] == 0
   assert res["node_usage_5m"][0]["rss_kb_max"] == 0


def test_partial_interval_on_flush_has_reduced_sample_count():
   agg = UsageAggregator(300, 60)
   assert agg.add_census([e()], at(0)) == []
   assert agg.add_census([e()], at(60)) == []
   res = by_kind(agg.flush())
   assert res["user_usage_5m"][0]["sample_count"] == 2
   assert res["user_usage_5m"][0]["expected_count"] == 5
   assert res["user_usage_5m"][0]["interval_start"] == at(0)
   assert agg.flush() == []


def test_unmeasured_cpu_and_unresolved_owners_stay_apart():
   agg = UsageAggregator(300, 60)
   agg.add_census(
      [
         e(username="uid:5001", uid=5001, cpu_seconds=None, gpu_mem_mb=0.0),
         e(username="uid:5001", uid=5001, cpu_seconds=3.0, gpu_mem_mb=200, threads=4),
         e(username="uid:5002", uid=5002, cpu_seconds=1.0, gpu_mem_mb=0.0),
      ],
      at(10),
      gpu_status="ok",
   )
   users = {r["username"]: r for r in by_kind(agg.flush())["user_usage_5m"]}
   assert set(users) == {"uid:5001", "uid:5002"}
   user = users["uid:5001"]
   assert user["unmeasured_count"] == 1 and user["cpu_seconds"] == 3.0
   assert user["gpu_mem_mb_avg"] == 200 and user["gpu_mem_mb_max"] == 200
   assert user["threads_avg"] == 5.0
   assert users["uid:5002"]["process_count_max"] == 1


def test_gpu_memory_is_null_when_no_census_measured_it():
   agg = UsageAggregator(300, 60)
   agg.add_census([e(gpu_mem_mb=None)], at(0), gpu_status="error")
   agg.add_census([e(gpu_mem_mb=None)], at(60), gpu_status="suspended")
   agg.add_census([e(gpu_mem_mb=None)], at(120))
   res = by_kind(agg.flush())
   for row in (res["user_usage_5m"][0], res["node_usage_5m"][0]):
      assert row["gpu_mem_mb_avg"] is None and row["gpu_mem_mb_max"] is None
      assert row["sample_count"] == 3


def test_gpu_memory_averages_over_measured_censuses_only():
   agg = UsageAggregator(300, 60)
   agg.add_census([e(gpu_mem_mb=100)], at(0), gpu_status="ok")
   agg.add_census([e(gpu_mem_mb=None)], at(60), gpu_status="error")
   agg.add_census([e(gpu_mem_mb=300)], at(120), gpu_status="ok")
   # A user missing from a measured census counts as zero there.
   agg.add_census([e(username="bob", gpu_mem_mb=0.0)], at(180), gpu_status="ok")
   res = by_kind(agg.flush())
   alice = next(r for r in res["user_usage_5m"] if r["username"] == "alice")
   assert alice["gpu_mem_mb_avg"] == 400 / 3 and alice["gpu_mem_mb_max"] == 300
   node = res["node_usage_5m"][0]
   assert node["gpu_mem_mb_avg"] == 400 / 3 and node["gpu_mem_mb_max"] == 300


def test_expected_count_is_an_int():
   for census_sec, expected in ((60, 5), (70, 4)):
      agg = UsageAggregator(300, census_sec)
      agg.add_census([e()], at(0))
      for _, row in agg.flush():
         assert row["expected_count"] == expected and type(row["expected_count"]) is int


def test_category_absent_from_some_censuses_divides_by_sample_count():
   censuses = [
      [
         e(category="python", rss_kb=100, cpu_seconds=1.0, state="D", interactive=True),
         e(category="python", rss_kb=200, cpu_seconds=2.0),
         e(category="shell", rss_kb=50, cpu_seconds=0.5, state="D"),
      ],
      [e(category="python", rss_kb=300, cpu_seconds=3.0)],
      [
         e(category="shell", rss_kb=70, cpu_seconds=1.0, interactive=True),
         e(category="build", rss_kb=40, cpu_seconds=4.0, state="D"),
      ],
   ]
   agg = UsageAggregator(300, 60)
   for i, rows in enumerate(censuses):
      agg.add_census(rows, at(60 * i))
   res = by_kind(agg.flush())
   for row in (res["user_usage_5m"][0], res["node_usage_5m"][0]):
      assert row["sample_count"] == 3
      cats = row["by_category"]
      assert cats["python"] == {"count": 1.0, "cpu_s": 6.0, "rss_kb": 200.0}
      assert cats["shell"] == {
         "count": pytest.approx(2 / 3),
         "cpu_s": 1.5,
         "rss_kb": pytest.approx(40.0),
      }
      assert cats["build"] == {
         "count": pytest.approx(1 / 3),
         "cpu_s": 4.0,
         "rss_kb": pytest.approx(40 / 3),
      }
      # Weighted by process samples (3 of 6 in D, 2 of 6 interactive), not a per-census
      # average of fractions (7/18 and 5/18).
      assert row["d_state_fraction"] == 0.5
      assert row["interactive_fraction"] == pytest.approx(1 / 3)


def test_intervals_are_epoch_aligned_and_close_on_later_census():
   agg = UsageAggregator(300, 60)
   agg.add_census([e()], at(299))
   out = agg.add_census([e()], at(300))
   starts = {r["interval_start"] for _, r in out}
   assert starts == {at(0)}
   out = agg.flush()
   assert {r["interval_start"] for _, r in out} == {at(300)}
