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


def test_unmeasured_gpu_and_null_username():
   agg = UsageAggregator(300, 60)
   agg.add_census(
      [
         e(username=None, cpu_seconds=None, gpu_mem_mb=None),
         e(username=None, cpu_seconds=3.0, gpu_mem_mb=200, threads=4),
      ],
      at(10),
   )
   user = by_kind(agg.flush())["user_usage_5m"][0]
   assert user["username"] is None
   assert user["unmeasured_count"] == 1 and user["cpu_seconds"] == 3.0
   assert user["gpu_mem_mb_avg"] == 200 and user["gpu_mem_mb_max"] == 200
   assert user["threads_avg"] == 5.0


def test_intervals_are_epoch_aligned_and_close_on_later_census():
   agg = UsageAggregator(300, 60)
   agg.add_census([e()], at(299))
   out = agg.add_census([e()], at(300))
   starts = {r["interval_start"] for _, r in out}
   assert starts == {at(0)}
   out = agg.flush()
   assert {r["interval_start"] for _, r in out} == {at(300)}
