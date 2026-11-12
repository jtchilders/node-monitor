from node_monitor.web.live_charts import (
   aggregateForGroup,
   buildPieDataset,
   colorFor,
)


def test_aggregate_empty_returns_empty():
   assert aggregateForGroup([], "category") == {"groups": [], "total": 0, "capped": False}


def test_aggregate_counts_and_sums():
   procs = [
      {"category": "ai_agent", "rss_kb": 100, "cpu_seconds": 1.0},
      {"category": "ai_agent", "rss_kb": 50, "cpu_seconds": 2.0},
      {"category": "python", "rss_kb": 10, "cpu_seconds": 0.1},
   ]
   agg = aggregateForGroup(procs, "category")
   assert (
      agg["groups"][0]["key"] == "ai_agent"
      and agg["groups"][0]["count"] == 2
      and agg["groups"][0]["rss"] == 150
   )


def test_aggregate_caps_at_12_with_other():
   procs = [{"username": f"u{i}", "rss_kb": 1, "cpu_seconds": 1} for i in range(20)]
   agg = aggregateForGroup(procs, "user")
   assert len(agg["groups"]) == 12 and agg["groups"][-1]["key"] == "Other" and agg["capped"] is True


def test_colorFor_category_reuses_CATEGORY_COLORS():
   assert colorFor("category", "ai_agent", 0) == "#8b5cf6"


def test_colorFor_unknown_group_is_deterministic():
   assert colorFor("user", "alice", 3) == colorFor("user", "alice", 99)


def test_buildPieDataset_uses_text_labels():
   agg = aggregateForGroup(
      [{"category": "<img onerror=alert(1)>", "rss_kb": 1, "cpu_seconds": 1}], "category"
   )
   ds = buildPieDataset(agg, "category")
   assert "<img" in ds["labels"][0]
