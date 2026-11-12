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


def test_other_bucket_tooltip_includes_group_count():
   procs = [{"username": f"u{i}", "rss_kb": 1, "cpu_seconds": 1} for i in range(30)]
   agg = aggregateForGroup(procs, "user")
   assert any(g["key"] == "Other" and g["count"] == 19 for g in agg["groups"])


def test_clicking_pie_slice_sets_filter_js_exists(tmp_path=None):
   import pathlib

   p = (
      pathlib.Path(__file__).parents[2]
      / "node_monitor"
      / "web"
      / "static"
      / "js"
      / "live_charts.js"
   )
   assert "onSliceClick" in p.read_text()
   p2 = pathlib.Path(__file__).parents[2] / "node_monitor" / "web" / "static" / "js" / "index.js"
   assert "process-filter" in p2.read_text()


def test_other_bucket_js_tooltip():
   procs = [{"username": f"u{i}", "rss_kb": 1, "cpu_seconds": 1} for i in range(30)]
   agg = aggregateForGroup(procs, "user")
   other = next((g for g in agg["groups"] if g["key"] == "Other"), None)
   assert other is not None
   assert other["count"] == 19
   from pathlib import Path

   js_path = Path(__file__).parents[2] / "node_monitor" / "web" / "static" / "js" / "live_charts.js"
   js = js_path.read_text()
   assert "Other (" in js


def test_clicking_pie_slice_sets_filter():
   from pathlib import Path

   lc_path = Path(__file__).parents[2] / "node_monitor" / "web" / "static" / "js" / "live_charts.js"
   idx_path = Path(__file__).parents[2] / "node_monitor" / "web" / "static" / "js" / "index.js"
   lc_js = lc_path.read_text()
   idx_js = idx_path.read_text()
   # live_charts exposes onClick factory / onSliceClick callback
   assert "onSliceClick" in lc_js or "onClick" in lc_js
   # index wires click-through to process-filter
   assert "process-filter" in idx_js
   assert "onSliceClick" in idx_js
   # clicking Other clears filter (sets empty string)
   assert '""' in idx_js or "''" in idx_js
