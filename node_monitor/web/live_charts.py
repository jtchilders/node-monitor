"""Python mirror of live_charts.js logic for pytest."""

PALETTE = [
   "#8b5cf6",
   "#06b6d4",
   "#f59e0b",
   "#ef4444",
   "#3b82f6",
   "#10b981",
   "#ec4899",
   "#14b8a6",
   "#f97316",
   "#a78bfa",
   "#22c55e",
   "#e879f9",
]

CATEGORY_COLORS = {
   "ai_agent": "#8b5cf6",
   "editor_server": "#06b6d4",
   "jupyter": "#f59e0b",
   "build": "#ef4444",
   "python": "#3b82f6",
   "shell": "#6b7280",
   "data_transfer": "#10b981",
   "fs_scan": "#a3a3a3",
   "scheduler": "#ec4899",
   "workflow": "#14b8a6",
   "system": "#52525b",
   "other": "#9ca3af",
}

MAX_SLICES = 15
MAX_BARS = 12


def _group_key(group, proc):
   if group == "category":
      return proc.get("category") or "other"
   if group == "activity":
      return proc.get("activity") or "other"
   if group == "user":
      return proc.get("username") or "unknown"
   if group == "node":
      return proc.get("label") or proc.get("node") or "unknown"
   if group == "presence":
      return proc.get("owner_presence") or "unknown"
   return proc.get(group) or "unknown"


GROUP_DEFS = {
   "category": {"label": "Category"},
   "activity": {"label": "Activity"},
   "user": {"label": "User"},
   "node": {"label": "Node"},
   "presence": {"label": "Presence"},
}


def aggregate_for_group(processes, group):
   if not processes:
      return {"groups": [], "total": 0, "capped": False}
   buckets = {}
   for p in processes:
      k = _group_key(group, p)
      b = buckets.get(k)
      if b is None:
         buckets[k] = {"key": k, "count": 0, "rss": 0, "cpu": 0}
         b = buckets[k]
      b["count"] += 1
      b["rss"] += p.get("rss_kb") or 0
      b["cpu"] += p.get("cpu_seconds") or 0
   groups = sorted(buckets.values(), key=lambda g: (-g["count"], -g["rss"]))
   total = len(processes)
   max_items = MAX_SLICES
   capped = False
   if len(groups) > max_items:
      capped = True
      head = groups[: max_items - 1]
      tail = groups[max_items - 1 :]
      # Merge into existing "other" if present to avoid duplicate other/Other
      existing = next((g for g in head if str(g["key"]).lower() == "other"), None)
      if existing is not None:
         for g in tail:
            existing["count"] += g["count"]
            existing["rss"] += g["rss"]
            existing["cpu"] += g["cpu"]
         existing["groupCount"] = existing.get("groupCount", 0) + len(tail)
         groups = head
      else:
         other = {
            "key": "Other",
            "count": sum(g["count"] for g in tail),
            "rss": sum(g["rss"] for g in tail),
            "cpu": sum(g["cpu"] for g in tail),
         }
         groups = head + [other]
   return {"groups": groups, "total": total, "capped": capped}


def _djb2(s):
   h = 5381
   for ch in s:
      h = ((h << 5) + h + ord(ch)) & 0xFFFFFFFF
   return h


def color_for(group, key, index):
   if group == "category":
      return CATEGORY_COLORS.get(key, "#9ca3af")
   return PALETTE[_djb2(str(key)) % len(PALETTE)]


def build_pie_dataset(agg, group):
   labels = [str(g["key"]) for g in agg["groups"]]
   data = [g["count"] for g in agg["groups"]]
   background_color = [color_for(group, g["key"], i) for i, g in enumerate(agg["groups"])]
   return {"labels": labels, "data": data, "backgroundColor": background_color}


def build_bar_datasets(agg, group):
   labels = [str(g["key"]) for g in agg["groups"]]
   rss_data = [g["rss"] for g in agg["groups"]]
   cpu_data = [g["cpu"] for g in agg["groups"]]
   background_color = [color_for(group, g["key"], i) for i, g in enumerate(agg["groups"])]
   return {
      "labels": labels,
      "rssData": rss_data,
      "cpuData": cpu_data,
      "backgroundColor": background_color,
   }


def build_resource_pie_dataset(agg, group, weight_key):
   if not agg or not agg.get("groups"):
      return {"labels": [], "data": [], "backgroundColor": []}
   labels = [str(g["key"]) for g in agg["groups"]]
   if weight_key == "rss":
      data = [g.get("rss") or 0 for g in agg["groups"]]
   elif weight_key == "cpu":
      data = [g.get("cpu") or 0 for g in agg["groups"]]
   else:
      data = [g.get("count") or 0 for g in agg["groups"]]
   background_color = [color_for(group, g["key"], i) for i, g in enumerate(agg["groups"])]
   return {"labels": labels, "data": data, "backgroundColor": background_color}


def format_resource_label(total_used, total_available, kind):
   if kind == "memory":
      # reuse fmt logic
      def _fmt(kb):
         if kb is None:
            return "-"
         if kb < 1024:
            return f"{kb} KiB"
         mib = kb / 1024
         if mib < 1024:
            return f"{mib:.1f} MiB"
         return f"{mib / 1024:.1f} GiB"

      used_str = _fmt(total_used or 0)
      avail_str = _fmt(total_available or 0)
      pct = 0
      if total_available and total_available > 0:
         pct = round((total_used or 0) / total_available * 100)
      return f"{used_str} used of {avail_str} ({pct}%)"
   if kind == "cpu":
      return f"{round(total_used or 0)}s CPU"
   return str(total_used or 0)


buildResourcePieDataset = build_resource_pie_dataset
formatResourceLabel = format_resource_label

# snake_case aliases matching test imports if needed
aggregateForGroup = aggregate_for_group
colorFor = color_for
buildPieDataset = build_pie_dataset
buildBarDatasets = build_bar_datasets
