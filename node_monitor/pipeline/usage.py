"""Roll enriched per-census process rows up into 5-minute per-user and per-node summaries."""

from __future__ import annotations

from datetime import datetime, timezone

_MAPS = ("by_category", "by_activity", "by_dormancy")

_COMMON_COLUMNS = (
   "interval_start",
   "process_count_avg",
   "process_count_max",
   "cpu_seconds",
   "rss_kb_avg",
   "rss_kb_max",
   "gpu_mem_mb_avg",
   "gpu_mem_mb_max",
   "threads_avg",
   "d_state_fraction",
   "interactive_fraction",
   "by_category",
   "by_activity",
   "by_dormancy",
   "by_category_dormancy",
   "sample_count",
   "expected_count",
   "unmeasured_count",
)

USER_USAGE_COLUMNS = ("username",) + _COMMON_COLUMNS
NODE_USAGE_COLUMNS = _COMMON_COLUMNS + (
   "user_count_avg",
   "user_count_max",
   "active_user_count_avg",
)


def _bump(table, key, cpu_s, rss_kb):
   cell = table.get(key)
   if cell is None:
      cell = table[key] = [0, 0.0, 0]
   cell[0] += 1
   cell[1] += cpu_s
   cell[2] += rss_kb


def _merge(into, other):
   for key, (count, cpu_s, rss_kb) in other.items():
      cell = into.get(key)
      if cell is None:
         cell = into[key] = [0, 0.0, 0]
      cell[0] += count
      cell[1] += cpu_s
      cell[2] += rss_kb


class _Tally:
   """One entity's (a user's or the node's) totals over the censuses of an interval."""

   def __init__(self):
      self.n = self.unmeasured = self.rss = self.threads = self.d = self.inter = 0
      self.cpu = 0.0
      self.gpu = 0
      self.n_max = self.rss_max = self.gpu_max = 0
      self.maps = {name: {} for name in _MAPS}
      self.cat_dorm = {}

   def fold(self, census):
      """Add one census's tally. A census the entity is missing from adds nothing, so its
      zero is counted by the caller dividing by the interval's sample_count."""
      self.n += census.n
      self.cpu += census.cpu
      self.unmeasured += census.unmeasured
      self.rss += census.rss
      self.gpu += census.gpu
      self.threads += census.threads
      self.d += census.d
      self.inter += census.inter
      self.n_max = max(self.n_max, census.n)
      self.rss_max = max(self.rss_max, census.rss)
      self.gpu_max = max(self.gpu_max, census.gpu)
      for name in _MAPS:
         _merge(self.maps[name], census.maps[name])
      for category, table in census.cat_dorm.items():
         _merge(self.cat_dorm.setdefault(category, {}), table)

   def row(self, start, samples, expected):
      def cell(c):
         return {"count": c[0] / samples, "cpu_s": c[1], "rss_kb": c[2] / samples}

      row = {
         "interval_start": start,
         "process_count_avg": self.n / samples,
         "process_count_max": self.n_max,
         "cpu_seconds": self.cpu,
         "rss_kb_avg": self.rss / samples,
         "rss_kb_max": self.rss_max,
         "gpu_mem_mb_avg": self.gpu / samples,
         "gpu_mem_mb_max": self.gpu_max,
         "threads_avg": self.threads / samples,
         "d_state_fraction": self.d / self.n if self.n else 0.0,
         "interactive_fraction": self.inter / self.n if self.n else 0.0,
      }
      for name in _MAPS:
         row[name] = {k: cell(c) for k, c in sorted(self.maps[name].items())}
      row["by_category_dormancy"] = {
         cat: {k: cell(c) for k, c in sorted(table.items())}
         for cat, table in sorted(self.cat_dorm.items())
      }
      row["sample_count"] = samples
      row["expected_count"] = expected
      row["unmeasured_count"] = self.unmeasured
      return row


class _CensusTally(_Tally):
   """Totals for a single census; `max` fields are then just that census's values."""

   def add(self, p):
      cpu_seconds = p["cpu_seconds"]
      cpu = cpu_seconds if cpu_seconds is not None else 0.0
      rss = p["rss_kb"]
      self.n += 1
      if cpu_seconds is None:
         self.unmeasured += 1
      self.cpu += cpu
      self.rss += rss
      if p["gpu_mem_mb"] is not None:
         self.gpu += p["gpu_mem_mb"]
      self.threads += p["threads"]
      if p["state"] == "D":
         self.d += 1
      if p["interactive"]:
         self.inter += 1
      dormancy = f"{p['cpu_state']}|{p['idle_bucket']}|{p['owner_presence']}"
      category = p["category"]
      _bump(self.maps["by_category"], category, cpu, rss)
      _bump(self.maps["by_activity"], p["activity"] or "unknown", cpu, rss)
      _bump(self.maps["by_dormancy"], dormancy, cpu, rss)
      _bump(self.cat_dorm.setdefault(category, {}), dormancy, cpu, rss)


class _Interval:
   def __init__(self, start):
      self.start = start
      self.samples = 0
      self.node = _Tally()
      self.users = {}
      self.user_sum = self.user_max = self.active_user_sum = 0

   def add(self, rows):
      node = _CensusTally()
      users = {}
      active = set()
      for p in rows:
         if not p["is_user_process"]:
            continue
         node.add(p)
         name = p["username"]
         users.setdefault(name, _CensusTally()).add(p)
         if p["cpu_state"] == "active":
            active.add(name)
      self.samples += 1
      self.node.fold(node)
      for name, tally in users.items():
         self.users.setdefault(name, _Tally()).fold(tally)
      self.user_sum += len(users)
      self.user_max = max(self.user_max, len(users))
      self.active_user_sum += len(active)


class UsageAggregator:
   """Emits user_usage_5m / node_usage_5m rows when a census arrives in a later interval.

   Averages divide by the interval's census count, so a user missing from some censuses
   counts as zero in those; a user missing from all of them gets no row.
   """

   def __init__(self, summary_interval_sec: float, census_interval_sec: float):
      self._interval = summary_interval_sec
      self._expected = summary_interval_sec / census_interval_sec
      self._open = None

   def _floor(self, recv: datetime) -> datetime:
      secs = recv.timestamp()
      return datetime.fromtimestamp(secs - secs % self._interval, tz=timezone.utc)

   def add_census(self, rows: list[dict], recv: datetime) -> list[tuple[str, dict]]:
      start = self._floor(recv)
      out = []
      if self._open is not None and start > self._open.start:
         out = self.flush()
      if self._open is None:
         self._open = _Interval(start)
      self._open.add(rows)
      return out

   def flush(self) -> list[tuple[str, dict]]:
      iv, self._open = self._open, None
      if iv is None:
         return []
      out = []
      for name, tally in sorted(iv.users.items(), key=lambda kv: (kv[0] is None, kv[0] or "")):
         row = {"username": name}
         row.update(tally.row(iv.start, iv.samples, self._expected))
         out.append(("user_usage_5m", row))
      node = iv.node.row(iv.start, iv.samples, self._expected)
      node["user_count_avg"] = iv.user_sum / iv.samples
      node["user_count_max"] = iv.user_max
      node["active_user_count_avg"] = iv.active_user_sum / iv.samples
      out.append(("node_usage_5m", node))
      return out
