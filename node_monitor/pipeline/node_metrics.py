"""Counters messages -> per-minute node_metrics_1m rows (pure; time comes from `recv`)."""

from datetime import datetime, timezone
from statistics import median

from node_monitor.pipeline.timestamps import require_aware

NODE_METRICS_COLUMNS = (
   "minute",
   "cpu_pct",
   "load1",
   "load5",
   "load15",
   "procs_running",
   "procs_total",
   "socket_count",
   "mem_total_kb",
   "mem_available_kb",
   "mem_cached_kb",
   "mem_shmem_kb",
   "mem_used_kb",
   "swap_used_kb",
   "network_rates",
   "lustre_md_rates",
   "gpus",
   "gpu_status",
   "sample_latency_sec",
   "clock_offset_sec",
   "probe_cpu_seconds",
   "probe_maxrss_kb",
   "probe_rss_kb",
   "sample_count",
   "expected_count",
)

# guest/guest_nice are already counted inside user/nice, so they are left out of the total.
CPU_KEYS = ("user", "nice", "system", "iowait", "irq", "softirq", "steal", "idle")


# Census minutes held while waiting for counters to open them.
_MAX_PENDING_CENSUS_MINUTES = 10


def _floor_minute(recv):
   return recv.replace(second=0, microsecond=0)


def _parse_ts(ts):
   return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)


def _fs_totals(md_ops):
   """{fs: {target: total}}; fs is the target name before '-MDT'."""
   out = {}
   for target, ops in (md_ops or {}).items():
      fs = target.split("-MDT")[0]
      out.setdefault(fs, {})[target] = ops.get("total", 0)
   return out


class _Minute:
   def __init__(self, minute):
      self.minute = minute
      self.last = None
      self.sample_count = 0
      self.offsets = []
      self.cpu = {k: 0 for k in CPU_KEYS}
      self.cpu_pairs = 0
      self.net = {}  # iface -> [rx_delta, tx_delta, elapsed]
      self.md = {}  # fs -> [delta, elapsed]
      self.probe_cpu = 0.0
      self.probe_pairs = 0
      self.probe_rss = None


class MinuteAggregator:
   def __init__(self, counter_interval_sec):
      self._expected = round(60 / counter_interval_sec)
      self._open = None
      self._prev = None  # (msg, recv, session_id)
      self._census = {}  # minute -> [gpus, max scan_seconds, gpu_status]
      self._last_closed = None

   def add_census(self, msg, recv):
      minute = _floor_minute(require_aware(recv))
      if self._open is not None and minute < self._open.minute:
         return
      if self._last_closed is not None and minute <= self._last_closed:
         return
      slot = self._census.setdefault(minute, [[], None, None])
      slot[0] = msg.get("gpus") or []
      scan = msg.get("scan_seconds")
      if scan is not None and (slot[1] is None or scan > slot[1]):
         slot[1] = scan
      slot[2] = msg.get("gpu_status")
      # Censuses only leave this map when their minute closes, which needs counters; a
      # census-only stream would otherwise grow it forever. The newest minutes are kept
      # because counters, when they come, are for the present.
      while len(self._census) > _MAX_PENDING_CENSUS_MINUTES:
         del self._census[min(self._census)]

   def add_counters(self, msg, recv, session_id):
      rows = []
      minute = _floor_minute(require_aware(recv))
      if self._open is not None and minute > self._open.minute:
         rows.append(self._close())
      # A receive clock stepping backwards folds into the open minute rather than
      # emitting an earlier row.
      if self._open is None:
         self._open = _Minute(minute)
      cur = self._open
      cur.sample_count += 1
      cur.last = msg
      rss = (msg.get("probe_self") or {}).get("rss_kb")
      if rss is not None:
         cur.probe_rss = rss
      try:
         cur.offsets.append((recv - _parse_ts(msg["ts"])).total_seconds())
      except (KeyError, TypeError, ValueError):
         pass
      prev = self._prev
      self._prev = (msg, recv, session_id)
      if prev is not None and prev[2] == session_id:
         elapsed = (recv - prev[1]).total_seconds()
         if elapsed > 0:
            self._add_pair(cur, prev[0], msg, elapsed)
      return rows

   def flush(self):
      if self._open is None:
         return []
      return [self._close()]

   def _add_pair(self, cur, a, b, elapsed):
      # Each counter family is checked on its own so one reset (e.g. a NIC) does not
      # discard the others.
      ja, jb = a.get("cpu_jiffies") or {}, b.get("cpu_jiffies") or {}
      if all(k in ja and k in jb for k in CPU_KEYS):
         d = {k: jb[k] - ja[k] for k in CPU_KEYS}
         if min(d.values()) >= 0:
            for k in CPU_KEYS:
               cur.cpu[k] += d[k]
            cur.cpu_pairs += 1

      na, nb = a.get("net") or {}, b.get("net") or {}
      for iface in na.keys() & nb.keys():
         drx = nb[iface]["rx_bytes"] - na[iface]["rx_bytes"]
         dtx = nb[iface]["tx_bytes"] - na[iface]["tx_bytes"]
         if drx < 0 or dtx < 0:
            continue
         acc = cur.net.setdefault(iface, [0, 0, 0.0])
         acc[0] += drx
         acc[1] += dtx
         acc[2] += elapsed

      fa, fb = _fs_totals(a.get("md_ops")), _fs_totals(b.get("md_ops"))
      for fs in fa.keys() & fb.keys():
         targets = fa[fs].keys() & fb[fs].keys()
         deltas = [fb[fs][t] - fa[fs][t] for t in targets]
         if not deltas or min(deltas) < 0:
            continue
         acc = cur.md.setdefault(fs, [0, 0.0])
         acc[0] += sum(deltas)
         acc[1] += elapsed

      pa, pb = a.get("probe_self") or {}, b.get("probe_self") or {}
      try:
         ca = pa["cpu_seconds"] + pa["children_cpu_seconds"]
         cb = pb["cpu_seconds"] + pb["children_cpu_seconds"]
      except KeyError:
         return
      if cb >= ca:
         cur.probe_cpu += cb - ca
         cur.probe_pairs += 1

   def _close(self):
      cur, self._open = self._open, None
      self._last_closed = cur.minute
      last = cur.last
      mem = last.get("mem") or {}
      total = sum(cur.cpu.values())
      cpu_pct = None
      if cur.cpu_pairs and total > 0:
         cpu_pct = {k: round(100.0 * cur.cpu[k] / total, 2) for k in CPU_KEYS}
      gpus, latency, gpu_status = self._census.pop(cur.minute, [[], None, None])
      for stale in [m for m in self._census if m < cur.minute]:
         del self._census[stale]
      probe = last.get("probe_self") or {}

      def diff(a, b):
         return None if a is None or b is None else a - b

      return {
         "minute": cur.minute,
         "cpu_pct": cpu_pct,
         "load1": last.get("load1"),
         "load5": last.get("load5"),
         "load15": last.get("load15"),
         "procs_running": last.get("procs_running"),
         "procs_total": last.get("procs_total"),
         "socket_count": last.get("socket_count"),
         "mem_total_kb": mem.get("total_kb"),
         "mem_available_kb": mem.get("available_kb"),
         "mem_cached_kb": mem.get("cached_kb"),
         "mem_shmem_kb": mem.get("shmem_kb"),
         "mem_used_kb": diff(mem.get("total_kb"), mem.get("available_kb")),
         "swap_used_kb": diff(mem.get("swap_total_kb"), mem.get("swap_free_kb")),
         # Sorted so stored maps have a stable key order, whatever order sets iterate in.
         "network_rates": {
            iface: {"rx_bytes_per_sec": rx / el, "tx_bytes_per_sec": tx / el}
            for iface, (rx, tx, el) in sorted(cur.net.items())
         },
         "lustre_md_rates": {fs: n / el for fs, (n, el) in sorted(cur.md.items())},
         "gpus": gpus,
         "gpu_status": gpu_status,
         "sample_latency_sec": latency,
         "clock_offset_sec": round(median(cur.offsets), 3) if cur.offsets else None,
         "probe_cpu_seconds": cur.probe_cpu if cur.probe_pairs else None,
         # ru_maxrss drifts tens of MiB on many-core kernels; rss_kb (smaps_rollup Rss on
         # kernels >= 4.14) is exact, so it is the one to judge probe memory by.
         "probe_maxrss_kb": probe.get("maxrss_kb"),
         "probe_rss_kb": cur.probe_rss,
         "sample_count": cur.sample_count,
         "expected_count": self._expected,
      }
