"""Per-node facade: probe messages and session events in, storage records out (pure)."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

from node_monitor.config import ClassificationConfig, CollectionConfig, NodeConfig, RedactionConfig
from node_monitor.pipeline.enrich import enrich_census
from node_monitor.pipeline.node_metrics import MinuteAggregator
from node_monitor.pipeline.records import (
   COLLECTION_EVENT,
   COMMAND_LINES,
   LIVE_NODE,
   LIVE_PROCESSES,
   NODE_HARDWARE,
   NODE_METRICS_1M,
   PROCESS_SAMPLES,
   Record,
)
from node_monitor.pipeline.redact import Redactor
from node_monitor.pipeline.tags import tags_for
from node_monitor.pipeline.timestamps import require_aware
from node_monitor.pipeline.usage import UsageAggregator

PROCESS_SAMPLE_COLUMNS = (
   "pid",
   "start_time",
   "username",
   "uid",
   "comm",
   "exe",
   "argv0",
   "cmdline",
   "cmdline_hash",
   "ppid",
   "parent_comm",
   "parent_exe",
   "parent_kind",
   "category",
   "activity",
   "tags",
   "state",
   "threads",
   "cpu_seconds",
   "rss_kb",
   "gpu_mem_mb",
   "interactive",
   "under_screen_or_tmux",
   "cpu_state",
   "idle_for_sec",
   "idle_for_is_lower_bound",
   "idle_bucket",
   "owner_presence",
   "hourly",
)


def _cmdline_hash(cmdline: str) -> str:
   return hashlib.sha256(cmdline.encode()).hexdigest()


class NodePipeline:
   def __init__(
      self,
      *,
      system: str,
      node: NodeConfig,
      collection: CollectionConfig,
      classification: ClassificationConfig,
      redaction: RedactionConfig,
   ):
      self._system = system
      self._node = node
      self._min_user_uid = classification.min_user_uid
      self._notable_cpu = classification.notable_cpu_seconds_per_min
      self._notable_rss_kb = int(classification.notable_rss_mb * 1024)
      self._redactor = Redactor(redaction.extra_patterns)
      self._minutes = MinuteAggregator(collection.counter_interval_sec)
      self._usage = UsageAggregator(collection.summary_interval_sec, collection.census_interval_sec)
      self._summary_sec = collection.summary_interval_sec
      self._session_id = 0
      self._boot_id = None
      self._probe_version = None
      self._last_census_at = None
      # The aggregators fold or drop late samples silently, so lateness is decided here
      # from what has already been emitted.
      self._last_minute = None
      self._last_interval = None

   def _row(self, **fields):
      row = {"system": self._system, "node": self._node.host}
      row.update(fields)
      return row

   def _event(self, event_type, detail, recv):
      return Record(COLLECTION_EVENT, self._row(ts=recv, event_type=event_type, detail=detail))

   def _late(self, msg, recv):
      return [self._event("late_sample", {"type": msg.get("type"), "recv": recv.isoformat()}, recv)]

   def _interval_start(self, recv):
      secs = recv.timestamp()
      return datetime.fromtimestamp(secs - secs % self._summary_sec, tz=timezone.utc)

   def _minute_records(self, rows):
      out = []
      for row in rows:
         self._last_minute = row["minute"]
         out.append(Record(NODE_METRICS_1M, self._row(**row)))
      return out

   def _usage_records(self, items):
      out = []
      for kind, row in items:
         self._last_interval = row["interval_start"]
         out.append(Record(kind, self._row(**row)))
      return out

   def on_message(self, msg: dict, recv: datetime) -> list[Record]:
      require_aware(recv)
      kind = msg.get("type")
      if kind == "hello":
         return self._hello(msg, recv)
      if kind == "counters":
         return self._counters(msg, recv)
      if kind == "census":
         return self._census(msg, recv)
      if kind == "error":
         detail = {"where": msg.get("where"), "message": msg.get("message")}
         return [self._event("probe_error", detail, recv)]
      return []

   def _hello(self, msg, recv):
      self._boot_id = msg.get("boot_id")
      self._probe_version = msg.get("probe_version")
      fields = {
         "boot_id": msg.get("boot_id"),
         "btime": msg.get("btime"),
         "collected_at": recv,
         "probe_version": msg.get("probe_version"),
         "python_version": msg.get("python_version"),
         "hostname_fqdn": msg.get("hostname_fqdn"),
      }
      fields.update(msg.get("hardware") or {})
      out = [Record(NODE_HARDWARE, self._row(**fields))]
      if msg.get("hostname_fqdn") != self._node.host:
         detail = {"expected": self._node.host, "reported": msg.get("hostname_fqdn")}
         out.append(self._event("hostname_mismatch", detail, recv))
      return out

   def _counters(self, msg, recv):
      minute = recv.replace(second=0, microsecond=0)
      if self._last_minute is not None and minute <= self._last_minute:
         return self._late(msg, recv)
      out = self._minute_records(self._minutes.add_counters(msg, recv, self._session_id))
      mem = msg.get("mem") or {}
      total, avail = mem.get("total_kb"), mem.get("available_kb")
      out.append(
         self._live_node(
            recv,
            "up",
            boot_id=self._boot_id,
            probe_version=self._probe_version,
            load1=msg.get("load1"),
            mem_used_kb=None if total is None or avail is None else total - avail,
            mem_total_kb=total,
            procs_total=msg.get("procs_total"),
            last_census_at=self._last_census_at,
         )
      )
      return out

   def _live_node(self, recv, state, **extra):
      return Record(
         LIVE_NODE,
         self._row(label=self._node.label, updated_at=recv, session_state=state, **extra),
      )

   def _is_notable(self, proc: dict) -> bool:
      cpu = proc.get("cpu_seconds")
      if cpu is not None and cpu >= self._notable_cpu:
         return True
      rss = proc.get("rss_kb")
      if rss is not None and rss >= self._notable_rss_kb:
         return True
      if proc.get("category") == "ai_agent":
         return True
      if proc.get("tags"):
         return True
      return False

   def _census(self, msg, recv):
      rows = msg.get("processes")
      if rows is None:
         # Summary-mode census (soak replay): gpu/scan metrics only.
         minute = recv.replace(second=0, microsecond=0)
         if self._last_minute is not None and minute <= self._last_minute:
            return self._late(msg, recv)
         self._last_census_at = recv
         self._minutes.add_census(msg, recv)
         return []
      start = self._interval_start(recv)
      if self._last_interval is not None and start <= self._last_interval:
         return self._late(msg, recv)
      self._last_census_at = recv
      gpu_status = msg.get("gpu_status")
      enriched = enrich_census(
         rows, redactor=self._redactor, min_user_uid=self._min_user_uid, gpu_status=gpu_status
      )
      # Add cmdline_hash and tags to enriched rows.
      for proc in enriched:
         cmdline = proc.get("cmdline") or ""
         proc["cmdline_hash"] = _cmdline_hash(cmdline)
         argv = cmdline.split() if cmdline else []
         proc["tags"] = tags_for(proc.get("activity"), argv)
      out = [Record(LIVE_PROCESSES, self._row(ts=recv, processes=enriched))]
      out += self._usage_records(self._usage.add_census(enriched, recv, gpu_status))
      self._minutes.add_census(msg, recv)
      # History: notable + hourly samples and command_lines sightings.
      out += self._history_records(enriched, recv)
      return out

   def _history_records(self, enriched: list[dict], recv: datetime) -> list[Record]:
      hourly = recv.minute == 0
      samples: list[Record] = []
      for proc in enriched:
         is_hourly = hourly and bool(proc.get("is_user_process"))
         if not (self._is_notable(proc) or is_hourly):
            continue
         row = self._row(ts=recv, hourly=is_hourly)
         for col in PROCESS_SAMPLE_COLUMNS:
            if col == "hourly":
               continue
            row[col] = proc.get(col)
         row["hourly"] = is_hourly
         samples.append(Record(PROCESS_SAMPLES, row))
      # command_lines: per distinct (username, cmdline_hash)
      seen: set[tuple] = set()
      cmdlines: list[Record] = []
      for proc in enriched:
         key = (proc.get("username"), proc.get("cmdline_hash"))
         if key in seen:
            continue
         seen.add(key)
         cmdlines.append(
            Record(
               COMMAND_LINES,
               self._row(
                  username=proc.get("username"),
                  cmdline_hash=proc.get("cmdline_hash"),
                  cmdline=proc.get("cmdline"),
                  exe=proc.get("exe"),
                  activity=proc.get("activity"),
                  category=proc.get("category"),
                  tags=proc.get("tags"),
                  first_seen=recv,
                  last_seen=recv,
                  sample_count=1,
                  nodes=[self._node.host],
               ),
            )
         )
      return samples + cmdlines

   def on_event(self, kind: str, detail: dict, recv: datetime) -> list[Record]:
      require_aware(recv)
      if kind == "session_started":
         self._session_id += 1
      out = [self._event(kind, dict(detail), recv)]
      if kind == "session_lost":
         # Only the state changes: the last known gauges stay as they were.
         out.append(self._live_node(recv, "down"))
      return out

   def flush(self) -> list[Record]:
      return self._minute_records(self._minutes.flush()) + self._usage_records(self._usage.flush())
