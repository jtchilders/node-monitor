"""Two nodes, six processes over three users (plus root) and a heartbeat, written the way the
daemon writes them."""

from datetime import datetime, timedelta, timezone

from node_monitor.config import RetentionConfig
from node_monitor.database.connect import connect
from node_monitor.database.maintenance import write_heartbeat
from node_monitor.database.writer import DatabaseWriter
from node_monitor.pipeline.enrich import LIVE_PROCESS_COLUMNS
from node_monitor.pipeline.records import LIVE_NODE, LIVE_PROCESSES, Record

SYSTEM = "polaris"


def proc(pid, username, category, **kw):
   base = dict.fromkeys(LIVE_PROCESS_COLUMNS)
   base.update(
      pid=pid,
      start_time=1000 + pid,
      uid=1000 + pid,
      username=username,
      category=category,
      activity="running",
      state="S",
      threads=1,
      rss_kb=1024,
      cpu_seconds=0.0,
      cpu_state="busy",
      idle_for_is_lower_bound=False,
      is_user_process=True,
      owner_presence="interactive",
      cmdline=f"cmd{pid}",
   )
   base.update(kw)
   return base


N1 = [
   proc(1, "alice", "ml", cpu_seconds=500, rss_kb=4194304, gpu_mem_mb=1000, threads=8,
        owner_presence="absent"),
   proc(
      2, "alice", "shell", cpu_seconds=5, rss_kb=10240, cpu_state="idle", idle_for_sec=7200,
      owner_presence="absent", cmdline="x" * 80,
   ),
   proc(
      3, "bob", "ml", cpu_seconds=50, rss_kb=1048576, gpu_mem_mb=500, threads=4,
      cpu_state="idle", idle_for_sec=100, idle_for_is_lower_bound=True,
   ),
]  # fmt: skip
N2 = [
   proc(4, "bob", "editor", cpu_seconds=20, rss_kb=524288, threads=2,
        cpu_state="idle", idle_for_sec=3600),
   proc(5, "carol", "shell", cpu_seconds=1, rss_kb=2048),
   proc(6, "root", "system", cpu_seconds=9999, rss_kb=100, is_user_process=False),
]  # fmt: skip


def populate(url, now=None):
   """Returns `now`. n1.example is labelled N1; n2.example's label is only in `nodes`."""
   now = now or datetime.now(timezone.utc)
   records = []
   for host, label, procs, load in (("n1.example", "N1", N1, 1.5), ("n2.example", None, N2, 0.2)):
      records.append(
         Record(
            LIVE_NODE,
            {
               "system": SYSTEM,
               "node": host,
               "label": label or "N2",
               "updated_at": now - timedelta(seconds=5),
               "session_state": "up",
               "boot_id": "b-" + host,
               "probe_version": 1,
               "load1": load,
               "mem_used_kb": 1048576,
               "mem_total_kb": 4194304,
               "procs_total": 100,
               "last_census_at": now - timedelta(seconds=45),
            },
         )  # fmt: skip
      )
      records.append(
         Record(
            LIVE_PROCESSES,
            {"system": SYSTEM, "node": host, "ts": now - timedelta(seconds=45), "processes": procs},
         )
      )
   writer = DatabaseWriter(url, retention=RetentionConfig())
   try:
      writer.write(records, now)
   finally:
      writer.close()
   return now


def put_heartbeat(url, now, *, age_sec=5, status=None):
   with connect(url, autocommit=True) as conn:
      write_heartbeat(
         conn,
         system=SYSTEM,
         collector_host="collector-1",
         version="9.9",
         started_at=now - timedelta(hours=1),
         now=now - timedelta(seconds=age_sec),
         status=status
         or {"state": "running", "connected": 2, "spooled_batches": 0, "sessions": {"N1": "up"}},
      )
