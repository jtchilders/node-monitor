"""The daemon end to end: fake ssh, a fake /proc per node, and a real test database."""

import asyncio
import contextlib
import dataclasses
import logging
import os
import signal
import sys
import threading
import time
from collections import Counter

import psycopg
import pytest

from node_monitor.collector.daemon import Daemon
from node_monitor.config import (
   ClassificationConfig,
   CollectionConfig,
   Config,
   ConfigError,
   DatabaseConfig,
   NodeConfig,
   SshConfig,
)
from node_monitor.database.connect import connect
from node_monitor.database.spool import Spool
from node_monitor.pipeline.node_pipeline import NodePipeline
from node_monitor.pipeline.records import (
   COLLECTION_EVENT,
   LIVE_NODE,
   LIVE_PROCESSES,
   NODE_HARDWARE,
   NODE_METRICS_1M,
   NODE_USAGE_5M,
   USER_USAGE_5M,
)
from tests.collector.test_soak import FAKE_SSH
from tests.database import conftest as pg
from tests.probe import fake_proc

# The pg fixtures live in tests/database/conftest.py; re-exported for this module.
pg_admin_url, pg_db, migrated_db = pg.pg_admin_url, pg.pg_db, pg.migrated_db

UNREACHABLE = "postgresql://localhost:1/x"
LABELS = ("n1", "n2")
HOSTS = {"n1": "h1", "n2": "h2"}

# What identifies a row of each durable kind in the database.
_KEY_FIELDS = {
   NODE_METRICS_1M: ("system", "node", "minute"),
   USER_USAGE_5M: ("system", "node", "username", "interval_start"),
   NODE_USAGE_5M: ("system", "node", "interval_start"),
   NODE_HARDWARE: ("system", "node", "boot_id"),
   COLLECTION_EVENT: ("system", "node", "ts", "event_type"),
}
_SUMMARY_TABLES = (NODE_METRICS_1M, USER_USAGE_5M, NODE_USAGE_5M)


def key(record):
   return (record.kind, tuple(record.row.get(f) for f in _KEY_FIELDS[record.kind]))


def is_durable(record):
   return record.kind not in (LIVE_NODE, LIVE_PROCESSES)


class Rig:
   """Two nodes, each on its own fake tree, run by one daemon writing to `url`."""

   def __init__(self, tmp_path, monkeypatch, url):
      self.tmp_path = tmp_path
      nodes_dir = tmp_path / "nodes"
      for label in LABELS:
         fake_proc.write_tree(
            nodes_dir / label,
            pids=[{"pid": 4242, "comm": "python", "cmdline": ["python", "x.py"]}],
            boot_id=f"boot-{label}-a",
         )
      # Relative probe roots resolve inside each node's own directory.
      monkeypatch.setenv("FAKE_SSH_CHDIR", str(nodes_dir))
      self.config = Config(
         system="test",
         nodes=tuple(NodeConfig(HOSTS[label], label, label) for label in LABELS),
         probe_python=sys.executable,
         working_dir=tmp_path / "work",
         ssh=SshConfig(),
         collection=CollectionConfig(
            counter_interval_sec=0.2,
            census_interval_sec=1.0,
            summary_interval_sec=2.0,
            session_silence_timeout_sec=5,
            session_max_lifetime_sec=3600,
            reconnect_backoff_sec=(0.05, 0.4),
         ),
         classification=ClassificationConfig(),
         source_path=tmp_path / "config.yaml",
         database=DatabaseConfig(url=url, connect_timeout_sec=2),
      )
      self.overrides = {
         "gpu": False,
         "proc_root": "proc",
         "sys_root": "sys",
         "lock_path": str(tmp_path / "probe.lock"),
      }
      self.tapped = []

   def daemon(self, config=None):
      return Daemon(
         config or self.config,
         ssh_binary=FAKE_SSH,
         probe_overrides=self.overrides,
         record_tap=self.tapped.append,
         db_backoff=(0.5, 2),
         flush_interval_sec=0.5,
      )

   def boot_id_file(self, label):
      return self.tmp_path / "nodes" / label / "proc" / "sys" / "kernel" / "random" / "boot_id"

   def spool_dir(self):
      return self.config.working_dir / "state" / "spool"

   def spooled_batches(self):
      return len(list(self.spool_dir().glob("*.jsonl")))


@contextlib.asynccontextmanager
async def running(daemon):
   """Run `daemon` in a task; however the test ends, stop it and wait for it."""
   task = asyncio.create_task(daemon.run())
   try:
      yield task
   finally:
      if not task.done():
         daemon.request_stop()
         await asyncio.wait({task}, timeout=30)
      if not task.done():
         task.cancel()
         await asyncio.wait({task})


async def finish(daemon, task, limit=30):
   daemon.request_stop()
   started = time.monotonic()
   done, _ = await asyncio.wait({task}, timeout=limit)
   assert done, f"run did not return within {limit} s"
   return task.result(), time.monotonic() - started


async def eventually(check, timeout, what):
   """Poll `check` (run in a thread: it may block on the database) until it is truthy."""
   deadline = time.monotonic() + timeout
   while True:
      result = await asyncio.to_thread(check)
      if result:
         return result
      if time.monotonic() > deadline:
         raise AssertionError(f"timed out after {timeout} s waiting for {what}")
      await asyncio.sleep(0.2)


def query(url, sql, params=()):
   with connect(url, connect_timeout_sec=5, autocommit=True) as conn:
      return conn.execute(sql, params).fetchall()


def event_count(url, event_type, node=None):
   sql = "SELECT count(*) FROM collection_events WHERE event_type = %s"
   params = [event_type]
   if node is not None:
      sql += " AND node = %s"
      params.append(node)
   return query(url, sql, params)[0][0]


def event_types(url):
   return {(t, n) for t, n in query(url, "SELECT event_type, node FROM collection_events")}


def summary_keys(url):
   keys = set()
   for kind in _SUMMARY_TABLES:
      fields = _KEY_FIELDS[kind]
      for row in query(url, f"SELECT {', '.join(fields)} FROM {kind}"):
         keys.add((kind, tuple(row)))
   return keys


def drop_client_sessions(admin_url, db_url):
   """Terminate every session on the test database, so the writer must reconnect."""
   with psycopg.connect(admin_url, autocommit=True) as admin:
      admin.execute(
         "SELECT pg_terminate_backend(pid, 5000) FROM pg_stat_activity"
         " WHERE datname = %s AND backend_type = 'client backend' AND pid <> pg_backend_pid()",
         (db_url.rsplit("/", 1)[1],),
      )


def test_missing_database_url_is_a_config_error(tmp_path, monkeypatch):
   rig = Rig(tmp_path, monkeypatch, None)
   with pytest.raises(ConfigError, match="database.url is not set"):
      rig.daemon()


async def test_daemon_survives_kill_reboot_and_outage(
   tmp_path, monkeypatch, migrated_db, pg_admin_url
):
   rig = Rig(tmp_path, monkeypatch, migrated_db)
   daemon = rig.daemon()
   async with running(daemon) as task:
      await eventually(
         lambda: len(query(migrated_db, "SELECT node FROM live_nodes")) == 2, 20, "live_nodes"
      )

      os.killpg(daemon.sessions["n1"].current_pid, signal.SIGKILL)
      await eventually(
         lambda: event_count(migrated_db, "session_started", "h1") >= 2, 15, "n1 to reconnect"
      )

      rig.boot_id_file("n2").write_text("boot-n2-b\n")
      os.killpg(daemon.sessions["n2"].current_pid, signal.SIGKILL)
      await eventually(
         lambda: event_count(migrated_db, "node_rebooted", "h2") >= 1, 15, "n2's reboot"
      )

      daemon.database_writer.url = UNREACHABLE
      # The URL is read on reconnect; ending the open session forces one.
      await asyncio.to_thread(drop_client_sessions, pg_admin_url, migrated_db)
      await eventually(lambda: rig.spooled_batches() > 0, 10, "a spooled batch")
      daemon.database_writer.url = migrated_db
      await eventually(lambda: event_count(migrated_db, "database_recovered") >= 1, 15, "recovery")

      assert not task.done()
      rc, _ = await finish(daemon, task)
   assert rc == 0

   tapped = {key(r) for r in rig.tapped if r.kind in _SUMMARY_TABLES}
   assert tapped and tapped <= summary_keys(migrated_db)
   assert rig.spooled_batches() == 0
   assert {
      ("collector_started", None),
      ("session_lost", "h1"),
      ("node_rebooted", "h2"),
      ("database_unavailable", None),
      ("database_recovered", None),
      ("collector_stopped", None),
   } <= event_types(migrated_db)
   ((status,),) = query(migrated_db, "SELECT status FROM collector_heartbeat")
   assert status["state"] == "stopped"
   # The last heartbeat tells the truth about the sessions too: none is running any more.
   assert status["sessions"] == {"n1": "stopped", "n2": "stopped"}


async def test_daemon_started_while_database_down_catches_up(tmp_path, monkeypatch, migrated_db):
   rig = Rig(tmp_path, monkeypatch, migrated_db)
   config = dataclasses.replace(
      rig.config, database=dataclasses.replace(rig.config.database, url=UNREACHABLE)
   )
   daemon = rig.daemon(config)
   async with running(daemon) as task:
      await asyncio.sleep(3)
      assert not task.done()
      assert rig.spooled_batches() > 0
      daemon.database_writer.url = migrated_db
      await eventually(
         lambda: (
            query(migrated_db, "SELECT count(*) FROM node_usage_5m")[0][0] > 0
            and rig.spooled_batches() == 0
         ),
         15,
         "the spool to drain into the database",
      )
      assert not task.done()
      rc, _ = await finish(daemon, task)
   assert rc == 0
   # Minutes close only at a minute boundary or at shutdown, which flushes the partial one.
   assert query(migrated_db, "SELECT count(*) FROM node_metrics_1m")[0][0] > 0
   assert rig.spooled_batches() == 0
   assert ("database_recovered", None) in event_types(migrated_db)


async def test_sigterm_during_outage_spools_everything(tmp_path, monkeypatch):
   rig = Rig(tmp_path, monkeypatch, UNREACHABLE)
   daemon = rig.daemon()
   async with running(daemon) as task:
      await asyncio.sleep(4)
      assert not task.done()
      rc, _ = await finish(daemon, task)
   assert rc == 0

   spooled = [
      record
      for _, batch in Spool(rig.spool_dir(), max_bytes=10**9).pending()
      for record in batch
      if is_durable(record)
   ]
   tapped = Counter(key(r) for r in rig.tapped if is_durable(r))
   in_spool = Counter(key(r) for r in spooled)
   assert tapped and not tapped - in_spool
   # Collector-level events (startup, outage, shutdown) are spooled but never tapped.
   extras = in_spool - tapped
   assert all(kind == COLLECTION_EVENT and fields[1] is None for kind, fields in extras)
   assert {fields[3] for kind, fields in extras} >= {
      "collector_started",
      "database_unavailable",
      "collector_stopped",
   }


@pytest.mark.skipif(
   threading.current_thread() is not threading.main_thread(),
   reason="signal handlers need the main thread",
)
async def test_signals_during_the_writer_drain_do_not_kill_the_process(
   tmp_path, monkeypatch, caplog
):
   rig = Rig(tmp_path, monkeypatch, UNREACHABLE)
   daemon = rig.daemon()
   buffered = daemon._writer_thread._buffered
   real_close = buffered.close
   signalled = []

   def slow_close():
      # Shutdown has reached the writer's last step; the event loop is waiting on its join.
      for sig in (signal.SIGTERM, signal.SIGINT):
         os.kill(os.getpid(), sig)
         signalled.append(sig)
         time.sleep(0.3)
      real_close()

   monkeypatch.setattr(buffered, "close", slow_close)
   caplog.set_level(logging.WARNING, logger="node_monitor.collector.daemon")
   async with running(daemon) as task:
      await asyncio.sleep(1)
      rc, _ = await finish(daemon, task)
   assert rc == 0
   assert signalled == [signal.SIGTERM, signal.SIGINT]
   assert "shutdown in progress; waiting for the writer" in caplog.text
   # Everything queued still reached the spool, the shutdown event included.
   spooled = [
      r.row["event_type"]
      for _, batch in Spool(rig.spool_dir(), max_bytes=10**9).pending()
      for r in batch
      if r.kind == COLLECTION_EVENT
   ]
   assert "collector_stopped" in spooled


async def test_pipeline_errors_and_crashes_become_events(tmp_path, monkeypatch, caplog):
   real_on_message = NodePipeline.on_message

   def failing_census(self, msg, recv):
      if msg.get("type") == "census":
         raise RuntimeError("bad census")
      return real_on_message(self, msg, recv)

   async def crash():
      raise RuntimeError("session bug")

   monkeypatch.setattr(NodePipeline, "on_message", failing_census)
   rig = Rig(tmp_path, monkeypatch, UNREACHABLE)
   daemon = rig.daemon()
   daemon.sessions["n1"].run = crash
   async with running(daemon) as task:
      await asyncio.sleep(3)
      assert not task.done()
      rc, _ = await finish(daemon, task)
   assert rc == 0

   events = [
      (r.row["event_type"], r.row["node"], r.row["detail"])
      for _, batch in Spool(rig.spool_dir(), max_bytes=10**9).pending()
      for r in batch
      if r.kind == COLLECTION_EVENT
   ]
   assert ("session_crashed", "h1", {"type": "RuntimeError", "error": "session bug"}) in events
   errors = [d for t, n, d in events if t == "pipeline_error" and n == "h2"]
   assert errors and all(d == {"type": "RuntimeError", "error": "bad census"} for d in errors)
   # The failing census cost only itself: n2's session kept running throughout.
   assert [n for t, n, _ in events if t == "session_started"] == ["h2"]
   assert not [n for t, n, _ in events if t == "session_lost"]
   assert any(r.kind == NODE_METRICS_1M and r.row["node"] == "h2" for r in rig.tapped)
   assert "session bug" in caplog.text


async def test_out_of_date_schema_refuses_to_start(tmp_path, monkeypatch, pg_db, caplog):
   rig = Rig(tmp_path, monkeypatch, pg_db)
   daemon = rig.daemon()
   caplog.set_level(logging.ERROR, logger="node_monitor.collector.daemon")
   async with running(daemon) as task:
      done, _ = await asyncio.wait({task}, timeout=15)
      assert done
      assert task.result() == 2
   assert "node-monitor database migrate" in caplog.text
   assert not rig.tapped
   assert rig.spooled_batches() == 0
