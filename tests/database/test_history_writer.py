from datetime import datetime, timedelta, timezone

from psycopg.rows import dict_row

from node_monitor.config import RetentionConfig
from node_monitor.database.connect import connect
from node_monitor.database.maintenance import apply_retention
from node_monitor.database.writer import DatabaseWriter
from node_monitor.pipeline.records import Record
from tests.pipeline import at

UTC = timezone.utc
NOW = at(3600)


def _writer(url, **kw):
   return DatabaseWriter(url, retention=RetentionConfig(), **kw)


def _write(url, records, now=NOW):
   w = _writer(url)
   try:
      return w.write(records, now)
   finally:
      w.close()


def _rows(url, query, params=()):
   with connect(url) as conn:
      conn.row_factory = dict_row
      return conn.execute(query, params).fetchall()


def _one(url, query, params=()):
   (r,) = _rows(url, query, params)
   return r


def _sample_row(ts, **overrides):
   base = {
      "system": "polaris",
      "node": "n1.example",
      "ts": ts,
      "pid": 1,
      "start_time": 100,
      "username": "alice",
      "uid": 1001,
      "comm": "python",
      "exe": "/usr/bin/python",
      "argv0": "python",
      "cmdline": "python train.py",
      "cmdline_hash": "abc123",
      "ppid": 10,
      "parent_comm": "bash",
      "parent_exe": "/bin/bash",
      "parent_kind": "shell",
      "category": "python",
      "activity": "python-generic",
      "tags": [],
      "state": "S",
      "threads": 1,
      "cpu_seconds": 0.5,
      "rss_kb": 100,
      "gpu_mem_mb": None,
      "interactive": False,
      "under_screen_or_tmux": False,
      "cpu_state": "active",
      "idle_for_sec": 0,
      "idle_for_is_lower_bound": False,
      "idle_bucket": "<5m",
      "owner_presence": "absent",
      "hourly": False,
   }
   base.update(overrides)
   return Record("process_samples", base)


def _cmdline_row(**overrides):
   base = {
      "system": "polaris",
      "username": "alice",
      "cmdline_hash": "h1",
      "cmdline": "python train.py",
      "exe": "/usr/bin/python",
      "activity": "python-generic",
      "category": "python",
      "tags": [],
      "first_seen": NOW,
      "last_seen": NOW,
      "sample_count": 1,
      "nodes": ["n1.example"],
   }
   base.update(overrides)
   return Record("command_lines", base)


def test_writer_upserts_samples_idempotent(migrated_db):
   ts = datetime(2026, 11, 1, 12, 0, tzinfo=UTC)
   rec = _sample_row(ts)
   # Create daily partition
   with connect(migrated_db, autocommit=True) as conn:
      conn.execute(
         "CREATE TABLE IF NOT EXISTS node_monitor.process_samples_y20261101"  # noqa: E501
         " PARTITION OF node_monitor.process_samples"
         " FOR VALUES FROM ('2026-11-01 00:00+00') TO ('2026-11-02 00:00+00')"
      )
   _write(migrated_db, [rec])
   assert _one(migrated_db, "SELECT count(*) AS n FROM process_samples")["n"] == 1
   # Idempotent replay
   _write(migrated_db, [rec])
   assert _one(migrated_db, "SELECT count(*) AS n FROM process_samples")["n"] == 1
   # Different pid inserts second row
   rec2 = _sample_row(ts, pid=2, start_time=200)
   _write(migrated_db, [rec2])
   assert _one(migrated_db, "SELECT count(*) AS n FROM process_samples")["n"] == 2

   # command_lines upsert
   cmd = _cmdline_row(first_seen=ts, last_seen=ts)
   _write(migrated_db, [cmd])
   assert _one(migrated_db, "SELECT sample_count AS n FROM command_lines")["n"] == 1
   later = ts + timedelta(hours=1)
   cmd2 = _cmdline_row(first_seen=later, last_seen=later, sample_count=1, nodes=["n2.example"])
   _write(migrated_db, [cmd2])
   row = _one(migrated_db, "SELECT first_seen, last_seen, sample_count, nodes FROM command_lines")
   assert row["first_seen"] == ts
   assert row["last_seen"] == later
   assert row["sample_count"] == 2
   assert sorted(row["nodes"]) == ["n1.example", "n2.example"]


def test_retention_drops_old_partition(migrated_db):
   """process_samples at exactly 30d must drop, command_lines at now-730d kept."""
   now = datetime(2026, 11, 15, 12, tzinfo=UTC)
   # Create daily partitions: one at exactly 30d old (should drop), one 29d old (keep)
   cutoff_day = (now - timedelta(days=30)).date()  # 2026-10-16
   keep_day = (now - timedelta(days=29)).date()  # 2026-10-17
   with connect(migrated_db, autocommit=True) as conn:
      for d in (cutoff_day, keep_day):
         nxt = d + timedelta(days=1)
         conn.execute(
            f"CREATE TABLE IF NOT EXISTS node_monitor.process_samples_y{d:%Y%m%d}"
            f" PARTITION OF node_monitor.process_samples"  # noqa: E501
            f" FOR VALUES FROM ('{d} 00:00+00') TO ('{nxt} 00:00+00')"
         )
      retention = RetentionConfig(process_samples_days=30, command_lines_days=730)
      result = apply_retention(conn, now, retention)
      assert f"process_samples_y{cutoff_day:%Y%m%d}" in result["dropped"]
      assert f"process_samples_y{keep_day:%Y%m%d}" not in result["dropped"]

   # command_lines boundary: last_seen == now-730d must be kept
   with connect(migrated_db, autocommit=True) as conn:
      exact = now - timedelta(days=730)
      old = now - timedelta(days=731)
      conn.execute(
         "INSERT INTO command_lines (system, username, cmdline_hash, cmdline, first_seen, last_seen, sample_count, nodes)"  # noqa: E501
         " VALUES ('polaris','alice','h_keep','cmd',%s,%s,1,ARRAY['n1'])",
         (exact, exact),
      )
      conn.execute(
         "INSERT INTO command_lines (system, username, cmdline_hash, cmdline, first_seen, last_seen, sample_count, nodes)"  # noqa: E501
         " VALUES ('polaris','bob','h_drop','cmd',%s,%s,1,ARRAY['n1'])",
         (old, old),
      )
      retention = RetentionConfig(process_samples_days=30, command_lines_days=730)
      result = apply_retention(conn, now, retention)
      remaining = {r[0] for r in conn.execute("SELECT username FROM command_lines").fetchall()}
      assert "alice" in remaining  # kept at boundary
      assert "bob" not in remaining  # dropped
