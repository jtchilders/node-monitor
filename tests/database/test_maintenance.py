import threading
import time
from datetime import datetime, timezone

import psycopg
import pytest

from node_monitor.config import RetentionConfig
from node_monitor.database import maintenance
from node_monitor.database.bootstrap import bootstrap
from node_monitor.database.connect import connect
from node_monitor.database.maintenance import (
   PARTITIONED,
   apply_retention,
   ensure_partition,
   ensure_partitions,
   run,
   write_heartbeat,
)
from node_monitor.database.migrate import migrate
from tests.database.test_bootstrap import _url, names  # noqa: F401

UTC = timezone.utc
NOW = datetime(2026, 11, 15, 12, tzinfo=UTC)


def _partitions(conn, table):
   return maintenance._attached_partitions(conn, table)


def test_ensure_partitions_creates_three_months_per_table_idempotently(migrated_db):
   with connect(migrated_db, autocommit=True) as conn:
      created = ensure_partitions(conn, NOW)
      assert len(created) == 12
      assert "node_metrics_1m_y2026m11" in created
      assert "collection_events_y2027m01" in created
      assert ensure_partitions(conn, NOW) == []
      for table in PARTITIONED:
         assert len(_partitions(conn, table)) == 3


def test_month_boundaries_are_utc(migrated_db):
   with connect(migrated_db, autocommit=True) as conn:
      ensure_partitions(conn, NOW)
      for ts in ("2026-11-30T23:59:59+00:00", "2026-12-01T00:00:00+00:00"):
         conn.execute(
            "INSERT INTO collection_events (ts, system, event_type, detail)"
            " VALUES (%s, 's', 'e', '{}')",
            (datetime.fromisoformat(ts),),
         )
      nov = conn.execute("SELECT count(*) FROM collection_events_y2026m11").fetchone()[0]
      dec = conn.execute("SELECT count(*) FROM collection_events_y2026m12").fetchone()[0]
      assert (nov, dec) == (1, 1)


def test_apply_retention_drops_only_expired_partitions(migrated_db):
   retention = RetentionConfig(collection_events_days=180)
   with connect(migrated_db, autocommit=True) as conn:
      for month in (4, 5, 6, 11):
         ensure_partition(conn, "collection_events", datetime(2026, month, 1, tzinfo=UTC))
      # Cutoff is 2026-05-19: April is wholly older, May still holds rows within retention.
      assert apply_retention(conn, NOW, retention) == {
         "dropped": ["collection_events_y2026m04"],
         "skipped": [],
      }
      assert _partitions(conn, "collection_events") == [
         "collection_events_y2026m05",
         "collection_events_y2026m06",
         "collection_events_y2026m11",
      ]
      # The dropped table is gone, not just detached.
      assert conn.execute("SELECT to_regclass('collection_events_y2026m04')").fetchone()[0] is None


def test_retention_never_drops_current_month_or_foreign_tables(migrated_db):
   with connect(migrated_db, autocommit=True) as conn:
      ensure_partition(conn, "collection_events", NOW)
      conn.execute("CREATE TABLE collection_events_y2020m01_extra (a int)")
      conn.execute("CREATE TABLE node_metrics_1m_old (a int)")
      conn.execute("CREATE TABLE collection_events_y2020m01 (LIKE collection_events)")
      # Zero days of retention still keeps the current month; unattached tables are untouched.
      result = apply_retention(conn, NOW, RetentionConfig(collection_events_days=0))
      assert result == {"dropped": [], "skipped": []}
      assert _partitions(conn, "collection_events") == ["collection_events_y2026m11"]
      for t in (
         "collection_events_y2020m01_extra",
         "node_metrics_1m_old",
         "collection_events_y2020m01",
      ):
         assert conn.execute("SELECT to_regclass(%s)", (t,)).fetchone()[0] is not None


def test_run_reports_created_and_dropped(migrated_db):
   with connect(migrated_db, autocommit=True) as conn:
      ensure_partition(conn, "collection_events", datetime(2026, 1, 1, tzinfo=UTC))
      result = run(conn, NOW, RetentionConfig())
      assert result["dropped"] == ["collection_events_y2026m01"]
      assert result["skipped"] == []
      assert len(result["created"]) == 12


def test_retention_skips_a_partition_whose_parent_is_locked(migrated_db, monkeypatch):
   monkeypatch.setattr(maintenance, "LOCK_TIMEOUT", "1s")
   retention = RetentionConfig(node_metrics_days=180, collection_events_days=180)
   with connect(migrated_db, autocommit=True) as conn, connect(migrated_db) as reader:
      for table in ("node_metrics_1m", "collection_events"):
         ensure_partition(conn, table, datetime(2026, 1, 1, tzinfo=UTC))
      before = conn.execute("SHOW lock_timeout").fetchone()[0]
      # A long reader transaction on one parent.
      reader.execute("LOCK TABLE node_monitor.node_metrics_1m IN ACCESS SHARE MODE")
      started = time.monotonic()
      result = apply_retention(conn, NOW, retention)
      assert time.monotonic() - started < 10
      assert result == {
         "dropped": ["collection_events_y2026m01"],
         "skipped": ["node_metrics_1m_y2026m01"],
      }
      assert _partitions(conn, "node_metrics_1m") == ["node_metrics_1m_y2026m01"]
      # Session settings are untouched: the timeout was local to the retention transaction.
      assert conn.execute("SHOW lock_timeout").fetchone()[0] == before
      reader.rollback()
      # Due again on the next run, once the reader is gone.
      assert apply_retention(conn, NOW, retention)["dropped"] == ["node_metrics_1m_y2026m01"]


def test_ensure_partition_gives_up_on_a_locked_parent(migrated_db, monkeypatch):
   monkeypatch.setattr(maintenance, "LOCK_TIMEOUT", "1s")
   with connect(migrated_db, autocommit=True) as conn, connect(migrated_db) as reader:
      reader.execute("LOCK TABLE node_monitor.node_metrics_1m IN ACCESS SHARE MODE")
      started = time.monotonic()
      with pytest.raises(psycopg.errors.LockNotAvailable):
         ensure_partition(conn, "node_metrics_1m", NOW)
      assert time.monotonic() - started < 10
      reader.rollback()
      assert ensure_partition(conn, "node_metrics_1m", NOW) is True


def test_concurrent_ensure_partition_creates_once(migrated_db):
   month = datetime(2027, 3, 1, tzinfo=UTC)
   results, errors = [], []
   barrier = threading.Barrier(2)
   conns = [connect(migrated_db, autocommit=True) for _ in range(2)]

   def work(conn):
      try:
         barrier.wait(timeout=10)
         results.append(ensure_partition(conn, "node_metrics_1m", month))
      except Exception as exc:  # reported below
         errors.append(exc)

   try:
      threads = [threading.Thread(target=work, args=(c,)) for c in conns]
      for t in threads:
         t.start()
      for t in threads:
         t.join(timeout=30)
   finally:
      for c in conns:
         c.close()
   assert errors == []
   assert sorted(results) == [False, True]


def test_reader_can_select_new_partition(pg_admin_url, names):  # noqa: F811
   bootstrap(pg_admin_url, **names)
   with connect(_url(pg_admin_url, names, "owner"), autocommit=True) as conn:
      migrate(conn)
      ensure_partition(conn, "node_metrics_1m", NOW)
   with connect(_url(pg_admin_url, names, "reader")) as conn:
      assert conn.execute("SELECT count(*) FROM node_metrics_1m_y2026m11").fetchone()[0] == 0


def test_heartbeat_upsert(migrated_db):
   started = datetime(2026, 11, 1, tzinfo=UTC)
   with connect(migrated_db) as conn:
      write_heartbeat(
         conn,
         system="s",
         collector_host="h1",
         version="1",
         started_at=started,
         now=NOW,
         status={"ok": True},
      )
      later = datetime(2026, 11, 15, 12, 5, tzinfo=UTC)
      write_heartbeat(
         conn,
         system="s",
         collector_host="h2",
         version="2",
         started_at=started,
         now=later,
         status={"ok": False, "nan": float("nan")},
      )
      rows = conn.execute(
         "SELECT collector_host, version, last_seen, status FROM collector_heartbeat"
      ).fetchall()
   assert rows == [("h2", "2", later, {"ok": False, "nan": None})]
