from datetime import datetime, timedelta, timezone

import pytest

from node_monitor.collector.upkeep import Upkeep
from node_monitor.config import RetentionConfig
from node_monitor.database import maintenance
from node_monitor.database.connect import connect
from tests.database import conftest as pg

# The pg fixtures live in tests/database/conftest.py; re-exported for this module.
pg_admin_url, pg_db, migrated_db = pg.pg_admin_url, pg.pg_db, pg.migrated_db

UTC = timezone.utc
NOW = datetime(2026, 11, 15, 12, tzinfo=UTC)
STARTED = datetime(2026, 11, 15, 11, tzinfo=UTC)


def make(url, **kw):
   return Upkeep(
      url,
      system="polaris",
      retention=RetentionConfig(),
      connect_timeout_sec=1,
      collector_host="head1",
      version="9.9",
      started_at=STARTED,
      **kw,
   )


def heartbeat(url):
   with connect(url, autocommit=True) as conn:
      return conn.execute(
         "SELECT collector_host, version, last_seen, status FROM collector_heartbeat"
         " WHERE system = 'polaris'"
      ).fetchone()


def types(events):
   return [e.row["event_type"] for e in events]


def test_tick_writes_heartbeat_and_maintains_partitions_once(migrated_db):
   upkeep = make(migrated_db)
   try:
      events = upkeep.tick(NOW, {"state": "running"})
      assert types(events) == ["partitions_maintained"]
      row = events[0].row
      assert row["node"] is None and row["system"] == "polaris" and row["ts"] == NOW
      assert row["detail"]["created"] and row["detail"]["skipped"] == []
      host, version, last_seen, status = heartbeat(migrated_db)
      assert (host, version, last_seen, status) == ("head1", "9.9", NOW, {"state": "running"})

      assert upkeep.tick(NOW + timedelta(seconds=10), {"state": "running"}) == []
      assert heartbeat(migrated_db)[2] == NOW

      assert upkeep.tick(NOW + timedelta(seconds=61), {"state": "running"}) == []
      assert heartbeat(migrated_db)[2] == NOW + timedelta(seconds=61)
   finally:
      upkeep.close(NOW + timedelta(seconds=62), {"state": "running"})


def test_close_writes_stopped_heartbeat(migrated_db):
   upkeep = make(migrated_db)
   upkeep.tick(NOW, {"state": "running"})
   upkeep.close(NOW + timedelta(seconds=5), {"state": "running", "x": 1})
   assert heartbeat(migrated_db)[3] == {"state": "stopped", "x": 1}


def test_tick_with_unreachable_database_returns_nothing_and_retries(monkeypatch):
   calls = []
   real = connect

   def counting(*a, **kw):
      calls.append(1)
      return real(*a, **kw)

   monkeypatch.setattr("node_monitor.collector.upkeep.connect", counting)
   upkeep = make("postgresql://localhost:1/x")
   assert upkeep.tick(NOW, {"state": "running"}) == []
   first = len(calls)
   assert first >= 1
   # Not hammered: nothing is due until the intervals pass.
   assert upkeep.tick(NOW + timedelta(seconds=10), {}) == []
   assert len(calls) == first
   assert upkeep.tick(NOW + timedelta(seconds=61), {}) == []
   assert len(calls) > first
   upkeep.close(NOW + timedelta(seconds=62), {})


def test_failed_maintenance_is_reported_and_retried_after_retry_interval(migrated_db, monkeypatch):
   def boom(conn, now, retention):
      raise RuntimeError("nope")

   monkeypatch.setattr(maintenance, "run", boom)
   upkeep = make(migrated_db)
   try:
      events = upkeep.tick(NOW, {})
      assert types(events) == ["maintenance_failed"]
      assert "nope" in events[0].row["detail"]["error"]
      assert upkeep.tick(NOW + timedelta(seconds=3599), {}) == []
      assert types(upkeep.tick(NOW + timedelta(seconds=3600), {})) == ["maintenance_failed"]
   finally:
      upkeep.close(NOW + timedelta(seconds=3601), {})


def test_skipped_retention_is_retried_after_retry_interval(migrated_db, monkeypatch):
   runs = []

   def skipping(conn, now, retention):
      runs.append(now)
      return {"created": [], "dropped": [], "skipped": ["x"]}

   monkeypatch.setattr(maintenance, "run", skipping)
   upkeep = make(migrated_db)
   try:
      events = upkeep.tick(NOW, {})
      assert events[0].row["detail"] == {"created": [], "dropped": [], "skipped": ["x"]}
      upkeep.tick(NOW + timedelta(seconds=3599), {})
      assert len(runs) == 1
      upkeep.tick(NOW + timedelta(seconds=3600), {})
      assert len(runs) == 2
   finally:
      upkeep.close(NOW + timedelta(seconds=3601), {})


def test_clean_maintenance_waits_the_full_interval(migrated_db, monkeypatch):
   runs = []

   def clean(conn, now, retention):
      runs.append(now)
      return {"created": [], "dropped": [], "skipped": []}

   monkeypatch.setattr(maintenance, "run", clean)
   upkeep = make(migrated_db)
   try:
      assert upkeep.tick(NOW, {}) == []
      upkeep.tick(NOW + timedelta(seconds=86399), {})
      assert len(runs) == 1
      upkeep.tick(NOW + timedelta(seconds=86400), {})
      assert len(runs) == 2
   finally:
      upkeep.close(NOW + timedelta(seconds=86401), {})


def test_tick_reconnects_after_the_connection_is_lost(migrated_db):
   upkeep = make(migrated_db)
   try:
      upkeep.tick(NOW, {})
      upkeep._conn.close()
      assert upkeep.tick(NOW + timedelta(seconds=61), {}) == []
      assert upkeep.tick(NOW + timedelta(seconds=122), {"state": "running"}) == []
      assert heartbeat(migrated_db)[2] == NOW + timedelta(seconds=122)
   finally:
      upkeep.close(NOW + timedelta(seconds=123), {})


def test_close_never_raises_when_database_is_down():
   upkeep = make("postgresql://localhost:1/x")
   upkeep.close(NOW, {"state": "running"})


@pytest.fixture(autouse=True)
def _no_wall_clock(monkeypatch):
   # Upkeep must take time only from its arguments.
   class Boom:
      @staticmethod
      def now(*a, **k):
         raise AssertionError("wall clock read")

   monkeypatch.setattr("node_monitor.collector.upkeep.datetime", Boom, raising=False)
