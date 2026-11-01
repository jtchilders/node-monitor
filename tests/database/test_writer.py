import math
from datetime import datetime, timedelta, timezone

import psycopg
import pytest
from psycopg.rows import dict_row

from node_monitor.config import RetentionConfig
from node_monitor.database import writer as writer_mod
from node_monitor.database.connect import DatabaseUnavailable, connect
from node_monitor.database.maintenance import ensure_partition, partition_name
from node_monitor.database.writer import DatabaseWriter, WriteResult
from node_monitor.pipeline.records import (
   COLLECTION_EVENT,
   LIVE_NODE,
   LIVE_PROCESSES,
   NODE_HARDWARE,
   NODE_METRICS_1M,
   NODE_USAGE_5M,
   USER_USAGE_5M,
   Record,
)
from tests.database.sessions import HARDWARE, full_hello, scripted_records
from tests.pipeline import at
from tests.pipeline.test_node_pipeline import census, counters, make_pipeline, row

UTC = timezone.utc
NOW = at(3600)
UNREACHABLE = "postgresql://localhost:1/x"
NODE = ("polaris", "n1.example")

COUNTED_TABLES = (
   "live_processes",
   "live_snapshots",
   "live_nodes",
   "nodes",
   "node_hardware",
   "node_metrics_1m",
   "user_usage_5m",
   "node_usage_5m",
   "collection_events",
)


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


def _counts(url):
   return {t: _one(url, f"SELECT count(*) AS n FROM {t}")["n"] for t in COUNTED_TABLES}


def _first(recs, kind, pred=lambda r: True):
   return next(r for r in recs if r.kind == kind and pred(r))


def _last(recs, kind, pred=lambda r: True):
   return [r for r in recs if r.kind == kind and pred(r)][-1]


def _metrics_row(minute, **overrides):
   base = _first(scripted_records(), NODE_METRICS_1M).row
   return Record(NODE_METRICS_1M, {**base, "minute": minute, **overrides})


def _event(ts, event_type="probe_error", node="n1.example"):
   return Record(
      COLLECTION_EVENT,
      {"system": "polaris", "node": node, "ts": ts, "event_type": event_type, "detail": {}},
   )


def _assert_stored(stored, expected, skip=()):
   for key, value in expected.items():
      if key in skip:
         continue
      assert stored[key] == value, key


# --- without a database ---------------------------------------------------------------


def test_unavailable_database_raises_database_unavailable():
   w = _writer(UNREACHABLE, connect_timeout_sec=2)
   with pytest.raises(DatabaseUnavailable):
      w.write([_event(NOW)], NOW)
   # Still unavailable on the next attempt: a failed connect is not kept.
   with pytest.raises(DatabaseUnavailable):
      w.write([_event(NOW)], NOW)
   w.close()


def test_unknown_kind_rejected_before_connecting():
   w = _writer(UNREACHABLE, connect_timeout_sec=2)
   with pytest.raises(ValueError, match="bogus"):
      w.write([_event(NOW), Record("bogus", {})], NOW)


def test_naive_now_rejected():
   w = _writer(UNREACHABLE, connect_timeout_sec=2)
   with pytest.raises(ValueError):
      w.write([_event(NOW)], NOW.replace(tzinfo=None))
   with pytest.raises(ValueError):
      w.write([_event(NOW.replace(tzinfo=None))], NOW)


def test_partition_name_uses_utc_month():
   assert partition_name("node_metrics_1m", datetime(2026, 11, 1, tzinfo=UTC)) == (
      "node_metrics_1m_y2026m11"
   )
   # 2026-12-01 01:00 at +05:00 is still November in UTC.
   east = timezone(timedelta(hours=5))
   assert partition_name("user_usage_5m", datetime(2026, 12, 1, 1, tzinfo=east)) == (
      "user_usage_5m_y2026m11"
   )


# --- with a database ------------------------------------------------------------------


def test_each_kind_round_trips(migrated_db):
   recs = scripted_records()
   batch = [
      _first(recs, COLLECTION_EVENT),
      _first(recs, NODE_HARDWARE),
      _last(recs, LIVE_NODE, lambda r: r.row["session_state"] == "up"),
      _last(recs, LIVE_PROCESSES),
      _first(recs, NODE_METRICS_1M, lambda r: r.row["cpu_pct"] is not None),
      _first(recs, USER_USAGE_5M),
      _first(recs, NODE_USAGE_5M),
   ]
   result = _write(migrated_db, batch)
   assert isinstance(result, WriteResult)
   assert result.rows_beyond_retention == {}
   assert result.rows_written == {
      "collection_events": 1,
      "node_hardware": 1,
      "live_nodes": 1,
      "live_processes": 2,
      "node_metrics_1m": 1,
      "user_usage_5m": 1,
      "node_usage_5m": 1,
   }
   assert sorted(result.partitions_created) == [
      "collection_events_y2026m11",
      "node_metrics_1m_y2026m11",
      "node_usage_5m_y2026m11",
      "user_usage_5m_y2026m11",
   ]
   event, hw, live_node, live, metrics, user_usage, node_usage = (r.row for r in batch)

   stored = _one(migrated_db, "SELECT * FROM collection_events")
   _assert_stored(stored, event)

   stored = _one(migrated_db, "SELECT * FROM node_hardware")
   assert hw["probe_version"] == 1 and hw["gpus"] == HARDWARE["gpus"]
   _assert_stored(stored, hw, skip={"cpu_count"})
   assert "cpu_count" not in stored

   stored = _one(migrated_db, "SELECT * FROM live_nodes")
   assert live_node["last_census_at"] is not None
   _assert_stored(stored, live_node)

   snap = _one(migrated_db, "SELECT * FROM live_snapshots")
   assert snap == {"system": "polaris", "node": "n1.example", "ts": live["ts"]}
   procs = _rows(migrated_db, "SELECT * FROM live_processes ORDER BY pid")
   assert len(procs) == len(live["processes"]) == 2
   for stored, proc in zip(procs, sorted(live["processes"], key=lambda p: p["pid"]), strict=True):
      _assert_stored(stored, proc)
      assert (stored["system"], stored["node"], stored["ts"]) == (*NODE, live["ts"])
   assert procs[0]["cmdline"] == "claude --token ***"

   for table, expected in (
      ("node_metrics_1m", metrics),
      ("user_usage_5m", user_usage),
      ("node_usage_5m", node_usage),
   ):
      _assert_stored(_one(migrated_db, f"SELECT * FROM {table}"), expected)

   node = _one(migrated_db, "SELECT * FROM nodes")
   assert node == {
      "system": "polaris",
      "node": "n1.example",
      "label": "N1",
      "first_seen": hw["collected_at"],
      "last_seen": live_node["updated_at"],
      "boot_id": "b1",
   }


def test_writing_a_batch_twice_is_idempotent(migrated_db):
   recs = scripted_records()
   _write(migrated_db, recs)
   first = _counts(migrated_db)
   summaries = {
      t: _rows(migrated_db, f"SELECT * FROM {t} ORDER BY 1, 2, 3")
      for t in ("node_metrics_1m", "user_usage_5m", "node_usage_5m", "node_hardware", "nodes")
   }
   result = _write(migrated_db, recs)
   assert result.partitions_created == []
   second = _counts(migrated_db)
   events = sum(r.kind == COLLECTION_EVENT for r in recs)
   assert events > 0
   # Events have no natural key, so only they are appended.
   assert second == {**first, "collection_events": first["collection_events"] + events}
   assert first["node_metrics_1m"] == 6 and first["live_processes"] == 2
   for table, before in summaries.items():
      assert _rows(migrated_db, f"SELECT * FROM {table} ORDER BY 1, 2, 3") == before, table


def _two_censuses():
   """Live records at T1 = at(0) (pid 1) and T2 = at(360) (pid 2) from one pipeline."""
   p = make_pipeline()
   old = p.on_message(counters(0), at(0))
   old += p.on_message(census([row(pid=1, start_time=1)]), at(0))
   new = p.on_message(counters(360), at(360))
   new += p.on_message(census([row(pid=2, start_time=2)]), at(360))
   pick = lambda recs: [r for r in recs if r.kind in (LIVE_PROCESSES, LIVE_NODE)]  # noqa: E731
   return pick(old), pick(new)


def _live_state(url):
   pids = [r["pid"] for r in _rows(url, "SELECT pid FROM live_processes ORDER BY pid")]
   snap = _one(url, "SELECT ts FROM live_snapshots")["ts"]
   node = _one(url, "SELECT updated_at, procs_total FROM live_nodes")
   return pids, snap, node["updated_at"]


def test_stale_live_snapshot_does_not_overwrite_newer(migrated_db):
   old, new = _two_censuses()
   t1, t2 = at(0), at(360)
   _write(migrated_db, new)
   result = _write(migrated_db, old)
   assert _live_state(migrated_db) == ([2], t2, t2)
   assert result.rows_written.get("live_processes", 0) == 0
   assert result.rows_written.get("live_nodes", 0) == 0
   node = _one(migrated_db, "SELECT first_seen, last_seen FROM nodes")
   assert node == {"first_seen": t1, "last_seen": t2}

   # An empty newer snapshot still wins over an older one with processes.
   (new_live,) = [r for r in new if r.kind == LIVE_PROCESSES]
   empty = new_live._replace(row={**new_live.row, "ts": at(400), "processes": []})
   _write(migrated_db, [empty])
   _write(migrated_db, old)
   assert _live_state(migrated_db) == ([], at(400), t2)

   # Equal timestamps re-apply, so replaying the current snapshot is harmless.
   _write(migrated_db, [empty])
   assert _live_state(migrated_db) == ([], at(400), t2)


def test_newest_snapshot_in_a_batch_wins(migrated_db):
   old, new = _two_censuses()
   result = _write(migrated_db, new + old)
   assert _live_state(migrated_db) == ([2], at(360), at(360))
   assert result.rows_written["live_processes"] == 1


def test_live_node_partial_update_keeps_gauges(migrated_db):
   p = make_pipeline()
   recs = p.on_message(full_hello(), at(0))
   recs += p.on_message(counters(0), at(0))
   up = _last(recs, LIVE_NODE)
   assert up.row["session_state"] == "up"
   lost = _last(p.on_event("session_lost", {"reason": "eof"}, at(5)), LIVE_NODE)
   assert "load1" not in lost.row
   _write(migrated_db, [up])
   _write(migrated_db, [lost])
   stored = _one(migrated_db, "SELECT * FROM live_nodes")
   assert stored == {
      "system": "polaris",
      "node": "n1.example",
      "label": "N1",
      "updated_at": at(5),
      "session_state": "down",
      "boot_id": "b1",
      "probe_version": 1,
      "load1": 0.5,
      "mem_used_kb": 600,
      "mem_total_kb": 1000,
      "procs_total": 300,
      "last_census_at": None,
   }
   # Replaying the older "up" row does not bring the node back up.
   _write(migrated_db, [up])
   assert _one(migrated_db, "SELECT session_state FROM live_nodes")["session_state"] == "down"


def test_live_node_ignores_unknown_keys_and_requires_state(migrated_db):
   base = {"system": "polaris", "node": "n1.example", "updated_at": at(0)}
   _write(migrated_db, [Record(LIVE_NODE, {**base, "session_state": "up", "bogus": 1})])
   assert _one(migrated_db, "SELECT session_state FROM live_nodes")["session_state"] == "up"
   with pytest.raises(ValueError, match="session_state"):
      _write(migrated_db, [Record(LIVE_NODE, base)])


def test_missing_partition_created_and_expired_rows_dropped(migrated_db):
   august = datetime(2026, 8, 15, 10, 0, tzinfo=UTC)
   expired = NOW - timedelta(days=800)
   result = _write(migrated_db, [_metrics_row(august), _metrics_row(expired)])
   assert result.partitions_created == ["node_metrics_1m_y2026m08"]
   assert result.rows_beyond_retention == {"node_metrics_1m": 1}
   assert result.rows_written == {"node_metrics_1m": 1}
   minutes = [r["minute"] for r in _rows(migrated_db, "SELECT minute FROM node_metrics_1m")]
   assert minutes == [august]
   parts = _rows(
      migrated_db,
      "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid"
      " WHERE i.inhparent = 'node_monitor.node_metrics_1m'::regclass",
   )
   assert [p["relname"] for p in parts] == ["node_metrics_1m_y2026m08"]

   # The partition now exists: nothing is created the second time.
   again = _write(migrated_db, [_metrics_row(august + timedelta(days=1))])
   assert again.partitions_created == []


def test_partitions_created_for_every_month_in_the_batch(migrated_db):
   usage = _first(scripted_records(), USER_USAGE_5M)
   july = datetime(2026, 7, 31, 23, 55, tzinfo=UTC)
   batch = [
      _metrics_row(datetime(2026, 8, 1, tzinfo=UTC)),
      _metrics_row(datetime(2026, 9, 30, 23, 59, tzinfo=UTC)),
      Record(USER_USAGE_5M, {**usage.row, "interval_start": july}),
      _event(datetime(2026, 10, 2, tzinfo=UTC), node=None),
      # Expired rows never create partitions.
      _event(NOW - timedelta(days=200)),
   ]
   result = _write(migrated_db, batch)
   assert sorted(result.partitions_created) == [
      "collection_events_y2026m10",
      "node_metrics_1m_y2026m08",
      "node_metrics_1m_y2026m09",
      "user_usage_5m_y2026m07",
   ]
   assert result.rows_beyond_retention == {"collection_events": 1}
   assert _one(migrated_db, "SELECT node FROM collection_events") == {"node": None}


def test_row_exactly_at_retention_cutoff_is_kept(migrated_db):
   cutoff = NOW - timedelta(days=730)
   result = _write(migrated_db, [_metrics_row(cutoff)])
   assert result.rows_beyond_retention == {}
   assert result.rows_written == {"node_metrics_1m": 1}


def test_non_finite_floats_stored_as_null(migrated_db):
   minute = datetime(2026, 11, 1, 12, 0, tzinfo=UTC)
   metrics = _metrics_row(
      minute,
      cpu_pct={"user": math.nan, "system": 12.5},
      load1=math.inf,
      load5=-math.inf,
      gpus=[{"util": math.nan, "mem_mb": 3.0}],
   )
   live = _first(scripted_records(), LIVE_PROCESSES)
   procs = [{**p, "cpu_seconds": math.nan, "idle_for_sec": math.inf} for p in live.row["processes"]]
   live = live._replace(row={**live.row, "processes": procs})
   _write(migrated_db, [metrics, live])
   stored = _one(
      migrated_db,
      "SELECT jsonb_typeof(cpu_pct->'user') AS user_type, cpu_pct, load1, load5, gpus"
      " FROM node_metrics_1m",
   )
   assert stored["user_type"] == "null"
   assert stored["cpu_pct"] == {"user": None, "system": 12.5}
   assert stored["load1"] is None and stored["load5"] is None
   assert stored["gpus"] == [{"util": None, "mem_mb": 3.0}]
   for p in _rows(migrated_db, "SELECT cpu_seconds, idle_for_sec FROM live_processes"):
      assert p == {"cpu_seconds": None, "idle_for_sec": None}


def test_node_hardware_keeps_first_collected_at_and_sets_boot_id(migrated_db):
   p = make_pipeline()
   first = _write(migrated_db, p.on_message(full_hello("b1"), at(0)))
   assert first.rows_written == {"node_hardware": 1}
   # A new session on the same boot re-sends hello.
   again = _write(migrated_db, p.on_message(full_hello("b1"), at(100)))
   assert again.rows_written == {"node_hardware": 0}
   hw = _rows(migrated_db, "SELECT boot_id, collected_at FROM node_hardware")
   assert hw == [{"boot_id": "b1", "collected_at": at(0)}]
   # A reboot, then a replayed hello from the old boot: the newer boot_id stays.
   _write(migrated_db, p.on_message(full_hello("b2"), at(200)))
   _write(migrated_db, p.on_message(full_hello("b1"), at(50)))
   node = _one(migrated_db, "SELECT boot_id, first_seen, last_seen FROM nodes")
   assert node == {"boot_id": "b2", "first_seen": at(0), "last_seen": at(200)}
   assert len(_rows(migrated_db, "SELECT 1 FROM node_hardware")) == 2


def test_node_hardware_without_boot_id_is_stored_as_unknown(migrated_db):
   recs = make_pipeline().on_message(full_hello(None), at(0))
   _write(migrated_db, [_first(recs, NODE_HARDWARE)])
   assert _one(migrated_db, "SELECT boot_id FROM node_hardware") == {"boot_id": "unknown"}
   # nodes.boot_id is only set from a known boot_id.
   assert _one(migrated_db, "SELECT boot_id FROM nodes") == {"boot_id": None}


def test_unknown_kind_rejected_without_partial_write(migrated_db):
   w = _writer(migrated_db)
   with pytest.raises(ValueError, match="bogus"):
      w.write([_event(NOW), Record("bogus", {"system": "polaris"})], NOW)
   assert _counts(migrated_db)["collection_events"] == 0
   # The writer is still usable.
   w.write([_event(NOW)], NOW)
   w.close()
   assert _counts(migrated_db)["collection_events"] == 1


def test_data_error_rolls_back_whole_batch_and_is_not_an_outage(migrated_db):
   w = _writer(migrated_db)
   bad = _metrics_row(datetime(2026, 11, 1, 12, 0, tzinfo=UTC), procs_total="not a number")
   with pytest.raises(psycopg.DataError):
      w.write([_event(NOW), bad], NOW)
   assert _counts(migrated_db)["collection_events"] == 0
   assert _counts(migrated_db)["node_metrics_1m"] == 0
   w.write([_event(NOW)], NOW)
   w.close()
   assert _counts(migrated_db)["collection_events"] == 1


@pytest.mark.parametrize(
   "exc, outage",
   [
      (psycopg.OperationalError("server closed the connection unexpectedly"), True),
      (psycopg.errors.ProgramLimitExceeded("index row size exceeds maximum"), False),
      (psycopg.errors.QueryCanceled("canceling statement due to statement timeout"), False),
      (psycopg.ProgrammingError("bug"), False),
   ],
)
def test_only_outages_become_database_unavailable(migrated_db, monkeypatch, exc, outage):
   w = _writer(migrated_db)

   def boom(self, conn, plan):
      raise exc

   monkeypatch.setattr(writer_mod.DatabaseWriter, "_apply", boom)
   if outage:
      with pytest.raises(DatabaseUnavailable) as info:
         w.write([_event(NOW)], NOW)
      assert info.value.__cause__ is exc
   else:
      with pytest.raises(type(exc)) as info:
         w.write([_event(NOW)], NOW)
      assert info.value is exc
   monkeypatch.undo()
   w.write([_event(NOW)], NOW)
   w.close()
   assert _counts(migrated_db)["collection_events"] == 1


def test_url_is_read_on_every_reconnect(migrated_db):
   w = _writer(UNREACHABLE, connect_timeout_sec=2)
   with pytest.raises(DatabaseUnavailable):
      w.write([_event(NOW)], NOW)
   w.url = migrated_db
   w.write([_event(NOW)], NOW)
   w.close()
   assert _counts(migrated_db)["collection_events"] == 1


def test_broken_connection_is_replaced_on_next_write(migrated_db, pg_admin_url):
   w = _writer(migrated_db)
   w.write([_event(NOW)], NOW)
   with psycopg.connect(pg_admin_url, autocommit=True) as admin:
      # The timeout makes it wait until the backend has gone.
      killed = admin.execute(
         "SELECT pg_terminate_backend(pid, 5000) FROM pg_stat_activity WHERE datname = %s",
         (migrated_db.rsplit("/", 1)[1],),
      ).fetchall()
   assert killed == [(True,)]
   with pytest.raises(DatabaseUnavailable):
      w.write([_event(NOW)], NOW)
   w.write([_event(NOW)], NOW)
   w.close()
   assert _counts(migrated_db)["collection_events"] == 2


def test_ensure_partition_reports_creation(migrated_db):
   month = datetime(2026, 12, 1, tzinfo=UTC)
   with connect(migrated_db, autocommit=True) as conn:
      assert ensure_partition(conn, "node_metrics_1m", month) is True
      assert ensure_partition(conn, "node_metrics_1m", month) is False
      bounds = conn.execute(
         "SELECT pg_get_expr(c.relpartbound, c.oid) FROM pg_class c WHERE c.relname = %s",
         ("node_metrics_1m_y2026m12",),
      ).fetchone()[0]
   assert "2026-12-01 00:00:00+00" in bounds and "2027-01-01 00:00:00+00" in bounds
