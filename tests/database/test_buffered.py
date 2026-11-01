import json
from datetime import datetime, timedelta

import psycopg
import pytest
from psycopg.rows import dict_row

from node_monitor.config import RetentionConfig
from node_monitor.database.buffered import BufferedWriter
from node_monitor.database.codec import decode_record
from node_monitor.database.connect import DatabaseUnavailable, connect
from node_monitor.database.spool import Spool
from node_monitor.database.writer import DatabaseWriter, WriteResult
from node_monitor.pipeline.records import COLLECTION_EVENT, NODE_METRICS_1M, Record
from tests.database.sessions import scripted_records
from tests.pipeline import at

UNREACHABLE = "postgresql://localhost:1/x"
SYSTEM = "polaris"


def batch(name, t=0):
   """A small realistic batch whose marker event names it, so its arrival can be checked."""
   metrics = next(r for r in scripted_records() if r.kind == NODE_METRICS_1M)
   minute = at(t).replace(second=0, microsecond=0)
   return [
      Record(NODE_METRICS_1M, {**metrics.row, "minute": minute}),
      Record(
         COLLECTION_EVENT,
         {
            "system": SYSTEM,
            "node": "n1.example",
            "ts": at(t),
            "event_type": "batch",
            "detail": {"name": name},
         },
      ),
   ]


def names(batches):
   return [b[-1].row["detail"]["name"] for b in batches]


def _read(path):
   return [decode_record(json.loads(line)) for line in path.read_text().splitlines()]


def events_of(events, event_type):
   return [e for e in events if e.row["event_type"] == event_type]


def spooled_names(spool):
   return names(b for _, b in spool.pending())


class StubWriter:
   """Stands in for DatabaseWriter: every write is one connection attempt."""

   def __init__(self):
      self.down = False
      self.attempts = 0
      self.written = []
      # name -> exception raised when the batch carrying that marker is written.
      self.poison = {}
      # Fail as unavailable after this many more successful writes.
      self.fail_after = None
      self.result = WriteResult({}, {}, [])
      # None: the database answers exactly when it is not down.
      self.ping_result = None

   def ping(self):
      return (not self.down) if self.ping_result is None else self.ping_result

   def write(self, records, now):
      self.attempts += 1
      if self.down:
         raise DatabaseUnavailable("database unavailable: connection refused")
      if self.fail_after is not None:
         if self.fail_after == 0:
            raise DatabaseUnavailable("database unavailable: connection reset")
         self.fail_after -= 1
      name = records[-1].row["detail"].get("name") if records else None
      if name in self.poison:
         raise self.poison[name]
      self.written.append(records)
      return self.result


def make(tmp_path, writer=None, *, max_bytes=10**7, **kw):
   spool = Spool(tmp_path / "spool", max_bytes=max_bytes)
   writer = writer or StubWriter()
   return BufferedWriter(writer, spool, system=SYSTEM, **kw), writer, spool


# --- with a stub writer ------------------------------------------------------------------


def _fail_b_repeatedly(tmp_path, *, answers, rounds):
   bw, w, spool = make(tmp_path, backoff=(5, 300))
   w.down = True
   for name in "ABC":
      bw.submit(batch(name), at(0))
   w.down = False
   w.ping_result = answers
   w.poison["B"] = DatabaseUnavailable("database unavailable: out of memory")
   events = []
   t = 0
   for i in range(rounds):
      next_retry = bw.status()["next_retry"]
      if next_retry is not None:
         t = max(t, int((datetime.fromisoformat(next_retry) - at(0)).total_seconds()))
      events += bw.submit(batch(f"fresh{i}", t), at(t))
      t += 1
   return bw, w, spool, events


def test_spooled_batch_failing_while_database_answers_is_quarantined_after_3_attempts(tmp_path):
   bw, w, spool, events = _fail_b_repeatedly(tmp_path, answers=True, rounds=3)
   assert [p.name for p in (spool.directory / "corrupt").iterdir()]
   quarantined = events_of(events, "spool_batch_quarantined")
   assert len(quarantined) == 1
   assert quarantined[0].row["detail"]["reason"] == "repeated failure while the database answered"
   assert quarantined[0].row["detail"]["attempts"] == 3
   assert names(w.written) == ["A", "C", "fresh0", "fresh1", "fresh2"]
   assert spooled_names(spool) == []


def test_spooled_batch_is_never_quarantined_while_ping_fails(tmp_path):
   bw, w, spool, events = _fail_b_repeatedly(tmp_path, answers=False, rounds=10)
   corrupt = spool.directory / "corrupt"
   assert not corrupt.exists() or not list(corrupt.iterdir())
   assert "B" in spooled_names(spool)
   assert events_of(events, "spool_batch_quarantined") == []


def test_no_database_attempts_before_next_retry(tmp_path):
   bw, w, spool = make(tmp_path, backoff=(5, 300))
   w.down = True
   events = bw.submit(batch("A"), at(0))
   assert w.attempts == 1
   assert len(events_of(events, "database_unavailable")) == 1
   assert bw.status()["next_retry"] == at(5).isoformat()
   # Before the retry time nothing touches the database, however often submit is called.
   for t in (1, 2, 4):
      assert bw.submit(batch(f"t{t}", t), at(t)) == []
   assert w.attempts == 1
   assert spooled_names(spool) == ["A", "t1", "t2", "t4"]
   # At the retry time it tries again.
   bw.submit(batch("B", 5), at(5))
   assert w.attempts == 2


def test_backoff_doubles_and_caps(tmp_path):
   bw, w, _ = make(tmp_path, backoff=(5, 40))
   w.down = True
   t = at(0)
   delays = []
   for i in range(7):
      events = bw.submit(batch(str(i)), t)
      # Only the first failure begins an outage; later ones extend it.
      assert len(events_of(events, "database_unavailable")) == (1 if i == 0 else 0)
      next_retry = bw.status()["next_retry"]
      delays.append((type(t).fromisoformat(next_retry) - t).total_seconds())
      t = type(t).fromisoformat(next_retry)
   assert delays == [5, 10, 20, 40, 40, 40, 40]
   assert w.attempts == 7

   # Recovery resets the delay.
   w.down = False
   events = bw.submit(batch("up"), t)
   assert len(events_of(events, "database_recovered")) == 1
   w.down = True
   bw.submit(batch("down"), t)
   assert bw.status()["next_retry"] == (t + timedelta(seconds=5)).isoformat()


def test_recovery_replays_in_order_then_writes_fresh_batch(tmp_path):
   bw, w, spool = make(tmp_path)
   bw.submit(batch("A"), at(0))
   w.down = True
   bw.submit(batch("B"), at(1))
   bw.submit(batch("C"), at(2))
   w.down = False
   events = bw.submit(batch("D"), at(10))
   assert names(w.written) == ["A", "B", "C", "D"]
   assert spool.batch_count() == 0
   (recovered,) = events_of(events, "database_recovered")
   assert recovered.row["detail"] == {
      "outage_sec": 9.0,
      "replayed_batches": 2,
      "replayed_records": 4,
   }


def test_replay_stopping_partway_spools_fresh_batch_behind_the_rest(tmp_path):
   bw, w, spool = make(tmp_path)
   w.down = True
   for i, name in enumerate("ABC"):
      bw.submit(batch(name), at(i))
   w.down = False
   # The database fails again after replaying A.
   w.fail_after = 1
   events = bw.submit(batch("D"), at(10))
   assert names(w.written) == ["A"]
   assert spooled_names(spool) == ["B", "C", "D"]
   assert events_of(events, "database_recovered") == []
   # The outage is extended, not begun again.
   assert events_of(events, "database_unavailable") == []
   status = bw.status()
   assert status["outage_since"] == at(0).isoformat()
   assert status["next_retry"] == at(20).isoformat()

   w.fail_after = None
   events = bw.submit(batch("E"), at(20))
   assert names(w.written) == ["A", "B", "C", "D", "E"]
   (recovered,) = events_of(events, "database_recovered")
   assert recovered.row["detail"]["replayed_batches"] == 4


def test_events_are_collector_level_rows(tmp_path):
   bw, w, _ = make(tmp_path)
   w.down = True
   (event,) = bw.submit(batch("A"), at(0))
   assert event.kind == COLLECTION_EVENT
   assert event.row == {
      "system": SYSTEM,
      "node": None,
      "ts": at(0),
      "event_type": "database_unavailable",
      "detail": {"error": "DatabaseUnavailable: database unavailable: connection refused"},
   }


def test_status(tmp_path):
   bw, w, spool = make(tmp_path)
   assert bw.status() == {
      "connected": False,
      "outage_since": None,
      "spooled_batches": 0,
      "spooled_bytes": 0,
      "next_retry": None,
   }
   bw.submit(batch("A"), at(0))
   assert bw.status()["connected"] is True
   w.down = True
   bw.submit(batch("B"), at(1))
   status = bw.status()
   assert status["connected"] is False
   assert status["outage_since"] == at(1).isoformat()
   assert status["next_retry"] == at(6).isoformat()
   assert status["spooled_batches"] == 1
   assert status["spooled_bytes"] == spool.size_bytes() > 0


def test_spool_drop_event_surfaces(tmp_path):
   probe = Spool(tmp_path / "probe", max_bytes=10**6)
   probe.append(batch("x"))
   # Room for two batches, not three.
   bw, w, spool = make(tmp_path, max_bytes=2 * probe.size_bytes() + 10)
   w.down = True
   bw.submit(batch("A"), at(0))
   assert bw.submit(batch("B"), at(1)) == []
   events = bw.submit(batch("C"), at(2))
   (dropped,) = events_of(events, "spool_dropped")
   assert dropped.row["detail"] == {"batches": 1, "records": 2}
   assert spooled_names(spool) == ["B", "C"]


def test_spool_write_failure_is_reported_not_raised(tmp_path):
   bw, w, spool = make(tmp_path, max_bytes=100)
   w.down = True
   events = bw.submit(batch("A"), at(0))
   (failed,) = events_of(events, "spool_write_failed")
   assert "exceeds spool max_bytes" in failed.row["detail"]["error"]
   assert spool.batch_count() == 0
   # Still buffering: the next batch is tried against the spool too, and nothing raises.
   events = bw.submit(batch("B"), at(1))
   assert len(events_of(events, "spool_write_failed")) == 1


def test_poisoned_spooled_batch_is_quarantined_and_replay_continues(tmp_path):
   bw, w, spool = make(tmp_path)
   w.down = True
   for i, name in enumerate("ABC"):
      bw.submit(batch(name), at(i))
   (poisoned_path,) = [p for p, b in spool.pending() if names([b]) == ["B"]]
   w.down = False
   w.poison["B"] = psycopg.errors.DatatypeMismatch("x" * 500)
   events = bw.submit(batch("D"), at(10))
   assert names(w.written) == ["A", "C", "D"]
   assert spool.batch_count() == 0
   assert (spool.directory / "corrupt" / poisoned_path.name).exists()
   (quarantined,) = events_of(events, "spool_batch_quarantined")
   assert quarantined.row["detail"] == {
      "file": poisoned_path.name,
      "error": "DatatypeMismatch: " + "x" * 200,
   }
   (recovered,) = events_of(events, "database_recovered")
   assert recovered.row["detail"]["replayed_batches"] == 2


def test_poisoned_fresh_batch_propagates_and_keeps_its_events(tmp_path):
   bw, w, spool = make(tmp_path)
   w.down = True
   bw.submit(batch("A"), at(0))
   w.down = False
   w.poison["B"] = ValueError("bad batch")
   with pytest.raises(ValueError, match="bad batch"):
      bw.submit(batch("B"), at(10))
   # The spool was replayed first and the database is reachable: the outage is over.
   assert names(w.written) == ["A"]
   assert spool.batch_count() == 0
   assert bw.status()["connected"] is True
   # The events of the failed call are not lost: the next call returns them.
   events = bw.submit(batch("C"), at(11))
   (recovered,) = events_of(events, "database_recovered")
   assert recovered.row["detail"]["replayed_batches"] == 1
   assert len(events_of(events, "spool_batch_quarantined")) == 1


def test_rejected_fresh_batch_is_kept_in_corrupt(tmp_path):
   bw, w, spool = make(tmp_path)
   bw.submit(batch("A"), at(0))
   w.poison["B"] = psycopg.errors.UntranslatableCharacter("y" * 500)
   with pytest.raises(psycopg.errors.UntranslatableCharacter):
      bw.submit(batch("B"), at(10))
   # Not spooled, so it is not retried, but not lost either.
   assert spool.batch_count() == 0
   (kept,) = (spool.directory / "corrupt").iterdir()
   assert names([_read(kept)]) == ["B"]
   events = bw.submit(batch("C"), at(11))
   (quarantined,) = events_of(events, "spool_batch_quarantined")
   assert quarantined.row["detail"] == {
      "file": kept.name,
      "error": "UntranslatableCharacter: " + "y" * 200,
   }
   assert names(w.written) == ["A", "C"]


def test_unreadable_spool_file_is_reported(tmp_path):
   bw, w, spool = make(tmp_path)
   spool.append(batch("A"))
   bad = spool.directory / "19990101T000000.000000-000001.jsonl"
   bad.write_text("{not json\n")
   events = bw.submit(batch("B"), at(0))
   assert names(w.written) == ["A", "B"]
   (quarantined,) = events_of(events, "spool_batch_quarantined")
   assert quarantined.row["detail"]["file"] == bad.name
   assert quarantined.row["detail"]["error"].startswith("JSONDecodeError: ")
   assert (spool.directory / "corrupt" / bad.name).exists()
   # Reported once.
   assert events_of(bw.submit(batch("C"), at(1)), "spool_batch_quarantined") == []


def test_rows_beyond_retention_event(tmp_path):
   w = StubWriter()
   w.result = WriteResult({}, {"node_metrics_1m": 2}, [])
   bw, _, _ = make(tmp_path, w)
   (event,) = bw.submit(batch("A"), at(0))
   assert event.row["event_type"] == "rows_beyond_retention"
   assert event.row["detail"] == {"node_metrics_1m": 2}


def test_leftover_spool_is_replayed_before_first_write(tmp_path):
   spool = Spool(tmp_path / "spool", max_bytes=10**7)
   spool.append(batch("old"))
   bw, w, _ = make(tmp_path)
   events = bw.submit(batch("new"), at(0))
   assert names(w.written) == ["old", "new"]
   (recovered,) = events_of(events, "database_recovered")
   # The outage began before this process; its length is unknown.
   assert recovered.row["detail"] == {
      "outage_sec": None,
      "replayed_batches": 1,
      "replayed_records": 2,
   }


def test_empty_submit_outside_an_outage_does_not_touch_the_database(tmp_path):
   bw, w, _ = make(tmp_path)
   assert bw.submit([], at(0)) == []
   assert w.attempts == 0


def test_empty_submit_probes_for_recovery_when_nothing_is_spooled(tmp_path):
   bw, w, spool = make(tmp_path, max_bytes=100)
   w.down = True
   bw.submit(batch("A"), at(0))  # too big for the spool: lost, spool stays empty
   w.down = False
   events = bw.submit([], at(5))
   assert w.attempts == 2
   assert len(events_of(events, "database_recovered")) == 1
   assert bw.status()["outage_since"] is None


def test_naive_now_rejected(tmp_path):
   bw, _, _ = make(tmp_path)
   with pytest.raises(ValueError):
      bw.submit(batch("A"), at(0).replace(tzinfo=None))


# --- with a database ----------------------------------------------------------------------


def _event_names(url):
   with connect(url) as conn:
      conn.row_factory = dict_row
      rows = conn.execute(
         "SELECT detail->>'name' AS name FROM collection_events"
         " WHERE event_type = 'batch' ORDER BY id"
      ).fetchall()
   return [r["name"] for r in rows]


def _real(tmp_path, url):
   writer = DatabaseWriter(url, retention=RetentionConfig(), connect_timeout_sec=2)
   spool = Spool(tmp_path / "spool", max_bytes=10**7)
   return BufferedWriter(writer, spool, system=SYSTEM), writer, spool


def test_outage_spools_then_replays_in_order_without_loss(tmp_path, migrated_db):
   bw, writer, spool = _real(tmp_path, migrated_db)
   try:
      events = bw.submit(scripted_records() + batch("A", 0), at(0))
      assert events == []

      writer.url = UNREACHABLE
      # The URL is read on reconnect; drop the open session so the next write reconnects.
      writer.close()
      events = bw.submit(batch("B", 60), at(60))
      events += bw.submit(events + batch("C", 70), at(70))
      assert len(events_of(events, "database_unavailable")) == 1
      assert spool.batch_count() == 2

      writer.url = migrated_db
      next_retry = type(at(0)).fromisoformat(bw.status()["next_retry"])
      events = bw.submit(batch("D", 120), next_retry)
      assert spool.batch_count() == 0
      (recovered,) = events_of(events, "database_recovered")
      assert recovered.row["detail"]["replayed_batches"] == 2
      assert _event_names(migrated_db) == ["A", "B", "C", "D"]
      # The outage's own event, carried into C's spooled batch, reached the database.
      with connect(migrated_db) as conn:
         (n,) = conn.execute(
            "SELECT count(*) FROM collection_events"
            " WHERE event_type = 'database_unavailable' AND node IS NULL"
         ).fetchone()
      assert n == 1
   finally:
      writer.close()


def test_backend_terminated_mid_session(tmp_path, migrated_db, pg_admin_url):
   bw, writer, spool = _real(tmp_path, migrated_db)
   try:
      assert bw.submit(batch("A", 0), at(0)) == []
      with psycopg.connect(pg_admin_url, autocommit=True) as admin:
         # Only the writer's own session in this test's database; the timeout waits until
         # the backend has gone.
         killed = admin.execute(
            "SELECT pg_terminate_backend(pid, 5000) FROM pg_stat_activity"
            " WHERE datname = %s AND backend_type = 'client backend' AND pid = %s",
            (migrated_db.rsplit("/", 1)[1], writer._conn.info.backend_pid),
         ).fetchall()
      assert killed == [(True,)]
      events = bw.submit(batch("B", 60), at(60))
      if events_of(events, "database_unavailable"):
         assert spooled_names(spool) == ["B"]
         next_retry = type(at(0)).fromisoformat(bw.status()["next_retry"])
         bw.submit(events + batch("C", 120), next_retry)
      else:
         bw.submit(batch("C", 120), at(120))
      assert spool.batch_count() == 0
      assert _event_names(migrated_db) == ["A", "B", "C"]
   finally:
      writer.close()
