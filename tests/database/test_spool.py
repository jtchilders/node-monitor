import os
import stat
from datetime import datetime, timezone

import pytest

from node_monitor.database.spool import Spool
from node_monitor.pipeline.records import LIVE_NODE, LIVE_PROCESSES, Record

TS = datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc)


def rec(kind="node_metrics", n=0, pad=0):
   return Record(kind, {"ts": TS, "n": n, "pad": "x" * pad})


def test_append_and_pending_round_trip_in_order(tmp_path):
   spool = Spool(tmp_path / "spool", max_bytes=10**6)
   for i in range(3):
      assert spool.append([rec(n=i), rec(LIVE_NODE, n=i)]).stored
   got = list(spool.pending())
   assert [b[0].row["n"] for _, b in got] == [0, 1, 2]
   assert got[0][1][0].row["ts"] == TS
   assert spool.batch_count() == 3
   spool.remove(got[0][0])
   assert spool.batch_count() == 2
   assert spool.size_bytes() > 0


def test_files_are_private(tmp_path):
   d = tmp_path / "spool"
   spool = Spool(d, max_bytes=10**6)
   spool.append([rec()])
   assert stat.S_IMODE(d.stat().st_mode) == 0o700
   (path,) = [p for p, _ in spool.pending()]
   assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_existing_loose_directory_is_tightened(tmp_path):
   d = tmp_path / "spool"
   d.mkdir(mode=0o755)
   os.chmod(d, 0o755)
   Spool(d, max_bytes=10**6)
   assert stat.S_IMODE(d.stat().st_mode) == 0o700


def test_cap_drops_live_only_batches_first(tmp_path):
   spool = Spool(tmp_path, max_bytes=2500)
   spool.append([rec("node_metrics", 1, pad=1000)])
   spool.append([rec(LIVE_PROCESSES, 2, pad=1000), rec(LIVE_NODE, 2)])
   res = spool.append([rec("node_metrics", 3, pad=1000)])
   assert res.stored
   assert res.dropped_batches == 1
   assert res.dropped_records == 2
   assert [b[0].row["n"] for _, b in spool.pending()] == [1, 3]


def test_cap_drops_oldest_when_no_live_only_batches(tmp_path):
   spool = Spool(tmp_path, max_bytes=2500)
   for i in range(1, 4):
      spool.append([rec("node_metrics", i, pad=1000)])
   assert [b[0].row["n"] for _, b in spool.pending()] == [2, 3]
   assert spool.size_bytes() <= 2500
   # the third append dropped batch 1 to make room
   assert spool.batch_count() == 2


def test_oversized_batch_not_stored(tmp_path):
   spool = Spool(tmp_path, max_bytes=500)
   spool.append([rec(n=1)])
   res = spool.append([rec(pad=2000)])
   assert not res.stored
   assert res.error
   assert res.dropped_batches == 0
   assert spool.batch_count() == 1


@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores permission bits")
def test_spool_write_failure_never_raises(tmp_path):
   d = tmp_path / "spool"
   spool = Spool(d, max_bytes=10**6)
   os.chmod(d, 0o500)
   try:
      res = spool.append([rec()])
   finally:
      os.chmod(d, 0o700)
   assert not res.stored
   assert res.error


def test_corrupt_file_quarantined(tmp_path):
   spool = Spool(tmp_path, max_bytes=10**6)
   spool.append([rec(n=1)])
   bad = tmp_path / "19990101T000000.000000-000001.jsonl"
   bad.write_text("{not json\n")
   assert [b[0].row["n"] for _, b in spool.pending()] == [1]
   assert not bad.exists()
   assert (tmp_path / "corrupt" / bad.name).exists()
   assert stat.S_IMODE((tmp_path / "corrupt").stat().st_mode) == 0o700
   assert spool.batch_count() == 1


def test_crash_mid_write_leaves_no_partial_batch(tmp_path):
   spool = Spool(tmp_path, max_bytes=10**6)
   (tmp_path / ".20261101T000000.000000-000009.jsonl.tmp").write_text('{"kind": "x"')
   assert list(spool.pending()) == []
   assert spool.batch_count() == 0
   assert spool.size_bytes() == 0


def test_restart_never_reuses_a_name(tmp_path):
   a = Spool(tmp_path, max_bytes=10**6)
   a.append([rec(n=1)])
   b = Spool(tmp_path, max_bytes=10**6)
   b.append([rec(n=2)])
   assert b.batch_count() == 2
   assert [x[0].row["n"] for _, x in b.pending()] == [1, 2]
