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


def test_recovers_when_directory_deleted(tmp_path):
   import shutil

   d = tmp_path / "spool"
   spool = Spool(d, max_bytes=10**6)
   assert spool.append([rec(n=1)]).stored
   shutil.rmtree(d)
   assert spool.append([rec(n=2)]).stored
   assert [b[0].row["n"] for _, b in spool.pending()] == [2]


@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores permission bits")
def test_recovers_when_directory_uncreatable_at_startup(tmp_path):
   parent = tmp_path / "ro"
   parent.mkdir()
   os.chmod(parent, 0o500)
   try:
      spool = Spool(parent / "sub" / "spool", max_bytes=10**6)
      assert not spool.append([rec()]).stored
   finally:
      os.chmod(parent, 0o700)
   assert spool.append([rec(n=1)]).stored
   assert [b[0].row["n"] for _, b in spool.pending()] == [1]


def test_deeply_nested_corrupt_file_quarantined_once(tmp_path):
   spool = Spool(tmp_path, max_bytes=10**6)
   bad = tmp_path / "19990101T000000.000000-000001.jsonl"
   bad.write_text("[" * 100000 + "\n")
   assert list(spool.pending()) == []
   assert list(spool.pending()) == []
   assert [p.name for p in (tmp_path / "corrupt").iterdir()] == [bad.name]


def test_quarantine_moves_batch_out_of_the_spool(tmp_path):
   spool = Spool(tmp_path, max_bytes=10**6)
   spool.append([rec(n=1)])
   spool.append([rec(n=2)])
   path, _ = next(spool.pending())
   spool.quarantine(path)
   assert not path.exists()
   assert (tmp_path / "corrupt" / path.name).exists()
   assert [b[0].row["n"] for _, b in spool.pending()] == [2]
   assert spool.batch_count() == 1
   ((remaining, _),) = spool.pending()
   assert spool.size_bytes() == remaining.stat().st_size


def test_quarantine_never_raises(tmp_path):
   spool = Spool(tmp_path, max_bytes=10**6)
   spool.quarantine(tmp_path / "missing.jsonl")
   spool.quarantine("not even a path object")


def test_live_only_batches_are_marked_in_their_name(tmp_path):
   spool = Spool(tmp_path, max_bytes=10**6)
   spool.append([rec(LIVE_PROCESSES, 1), rec(LIVE_NODE, 1)])
   spool.append([rec(LIVE_NODE, 2), rec("node_metrics", 2)])
   live, mixed = [p.name for p, _ in spool.pending()]
   assert live.endswith("-000001-L.jsonl")
   assert mixed.endswith("-000002.jsonl")


def test_append_that_fits_opens_no_existing_file(tmp_path, monkeypatch):
   import builtins
   import io

   spool = Spool(tmp_path, max_bytes=10**7)
   for i in range(50):
      spool.append([rec(LIVE_NODE if i % 2 else "node_metrics", i, pad=100)])
   existing = {str(p) for p, _ in spool.pending()}
   assert len(existing) == 50
   opened = []
   real_builtin_open, real_io_open, real_os_open = builtins.open, io.open, os.open

   def spy(real):
      def wrapper(file, *args, **kwargs):
         opened.append(str(file))
         return real(file, *args, **kwargs)

      return wrapper

   monkeypatch.setattr(builtins, "open", spy(real_builtin_open))
   monkeypatch.setattr(io, "open", spy(real_io_open))
   monkeypatch.setattr(os, "open", spy(real_os_open))
   assert spool.append([rec(n=99)]).stored
   monkeypatch.undo()
   assert opened, "the spy saw no open at all"
   assert existing.isdisjoint(opened)


def test_eviction_opens_no_batch_it_keeps(tmp_path, monkeypatch):
   spool = Spool(tmp_path, max_bytes=2500)
   spool.append([rec("node_metrics", 1, pad=1000)])
   spool.append([rec(LIVE_PROCESSES, 2, pad=1000), rec(LIVE_NODE, 2)])
   kept = [str(p) for p, _ in spool.pending()][0]
   opened = []
   real_open = open

   def spy(file, *args, **kwargs):
      opened.append(str(file))
      return real_open(file, *args, **kwargs)

   monkeypatch.setattr("builtins.open", spy)
   res = spool.append([rec("node_metrics", 3, pad=1000)])
   monkeypatch.undo()
   assert res.dropped_batches == 1 and res.dropped_records == 2
   assert kept not in opened


def test_old_names_without_marker_are_kept_in_order_and_dropped_last(tmp_path):
   # Written by an earlier version: live-only, but its name does not say so.
   old = tmp_path / "19990101T000000.000000-000007.jsonl"
   spool_tmp = Spool(tmp_path / "w", max_bytes=10**6)
   spool_tmp.append([rec(LIVE_NODE, 0, pad=1000)])
   ((written, _),) = spool_tmp.pending()
   os.replace(written, old)
   # A new spool continues the sequence past the old name.
   spool = Spool(tmp_path, max_bytes=2500)
   spool.append([rec(LIVE_NODE, 1, pad=1000)])
   names = [p.name for p, _ in spool.pending()]
   assert names[0] == old.name and names[1].endswith("-000008-L.jsonl")
   # The marked live-only batch goes first; the unmarked one counts as ordinary data.
   res = spool.append([rec("node_metrics", 2, pad=1000)])
   assert res.dropped_batches == 1
   assert [b[0].row["n"] for _, b in spool.pending()] == [0, 2]


def test_pending_reports_quarantined_files_once(tmp_path):
   spool = Spool(tmp_path, max_bytes=10**6)
   spool.append([rec(n=1)])
   bad = tmp_path / "19990101T000000.000000-000001.jsonl"
   bad.write_text("{not json\n")
   assert spool.take_quarantined() == []
   list(spool.pending())
   ((name, reason),) = spool.take_quarantined()
   assert name == bad.name
   assert reason.startswith("JSONDecodeError: ")
   assert spool.take_quarantined() == []
   list(spool.pending())
   assert spool.take_quarantined() == []


def test_quarantine_records_writes_into_corrupt_in_spool_order(tmp_path):
   spool = Spool(tmp_path, max_bytes=10**6)
   spool.append([rec(n=1)])
   name = spool.quarantine_records([rec(n=2), rec(LIVE_NODE, 2)], "DataError: bad")
   spool.append([rec(n=3)])
   path = tmp_path / "corrupt" / name
   assert stat.S_IMODE(path.stat().st_mode) == 0o600
   assert stat.S_IMODE((tmp_path / "corrupt").stat().st_mode) == 0o700
   # Out of the spool, but moved back it replays in its original place.
   assert spool.batch_count() == 2
   os.replace(path, tmp_path / name)
   assert [b[0].row["n"] for _, b in spool.pending()] == [1, 2, 3]


def test_quarantine_records_never_raises(tmp_path):
   spool = Spool(tmp_path, max_bytes=10**6)
   (tmp_path / "corrupt").write_text("a file where the directory should be")
   assert spool.quarantine_records([rec()], "x") is None
   assert spool.quarantine_records([Record("x", {"bad": object()})], "x") is None


def test_unstorable_strings_round_trip_through_the_spool(tmp_path):
   # Lone surrogates and NULs are cleaned only when written to the database.
   spool = Spool(tmp_path, max_bytes=10**6)
   value = "caf\udcff a\x00b"
   assert spool.append([Record("node_metrics", {"exe": value})]).stored
   ((_, (got,)),) = spool.pending()
   assert got.row["exe"] == value
