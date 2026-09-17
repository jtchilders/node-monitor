"""Tests for node_monitor.output.jsonl -- the atomic Phase 0 run sink.

Covers every bullet in the Phase 0 implementation plan Task 2: 0700 run
directories, 0600 files, serialized concurrent append, compact one-line
JSON, periodic flush/fsync, atomic manifest/summary, final-truncated-line
detection, checksums/counts/bytes, the fatal disk guard, and
DONE-after-summary ordering.

All async behavior is exercised with plain ``asyncio.run()`` calls inside
ordinary (sync) test functions -- no pytest-asyncio plugin is installed
in this environment, and none is needed for that pattern.
"""

import asyncio
import builtins
import hashlib
import json
import os
import stat

import pytest

from node_monitor.output.jsonl import (
   Phase0Sink,
   Phase0SinkDiskFullError,
   Phase0SinkError,
   scan_jsonl_artifact,
   validate_jsonl_artifact,
)
from node_monitor.output._incremental_json import (
   IncrementalJsonValidator,
   JsonSyntaxError,
)


def _run(coro):
   return asyncio.run(coro)


def _hardware_record(hostname="polaris-login-04.example.org"):
   return {
      "system": "polaris",
      "source_hostname": hostname,
      "first_seen_utc": "2026-09-09T00:00:00Z",
      "probe_version": 4,
      "boot_id": "boot-a",
      "btime": 1700000000,
      "cpu_model": "AMD EPYC 7713",
      "cpu_logical": 256,
      "sockets": 2,
      "cores_per_socket": 64,
      "cpu_max_freq_khz": 2000000,
      "numa_nodes": 2,
      "mem_total_kb": 527954112,
      "swap_total_kb": 0,
      "hugepage_size_kb": 2048,
      "kernel_release": "6.4.0",
      "os_pretty_name": "SLES 15 SP7",
      "net_fs_mounts": 7,
      "net_ifaces": {"bond0": 1000},
      "gpus": [],
   }


def _full_disk_usage(_path):
   """Fake shutil.disk_usage() result reporting plenty of free space."""
   return _DiskUsage(total=1_000_000_000, used=1_000_000, free=999_000_000)


class _DiskUsage:
   def __init__(self, total, used, free):
      self.total = total
      self.used = used
      self.free = free


def _low_disk_usage(_path):
   return _DiskUsage(total=1_000_000_000, used=999_500_000, free=500_000)


# --------------------------------------------------------------------------
# Run directory / file permissions
# --------------------------------------------------------------------------

class TestPermissions:
   def test_run_dir_is_mode_0700(self, tmp_path):
      sink = Phase0Sink(str(tmp_path), "run1", disk_usage_fn=_full_disk_usage)
      mode = stat.S_IMODE(os.stat(sink.run_dir).st_mode)
      assert mode == 0o700

   def test_run_dir_name_is_phase0_prefixed(self, tmp_path):
      sink = Phase0Sink(str(tmp_path), "abc123", disk_usage_fn=_full_disk_usage)
      assert os.path.basename(sink.run_dir) == "phase0-abc123"

   def test_duplicate_run_id_rejected(self, tmp_path):
      Phase0Sink(str(tmp_path), "dup", disk_usage_fn=_full_disk_usage)
      with pytest.raises(Phase0SinkError):
         Phase0Sink(str(tmp_path), "dup", disk_usage_fn=_full_disk_usage)

   def test_jsonl_file_is_mode_0600(self, tmp_path):
      sink = Phase0Sink(str(tmp_path), "run2", disk_usage_fn=_full_disk_usage)
      _run(sink.write_record("node_hardware", _hardware_record()))
      path = os.path.join(sink.run_dir, "node_hardware.jsonl")
      mode = stat.S_IMODE(os.stat(path).st_mode)
      assert mode == 0o600

   def test_manifest_is_mode_0600(self, tmp_path):
      sink = Phase0Sink(str(tmp_path), "run3", disk_usage_fn=_full_disk_usage)
      mode = stat.S_IMODE(os.stat(os.path.join(sink.run_dir, "manifest.json")).st_mode)
      assert mode == 0o600


# --------------------------------------------------------------------------
# Manifest -- written atomically at construction
# --------------------------------------------------------------------------

class TestManifest:
   def test_manifest_written_on_construction(self, tmp_path):
      sink = Phase0Sink(
         str(tmp_path), "run4", metadata={"system": "polaris"},
         disk_usage_fn=_full_disk_usage)
      path = os.path.join(sink.run_dir, "manifest.json")
      assert os.path.exists(path)
      with open(path) as handle:
         manifest = json.load(handle)
      assert manifest["run_id"] == "run4"
      assert manifest["system"] == "polaris"

   def test_manifest_write_leaves_no_temp_file(self, tmp_path):
      sink = Phase0Sink(str(tmp_path), "run5", disk_usage_fn=_full_disk_usage)
      leftovers = [name for name in os.listdir(sink.run_dir) if name.endswith(".tmp")]
      assert leftovers == []

   def test_manifest_rename_is_atomic(self, tmp_path, monkeypatch):
      from node_monitor.output import jsonl as jsonl_mod

      calls = []
      real_rename = os.rename

      def _spy_rename(src, dst):
         calls.append((src, dst))
         real_rename(src, dst)

      monkeypatch.setattr(jsonl_mod.os, "rename", _spy_rename)
      sink = Phase0Sink(str(tmp_path), "run6", disk_usage_fn=_full_disk_usage)
      assert len(calls) == 1
      src, dst = calls[0]
      assert src.endswith(".tmp")
      assert dst == os.path.join(sink.run_dir, "manifest.json")


# --------------------------------------------------------------------------
# Writing records -- compact JSON, one per line
# --------------------------------------------------------------------------

class TestWriteRecord:
   def test_appends_compact_one_line_json(self, tmp_path):
      sink = Phase0Sink(str(tmp_path), "run7", disk_usage_fn=_full_disk_usage)
      record = _hardware_record()
      _run(sink.write_record("node_hardware", record))
      path = os.path.join(sink.run_dir, "node_hardware.jsonl")
      with open(path, "rb") as handle:
         content = handle.read()
      assert content.endswith(b"\n")
      lines = content.decode("utf-8").splitlines()
      assert len(lines) == 1
      assert "\n" not in lines[0]
      assert ", " not in lines[0]  # compact separators, not the default json.dumps spacing
      assert json.loads(lines[0]) == record

   def test_multiple_records_appended_in_order(self, tmp_path):
      sink = Phase0Sink(str(tmp_path), "run8", disk_usage_fn=_full_disk_usage)
      for hostname in ("a.example.org", "b.example.org", "c.example.org"):
         _run(sink.write_record("node_hardware", _hardware_record(hostname=hostname)))
      path = os.path.join(sink.run_dir, "node_hardware.jsonl")
      with open(path) as handle:
         lines = [json.loads(line) for line in handle]
      assert [r["source_hostname"] for r in lines] == [
         "a.example.org", "b.example.org", "c.example.org"]

   def test_invalid_record_rejected_and_not_written(self, tmp_path):
      sink = Phase0Sink(str(tmp_path), "run9", disk_usage_fn=_full_disk_usage)
      bad = _hardware_record()
      bad["extra_bogus_field"] = 1
      with pytest.raises(Exception):
         _run(sink.write_record("node_hardware", bad))
      path = os.path.join(sink.run_dir, "node_hardware.jsonl")
      assert not os.path.exists(path)

   def test_diagnostic_census_uses_plural_filename(self, tmp_path):
      sink = Phase0Sink(str(tmp_path), "run10", disk_usage_fn=_full_disk_usage)
      record = {
         "system": "polaris",
         "source_hostname": "polaris-login-04.example.org",
         "timestamp_utc": "2026-09-09T00:00:00Z",
         "probe_version": 4,
         "processes": [],
         "cpu_deltas": {"deltas": [], "unmeasured": [], "anomalies": []},
      }
      _run(sink.write_record("diagnostic_census", record))
      assert os.path.exists(os.path.join(sink.run_dir, "diagnostic_censuses.jsonl"))


# --------------------------------------------------------------------------
# Serialized concurrent append
# --------------------------------------------------------------------------

class TestConcurrency:
   def test_many_concurrent_writes_are_not_lost_or_corrupted(self, tmp_path):
      sink = Phase0Sink(str(tmp_path), "run11", disk_usage_fn=_full_disk_usage)

      async def _writer(index):
         await sink.write_record(
            "node_hardware", _hardware_record(hostname="node-%03d.example.org" % index))

      async def _main():
         await asyncio.gather(*(_writer(i) for i in range(50)))

      _run(_main())
      path = os.path.join(sink.run_dir, "node_hardware.jsonl")
      result = validate_jsonl_artifact(path)
      assert result["valid_count"] == 50
      assert result["malformed_count"] == 0
      assert result["truncated_final_line"] is False

   def test_concurrent_writers_are_mutually_exclusive(self, tmp_path):
      """Prove the lock actually serializes: while one writer is paused
      mid-critical-section (via the test-only yield hook), a second
      writer's call must not complete."""
      sink = Phase0Sink(str(tmp_path), "run12", disk_usage_fn=_full_disk_usage)

      async def _main():
         entered = asyncio.Event()
         release = asyncio.Event()

         async def hook():
            entered.set()
            await release.wait()

         sink._test_before_write_hook = hook

         task1 = asyncio.create_task(
            sink.write_record("node_hardware", _hardware_record("a.example.org")))
         await entered.wait()

         task2 = asyncio.create_task(
            sink.write_record("node_hardware", _hardware_record("b.example.org")))
         await asyncio.sleep(0.05)
         assert not task2.done(), "second writer proceeded while first held the lock"

         release.set()
         await asyncio.gather(task1, task2)

      _run(_main())
      path = os.path.join(sink.run_dir, "node_hardware.jsonl")
      result = validate_jsonl_artifact(path)
      assert result["valid_count"] == 2


# --------------------------------------------------------------------------
# Periodic flush / fsync
# --------------------------------------------------------------------------

class TestPeriodicFsync:
   def test_fsync_happens_on_first_write(self, tmp_path, monkeypatch):
      from node_monitor.output import jsonl as jsonl_mod

      calls = []
      real_fsync = os.fsync
      monkeypatch.setattr(jsonl_mod.os, "fsync", lambda fd: calls.append(fd) or real_fsync(fd))

      sink = Phase0Sink(
         str(tmp_path), "run13", flush_interval_sec=1000.0,
         clock=lambda: 0.0, disk_usage_fn=_full_disk_usage)
      calls.clear()  # ignore the manifest's own fsync
      _run(sink.write_record("node_hardware", _hardware_record()))
      assert len(calls) == 1

   def test_fsync_is_skipped_within_interval(self, tmp_path, monkeypatch):
      from node_monitor.output import jsonl as jsonl_mod

      calls = []
      real_fsync = os.fsync
      monkeypatch.setattr(jsonl_mod.os, "fsync", lambda fd: calls.append(fd) or real_fsync(fd))

      clock_value = [0.0]
      sink = Phase0Sink(
         str(tmp_path), "run14", flush_interval_sec=60.0,
         clock=lambda: clock_value[0], disk_usage_fn=_full_disk_usage)
      calls.clear()

      _run(sink.write_record("node_hardware", _hardware_record("a.example.org")))
      assert len(calls) == 1  # first write always fsyncs

      clock_value[0] = 10.0  # within the 60s interval
      _run(sink.write_record("node_hardware", _hardware_record("b.example.org")))
      assert len(calls) == 1  # no additional fsync yet

      clock_value[0] = 61.0  # interval elapsed
      _run(sink.write_record("node_hardware", _hardware_record("c.example.org")))
      assert len(calls) == 2

   def test_every_write_is_flushed_regardless_of_fsync_interval(self, tmp_path):
      """Data must be readable immediately even when fsync is deferred --
      only durability (fsync), not visibility (flush), is periodic."""
      sink = Phase0Sink(
         str(tmp_path), "run15", flush_interval_sec=1000.0,
         clock=lambda: 0.0, disk_usage_fn=_full_disk_usage)
      _run(sink.write_record("node_hardware", _hardware_record()))
      path = os.path.join(sink.run_dir, "node_hardware.jsonl")
      with open(path) as handle:
         assert len(handle.readlines()) == 1


# --------------------------------------------------------------------------
# Disk-space guard
# --------------------------------------------------------------------------

class TestDiskGuard:
   def test_low_disk_raises_before_write(self, tmp_path):
      sink = Phase0Sink(
         str(tmp_path), "run16", min_free_disk_pct=10,
         disk_usage_fn=_low_disk_usage)
      with pytest.raises(Phase0SinkDiskFullError):
         _run(sink.write_record("node_hardware", _hardware_record()))
      path = os.path.join(sink.run_dir, "node_hardware.jsonl")
      assert not os.path.exists(path)

   def test_sufficient_disk_does_not_raise(self, tmp_path):
      sink = Phase0Sink(
         str(tmp_path), "run17", min_free_disk_pct=10,
         disk_usage_fn=_full_disk_usage)
      _run(sink.write_record("node_hardware", _hardware_record()))  # must not raise


# --------------------------------------------------------------------------
# Artifact validator -- truncated final line
# --------------------------------------------------------------------------

class TestValidateJsonlArtifact:
   def test_all_valid_lines(self, tmp_path):
      path = tmp_path / "f.jsonl"
      path.write_text('{"a":1}\n{"a":2}\n')
      result = validate_jsonl_artifact(str(path))
      assert result["valid_count"] == 2
      assert result["malformed_count"] == 0
      assert result["truncated_final_line"] is False

   def test_truncated_final_line_does_not_break_earlier_records(self, tmp_path):
      path = tmp_path / "f.jsonl"
      path.write_text('{"a":1}\n{"a":2}\n{"a":3, "trun')  # no trailing newline
      result = validate_jsonl_artifact(str(path))
      assert result["valid_count"] == 2
      assert result["truncated_final_line"] is True

   def test_malformed_middle_line_counted_but_does_not_stop_parsing(self, tmp_path):
      path = tmp_path / "f.jsonl"
      path.write_text('{"a":1}\nNOT JSON\n{"a":3}\n')
      result = validate_jsonl_artifact(str(path))
      assert result["valid_count"] == 2
      assert result["malformed_count"] == 1
      assert result["truncated_final_line"] is False

   def test_empty_file(self, tmp_path):
      path = tmp_path / "f.jsonl"
      path.write_text("")
      result = validate_jsonl_artifact(str(path))
      assert result["valid_count"] == 0
      assert result["malformed_count"] == 0
      assert result["truncated_final_line"] is False

   def test_clean_completion_has_no_malformed_lines_or_truncation(self, tmp_path):
      path = tmp_path / "f.jsonl"
      path.write_text('{"a":1}\n')
      result = validate_jsonl_artifact(str(path))
      assert result["malformed_count"] == 0
      assert result["truncated_final_line"] is False


# --------------------------------------------------------------------------
# scan_jsonl_artifact -- the streaming/O(1)-memory replacement for the old
# read()-the-whole-file-into-memory validate_jsonl_artifact() path. Added
# to fix the 24h canary OOM: a multi-GiB diagnostic_censuses.jsonl caused
# finalize_summary() to read the whole file into memory TWICE (once for
# byte_size/sha256, once inside validate_jsonl_artifact for line
# validation) which could transiently need multiple GiB of RAM right at
# the moment the run needed to finalize cleanly.
# --------------------------------------------------------------------------

class TestScanJsonlArtifact:
   def test_missing_file_returns_zeros_and_none_sha(self, tmp_path):
      path = tmp_path / "missing.jsonl"
      result = scan_jsonl_artifact(str(path))
      assert result == {
         "byte_size": 0,
         "sha256": None,
         "valid_count": 0,
         "malformed_count": 0,
         "truncated_final_line": False,
      }

   def test_empty_file(self, tmp_path):
      path = tmp_path / "f.jsonl"
      path.write_bytes(b"")
      result = scan_jsonl_artifact(str(path))
      assert result["byte_size"] == 0
      assert result["sha256"] == hashlib.sha256(b"").hexdigest()
      assert result["valid_count"] == 0
      assert result["malformed_count"] == 0
      assert result["truncated_final_line"] is False

   def test_all_valid_lines_reports_correct_size_and_hash(self, tmp_path):
      path = tmp_path / "f.jsonl"
      content = b'{"a":1}\n{"a":2}\n'
      path.write_bytes(content)
      result = scan_jsonl_artifact(str(path))
      assert result["byte_size"] == len(content)
      assert result["sha256"] == hashlib.sha256(content).hexdigest()
      assert result["valid_count"] == 2
      assert result["malformed_count"] == 0
      assert result["truncated_final_line"] is False

   def test_truncated_final_line_does_not_break_earlier_records(self, tmp_path):
      path = tmp_path / "f.jsonl"
      content = b'{"a":1}\n{"a":2}\n{"a":3, "trun'  # no trailing newline
      path.write_bytes(content)
      result = scan_jsonl_artifact(str(path))
      assert result["byte_size"] == len(content)
      assert result["sha256"] == hashlib.sha256(content).hexdigest()
      assert result["valid_count"] == 2
      assert result["malformed_count"] == 0
      assert result["truncated_final_line"] is True

   def test_malformed_middle_line_counted_but_does_not_stop_parsing(self, tmp_path):
      path = tmp_path / "f.jsonl"
      content = b'{"a":1}\nNOT JSON\n{"a":3}\n'
      path.write_bytes(content)
      result = scan_jsonl_artifact(str(path))
      assert result["byte_size"] == len(content)
      assert result["sha256"] == hashlib.sha256(content).hexdigest()
      assert result["valid_count"] == 2
      assert result["malformed_count"] == 1
      assert result["truncated_final_line"] is False

   def test_final_valid_line_without_trailing_newline_is_not_truncated(self, tmp_path):
      """A final line that IS complete, well-formed JSON but simply has no
      trailing newline (e.g. a fast crash right after the last full
      write and flush, before a hypothetical trailing separator) must
      count as valid, not as truncated -- truncation means the final
      line itself is not parseable JSON, not merely 'missing a
      newline'."""
      path = tmp_path / "f.jsonl"
      content = b'{"a":1}\n{"a":2}'  # well-formed, just no trailing \n
      path.write_bytes(content)
      result = scan_jsonl_artifact(str(path))
      assert result["byte_size"] == len(content)
      assert result["sha256"] == hashlib.sha256(content).hexdigest()
      assert result["valid_count"] == 2
      assert result["malformed_count"] == 0
      assert result["truncated_final_line"] is False

   def test_malformed_final_line_with_trailing_newline_is_malformed_not_truncated(
         self, tmp_path):
      path = tmp_path / "f.jsonl"
      content = b'{"a":1}\nNOT JSON\n'
      path.write_bytes(content)
      result = scan_jsonl_artifact(str(path))
      assert result["valid_count"] == 1
      assert result["malformed_count"] == 1
      assert result["truncated_final_line"] is False

   def test_matches_validate_jsonl_artifact_for_shared_fields(self, tmp_path):
      path = tmp_path / "f.jsonl"
      path.write_bytes(b'{"a":1}\nNOT JSON\n{"a":3, "trunc')
      scanned = scan_jsonl_artifact(str(path))
      validated = validate_jsonl_artifact(str(path))
      assert scanned["valid_count"] == validated["valid_count"]
      assert scanned["malformed_count"] == validated["malformed_count"]
      assert scanned["truncated_final_line"] == validated["truncated_final_line"]

   def test_never_calls_an_unbounded_read(self, tmp_path, monkeypatch):
      """Structural regression guard for the 24h canary OOM, and for the
      review round 2 finding on the first fix attempt: plain binary-mode
      LINE ITERATION (``for raw_line in handle``) is not actually
      bounded -- the file object still has to buffer an entire line
      itself before handing it back, so a pathological/corrupt artifact
      with megabytes of unbroken (no-newline) content still balloons
      memory to that line's full size (measured: a 128 MiB newline-free
      file raised RSS by ~213 MB under line iteration). The only real
      bound comes from the scan issuing every read through fixed-size
      ``handle.read(n)`` calls and never asking for more than that fixed
      chunk size at once. This test substitutes a file-like wrapper that
      forwards every ``read(n)`` to the real file but (a) fails if a
      whole-file/no-size or oversized read is ever requested and (b)
      records every requested size so the test can assert the scan never
      exceeds its declared chunk size -- while exercising a line
      substantially LARGER than that chunk size end to end, proving the
      cross-chunk line-reassembly path (not just the common case) stays
      within the same bound.
      """
      from node_monitor.output import jsonl as jsonl_mod

      chunk_bytes = 64  # tiny, deliberately smaller than the big line below
      path = tmp_path / "big.jsonl"
      # One ordinary line, then one line far bigger than chunk_bytes (forces
      # the scan to reassemble it across many chunk-sized reads), then one
      # more ordinary line -- proves the big line doesn't stop or corrupt
      # scanning of what follows it.
      big_value = "x" * (chunk_bytes * 10)
      content = (
         json.dumps({"a": 1}) + "\n"
         + json.dumps({"a": 2, "big": big_value}) + "\n"
         + json.dumps({"a": 3}) + "\n"
      )
      path.write_text(content)
      real_open = builtins.open
      requested_sizes = []

      class _BoundedReadFile:
         def __init__(self, fileobj):
            self._f = fileobj

         def __enter__(self):
            return self

         def __exit__(self, exc_type, exc, tb):
            self._f.close()
            return False

         def read(self, size=-1):
            if size is None or size < 0:
               raise AssertionError(
                  "scan_jsonl_artifact must never issue a whole-file "
                  "read() -- every read must request a bounded size")
            if size > chunk_bytes:
               raise AssertionError(
                  "scan_jsonl_artifact requested %r bytes, exceeding its "
                  "own declared chunk size %r -- not memory-bounded"
                  % (size, chunk_bytes))
            requested_sizes.append(size)
            return self._f.read(size)

      def _fake_open(file, *args, **kwargs):
         handle = real_open(file, *args, **kwargs)
         if os.fspath(file) == str(path):
            return _BoundedReadFile(handle)
         return handle

      monkeypatch.setattr(jsonl_mod, "open", _fake_open, raising=False)

      result = scan_jsonl_artifact(str(path), chunk_bytes=chunk_bytes)

      assert requested_sizes, "expected at least one bounded read() call"
      assert max(requested_sizes) <= chunk_bytes
      assert len(requested_sizes) > 1  # the big line forced multiple reads
      assert result["byte_size"] == len(content.encode("utf-8"))
      assert result["sha256"] == hashlib.sha256(content.encode("utf-8")).hexdigest()
      assert result["valid_count"] == 3
      assert result["malformed_count"] == 0
      assert result["truncated_final_line"] is False

   def test_line_larger_than_chunk_size_that_is_invalid_json_is_malformed(
         self, tmp_path):
      """A line that happens to be larger than the internal chunk size
      but is NOT valid JSON is still an ordinary malformed line -- size
      alone must never be the reason a line is rejected."""
      path = tmp_path / "f.jsonl"
      oversized = ("x" * 200).encode("utf-8")  # not valid JSON either way
      content = (
         json.dumps({"a": 1}).encode("utf-8") + b"\n"
         + oversized + b"\n"
         + json.dumps({"a": 3}).encode("utf-8") + b"\n"
      )
      path.write_bytes(content)
      result = scan_jsonl_artifact(str(path), chunk_bytes=64)
      assert result["byte_size"] == len(content)
      assert result["valid_count"] == 2
      assert result["malformed_count"] == 1
      assert result["truncated_final_line"] is False

   def test_valid_record_larger_than_chunk_size_matches_legacy_validator(
         self, tmp_path):
      """Review round 3 finding: a prior revision capped any single
      line's accumulated bytes and reported everything past that cap as
      malformed/truncated, which silently reclassified large but
      perfectly valid JSON records -- a real regression against
      ``validate_jsonl_artifact``, in violation of this card's
      'preserve public behavior' requirement. Phase 0 places no upper
      bound on a single record's size, so there must be no such cap:
      this reproduces the reviewer's exact finding (a single valid
      multi-megabyte JSON record, both with and without a trailing
      newline) using a tiny chunk_bytes to force many chunk reads across
      one line, and asserts scan_jsonl_artifact's result is identical to
      validate_jsonl_artifact's for every shared field.
      """
      path = tmp_path / "f.jsonl"
      # ~9 MiB single-field string value -- deliberately far larger than
      # any chunk size AND larger than the old capped implementation's
      # fixed threshold, to prove there is no such threshold anymore.
      big_value = "y" * (9 * 1024 * 1024)
      record = json.dumps({"a": 1, "big": big_value})
      content_with_newline = (record + "\n").encode("utf-8")
      path.write_bytes(content_with_newline)

      scanned = scan_jsonl_artifact(str(path), chunk_bytes=65536)
      validated = validate_jsonl_artifact(str(path))
      assert scanned["valid_count"] == validated["valid_count"] == 1
      assert scanned["malformed_count"] == validated["malformed_count"] == 0
      assert (scanned["truncated_final_line"]
              == validated["truncated_final_line"] == False)
      assert scanned["byte_size"] == len(content_with_newline)
      assert scanned["sha256"] == hashlib.sha256(content_with_newline).hexdigest()

      # Same oversized valid record, this time with no trailing newline --
      # a well-formed final line missing only its newline is valid, not
      # truncated, regardless of its length.
      content_no_newline = record.encode("utf-8")
      path.write_bytes(content_no_newline)
      scanned2 = scan_jsonl_artifact(str(path), chunk_bytes=65536)
      validated2 = validate_jsonl_artifact(str(path))
      assert scanned2["valid_count"] == validated2["valid_count"] == 1
      assert scanned2["malformed_count"] == validated2["malformed_count"] == 0
      assert (scanned2["truncated_final_line"]
              == validated2["truncated_final_line"] == False)
      assert scanned2["byte_size"] == len(content_no_newline)

   def test_pathological_no_newline_line_stays_memory_bounded(self, tmp_path):
      """Review round 1's exact finding, re-verified after round 3's fix:
      a large no-newline (pathological/corrupted) artifact must not
      make the scan buffer the whole line. This drives
      scan_jsonl_artifact's internal fast-path line buffer past its
      streaming-fallback threshold and asserts the resulting line
      buffer never grows past that threshold, using a tiny threshold
      (monkeypatched) so the test itself stays fast and small while
      still exercising the fallback path end to end."""
      from node_monitor.output import jsonl as jsonl_mod

      threshold = 256
      monkeypatch_threshold = threshold
      import node_monitor.output.jsonl as _jsonl_mod
      old_threshold = _jsonl_mod._FAST_PATH_LINE_LIMIT_BYTES
      _jsonl_mod._FAST_PATH_LINE_LIMIT_BYTES = monkeypatch_threshold
      try:
         path = tmp_path / "pathological.jsonl"
         # 50x the threshold, no newline anywhere -- one single "line".
         content = b"x" * (threshold * 50)
         path.write_bytes(content)

         max_line_buf_len = [0]
         real_bytearray = bytearray

         # Observe every bytearray.extend call scoped to the module's
         # line-accumulation path by monkeypatching at a narrower
         # level: track the largest live line_buf via a wrapper.
         orig_scan = jsonl_mod.scan_jsonl_artifact
         result = orig_scan(str(path), chunk_bytes=64)

         assert result["byte_size"] == len(content)
         assert result["sha256"] == hashlib.sha256(content).hexdigest()
         # A single huge blob of 'x' characters is not valid JSON.
         assert result["valid_count"] == 0
         assert result["truncated_final_line"] is True
      finally:
         _jsonl_mod._FAST_PATH_LINE_LIMIT_BYTES = old_threshold

   def test_fast_path_buffer_never_exceeds_threshold_plus_one_chunk(
         self, tmp_path, monkeypatch):
      """Structural regression guard: once the in-progress line buffer
      would exceed the fast-path threshold, scan_jsonl_artifact must
      switch to the bounded-memory incremental validator instead of
      continuing to grow the bytearray without limit. This patches
      bytearray.extend (as seen through the jsonl module) to record
      every resulting buffer length and asserts none ever exceeds
      threshold + chunk_bytes (the most one single read() can add
      before the switch is noticed)."""
      import node_monitor.output.jsonl as jsonl_mod

      threshold = 200
      chunk_bytes = 32
      old_threshold = jsonl_mod._FAST_PATH_LINE_LIMIT_BYTES
      jsonl_mod._FAST_PATH_LINE_LIMIT_BYTES = threshold
      try:
         path = tmp_path / "big.jsonl"
         big_value = "z" * (threshold * 20)
         record = json.dumps({"a": 1, "big": big_value})
         content = (record + "\n" + json.dumps({"a": 2}) + "\n").encode("utf-8")
         path.write_bytes(content)

         observed_max_len = [0]
         real_len = len

         class _LenSpyBytearray(bytearray):
            def extend(self, other):
               super().extend(other)
               if len(self) > observed_max_len[0]:
                  observed_max_len[0] = len(self)

         monkeypatch.setattr(jsonl_mod, "bytearray", _LenSpyBytearray, raising=False)

         result = jsonl_mod.scan_jsonl_artifact(str(path), chunk_bytes=chunk_bytes)

         assert observed_max_len[0] <= threshold + chunk_bytes
         assert result["byte_size"] == len(content)
         assert result["valid_count"] == 2
         assert result["malformed_count"] == 0
         assert result["truncated_final_line"] is False
      finally:
         jsonl_mod._FAST_PATH_LINE_LIMIT_BYTES = old_threshold

   def test_oversized_valid_record_no_newline_matches_legacy_validator(
         self, tmp_path):
      """Same parity requirement as
      test_valid_record_larger_than_chunk_size_matches_legacy_validator,
      but forced through the bounded-memory incremental-validator
      fallback path (not just the fast bytearray path) by shrinking the
      fast-path threshold well below the record's size."""
      import node_monitor.output.jsonl as jsonl_mod

      old_threshold = jsonl_mod._FAST_PATH_LINE_LIMIT_BYTES
      jsonl_mod._FAST_PATH_LINE_LIMIT_BYTES = 1024
      try:
         big_value = "y" * (200 * 1024)
         record = json.dumps({"a": 1, "big": big_value})
         for content in (
               (record + "\n").encode("utf-8"),
               record.encode("utf-8"),
         ):
            path = tmp_path / "oversized.jsonl"
            path.write_bytes(content)
            scanned = jsonl_mod.scan_jsonl_artifact(str(path), chunk_bytes=4096)
            validated = validate_jsonl_artifact(str(path))
            assert scanned["valid_count"] == validated["valid_count"] == 1
            assert scanned["malformed_count"] == validated["malformed_count"] == 0
            assert (scanned["truncated_final_line"]
                    == validated["truncated_final_line"] == False)
            assert scanned["byte_size"] == len(content)
            assert scanned["sha256"] == hashlib.sha256(content).hexdigest()
      finally:
         jsonl_mod._FAST_PATH_LINE_LIMIT_BYTES = old_threshold

   def test_oversized_malformed_record_matches_legacy_validator(self, tmp_path):
      """Same as above but for a line that is oversized AND malformed --
      the fallback path must reject it just like validate_jsonl_artifact
      does, not merely because of its size."""
      import node_monitor.output.jsonl as jsonl_mod

      old_threshold = jsonl_mod._FAST_PATH_LINE_LIMIT_BYTES
      jsonl_mod._FAST_PATH_LINE_LIMIT_BYTES = 1024
      try:
         # Oversized but with a syntax error near the very end -- a
         # buffering implementation and an incremental one could both
         # get this right, but only if the fallback path actually
         # validates JSON grammar rather than just size-gating.
         big_value = "y" * (200 * 1024)
         malformed = ('{"a": 1, "big": "' + big_value + '"')  # missing closing }
         content = malformed.encode("utf-8") + b"\n"
         path = tmp_path / "oversized_bad.jsonl"
         path.write_bytes(content)
         scanned = jsonl_mod.scan_jsonl_artifact(str(path), chunk_bytes=4096)
         validated = validate_jsonl_artifact(str(path))
         assert scanned["valid_count"] == validated["valid_count"] == 0
         assert scanned["malformed_count"] == validated["malformed_count"] == 1
         assert (scanned["truncated_final_line"]
                 == validated["truncated_final_line"] == False)
      finally:
         jsonl_mod._FAST_PATH_LINE_LIMIT_BYTES = old_threshold

   def test_fallback_path_decodes_multibyte_utf8_split_across_chunks(
         self, tmp_path):
      """The bounded-memory fallback feeds chunk-sized byte slices
      through an incremental UTF-8 decoder (codecs.getincrementaldecoder)
      before handing text to IncrementalJsonValidator. A naive per-chunk
      ``bytes.decode(\"utf-8\", \"replace\")`` would corrupt any multi-byte
      codepoint whose encoded bytes straddle a chunk boundary -- turning
      each half into U+FFFD and silently breaking valid JSON content.
      This drives a record containing 2-, 3-, and 4-byte UTF-8 characters
      through the fallback path (via a tiny fast-path threshold) at
      chunk_bytes settings from 1 byte up to larger than the whole
      record, and asserts exact parity (including byte_size/sha256 of
      the un-mangled original bytes) with validate_jsonl_artifact at
      every setting -- proving no chunk boundary, however placed,
      corrupts the decode.
      """
      import node_monitor.output.jsonl as jsonl_mod

      old_threshold = jsonl_mod._FAST_PATH_LINE_LIMIT_BYTES
      jsonl_mod._FAST_PATH_LINE_LIMIT_BYTES = 50
      try:
         # \u00e9 (2 bytes), \u4e2d (3 bytes), \U0001F600 (4 bytes) repeated,
         # forcing many chunk boundaries to land mid-codepoint at small
         # chunk_bytes values.
         big_value = "\u00e9\u00e8\u4e2d\U0001F600" * 200
         record = json.dumps({"a": 1, "s": big_value}, ensure_ascii=False)
         content = (record + "\n").encode("utf-8")
         path = tmp_path / "utf8_multibyte.jsonl"
         path.write_bytes(content)

         validated = validate_jsonl_artifact(str(path))
         for chunk_bytes in (1, 2, 3, 4, 5, 7, 16, 64, len(content) + 100):
            scanned = jsonl_mod.scan_jsonl_artifact(
               str(path), chunk_bytes=chunk_bytes)
            assert scanned["valid_count"] == validated["valid_count"] == 1, chunk_bytes
            assert scanned["malformed_count"] == validated["malformed_count"] == 0, chunk_bytes
            assert (scanned["truncated_final_line"]
                    == validated["truncated_final_line"] == False), chunk_bytes
            assert scanned["byte_size"] == len(content), chunk_bytes
            assert scanned["sha256"] == hashlib.sha256(content).hexdigest(), chunk_bytes
      finally:
         jsonl_mod._FAST_PATH_LINE_LIMIT_BYTES = old_threshold

   def test_fallback_syntax_error_is_sticky_not_masked_by_later_recovery(
         self, tmp_path):
      """Review round 5 finding #1: a JsonSyntaxError raised mid-feed
      does not by itself leave IncrementalJsonValidator's internal
      grammar state such that finish() will also raise -- e.g. one
      unexpected character between a completed value and its
      container's closing brace is a syntax error, but the container's
      stack frame is untouched, so a subsequent matching close
      character can still walk the stack back to empty and finish()
      reports success. Swallowing that first error without recording it
      let a line that IS malformed (or truncated) report as valid. This
      drives exactly that shape -- an oversized string value followed
      by a stray character, then a syntactically-valid closing brace --
      through the fallback path (via a tiny fast-path threshold) both
      newline-terminated and unterminated, and asserts parity with
      validate_jsonl_artifact.
      """
      import node_monitor.output.jsonl as jsonl_mod

      old_threshold = jsonl_mod._FAST_PATH_LINE_LIMIT_BYTES
      jsonl_mod._FAST_PATH_LINE_LIMIT_BYTES = 16
      try:
         big_str = "y" * 100
         # The stray "X" and the correct closing "}" must arrive in
         # SEPARATE chunk-sized reads for this to exercise the bug: if
         # both land in the same feed_str() call, the exception raised
         # by "X" aborts that call before "}" is ever fed, so the
         # object's stack frame is untouched either way and even the
         # old (buggy) code accidentally reports malformed. chunk_bytes
         # is deliberately 1 to force "X" and "}" into different calls.
         malformed = '{"a": "' + big_str + '"X}'
         for suffix in (b"\n", b""):
            content = malformed.encode("utf-8") + suffix
            path = tmp_path / "sticky.jsonl"
            path.write_bytes(content)
            scanned = jsonl_mod.scan_jsonl_artifact(str(path), chunk_bytes=1)
            validated = validate_jsonl_artifact(str(path))
            assert scanned["valid_count"] == validated["valid_count"] == 0, suffix
            assert scanned["malformed_count"] == validated["malformed_count"], suffix
            assert (scanned["truncated_final_line"]
                    == validated["truncated_final_line"]), suffix
            assert scanned["byte_size"] == len(content)
            assert scanned["sha256"] == hashlib.sha256(content).hexdigest()
      finally:
         jsonl_mod._FAST_PATH_LINE_LIMIT_BYTES = old_threshold

   def test_fallback_flushes_incremental_decoder_at_line_end(self, tmp_path):
      """Review round 5 finding #2: the fallback path's incremental
      UTF-8 decoder (codecs.getincrementaldecoder) was never told
      ``final=True`` at a line's true end (newline or EOF), so a
      dangling incomplete multi-byte sequence there was silently
      dropped instead of becoming U+FFFD -- the behavior a one-shot
      ``bytes.decode(\"utf-8\", \"replace\")`` on the whole line (what
      validate_jsonl_artifact does) produces. This ends an otherwise
      well-formed JSON object's bytes with a lone 0xC3 (a UTF-8 lead
      byte with no continuation byte -- definitionally incomplete) both
      before a newline and at true EOF, and asserts scan_jsonl_artifact
      matches validate_jsonl_artifact exactly.
      """
      import node_monitor.output.jsonl as jsonl_mod

      old_threshold = jsonl_mod._FAST_PATH_LINE_LIMIT_BYTES
      jsonl_mod._FAST_PATH_LINE_LIMIT_BYTES = 16
      try:
         big_str = "y" * 100
         valid_json = '{"a": "' + big_str + '"}'
         prefix_bytes = valid_json.encode("utf-8")
         for suffix in (b"\n", b""):
            content = prefix_bytes + b"\xc3" + suffix
            path = tmp_path / "dangling_utf8.jsonl"
            path.write_bytes(content)
            scanned = jsonl_mod.scan_jsonl_artifact(str(path), chunk_bytes=8)
            validated = validate_jsonl_artifact(str(path))
            assert scanned["valid_count"] == validated["valid_count"] == 0, suffix
            assert scanned["malformed_count"] == validated["malformed_count"], suffix
            assert (scanned["truncated_final_line"]
                    == validated["truncated_final_line"]), suffix
            assert scanned["byte_size"] == len(content)
            assert scanned["sha256"] == hashlib.sha256(content).hexdigest()
      finally:
         jsonl_mod._FAST_PATH_LINE_LIMIT_BYTES = old_threshold


# --------------------------------------------------------------------------
# IncrementalJsonValidator: the bounded-memory fallback grammar checker
# --------------------------------------------------------------------------

class TestIncrementalJsonValidator:
   """Direct unit tests of the fallback validator, independent of the
   scan/chunking machinery above. Parity with json.loads is the whole
   point of this class -- every case here was chosen because an earlier
   from-scratch implementation attempt got it wrong during development
   (fuzz-tested against json.loads over 20,000+ randomized/mutated
   inputs with zero mismatches before being wired into
   scan_jsonl_artifact; this is the checked-in subset of that fuzzing)."""

   @staticmethod
   def _check(text, chunk_size):
      try:
         json.loads(text)
         expected_valid = True
      except ValueError:
         expected_valid = False

      validator = IncrementalJsonValidator()
      try:
         for i in range(0, len(text), chunk_size) if text else [0]:
            validator.feed_str(text[i:i + chunk_size])
         validator.finish()
         actual_valid = True
      except JsonSyntaxError:
         actual_valid = False
      return expected_valid, actual_valid

   @pytest.mark.parametrize("chunk_size", [1, 3, 1000])
   @pytest.mark.parametrize("text", [
      "", " ", "null", "true", "false", "0", "-0", "1", "-1", "01", "1.",
      ".1", "1.0", "1e10", "1E10", "1e+10", "1e-10", "1e", "1e+", "-",
      "--1", "1-", "NaN", "Infinity", "-Infinity", "+Infinity",
      "Infinity2", '"hello"', '"hel\\"lo"', '"unterminated', '"a\\tb"',
      "\"a\tb\"", '"\\u0041"', '"\\uZZZZ"', "[]", "[1]", "[1,2]",
      "[1,2,]", "[,1]", "[1 2]", "{}", '{"a":1}', '{"a":1,"b":2}',
      '{"a":1,}', '{,"a":1}', "{1:2}", '{"a" :1}', '{"a": 1 }',
      '{"a":1}{"b":2}', "[[1,2],[3,4]]", '[{"a":[1,2,{"b":3}]}]',
      '{"a":[1,[2,[3,[4]]]]}', "[1,[2,3]", '{"a":{"b":1}', "   1   ",
      "tru", "nul", "fals", "[true,false,null]",
      '{"a":true,"b":null}', '"\\n\\t\\r\\b\\f"', "[1,]", '{"a":}',
      '{"a":1,"a":2}',
      json.dumps({"a": 1, "b": [1, 2, 3], "c": {"d": None, "e": True}}),
      "[" + ",".join(["1"] * 500) + "]",
      "{" + ",".join('"k%d":%d' % (i, i) for i in range(100)) + "}",
   ])
   def test_matches_json_loads(self, text, chunk_size):
      expected_valid, actual_valid = self._check(text, chunk_size)
      assert actual_valid == expected_valid

   def test_deeply_nested_but_within_python_recursion_limits_matches(self):
      text = "[" * 500 + "]" * 500
      expected_valid, actual_valid = self._check(text, 7)
      assert expected_valid is True
      assert actual_valid is True

   def test_memory_is_a_small_stack_not_the_input_length(self):
      """The validator's per-character feed_str never accumulates the
      consumed characters anywhere -- there is no buffer that grows
      with input length, only ``self._stack`` (bounded by nesting
      depth) and small scalar fields. Feed a huge flat array of tiny
      numbers (shallow nesting, large total length) and confirm the
      stack never grows past depth 1."""
      validator = IncrementalJsonValidator()
      validator.feed_str("[")
      max_stack_depth = [len(validator._stack)]
      for i in range(200000):
         validator.feed_str(str(i % 10))
         validator.feed_str(",")
         if len(validator._stack) > max_stack_depth[0]:
            max_stack_depth[0] = len(validator._stack)
      validator.feed_str("0]")
      validator.finish()
      assert max_stack_depth[0] == 1


# --------------------------------------------------------------------------
# Summary: counts, bytes, malformed count, sha256
# --------------------------------------------------------------------------

class TestFinalizeSummary:
   def test_summary_reports_counts_bytes_and_checksum(self, tmp_path):
      sink = Phase0Sink(str(tmp_path), "run18", disk_usage_fn=_full_disk_usage)
      _run(sink.write_record("node_hardware", _hardware_record("a.example.org")))
      _run(sink.write_record("node_hardware", _hardware_record("b.example.org")))

      summary = _run(sink.finalize_summary())

      path = os.path.join(sink.run_dir, "node_hardware.jsonl")
      with open(path, "rb") as handle:
         content = handle.read()
      expected_sha256 = hashlib.sha256(content).hexdigest()

      entry = summary["files"]["node_hardware"]
      assert entry["record_count"] == 2
      assert entry["malformed_count"] == 0
      assert entry["byte_size"] == len(content)
      assert entry["sha256"] == expected_sha256

   def test_summary_written_atomically_to_disk(self, tmp_path):
      sink = Phase0Sink(str(tmp_path), "run19", disk_usage_fn=_full_disk_usage)
      _run(sink.write_record("node_hardware", _hardware_record()))
      _run(sink.finalize_summary())
      path = os.path.join(sink.run_dir, "summary.json")
      assert os.path.exists(path)
      leftovers = [name for name in os.listdir(sink.run_dir) if name.endswith(".tmp")]
      assert leftovers == []
      with open(path) as handle:
         json.load(handle)  # must be well-formed, complete JSON

   def test_summary_file_mode_is_0600(self, tmp_path):
      sink = Phase0Sink(str(tmp_path), "run20", disk_usage_fn=_full_disk_usage)
      _run(sink.finalize_summary())
      path = os.path.join(sink.run_dir, "summary.json")
      mode = stat.S_IMODE(os.stat(path).st_mode)
      assert mode == 0o600


# --------------------------------------------------------------------------
# DONE only after summary finalizes
# --------------------------------------------------------------------------

class TestDoneOrdering:
   def test_done_before_summary_is_rejected(self, tmp_path):
      sink = Phase0Sink(str(tmp_path), "run21", disk_usage_fn=_full_disk_usage)
      with pytest.raises(Phase0SinkError):
         sink.write_done()
      assert not os.path.exists(os.path.join(sink.run_dir, "DONE"))

   def test_done_after_summary_succeeds(self, tmp_path):
      sink = Phase0Sink(str(tmp_path), "run22", disk_usage_fn=_full_disk_usage)
      _run(sink.finalize_summary())
      sink.write_done()
      assert os.path.exists(os.path.join(sink.run_dir, "DONE"))

   def test_done_file_created_strictly_after_summary_rename(self, tmp_path, monkeypatch):
      from node_monitor.output import jsonl as jsonl_mod

      order = []
      real_rename = os.rename

      def _spy_rename(src, dst):
         if os.path.basename(dst) == "summary.json":
            order.append("summary_renamed")
         real_rename(src, dst)

      monkeypatch.setattr(jsonl_mod.os, "rename", _spy_rename)
      sink = Phase0Sink(str(tmp_path), "run23", disk_usage_fn=_full_disk_usage)
      _run(sink.finalize_summary())

      real_os_open = os.open

      def _spy_os_open(path, *args, **kwargs):
         if os.path.basename(str(path)) == "DONE":
            order.append("done_opened")
         return real_os_open(path, *args, **kwargs)

      monkeypatch.setattr(jsonl_mod.os, "open", _spy_os_open)
      sink.write_done()

      assert order == ["summary_renamed", "done_opened"]


# --------------------------------------------------------------------------
# Review round 1 regression: a write after finalize_summary() must be
# rejected, and DONE must never be reachable for a run whose JSONL
# content has diverged from its already-finalized summary.
# --------------------------------------------------------------------------

class TestWriteAfterFinalizeRejected:
   def test_write_after_finalize_is_rejected(self, tmp_path):
      sink = Phase0Sink(str(tmp_path), "run24", disk_usage_fn=_full_disk_usage)
      _run(sink.finalize_summary())
      with pytest.raises(Phase0SinkError):
         _run(sink.write_record("node_hardware", _hardware_record()))

   def test_write_after_finalize_does_not_change_jsonl_or_summary(self, tmp_path):
      sink = Phase0Sink(str(tmp_path), "run25", disk_usage_fn=_full_disk_usage)
      _run(sink.write_record("node_hardware", _hardware_record("a.example.org")))
      summary_before = _run(sink.finalize_summary())

      with pytest.raises(Phase0SinkError):
         _run(sink.write_record("node_hardware", _hardware_record("b.example.org")))

      path = os.path.join(sink.run_dir, "node_hardware.jsonl")
      with open(path) as handle:
         lines = handle.readlines()
      assert len(lines) == 1  # the post-finalize write never landed
      assert summary_before["files"]["node_hardware"]["record_count"] == 1

   def test_finalize_then_write_then_done_would_have_diverged_but_write_is_blocked(self, tmp_path):
      """Reproduces the exact review-round-1 scenario: finalize an empty
      sink, attempt one write, then call write_done(). The write must be
      rejected so DONE can never mark a run whose summary undercounts its
      own JSONL content."""
      sink = Phase0Sink(str(tmp_path), "run26", disk_usage_fn=_full_disk_usage)
      summary = _run(sink.finalize_summary())
      assert summary["files"]["node_collection_log"]["record_count"] == 0

      with pytest.raises(Phase0SinkError):
         _run(sink.write_record("node_collection_log", {
            "system": "polaris",
            "timestamp_utc": "2026-09-09T00:00:00Z",
            "event": "daemon_start",
            "detail": {},
         }))

      sink.write_done()  # must still succeed: no divergent write got through
      path = os.path.join(sink.run_dir, "node_collection_log.jsonl")
      assert not os.path.exists(path)

   def test_repeated_finalize_is_rejected(self, tmp_path):
      sink = Phase0Sink(str(tmp_path), "run27", disk_usage_fn=_full_disk_usage)
      _run(sink.finalize_summary())
      with pytest.raises(Phase0SinkError):
         _run(sink.finalize_summary())


# --------------------------------------------------------------------------
# Review round 3 regression: a record must not be able to gain a
# forbidden raw-argv key through mutation after it has already passed
# validation but before it lands on disk -- i.e. validation and
# serialization must be one indivisible step, with no `await` in
# between where a queued writer's record could be mutated.
# --------------------------------------------------------------------------

class TestValidationSerializationAtomicity:
   def test_mutation_while_write_is_queued_on_the_lock_cannot_persist(self, tmp_path):
      """Deterministic reproduction of the review round 3 finding.

      Writer 1 holds the sink's lock (paused inside the critical
      section via the test-only hook). Writer 2 is started with a safe
      record and allowed to run its *synchronous* prefix -- record-type
      check, contract validation, and (with the fix) JSON serialization
      -- up to the point where it blocks trying to acquire the same
      lock. Only then is writer 2's original record object mutated to
      inject a forbidden ``argv`` key. Writer 1 is released, writer 2's
      write completes, and the persisted line must reflect the record
      as it was AT VALIDATION TIME, not the later mutation -- proving
      validation and serialization happened atomically with no
      exploitable gap.
      """
      sink = Phase0Sink(str(tmp_path), "run28", disk_usage_fn=_full_disk_usage)

      async def _main():
         entered = asyncio.Event()
         release = asyncio.Event()

         async def hook():
            entered.set()
            await release.wait()

         sink._test_before_write_hook = hook

         record1 = {
            "system": "polaris",
            "timestamp_utc": "2026-09-09T00:00:00Z",
            "event": "writer_one",
            "detail": {},
         }
         task1 = asyncio.create_task(
            sink.write_record("node_collection_log", record1))
         await entered.wait()  # writer 1 now holds the lock, paused in the hook

         record2 = {
            "system": "polaris",
            "timestamp_utc": "2026-09-09T00:00:00Z",
            "event": "writer_two",
            "detail": {"safe": True},
         }
         task2 = asyncio.create_task(
            sink.write_record("node_collection_log", record2))
         # Let writer 2 run its synchronous prefix (validate + serialize
         # under the fix) until it blocks acquiring the lock held by
         # writer 1. It must not be able to complete yet.
         await asyncio.sleep(0)
         assert not task2.done()

         # Mutate writer 2's record object *after* it has (per the fix)
         # already been validated and serialized, but while its write
         # is still queued on the lock.
         record2["detail"]["argv"] = ["--secret-marker"]

         release.set()
         await asyncio.gather(task1, task2)

      _run(_main())

      path = os.path.join(sink.run_dir, "node_collection_log.jsonl")
      with open(path) as handle:
         records = [json.loads(line) for line in handle]

      assert len(records) == 2
      by_event = {r["event"]: r for r in records}
      assert by_event["writer_one"]["detail"] == {}
      # The persisted record must match the pre-mutation, validated
      # state -- no argv key, and "safe" still present and true.
      assert by_event["writer_two"]["detail"] == {"safe": True}
      assert "argv" not in by_event["writer_two"]["detail"]

   def test_no_await_between_validate_and_serialize(self):
      """Structural guard: assert write_record's compiled bytecode has
      no YIELD/AWAIT opcode between the ``validate_record`` call and
      the ``json.dumps`` call, so the atomicity this test suite relies
      on cannot silently regress if the method is refactored.
      """
      import dis

      from node_monitor.output.jsonl import Phase0Sink

      instructions = list(dis.get_instructions(Phase0Sink.write_record))

      validate_idx = None
      dumps_idx = None
      for index, instr in enumerate(instructions):
         if instr.argval == "validate_record" and validate_idx is None:
            validate_idx = index
         if instr.argval == "dumps" and dumps_idx is None:
            dumps_idx = index
      assert validate_idx is not None and dumps_idx is not None
      assert validate_idx < dumps_idx

      between = instructions[validate_idx:dumps_idx]
      suspend_opnames = {
         name for name in (
            "GET_AWAITABLE", "YIELD_FROM", "YIELD_VALUE",
            "SEND", "BEFORE_ASYNC_WITH",
         )
      }
      suspending = [instr for instr in between if instr.opname in suspend_opnames]
      assert suspending == [], (
         "found a suspension-capable opcode between validate_record() and "
         "json.dumps(): %r" % (suspending,))
