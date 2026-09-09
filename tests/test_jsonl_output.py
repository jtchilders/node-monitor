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
import hashlib
import json
import os
import stat

import pytest

from node_monitor.output.jsonl import (
   Phase0Sink,
   Phase0SinkDiskFullError,
   Phase0SinkError,
   validate_jsonl_artifact,
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
