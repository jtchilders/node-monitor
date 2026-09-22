"""Tests for node_monitor.output.jsonl.finalize_orphaned_run -- the
recovery finalizer for an orphaned Phase 0 run directory whose daemon
process died before writing summary.json/DONE.

Incident this exists for: phase0-canary-20260916T172446Z collected a
complete ~24h dataset but the daemon died inside the OLD whole-file
finalization path (see jsonl.scan_jsonl_artifact's own docstring) before
ever writing summary.json/DONE. This module's tests prove the recovery
finalizer:

* Produces a summary using the exact same streaming scan
  (``scan_jsonl_artifact``/its internal ``_scan_jsonl_fileobj``) the
  production ``Phase0Sink.finalize_summary`` uses -- never a second,
  independently-implemented checksum/validation pass.
* Never overwrites an existing summary.json/DONE.
* Strictly fails closed BEFORE publishing summary.json/DONE when there
  are no .jsonl artifacts at all, or any artifact has a malformed line
  or a truncated final line -- recovery never blesses corrupt or
  non-dataset input as finalized.
* Publishes DONE with the same write-temp/fsync/no-clobber-link
  discipline as summary.json, so a crash or write/fsync failure can
  never leave a final-named DONE (empty or partial) behind.
* Never fabricates an "acceptance" verdict -- the crashed daemon's own
  in-memory telemetry cannot be reconstructed, and the summary must
  never claim it was.
* Marks its output with an explicit "recovery" section distinguishing
  it from an ordinary clean daemon completion.
* Refuses a symlinked/foreign-owned run directory or artifact rather
  than following it (TOCTOU/symlink-substitution hardening).
* Preserves 0700/0600 permissions and never touches PostgreSQL/remote
  systems (this module makes no network or subprocess calls at all).
"""

import hashlib
import json
import os
import stat

import pytest

from node_monitor.output.jsonl import (
   Phase0Sink,
   Phase0SinkError,
   RecoveryFinalizeError,
   finalize_orphaned_run,
)


def _full_disk_usage(_path):
   return _DiskUsage(total=1_000_000_000, used=1_000_000, free=999_000_000)


class _DiskUsage:
   def __init__(self, total, used, free):
      self.total = total
      self.used = used
      self.free = free


def _hardware_record(hostname="polaris-login-04.example.org"):
   return {
      "system": "polaris",
      "source_hostname": hostname,
      "first_seen_utc": "2026-09-16T17:24:46Z",
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


def _make_orphaned_run_dir(tmp_path, run_id="canary-20260916T172446Z"):
   """Build a run directory shaped exactly like the real incident: real
   JSONL artifacts written through the production sink internals (never
   hand-crafted file content that could drift from the real on-disk
   format), but WITHOUT ever calling finalize_summary()/write_done() --
   simulating the daemon dying before it got there.
   """
   import asyncio

   sink = Phase0Sink(
      str(tmp_path), run_id, disk_usage_fn=_full_disk_usage)
   asyncio.run(sink.write_record(
      "node_hardware", _hardware_record("a.example.org")))
   asyncio.run(sink.write_record(
      "node_hardware", _hardware_record("b.example.org")))
   # Close the sink's own open file handles directly (bypassing
   # finalize_summary/write_done entirely) -- mirrors a killed process
   # leaving its fds closed by the OS but never reaching finalization.
   for handle in sink._file_handles.values():
      handle.close()
   return sink.run_dir


# --------------------------------------------------------------------------
# Success path
# --------------------------------------------------------------------------

class TestFinalizeOrphanedRunSuccess:
   def test_produces_summary_and_done(self, tmp_path):
      run_dir = _make_orphaned_run_dir(tmp_path)

      summary = finalize_orphaned_run(run_dir)

      assert os.path.exists(os.path.join(run_dir, "summary.json"))
      assert os.path.exists(os.path.join(run_dir, "DONE"))
      assert summary["files"]["node_hardware"]["record_count"] == 2

   def test_summary_matches_production_streaming_scan(self, tmp_path):
      """The recovery path must compute byte_size/sha256/malformed_count
      identically to the production scan_jsonl_artifact -- proving it
      reuses that function rather than reimplementing checksums.
      """
      from node_monitor.output.jsonl import scan_jsonl_artifact

      run_dir = _make_orphaned_run_dir(tmp_path)
      artifact_path = os.path.join(run_dir, "node_hardware.jsonl")
      expected = scan_jsonl_artifact(artifact_path)

      summary = finalize_orphaned_run(run_dir)

      entry = summary["files"]["node_hardware"]
      assert entry["byte_size"] == expected["byte_size"]
      assert entry["sha256"] == expected["sha256"]
      assert entry["malformed_count"] == expected["malformed_count"]
      assert entry["truncated_final_line"] == expected["truncated_final_line"]

   def test_summary_has_no_acceptance_key(self, tmp_path):
      """The daemon's own in-memory acceptance telemetry is gone with
      the crashed process; recovery must never fabricate it.
      """
      run_dir = _make_orphaned_run_dir(tmp_path)
      summary = finalize_orphaned_run(run_dir)
      assert "acceptance" not in summary

   def test_summary_marks_recovery_distinctly(self, tmp_path):
      run_dir = _make_orphaned_run_dir(tmp_path)
      summary = finalize_orphaned_run(run_dir)
      assert summary["recovery"]["recovered"] is True
      assert "reason" in summary["recovery"]

   def test_run_id_inferred_from_directory_name(self, tmp_path):
      run_dir = _make_orphaned_run_dir(
         tmp_path, run_id="canary-20260916T172446Z")
      summary = finalize_orphaned_run(run_dir)
      assert summary["run_id"] == "canary-20260916T172446Z"

   def test_explicit_run_id_overrides_inference(self, tmp_path):
      run_dir = _make_orphaned_run_dir(tmp_path)
      summary = finalize_orphaned_run(run_dir, run_id="explicit-id")
      assert summary["run_id"] == "explicit-id"

   def test_summary_file_written_atomically_no_leftover_tmp(self, tmp_path):
      run_dir = _make_orphaned_run_dir(tmp_path)
      finalize_orphaned_run(run_dir)
      leftovers = [
         name for name in os.listdir(run_dir)
         if ".tmp" in name]
      assert leftovers == []

   def test_summary_and_done_file_modes_are_0600(self, tmp_path):
      run_dir = _make_orphaned_run_dir(tmp_path)
      finalize_orphaned_run(run_dir)
      for name in ("summary.json", "DONE"):
         mode = stat.S_IMODE(os.stat(os.path.join(run_dir, name)).st_mode)
         assert mode == 0o600, "%s has mode %o" % (name, mode)

   def test_run_dir_mode_unchanged_at_0700(self, tmp_path):
      run_dir = _make_orphaned_run_dir(tmp_path)
      before = stat.S_IMODE(os.stat(run_dir).st_mode)
      assert before == 0o700
      finalize_orphaned_run(run_dir)
      after = stat.S_IMODE(os.stat(run_dir).st_mode)
      assert after == 0o700

   def test_zero_record_types_report_zero_not_fabricated(self, tmp_path):
      """A record type the run never wrote a single record for (e.g.
      node_collection_log in this fixture) must report real zeros, not
      be silently omitted or given fabricated nonzero values.
      """
      run_dir = _make_orphaned_run_dir(tmp_path)
      summary = finalize_orphaned_run(run_dir)
      entry = summary["files"]["node_collection_log"]
      assert entry["record_count"] == 0
      assert entry["byte_size"] == 0
      assert entry["sha256"] is None

   def test_manifest_present_flag_reflects_real_manifest(self, tmp_path):
      run_dir = _make_orphaned_run_dir(tmp_path)
      assert os.path.exists(os.path.join(run_dir, "manifest.json"))
      summary = finalize_orphaned_run(run_dir)
      assert summary["recovery"]["manifest_present"] is True

   def test_jsonl_artifact_content_untouched(self, tmp_path):
      """Recovery only ever reads .jsonl artifacts, never writes/
      modifies/removes them."""
      run_dir = _make_orphaned_run_dir(tmp_path)
      path = os.path.join(run_dir, "node_hardware.jsonl")
      with open(path, "rb") as handle:
         before = handle.read()
      finalize_orphaned_run(run_dir)
      with open(path, "rb") as handle:
         after = handle.read()
      assert before == after


# --------------------------------------------------------------------------
# Refusal / error paths
# --------------------------------------------------------------------------

class TestFinalizeOrphanedRunRefusals:
   def test_missing_run_dir_raises(self, tmp_path):
      missing = str(tmp_path / "does-not-exist")
      with pytest.raises(RecoveryFinalizeError):
         finalize_orphaned_run(missing)

   def test_run_dir_that_is_a_file_raises(self, tmp_path):
      path = tmp_path / "not-a-dir"
      path.write_text("hello")
      with pytest.raises(RecoveryFinalizeError):
         finalize_orphaned_run(str(path))

   def test_symlinked_run_dir_refused(self, tmp_path):
      real_dir = tmp_path / "real-run"
      real_dir.mkdir(mode=0o700)
      link = tmp_path / "phase0-linked-run"
      os.symlink(str(real_dir), str(link))
      with pytest.raises(RecoveryFinalizeError):
         finalize_orphaned_run(str(link))

   def test_existing_summary_json_refused_not_overwritten(self, tmp_path):
      run_dir = _make_orphaned_run_dir(tmp_path)
      existing_summary_path = os.path.join(run_dir, "summary.json")
      with open(existing_summary_path, "w") as handle:
         json.dump({"already": "finalized"}, handle)
      os.chmod(existing_summary_path, 0o600)

      with pytest.raises(RecoveryFinalizeError):
         finalize_orphaned_run(run_dir)

      # The pre-existing content must survive untouched.
      with open(existing_summary_path) as handle:
         content = json.load(handle)
      assert content == {"already": "finalized"}

   def test_existing_done_refused_not_overwritten(self, tmp_path):
      run_dir = _make_orphaned_run_dir(tmp_path)
      done_path = os.path.join(run_dir, "DONE")
      with open(done_path, "w") as handle:
         handle.write("2020-01-01T00:00:00Z\n")
      os.chmod(done_path, 0o600)
      before_mtime = os.stat(done_path).st_mtime_ns

      with pytest.raises(RecoveryFinalizeError):
         finalize_orphaned_run(run_dir)

      after_mtime = os.stat(done_path).st_mtime_ns
      assert before_mtime == after_mtime
      # And a legitimately orphaned run (no DONE) must still be refused
      # from producing a fresh summary.json when DONE alone pre-exists,
      # not just when both exist -- prove no summary.json got written.
      assert not os.path.exists(os.path.join(run_dir, "summary.json"))

   def test_symlinked_jsonl_artifact_refused(self, tmp_path):
      run_dir = _make_orphaned_run_dir(tmp_path)
      foreign = tmp_path / "foreign.jsonl"
      foreign.write_text('{"evil": true}\n')
      artifact_path = os.path.join(run_dir, "node_hardware.jsonl")
      os.unlink(artifact_path)
      os.symlink(str(foreign), artifact_path)

      with pytest.raises(RecoveryFinalizeError):
         finalize_orphaned_run(run_dir)

   def test_symlinked_manifest_refused(self, tmp_path):
      run_dir = _make_orphaned_run_dir(tmp_path)
      foreign = tmp_path / "foreign-manifest.json"
      foreign.write_text('{"evil": true}\n')
      manifest_path = os.path.join(run_dir, "manifest.json")
      os.unlink(manifest_path)
      os.symlink(str(foreign), manifest_path)

      with pytest.raises(RecoveryFinalizeError):
         finalize_orphaned_run(run_dir)

   def test_unparseable_run_id_without_explicit_override_raises(self, tmp_path):
      run_dir = _make_orphaned_run_dir(tmp_path)
      renamed = str(tmp_path / "not-the-right-shape")
      os.rename(run_dir, renamed)
      with pytest.raises(RecoveryFinalizeError):
         finalize_orphaned_run(renamed)

   def test_unparseable_run_id_with_explicit_override_succeeds(self, tmp_path):
      run_dir = _make_orphaned_run_dir(tmp_path)
      renamed = str(tmp_path / "not-the-right-shape")
      os.rename(run_dir, renamed)
      summary = finalize_orphaned_run(renamed, run_id="override-id")
      assert summary["run_id"] == "override-id"

   def test_malformed_manifest_json_raises(self, tmp_path):
      run_dir = _make_orphaned_run_dir(tmp_path)
      manifest_path = os.path.join(run_dir, "manifest.json")
      with open(manifest_path, "w") as handle:
         handle.write("{not valid json")
      with pytest.raises(RecoveryFinalizeError):
         finalize_orphaned_run(run_dir)

   def test_recovery_finalize_error_is_a_phase0_sink_error(self, tmp_path):
      """Existing callers that already catch Phase0SinkError (e.g. any
      future daemon/CLI integration) must still catch this."""
      missing = str(tmp_path / "does-not-exist")
      with pytest.raises(Phase0SinkError):
         finalize_orphaned_run(missing)

   def test_foreign_owned_run_dir_refused(self, tmp_path, monkeypatch):
      """Simulates a run directory not owned by the invoking user by
      monkeypatching os.geteuid rather than requiring real root/second
      user access in this sandboxed test environment.
      """
      run_dir = _make_orphaned_run_dir(tmp_path)
      from node_monitor.output import jsonl as jsonl_mod
      monkeypatch.setattr(jsonl_mod.os, "geteuid", lambda: 999999)
      with pytest.raises(RecoveryFinalizeError):
         finalize_orphaned_run(run_dir)


# --------------------------------------------------------------------------
# Malformed / truncated / empty artifact content is strictly REFUSED,
# never silently finalized -- publishing summary.json/DONE over
# untrustworthy input would bless corrupt or non-dataset content as a
# completed recovery, contradicting this tool's fail-closed requirement.
# --------------------------------------------------------------------------

class TestFinalizeOrphanedRunArtifactAccounting:
   def test_malformed_line_is_refused_before_any_write(self, tmp_path):
      run_dir = _make_orphaned_run_dir(tmp_path)
      path = os.path.join(run_dir, "node_hardware.jsonl")
      with open(path, "a") as handle:
         handle.write("{not valid json\n")

      with pytest.raises(RecoveryFinalizeError):
         finalize_orphaned_run(run_dir)

      assert not os.path.exists(os.path.join(run_dir, "summary.json"))
      assert not os.path.exists(os.path.join(run_dir, "DONE"))

   def test_truncated_final_line_is_refused_before_any_write(self, tmp_path):
      run_dir = _make_orphaned_run_dir(tmp_path)
      path = os.path.join(run_dir, "node_hardware.jsonl")
      with open(path, "a") as handle:
         handle.write('{"system": "polaris", "trun')  # no trailing newline

      with pytest.raises(RecoveryFinalizeError):
         finalize_orphaned_run(run_dir)

      assert not os.path.exists(os.path.join(run_dir, "summary.json"))
      assert not os.path.exists(os.path.join(run_dir, "DONE"))

   def test_no_jsonl_artifacts_at_all_is_refused(self, tmp_path):
      """An empty/non-dataset directory (no .jsonl artifacts written at
      all) is not this incident's shape and must never be blessed as a
      completed recovery."""
      run_dir = os.path.join(str(tmp_path), "phase0-empty-run")
      os.mkdir(run_dir, mode=0o700)

      with pytest.raises(RecoveryFinalizeError):
         finalize_orphaned_run(run_dir)

      assert not os.path.exists(os.path.join(run_dir, "summary.json"))
      assert not os.path.exists(os.path.join(run_dir, "DONE"))


# --------------------------------------------------------------------------
# DONE is published atomically -- a write/fsync failure partway through
# writing DONE's content must never leave a final-named DONE (empty or
# partial) that would falsely signal a completed finalization.
# --------------------------------------------------------------------------

class TestDoneFilePublishedAtomically:
   def test_fsync_failure_leaves_no_final_named_done(self, tmp_path, monkeypatch):
      """Inject a failure into the fsync call that persists DONE's
      content BEFORE it is linked into its final name. If DONE were
      created directly under its final name (the pre-fix behavior),
      this would leave an empty 'DONE' on disk despite the raised
      exception -- a false completion signal. With write-temp +
      fsync + link discipline, the failure must happen while only the
      hidden temp name exists, so no final-named DONE is ever visible.
      """
      run_dir = _make_orphaned_run_dir(tmp_path)
      from node_monitor.output import jsonl as jsonl_mod

      real_fsync = os.fsync
      calls = {"n": 0}

      def _flaky_fsync(fd):
         calls["n"] += 1
         # Let the run-directory-open fsync-free path and summary.json's
         # own two fsyncs (file + directory) through untouched; fail
         # only on DONE's own content fsync (the first fsync call made
         # from inside _write_done_file_dir_fd).
         if calls["n"] > 2:
            raise OSError("injected fsync failure for DONE content")
         return real_fsync(fd)

      monkeypatch.setattr(jsonl_mod.os, "fsync", _flaky_fsync)

      with pytest.raises(OSError):
         finalize_orphaned_run(run_dir)

      # summary.json legitimately got written before the injected DONE
      # failure (finalize_orphaned_run does not roll it back on a DONE
      # write failure -- see _finalize_orphaned_run_locked's own
      # "inconsistent state" comment) -- what this test proves is that
      # DONE specifically never exists under its real name, empty or
      # otherwise, and that no stray temp file is left behind either.
      assert not os.path.exists(os.path.join(run_dir, "DONE"))
      leftovers = [
         name for name in os.listdir(run_dir)
         if name.startswith("DONE.tmp-")]
      assert leftovers == []
