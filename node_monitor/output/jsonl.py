"""node_monitor.output.jsonl -- atomic Phase 0 run directory and JSONL sink.

Design: PHASE0_DAEMON_DESIGN.md "Output contract". A run creates a mode
0700 directory, one 0600 JSONL file per record type, an atomically
written manifest/summary, and a DONE flag written only after the summary
finalizes. All writes funnel through one asyncio lock so record content
is never interleaved even when multiple daemon loops append at once.

Concurrency model: this module assumes a single asyncio event loop (the
daemon has exactly one). ``asyncio.Lock`` serializes ``write_record``
calls across coroutines in that loop; it is not safe across processes or
threads, which the design does not call for -- "one sink" per the file
map.
"""

import asyncio
import hashlib
import json
import os
import shutil
import time

from node_monitor.output.contracts import validate_record


class Phase0SinkError(Exception):
   """Raised for any sink-level invariant violation (bad ordering, a
   duplicate run id, etc.) that is not itself a disk-space problem."""


class Phase0SinkDiskFullError(Phase0SinkError):
   """Raised when free disk space drops below the configured floor.

   Design: "Output write/flush failure or low disk is fatal because
   JSONL is the only Phase 0 result." Deliberately a distinct exception
   type from the generic Phase0SinkError so a caller (the daemon's
   orchestration layer) can catch disk exhaustion specifically without
   string-matching a message.
   """


# Record type -> JSONL filename, exactly matching PHASE0_DAEMON_DESIGN.md's
# "Output contract" file list. diagnostic_census is plural on disk
# ("diagnostic_censuses.jsonl") even though its record_type/validator name
# is singular, matching the design doc's file tree verbatim.
_FILENAMES = {
   "node_hardware": "node_hardware.jsonl",
   "node_counter_samples": "node_counter_samples.jsonl",
   "node_usage_intervals": "node_usage_intervals.jsonl",
   "node_poll_failures": "node_poll_failures.jsonl",
   "node_collection_log": "node_collection_log.jsonl",
   "diagnostic_census": "diagnostic_censuses.jsonl",
}

_RUN_DIR_MODE = 0o700
_FILE_MODE = 0o600

_DEFAULT_FLUSH_INTERVAL_SEC = 5.0
_DEFAULT_MIN_FREE_DISK_PCT = 10


def _atomic_write_json(path, payload):
   """Write ``payload`` as JSON to ``path`` via write-temp/fsync/rename,
   then fsync the containing directory -- design: "Manifest and summary
   use write-temp, fsync, rename, and directory fsync."
   """
   directory = os.path.dirname(path)
   tmp_path = path + ".tmp"
   fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, _FILE_MODE)
   try:
      with os.fdopen(fd, "w") as handle:
         json.dump(payload, handle, indent=2, sort_keys=True)
         handle.write("\n")
         handle.flush()
         os.fsync(handle.fileno())
      os.chmod(tmp_path, _FILE_MODE)
      os.rename(tmp_path, path)
   except BaseException:
      if os.path.exists(tmp_path):
         os.unlink(tmp_path)
      raise

   dir_fd = os.open(directory, os.O_RDONLY)
   try:
      os.fsync(dir_fd)
   finally:
      os.close(dir_fd)


def validate_jsonl_artifact(path):
   """Validate one JSONL artifact file, tolerating a truncated final line.

   Design: "Artifact validator reports a truncated final line without
   making earlier records unreadable." A crash mid-write can only ever
   damage the LAST line (writes are appended whole, one at a time, under
   the sink's lock) so this walks every line, counts well-formed JSON
   objects, counts malformed lines, and -- specifically for the final
   line only, when the file does not end in a newline -- reports
   truncation separately from an ordinary malformed line elsewhere in
   the file.

   Returns {"valid_count", "malformed_count", "truncated_final_line"}.
   """
   if not os.path.exists(path):
      return {"valid_count": 0, "malformed_count": 0, "truncated_final_line": False}

   with open(path, "rb") as handle:
      content = handle.read()

   if not content:
      return {"valid_count": 0, "malformed_count": 0, "truncated_final_line": False}

   ends_with_newline = content.endswith(b"\n")
   text = content.decode("utf-8", "replace")
   lines = text.split("\n")
   if lines and lines[-1] == "":
      lines.pop()  # trailing newline produces one empty split segment

   valid_count = 0
   malformed_count = 0
   truncated_final_line = False

   last_index = len(lines) - 1
   for index, line in enumerate(lines):
      if not line:
         continue
      try:
         json.loads(line)
         valid_count += 1
      except ValueError:
         if index == last_index and not ends_with_newline:
            truncated_final_line = True
         else:
            malformed_count += 1

   return {
      "valid_count": valid_count,
      "malformed_count": malformed_count,
      "truncated_final_line": truncated_final_line,
   }


class Phase0Sink:
   """Owns one Phase 0 run directory and every JSONL/metadata file in it.

   All state mutation (opening files, writing records, tracking counts)
   happens behind a single ``asyncio.Lock`` acquired in ``write_record``,
   so callers never need their own locking regardless of how many
   concurrent daemon loops hold a reference to the same sink.
   """

   def __init__(self, output_root, run_id, metadata=None,
                flush_interval_sec=_DEFAULT_FLUSH_INTERVAL_SEC,
                min_free_disk_pct=_DEFAULT_MIN_FREE_DISK_PCT,
                clock=time.monotonic, disk_usage_fn=shutil.disk_usage):
      self.output_root = output_root
      self.run_id = run_id
      self.run_dir = os.path.join(output_root, "phase0-%s" % run_id)
      self._flush_interval_sec = flush_interval_sec
      self._min_free_disk_pct = min_free_disk_pct
      self._clock = clock
      self._disk_usage_fn = disk_usage_fn

      # Created lazily (see _get_lock) rather than here: asyncio.Lock()
      # binds to the running event loop at construction time on Python
      # 3.9, but Phase0Sink itself is constructed synchronously outside
      # any loop (the daemon builds the sink before starting its event
      # loop). Deferring construction to first async use avoids a
      # RuntimeError("no current event loop") for perfectly normal
      # synchronous construction.
      self._lock = None
      self._file_handles = {}
      self._record_counts = {name: 0 for name in _FILENAMES}
      self._last_fsync_at = {}
      self._summary_finalized = False
      # Test-only hook: an async callable invoked once inside the locked
      # critical section, after the disk guard and validation but before
      # the actual write, purely so a test can pause one writer mid-hold
      # and prove a second writer blocks on the same lock. Never set in
      # production; defaults to a no-op.
      self._test_before_write_hook = None

      self._create_run_dir()
      self._write_manifest(metadata or {})

   def _create_run_dir(self):
      if os.path.exists(self.run_dir):
         raise Phase0SinkError(
            "run directory already exists: %r (duplicate run id?)" % (self.run_dir,))
      os.makedirs(self.run_dir, mode=_RUN_DIR_MODE)
      # makedirs' mode is subject to umask; force the exact bits the
      # design requires (0700) regardless of the process umask.
      os.chmod(self.run_dir, _RUN_DIR_MODE)

   def _write_manifest(self, metadata):
      payload = dict(metadata)
      payload["run_id"] = self.run_id
      payload["created_utc"] = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + "Z"
      _atomic_write_json(os.path.join(self.run_dir, "manifest.json"), payload)

   def _check_disk_guard(self):
      usage = self._disk_usage_fn(self.output_root)
      free_pct = 100.0 * usage.free / usage.total if usage.total else 0.0
      if free_pct < self._min_free_disk_pct:
         raise Phase0SinkDiskFullError(
            "free disk %.2f%% below minimum %.2f%% for output_root %r"
            % (free_pct, self._min_free_disk_pct, self.output_root))

   def _handle_for(self, record_type):
      handle = self._file_handles.get(record_type)
      if handle is None:
         path = os.path.join(self.run_dir, _FILENAMES[record_type])
         fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, _FILE_MODE)
         handle = os.fdopen(fd, "a", encoding="utf-8")
         os.chmod(path, _FILE_MODE)
         self._file_handles[record_type] = handle
      return handle

   def _get_lock(self):
      if self._lock is None:
         self._lock = asyncio.Lock()
      return self._lock

   async def write_record(self, record_type, record):
      """Validate and append one record to its record type's JSONL file.

      Raises ``ContractError`` (from node_monitor.output.contracts) for
      an invalid record -- validation happens before any file is even
      opened, so a rejected record leaves no trace on disk.
      Raises ``Phase0SinkDiskFullError`` if free disk is below the
      configured floor; the write never proceeds in that case either.
      Raises ``Phase0SinkError`` if called after ``finalize_summary()``
      has already run -- a write must never land after the summary that
      is supposed to describe the run's final content.

      Review round 3 finding: ``record`` is a caller-owned mutable
      object. The old code validated it here but only serialized it
      later, after awaiting the sink's lock -- a genuine ``await``
      suspension point across which another coroutine could mutate the
      same object (e.g. inject a forbidden argv key into a nested dict)
      before the now-stale ``validated`` reference was finally
      ``json.dumps``-ed. To close that TOCTOU window, validation and
      serialization now happen back to back with no ``await`` between
      them -- nothing can run on this single-threaded event loop in
      that gap -- so ``line`` is fixed, immutable bytes computed from
      exactly the structure that passed the privacy validator, before
      this coroutine ever yields control by awaiting the lock.
      """
      if record_type not in _FILENAMES:
         raise Phase0SinkError("unknown record_type %r" % (record_type,))

      validated = validate_record(record_type, record)
      # No `await` between validation and serialization: this line and
      # the one above run atomically with respect to every other
      # coroutine on this event loop, so `record` cannot be mutated in
      # between. `line` is the immutable, already-serialized snapshot
      # that every later step (lock wait, disk guard, hook, actual
      # write) is scoped to.
      line = json.dumps(validated, separators=(",", ":"))

      async with self._get_lock():
         if self._summary_finalized:
            raise Phase0SinkError(
               "write_record() called after finalize_summary(); a run's "
               "JSONL content must never diverge from its finalized "
               "summary")
         self._check_disk_guard()

         if self._test_before_write_hook is not None:
            await self._test_before_write_hook()

         handle = self._handle_for(record_type)
         handle.write(line + "\n")
         handle.flush()

         now = self._clock()
         last = self._last_fsync_at.get(record_type)
         if last is None or (now - last) >= self._flush_interval_sec:
            os.fsync(handle.fileno())
            self._last_fsync_at[record_type] = now

         self._record_counts[record_type] += 1

   async def finalize_summary(self):
      """Close every open JSONL file, validate each artifact, and write
      an atomic summary.json with per-file record counts, byte sizes,
      malformed-line counts, and SHA-256 checksums.

      Safe to call with zero records written to any given file -- that
      file simply never got a handle and reports zero counts/size, so a
      partial (signal-driven) shutdown still produces a valid summary
      for whichever files did receive data.

      Must be called exactly once per sink: a second call raises
      ``Phase0SinkError`` rather than silently re-finalizing, and once
      finalized, ``write_record`` also refuses further writes -- so a
      finalized run's summary can never diverge from its JSONL content
      (review round 1 finding: a write slipping in after finalize_summary
      previously left DONE-marked artifacts whose summary undercounted
      their own files).
      """
      async with self._get_lock():
         if self._summary_finalized:
            raise Phase0SinkError(
               "finalize_summary() called twice; a run's summary must "
               "be produced exactly once")
         for handle in self._file_handles.values():
            handle.flush()
            os.fsync(handle.fileno())
            handle.close()
         self._file_handles = {}

         files_summary = {}
         for record_type, filename in _FILENAMES.items():
            path = os.path.join(self.run_dir, filename)
            if os.path.exists(path):
               with open(path, "rb") as handle:
                  content = handle.read()
               validation = validate_jsonl_artifact(path)
               files_summary[record_type] = {
                  "filename": filename,
                  "record_count": self._record_counts[record_type],
                  "byte_size": len(content),
                  "malformed_count": validation["malformed_count"],
                  "truncated_final_line": validation["truncated_final_line"],
                  "sha256": hashlib.sha256(content).hexdigest(),
               }
            else:
               files_summary[record_type] = {
                  "filename": filename,
                  "record_count": 0,
                  "byte_size": 0,
                  "malformed_count": 0,
                  "truncated_final_line": False,
                  "sha256": None,
               }

         summary = {
            "run_id": self.run_id,
            "finalized_utc": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + "Z",
            "files": files_summary,
         }
         _atomic_write_json(os.path.join(self.run_dir, "summary.json"), summary)
         self._summary_finalized = True
         return summary

   def write_done(self):
      """Write the DONE flag. Design: "DONE is written only after summary
      finalization." Calling this before ``finalize_summary`` is a sink
      misuse, not a runtime/environment condition, so it raises
      synchronously and eagerly rather than silently no-op'ing.
      """
      if not self._summary_finalized:
         raise Phase0SinkError(
            "write_done() called before finalize_summary(); "
            "DONE must never appear before the summary is complete")
      path = os.path.join(self.run_dir, "DONE")
      fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, _FILE_MODE)
      with os.fdopen(fd, "w") as handle:
         handle.write(
            time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + "Z\n")
         handle.flush()
         os.fsync(handle.fileno())
      os.chmod(path, _FILE_MODE)
