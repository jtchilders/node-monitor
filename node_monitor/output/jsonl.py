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
import codecs
import hashlib
import json
import os
import re
import shutil
import stat
import time

from node_monitor.output._incremental_json import (
   IncrementalJsonValidator,
   JsonSyntaxError,
)
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


# Review round 2 finding on the first version of this fix: plain binary-mode
# line iteration (``for raw_line in handle``) asks the file object itself to
# find the next ``b"\n"``, and when a "line" has none -- e.g. a pathological
# or corrupted artifact with megabytes of unbroken content -- the file
# object has no choice but to grow its own return buffer to hold that whole
# span before it can even give it back to this function. A 128 MiB
# newline-free artifact measurably raised RSS by ~213 MB under that
# approach. Fixed-size ``handle.read(_SCAN_CHUNK_BYTES)`` calls are the only
# way to put a real ceiling on what any single I/O call can hand back,
# independent of where (or whether) a newline appears in the file.
_SCAN_CHUNK_BYTES = 1 << 20  # 1 MiB per read() call, never more.

# Review round 2 -> round 3 -> round 4 history on the in-progress-line
# buffer (see scan_jsonl_artifact's docstring for the full story): a
# per-line byte cap that rejects anything past it as malformed/truncated
# breaks legitimate large JSON records (round 3's finding); no cap at all
# reintroduces round 2's unbounded-memory defect for a pathological
# no-newline line. Both constraints are satisfiable together ONLY by
# switching, once a line's buffered bytes cross this threshold, from
# "accumulate bytes then call json.loads() once" to an incremental
# grammar validator that consumes bytes as they arrive and never
# retains them (node_monitor.output._incremental_json). Below the
# threshold the fast bytearray+json.loads path is used unchanged (it is
# far faster for realistic Phase 0 record sizes -- see the module
# docstring's benchmark note); at or above it, memory for that one line
# is bounded by roughly this threshold plus one chunk_bytes, regardless
# of how much larger the actual line turns out to be.
_FAST_PATH_LINE_LIMIT_BYTES = 1 << 20  # 1 MiB


def scan_jsonl_artifact(path, chunk_bytes=_SCAN_CHUNK_BYTES):
   """Stream one JSONL artifact in bounded I/O and bounded per-line
   memory, computing everything ``finalize_summary`` needs (byte size,
   SHA-256, valid/malformed line counts, truncated-final-line status)
   in a single pass.

   Design/incident context: the 24h canary run phase0-canary-
   20260916T172446Z died during finalization with no summary.json/DONE.
   Its diagnostic_censuses.jsonl artifact was 1,764,399,017 bytes --
   ``finalize_summary`` used to ``handle.read()`` that whole file once
   for byte_size/sha256, then ``validate_jsonl_artifact`` read it a
   SECOND time in full to split it into a Python list of lines, so
   finalization transiently needed multiple GiB of RAM at exactly the
   moment a run needed to finish cleanly. This function replaces both
   of those whole-file reads with one pass of fixed-size
   ``handle.read(chunk_bytes)`` calls (never a bare/whole-file ``.read()``,
   never ``.split()`` on the whole content, never plain line iteration --
   see ``_SCAN_CHUNK_BYTES``'s comment for why iteration alone is not
   bounded). A running ``hashlib.sha256`` digests each fixed-size chunk as
   it arrives; each chunk is searched for ``b"\\n"`` boundaries with
   ``bytes.find`` to split it (at most ``chunk_bytes`` of data, so this
   split is itself bounded) into completed lines plus at most one
   still-open line carried into the next chunk.

   Per-line validation is a two-tier strategy (see
   ``_FAST_PATH_LINE_LIMIT_BYTES``'s comment for the review history
   that led here): while a line's accumulated bytes stay under
   ``_FAST_PATH_LINE_LIMIT_BYTES``, they are held in a plain
   ``bytearray`` and validated with one ordinary ``json.loads`` call
   once the line completes -- fast, and by construction identical to
   ``validate_jsonl_artifact``'s per-line verdict, for every realistic
   Phase 0 record. If a line's accumulated bytes ever cross that
   threshold, the bytes already buffered plus everything still to come
   for that same line are instead fed incrementally into an
   ``IncrementalJsonValidator`` (node_monitor.output._incremental_json),
   which tracks only JSON grammar STATE -- a stack of tiny frames
   bounded by nesting depth -- and never retains the line's content.
   This keeps memory bounded (roughly ``_FAST_PATH_LINE_LIMIT_BYTES``
   plus one ``chunk_bytes``, regardless of how much larger the line
   actually is) while still agreeing exactly with
   ``validate_jsonl_artifact`` on whether any line -- of any length --
   is valid JSON, since ``IncrementalJsonValidator`` independently
   implements the identical grammar ``json.loads`` accepts (verified by
   randomized/mutation fuzz testing against ``json.loads`` during
   development; see ``tests.test_jsonl_output.TestIncrementalJsonValidator``).

   Truncation semantics are preserved exactly from
   ``validate_jsonl_artifact``: a crash mid-write can only ever damage
   the LAST line (writes are appended whole, one at a time, under the
   sink's lock), so only the final line, and only when the file does
   not end in a newline, is eligible to be reported as
   ``truncated_final_line`` instead of an ordinary malformed line.

   Returns ``{"byte_size", "sha256", "valid_count", "malformed_count",
   "truncated_final_line"}`` -- the same fields ``finalize_summary``
   already assembled from ``validate_jsonl_artifact`` plus the two
   separately-read ``byte_size``/``sha256`` fields, now computed
   together in the one pass.
   """
   if not os.path.exists(path):
      return {
         "byte_size": 0,
         "sha256": None,
         "valid_count": 0,
         "malformed_count": 0,
         "truncated_final_line": False,
      }

   with open(path, "rb") as handle:
      return _scan_jsonl_fileobj(handle, chunk_bytes=chunk_bytes)


def _scan_jsonl_fileobj(handle, chunk_bytes=_SCAN_CHUNK_BYTES):
   """The actual streaming scan behind ``scan_jsonl_artifact``, operating
   on an ALREADY-OPEN binary file object rather than a path string.

   Factored out so a caller holding its own already-verified, already-open
   file object (e.g. ``finalize_orphaned_run``, which opens each artifact
   through a verified directory file descriptor to close a symlink/TOCTOU
   window) can reuse the exact same scanning logic ``scan_jsonl_artifact``
   uses, without a second path-based ``open()`` that would throw away that
   verification and reintroduce the very race the caller opened the file
   to avoid. ``handle`` is consumed but never closed here -- the caller
   owns its lifecycle.
   """
   digest = hashlib.sha256()
   byte_size = 0
   valid_count = 0
   malformed_count = 0
   truncated_final_line = False

   # The only per-line state carried across chunk-read iterations while
   # under the fast-path threshold: the bytes of whichever line is
   # currently in progress. Freed (a fresh bytearray) the instant that
   # line is parsed/counted, so at any given moment this holds exactly
   # one line's worth of content -- up to the threshold -- never the
   # whole file, never a list of all lines.
   line_buf = bytearray()

   # Set instead of line_buf once a line crosses
   # _FAST_PATH_LINE_LIMIT_BYTES: an IncrementalJsonValidator consuming
   # that same line's remaining bytes without retaining them, plus the
   # incremental UTF-8 decoder feeding it (mirrors
   # ``content.decode("utf-8", "replace")`` exactly, just incrementally
   # -- see decode_incremental's own note).
   fallback_validator = None
   fallback_decoder = None
   # Review round 5 finding: JsonSyntaxError raised mid-feed does not by
   # itself leave the validator's grammar state such that finish() will
   # also raise -- e.g. an unexpected character between a completed
   # value and its container's closing brace/bracket is a syntax error,
   # but the container's stack frame is untouched, so a subsequent
   # matching close character can still walk the stack back to empty
   # and finish() reports success. Swallowing the error without
   # recording it therefore let a line that IS malformed report as
   # valid. This flag makes any syntax error sticky for the rest of the
   # current line, independent of what the validator's internal state
   # happens to look like afterward.
   fallback_failed = False

   def _decode_incremental(raw_bytes, final):
      # decode() with errors="replace" one chunk at a time is exactly
      # equivalent to decoding the whole line's bytes at once with
      # errors="replace", for any way the bytes are chopped up --
      # verified during development with randomized chunking including
      # splits that land inside multi-byte sequences. ``final=True``
      # must be passed at the true end of the line's bytes (a real
      # newline terminator, or EOF) -- see review round 5 finding #2:
      # omitting it silently drops a dangling incomplete multi-byte
      # sequence instead of turning it into U+FFFD the way a one-shot
      # ``bytes.decode("utf-8", "replace")`` on the whole line does.
      return fallback_decoder.decode(raw_bytes, final)

   def _start_fallback():
      nonlocal fallback_validator, fallback_decoder, fallback_failed, line_buf
      fallback_validator = IncrementalJsonValidator()
      fallback_decoder = codecs.getincrementaldecoder("utf-8")("replace")
      fallback_failed = False
      if line_buf:
         text = _decode_incremental(bytes(line_buf), False)
         try:
            fallback_validator.feed_str(text)
         except JsonSyntaxError:
            fallback_failed = True  # sticky -- see fallback_failed's comment
      line_buf = bytearray()

   def _feed_fallback(raw_bytes):
      nonlocal fallback_failed
      text = _decode_incremental(raw_bytes, False)
      if fallback_failed:
         return  # already known-invalid; keep consuming bytes, don't re-feed
      try:
         fallback_validator.feed_str(text)
      except JsonSyntaxError:
         fallback_failed = True  # sticky -- see fallback_failed's comment

   def _finalize_fallback_line():
      nonlocal fallback_validator, fallback_decoder, fallback_failed
      nonlocal valid_count, malformed_count
      # Flush the incremental UTF-8 decoder with final=True so a
      # dangling incomplete multi-byte sequence at the line's true end
      # becomes U+FFFD (matching validate_jsonl_artifact's one-shot
      # decode) instead of being silently dropped -- review round 5
      # finding #2.
      tail_text = _decode_incremental(b"", True)
      if tail_text and not fallback_failed:
         try:
            fallback_validator.feed_str(tail_text)
         except JsonSyntaxError:
            fallback_failed = True
      if fallback_failed:
         malformed_count += 1
      else:
         try:
            fallback_validator.finish()
            valid_count += 1
         except JsonSyntaxError:
            malformed_count += 1
      fallback_validator = None
      fallback_decoder = None

   def _append_to_current_line(segment):
      # Route segment either into the fast bytearray path or the
      # bounded-memory fallback, switching the moment the fast path
      # would exceed its threshold.
      nonlocal line_buf
      if fallback_validator is not None:
         if segment:
            _feed_fallback(segment)
         return
      if segment:
         line_buf.extend(segment)
      if len(line_buf) > _FAST_PATH_LINE_LIMIT_BYTES:
         _start_fallback()

   def _finalize_complete_line():
      # Called exactly when a real b"\n" terminator has been found for the
      # accumulated line -- i.e. this line can never be the truncated
      # final line, only valid or ordinarily malformed.
      nonlocal malformed_count, valid_count, line_buf
      if fallback_validator is not None:
         _finalize_fallback_line()
         return
      if line_buf:
         try:
            json.loads(bytes(line_buf).decode("utf-8", "replace"))
            valid_count += 1
         except ValueError:
            malformed_count += 1
      # else: an empty line (consecutive newlines) -- matches
      # validate_jsonl_artifact's `if not line: continue`, counted as
      # neither valid nor malformed.

   while True:
      chunk = handle.read(chunk_bytes)
      if not chunk:
         break
      digest.update(chunk)
      byte_size += len(chunk)

      start = 0
      chunk_len = len(chunk)
      while True:
         newline_index = chunk.find(b"\n", start)
         if newline_index == -1:
            remainder = chunk[start:chunk_len]
            _append_to_current_line(remainder)
            break

         segment = chunk[start:newline_index]
         _append_to_current_line(segment)

         _finalize_complete_line()
         line_buf = bytearray()
         start = newline_index + 1

   # A non-empty (or in-fallback) line buffer at EOF means the file's
   # last bytes were never terminated by b"\n" -- exactly the
   # truncated-final-line case (or a well-formed final line missing
   # only its trailing newline, which is valid, not truncated).
   if fallback_validator is not None:
      # Same final-flush-then-sticky-failure logic as
      # _finalize_fallback_line, but a failure at true EOF (no
      # newline) is "truncated", not "malformed" -- matching every
      # other truncated-final-line branch in this function.
      tail_text = _decode_incremental(b"", True)
      if tail_text and not fallback_failed:
         try:
            fallback_validator.feed_str(tail_text)
         except JsonSyntaxError:
            fallback_failed = True
      if fallback_failed:
         truncated_final_line = True
      else:
         try:
            fallback_validator.finish()
            valid_count += 1
         except JsonSyntaxError:
            truncated_final_line = True
   elif line_buf:
      try:
         json.loads(bytes(line_buf).decode("utf-8", "replace"))
         valid_count += 1
      except ValueError:
         truncated_final_line = True

   return {
      "byte_size": byte_size,
      "sha256": digest.hexdigest(),
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

   async def finalize_summary(self, acceptance_fn=None):
      """Close every open JSONL file, validate each artifact, and write
      an atomic summary.json with per-file record counts, byte sizes,
      malformed-line counts, and SHA-256 checksums.

      ``acceptance_fn``: optional callable ``acceptance_fn(files_summary)
      -> dict`` invoked AFTER every file has been validated/summarized
      but BEFORE the single atomic ``summary.json`` write -- its return
      value is included verbatim under the summary's own ``acceptance``
      key. This is the one hook a caller (``node_monitor.daemon``) uses
      to compute the Phase 0 canary acceptance verdict from exactly the
      same finalized per-file metadata this method already produces,
      without this sink ever needing to know what "acceptance" means or
      importing ``node_monitor.output.acceptance`` itself. Backward
      compatible: omitted (or ``None``), the summary is written with no
      ``acceptance`` key at all, exactly as before this hook existed --
      no caller of the pre-existing zero-argument signature is broken.
      There is still exactly one ``summary.json`` write either way: the
      hook's result is folded into the same ``summary`` dict this
      method was already about to write, never a second write/patch
      (design: \"Do not write/patch summary twice\").

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

         # One streaming pass per artifact (scan_jsonl_artifact), not the
         # old two whole-file reads (a raw read() here for byte_size/
         # sha256, then a second full read() inside
         # validate_jsonl_artifact for line validation). See
         # scan_jsonl_artifact's docstring for the canary incident this
         # fixes: a multi-GiB artifact made that double whole-file-read
         # pattern transiently need multiple GiB of RAM right at
         # finalization.
         files_summary = {}
         for record_type, filename in _FILENAMES.items():
            path = os.path.join(self.run_dir, filename)
            scan = scan_jsonl_artifact(path)
            files_summary[record_type] = {
               "filename": filename,
               "record_count": self._record_counts[record_type],
               "byte_size": scan["byte_size"],
               "malformed_count": scan["malformed_count"],
               "truncated_final_line": scan["truncated_final_line"],
               "sha256": scan["sha256"],
            }

         summary = {
            "run_id": self.run_id,
            "finalized_utc": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + "Z",
            "files": files_summary,
         }
         if acceptance_fn is not None:
            summary["acceptance"] = acceptance_fn(files_summary)
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
      _write_done_file(self.run_dir)


def _write_done_file(run_dir):
   """Write the DONE flag file into ``run_dir``.

   Shared by ``Phase0Sink.write_done`` (the clean daemon-completion path)
   and ``finalize_orphaned_run`` (the recovery path) so there is exactly
   one place that decides DONE's on-disk format/mode -- never two
   independently maintained writers that could drift.
   """
   path = os.path.join(run_dir, "DONE")
   fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, _FILE_MODE)
   with os.fdopen(fd, "w") as handle:
      handle.write(
         time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + "Z\n")
      handle.flush()
      os.fsync(handle.fileno())
   os.chmod(path, _FILE_MODE)


# --------------------------------------------------------------------------
# Recovery finalization for an orphaned run directory.
#
# Incident: phase0-canary-20260916T172446Z collected a complete ~24h
# dataset but the daemon process died inside the OLD whole-file
# finalization path (see scan_jsonl_artifact's own docstring) before it
# ever wrote summary.json/DONE. The daemon's in-process acceptance
# telemetry (bounded scheduling-delay/probe-wall-time samples, per-node
# success totals, complete-window counts -- see daemon.Daemon.
# _build_acceptance) lived only in that dead process's memory and is
# gone; it cannot be reconstructed from the JSONL artifacts alone
# without silently fabricating numbers the daemon never actually
# measured. finalize_orphaned_run therefore produces a DIFFERENT kind
# of summary from a normal completion: it runs the exact same
# production streaming scanner (scan_jsonl_artifact) this module's own
# finalize_summary() uses, so byte sizes/checksums/malformed-line
# counts/record counts are computed by the identical, already-reviewed
# code path -- never a second, hand-rolled checksum/scan implementation
# -- but it never invents an "acceptance" verdict, and it marks the
# summary with an explicit "recovery" section so nothing downstream can
# mistake this for an ordinary clean daemon completion.
# --------------------------------------------------------------------------

class RecoveryFinalizeError(Phase0SinkError):
   """Raised when an orphaned run directory cannot be safely finalized.

   Distinct from ``Phase0SinkError`` only in name (it IS one, so
   existing callers that catch ``Phase0SinkError`` still catch this) --
   kept as its own class purely so a recovery CLI/tool can report a
   recognizable, recovery-specific error type without string-matching
   a message.
   """


def _open_verified_run_dir(run_dir):
   """Open ``run_dir`` by path, then verify -- via ``os.fstat`` on the
   ALREADY-OPEN file descriptor, never a second, separate ``os.stat``
   or ``os.path`` call on the path string -- that what got opened is a
   real directory, not a symlink (or a symlink swapped in between any
   check and this open).

   ``O_NOFOLLOW`` refuses to open the path at all if its last component
   is itself a symlink (closing the classic "attacker replaces the
   run directory with a symlink to something the caller does not own,
   racing between a stat() and a later open()" TOCTOU window). Rejecting
   any run directory whose OWNER is not the current effective user
   closes an adjacent hazard: a world-writable/attacker-owned directory
   that merely happens to be named like a run dir, sitting somewhere an
   operator might be fooled into pointing this tool at.

   Returns the open directory file descriptor. Every subsequent
   operation on this run directory's contents in this module uses this
   SAME fd via ``dir_fd=``/``*at()`` semantics, never a fresh path
   re-open -- so a symlink swapped in after this check can never be
   substituted for any file this function's caller goes on to touch.
   """
   try:
      fd = os.open(
         run_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
   except OSError as exc:
      raise RecoveryFinalizeError(
         "cannot open run directory %r: %s" % (run_dir, exc)) from exc

   try:
      info = os.fstat(fd)
   except OSError:
      os.close(fd)
      raise

   if not stat.S_ISDIR(info.st_mode):
      os.close(fd)
      raise RecoveryFinalizeError(
         "refusing to finalize %r: not a real directory (symlink or "
         "other non-directory substituted for the run directory)"
         % (run_dir,))
   if info.st_uid != os.geteuid():
      os.close(fd)
      raise RecoveryFinalizeError(
         "refusing to finalize %r: owned by uid %d, not the current "
         "effective uid %d" % (run_dir, info.st_uid, os.geteuid()))
   return fd


def _open_child_verified(dir_fd, name):
   """Open a single named child of an already-verified directory fd,
   with the same ``O_NOFOLLOW`` + owner-verification discipline as
   ``_open_verified_run_dir``, resolved relative to ``dir_fd`` (never a
   freshly joined path string) so a symlink or a swapped file cannot be
   substituted for that one child between this check and any later use
   of the SAME already-open descriptor. Returns ``None`` (opening
   nothing) if the child does not exist at all -- a genuinely absent
   optional artifact (e.g. a record type the run never wrote a single
   record for) is not itself suspicious.
   """
   try:
      fd = os.open(
         name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=dir_fd)
   except FileNotFoundError:
      return None
   except OSError as exc:
      raise RecoveryFinalizeError(
         "cannot open %r inside run directory: %s" % (name, exc)) from exc

   try:
      info = os.fstat(fd)
   except OSError:
      os.close(fd)
      raise
   if not stat.S_ISREG(info.st_mode):
      os.close(fd)
      raise RecoveryFinalizeError(
         "refusing to finalize: %r is not a regular file (symlink or "
         "other non-regular object substituted for a run artifact)"
         % (name,))
   if info.st_uid != os.geteuid():
      os.close(fd)
      raise RecoveryFinalizeError(
         "refusing to finalize: %r is owned by uid %d, not the current "
         "effective uid %d" % (name, info.st_uid, os.geteuid()))
   return fd


_RUN_ID_RE = re.compile(r"^phase0-(?P<run_id>.+)$")


def finalize_orphaned_run(run_dir, run_id=None):
   """Finalize an orphaned Phase 0 run directory whose daemon process
   died before writing ``summary.json``/``DONE``, WITHOUT reimplementing
   any checksum/validation logic of its own -- every artifact is scanned
   through the exact same production ``scan_jsonl_artifact`` this
   module's own ``Phase0Sink.finalize_summary`` uses, so an orphaned run
   is validated by the identical, already-reviewed streaming code path a
   clean run goes through, never a second hand-rolled implementation.

   Strictly fail-closed:

   * Refuses a run directory that is not a real, owned-by-us directory,
     or is a symlink (see ``_open_verified_run_dir``) -- guards against
     both a plain path-substitution attack and a symlink swapped in
     between any earlier check (e.g. an operator's ``ls``) and this
     call.
   * Refuses any per-artifact child (a ``.jsonl`` file, ``manifest.json``,
     an existing ``summary.json``) that is not a real, owned-by-us,
     regular file once opened by this SAME already-verified directory
     descriptor (``_open_child_verified``) -- a symlink or a swapped
     file for any one artifact is refused, not silently followed.
   * NEVER overwrites an existing ``summary.json`` or ``DONE`` -- either
     one already present is an unconditional refusal (``FileExistsError``
     is not close enough: this function never even attempts the write
     when either is already there), because a run that already reached
     one of those states was not actually orphaned, and overwriting
     either would risk destroying real historical data with an
     inferior, reconstructed one.
   * NEVER fabricates the daemon's own in-memory acceptance telemetry
     (bounded scheduling-delay/probe-wall-time samples, per-node
     success totals, complete-window counts) -- that state lived only
     in the crashed process and is gone. The produced summary has no
     ``acceptance`` key at all (exactly ``Phase0Sink.finalize_summary``'s
     own documented behavior when its optional ``acceptance_fn`` hook is
     omitted), and instead carries an explicit ``recovery`` section
     (see below) so nothing downstream can mistake this artifact for an
     ordinary clean daemon completion.
   * Never runs with elevated privileges of its own, never touches
     PostgreSQL or any remote system, and never removes/modifies any
     ``.jsonl`` artifact -- it only ever reads them (through the shared
     directory fd) and writes two new files, ``summary.json`` and
     ``DONE``.

   ``run_dir``: the exact orphaned run directory to finalize (e.g.
      ``/home/parton/phase0-runs/phase0-canary-20260916T172446Z``). No
      "most recent run" discovery happens here -- a recovery operation
      must always name its target explicitly, never guess.
   ``run_id``: the run id to record in the summary. Defaults to the
      ``<id>`` parsed out of ``phase0-<id>`` in ``os.path.basename(run_dir)``;
      pass this explicitly only when the directory was renamed away from
      that naming convention (raises ``RecoveryFinalizeError`` if omitted
      and the basename does not match).

   Returns the summary dict that was written (identical shape to
   ``Phase0Sink.finalize_summary``'s return value, plus the added
   ``recovery`` key).

   Raises ``RecoveryFinalizeError`` (a ``Phase0SinkError`` subclass) for
   every refusal case above. Raises ``OSError`` for a genuine I/O
   failure (e.g. disk full while writing the recovered summary) --
   deliberately NOT swallowed, so a recovery run that cannot actually
   write its output fails loudly rather than silently reporting success.
   """
   abs_run_dir = os.path.abspath(run_dir)
   dir_fd = _open_verified_run_dir(abs_run_dir)
   try:
      return _finalize_orphaned_run_locked(abs_run_dir, dir_fd, run_id)
   finally:
      os.close(dir_fd)


def _atomic_write_json_dir_fd(dir_fd, name, payload):
   """Write ``payload`` as JSON to a NEW file named ``name`` inside the
   directory referenced by ``dir_fd``, atomically and WITHOUT ever
   clobbering an existing file of that name.

   Every step (temp-file create, publish, temp-file removal) happens
   relative to ``dir_fd`` (never a freshly re-resolved path string), so
   nothing between this function's own steps can be swapped out from
   under it. Publishing the temp file under the real name uses
   ``os.link()`` rather than ``os.rename()``/``os.replace()`` --
   ``rename``/``replace`` succeed by DESIGN even when the destination
   already exists (silently clobbering it), which is exactly the
   behavior this function must never have; ``os.link()`` instead raises
   ``FileExistsError`` when the destination name is already taken,
   giving "publish without ever clobbering" as a single atomic
   filesystem operation with no separate existence probe beforehand
   (a probe-then-act pair would itself be the exact TOCTOU gap this
   whole module works to avoid). This is the same "write-temp, publish,
   remove temp" shape ``_atomic_write_json`` already uses for a
   path-based destination, adapted to dir_fd-relative operations for a
   caller (``finalize_orphaned_run``) that must never re-resolve the run
   directory by path after its initial verified open.

   Raises ``FileExistsError`` if ``name`` already exists -- the caller
   must not have already probed for existence and treated an ABSENT
   file as a green light before calling this (that gap is exactly what
   this function's ``os.link`` atomicity closes).
   """
   tmp_name = name + ".tmp-%d" % os.getpid()
   fd = os.open(
      tmp_name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
      _FILE_MODE, dir_fd=dir_fd)
   try:
      with os.fdopen(fd, "w") as handle:
         json.dump(payload, handle, indent=2, sort_keys=True)
         handle.write("\n")
         handle.flush()
         os.fsync(handle.fileno())
      os.chmod(tmp_name, _FILE_MODE, dir_fd=dir_fd)
      os.link(tmp_name, name, src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
   finally:
      try:
         os.unlink(tmp_name, dir_fd=dir_fd)
      except FileNotFoundError:
         pass
   os.fsync(dir_fd)


def _write_done_file_dir_fd(dir_fd):
   """DONE-flag equivalent of ``_atomic_write_json_dir_fd``: creates the
   file with ``O_EXCL`` (atomically refusing to clobber an existing
   DONE) directly under the real name -- DONE's content is a single
   timestamp line, not something that needs a rename-into-place for
   atomicity of PARTIAL content the way JSON does, but ``O_EXCL`` still
   gives the same "never overwrite" guarantee as ``_atomic_write_json_dir_fd``'s
   link step.
   """
   fd = os.open(
      "DONE", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
      _FILE_MODE, dir_fd=dir_fd)
   with os.fdopen(fd, "w") as handle:
      handle.write(
         time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + "Z\n")
      handle.flush()
      os.fsync(handle.fileno())
   os.chmod("DONE", _FILE_MODE, dir_fd=dir_fd)
   os.fsync(dir_fd)


def _finalize_orphaned_run_locked(abs_run_dir, dir_fd, run_id):
   # Fail fast, before touching anything else, whenever EITHER
   # summary.json or DONE already exists -- checked via the SAME
   # verified directory fd (never a fresh path-based os.path.exists,
   # which would re-resolve the name and reopen exactly the TOCTOU
   # window every other lookup in this function goes out of its way to
   # close). This is a pre-flight convenience check only: the actual
   # writes below are ALSO independently atomic/no-clobber (O_EXCL /
   # os.link) as the real safety net, so a file created between this
   # check and that write is still refused, never silently overwritten.
   # Checking both names up front (rather than only checking summary.json
   # right before writing it) matters for a run in the unusual state of
   # DONE present without summary.json: writing a brand-new summary.json
   # in that case would still not be "overwriting" summary.json, but it
   # would leave a stale, unrelated DONE sitting next to a freshly
   # reconstructed summary -- an inconsistent pairing this function must
   # never produce.
   for existing_name in ("summary.json", "DONE"):
      existing_fd = _open_child_verified(dir_fd, existing_name)
      if existing_fd is not None:
         os.close(existing_fd)
         raise RecoveryFinalizeError(
            "refusing to finalize %r: %r already exists -- this run "
            "was not actually orphaned, or a prior finalize attempt "
            "already completed; recovery never overwrites an existing "
            "summary/DONE" % (abs_run_dir, existing_name))

   if run_id is None:
      match = _RUN_ID_RE.match(os.path.basename(abs_run_dir))
      if not match:
         raise RecoveryFinalizeError(
            "cannot infer run_id from directory name %r (expected "
            "'phase0-<run_id>'); pass run_id explicitly"
            % (os.path.basename(abs_run_dir),))
      run_id = match.group("run_id")

   # manifest.json is optional context only (never required to
   # finalize -- a run that crashed before even writing its manifest
   # is not this incident's shape, but recovery should not need a
   # manifest to do its one job of scanning the JSONL artifacts that
   # DO exist). Verified through the same owned-regular-file check as
   # every other artifact; a symlinked/foreign manifest is refused
   # rather than silently read.
   manifest = None
   manifest_fd = _open_child_verified(dir_fd, "manifest.json")
   if manifest_fd is not None:
      try:
         with os.fdopen(manifest_fd, "r") as handle:
            manifest = json.load(handle)
      except (OSError, ValueError) as exc:
         raise RecoveryFinalizeError(
            "manifest.json exists but could not be read as JSON: %s"
            % (exc,)) from exc

   files_summary = {}
   for record_type, filename in _FILENAMES.items():
      artifact_fd = _open_child_verified(dir_fd, filename)
      if artifact_fd is None:
         # No records of this type were ever written -- exactly the
         # same "safe to call with zero records written to any given
         # file" case Phase0Sink.finalize_summary's own docstring
         # documents, not a recovery-specific defect.
         scan = {
            "byte_size": 0, "sha256": None, "valid_count": 0,
            "malformed_count": 0, "truncated_final_line": False,
         }
         record_count = 0
      else:
         # os.fdopen takes ownership of artifact_fd -- its own `with`
         # block closes it, so no separate os.close is needed or
         # correct here.
         with os.fdopen(artifact_fd, "rb") as artifact_handle:
            scan = _scan_jsonl_fileobj(artifact_handle)
         record_count = scan["valid_count"]
      files_summary[record_type] = {
         "filename": filename,
         "record_count": record_count,
         "byte_size": scan["byte_size"],
         "malformed_count": scan["malformed_count"],
         "truncated_final_line": scan["truncated_final_line"],
         "sha256": scan["sha256"],
      }

   summary = {
      "run_id": run_id,
      "finalized_utc": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + "Z",
      "files": files_summary,
      "recovery": {
         "recovered": True,
         "reason": (
            "daemon process died before finalize_summary()/write_done() "
            "completed (old whole-file finalization path); artifacts "
            "were re-scanned and summarized after the fact by the "
            "recovery finalizer, never by the daemon itself"),
         "recovered_utc": time.strftime(
            "%Y-%m-%dT%H:%M:%S", time.gmtime()) + "Z",
         "manifest_present": manifest is not None,
      },
   }
   # Never overwrite an existing summary.json/DONE. Both writes below
   # are ATOMICALLY all-or-nothing against a concurrently or previously
   # created same-named file (O_EXCL / os.link's own exclusivity) --
   # deliberately NOT a separate "check absent, then write" pair, which
   # would reopen exactly the TOCTOU window every other path-resolution
   # in this function goes out of its way to close. A run that already
   # reached either state was not actually orphaned; this raises
   # FileExistsError (an OSError, so it is NOT silently swallowed by
   # any Phase0SinkError-only catch elsewhere) rather than clobbering
   # real historical data with an inferior, reconstructed one.
   try:
      _atomic_write_json_dir_fd(dir_fd, "summary.json", summary)
   except FileExistsError as exc:
      raise RecoveryFinalizeError(
         "refusing to finalize %r: 'summary.json' already exists -- "
         "this run was not actually orphaned, or a prior finalize "
         "attempt already completed; recovery never overwrites an "
         "existing summary/DONE" % (abs_run_dir,)) from exc
   try:
      _write_done_file_dir_fd(dir_fd)
   except FileExistsError as exc:
      # summary.json was just written successfully above, but DONE
      # already existed -- an inconsistent state (summary present,
      # pre-existing DONE from some other origin) that must be
      # surfaced loudly rather than silently left half-finalized.
      raise RecoveryFinalizeError(
         "wrote summary.json for %r but 'DONE' already existed -- "
         "refusing to overwrite it; the run directory is now in an "
         "inconsistent state (summary.json written, pre-existing DONE "
         "left untouched) and needs manual inspection"
         % (abs_run_dir,)) from exc
   return summary
