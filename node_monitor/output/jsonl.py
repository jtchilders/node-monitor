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
import shutil
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

   Returns ``{\"byte_size\", \"sha256\", \"valid_count\", \"malformed_count\",
   \"truncated_final_line\"}`` -- the same fields ``finalize_summary``
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

   with open(path, "rb") as handle:
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
      path = os.path.join(self.run_dir, "DONE")
      fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, _FILE_MODE)
      with os.fdopen(fd, "w") as handle:
         handle.write(
            time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + "Z\n")
         handle.flush()
         os.fsync(handle.fileno())
      os.chmod(path, _FILE_MODE)
