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
import gzip
import hashlib
import json
import os
import re
import shutil
import stat
import time
import zlib

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

# Kanban task B7: the diagnostic_census artifact is by far the largest
# (a real 24h canary produced 1,764,399,017 bytes) and Workstream B
# roughly doubles its per-row size, so it is the only artifact that
# may optionally be written gzip-compressed on disk
# (Phase0Config.compress_census, default False). Every OTHER record
# type is always plain JSONL -- there is deliberately no generalized
# per-record-type compression map, only this one flag-gated filename
# for the one artifact that is worth it.
_CENSUS_RECORD_TYPE = "diagnostic_census"
_CENSUS_FILENAME_PLAIN = _FILENAMES[_CENSUS_RECORD_TYPE]
_CENSUS_FILENAME_GZIP = _CENSUS_FILENAME_PLAIN + ".gz"


def _filenames_for(compress_census):
   """Return the record_type -> on-disk-filename map to use for one run,
   identical to ``_FILENAMES`` except the census entry is swapped for
   its ``.gz`` counterpart when ``compress_census`` is True. A small
   indirection over a single shared base map -- never a second,
   independently maintained ``_FILENAMES``-shaped dict that could
   silently drift from it (e.g. if a future record type were added to
   one map and not the other).
   """
   if not compress_census:
      return _FILENAMES
   filenames = dict(_FILENAMES)
   filenames[_CENSUS_RECORD_TYPE] = _CENSUS_FILENAME_GZIP
   return filenames


# gzip magic bytes -- the two leading bytes of every valid gzip stream
# (RFC 1952 section 2.3.1: ID1=0x1f, ID2=0x8b). Detected by CONTENT,
# never trusted from the filename extension alone (design: "gzip
# detected by magic bytes") -- a ``.jsonl.gz`` file that is somehow
# plain text, or a plain ``.jsonl`` file that somehow got gzipped
# content, is handled correctly either way.
_GZIP_MAGIC = b"\x1f\x8b"

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

   Kanban task B7: gzip-transparent, exactly like ``scan_jsonl_artifact``
   -- this is now a thin wrapper around that function (never a second,
   independently maintained line-validation implementation that could
   silently drift from it, gzip-aware or otherwise) that simply drops
   the two fields (``byte_size``/``sha256``) this function's callers
   never asked for. Detects gzip by CONTENT (magic bytes), never by
   trusting ``path``'s extension.

   Returns {"valid_count", "malformed_count", "truncated_final_line"}.
   """
   scan = scan_jsonl_artifact(path)
   return {
      "valid_count": scan["valid_count"],
      "malformed_count": scan["malformed_count"],
      "truncated_final_line": scan["truncated_final_line"],
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

   If the first bytes read from ``handle`` are the gzip magic
   (``_GZIP_MAGIC``), this transparently routes through the bounded
   gzip-decode scan (``_scan_gzip_fileobj``) instead of treating the
   on-disk bytes as plain JSONL text -- kanban task B7: gzip is
   detected by CONTENT, never trusted from a filename extension alone,
   so a caller need not know or care whether the path it opened is
   ``diagnostic_censuses.jsonl`` or ``diagnostic_censuses.jsonl.gz``.
   """
   peek = handle.read(len(_GZIP_MAGIC))
   if peek == _GZIP_MAGIC:
      return _scan_gzip_fileobj(handle, peek, chunk_bytes=chunk_bytes)
   return _scan_plain_jsonl_fileobj(handle, peek, chunk_bytes=chunk_bytes)


class _LineAccountant:
   """Streaming JSONL line-accounting state machine: counts valid/
   malformed lines and detects a truncated final line, from whatever
   raw bytes are fed to it via ``feed()`` -- one line's worth of state
   at a time, never the whole artifact. Shared by both
   ``_scan_plain_jsonl_fileobj`` (fed directly from the file's own
   bytes) and ``_scan_gzip_fileobj`` (fed from the bounded gzip-decode
   output) so the two scans agree EXACTLY on what counts as valid,
   malformed, or truncated -- there is only one JSONL-grammar verdict
   implementation in this module, regardless of what container format
   the bytes arrived in.

   This is exactly the fast-path-bytearray / bounded-memory-fallback
   two-tier strategy ``_scan_plain_jsonl_fileobj`` (née
   ``_scan_jsonl_fileobj``) has always used -- see that function's
   original docstring/review history for why both tiers are required
   together. Extracted into its own class purely so it can be driven
   by two different byte sources without duplicating the (subtle,
   multiply-reviewed) per-line state machine.
   """

   def __init__(self):
      self.valid_count = 0
      self.malformed_count = 0
      self.truncated_final_line = False
      self._line_buf = bytearray()
      self._fallback_validator = None
      self._fallback_decoder = None
      self._fallback_failed = False

   def _decode_incremental(self, raw_bytes, final):
      return self._fallback_decoder.decode(raw_bytes, final)

   def _start_fallback(self):
      self._fallback_validator = IncrementalJsonValidator()
      self._fallback_decoder = codecs.getincrementaldecoder("utf-8")("replace")
      self._fallback_failed = False
      if self._line_buf:
         text = self._decode_incremental(bytes(self._line_buf), False)
         try:
            self._fallback_validator.feed_str(text)
         except JsonSyntaxError:
            self._fallback_failed = True
      self._line_buf = bytearray()

   def _feed_fallback(self, raw_bytes):
      text = self._decode_incremental(raw_bytes, False)
      if self._fallback_failed:
         return
      try:
         self._fallback_validator.feed_str(text)
      except JsonSyntaxError:
         self._fallback_failed = True

   def _finalize_fallback_line(self):
      tail_text = self._decode_incremental(b"", True)
      if tail_text and not self._fallback_failed:
         try:
            self._fallback_validator.feed_str(tail_text)
         except JsonSyntaxError:
            self._fallback_failed = True
      if self._fallback_failed:
         self.malformed_count += 1
      else:
         try:
            self._fallback_validator.finish()
            self.valid_count += 1
         except JsonSyntaxError:
            self.malformed_count += 1
      self._fallback_validator = None
      self._fallback_decoder = None

   def _append_to_current_line(self, segment):
      if self._fallback_validator is not None:
         if segment:
            self._feed_fallback(segment)
         return
      if segment:
         self._line_buf.extend(segment)
      if len(self._line_buf) > _FAST_PATH_LINE_LIMIT_BYTES:
         self._start_fallback()

   def _finalize_complete_line(self):
      if self._fallback_validator is not None:
         self._finalize_fallback_line()
         return
      if self._line_buf:
         try:
            json.loads(bytes(self._line_buf).decode("utf-8", "replace"))
            self.valid_count += 1
         except ValueError:
            self.malformed_count += 1
      # else: an empty line (consecutive newlines) -- counted as
      # neither valid nor malformed, matching validate_jsonl_artifact.

   def feed(self, chunk):
      """Feed one bounded chunk of raw (already-decompressed, for the
      gzip caller) bytes, splitting it on ``b"\\n"`` and finalizing
      every complete line found. At most ``chunk`` worth of new data is
      ever processed per call; the still-open final line's bytes are
      carried in internal state (bounded exactly as
      ``_scan_plain_jsonl_fileobj`` always was) across calls.
      """
      start = 0
      chunk_len = len(chunk)
      while True:
         newline_index = chunk.find(b"\n", start)
         if newline_index == -1:
            remainder = chunk[start:chunk_len]
            self._append_to_current_line(remainder)
            break
         segment = chunk[start:newline_index]
         self._append_to_current_line(segment)
         self._finalize_complete_line()
         self._line_buf = bytearray()
         start = newline_index + 1

   def finalize(self, forced_truncated=False):
      """Called once at the true end of input. ``forced_truncated``
      lets a caller (the gzip scan) declare the final line truncated
      REGARDLESS of whether it parses as valid JSON -- e.g. a gzip
      stream that never reached a clean end-of-stream marker means the
      underlying JSONL bytes themselves may be incomplete even where
      they happen to look like well-formed JSON so far.
      """
      if forced_truncated:
         self.truncated_final_line = True
         return
      if self._fallback_validator is not None:
         tail_text = self._decode_incremental(b"", True)
         if tail_text and not self._fallback_failed:
            try:
               self._fallback_validator.feed_str(tail_text)
            except JsonSyntaxError:
               self._fallback_failed = True
         if self._fallback_failed:
            self.truncated_final_line = True
         else:
            try:
               self._fallback_validator.finish()
               self.valid_count += 1
            except JsonSyntaxError:
               self.truncated_final_line = True
      elif self._line_buf:
         try:
            json.loads(bytes(self._line_buf).decode("utf-8", "replace"))
            self.valid_count += 1
         except ValueError:
            self.truncated_final_line = True


def _scan_plain_jsonl_fileobj(handle, first_bytes, chunk_bytes=_SCAN_CHUNK_BYTES):
   """The plain-text JSONL scan behind ``_scan_jsonl_fileobj``, given
   whichever bytes were already peeked off the front of ``handle`` to
   decide it was NOT gzip (``first_bytes`` -- up to
   ``len(_GZIP_MAGIC)`` bytes, already consumed from ``handle`` and
   must be folded back into the scan as the start of the first chunk,
   never re-read).
   """
   digest = hashlib.sha256()
   byte_size = 0
   accountant = _LineAccountant()

   while True:
      chunk = first_bytes if first_bytes is not None else handle.read(chunk_bytes)
      first_bytes = None
      if not chunk:
         break
      digest.update(chunk)
      byte_size += len(chunk)
      accountant.feed(chunk)

   accountant.finalize()

   return {
      "byte_size": byte_size,
      "sha256": digest.hexdigest(),
      "valid_count": accountant.valid_count,
      "malformed_count": accountant.malformed_count,
      "truncated_final_line": accountant.truncated_final_line,
   }


# Bounded output chunk size for the gzip decompressor -- how much
# DECOMPRESSED data ``zlib.decompressobj.decompress()`` is allowed to
# hand back per call. Mirrors ``_SCAN_CHUNK_BYTES``'s own bound but on
# the decompressed side: capping this is what keeps a highly
# compressible pathological gzip member (e.g. gigabytes of repeated
# bytes compressing down to a tiny compressed size) from handing back
# an enormous decompressed blob in one call -- the compressed
# ``chunk_bytes`` read alone would not bound that.
_GZIP_DECODE_OUT_CHUNK_BYTES = 1 << 20  # 1 MiB per decompress() call.


def _scan_gzip_fileobj(handle, first_bytes, chunk_bytes=_SCAN_CHUNK_BYTES):
   """Bounded-memory scan of a gzip-compressed JSONL artifact, given
   whichever bytes were already peeked off the front of ``handle`` to
   detect the gzip magic (``first_bytes`` -- exactly ``_GZIP_MAGIC``,
   already consumed from ``handle`` and folded back as the start of
   the first compressed chunk, never re-read).

   Design: kanban task B7's validated bounded gzip-decode approach.
   ``handle`` is read in fixed-size COMPRESSED chunks (``chunk_bytes``,
   same bound as the plain scan) -- a running ``hashlib.sha256`` digests
   each compressed chunk exactly as read, so ``byte_size``/``sha256``
   describe the ON-DISK (compressed) bytes, never the decompressed
   payload. Each compressed chunk is fed to a single
   ``zlib.decompressobj(zlib.MAX_WBITS | 16)`` (the documented
   incantation for gzip-framed, as opposed to raw zlib-framed, data);
   its decompressed output is drained in bounded
   ``_GZIP_DECODE_OUT_CHUNK_BYTES``-sized pieces (via the
   ``unconsumed_tail`` loop) and fed straight into a ``_LineAccountant``
   -- the SAME line-validation state machine ``_scan_plain_jsonl_fileobj``
   uses, so a gzip census and a plain census agree exactly on what
   counts as a valid/malformed/truncated line. At no point is the
   whole compressed file, or the whole decompressed payload, held in
   memory at once.

   Truncation semantics (empirically validated during this card's
   design phase): a clean, fully-closed gzip stream decompresses with
   ``decompressor.eof`` ending True and never raises; ANY of (a) a
   ``zlib.error`` raised mid-decode (CRC/incorrect-data-check -- a
   corrupted member), (b) input exhausted with ``eof`` still False (a
   trailer cut short, or a stream truncated anywhere before its final
   member), is reported as ``truncated_final_line=True`` -- exactly
   mirroring the plain scan's truncated-final-line semantics, just
   keyed off the gzip framing's own end-of-stream signal instead of a
   missing trailing newline. Earlier, successfully-decompressed lines
   are still counted normally; a truncated/corrupt gzip never crashes
   this function and never silently reports as clean.

   A genuinely empty (0 records) but CLEANLY CLOSED gzip member (i.e.
   ``gzip.compress(b"")``, or ``Phase0Sink``'s write path finalizing
   with zero writes) decompresses to zero bytes with ``eof=True`` --
   correctly NOT truncated. A literal zero-byte file (no gzip framing
   at all) can only reach this function if its first two bytes somehow
   still matched ``_GZIP_MAGIC``, which is impossible for a 0-byte
   file -- that case is handled by ``_scan_jsonl_fileobj``'s peek
   returning ``b\"\"`` and routing to the plain scan instead, which
   already reports a 0-byte file as valid_count=0/malformed_count=0/
   truncated_final_line=False (an empty artifact, not a truncated
   one) -- consistent with a "no records written" run.
   """
   digest = hashlib.sha256()
   byte_size = 0
   accountant = _LineAccountant()
   decompressor = zlib.decompressobj(zlib.MAX_WBITS | 16)
   decode_error = False

   while True:
      compressed = first_bytes if first_bytes is not None else handle.read(chunk_bytes)
      first_bytes = None
      if not compressed:
         break
      digest.update(compressed)
      byte_size += len(compressed)

      if decode_error or decompressor.eof:
         # Already known-corrupt or already reached a clean end-of-
         # stream marker: keep consuming (and hashing/sizing) whatever
         # bytes remain on disk -- e.g. trailing garbage after a valid
         # member -- without feeding them to the decompressor again.
         continue

      pending = compressed
      while pending:
         try:
            piece = decompressor.decompress(pending, _GZIP_DECODE_OUT_CHUNK_BYTES)
         except zlib.error:
            decode_error = True
            break
         if piece:
            accountant.feed(piece)
         pending = decompressor.unconsumed_tail
         if decompressor.eof:
            break

   if not decode_error and not decompressor.eof:
      try:
         tail = decompressor.flush()
         if tail:
            accountant.feed(tail)
      except zlib.error:
         decode_error = True

   truncated = decode_error or not decompressor.eof
   accountant.finalize(forced_truncated=truncated)

   return {
      "byte_size": byte_size,
      "sha256": digest.hexdigest(),
      "valid_count": accountant.valid_count,
      "malformed_count": accountant.malformed_count,
      "truncated_final_line": accountant.truncated_final_line,
   }


class _GzipTextAppendHandle:
   """Adapts a ``gzip.GzipFile`` (binary, append-mode) plus its
   underlying raw binary file object to the same narrow interface
   ``write_record``/``finalize_summary`` already use against a plain
   ``os.fdopen(fd, "a", encoding="utf-8")`` text handle: ``write(str)``,
   ``flush()``, ``fileno()``, ``close()``.

   Kept deliberately minimal -- this is not a general-purpose file-like
   shim, just the exact four operations the rest of this module needs,
   so gzip-vs-plain stays an invisible implementation detail to every
   caller of ``Phase0Sink._handle_for``.

   ``write(text)`` encodes ``text`` as UTF-8 (matching the plain
   handle's ``encoding="utf-8"``) and feeds it to the ``GzipFile``,
   which itself buffers/compresses/writes to the underlying raw binary
   file object as needed -- ``write_record`` calling ``.flush()``
   right after every ``write()`` (exactly as it always has for the
   plain path) is what keeps each record's compressed bytes actually
   pushed into the raw OS-level file promptly, not held indefinitely
   inside zlib's own internal buffer.

   ``fileno()`` returns the RAW underlying fd (never the ``GzipFile``
   object's own, which does not implement a real ``fileno()`` in every
   version) -- ``write_record``'s ``os.fsync(handle.fileno())`` must
   fsync the actual on-disk bytes, and ``flush()`` is called on the
   ``GzipFile`` immediately beforehand in both call sites to guarantee
   the compressor's pending output has already been pushed into that
   same raw fd before the fsync happens.

   ``close()`` closes the ``GzipFile`` FIRST (writing gzip's own
   trailer/CRC/end-of-stream marker so the on-disk stream is a cleanly
   terminated gzip member -- this is what makes a 0-record run's
   census a validly-closed EMPTY-payload gzip, not a truncated one),
   THEN closes the raw underlying file object.
   """

   def __init__(self, gzobj, raw):
      self._gzobj = gzobj
      self._raw = raw

   def write(self, text):
      self._gzobj.write(text.encode("utf-8"))

   def flush(self):
      self._gzobj.flush()

   def fileno(self):
      return self._raw.fileno()

   def close(self):
      self._gzobj.close()
      self._raw.close()


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
                clock=time.monotonic, disk_usage_fn=shutil.disk_usage,
                compress_census=False, keep_raw_args=False):
      self.output_root = output_root
      self.run_id = run_id
      self.run_dir = os.path.join(output_root, "phase0-%s" % run_id)
      self._flush_interval_sec = flush_interval_sec
      self._min_free_disk_pct = min_free_disk_pct
      self._clock = clock
      self._disk_usage_fn = disk_usage_fn
      # Kanban task C1: reversed privacy posture -- threaded into
      # validate_record() for diagnostic_census writes only (see
      # write_record below); every other record type ignores it.
      self._keep_raw_args = keep_raw_args
      # Kanban task B7: opt-in gzip compression for the diagnostic_census
      # artifact only -- see _filenames_for()'s own docstring. Resolved
      # ONCE at construction into self._filenames so every other method
      # (write, finalize) uses this single per-run filename map rather
      # than re-deciding per call.
      self._compress_census = compress_census
      self._filenames = _filenames_for(compress_census)

      # Created lazily (see _get_lock) rather than here: asyncio.Lock()
      # binds to the running event loop at construction time on Python
      # 3.9, but Phase0Sink itself is constructed synchronously outside
      # any loop (the daemon builds the sink before starting its event
      # loop). Deferring construction to first async use avoids a
      # RuntimeError("no current event loop") for perfectly normal
      # synchronous construction.
      self._lock = None
      self._file_handles = {}
      self._record_counts = {name: 0 for name in self._filenames}
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
      """Return the (lazily opened, cached) writable text handle for
      ``record_type``, honoring ``self._compress_census``.

      For the plain path this is the same append-mode text handle as
      before. For the gzip-compressed census, the underlying fd is
      opened exactly the same way (``O_WRONLY|O_CREAT|O_APPEND``,
      mode 0600) and then wrapped in a ``gzip.GzipFile`` in append
      mode so ``write_record`` can call ``.write(line + "\\n")``
      identically regardless of which path this run uses. Kanban task
      B7 design: "on crash the gzip census may lose slightly more
      trailing data than plain, but the run is recoverable and
      truncation is REPORTED" -- see ``write_record``'s fsync step and
      ``_scan_gzip_fileobj`` for how that truncation is detected.
      """
      handle = self._file_handles.get(record_type)
      if handle is None:
         path = os.path.join(self.run_dir, self._filenames[record_type])
         fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, _FILE_MODE)
         if self._compress_census and record_type == _CENSUS_RECORD_TYPE:
            raw = os.fdopen(fd, "ab")
            gzobj = gzip.GzipFile(fileobj=raw, mode="ab")
            handle = _GzipTextAppendHandle(gzobj, raw)
         else:
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

      validated = validate_record(record_type, record, keep_raw_args=self._keep_raw_args)
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
         for record_type, filename in self._filenames.items():
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
   * NEVER publishes ``summary.json``/``DONE`` when the artifact content
     itself cannot be trusted as a complete, uncorrupted dataset: a run
     directory with zero ``.jsonl`` artifacts at all, or any artifact
     with a malformed line or a truncated final line, is refused
     BEFORE either output is written -- recovery reconstructs a
     genuinely complete dataset's terminal state, never blesses
     corrupt or empty input as finalized.
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
   """DONE-flag equivalent of ``_atomic_write_json_dir_fd``, published
   with the exact same write-temp/fsync/link/no-clobber discipline --
   the content is written and fsynced into a hidden temp name FIRST,
   and only linked into the real ``DONE`` name once that content is
   safely on disk. A crash or write/fsync failure partway through
   therefore can never leave a final-named ``DONE`` at all (empty or
   partial), which would otherwise falsely signal a completed
   finalization to anything checking for ``DONE``'s mere presence
   (e.g. ``validate-run``). ``os.link`` (not ``os.rename``/``os.replace``)
   is used for the same reason ``_atomic_write_json_dir_fd`` uses it:
   it raises ``FileExistsError`` rather than silently clobbering an
   existing ``DONE``.
   """
   tmp_name = "DONE.tmp-%d" % os.getpid()
   fd = os.open(
      tmp_name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
      _FILE_MODE, dir_fd=dir_fd)
   try:
      with os.fdopen(fd, "w") as handle:
         handle.write(
            time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + "Z\n")
         handle.flush()
         os.fsync(handle.fileno())
      os.chmod(tmp_name, _FILE_MODE, dir_fd=dir_fd)
      os.link(tmp_name, "DONE", src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
   finally:
      try:
         os.unlink(tmp_name, dir_fd=dir_fd)
      except FileNotFoundError:
         pass
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
   any_artifact_present = False
   for record_type, filename in _FILENAMES.items():
      if record_type == _CENSUS_RECORD_TYPE:
         # The gzip flag is a per-run Phase0Sink construction choice
         # (Phase0Config.compress_census) that this recovery path has
         # no other record of -- an orphaned directory carries no
         # manifest field for it, so both possible on-disk census
         # names are tried. At most one can exist for any given run
         # (Phase0Sink always opens exactly one of them), so trying
         # the gzip name first when present is unambiguous; falling
         # back to the plain name preserves this function's existing
         # "never written" zero-count case when NEITHER exists.
         filename = _CENSUS_FILENAME_GZIP
         artifact_fd = _open_child_verified(dir_fd, filename)
         if artifact_fd is None:
            filename = _CENSUS_FILENAME_PLAIN
            artifact_fd = _open_child_verified(dir_fd, filename)
      else:
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
         any_artifact_present = True
         # os.fdopen takes ownership of artifact_fd -- its own `with`
         # block closes it, so no separate os.close is needed or
         # correct here. _scan_jsonl_fileobj itself detects gzip vs
         # plain content by magic bytes (never trusting `filename`'s
         # extension), so this one call is correct for either on-disk
         # form -- the SAME scanner Phase0Sink.finalize_summary uses,
         # never a forked/duplicate scan implementation.
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

   # Strictly fail-closed on the artifact content itself, BEFORE either
   # summary.json or DONE is published: this recovery tool's entire
   # purpose is to finalize a run whose daemon collected a genuinely
   # complete dataset and only failed to reach the old whole-file
   # finalization step -- never to bless an empty, corrupt, or
   # mid-write directory as "finalized". A directory with zero .jsonl
   # artifacts at all is not this incident's shape (nothing was ever
   # collected, or the wrong directory was pointed at); any malformed
   # line or a truncated final line means the artifact content itself
   # cannot be trusted as a complete, uncorrupted dataset. Earlier
   # per-file accounting (malformed_count/truncated_final_line) still
   # exists above for diagnostic purposes inside this function's own
   # scan, but must never reach a published summary/DONE -- refusal
   # happens here, before either write, not merely reported alongside
   # a "successful" finalize.
   if not any_artifact_present:
      raise RecoveryFinalizeError(
         "refusing to finalize %r: no .jsonl artifact files found -- "
         "this is not an orphaned run with a complete collected "
         "dataset (nothing was ever written, or this is the wrong "
         "directory)" % (abs_run_dir,))
   malformed_files = sorted(
      record_type for record_type, entry in files_summary.items()
      if entry["malformed_count"] > 0)
   if malformed_files:
      raise RecoveryFinalizeError(
         "refusing to finalize %r: malformed JSONL line(s) found in "
         "%s -- artifact content cannot be trusted as a complete, "
         "uncorrupted dataset; recovery never publishes summary.json/"
         "DONE over corrupt input" % (abs_run_dir, ", ".join(malformed_files)))
   truncated_files = sorted(
      record_type for record_type, entry in files_summary.items()
      if entry["truncated_final_line"])
   if truncated_files:
      raise RecoveryFinalizeError(
         "refusing to finalize %r: truncated final line found in %s "
         "-- artifact content cannot be trusted as a complete, "
         "uncorrupted dataset; recovery never publishes summary.json/"
         "DONE over corrupt input" % (abs_run_dir, ", ".join(truncated_files)))

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
