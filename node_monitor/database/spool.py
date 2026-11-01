"""Append-only on-disk store of record batches, used while PostgreSQL is unreachable.

The collector must never lose data to an outage and must never crash because of the spool,
so every failure here is reported in a result rather than raised.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from node_monitor.database.codec import decode_record, encode_record
from node_monitor.pipeline.records import LIVE_NODE, LIVE_PROCESSES, Record

log = logging.getLogger(__name__)

_SUFFIX = ".jsonl"
# `<stamp>-<seq>.jsonl`, or `<stamp>-<seq>-L.jsonl` for a batch of live records only, so
# eviction can tell them apart without reading files. Names written before the marker
# existed have none and count as not live-only. Either way names sort in write order.
_LIVE_MARK = "-L"
_NAME_RE = re.compile(r"^\d{8}T\d{6}\.\d{6}-(\d{6})(?:-L)?\.jsonl$")
# Live rows only ever describe a node's current state, so a newer batch supersedes them.
LIVE_KINDS = frozenset({LIVE_PROCESSES, LIVE_NODE})
_ERROR_CHARS = 200


def _describe(exc: BaseException) -> str:
   return f"{type(exc).__name__}: {str(exc)[:_ERROR_CHARS]}"


def is_live_only(path: Path) -> bool:
   """True for a batch written with nothing but LIVE_KINDS records."""
   return path.name.endswith(_LIVE_MARK + _SUFFIX)


@dataclass(frozen=True)
class SpoolResult:
   stored: bool
   dropped_batches: int = 0
   dropped_records: int = 0
   error: str | None = None


class Spool:
   def __init__(self, directory: Path, *, max_bytes: int):
      self.directory = Path(directory)
      self.max_bytes = max_bytes
      self._lock = threading.Lock()
      self._seq = 0
      # (file name, reason) of each batch pending() quarantined, until take_quarantined().
      self._quarantined: list[tuple[str, str]] = []
      # Unreadable batches that could not be moved to corrupt/: reported once, then skipped.
      self._stuck: set[Path] = set()
      self._ensure_directory()

   def _ensure_directory(self) -> str | None:
      """Create the directory if needed and re-seed the sequence; returns an error or None.

      Called again at write time because the directory can vanish or be uncreatable at
      startup, and the spool must recover once it is available rather than stay disabled.
      """
      try:
         self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
         # mkdir's mode is masked by umask and ignored for an existing directory.
         os.chmod(self.directory, 0o700)
      except OSError as e:
         log.error("spool directory %s unusable: %s", self.directory, e)
         return f"{type(e).__name__}: {e}"
      # Continue past any existing file so a restart never reuses a name.
      for p in self._batch_files():
         m = _NAME_RE.match(p.name)
         if m:
            self._seq = max(self._seq, int(m.group(1)))
      return None

   def _batch_files(self) -> list[Path]:
      try:
         names = os.listdir(self.directory)
      except OSError:
         return []
      return [
         self.directory / n
         for n in sorted(names)
         if n.endswith(_SUFFIX) and not n.startswith(".") and (self.directory / n).is_file()
      ]

   @staticmethod
   def _size(path: Path) -> int:
      try:
         return path.stat().st_size
      except OSError:
         return 0

   def size_bytes(self) -> int:
      return sum(self._size(p) for p in self._batch_files())

   def batch_count(self) -> int:
      return len(self._batch_files())

   def batch_files(self) -> list[Path]:
      """The stored batches, oldest first. Only names are read."""
      return self._batch_files()

   @staticmethod
   def _count_records(path: Path) -> int:
      """Lines in a batch, one record each; 0 if it cannot be read."""
      try:
         with open(path, "rb") as f:
            return sum(1 for line in f if line.strip())
      except OSError:
         return 0

   def _drop(self, path: Path) -> int | None:
      """Delete a batch and return its record count, or None if it could not be deleted."""
      count = self._count_records(path)
      try:
         path.unlink()
      except OSError:
         return None
      return count

   def _make_room(self, needed: int) -> tuple[int, int]:
      """Delete batches until `needed` more bytes fit; superseded live-only batches go first.

      Only file names and sizes are consulted, so an append costs the same however much is
      spooled; just the batches being dropped are read, to count their records.
      """
      files = self._batch_files()
      sizes = {p: self._size(p) for p in files}
      total = sum(sizes.values())
      batches = records = 0
      if total + needed <= self.max_bytes:
         return batches, records
      live_only = [p for p in files if is_live_only(p)]
      for p in live_only + [p for p in files if not is_live_only(p)]:
         if total + needed <= self.max_bytes:
            break
         n = self._drop(p)
         if n is None:
            continue
         total -= sizes[p]
         batches += 1
         records += n
      return batches, records

   def append(self, records: list[Record]) -> SpoolResult:
      """Store one batch. Never raises."""
      try:
         return self._append(records)
      except Exception as e:  # noqa: BLE001 - the collector must survive any spool failure
         log.error("spool append failed: %s", e)
         return SpoolResult(False, error=f"{type(e).__name__}: {e}")

   def _append(self, records: list[Record]) -> SpoolResult:
      if not records:
         return SpoolResult(True)
      payload = "".join(json.dumps(encode_record(r), separators=(",", ":")) + "\n" for r in records)
      data = payload.encode("utf-8")
      if len(data) > self.max_bytes:
         return SpoolResult(
            False, error=f"batch of {len(data)} bytes exceeds spool max_bytes {self.max_bytes}"
         )
      with self._lock:
         if not self.directory.is_dir():
            err = self._ensure_directory()
            if err:
               return SpoolResult(False, error=err)
         dropped_b, dropped_r = self._make_room(len(data))
         live_only = all(r.kind in LIVE_KINDS for r in records)
         for attempt in (1, 2):
            try:
               self._write(self.directory, data, live_only)
               break
            except FileNotFoundError as e:
               if attempt == 2 or self._ensure_directory():
                  return SpoolResult(False, dropped_b, dropped_r, f"{type(e).__name__}: {e}")
            except OSError as e:
               return SpoolResult(False, dropped_b, dropped_r, f"{type(e).__name__}: {e}")
      return SpoolResult(True, dropped_b, dropped_r)

   def _write(self, directory: Path, data: bytes, live_only: bool) -> str:
      """Write a batch atomically under the next name in sequence; returns that name."""
      self._seq += 1
      stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%f")
      name = f"{stamp}-{self._seq:06d}{_LIVE_MARK if live_only else ''}{_SUFFIX}"
      tmp = directory / f".{name}.tmp"
      try:
         fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
         try:
            with os.fdopen(fd, "wb") as f:
               f.write(data)
               f.flush()
               os.fsync(f.fileno())
         except BaseException:
            tmp.unlink(missing_ok=True)
            raise
         os.replace(tmp, directory / name)
      except OSError:
         try:
            tmp.unlink(missing_ok=True)
         except OSError:
            pass
         raise
      return name

   def _corrupt_directory(self) -> Path:
      corrupt = self.directory / "corrupt"
      corrupt.mkdir(mode=0o700, exist_ok=True)
      os.chmod(corrupt, 0o700)
      return corrupt

   def quarantine(self, path: Path) -> bool:
      """Move a batch into `corrupt/`, out of pending, size and count. Never raises.

      For a batch that can never be written; it is kept for inspection, not replayed.
      Returns whether it was moved.
      """
      try:
         path = Path(path)
         os.replace(path, self._corrupt_directory() / path.name)
         return True
      except Exception as e:  # noqa: BLE001 - the collector must survive any spool failure
         log.error("could not quarantine %s: %s", path, e)
         return False

   def quarantine_records(self, records: list[Record], reason: str) -> str | None:
      """Store a batch that was never spooled directly in `corrupt/`. Never raises.

      For a fresh batch the database rejected. It takes the next name in the spool's
      sequence, so moved back into the spool directory it replays in its original place.
      Returns the file name, or None if it could not be stored.
      """
      try:
         payload = "".join(
            json.dumps(encode_record(r), separators=(",", ":")) + "\n" for r in records
         )
         live_only = all(r.kind in LIVE_KINDS for r in records)
         with self._lock:
            name = self._write(self._corrupt_directory(), payload.encode("utf-8"), live_only)
      except Exception as e:  # noqa: BLE001 - the collector must survive any spool failure
         log.error("could not quarantine %d records (%s): %s", len(records), reason, e)
         return None
      log.error("quarantined %d records as corrupt/%s: %s", len(records), name, reason)
      return name

   def take_quarantined(self) -> list[tuple[str, str]]:
      """(file name, reason) of each batch pending() quarantined since the last call."""
      with self._lock:
         taken, self._quarantined = self._quarantined, []
      return taken

   def pending(self) -> Iterator[tuple[Path, list[Record]]]:
      """Yield (path, records) for each stored batch, oldest first.

      An unreadable batch is moved to `corrupt/` and reported by take_quarantined().
      """
      for path in self._batch_files():
         batch = self.read(path)
         if batch is not None:
            yield path, batch

   def read(self, path: Path) -> list[Record] | None:
      """The records of one stored batch, or None if it cannot be read. Never raises.

      An unreadable batch is moved to `corrupt/` and reported by take_quarantined().
      """
      if path in self._stuck:
         return None
      try:
         with open(path, encoding="utf-8") as f:
            return [decode_record(json.loads(line)) for line in f if line.strip()]
      except Exception as e:  # noqa: BLE001 - e.g. RecursionError on hostile JSON
         log.error("quarantining unreadable spool file %s: %s", path.name, e)
         reason = _describe(e)
         if not self.quarantine(path):
            self._stuck.add(path)
            reason += " (could not be moved to corrupt/)"
         with self._lock:
            self._quarantined.append((path.name, reason))
         return None

   def remove(self, path: Path) -> None:
      try:
         Path(path).unlink(missing_ok=True)
      except OSError as e:
         log.error("could not remove spool file %s: %s", path, e)
