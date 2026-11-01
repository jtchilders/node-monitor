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
_NAME_RE = re.compile(r"^\d{8}T\d{6}\.\d{6}-(\d{6})\.jsonl$")
_LIVE_KINDS = frozenset({LIVE_PROCESSES, LIVE_NODE})


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
      self._init_error: str | None = None
      try:
         self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
         # mkdir's mode is masked by umask and ignored for an existing directory.
         os.chmod(self.directory, 0o700)
      except OSError as e:
         self._init_error = str(e)
         log.error("spool directory %s unusable: %s", self.directory, e)
      # Continue past any existing file so a restart never reuses a name.
      for p in self._batch_files():
         m = _NAME_RE.match(p.name)
         if m:
            self._seq = max(self._seq, int(m.group(1)))

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

   def _read_kinds(self, path: Path) -> list[str] | None:
      try:
         with open(path, encoding="utf-8") as f:
            return [json.loads(line)["kind"] for line in f if line.strip()]
      except (OSError, ValueError, KeyError, TypeError):
         return None

   def _drop(self, path: Path) -> int | None:
      """Delete a batch and return its record count, or None if it could not be deleted."""
      kinds = self._read_kinds(path)
      try:
         path.unlink()
      except OSError:
         return None
      return len(kinds) if kinds is not None else 0

   def _make_room(self, needed: int) -> tuple[int, int]:
      """Delete batches until `needed` more bytes fit; superseded live-only batches go first."""
      files = self._batch_files()
      sizes = {p: self._size(p) for p in files}
      total = sum(sizes.values())
      batches = records = 0
      live_only = []
      for p in files:
         kinds = self._read_kinds(p)
         if kinds and all(k in _LIVE_KINDS for k in kinds):
            live_only.append(p)
      live_set = set(live_only)
      for p in live_only + [p for p in files if p not in live_set]:
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
      if self._init_error:
         return SpoolResult(False, error=self._init_error)
      if not records:
         return SpoolResult(True)
      payload = "".join(json.dumps(encode_record(r), separators=(",", ":")) + "\n" for r in records)
      data = payload.encode("utf-8")
      if len(data) > self.max_bytes:
         return SpoolResult(
            False, error=f"batch of {len(data)} bytes exceeds spool max_bytes {self.max_bytes}"
         )
      with self._lock:
         dropped_b, dropped_r = self._make_room(len(data))
         self._seq += 1
         stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%f")
         name = f"{stamp}-{self._seq:06d}{_SUFFIX}"
         tmp = self.directory / f".{name}.tmp"
         final = self.directory / name
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
            os.replace(tmp, final)
         except OSError as e:
            try:
               tmp.unlink(missing_ok=True)
            except OSError:
               pass
            return SpoolResult(False, dropped_b, dropped_r, f"{type(e).__name__}: {e}")
      return SpoolResult(True, dropped_b, dropped_r)

   def _quarantine(self, path: Path) -> None:
      corrupt = self.directory / "corrupt"
      try:
         corrupt.mkdir(mode=0o700, exist_ok=True)
         os.chmod(corrupt, 0o700)
         os.replace(path, corrupt / path.name)
      except OSError as e:
         log.error("could not quarantine %s: %s", path, e)

   def pending(self) -> Iterator[tuple[Path, list[Record]]]:
      """Yield (path, records) for each stored batch, oldest first."""
      for path in self._batch_files():
         try:
            with open(path, encoding="utf-8") as f:
               batch = [decode_record(json.loads(line)) for line in f if line.strip()]
         except (OSError, ValueError, KeyError, TypeError) as e:
            log.error("quarantining unreadable spool file %s: %s", path.name, e)
            self._quarantine(path)
            continue
         yield path, batch

   def remove(self, path: Path) -> None:
      try:
         Path(path).unlink(missing_ok=True)
      except OSError as e:
         log.error("could not remove spool file %s: %s", path, e)
