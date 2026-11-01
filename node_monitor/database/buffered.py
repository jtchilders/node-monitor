"""Writes record batches to PostgreSQL, spooling them to disk while it is unreachable.

The collector must never lose data to a database outage and must never crash because of
one. During an outage batches go to the spool; once the database answers again the spool
is replayed oldest first, and only then is the new batch written, so rows reach the
database in the order they were produced.
"""

from __future__ import annotations

import logging
import os
from collections import Counter
from datetime import datetime, timedelta

from node_monitor.database.connect import DatabaseUnavailable
from node_monitor.database.spool import Spool
from node_monitor.database.writer import DatabaseWriter, WriteResult
from node_monitor.pipeline.records import COLLECTION_EVENT, Record
from node_monitor.pipeline.timestamps import require_aware

log = logging.getLogger(__name__)

_ERROR_CHARS = 200


def _describe(exc: BaseException) -> str:
   return f"{type(exc).__name__}: {str(exc)[:_ERROR_CHARS]}"


def _exists(path) -> bool:
   try:
      return os.path.lexists(path)
   except Exception:  # noqa: BLE001 - unknown counts as gone; replay will retry it next run
      return False


class BufferedWriter:
   """Writes through `writer`, falling back to `spool` while the database is unavailable.

   `submit` returns collector-level collection_event records describing what happened
   (outage, recovery, spool trouble). They are not written by the call that made them: the
   caller passes them back in its next `submit`, so they follow the same path as any data.

   Not thread-safe and needs no locking: it is driven from a single writer thread. All
   times come from the `now` passed in, never from the clock.
   """

   def __init__(
      self,
      writer: DatabaseWriter,
      spool: Spool,
      *,
      system: str,
      backoff: tuple[float, float] = (5, 300),
      max_replay_attempts: int = 3,
   ):
      self._writer = writer
      self._spool = spool
      self._system = system
      self._backoff = backoff
      self._max_replay_attempts = max_replay_attempts
      # path -> outage-class failures of that spooled batch while the database answered.
      self._replay_failures: dict = {}
      # True once a write has succeeded, until the next outage.
      self._connected = False
      self._outage_since: datetime | None = None
      self._next_retry: datetime | None = None
      self._delay: float | None = None
      # Replayed during the current outage, which may take several attempts to drain.
      self._replayed_batches = 0
      self._replayed_records = 0
      # Events of a submit that raised, returned by the next one so they are not lost.
      self._carried: list[Record] = []
      # Spool files that could be neither removed after replay nor quarantined: skipped
      # for the rest of this process rather than replayed on every submit.
      self._skip: set = set()

   def submit(self, records: list[Record], now: datetime) -> list[Record]:
      """Write `records`, or spool them during an outage; return the events generated.

      Raises only for an error that is not an outage while writing `records` themselves
      (a bug: the batch would fail the same way on every retry). The batch is first kept
      in the spool's `corrupt/` directory, so it is not lost, and a
      spool_batch_quarantined event saying so is returned by the next call. The spool is
      never allowed to raise.
      """
      require_aware(now)
      events, self._carried = self._carried, []
      beyond = Counter()
      try:
         if self._next_retry is not None and now < self._next_retry:
            self._store(records, now, events)
         else:
            self._attempt(records, now, events, beyond)
      except Exception:
         self._carried = self._finish(events, beyond, now)
         raise
      return self._finish(events, beyond, now)

   def status(self) -> dict:
      return {
         "connected": self._connected,
         "outage_since": self._outage_since.isoformat() if self._outage_since else None,
         "spooled_batches": self._spool.batch_count(),
         "spooled_bytes": self._spool.size_bytes(),
         "next_retry": self._next_retry.isoformat() if self._next_retry else None,
      }

   def close(self) -> None:
      """Close the database connection. Never raises."""
      try:
         self._writer.close()
      except Exception as exc:  # noqa: BLE001 - shutting down regardless
         log.warning("closing the database writer failed: %s", _describe(exc))

   # --- writing ------------------------------------------------------------------------

   def _attempt(self, records, now, events, beyond):
      try:
         wrote = self._replay(now, events, beyond)
         # With nothing replayed, an empty write still tells whether the outage is over.
         if records or (self._outage_since is not None and not wrote):
            try:
               result = self._writer.write(records, now)
            except DatabaseUnavailable:
               raise
            except Exception as exc:
               # A successful replay proves the database is back even though this batch
               # is bad; without one the outage state is left for the next call to settle.
               if wrote:
                  self._recover(now, events)
               self._quarantine_fresh(records, exc, now, events)
               raise
            beyond.update(result.rows_beyond_retention)
            wrote = True
      except DatabaseUnavailable as exc:
         self._fail(exc, records, now, events)
         return
      if wrote:
         self._recover(now, events)

   def _replay(self, now, events, beyond) -> bool:
      """Write every spooled batch, oldest first; True if any was written.

      Raises DatabaseUnavailable, leaving the batches not yet written on disk in order.
      """
      wrote = False
      for path, batch in self._spool.pending():
         if path in self._skip:
            continue
         try:
            result: WriteResult = self._writer.write(batch, now)
         except DatabaseUnavailable as exc:
            # An error classed as an outage can be deterministic (out of memory, temp file
            # limit); if the database still answers, this batch alone is the suspect, and
            # it must not block the spool forever.
            if not self._writer.ping():
               raise
            failures = self._replay_failures.get(path, 0) + 1
            if failures < self._max_replay_attempts:
               self._replay_failures[path] = failures
               raise
            log.error("quarantining spool file %s after %d failures", path.name, failures)
            self._quarantine_spooled(
               path,
               now,
               events,
               {
                  "error": _describe(exc),
                  "reason": "repeated failure while the database answered",
                  "attempts": failures,
               },
            )
            continue
         except Exception as exc:  # noqa: BLE001 - one bad batch must not block the rest
            log.error("quarantining spool file %s: %s", path.name, _describe(exc))
            self._quarantine_spooled(path, now, events, {"error": _describe(exc)})
            continue
         self._replay_failures.pop(path, None)
         self._spool.remove(path)
         if _exists(path):
            self._skip.add(path)
         wrote = True
         self._replayed_batches += 1
         self._replayed_records += len(batch)
         beyond.update(result.rows_beyond_retention)
      return wrote

   def _quarantine_spooled(self, path, now, events, detail):
      self._replay_failures.pop(path, None)
      self._spool.quarantine(path)
      if _exists(path):
         self._skip.add(path)
      events.append(self._event(now, "spool_batch_quarantined", {"file": path.name, **detail}))

   def _quarantine_fresh(self, records, exc, now, events):
      if not records:
         return
      name = self._spool.quarantine_records(records, _describe(exc))
      events.append(
         self._event(now, "spool_batch_quarantined", {"file": name, "error": _describe(exc)})
      )

   def _store(self, records, now, events):
      if not records:
         return
      result = self._spool.append(records)
      if result.dropped_batches:
         events.append(
            self._event(
               now,
               "spool_dropped",
               {"batches": result.dropped_batches, "records": result.dropped_records},
            )
         )
      if not result.stored:
         log.error("could not spool %d records: %s", len(records), result.error)
         events.append(self._event(now, "spool_write_failed", {"error": result.error}))

   # --- outage state -------------------------------------------------------------------

   def _fail(self, exc, records, now, events):
      self._connected = False
      if self._outage_since is None:
         log.warning("database unavailable, spooling: %s", exc)
         self._outage_since = now
         self._delay = min(self._backoff)
         events.append(self._event(now, "database_unavailable", {"error": _describe(exc)}))
      else:
         self._delay = min(self._delay * 2, self._backoff[1])
      self._next_retry = now + timedelta(seconds=self._delay)
      self._store(records, now, events)

   def _recover(self, now, events):
      if self._outage_since is not None or self._replayed_batches:
         # A spool left by an earlier process means an outage of unknown length.
         since = self._outage_since
         outage_sec = (now - since).total_seconds() if since is not None else None
         log.info("database recovered; replayed %d spooled batches", self._replayed_batches)
         events.append(
            self._event(
               now,
               "database_recovered",
               {
                  "outage_sec": outage_sec,
                  "replayed_batches": self._replayed_batches,
                  "replayed_records": self._replayed_records,
               },
            )
         )
      self._connected = True
      self._outage_since = None
      self._next_retry = None
      self._delay = None
      self._replayed_batches = 0
      self._replayed_records = 0

   # --- events -------------------------------------------------------------------------

   def _event(self, now, event_type, detail):
      return Record(
         COLLECTION_EVENT,
         {
            "system": self._system,
            "node": None,
            "ts": now,
            "event_type": event_type,
            "detail": detail,
         },
      )

   def _finish(self, events, beyond, now):
      """`events` plus those for spool files found unreadable and the retention drops."""
      for name, reason in self._spool.take_quarantined():
         events.append(self._event(now, "spool_batch_quarantined", {"file": name, "error": reason}))
      return events + self._beyond_event(beyond, now)

   def _beyond_event(self, beyond, now):
      if not beyond:
         return []
      return [self._event(now, "rows_beyond_retention", dict(beyond))]
