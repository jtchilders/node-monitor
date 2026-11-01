"""The collector's periodic housekeeping: heartbeat, partition creation and retention.

Driven from the writer thread, so it owns its own autocommit connection and never shares
the writer's. Time comes only from the `now` passed in. Nothing here may raise: a database
outage must not take the collector down, and `BufferedWriter` already reports outages.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime, timedelta

from node_monitor.config import RetentionConfig
from node_monitor.database import maintenance
from node_monitor.database.connect import connect
from node_monitor.pipeline.records import COLLECTION_EVENT, Record

log = logging.getLogger(__name__)

_ERROR_CHARS = 200


def _describe(exc: BaseException) -> str:
   return f"{type(exc).__name__}: {str(exc)[:_ERROR_CHARS]}"


class Upkeep:
   def __init__(
      self,
      url: str,
      *,
      system: str,
      retention: RetentionConfig,
      connect_timeout_sec: float,
      collector_host: str,
      version: str,
      started_at: datetime,
      heartbeat_interval_sec: float = 60,
      maintenance_interval_sec: float = 86400,
      retry_interval_sec: float = 3600,
   ):
      self._url = url
      self._system = system
      self._retention = retention
      self._connect_timeout_sec = connect_timeout_sec
      self._collector_host = collector_host
      self._version = version
      self._started_at = started_at
      self._heartbeat_interval = timedelta(seconds=heartbeat_interval_sec)
      self._maintenance_interval = timedelta(seconds=maintenance_interval_sec)
      self._retry_interval = timedelta(seconds=retry_interval_sec)
      self._conn = None
      # None means due at the next tick.
      self._last_heartbeat_attempt: datetime | None = None
      self._next_maintenance: datetime | None = None
      # True while heartbeats are failing, so an outage is logged once, not per attempt.
      self._heartbeat_failing = False

   def tick(self, now: datetime, status: Callable[[], dict]) -> list[Record]:
      """Do whatever is due; return collector-level events to be written. Never raises.

      `status` builds the heartbeat's status; it is called only when a heartbeat is due,
      since building it can be costly (it lists the spool) and ticks come every round.
      """
      try:
         return self._tick(now, status)
      except Exception:  # noqa: BLE001 - housekeeping must never stop the writer
         log.exception("upkeep failed")
         self._drop_connection()
         return []

   def close(self, now: datetime, status: dict) -> None:
      """Write a last heartbeat marked stopped, then close. Never raises."""
      status = {**status, "state": "stopped"}
      # Idle since the last heartbeat, an open connection may have died with a database
      # restart; that deserves one retry on a fresh one. No connection means the database
      # was already failing, and a second attempt would only delay shutdown.
      attempts = 2 if self._conn is not None else 1
      for attempt in range(attempts):
         try:
            self._heartbeat(now, status)
            break
         except Exception as exc:  # noqa: BLE001
            self._drop_connection()
            if attempt == attempts - 1:
               log.warning("final heartbeat failed: %s", _describe(exc))
      self._drop_connection()

   # --- work ---------------------------------------------------------------------------

   def _tick(self, now: datetime, status: Callable[[], dict]) -> list[Record]:
      heartbeat_due = (
         self._last_heartbeat_attempt is None
         or now - self._last_heartbeat_attempt >= self._heartbeat_interval
      )
      maintenance_due = self._next_maintenance is None or now >= self._next_maintenance
      events: list[Record] = []
      if heartbeat_due:
         self._last_heartbeat_attempt = now
         current = status()
         try:
            self._heartbeat(now, current)
         except Exception as exc:  # noqa: BLE001
            self._drop_connection()
            self._heartbeat_failed(exc)
            # The database is unreachable or unwell; maintenance would fail the same way.
            if maintenance_due:
               self._next_maintenance = now + self._heartbeat_interval
            return events
         self._heartbeat_ok()
      if maintenance_due:
         events.extend(self._maintain(now))
      return events

   def _heartbeat(self, now: datetime, status: dict) -> None:
      maintenance.write_heartbeat(
         self._connection(),
         system=self._system,
         collector_host=self._collector_host,
         version=self._version,
         started_at=self._started_at,
         now=now,
         status=status,
      )

   def _maintain(self, now: datetime) -> list[Record]:
      try:
         result = maintenance.run(self._connection(), now, self._retention)
      except Exception as exc:  # noqa: BLE001
         log.exception("partition maintenance failed")
         self._drop_connection()
         self._next_maintenance = now + self._retry_interval
         return [self._event(now, "maintenance_failed", {"error": _describe(exc)})]
      clean = not result["skipped"]
      self._next_maintenance = now + (self._maintenance_interval if clean else self._retry_interval)
      if not any(result[key] for key in ("created", "dropped", "skipped")):
         return []
      detail = {key: result[key] for key in ("created", "dropped", "skipped")}
      return [self._event(now, "partitions_maintained", detail)]

   # --- state --------------------------------------------------------------------------

   def _heartbeat_failed(self, exc: Exception) -> None:
      if not self._heartbeat_failing:
         log.warning("heartbeat failed: %s", _describe(exc))
      self._heartbeat_failing = True

   def _heartbeat_ok(self) -> None:
      if self._heartbeat_failing:
         log.info("heartbeat succeeded again")
      self._heartbeat_failing = False

   def _connection(self):
      if self._conn is None or self._conn.closed:
         self._conn = connect(
            self._url, connect_timeout_sec=self._connect_timeout_sec, autocommit=True
         )
      return self._conn

   def _drop_connection(self) -> None:
      conn, self._conn = self._conn, None
      if conn is not None:
         try:
            conn.close()
         except Exception:  # noqa: BLE001 - already discarding it
            pass

   def _event(self, now: datetime, event_type: str, detail: dict) -> Record:
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
