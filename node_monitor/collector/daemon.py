"""The collector daemon: probe sessions feed the per-node pipelines, whose records go
through the record queue to the single writer thread.

The event loop runs the sessions and the pipelines; every database call happens on the
writer thread (the schema check at startup aside), so a slow or absent database never
stalls collection. A database outage is not fatal at any point: the writer spools until
it recovers.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
from collections.abc import Callable
from datetime import datetime, timezone

from node_monitor import version
from node_monitor.collector.fleet import build_sessions, run_sessions
from node_monitor.collector.record_queue import RecordQueue
from node_monitor.collector.session import NodeSession
from node_monitor.collector.upkeep import Upkeep
from node_monitor.collector.writer_thread import WriterThread
from node_monitor.config import Config, ConfigError
from node_monitor.database.buffered import BufferedWriter
from node_monitor.database.connect import connect, is_unavailable
from node_monitor.database.migrate import SchemaOutOfDate, check_schema
from node_monitor.database.spool import Spool
from node_monitor.database.writer import DatabaseWriter
from node_monitor.pipeline.node_pipeline import NodePipeline
from node_monitor.pipeline.records import COLLECTION_EVENT, Record

log = logging.getLogger(__name__)

# The writer's last rounds may be replaying a spool; past this the process stops waiting.
WRITER_JOIN_TIMEOUT_SEC = 60
_ERROR_CHARS = 200


def _now() -> datetime:
   return datetime.now(timezone.utc)


class Daemon:
   """Everything is built here, outside the event loop; `run` drives it inside one.

   `record_tap`, if given, sees every per-node record the pipelines produce, before it is
   queued. Collector-level events (startup, shutdown, outages) do not pass through it.
   """

   def __init__(
      self,
      config: Config,
      *,
      ssh_binary: str = "ssh",
      probe_overrides: dict | None = None,
      record_tap: Callable[[Record], None] | None = None,
      db_backoff: tuple[float, float] = (5, 300),
      flush_interval_sec: float = 5.0,
   ):
      if not config.database.url:
         raise ConfigError("database.url is not set")
      self._config = config
      self._record_tap = record_tap
      self._nodes = {node.label: node for node in config.nodes}
      self._pipelines = {
         node.label: NodePipeline(
            system=config.system,
            node=node,
            collection=config.collection,
            classification=config.classification,
            redaction=config.redaction,
         )
         for node in config.nodes
      }
      # Read by the writer thread for the heartbeat; only values change after this.
      self._session_states = {node.label: "starting" for node in config.nodes}
      self._queue = RecordQueue()
      db = config.database
      self.database_writer = DatabaseWriter(
         db.url, retention=config.retention, connect_timeout_sec=db.connect_timeout_sec
      )
      spool = Spool(config.working_dir / "state" / "spool", max_bytes=config.spool.max_bytes)
      buffered = BufferedWriter(
         self.database_writer, spool, system=config.system, backoff=db_backoff
      )
      upkeep = Upkeep(
         db.url,
         system=config.system,
         retention=config.retention,
         connect_timeout_sec=db.connect_timeout_sec,
         collector_host=socket.getfqdn(),
         version=version.describe(),
         started_at=_now(),
      )
      self._writer_thread = WriterThread(
         self._queue,
         buffered,
         upkeep,
         status_fn=self._status,
         clock=_now,
         flush_interval_sec=flush_interval_sec,
      )
      self.sessions: dict[str, NodeSession] = build_sessions(
         config,
         on_message=self._on_message,
         on_event=self._on_event,
         ssh_binary=ssh_binary,
         probe_overrides=probe_overrides,
      )
      # Created in run(): an asyncio.Event made outside the loop may bind to the wrong one.
      self._stop: asyncio.Event | None = None
      self._stop_requested = False

   def request_stop(self) -> None:
      """Ask `run` to shut down. Call from the event loop's thread."""
      self._stop_requested = True
      if self._stop is not None:
         self._stop.set()

   async def run(self, duration_sec: float | None = None) -> int:
      """Collect until `duration_sec` elapses, request_stop(), or SIGINT/SIGTERM. Call once.

      Returns 0 after a clean shutdown, 2 if the database schema is out of date.
      """
      self._stop = asyncio.Event()
      if self._stop_requested:
         self._stop.set()
      if not await asyncio.to_thread(self._schema_ok):
         return 2
      self._writer_thread.start()
      try:
         detail = {"version": version.describe(), "host": socket.getfqdn(), "pid": os.getpid()}
         self._queue.put([self._collector_event("collector_started", detail)])
         # Returns only once every session has stopped, so no callback can still put.
         await run_sessions(
            self.sessions, duration_sec=duration_sec, stop=self._stop, on_crash=self._on_crash
         )
      finally:
         self._finish_queue()
         await self._join_writer()
      return 0

   # --- startup and shutdown -----------------------------------------------------------

   def _schema_ok(self) -> bool:
      """False only for a schema known to be out of date. Never raises."""
      db = self._config.database
      try:
         with connect(db.url, connect_timeout_sec=db.connect_timeout_sec) as conn:
            check_schema(conn)
      except SchemaOutOfDate as exc:
         log.error("%s; run `node-monitor database migrate` before starting the collector", exc)
         return False
      except Exception as exc:  # noqa: BLE001 - the collector never exits on database errors
         if is_unavailable(exc):
            log.warning("database unavailable at startup, spooling until it returns: %s", exc)
         else:
            log.warning("could not check the database schema, starting anyway: %s", exc)
      return True

   def _finish_queue(self) -> None:
      records: list[Record] = []
      for label, pipeline in self._pipelines.items():
         try:
            records += self._tapped(pipeline.flush())
         except Exception as exc:  # noqa: BLE001 - the other nodes' partial data still counts
            log.exception("flushing the pipeline for %s failed", label)
            records += self._tapped([self._pipeline_error(label, exc, _now())])
      records.append(self._collector_event("collector_stopped", {}))
      try:
         self._queue.put(records)
      finally:
         self._queue.close()

   async def _join_writer(self) -> None:
      await asyncio.to_thread(self._writer_thread.join, WRITER_JOIN_TIMEOUT_SEC)
      if self._writer_thread.is_alive():
         log.error(
            "writer thread still running after %d s; the process exits once it finishes",
            WRITER_JOIN_TIMEOUT_SEC,
         )

   # --- session callbacks (event loop thread) ------------------------------------------

   def _on_message(self, label: str, msg: dict) -> None:
      recv = _now()
      self._deliver(label, recv, lambda: self._pipelines[label].on_message(msg, recv))

   def _on_event(self, label: str, kind: str, detail: dict) -> None:
      recv = _now()
      if kind == "session_started":
         self._session_states[label] = "up"
      elif kind == "session_lost":
         self._session_states[label] = "down"
      self._deliver(label, recv, lambda: self._pipelines[label].on_event(kind, detail, recv))

   def _on_crash(self, label: str, exc: BaseException) -> None:
      log.error("session for %s ended with %r", label, exc)
      detail = {"type": type(exc).__name__, "error": str(exc)[:_ERROR_CHARS]}
      self._on_event(label, "session_crashed", detail)
      self._session_states[label] = "down"

   def _deliver(self, label: str, recv: datetime, produce: Callable[[], list[Record]]) -> None:
      # A pipeline bug must cost one message, never the node's session.
      try:
         records = produce()
      except Exception as exc:  # noqa: BLE001
         log.exception("pipeline for %s failed", label)
         records = [self._pipeline_error(label, exc, recv)]
      self._queue.put(self._tapped(records))

   def _tapped(self, records: list[Record]) -> list[Record]:
      if self._record_tap is not None:
         for record in records:
            self._record_tap(record)
      return records

   # --- records and status -------------------------------------------------------------

   def _pipeline_error(self, label: str, exc: BaseException, recv: datetime) -> Record:
      return Record(
         COLLECTION_EVENT,
         {
            "system": self._config.system,
            "node": self._nodes[label].host,
            "ts": recv,
            "event_type": "pipeline_error",
            "detail": {"type": type(exc).__name__, "error": str(exc)[:_ERROR_CHARS]},
         },
      )

   def _collector_event(self, event_type: str, detail: dict) -> Record:
      return Record(
         COLLECTION_EVENT,
         {
            "system": self._config.system,
            "node": None,
            "ts": _now(),
            "event_type": event_type,
            "detail": detail,
         },
      )

   def _status(self) -> dict:
      return {
         "state": "running",
         "sessions": dict(self._session_states),
         **self._writer_thread.status(),
      }
