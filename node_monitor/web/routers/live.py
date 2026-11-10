"""Live JSON APIs: processes, nodes, users, events."""

from datetime import datetime, timezone

import psycopg
from fastapi import APIRouter, HTTPException, Query, Request

from node_monitor.database.connect import DatabaseUnavailable
from node_monitor.web.deps import web_reader

router = APIRouter(prefix="/api/live", tags=["live"])

_STALE_AFTER_SEC = 120

_ALLOWED_PROCESS_SORTS = ["cpu", "gpu", "idle", "rss", "threads"]
_ALLOWED_USER_SORTS = ["cpu", "gpu", "idle", "procs", "rss"]


def _handle_sort_error(exc: ValueError) -> None:
   if "unknown sort" in str(exc):
      raise HTTPException(status_code=400, detail=str(exc)) from exc
   raise exc


def _serialize_row(row: dict) -> dict:
   out: dict = {}
   for k, v in row.items():
      if isinstance(v, datetime):
         out[k] = v.isoformat()
      else:
         out[k] = v
   return out


def _total_and_slice(
   conn, system: str, fetch_all, *, limit: int | None, offset: int | None, **kw
) -> tuple[list[dict], int]:
   """Total before offset, window is [offset:offset+limit]. fetch_all(conn, system, **kw)."""
   all_rows = fetch_all(conn, system, **kw)
   total = len(all_rows)
   start = offset or 0
   items = all_rows[start:]
   if limit is not None:
      items = items[:limit]
   return items, total


@router.get("/processes")
def live_processes(
   request: Request,
   node: str | None = None,
   user: str | None = None,
   category: str | None = None,
   sort: str = Query(default="cpu"),
   limit: int | None = Query(default=None, ge=0),
   offset: int | None = Query(default=None, ge=0),
   include_stale: bool = False,
):
   cfg = request.app.state.config
   try:
      from node_monitor.database import queries
   except ImportError as exc:
      raise HTTPException(status_code=500, detail=str(exc)) from exc

   max_age = None if include_stale else _STALE_AFTER_SEC
   try:
      _order_check = queries._order  # noqa: SLF001 — validate before opening DB
      _order_check(queries._PROCESS_SORTS, sort, False)
   except ValueError as exc:
      _handle_sort_error(exc)

   try:
      with web_reader(cfg) as conn:
         items, total = _total_and_slice(
            conn,
            cfg.system,
            queries.processes,
            limit=limit,
            offset=offset,
            user=user,
            node=node,
            category=category,
            sort=sort,
            max_age_sec=max_age,
         )
   except (psycopg.Error, DatabaseUnavailable, OSError) as exc:
      raise HTTPException(status_code=503, detail="database unavailable") from exc

   return {"items": [_serialize_row(r) for r in items], "total": total}


@router.get("/nodes")
def live_nodes(request: Request):
   cfg = request.app.state.config
   try:
      with web_reader(cfg) as conn:
         from node_monitor.database import queries

         hb = queries.heartbeat(conn, cfg.system)
         rows = queries.node_states(conn, cfg.system)
   except (psycopg.Error, DatabaseUnavailable, OSError) as exc:
      raise HTTPException(status_code=503, detail="database unavailable") from exc

   now = datetime.now(timezone.utc)
   nodes = []
   for row in rows:
      raw = _serialize_row(row)
      updated_age_s = int((now - row["updated_at"]).total_seconds())
      updated_age_s = max(0, updated_age_s)
      if updated_age_s > _STALE_AFTER_SEC:
         state = "stale"
      else:
         state = row["session_state"] or "unknown"
      raw["state"] = state
      raw["updated_age_s"] = updated_age_s
      nodes.append(raw)

   heartbeat_age_s = None
   heartbeat_ok = False
   if hb is not None:
      heartbeat_age_s = int((now - hb["last_seen"]).total_seconds())
      heartbeat_age_s = max(0, heartbeat_age_s)
      stopped = (hb.get("status") or {}).get("state") == "stopped"
      heartbeat_ok = not stopped and heartbeat_age_s < 180

   return {
      "items": nodes,
      "total": len(nodes),
      "heartbeat": _serialize_row(hb) if hb else None,
      "heartbeat_age_s": heartbeat_age_s,
      "heartbeat_ok": heartbeat_ok,
   }


@router.get("/users")
def live_users(
   request: Request,
   sort: str = Query(default="rss"),
   limit: int | None = Query(default=None, ge=0),
   offset: int | None = Query(default=None, ge=0),
):
   cfg = request.app.state.config
   try:
      from node_monitor.database import queries

      queries._order(queries._USER_SORTS, sort, False)  # noqa: SLF001
   except ValueError as exc:
      _handle_sort_error(exc)

   try:
      with web_reader(cfg) as conn:
         from node_monitor.database import queries as q

         items, total = _total_and_slice(
            conn, cfg.system, q.users, limit=limit, offset=offset, sort=sort
         )
   except (psycopg.Error, DatabaseUnavailable, OSError) as exc:
      raise HTTPException(status_code=503, detail="database unavailable") from exc

   return {"items": [_serialize_row(r) for r in items], "total": total}


@router.get("/events")
def live_events(
   request: Request,
   limit: int | None = Query(default=50, ge=0),
   offset: int | None = Query(default=None, ge=0),
):
   cfg = request.app.state.config
   try:
      with web_reader(cfg) as conn:
         from node_monitor.database import queries

         items, total = _total_and_slice(
            conn, cfg.system, queries.events, limit=limit, offset=offset
         )
   except (psycopg.Error, DatabaseUnavailable, OSError) as exc:
      raise HTTPException(status_code=503, detail="database unavailable") from exc

   return {"items": [_serialize_row(r) for r in items], "total": total}
