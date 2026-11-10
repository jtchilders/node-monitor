"""History roll-up APIs."""

from datetime import datetime, timezone

import psycopg
from fastapi import APIRouter, HTTPException, Request

from node_monitor.database.connect import DatabaseUnavailable
from node_monitor.web.deps import web_reader

router = APIRouter(prefix="/api/history", tags=["history"])


def _parse_ts(s: str) -> datetime:
   try:
      dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
      if dt.tzinfo is None:
         dt = dt.replace(tzinfo=timezone.utc)
      return dt.astimezone(timezone.utc)
   except Exception as exc:
      raise HTTPException(status_code=400, detail=f"invalid timestamp {s!r}") from exc


@router.get("/node/{node}")
def node_history(
   node: str,
   request: Request,
   from_: str | None = None,
   to: str | None = None,
   bucket: str | None = None,
):
   # alias query params `from`/`to` (FastAPI uses from_ due to keyword)
   # but we expect ?from=&to= — need to read raw query
   # FastAPI maps `from_` from ?from via alias if we declare; instead read manually
   # This endpoint is called with alias handling in wrapper below; keep simple
   return _node_impl(node, request, from_, to, bucket)


def _node_impl(node, request, from_s, to_s, bucket):
   q = request.query_params
   from_s = q.get("from", from_s)
   to_s = q.get("to", to_s)
   bucket = q.get("bucket", bucket)
   if not from_s or not to_s:
      raise HTTPException(status_code=400, detail="from and to required")
   from_ts = _parse_ts(from_s)
   to_ts = _parse_ts(to_s)
   if from_ts >= to_ts:
      raise HTTPException(status_code=400, detail="from must be before to")
   span = (to_ts - from_ts).total_seconds()
   if bucket is None:
      if span <= 24 * 3600:
         bucket = "1m"
      elif span <= 30 * 24 * 3600:
         bucket = "1h"
      else:
         bucket = "1d"
   if bucket not in ("1m", "1h", "1d"):
      raise HTTPException(status_code=400, detail=f"unknown bucket {bucket!r}")
   if bucket == "1m" and span > 7 * 24 * 3600:
      raise HTTPException(status_code=400, detail="raw 1m range >7d not allowed; use 1h or 1d")
   cfg = request.app.state.config
   try:
      from node_monitor.database import queries

      with web_reader(cfg) as conn:
         items = queries.node_history(conn, cfg.system, node, from_ts, to_ts, bucket)
   except (psycopg.Error, DatabaseUnavailable, OSError) as exc:
      raise HTTPException(status_code=503, detail="database unavailable") from exc
   out = []
   for r in items:
      d = dict(r)
      for k, v in list(d.items()):
         if isinstance(v, datetime):
            d[k] = v.isoformat()
      out.append(d)
   return {"items": out, "total": len(out)}


@router.get("/user/{username}")
def user_history(username: str, request: Request):
   q = request.query_params
   from_s = q.get("from")
   to_s = q.get("to")
   bucket = q.get("bucket")
   if not from_s or not to_s:
      raise HTTPException(status_code=400, detail="from and to required")
   from_ts = _parse_ts(from_s)
   to_ts = _parse_ts(to_s)
   if from_ts >= to_ts:
      raise HTTPException(status_code=400, detail="from must be before to")
   span = (to_ts - from_ts).total_seconds()
   if bucket is None:
      if span <= 24 * 3600:
         bucket = "5m"
      elif span <= 30 * 24 * 3600:
         bucket = "1h"
      else:
         bucket = "1d"
   if bucket not in ("5m", "1h", "1d"):
      raise HTTPException(status_code=400, detail=f"unknown bucket {bucket!r}")
   if bucket == "5m" and span > 7 * 24 * 3600:
      raise HTTPException(status_code=400, detail="raw 5m range >7d not allowed")
   cfg = request.app.state.config
   try:
      from node_monitor.database import queries

      with web_reader(cfg) as conn:
         # 404 if user never seen
         if hasattr(conn, "execute"):
            conn.execute(
               "SELECT 1 FROM user_usage_5m WHERE system=%s AND username=%s LIMIT 1",
               (cfg.system, username),
            ).fetchone()
         # Use queries helper via raw check
         import psycopg
         from psycopg.rows import dict_row

         with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
               "SELECT 1 FROM user_usage_5m WHERE system=%s AND username=%s LIMIT 1",
               (cfg.system, username),
            )
            if cur.fetchone() is None:
               # also check live
               cur.execute(
                  "SELECT 1 FROM live_processes WHERE system=%s AND username=%s LIMIT 1",
                  (cfg.system, username),
               )
               if cur.fetchone() is None:
                  raise HTTPException(status_code=404, detail=f"user {username!r} not found")
         items = queries.user_history(conn, cfg.system, username, from_ts, to_ts, bucket)
   except HTTPException:
      raise
   except (psycopg.Error, DatabaseUnavailable, OSError) as exc:
      raise HTTPException(status_code=503, detail="database unavailable") from exc
   out = []
   for r in items:
      d = dict(r)
      for k, v in list(d.items()):
         if isinstance(v, datetime):
            d[k] = v.isoformat()
      out.append(d)
   return {"items": out, "total": len(out)}
