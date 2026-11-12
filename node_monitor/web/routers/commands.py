"""Command search API over command_lines."""

from __future__ import annotations

import re

import psycopg
from fastapi import APIRouter, HTTPException, Request
from psycopg.rows import dict_row

from node_monitor.database.connect import DatabaseUnavailable
from node_monitor.web.deps import web_reader

router = APIRouter(prefix="/api/commands", tags=["commands"])


def _escape_like(s: str) -> str:
   return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


@router.get("/search")
def search(request: Request):
   q = request.query_params
   raw_q = q.get("q")
   raw_regex = q.get("regex")
   user = q.get("user")
   activity = q.get("activity")
   tag = q.get("tag")

   # Validate limit/offset
   try:
      limit = int(q.get("limit", "50"))
   except ValueError as exc:
      raise HTTPException(status_code=400, detail="invalid limit") from exc
   try:
      offset = int(q.get("offset", "0"))
   except ValueError as exc:
      raise HTTPException(status_code=400, detail="invalid offset") from exc
   if limit < 1 or limit > 100:
      raise HTTPException(status_code=400, detail="limit must be 1..100")
   if offset < 0:
      raise HTTPException(status_code=400, detail="offset must be >=0")

   # Validate regex syntax early (Python re) to give 400 before hitting DB
   if raw_regex is not None:
      try:
         re.compile(raw_regex)
      except re.error as exc:
         raise HTTPException(status_code=400, detail="invalid regex: " + str(exc)) from exc

   cfg = request.app.state.config

   # Build WHERE clauses with params
   conditions: list[str] = ["system=%s"]
   params: list[object] = [cfg.system]

   if raw_q is not None and raw_q != "":
      escaped = _escape_like(raw_q)
      conditions.append("cmdline ILIKE %s ESCAPE '\\'")
      params.append("%" + escaped + "%")

   if raw_regex is not None and raw_regex != "":
      conditions.append("cmdline ~ %s")
      params.append(raw_regex)

   if user:
      conditions.append("username=%s")
      params.append(user)

   if activity:
      conditions.append("activity=%s")
      params.append(activity)

   if tag:
      conditions.append("%s = ANY(tags)")
      params.append(tag)

   where_sql = " AND ".join(conditions)

   try:
      with web_reader(cfg) as conn:
         with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
               "SELECT count(*)::bigint AS total FROM command_lines WHERE " + where_sql,
               tuple(params),
            )
            total_row = cur.fetchone()
            total = int(total_row["total"]) if total_row else 0

            cur.execute(
               "SELECT cmdline, username, activity, category, tags,"
               " first_seen, last_seen, nodes, sample_count"
               " FROM command_lines"
               " WHERE " + where_sql + " ORDER BY last_seen DESC, cmdline"
               " LIMIT %s OFFSET %s",
               tuple(params) + (limit, offset),
            )
            rows = cur.fetchall()
   except HTTPException:
      raise
   except (psycopg.Error, DatabaseUnavailable, OSError) as exc:
      raise HTTPException(status_code=503, detail="database unavailable") from exc

   items = []
   for r in rows:
      items.append(
         {
            "cmdline": r["cmdline"],
            "username": r["username"],
            "activity": r["activity"],
            "category": r["category"],
            "tags": r["tags"] if r["tags"] is not None else [],
            "first_seen": r["first_seen"].isoformat() if r["first_seen"] else None,
            "last_seen": r["last_seen"].isoformat() if r["last_seen"] else None,
            "nodes": r["nodes"] if r["nodes"] is not None else [],
            "sample_count": int(r["sample_count"]) if r["sample_count"] is not None else 0,
         }
      )

   return {"items": items, "total": total}
