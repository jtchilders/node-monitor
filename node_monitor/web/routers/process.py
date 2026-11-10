"""Single live process lookup."""

from datetime import datetime

import psycopg
from fastapi import APIRouter, HTTPException, Request

from node_monitor.database.connect import DatabaseUnavailable
from node_monitor.web.deps import web_reader

router = APIRouter(prefix="/api/process", tags=["process"])


def _serialize_row(row: dict) -> dict:
   out: dict = {}
   for k, v in row.items():
      if isinstance(v, datetime):
         out[k] = v.isoformat()
      else:
         out[k] = v
   return out


@router.get("/{system}/{node}/{pid}/{start_time}")
def get_process(system: str, node: str, pid: int, start_time: int, request: Request):
   cfg = request.app.state.config
   try:
      with web_reader(cfg) as conn:
         from node_monitor.database import queries

         row = queries.process_by_key(conn, system, node, pid, start_time)
         if row is None:
            raise HTTPException(
               status_code=404,
               detail=f"process {system}/{node}/{pid}/{start_time} not found",
            )
         # Build parent chain: follow ppid links on same node.
         try:
            chain = queries.parent_chain(conn, system, row["node"], row.get("ppid"))
            # Also include direct parent fields as fallback if chain empty.
         except Exception:
            chain = []
         data = _serialize_row(row)
         data["parent_chain"] = [_serialize_row(c) for c in chain]
         return data
   except HTTPException:
      raise
   except (psycopg.Error, DatabaseUnavailable, OSError) as exc:
      raise HTTPException(status_code=503, detail="database unavailable") from exc
