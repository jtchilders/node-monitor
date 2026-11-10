"""FastAPI application factory."""

import json
from datetime import datetime, timezone
from pathlib import Path

import jinja2
import psycopg
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from node_monitor.config import Config, find_config_path, load_config
from node_monitor.database.connect import DatabaseUnavailable

_HEARTBEAT_STALE_SEC = 180
_NODE_STALE_SEC = 120

_templates = jinja2.Environment(
   loader=jinja2.FileSystemLoader(str(Path(__file__).parent / "templates")),
   autoescape=jinja2.select_autoescape(["html", "xml"]),
)


def create_app(config: Config | None = None) -> FastAPI:
   if config is None:
      path = find_config_path()
      config = load_config(str(path))

   app = FastAPI(title="node-monitor")
   app.state.config = config

   static_dir = Path(__file__).parent / "static"
   app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

   from node_monitor.web.routers.live import router as live_router

   app.include_router(live_router)

   from node_monitor.web.routers.process import router as process_router

   app.include_router(process_router)

   from node_monitor.web.routers.history import router as history_router

   app.include_router(history_router)

   @app.get("/api/health")
   def health():
      cfg: Config = app.state.config
      now = datetime.now(timezone.utc)
      url = cfg.web.database_url
      if not url:
         return {
            "ok": False,
            "heartbeat_age_s": None,
            "nodes": [],
            "db_ok": False,
         }
      try:
         from node_monitor.database import queries
         from node_monitor.web.deps import web_reader

         with web_reader(cfg) as conn:
            hb = queries.heartbeat(conn, cfg.system)
            rows = queries.node_states(conn, cfg.system)
      except (psycopg.Error, DatabaseUnavailable, OSError):
         return {
            "ok": False,
            "heartbeat_age_s": None,
            "nodes": [],
            "db_ok": False,
         }

      if hb is None:
         heartbeat_age_s = None
         ok = False
      else:
         heartbeat_age_s = int((now - hb["last_seen"]).total_seconds())
         heartbeat_age_s = max(0, heartbeat_age_s)
         # Stopped is not healthy even when fresh (matches CLI status).
         stopped = (hb.get("status") or {}).get("state") == "stopped"
         ok = not stopped and heartbeat_age_s < _HEARTBEAT_STALE_SEC

      nodes = []
      for row in rows:
         updated_age_s = int((now - row["updated_at"]).total_seconds())
         updated_age_s = max(0, updated_age_s)
         if updated_age_s > _NODE_STALE_SEC:
            state = "stale"
         else:
            state = row["session_state"] or "unknown"
         nodes.append({"label": row["label"], "state": state, "updated_age_s": updated_age_s})

      return {
         "ok": ok,
         "heartbeat_age_s": heartbeat_age_s,
         "nodes": nodes,
         "db_ok": True,
      }

   @app.get("/", response_class=HTMLResponse)
   def index(request: Request):
      cfg: Config = app.state.config
      now = datetime.now(timezone.utc)
      nodes_data: list[dict] = []
      heartbeat_age_s2: int | None = None
      heartbeat_state = None
      ok2 = False
      last_gap = None
      events: list[dict] = []
      category_counts: dict[str, int] = {}
      dormancy_by_category: dict[str, dict] = {}
      processes: list[dict] = []
      total_processes = 0
      try:
         from node_monitor.database import queries
         from node_monitor.web.deps import web_reader

         with web_reader(cfg) as conn:
            hb = queries.heartbeat(conn, cfg.system)
            rows = queries.node_states(conn, cfg.system)
            if hb is not None:
               heartbeat_age_s2 = int((now - hb["last_seen"]).total_seconds())
               heartbeat_age_s2 = max(0, heartbeat_age_s2)
               stopped = (hb.get("status") or {}).get("state") == "stopped"
               ok2 = not stopped and heartbeat_age_s2 < _HEARTBEAT_STALE_SEC
               heartbeat_state = (hb.get("status") or {}).get("state")
            for row in rows:
               updated_age_s = int((now - row["updated_at"]).total_seconds())
               updated_age_s = max(0, updated_age_s)
               state = (
                  "stale"
                  if updated_age_s > _NODE_STALE_SEC
                  else (row["session_state"] or "unknown")
               )
               nodes_data.append(
                  {
                     "label": row["label"],
                     "state": state,
                     "updated_age_s": updated_age_s,
                     "load1": row.get("load1"),
                     "mem_used_kb": row.get("mem_used_kb"),
                     "mem_total_kb": row.get("mem_total_kb"),
                     "procs_total": row.get("procs_total"),
                     "users": row.get("users", 0),
                     "user_processes": row.get("user_processes", 0),
                     "probe_version": row.get("probe_version"),
                     "node": row.get("node"),
                  }
               )
            try:
               processes = queries.processes(conn, cfg.system, limit=2000)
               total_processes = len(processes)
               for p in processes:
                  cat = p.get("category") or "other"
                  category_counts[cat] = category_counts.get(cat, 0) + 1
            except Exception:
               pass
            try:
               for p in processes:
                  if p.get("owner_presence") == "absent" and p.get("cpu_state") == "idle":
                     sec = p.get("idle_for_sec") or 0
                     if sec > 3600:
                        cat = p.get("category") or "other"
                        entry = dormancy_by_category.setdefault(cat, {"count": 0, "rss_kb": 0})
                        entry["count"] += 1
                        entry["rss_kb"] += int(p.get("rss_kb") or 0)
            except Exception:
               pass
            try:
               events = queries.events(conn, cfg.system, limit=20)
               for ev in events:
                  if ev.get("event_type") in ("session_lost", "gap", "node_rebooted"):
                     last_gap = ev.get("ts").isoformat() if ev.get("ts") else None
                     break
            except Exception:
               pass
      except Exception:
         pass
      category_colors = {
         "ai_agent": "#8b5cf6",
         "editor_server": "#06b6d4",
         "jupyter": "#f59e0b",
         "build": "#ef4444",
         "python": "#3b82f6",
         "shell": "#6b7280",
         "data_transfer": "#10b981",
         "fs_scan": "#a3a3a3",
         "scheduler": "#ec4899",
         "workflow": "#14b8a6",
         "system": "#52525b",
         "other": "#9ca3af",
      }

      def _json_default(o):
         if hasattr(o, "isoformat"):
            return o.isoformat()
         return str(o)

      initial = {"processes": processes[:500]}
      tmpl = _templates.get_template("index.html")
      html = tmpl.render(
         ok=ok2,
         heartbeat_age_s=heartbeat_age_s2,
         heartbeat_state=heartbeat_state,
         nodes=nodes_data,
         last_gap=last_gap,
         events=events,
         category_counts=category_counts,
         category_colors=category_colors,
         dormancy_by_category=dormancy_by_category,
         processes=processes[:100],
         total_processes=total_processes,
         initial_json=json.dumps(initial, default=_json_default),
      )
      return HTMLResponse(html)

   @app.get("/node/{node}", response_class=HTMLResponse)
   def node_page(node: str, request: Request):
      cfg: Config = app.state.config
      try:
         from node_monitor.database import queries
         from node_monitor.web.deps import web_reader

         with web_reader(cfg) as conn:
            rows = queries.node_states(conn, cfg.system)
            # find matching node by label or host
            match = None
            for r in rows:
               if r["label"] == node or r["node"] == node:
                  match = r
                  break
            if match is None:
               from fastapi.responses import JSONResponse

               return JSONResponse(status_code=404, content={"detail": "Not Found"})
            now2 = datetime.now(timezone.utc)
            updated_age_s = int((now2 - match["updated_at"]).total_seconds())
            updated_age_s = max(0, updated_age_s)
            state = (
               "stale" if updated_age_s > _NODE_STALE_SEC else (match["session_state"] or "unknown")
            )
            node_label = match["label"]
            # live processes for this node for table
            try:
               procs = queries.processes(conn, cfg.system, node=node, limit=200)
            except Exception:
               procs = []
            node_json = json.dumps({"label": node_label, "node": match["node"]})
            tmpl2 = _templates.get_template("node.html")
            html2 = tmpl2.render(
               node=node,
               node_label=node_label,
               state=state,
               updated_age_s=updated_age_s,
               node_json=node_json,
               processes=procs,
            )
            return HTMLResponse(html2)
      except Exception as exc:
         # if we already returned 404 above, don't swallow
         if "Not Found" in str(exc):
            raise
         return HTMLResponse(f"error: {exc}", status_code=500)
      return HTMLResponse("not found", status_code=404)

   return app
