"""FastAPI application factory."""

from datetime import datetime, timezone
from pathlib import Path

import psycopg
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from node_monitor.config import Config, find_config_path, load_config
from node_monitor.database.connect import DatabaseUnavailable

_HEARTBEAT_STALE_SEC = 180
_NODE_STALE_SEC = 120


def create_app(config: Config | None = None) -> FastAPI:
   if config is None:
      path = find_config_path()
      config = load_config(str(path))

   app = FastAPI(title="node-monitor")
   app.state.config = config

   static_dir = Path(__file__).parent / "static"
   app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

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

   return app
