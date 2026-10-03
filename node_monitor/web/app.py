"""node_monitor.web.app -- FastAPI routes and exact static-resource allowlist."""
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.staticfiles import StaticFiles
import os

_APP_DIR = os.path.dirname(os.path.abspath(__file__))


def create_app(service):
   app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

   @app.get("/health")
   async def health():
      return {"status": "ok"}

   @app.get("/api/dashboard")
   async def dashboard(node: str, range: str = "1h", username: str = None):
      try:
         from node_monitor.web.service import DashboardRequestError, DashboardServiceError, DashboardService
         payload = await service.dashboard(node=node, range_name=range, username=username)
         return Response(content=payload, media_type="application/json")
      except DashboardRequestError:
         raise HTTPException(status_code=422, detail="invalid dashboard request") from None
      except DashboardServiceError:
         raise HTTPException(status_code=503, detail="dashboard refresh failed") from None
      except Exception:
         raise HTTPException(status_code=503, detail="dashboard refresh failed") from None

   # Exact static allowlist only
   static_dir = os.path.join(_APP_DIR, "static")
   allowed_files = {"index.html", "styles.css", "app.js", "chart.umd.min.js"}

   @app.get("/")
   async def root():
      index_path = os.path.join(static_dir, "index.html")
      with open(index_path, "r") as f:
         content = f.read()
      return Response(content=content, media_type="text/html")

   @app.get("/static/{filename}")
   async def static_file(filename: str):
      if filename not in allowed_files:
         raise HTTPException(status_code=404)
      file_path = os.path.join(static_dir, filename)
      real_path = os.path.abspath(file_path)
      static_abs = os.path.abspath(static_dir)
      if not real_path.startswith(static_abs + "/") and real_path != static_abs:
         raise HTTPException(status_code=404)
      with open(real_path, "rb") as f:
         content = f.read()
      media = {".html": "text/html", ".css": "text/css",
               ".js": "application/javascript"}.get(
         os.path.splitext(filename)[1], "application/octet-stream")
      return Response(content=content, media_type=media)

   return app
