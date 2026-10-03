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

   # Exact static allowlist only
   static_dir = os.path.join(_APP_DIR, "static")
   allowed_files = {"index.html", "styles.css", "app.js", "chart.umd.min.js"}

   @app.get("/")
   async def root():
      index_path = os.path.join(static_dir, "index.html")
      with open(index_path, "r") as f:
         content = f.read()
      return Response(content=content, media_type="text/html")

   STATIC_ROUTES = {
      "/static/index.html": ("index.html", "text/html"),
      "/static/styles.css": ("styles.css", "text/css"),
      "/static/app.js": ("app.js", "application/javascript"),
      "/static/chart.umd.min.js": ("chart.umd.min.js", "application/javascript"),
   }

   @app.get("/static/index.html")
   async def static_index():
      return Response(open(os.path.join(static_dir, "index.html"), "rb").read(), media_type="text/html")

   @app.get("/static/styles.css")
   async def static_styles():
      return Response(open(os.path.join(static_dir, "styles.css"), "rb").read(), media_type="text/css")

   @app.get("/static/app.js")
   async def static_app():
      return Response(open(os.path.join(static_dir, "app.js"), "rb").read(), media_type="application/javascript")

   @app.get("/static/chart.umd.min.js")
   async def static_chart():
      return Response(open(os.path.join(static_dir, "chart.umd.min.js"), "rb").read(), media_type="application/javascript")

   return app
