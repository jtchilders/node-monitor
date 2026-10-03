"""node_monitor.web.app -- FastAPI routes, exact static allowlist."""
from fastapi import FastAPI, HTTPException, Response

from node_monitor.web.static_impl import STATIC_ROUTES, read_static


def create_app(service):
   app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

   @app.get("/health")
   async def health():
      return {"status": "ok"}

   @app.get("/api/dashboard")
   async def dashboard(node: str, range: str = "1h", username: str = None):
      from node_monitor.web.service import (
         DashboardRequestError, DashboardServiceError)
      try:
         payload = await service.dashboard(
            node=node, range_name=range, username=username)
         return Response(content=payload, media_type="application/json")
      except DashboardRequestError:
         raise HTTPException(
            status_code=422, detail="invalid dashboard request") from None
      except DashboardServiceError:
         raise HTTPException(
            status_code=503, detail="dashboard refresh failed") from None

   for route, meta in STATIC_ROUTES.items():
      name = meta[0] if isinstance(meta, tuple) else route.split("/")[-1]
      media = meta[1] if isinstance(meta, tuple) and len(meta) > 1 else meta[0]

      async def handler(name=name, media=media):
         return Response(content=read_static(name), media_type=media)
      app.get(route)(handler)

   @app.get("/")
   async def root():
      return Response(content=read_static("index.html"), media_type="text/html")

   return app
