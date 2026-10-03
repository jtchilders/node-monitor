"""Task 6: exact API bodies, real exception mapping, no CORS/docs, static allowlist."""
import hashlib
import os

import pytest

CHART_HASH = (
   "d2af8974e95271638772e9e9524db5b9a6f58d6ec2d5d781"
   "400447b4a31c681e"
)
CHART_SIZE = 205399


def test_exact_422_and_503_bodies():
   """Dashboard invalid range -> 422 exact body; service error -> 503 exact body."""
   from fastapi.testclient import TestClient
   from node_monitor.web.app import create_app
   from node_monitor.web.service import DashboardRequestError
   from node_monitor.web.service import DashboardServiceError

   class Dummy:
      async def dashboard(self, **kw):
         if kw.get("range_name") not in ("1h", "3h", "6h", "12h", "24h"):
            raise DashboardRequestError("bad range")
         raise DashboardServiceError("fail")

   app = create_app(Dummy())
   client = TestClient(app)
   r422 = client.get("/api/dashboard", params={"node": "n", "range": "bad"})
   assert r422.status_code == 422
   assert r422.json() == {"detail": "invalid dashboard request"}
   r503 = client.get("/api/dashboard", params={"node": "n", "range": "1h"})
   assert r503.status_code == 503
   assert r503.json() == {"detail": "dashboard refresh failed"}


def test_success_bytes_and_content_type():
   """Success path returns raw bytes with application/json content-type."""
   from fastapi.testclient import TestClient
   from node_monitor.web.app import create_app

   class Ok:
      async def dashboard(self, **kw):
         return b'{"ok":true}'

   app = create_app(Ok())
   client = TestClient(app)
   resp = client.get("/api/dashboard", params={"node": "n", "range": "1h"})
   assert resp.status_code == 200
   assert resp.content == b'{"ok":true}'
   assert resp.headers.get("content-type") == "application/json"


def test_health_spy_only():
   """/health returns {status: ok} and does not invoke the service."""
   from fastapi.testclient import TestClient
   from node_monitor.web.app import create_app

   calls = []

   class Spy:
      async def dashboard(self, **kw):
         calls.append(kw)
         raise Exception("should not be called")

   client = TestClient(create_app(Spy()))
   assert client.get("/health").json() == {"status": "ok"}
   assert len(calls) == 0


def test_no_cors_middleware():
   """create_app must not install any CORS middleware."""
   from node_monitor.web.app import create_app

   app = create_app(None)
   cors_names = {
      "CORSMiddleware",
      "cors",
   }
   for mw in getattr(app, "user_middleware", []):
      cls_name = getattr(getattr(mw, "cls", None), "__name__", "")
      assert cls_name not in cors_names, (
         "CORS middleware must not be installed; found: %s" % cls_name
      )


def test_no_docs_redoc_openapi():
   """create_app must disable /docs, /redoc, and /openapi.json."""
   from fastapi.testclient import TestClient
   from node_monitor.web.app import create_app

   app = create_app(None)
   assert app.docs_url is None, "docs_url must be None"
   assert app.redoc_url is None, "redoc_url must be None"
   assert app.openapi_url is None, "openapi_url must be None"

   client = TestClient(app)
   assert client.get("/docs").status_code == 404
   assert client.get("/redoc").status_code == 404
   assert client.get("/openapi.json").status_code == 404


def test_static_exact_routes():
   """Exact static allowlist: four routes 200; everything else 404."""
   from fastapi.testclient import TestClient
   from node_monitor.web.app import create_app

   client = TestClient(create_app(None))
   assert client.get("/").status_code == 200
   for route in (
      "/static/index.html",
      "/static/styles.css",
      "/static/app.js",
      "/static/chart.umd.min.js",
   ):
      assert client.get(route).status_code == 200, (
         "expected 200 for %s" % route
      )
   for bad in (
      "/static/config.py",
      "/static/config.yaml",
      "/static/../config.py",
   ):
      assert client.get(bad).status_code == 404, (
         "expected 404 for %s" % bad
      )


def test_static_encoded_traversal():
   """Percent-encoded traversal variants must not resolve to 200."""
   from fastapi.testclient import TestClient
   from node_monitor.web.app import create_app

   client = TestClient(create_app(None))
   # Encoded dot-dot forms must be rejected
   encoded_variants = [
      "/static/%2e%2e/config.py",       # %2e%2e = ..
      "/static/..%2Fconfig.py",          # ..%2F = ../
      "/static/%2e%2e%2fconfig.py",      # %2e%2e%2f = ../
      "/static/%2F%2Fconfig.py",         # double encoded slash
      "/static/foo%00bar.js",            # null byte
   ]
   for path in encoded_variants:
      resp = client.get(path)
      assert resp.status_code in (400, 404), (
         "encoded traversal %r must return 400 or 404; got %d"
         % (path, resp.status_code)
      )


def test_production_service_invalid_range():
   """Real DashboardService raises DashboardRequestError for an unknown range."""
   import asyncio
   from node_monitor.web.service import DashboardService
   from node_monitor.web.service import DashboardRequestError

   svc = DashboardService(None, system="test")

   async def run():
      return await svc.dashboard(node="n", range_name="7d", username=None)

   with pytest.raises(DashboardRequestError):
      asyncio.run(run())


def test_production_service_username_too_long():
   """Real DashboardService raises DashboardRequestError for an overlong username."""
   import asyncio
   from node_monitor.web.service import DashboardService
   from node_monitor.web.service import DashboardRequestError
   from node_monitor.web.queries import MAX_USERNAME_BYTES

   long_username = "x" * (MAX_USERNAME_BYTES + 1)
   svc = DashboardService(None, system="test")

   async def run():
      return await svc.dashboard(node="n", range_name="1h", username=long_username)

   with pytest.raises(DashboardRequestError):
      asyncio.run(run())
