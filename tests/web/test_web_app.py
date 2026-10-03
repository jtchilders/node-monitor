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


# ---------------------------------------------------------------------------
# Task 6 correction: static security and route introspection tests
# ---------------------------------------------------------------------------

def test_read_static_traversal_raises_fixed_message():
   """read_static('../../secret-SENTINEL') raises ValueError with fixed message,
   never reflecting attacker input in the exception message."""
   from node_monitor.web.static_impl import read_static

   attacker_input = "../../secret-SENTINEL-x9y8z7w6"
   try:
      read_static(attacker_input)
      assert False, "expected ValueError"
   except ValueError as exc:
      msg = str(exc)
      # Fixed message must be present
      assert msg == "invalid static file name", (
         "ValueError message must be 'invalid static file name'; got %r" % msg
      )
      # Attacker input must NOT be reflected in exception message
      assert attacker_input not in msg, (
         "attacker input reflected in ValueError message: %r" % msg
      )
      assert "secret" not in msg, (
         "attacker input reflected in ValueError message: %r" % msg
      )


def test_read_static_does_not_reflect_attacker_input():
   """Attacker-supplied names that are not in the allowlist produce only the
   fixed ValueError message -- no reflection of the attacker-controlled value."""
   from node_monitor.web.static_impl import read_static

   attacker_names = [
      "../../etc/passwd",
      "secret-data.db",
      "/absolute/path",
      "index.html; rm -rf /",
      "\x00null",
   ]
   for name in attacker_names:
      try:
         read_static(name)
         assert False, "expected ValueError for %r" % name
      except ValueError as exc:
         assert name not in str(exc), (
            "attacker name reflected in error: input=%r msg=%r" % (name, str(exc))
         )


def test_static_route_handlers_no_query_params():
   """Static route handlers expose no query parameters (name/media cannot affect response)."""
   from fastapi.testclient import TestClient
   from node_monitor.web.app import create_app

   client = TestClient(create_app(None))

   # The four allowed static routes exist and return 200
   static_routes = [
      "/static/index.html",
      "/static/styles.css",
      "/static/app.js",
      "/static/chart.umd.min.js",
   ]
   for route in static_routes:
      # Normal request
      normal = client.get(route)
      assert normal.status_code == 200, "expected 200 for %s" % route
      normal_bytes = normal.content
      normal_ct = normal.headers.get("content-type", "")

      # Attempt to inject attacker 'name' and 'media' query params
      injected = client.get(route, params={"name": "../../etc/passwd",
                                            "media": "text/dangerous"})
      assert injected.status_code == 200, (
         "injected params must not break route; got %d for %s"
         % (injected.status_code, route)
      )
      # Response bytes must be identical (attacker params ignored)
      assert injected.content == normal_bytes, (
         "injected 'name' param must not change response bytes for %s" % route
      )
      # Content-type must be identical (attacker 'media' param ignored)
      assert injected.headers.get("content-type", "") == normal_ct, (
         "injected 'media' param must not change content-type for %s" % route
      )


def test_static_fallback_arbitrary_paths_404():
   """Arbitrary paths under /static/{path:path} return 404."""
   from fastapi.testclient import TestClient
   from node_monitor.web.app import create_app

   client = TestClient(create_app(None))
   arbitrary_paths = [
      "/static/notexist.js",
      "/static/config.yaml",
      "/static/secrets.env",
      "/static/foo/bar/baz",
      "/static/",
   ]
   for path in arbitrary_paths:
      resp = client.get(path)
      assert resp.status_code == 404, (
         "expected 404 for %s; got %d" % (path, resp.status_code)
      )
