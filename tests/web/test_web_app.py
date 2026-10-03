"""Task 6: exact API bodies and real exception mapping."""
import hashlib, os
from fastapi.testclient import TestClient
CHART_HASH = ("d2af8974e95271638772e9e9524db5b9a6f58d6ec2d5d781"
              "400447b4a31c681e")
CHART_SIZE = 205399


def test_exact_422_and_503_bodies():
   from node_monitor.web.app import create_app
   from node_monitor.web.service import (
      DashboardRequestError, DashboardServiceError)
   class Dummy:
      async def dashboard(self, **kw):
         if kw.get("range_name") not in ("1h","3h","6h","12h","24h"):
            raise DashboardRequestError("bad range")
         raise DashboardServiceError("fail")
   app = create_app(Dummy())
   client = TestClient(app)
   r422 = client.get("/api/dashboard", params={"node":"n","range":"bad"})
   assert r422.status_code == 422
   body422 = r422.json()
   assert body422 == {"detail":"invalid dashboard request"}
   r503 = client.get("/api/dashboard", params={"node":"n","range":"1h"})
   assert r503.status_code == 503
   assert r503.json() == {"detail":"dashboard refresh failed"}


def test_success_bytes_and_content_type():
   from node_monitor.web.app import create_app
   class Ok:
      async def dashboard(self, **kw):
         return b'{"ok":true}'
   app = create_app(Ok())
   client = TestClient(app)
   resp = client.get("/api/dashboard", params={"node":"n","range":"1h"})
   assert resp.status_code == 200
   assert resp.content == b'{"ok":true}'
   assert resp.headers.get("content-type") == "application/json"


def test_health_spy_only():
   from node_monitor.web.app import create_app
   calls = []
   class Spy:
      async def dashboard(self, **kw):
         calls.append(kw)
         raise Exception("no")
   client = TestClient(create_app(Spy()))
   assert client.get("/health").json() == {"status":"ok"}
   assert len(calls) == 0


def test_static_exact_routes():
   from node_monitor.web.app import create_app
   client = TestClient(create_app(None))
   assert client.get("/").status_code == 200
   for route in ("/static/index.html","/static/styles.css",
                 "/static/app.js","/static/chart.umd.min.js"):
      assert client.get(route).status_code == 200
   for bad in ("/static/config.py","/static/config.yaml","/static/../x"):
      assert client.get(bad).status_code == 404
