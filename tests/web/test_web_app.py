"""Task 6 tests: route/static/CLI/packaging with real assertions."""
import hashlib
import importlib
import os
import sys

import pytest
from fastapi.testclient import TestClient

@pytest.fixture
def test_client():
   from node_monitor.web.service import DashboardService
   from node_monitor.web.app import create_app
   # Minimal dummy service that raises expected errors
   class DummyService:
      async def dashboard(self, node, range_name, username=None):
         from node_monitor.web.service import DashboardRequestError, DashboardServiceError
         if range_name not in ("1h","3h","6h","12h","24h"):
            raise DashboardRequestError("bad range")
         raise DashboardServiceError("fail")
   app = create_app(DummyService())
   return TestClient(app)

CHART_PATH = "node_monitor/web/static/chart.umd.min.js"
CHART_HASH = "d2af8974e95271638772e9e9524db5b9a6f58d6ec2d5d781400447b4a31c681e"
CHART_SIZE = 205399


def test_chart_js_checksum_size_content_gate():
   assert os.path.exists(CHART_PATH)
   data = open(CHART_PATH, "rb").read()
   assert len(data) == CHART_SIZE, f"size={len(data)} expected {CHART_SIZE}"
   assert hashlib.sha256(data).hexdigest() == CHART_HASH
   # Non-placeholder: must contain real Chart.js, not placeholder string
   assert b"Chart" in data or b"chart" in data
   assert b"placeholder" not in data


def test_health_never_touches_database():
   from node_monitor.web.app import create_app
   from node_monitor.web.service import DashboardService
   calls = []
   class SpyService:
      async def dashboard(self, **kw):
         calls.append(kw)
         raise Exception("should not call")
   app = create_app(SpyService())
   from fastapi.testclient import TestClient
   client = TestClient(app)
   resp = client.get("/health")
   assert resp.status_code == 200
   assert resp.json() == {"status": "ok"}
   assert len(calls) == 0, "health must not invoke dashboard()"


def test_dashboard_unknown_range_is_422(test_client):
   # Without a real DB, we test that app exists and responds to bad params
   # by ensuring the route is registered and handles invalid range
   from node_monitor.web.service import DashboardRequestError
   # At minimum the app factory creates the route; actual 422 requires service wiring
   resp = test_client.get("/api/dashboard", params={"node": "n", "range": "bad"})
   # Should not be 500; with a dummy service that raises DashboardRequestError -> 422
   assert resp.status_code == 422


def test_static_routes_exact_allowlist(test_client):
   assert test_client.get("/").status_code == 200
   assert test_client.get("/static/styles.css").status_code == 200
   assert test_client.get("/static/app.js").status_code == 200
   assert test_client.get("/static/chart.umd.min.js").status_code == 200
   assert test_client.get("/static/../config.py").status_code == 404
   assert test_client.get("/static/config.yaml").status_code == 404


def test_no_docs_openapi(test_client):
   assert test_client.get("/docs").status_code == 404
   assert test_client.get("/redoc").status_code == 404
   assert test_client.get("/openapi.json").status_code == 404

def test_web_cli_only_config_option():
   from node_monitor.cli.main import cli
   web_cmd = cli.commands.get("web")
   assert web_cmd is not None
   params = {p.name for p in web_cmd.params}
   assert params == {"config_path"}
