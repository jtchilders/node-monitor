"""Task 5: /analytics page tests."""

import pathlib

import yaml
from fastapi.testclient import TestClient

from node_monitor.config import load_config
from tests.database import conftest as pg
from tests.database.live_fixture import populate
from tests.test_config import MINIMAL

migrated_db = pg.migrated_db


def _client_for(url, tmp_path):
   base = tmp_path / "config.yaml"
   base.write_text(
      yaml.safe_dump({**MINIMAL, "working_dir": str(tmp_path / "wd"), "web": {"database_url": url}})
   )
   config = load_config(str(base))
   from node_monitor.web.app import create_app

   return create_app(config)


def _client_bad(tmp_path):
   import yaml as _yaml

   base = tmp_path / "config.yaml"
   base.write_text(
      _yaml.safe_dump(
         {
            **MINIMAL,
            "working_dir": str(tmp_path / "wd"),
            "web": {"database_url": "postgresql://localhost:1/x"},
         }
      )
   )
   config = load_config(str(base))
   from node_monitor.web.app import create_app

   return create_app(config)


def test_analytics_page_200(migrated_db, tmp_path):
   url = migrated_db
   populate(url)
   app = _client_for(url, tmp_path)
   client = TestClient(app)
   resp = client.get("/analytics")
   assert resp.status_code == 200, resp.text


def test_analytics_page_200_without_db(tmp_path):
   app = _client_bad(tmp_path)
   client = TestClient(app)
   resp = client.get("/analytics")
   assert resp.status_code == 200, resp.text


def test_analytics_charts_render(migrated_db, tmp_path):
   url = migrated_db
   populate(url)
   app = _client_for(url, tmp_path)
   client = TestClient(app)
   resp = client.get("/analytics")
   assert resp.status_code == 200
   text = resp.text.lower()
   assert text.count("<canvas") >= 3, f"expected >=3 canvases, got {text.count('<canvas')}"
   assert "canvas" in text
   assert "chart" in text
   assert "analytics.js" in resp.text or "analytics" in text


def test_analytics_js_uses_chartjs(migrated_db, tmp_path):
   url = migrated_db
   populate(url)
   _client_for(url, tmp_path)
   js_path = pathlib.Path("node_monitor/web/static/js/analytics.js")
   assert js_path.exists(), "analytics.js not created"
   js = js_path.read_text()
   assert "Chart" in js, "JS must use Chart.js"
   assert "30000" in js or "30 * 1000" in js or "30*1000" in js, "must poll every 30s"
   assert "/api/analytics/" in js, "JS must fetch analytics APIs"
   for ep in ("category-mix", "agent-adoption", "dormancy", "leaderboard", "coverage"):
      assert ep in js, f"JS must reference {ep} endpoint"


def test_range_picker_drives_bucket_param(migrated_db, tmp_path):
   url = migrated_db
   populate(url)
   app = _client_for(url, tmp_path)
   client = TestClient(app)
   resp = client.get("/analytics")
   assert resp.status_code == 200
   assert "data-range" in resp.text or "range" in resp.text.lower()
   assert "bucket" in resp.text.lower(), "page must reference bucket param"
   js_path = pathlib.Path("node_monitor/web/static/js/analytics.js")
   assert js_path.exists()
   js = js_path.read_text()
   assert "bucket" in js
   for r in ("1h", "24h", "7d", "30d", "1y", "2y"):
      assert r in js or r in resp.text, f"range {r} missing from page/JS"
   assert "bucket=" in js or "bucket" in js


def test_range_picker_bucket_mapping(migrated_db, tmp_path):
   js_path = pathlib.Path("node_monitor/web/static/js/analytics.js")
   assert js_path.exists()
   js = js_path.read_text()
   assert "1h" in js
   assert "1d" in js
   assert "category-mix" in js or "dormancy" in js
   assert "bucket" in js


def test_leaderboards_paged(migrated_db, tmp_path):
   url = migrated_db
   populate(url)
   app = _client_for(url, tmp_path)
   client = TestClient(app)
   resp = client.get("/analytics")
   assert resp.status_code == 200
   text = resp.text.lower()
   assert "leaderboard" in text or "leader" in text
   assert "page" in text or "paged" in text or "data-page" in text or "show more" in text
   js_path = pathlib.Path("node_monitor/web/static/js/analytics.js")
   assert js_path.exists()
   js = js_path.read_text().lower()
   assert "leaderboard" in js
   assert "page" in js


def test_coverage_bars(migrated_db, tmp_path):
   url = migrated_db
   populate(url)
   app = _client_for(url, tmp_path)
   client = TestClient(app)
   resp = client.get("/analytics")
   assert resp.status_code == 200
   text = resp.text.lower()
   assert "coverage" in text
   js_path = pathlib.Path("node_monitor/web/static/js/analytics.js")
   assert js_path.exists()
   js = js_path.read_text()
   assert "coverage" in js.lower()


def test_xss_category_as_textcontent():
   js_path = pathlib.Path("node_monitor/web/static/js/analytics.js")
   assert js_path.exists(), "analytics.js not created"
   js = js_path.read_text()
   assert "textContent" in js, "must use textContent for dynamic values"
   assert "innerHTML" not in js, "must never use innerHTML (XSS via category)"


def test_xss_template_escapes_category(migrated_db, tmp_path):
   url = migrated_db
   populate(url)
   app = _client_for(url, tmp_path)
   client = TestClient(app)
   resp = client.get("/analytics")
   assert resp.status_code == 200
   assert "<script>alert" not in resp.text
   js_path = pathlib.Path("node_monitor/web/static/js/analytics.js")
   if js_path.exists():
      js = js_path.read_text()
      assert "innerHTML" not in js
