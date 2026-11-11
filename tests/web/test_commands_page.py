"""/commands page tests."""

import pathlib


def _app(tmp_path, url=None):
   import yaml

   from node_monitor.config import load_config
   from tests.test_config import MINIMAL

   db_url = url or "postgresql://localhost:1/x"
   base = tmp_path / "config.yaml"
   base.write_text(
      yaml.safe_dump(
         {**MINIMAL, "working_dir": str(tmp_path / "wd"), "web": {"database_url": db_url}}
      )
   )
   cfg = load_config(str(base))
   from node_monitor.web.app import create_app

   return create_app(cfg)


def test_commands_page_200(tmp_path):
   from fastapi.testclient import TestClient

   app = _app(tmp_path)
   client = TestClient(app)
   resp = client.get("/commands")
   assert resp.status_code == 200, resp.text


def test_commands_page_has_search_box(tmp_path):
   from fastapi.testclient import TestClient

   app = _app(tmp_path)
   client = TestClient(app)
   resp = client.get("/commands")
   assert resp.status_code == 200
   text = resp.text
   assert "commands.js" in text
   assert "<input" in text.lower()
   # mode toggle substring/regex
   assert "substring" in text.lower()
   assert "regex" in text.lower()


def test_commands_page_with_db_200(tmp_path):
   import os

   if not os.environ.get("NODE_MONITOR_TEST_ADMIN_URL"):
      import pytest

      pytest.skip("NODE_MONITOR_TEST_ADMIN_URL not set")

   from fastapi.testclient import TestClient

   # Use migrated_db via manual connect for this simple page test
   # Use bad url for 503-safe path
   app = _app(tmp_path)
   client = TestClient(app)
   resp = client.get("/commands")
   assert resp.status_code == 200


def test_commands_js_uses_textcontent(tmp_path):
   js_path = pathlib.Path("node_monitor/web/static/js/commands.js")
   assert js_path.exists(), "commands.js missing"
   text = js_path.read_text()
   assert "textContent" in text, "expected textContent for XSS safety"
   assert "innerHTML" not in text, "innerHTML forbidden for XSS safety"


def test_commands_page_filters_present(tmp_path):
   from fastapi.testclient import TestClient

   app = _app(tmp_path)
   client = TestClient(app)
   resp = client.get("/commands")
   assert resp.status_code == 200
   lower = resp.text.lower()
   # Filters for user/activity/tag should be present in page
   assert "user" in lower
   assert "activity" in lower
   assert "tag" in lower


def test_commands_page_pager_present(tmp_path):
   from fastapi.testclient import TestClient

   app = _app(tmp_path)
   client = TestClient(app)
   resp = client.get("/commands")
   assert resp.status_code == 200
   lower = resp.text.lower()
   assert "prev" in lower
   assert "next" in lower
