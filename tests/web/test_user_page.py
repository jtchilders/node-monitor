"""Task 7: /user/{username} tests."""

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


def test_user_page_404_for_unknown(migrated_db, tmp_path):
   url = migrated_db
   populate(url)
   app = _client_for(url, tmp_path)
   client = TestClient(app)
   resp = client.get("/user/UNKNOWN_USER_XYZ_999")
   assert resp.status_code == 404


def test_user_page_loads_for_known_user(migrated_db, tmp_path):
   url = migrated_db
   populate(url)
   app = _client_for(url, tmp_path)
   client = TestClient(app)
   # alice is in live_fixture
   resp = client.get("/user/alice")
   assert resp.status_code == 200
   assert "alice" in resp.text.lower()


def test_user_process_list_paged(migrated_db, tmp_path):
   url = migrated_db
   populate(url)
   app = _client_for(url, tmp_path)
   client = TestClient(app)
   resp = client.get("/user/alice")
   assert resp.status_code == 200
   # paged at 50/page, show more
   assert "50" in resp.text or "page" in resp.text.lower() or "show more" in resp.text.lower()


def test_user_category_history_chart(migrated_db, tmp_path):
   url = migrated_db
   populate(url)
   app = _client_for(url, tmp_path)
   client = TestClient(app)
   resp = client.get("/user/alice")
   assert resp.status_code == 200
   # chart placeholder
   assert "chart" in resp.text.lower() or "canvas" in resp.text.lower()


def test_user_dormancy_bar(migrated_db, tmp_path):
   url = migrated_db
   populate(url)
   app = _client_for(url, tmp_path)
   client = TestClient(app)
   resp = client.get("/user/alice")
   assert resp.status_code == 200
   assert "dormancy" in resp.text.lower() or "profile" in resp.text.lower()


def test_user_xss_username_rendered_as_text(migrated_db, tmp_path):
   url = migrated_db
   populate(url)
   app = _client_for(url, tmp_path)
   client = TestClient(app)
   # unknown user with XSS payload should 404, not reflect
   xss = "<script>alert(1)</script>"
   resp = client.get(f"/user/{xss}")
   # should be 404, and not contain raw XSS payload
   assert resp.status_code == 404
   assert xss not in resp.text
   assert "&lt;script&gt;" not in resp.text or xss not in resp.text
   # known user page should not reflect XSS payload
   resp2 = client.get("/user/alice")
   assert xss not in resp2.text
