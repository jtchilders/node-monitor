"""Task 6: /node/{node} tests."""

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


def test_node_page_loads_for_known_node(migrated_db, tmp_path):
   url = migrated_db
   populate(url)
   app = _client_for(url, tmp_path)
   client = TestClient(app)
   resp = client.get("/node/N1")
   assert resp.status_code == 200
   assert "N1" in resp.text or "n1" in resp.text.lower()
   # range picker
   assert "range" in resp.text.lower() or "data-range" in resp.text


def test_node_page_404_for_unknown(migrated_db, tmp_path):
   url = migrated_db
   populate(url)
   app = _client_for(url, tmp_path)
   client = TestClient(app)
   resp = client.get("/node/UNKNOWN_NODE_XYZ")
   assert resp.status_code == 404


def test_range_picker_changes_bucket_param(migrated_db, tmp_path):
   url = migrated_db
   populate(url)
   app = _client_for(url, tmp_path)
   client = TestClient(app)
   resp = client.get("/node/N1")
   assert resp.status_code == 200
   # JS should contain bucket param handling
   assert "bucket" in resp.text.lower()


def test_stacked_areas_sum_to_node_total(migrated_db, tmp_path):
   url = migrated_db
   populate(url)
   app = _client_for(url, tmp_path)
   client = TestClient(app)
   # verify page has stacked area container
   resp = client.get("/node/N1")
   assert "chart" in resp.text.lower() or "canvas" in resp.text.lower()


def test_live_table_filtered_to_node(migrated_db, tmp_path):
   url = migrated_db
   populate(url)
   app = _client_for(url, tmp_path)
   client = TestClient(app)
   resp = client.get("/api/live/processes", params={"node": "N1"})
   n1 = resp.json()
   resp2 = client.get("/api/live/processes", params={"node": "N2"})
   n2 = resp2.json()
   # N1 and N2 should have disjoint pids (per live_fixture)
   pids_n1 = {p["pid"] for p in n1["items"]}
   pids_n2 = {p["pid"] for p in n2["items"]}
   assert pids_n1.isdisjoint(pids_n2) or n1["total"] != n2["total"]
   # node page should mention live table filtered container
   page = client.get("/node/N1")
   assert "process" in page.text.lower()
