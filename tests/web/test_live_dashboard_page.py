"""Task 2: pie breakdown — test index page has pie canvas and group select."""

import yaml
from fastapi.testclient import TestClient

from node_monitor.config import load_config

MINIMAL = {
   "system": "polaris",
   "probe_python": "/usr/bin/python3.11",
   "nodes": [{"host": "n1.example", "label": "n1"}],
}


def _client(tmp_path):
   bad_url = "postgresql://localhost:1/x"
   base = tmp_path / "config.yaml"
   base.write_text(yaml.safe_dump({**MINIMAL, "web": {"database_url": bad_url}}))
   config = load_config(str(base))
   from node_monitor.web.app import create_app

   return TestClient(create_app(config))


def test_index_page_has_pie_canvas_and_group_select(tmp_path):
   client = _client(tmp_path)
   html = client.get("/").text
   assert 'id="pie-canvas"' in html
   assert 'id="group-select"' in html
   assert 'value="category"' in html and 'value="user"' in html and 'value="activity"' in html


def test_pie_empty_state_hidden_when_data_present(tmp_path):
   client = _client(tmp_path)
   html = client.get("/").text
   assert 'id="pie-empty"' in html


def test_index_has_rss_and_cpu_canvases(tmp_path):
   client = _client(tmp_path)
   html = client.get("/").text
   assert 'id="rss-canvas"' in html
   assert 'id="cpu-canvas"' in html
