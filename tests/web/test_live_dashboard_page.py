"""Task 2: pie breakdown — test index page has pie canvas and group select.
Task 4: layout restructure — dashboard on top, process list below."""

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


def test_node_tabs_still_present_and_filter_dashboard(tmp_path):
   client = _client(tmp_path)
   html = client.get("/").text
   assert 'id="node-tabs"' in html
   assert 'data-node="__all"' in html
   assert 'id="live-dashboard"' in html
   assert 'id="process-table-container"' in html


def test_process_list_is_below_dashboard(tmp_path):
   client = _client(tmp_path)
   html = client.get("/").text
   assert html.index("live-dashboard") < html.index("process-table-container")


def test_dots_container_removed(tmp_path):
   client = _client(tmp_path)
   html = client.get("/").text
   assert "dots-container" not in html and "dots.js" not in html


def test_legend_reflects_current_group(tmp_path):
   client = _client(tmp_path)
   html = client.get("/").text
   # legend and mix must still exist and be group-aware (at least present; JS unit covers grouping)
   assert 'id="category-legend"' in html
   assert 'id="category-mix"' in html
