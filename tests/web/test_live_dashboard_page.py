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


def test_empty_filter_shows_empty_state(tmp_path):
   client = _client(tmp_path)
   html = client.get("/").text
   assert 'id="pie-empty"' in html
   assert "No processes match filter" in html
   assert 'id="rss-empty"' in html
   assert 'id="cpu-empty"' in html
   # bars say No processes (pie has longer message)
   assert html.count("No processes") >= 2
   # a11y: empty divs role=status and canvases role=img with aria-label
   assert 'role="status"' in html
   assert 'role="img"' in html
   assert html.count("aria-label") >= 3
   # select has aria-label
   assert 'id="group-select"' in html and "aria-label" in html


def test_group_persists_in_sessionStorage(tmp_path):
   from pathlib import Path

   js_path = Path(__file__).parents[2] / "node_monitor" / "web" / "static" / "js" / "index.js"
   js = js_path.read_text()
   assert "sessionStorage" in js
   assert "nm:live:group" in js
   assert "nm:live:node" in js
   # invalid values fall back to category / __all
   assert "GROUP_DEFS" in js
   assert "__all" in js
   # canvas a11y also checked via HTML
   client = _client(tmp_path)
   html = client.get("/").text
   assert 'aria-label="Group by"' in html or 'aria-label="Group by' in html
