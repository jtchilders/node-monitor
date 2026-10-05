"""Real-browser harness for the production dashboard static assets."""
import copy
import json
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest
from playwright.sync_api import sync_playwright


STATIC_DIR = (Path(__file__).resolve().parents[2]
              / "node_monitor" / "web" / "static")
STATIC_TYPES = {
   "index.html": "text/html; charset=utf-8",
   "styles.css": "text/css; charset=utf-8",
   "app.js": "application/javascript; charset=utf-8",
   "chart.umd.min.js": "application/javascript; charset=utf-8",
}


@dataclass
class WebState:
   snapshot: dict
   status: int = 200
   inventory_status: int = 200
   inventory_body: object = None
   dashboard_status_once: object = None
   delay_event: object = None
   requests: list = field(default_factory=list)
   inventory_requests: list = field(default_factory=list)

   def set_snapshot(self, snapshot):
      self.snapshot = copy.deepcopy(snapshot)
      self.status = 200
      self.delay_event = None

   def fail(self, status=503):
      self.status = status
      self.delay_event = None

   def fail_inventory(self, status=503):
      self.inventory_status = status

   def set_inventory(self, nodes):
      self.inventory_status = 200
      self.inventory_body = {"system": "polaris", "nodes": copy.deepcopy(nodes)}

   def set_dashboard_status_once(self, status):
      self.dashboard_status_once = status

   def delay(self):
      self.delay_event = threading.Event()
      return self.delay_event


class LiveWeb:
   def __init__(self, snapshot):
      self.state = WebState(copy.deepcopy(snapshot))
      self.server = None
      self.thread = None
      self.url = None

   def start(self):
      state = self.state

      class Handler(BaseHTTPRequestHandler):
         def log_message(self, _format, *args):
            return

         def send_bytes(self, status, content_type, body):
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

         def do_GET(self):
            parsed = urlparse(self.path)
            if parsed.path == "/api/nodes":
               state.inventory_requests.append(parsed.path)
               if state.inventory_status != 200:
                  self.send_bytes(
                     state.inventory_status, "application/json", b"{}")
                  return
               payload = state.inventory_body
               if payload is None:
                  hostname = state.snapshot.get("hardware", {}).get(
                     "source_hostname", state.snapshot.get("node"))
                  payload = {
                     "system": state.snapshot.get("hardware", {}).get(
                        "system", "polaris"),
                     "nodes": [{
                        "id": hostname, "label": hostname,
                        "configured": True, "role": "local",
                     }] if hostname else [],
                  }
               body = json.dumps(payload).encode("utf-8")
               self.send_bytes(200, "application/json; charset=utf-8", body)
               return
            if parsed.path == "/api/dashboard":
               state.requests.append(parse_qs(parsed.query))
               if state.dashboard_status_once is not None:
                  status = state.dashboard_status_once
                  state.dashboard_status_once = None
                  self.send_bytes(status, "application/json", b"{}")
                  return
               if state.delay_event is not None:
                  state.delay_event.wait(timeout=5)
               if state.status != 200:
                  self.send_bytes(state.status, "application/json", b"{}")
                  return
               body = json.dumps(state.snapshot).encode("utf-8")
               self.send_bytes(200, "application/json; charset=utf-8", body)
               return
            if parsed.path == "/":
               body = (STATIC_DIR / "index.html").read_bytes()
               self.send_bytes(200, STATIC_TYPES["index.html"], body)
               return
            if parsed.path.startswith("/static/"):
               name = parsed.path[len("/static/"):]
               if name not in STATIC_TYPES:
                  self.send_bytes(404, "text/plain", b"not found")
                  return
               self.send_bytes(200, STATIC_TYPES[name],
                               (STATIC_DIR / name).read_bytes())
               return
            self.send_bytes(404, "text/plain", b"not found")

      self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
      port = self.server.server_address[1]
      self.url = "http://127.0.0.1:%d" % port
      self.thread = threading.Thread(target=self.server.serve_forever,
                                     daemon=True)
      self.thread.start()
      return self

   def close(self):
      if self.server is not None:
         self.server.shutdown()
         self.server.server_close()
      if self.thread is not None:
         self.thread.join(timeout=3)


@pytest.fixture
def snapshot_complete():
   return {
      "server_utc_now": "2026-10-03T12:00:00+00:00",
      "node": "login-04",
      "range_hours": 1,
      "hardware": {
         "system": "polaris",
         "source_hostname": "login-04",
         "mem_total_kb": 131072000,
         "cpu_model": "Intel Xeon",
         "cpu_logical": 96,
         "os_pretty_name": "RHEL 8",
      },
      "counters": {
         "rows": [
            {
               "window_end": "2026-10-03T11:57:00+00:00",
               "sample_count": 6,
               "expected_count": 6,
               "coverage": 1.0,
               "complete": True,
               "mem_available_kb": 65000000,
               "load1": 1.2,
               "load5": 0.9,
               "load15": 0.7,
               "procs_running": 3,
               "procs_total": 300,
               "cpu_busy_pct": {"p50": 20.0, "p95": 50.0, "max": 80.0},
               "network_rates": {
                  "eth0": {"rx_bytes_per_sec": {"p50": 1200.5, "p95": 3000, "max": 5000}, "tx_bytes_per_sec": {"p50": 800.0, "p95": 1500, "max": 2500}},
                  "lo": {"rx_bytes_per_sec": {"p50": 100, "p95": 200, "max": 300}, "tx_bytes_per_sec": {"p50": 100, "p95": 200, "max": 300}},
               },
               "lustre_md_summary": {
                  "read": {"p50_sum": 120.5, "p95_sum": 300.0, "max_sum": 500.0, "target_count": 2},
                  "write": {"p50_sum": 80.0, "p95_sum": 150.0, "max_sum": 250.0, "target_count": 3},
               },
            },
            {
               "window_end": "2026-10-03T11:59:00+00:00",
               "sample_count": 6,
               "expected_count": 6,
               "coverage": 1.0,
               "complete": True,
               "mem_available_kb": 64000000,
               "load1": 1.5,
               "load5": 1.0,
               "load15": 0.8,
               "procs_running": 4,
               "procs_total": 400,
               "cpu_busy_pct": {"p50": 25.0, "p95": 60.0, "max": 90.0},
               "network_rates": {
                  "eth0": {"rx_bytes_per_sec": {"p50": 1300.0, "p95": 3100, "max": 5200}, "tx_bytes_per_sec": {"p50": 850.0, "p95": 1600, "max": 2600}},
               },
               "lustre_md_summary": {
                  "read": {"p50_sum": 130.0, "p95_sum": 310.0, "max_sum": 520.0, "target_count": 2},
                  "write": {"p50_sum": 85.0, "p95_sum": 160.0, "max_sum": 260.0, "target_count": 3},
               },
            },
         ],
         "newest_window_end": "2026-10-03T11:59:00+00:00",
         "is_fresh": True,
         "status": "complete",
         "gaps": {"missing_count": 0},
         "latest": {
            "mem_used_physical_kb": 67072000,
            "load1": 1.5,
            "load5": 1.0,
            "load15": 0.8,
            "procs_running": 4,
            "procs_total": 400,
            "cpu_busy_pct": {"p50": 25.0, "p95": 60.0, "max": 90.0},
         },
      },
      "usage": {
         "grains": [
            {
               "interval_end": "2026-10-03T11:30:00+00:00",
               "category": "interactive",
               "activity": "shell",
               "cpu_seconds": 120.5,
               "complete": True,
               "process_count_p50": 4,
               "process_count_p50_username": "alice",
               "process_count_p95": 8,
               "process_count_p95_username": "bob",
               "process_count_max": 12,
               "process_count_max_username": "alice",
               "rss_p50_kb": 102400,
               "rss_p50_username": "alice",
               "rss_p95_kb": 204800,
               "rss_p95_username": "bob",
               "rss_max_kb": 307200,
               "rss_max_username": "alice",
               "d_state_fraction": 0.08,
               "d_state_username": "alice",
               "interactivity_fraction": 0.15,
               "interactivity_username": "alice",
            },
            {
               "interval_end": "2026-10-03T11:45:00+00:00",
               "category": "batch",
               "activity": "compute",
               "cpu_seconds": 300.2,
               "complete": True,
               "process_count_p50": 2,
               "process_count_p50_username": "bob",
               "process_count_p95": 5,
               "process_count_p95_username": "bob",
               "process_count_max": 10,
               "process_count_max_username": "bob",
               "rss_p50_kb": 51200,
               "rss_p50_username": "alice",
               "rss_p95_kb": 153600,
               "rss_p95_username": "bob",
               "rss_max_kb": 256000,
               "rss_max_username": "bob",
               "d_state_fraction": 0.12,
               "d_state_username": "alice",
               "interactivity_fraction": 0.05,
               "interactivity_username": "alice",
            },
         ],
         "newest_interval_end": "2026-10-03T11:45:00+00:00",
         "is_fresh": True,
         "status": "complete",
         "gaps": {"missing_count": 0},
      },
      "nodes_inventory": [
        {"id": "polaris-login-04.hsn.cm.polaris.alcf.anl.gov", "label": "login-04", "configured": False, "role": "local"},
     ],
     "poll_failures": [{
         "recorded_at": "2026-10-03T11:30:00+00:00",
         "failure_type": "timeout",
         "detail": "connect refused",
         "breaker_state": "open",
      }],
      "collection_log": [],
   }


@pytest.fixture
def live_web(snapshot_complete):
   web = LiveWeb(snapshot_complete).start()
   try:
      yield web
   finally:
      web.close()


@pytest.fixture
def browser_page():
   errors = []
   external = []
   with sync_playwright() as playwright:
      browser = playwright.chromium.launch()
      context = browser.new_context()

      def route_request(route):
         parsed = urlparse(route.request.url)
         if parsed.hostname not in ("127.0.0.1", "localhost"):
            external.append(route.request.url)
            route.abort()
         else:
            route.continue_()

      context.route("**/*", route_request)
      page = context.new_page()
      page.on("console", lambda message: (
         errors.append(message.text) if message.type == "error" else None))
      page.on("pageerror", lambda error: errors.append(str(error)))
      yield page, errors, external
      context.close()
      browser.close()
