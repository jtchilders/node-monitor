"""Real TCP loopback integration (Task 4): ephemeral port, /health,
production-shaped /api/dashboard without live DB mutation (controlled
service seam, no mutation hooks).
"""
import json
import multiprocessing
import os
import socket
import sys
import time

import httpx
import pytest

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(
   os.path.abspath(__file__))))
sys.path.insert(0, _REPO)

from node_monitor.web.runtime import run_uvicorn
from node_monitor.web.app import create_app


def _free_port():
   s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
   s.bind(("127.0.0.1", 0))
   port = s.getsockname()[1]
   s.close()
   return port


class FakeService:
   async def dashboard(self, *, node, range_name, username):
      return json.dumps({
         "node": node,
         "range_hours": 1,
         "hardware": {"system": "test"},
         "counters": {"rows": [], "status": "empty",
            "gaps": {"missing_count": 0, "intervals": []},
            "latest": None},
         "usage": {"grains": [], "status": "empty",
            "gaps": {"missing_count": 0, "intervals": []}},
      }).encode("utf-8")


def _serve(host, port):
   service = FakeService()
   app = create_app(service)
   run_uvicorn(app, host=host, port=port)


def test_ephemeral_tcp_health_and_dashboard_shape():
   port = _free_port()
   server_process = multiprocessing.Process(
      target=_serve, args=("127.0.0.1", port))
   server_process.start()

   url_health = "http://127.0.0.1:%d/health" % port
   url_dashboard = "http://127.0.0.1:%d/api/dashboard?node=n1&range=1h" % port

   try:
      ready = False
      for _ in range(40):
         if not server_process.is_alive():
            break
         try:
            r = httpx.get(url_health, timeout=0.3)
            if r.status_code == 200:
               ready = True
               break
         except httpx.HTTPError:
            pass
         time.sleep(0.1)
      assert ready, "server never became ready on port %d" % port

      r_health = httpx.get(url_health, timeout=5.0)
      assert r_health.status_code == 200
      assert r_health.json() == {"status": "ok"}

      r_dash = httpx.get(url_dashboard, timeout=5.0)
      assert r_dash.status_code == 200
      dash = r_dash.json()
      assert dash.get("node") == "n1"
      assert "hardware" in dash
      assert "counters" in dash
   finally:
      server_process.terminate()
      server_process.join(timeout=5.0)
      if server_process.is_alive():
         server_process.kill()
         server_process.join(timeout=5.0)
   assert not server_process.is_alive()


def test_occupied_port_fails_bounded():
   blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
   blocker.bind(("127.0.0.1", 0))
   occupied_port = blocker.getsockname()[1]
   blocker.listen(1)

   process = multiprocessing.Process(
      target=_serve,
      args=("127.0.0.1", occupied_port))
   try:
      process.start()
      process.join(timeout=5.0)
      assert not process.is_alive(), "run_uvicorn hung on occupied port"
      assert process.exitcode not in (None, 0)
   finally:
      if process.is_alive():
         process.kill()
         process.join(timeout=5.0)
      blocker.close()
