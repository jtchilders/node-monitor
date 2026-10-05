"""Behavioral tests for the database-backed node inventory API."""
import asyncio
import json
from unittest.mock import MagicMock

import pytest

from node_monitor.config import NodeConfig


class _Result:

   def __init__(self, hostnames):
      self._hostnames = hostnames

   def all(self):
      return [(hostname,) for hostname in self._hostnames]


class _Connection:

   def __init__(self, hostnames):
      self._hostnames = hostnames
      self.calls = []

   def execute(self, statement, params):
      self.calls.append((str(statement), params))
      return _Result(self._hostnames)


def _service(hostnames, config_nodes=()):
   from node_monitor.web.service import DashboardService
   connection = _Connection(hostnames)
   service = DashboardService(
      MagicMock(), system="polaris", config_nodes=config_nodes)

   async def run(engine, operation, timeout_sec):
      return operation(connection, None)

   return service, connection, run


def test_inventory_uses_fixed_select_and_exact_metadata(monkeypatch):
   configured = (
      NodeConfig("polaris-login-04.example.org", "local",
                 display_name="login-04"),
      NodeConfig("polaris-login-01.example.org", "remote",
                 display_name="login-01"),
      NodeConfig("absent.example.org", "remote", display_name="absent"),
   )
   service, connection, run = _service(
      ["zeta.example.org", "polaris-login-01.example.org",
       "polaris-login-04.example.org"], configured)
   monkeypatch.setattr(
      "node_monitor.database.web.run_dashboard_with_deadline", run)

   data = json.loads(asyncio.run(service.nodes()))

   assert data == {
      "system": "polaris",
      "nodes": [
         {"id": "polaris-login-04.example.org", "label": "login-04",
          "configured": True, "role": "local"},
         {"id": "polaris-login-01.example.org", "label": "login-01",
          "configured": True, "role": "remote"},
         {"id": "zeta.example.org", "label": "zeta.example.org",
          "configured": False, "role": None},
      ],
   }
   sql, params = connection.calls[0]
   assert "SELECT DISTINCT source_hostname" in sql
   assert "LIMIT 129" in sql
   assert not any(word in sql.upper() for word in ("INSERT", "UPDATE", "DELETE"))
   assert params == {"system": "polaris"}


def test_inventory_fallback_label_is_conservative(monkeypatch):
   service, _, run = _service([
      "polaris-login-07.hsn.cm.polaris.alcf.anl.gov",
      "polaris-gpu-07.hsn.cm.polaris.alcf.anl.gov",
   ])
   monkeypatch.setattr(
      "node_monitor.database.web.run_dashboard_with_deadline", run)
   nodes = json.loads(asyncio.run(service.nodes()))["nodes"]
   assert nodes[0]["label"] == "polaris-gpu-07.hsn.cm.polaris.alcf.anl.gov"
   assert nodes[1]["label"] == "login-07"


def test_inventory_empty(monkeypatch):
   service, _, run = _service([])
   monkeypatch.setattr(
      "node_monitor.database.web.run_dashboard_with_deadline", run)
   assert json.loads(asyncio.run(service.nodes())) == {
      "system": "polaris", "nodes": []}


def test_inventory_rejects_129_rows_chainless(monkeypatch):
   from node_monitor.web.service import NodesServiceError
   service, _, run = _service(
      ["node-%03d.example.org" % number for number in range(129)])
   monkeypatch.setattr(
      "node_monitor.database.web.run_dashboard_with_deadline", run)
   with pytest.raises(NodesServiceError) as caught:
      asyncio.run(service.nodes())
   assert caught.value.__cause__ is None


def test_inventory_rejects_payload_over_64kib(monkeypatch):
   from node_monitor.web.service import NodesTooLarge
   service, _, run = _service(
      ["node-%03d-%s" % (number, "x" * 700) for number in range(100)])
   monkeypatch.setattr(
      "node_monitor.database.web.run_dashboard_with_deadline", run)
   with pytest.raises(NodesTooLarge):
      asyncio.run(service.nodes())


def test_inventory_query_error_is_sanitized(monkeypatch):
   from node_monitor.web.service import DashboardService, NodesServiceError
   service = DashboardService(MagicMock(), system="polaris")

   async def fail(engine, operation, timeout_sec):
      raise RuntimeError("postgresql://reader:SECRET@host/db SELECT secret")

   monkeypatch.setattr(
      "node_monitor.database.web.run_dashboard_with_deadline", fail)
   with pytest.raises(NodesServiceError) as caught:
      asyncio.run(service.nodes())
   assert str(caught.value) == "node inventory query failed"
   assert caught.value.__cause__ is None


def test_nodes_route_returns_bytes_and_fixed_503():
   from fastapi.testclient import TestClient
   from node_monitor.web.app import create_app
   from node_monitor.web.service import NodesServiceError

   class Service:
      fail = False

      async def nodes(self):
         if self.fail:
            raise NodesServiceError("SECRET")
         return b'{"system":"polaris","nodes":[]}'

   service = Service()
   client = TestClient(create_app(service))
   response = client.get("/api/nodes")
   assert response.status_code == 200
   assert response.content == b'{"system":"polaris","nodes":[]}'
   assert response.headers["content-type"] == "application/json"
   service.fail = True
   response = client.get("/api/nodes")
   assert response.status_code == 503
   assert response.json() == {"detail": "node inventory failed"}
   assert "SECRET" not in response.text
