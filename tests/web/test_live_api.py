"""Task 2: live API tests — processes, nodes, users, events."""

from datetime import timedelta

import pytest
import yaml

from node_monitor.database.connect import connect
from tests.database import conftest as pg
from tests.database.live_fixture import SYSTEM, populate, put_heartbeat
from tests.test_config import MINIMAL

migrated_db = pg.migrated_db
pg_admin_url = pg.pg_admin_url
pg_db = pg.pg_db


@pytest.fixture(autouse=True)
def _no_env(monkeypatch):
   for var in ("NODE_MONITOR_DB_URL", "NODE_MONITOR_WEB_DB_URL", "NODE_MONITOR_CONFIG"):
      monkeypatch.delenv(var, raising=False)


def _client_for(url, tmp_path):
   from fastapi.testclient import TestClient

   from node_monitor.config import load_config
   from node_monitor.web.app import create_app

   base = tmp_path / "config.yaml"
   base.write_text(
      yaml.safe_dump({**MINIMAL, "working_dir": str(tmp_path / "wd"), "web": {"database_url": url}})
   )
   config = load_config(str(base))
   app = create_app(config)
   return TestClient(app)


# ---------------------------------------------------------------------------
# /api/live/processes
# ---------------------------------------------------------------------------


def test_processes_list_and_filter_sort(migrated_db, tmp_path):
   url = migrated_db
   populate(url)
   client = _client_for(url, tmp_path)

   resp = client.get("/api/live/processes")
   assert resp.status_code == 200, resp.text
   data = resp.json()
   assert "items" in data and "total" in data
   # 5 user processes (root excluded by default)
   assert data["total"] == 5
   assert len(data["items"]) == 5
   # Default sort is cpu descending: pid 1 is busiest
   assert data["items"][0]["pid"] == 1

   # Filter by node label
   resp = client.get("/api/live/processes", params={"node": "N1"})
   assert resp.status_code == 200
   assert resp.json()["total"] == 3

   # Filter by user
   resp = client.get("/api/live/processes", params={"user": "alice"})
   assert resp.status_code == 200
   assert resp.json()["total"] == 2

   # Filter by category
   resp = client.get("/api/live/processes", params={"category": "ml"})
   assert resp.status_code == 200
   assert resp.json()["total"] == 2

   # Sort by rss
   resp = client.get("/api/live/processes", params={"sort": "rss"})
   assert resp.status_code == 200
   assert resp.json()["items"][0]["pid"] == 1

   # Pagination: limit
   resp = client.get("/api/live/processes", params={"limit": 2})
   assert resp.status_code == 200
   assert len(resp.json()["items"]) == 2
   assert resp.json()["total"] == 5

   # Unknown user → 200 empty
   resp = client.get("/api/live/processes", params={"user": "nobody_xyz"})
   assert resp.status_code == 200
   assert resp.json()["total"] == 0
   assert resp.json()["items"] == []


def test_processes_include_stale(migrated_db, tmp_path):
   url = migrated_db
   now = populate(url)
   # Make n1 snapshot stale (> 120s old)
   with connect(url, autocommit=True) as conn:
      conn.execute(
         "UPDATE live_snapshots SET ts = %s WHERE node = 'n1.example'",
         (now - timedelta(seconds=999),),
      )
   client = _client_for(url, tmp_path)
   # Without include_stale: N1 excluded (default false)
   resp = client.get("/api/live/processes")
   assert resp.status_code == 200
   assert resp.json()["total"] == 2  # only n2

   # With include_stale=true: all included
   resp = client.get("/api/live/processes", params={"include_stale": "true"})
   assert resp.status_code == 200
   assert resp.json()["total"] == 5


def test_processes_unknown_sort_400(tmp_path):
   import yaml
   from fastapi.testclient import TestClient

   from node_monitor.config import load_config
   from node_monitor.web.app import create_app

   base = tmp_path / "config.yaml"
   base.write_text(
      yaml.safe_dump(
         {
            **MINIMAL,
            "working_dir": str(tmp_path / "wd"),
            "web": {"database_url": "postgresql://localhost:1/x"},
         }
      )
   )
   config = load_config(str(base))
   app = create_app(config)
   client = TestClient(app)

   resp = client.get("/api/live/processes", params={"sort": "bogus"})
   assert resp.status_code == 400, resp.text
   body = resp.json()
   # Must mention allowed values
   assert "detail" in body

   # SQL injection via sort
   resp = client.get("/api/live/processes", params={"sort": "cpu; DROP TABLE live_processes; --"})
   assert resp.status_code == 400


# ---------------------------------------------------------------------------
# /api/live/nodes
# ---------------------------------------------------------------------------


def test_nodes_lists(migrated_db, tmp_path):
   url = migrated_db
   now = populate(url)
   put_heartbeat(url, now)
   client = _client_for(url, tmp_path)
   resp = client.get("/api/live/nodes")
   assert resp.status_code == 200, resp.text
   data = resp.json()
   # Paginated wrapper
   assert "items" in data and "total" in data
   assert data["total"] == 2
   assert len(data["items"]) == 2
   labels = {n["label"] for n in data["items"]}
   assert labels == {"N1", "N2"}


# ---------------------------------------------------------------------------
# /api/live/users
# ---------------------------------------------------------------------------


def test_users_list_and_sort(migrated_db, tmp_path):
   url = migrated_db
   populate(url)
   client = _client_for(url, tmp_path)

   resp = client.get("/api/live/users")
   assert resp.status_code == 200, resp.text
   data = resp.json()
   assert "items" in data and "total" in data
   assert data["total"] == 3
   # Default sort rss descending: alice has most RSS
   assert data["items"][0]["username"] == "alice"

   # Sort by cpu
   resp = client.get("/api/live/users", params={"sort": "cpu"})
   assert resp.status_code == 200
   assert resp.json()["items"][0]["username"] == "alice"

   # Limit
   resp = client.get("/api/live/users", params={"limit": 1})
   assert resp.status_code == 200
   assert len(resp.json()["items"]) == 1
   assert resp.json()["total"] == 3

   # Unknown sort → 400
   resp = client.get("/api/live/users", params={"sort": "bogus"})
   assert resp.status_code == 400

   # SQL injection via sort
   resp = client.get("/api/live/users", params={"sort": "rss; DROP TABLE live_nodes; --"})
   assert resp.status_code == 400


# ---------------------------------------------------------------------------
# /api/live/events
# ---------------------------------------------------------------------------


def test_events_recent(migrated_db, tmp_path):
   url = migrated_db
   now = populate(url)
   # Write a few collection events directly
   with connect(url, autocommit=True) as conn:
      from node_monitor.database.maintenance import ensure_partition

      ensure_partition(conn, "collection_events", now)
      for i in range(3):
         conn.execute(
            "INSERT INTO collection_events (ts, system, node, event_type, detail)"
            " VALUES (%s, %s, %s, %s, %s::jsonb)",
            (
               now - timedelta(seconds=i * 10),
               SYSTEM,
               "n1.example",
               "ssh_error",
               '{"n": ' + str(i) + "}",
            ),
         )
   client = _client_for(url, tmp_path)
   resp = client.get("/api/live/events")
   assert resp.status_code == 200, resp.text
   data = resp.json()
   assert "items" in data and "total" in data
   assert data["total"] == 3
   assert len(data["items"]) == 3

   # Limit param
   resp = client.get("/api/live/events", params={"limit": 1})
   assert resp.status_code == 200
   assert len(resp.json()["items"]) == 1
   assert resp.json()["total"] == 3


# ---------------------------------------------------------------------------
# /api/live/processes offset pagination
# ---------------------------------------------------------------------------


def test_processes_offset_pagination(migrated_db, tmp_path):
   url = migrated_db
   populate(url)
   client = _client_for(url, tmp_path)
   all_ids = [r["pid"] for r in client.get("/api/live/processes").json()["items"]]
   paged = client.get("/api/live/processes", params={"limit": 2, "offset": 2}).json()
   assert paged["items"][0]["pid"] == all_ids[2]
   assert paged["total"] == 5
