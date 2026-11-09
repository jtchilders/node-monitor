"""Task 1: /api/health and vendor pin tests."""

import re
import unittest.mock as mock
from datetime import timedelta
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from node_monitor.cli.main import cli
from node_monitor.database.connect import connect
from tests.database import conftest as pg
from tests.database.live_fixture import populate, put_heartbeat
from tests.test_config import MINIMAL

pg_admin_url = pg.pg_admin_url
pg_db = pg.pg_db
migrated_db = pg.migrated_db


@pytest.fixture(autouse=True)
def _no_env(monkeypatch):
   for var in ("NODE_MONITOR_DB_URL", "NODE_MONITOR_WEB_DB_URL", "NODE_MONITOR_CONFIG"):
      monkeypatch.delenv(var, raising=False)


def _config(tmp_path, url):
   path = tmp_path / "config.yaml"
   web = {"database_url": url} if url else {}
   path.write_text(yaml.safe_dump({**MINIMAL, "working_dir": str(tmp_path / "wd"), "web": web}))
   return str(path)


def _client_for(url, tmp_path):
   """FastAPI TestClient backed by `url`, without touching real config files."""
   from fastapi.testclient import TestClient

   from node_monitor.config import load_config
   from node_monitor.web.app import create_app

   # Minimal config that points only the web reader at `url`.
   base = tmp_path / "config.yaml"
   base.write_text(
      yaml.safe_dump({**MINIMAL, "working_dir": str(tmp_path / "wd"), "web": {"database_url": url}})
   )
   config = load_config(str(base))
   app = create_app(config)
   return TestClient(app)


# ---------------------------------------------------------------------------
# /api/health
# ---------------------------------------------------------------------------


def test_health_ok_and_stale(migrated_db, tmp_path):
   url = migrated_db
   now = populate(url)
   put_heartbeat(url, now, age_sec=5)
   client = _client_for(url, tmp_path)
   resp = client.get("/api/health")
   assert resp.status_code == 200, resp.text
   data = resp.json()
   assert data["ok"] is True
   assert data["db_ok"] is True
   assert isinstance(data["heartbeat_age_s"], int)
   assert data["heartbeat_age_s"] < 10
   assert len(data["nodes"]) == 2
   for node in data["nodes"]:
      assert "label" in node and "state" in node and "updated_age_s" in node
   # All nodes fresh: none stale
   assert {n["state"] for n in data["nodes"]} == {"up"}
   for n in data["nodes"]:
      assert n["updated_age_s"] < 10

   # Make heartbeat stale (older than 180 s) -> ok becomes False
   put_heartbeat(url, now, age_sec=400)
   client2 = _client_for(url, tmp_path)
   resp2 = client2.get("/api/health")
   assert resp2.status_code == 200, resp2.text
   data2 = resp2.json()
   assert data2["ok"] is False
   assert data2["heartbeat_age_s"] >= 300
   assert data2["db_ok"] is True

   # Make one node stale (updated_at older than 120 s) -> that node's state is "stale"
   with connect(url, autocommit=True) as conn:
      conn.execute(
         "UPDATE live_nodes SET updated_at = %s WHERE node = 'n1.example'",
         (now - timedelta(seconds=300),),
      )
   # Heartbeat still stale, so ok stays False, but verify node staleness
   client3 = _client_for(url, tmp_path)
   data3 = client3.get("/api/health").json()
   states = {n["label"]: n["state"] for n in data3["nodes"]}
   assert states["N1"] == "stale"
   assert data3["nodes"][0]["updated_age_s"] >= 200 or data3["nodes"][1]["updated_age_s"] >= 200

   # Fresh heartbeat again, one node still stale -> node shows stale but ok is True
   # (ok reflects heartbeat health, nodes are informational)
   put_heartbeat(url, now, age_sec=5)
   client4 = _client_for(url, tmp_path)
   data4 = client4.get("/api/health").json()
   assert data4["ok"] is True
   states4 = {n["label"]: n["state"] for n in data4["nodes"]}
   assert states4["N1"] == "stale"


def test_health_no_heartbeat_is_not_ok(migrated_db, tmp_path):
   url = migrated_db
   populate(migrated_db)
   # No heartbeat row at all
   client = _client_for(url, tmp_path)
   resp = client.get("/api/health")
   assert resp.status_code == 200, resp.text
   data = resp.json()
   assert data["ok"] is False
   assert data["heartbeat_age_s"] is None
   assert data["db_ok"] is True
   # Nodes still returned
   assert len(data["nodes"]) == 2


def test_health_db_unreachable_still_200(tmp_path):
   # Use an unreachable URL: the endpoint should return db_ok=False, ok=False, not 500.
   bad_url = "postgresql://localhost:1/nonexistent"
   client = _client_for(bad_url, tmp_path)
   resp = client.get("/api/health")
   assert resp.status_code == 200, resp.text
   data = resp.json()
   assert data["db_ok"] is False
   assert data["ok"] is False
   assert data["heartbeat_age_s"] is None


# ---------------------------------------------------------------------------
# CLI `node-monitor web`
# ---------------------------------------------------------------------------


def test_web_missing_url_exits_1_no_traceback(tmp_path):
   cfg = _config(tmp_path, None)
   result = CliRunner().invoke(cli, ["web", "--config", cfg])
   assert result.exit_code == 1
   assert "web.database_url is not set" in result.output
   assert "Traceback" not in result.output
   # Single line (plus the Click "Error:" prefix); no multi-line traceback.
   assert len(result.output.strip().splitlines()) == 1


def test_web_unreachable_db_still_starts(tmp_path):
   # An unreachable DB should not block `web` startup: the server handles DB errors at
   # request time. The CLI only validates that web.database_url is present.
   cfg = _config(tmp_path, "postgresql://localhost:1/x")
   # We patch uvicorn.run so the test does not actually start a server.
   with mock.patch("uvicorn.run") as m:
      result = CliRunner().invoke(cli, ["web", "--config", cfg, "--port", "19999"])
      assert result.exit_code == 0, result.output
      assert m.called
      kwargs = m.call_args
      # uvicorn.run(app, host=..., port=...)
      assert kwargs is not None


# ---------------------------------------------------------------------------
# Vendor files
# ---------------------------------------------------------------------------


def test_vendor_files_exist_and_are_pinned():
   vendor = Path("node_monitor/web/static/vendor")
   assert vendor.is_dir(), f"{vendor} missing"
   vue = vendor / "vue.global.prod.js"
   chart = vendor / "chart.umd.min.js"
   assert vue.is_file(), f"{vue} missing"
   assert chart.is_file(), f"{chart} missing"
   # Pinned versions: vue 3.4.x and chart.js 4.4.x must appear in file headers/comments.
   vue_text = vue.read_text(errors="replace")
   chart_text = chart.read_text(errors="replace")
   assert re.search(r"3\.4\.\d+", vue_text), "vue file does not contain a 3.4.x version string"
   assert re.search(r"4\.4\.\d+", chart_text), (
      "chart.js file does not contain a 4.4.x version string"
   )
   # Files are non-trivial (not empty placeholders)
   assert vue.stat().st_size > 10_000, f"vue file too small: {vue.stat().st_size}"
   assert chart.stat().st_size > 10_000, f"chart.js file too small: {chart.stat().st_size}"
