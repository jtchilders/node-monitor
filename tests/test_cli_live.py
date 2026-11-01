import json
from datetime import timedelta

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


def run(*args):
   return CliRunner().invoke(cli, list(args))


@pytest.fixture
def live(migrated_db, tmp_path):
   now = populate(migrated_db)
   return migrated_db, now, _config(tmp_path, migrated_db)


def test_status_fresh_heartbeat_exit_0(live):
   url, now, cfg = live
   put_heartbeat(url, now)
   result = run("status", "--config", cfg)
   assert result.exit_code == 0, result.output
   for expected in ("collector-1", "9.9", "N1", "N2", "up"):
      assert expected in result.output
   assert "STALE" not in result.output


def test_status_stale_heartbeat_exit_1(live):
   url, now, cfg = live
   put_heartbeat(url, now, age_sec=400)
   result = run("status", "--config", cfg)
   assert result.exit_code == 1
   assert "STALE" in result.output


def test_status_missing_heartbeat_exit_1(live):
   _, _, cfg = live
   result = run("status", "--config", cfg)
   assert result.exit_code == 1
   assert "no heartbeat" in result.output


def test_status_json(live):
   url, now, cfg = live
   put_heartbeat(url, now)
   result = run("status", "--config", cfg, "--format", "json")
   assert result.exit_code == 0, result.output
   data = json.loads(result.output)
   assert data["collector"]["host"] == "collector-1" and data["collector"]["stale"] is False
   assert [n["label"] for n in data["nodes"]] == ["N1", "N2"]


def test_status_and_nodes_mark_stale_node(live):
   url, now, cfg = live
   put_heartbeat(url, now)
   with connect(url, autocommit=True) as conn:
      conn.execute(
         "UPDATE live_nodes SET updated_at = %s, session_state = 'up' WHERE node = 'n1.example'",
         (now - timedelta(minutes=10),),
      )
   for command in ("status", "nodes"):
      result = run(command, "--config", cfg, "--format", "json")
      data = json.loads(result.output)
      nodes = data["nodes"] if command == "status" else data
      states = {n["label"]: n["state"] for n in nodes}
      assert states == {"N1": "stale", "N2": "up"}, command
      table = run(command, "--config", cfg)
      assert "stale" in table.output


def test_nodes_table_shows_memory_and_counts(live):
   _, _, cfg = live
   result = run("nodes", "--config", cfg)
   assert result.exit_code == 0, result.output
   for expected in ("1.0 GiB", "4.0 GiB", "25.0%", "1.50"):
      assert expected in result.output


def test_nodes_csv(live):
   _, _, cfg = live
   result = run("nodes", "--config", cfg, "--format", "csv")
   lines = result.output.splitlines()
   assert lines[0].startswith("label,state")
   assert len(lines) == 3


def test_cli_unreachable_database_is_one_line_exit_1(tmp_path):
   cfg = _config(tmp_path, "postgresql://localhost:1/x")
   for command in ("status", "nodes"):
      result = run(command, "--config", cfg)
      assert result.exit_code == 1
      assert "database unreachable" in result.output
      assert "Traceback" not in result.output
      assert len(result.output.strip().splitlines()) == 1


def test_cli_missing_web_url_exit_1(tmp_path):
   cfg = _config(tmp_path, None)
   for command in ("status", "nodes"):
      result = run(command, "--config", cfg)
      assert result.exit_code == 1
      assert f"web.database_url is not set in {cfg}" in result.output
