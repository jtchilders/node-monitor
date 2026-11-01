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


def _json(*args):
   result = run(*args, "--format", "json")
   assert result.exit_code == 0, result.output
   return json.loads(result.output)


def test_top_default_sort_and_limit(live):
   _, _, cfg = live
   assert [r["pid"] for r in _json("top", "--config", cfg)] == [1, 3, 4, 2, 5]
   assert [r["pid"] for r in _json("top", "--config", cfg, "--limit", "2")] == [1, 3]
   assert len(_json("top", "--config", cfg, "--limit", "0")) == 5
   assert [r["pid"] for r in _json("top", "--config", cfg, "--sort", "rss", "--reverse")][0] == 5
   table = run("top", "--config", cfg)
   assert table.exit_code == 0, table.output
   assert "N1" in table.output and "root" not in table.output


def test_top_user_filter(live):
   _, _, cfg = live
   rows = _json("top", "--config", cfg, "-u", "bob")
   assert {r["username"] for r in rows} == {"bob"}
   assert _json("top", "--config", cfg, "-n", "N2", "-c", "editor")[0]["pid"] == 4


def test_top_json_keeps_full_command(live):
   _, _, cfg = live
   by_pid = {r["pid"]: r for r in _json("top", "--config", cfg)}
   assert by_pid[2]["cmdline"] == "x" * 80
   assert by_pid[3]["idle_for_sec"] == 100 and by_pid[3]["idle_for_is_lower_bound"] is True
   table = run("top", "--config", cfg).output
   assert "x" * 59 + "…" in table and "x" * 61 not in table
   assert "≥1m" in table
   csv_out = run("top", "--config", cfg, "--format", "csv").output
   assert "x" * 80 in csv_out and "idle_for_is_lower_bound" in csv_out.splitlines()[0]


def test_users_sorted_by_rss(live):
   _, _, cfg = live
   rows = _json("users", "--config", cfg)
   assert [r["username"] for r in rows] == ["alice", "bob", "carol"]
   assert rows[0]["processes"] == 2 and rows[0]["top_category"] == "ml"
   assert [r["username"] for r in _json("users", "--config", cfg, "--sort", "procs")][0] in (
      "alice",
      "bob",
   )
   assert "alice" in run("users", "--config", cfg).output


def test_user_shows_all_categories_and_summary(live):
   _, _, cfg = live
   result = run("user", "bob", "--config", cfg)
   assert result.exit_code == 0, result.output
   assert "2 processes" in result.output and "2 nodes" in result.output
   assert "1.5 GiB" in result.output
   rows = _json("user", "alice", "--config", cfg)
   assert {r["category"] for r in rows} == {"ml", "shell"}
   assert [r["pid"] for r in _json("user", "root", "--config", cfg)] == [6]


def test_user_unknown_exit_1(live):
   _, _, cfg = live
   result = run("user", "nobody", "--config", cfg)
   assert result.exit_code == 1
   assert result.output.strip().splitlines() == ["Error: no live processes for nobody"]


def test_idle_threshold_and_absent_only(live):
   _, _, cfg = live
   # pid 2: idle 2h, owner absent. pid 4: idle 1h but its owner is present.
   assert [r["pid"] for r in _json("idle", "--config", cfg)] == [2]
   assert [r["pid"] for r in _json("idle", "--config", cfg, "--min-idle", "3h")] == []
   table = run("idle", "--config", cfg).output
   assert "2h00m" in table and "absent" in table


def test_idle_any_presence(live):
   _, _, cfg = live
   rows = _json("idle", "--config", cfg, "--any-presence", "--min-idle", "1h")
   assert [r["pid"] for r in rows] == [4, 2]
   rows = _json("idle", "--config", cfg, "--any-presence", "--min-idle", "90s")
   assert [r["pid"] for r in rows] == [3, 4, 2]


@pytest.mark.parametrize("value", ["soon", "-5m", "1.5h", "", "10x"])
def test_min_idle_parsing_rejects_garbage(live, value):
   _, _, cfg = live
   result = run("idle", "--config", cfg, "--min-idle", value)
   assert result.exit_code == 2
   assert "min-idle" in result.output.lower()


@pytest.mark.parametrize(
   "value,seconds", [("30m", 1800), ("2h", 7200), ("1d", 86400), ("90s", 90), ("45", 45)]
)
def test_min_idle_accepts_units(live, value, seconds):
   _, _, cfg = live
   from node_monitor.cli.live import IdleDuration

   assert IdleDuration().convert(value, None, None) == seconds
