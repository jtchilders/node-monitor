"""Tests for Task 7 CLI: history, commands search, reclassify."""

import json

import yaml
from click.testing import CliRunner

from node_monitor.cli.main import cli
from tests.database import conftest as pg
from tests.test_config import MINIMAL

migrated_db = pg.migrated_db
pg_admin_url = pg.pg_admin_url
pg_db = pg.pg_db


def _config(tmp_path, *, web_url=None, db_url=None):
   path = tmp_path / "config.yaml"
   data = dict(MINIMAL)
   data["working_dir"] = str(tmp_path / "wd")
   if web_url is not None:
      data["web"] = {"database_url": web_url}
   elif web_url is None and "web" in data:
      data.pop("web", None)
   # explicit empty web means missing URL
   if web_url is None:
      data["web"] = {}
   if db_url is not None:
      data["database"] = {"url": db_url}
   path.write_text(yaml.safe_dump(data))
   return str(path)


def _config_no_web(tmp_path):
   """Config with web.database_url missing."""
   path = tmp_path / "config.yaml"
   data = dict(MINIMAL)
   data["working_dir"] = str(tmp_path / "wd")
   data["web"] = {}
   if "database" in data:
      data.pop("database")
   path.write_text(yaml.safe_dump(data))
   return str(path)


def run(*args):
   return CliRunner().invoke(cli, list(args))


# ---------------------------------------------------------------------------
# history
# ---------------------------------------------------------------------------


def test_history_requires_user_or_node(tmp_path):
   cfg = _config_no_web(tmp_path)
   result = run("history", "--config", cfg)
   assert result.exit_code != 0


def test_history_missing_web_url_is_one_line(tmp_path):
   cfg = _config_no_web(tmp_path)
   result = run("history", "--user", "alice", "--config", cfg)
   assert result.exit_code == 1
   assert "web.database_url" in result.output
   assert "Traceback" not in result.output
   assert len(result.output.strip().splitlines()) == 1


def test_history_outputs_iso_timestamps_json(migrated_db, tmp_path):
   from datetime import datetime, timedelta, timezone

   from node_monitor.database.connect import connect

   url = migrated_db
   # Ensure migrated
   now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
   interval_start = now - timedelta(hours=2)
   # Seed user_usage_5m
   with connect(url, autocommit=True) as conn:
      # ensure partition exists for this interval
      from node_monitor.database.maintenance import ensure_partition

      ensure_partition(conn, "user_usage_5m", interval_start)
      conn.execute(
         "INSERT INTO user_usage_5m"
         " (system, node, username, interval_start, cpu_seconds, rss_kb_avg,"
         "  process_count_avg, sample_count, expected_count)"
         " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
         ("polaris", "n1", "alice", interval_start, 10.0, 1024, 2.0, 5, 5),
      )
   cfg_path = tmp_path / "config.yaml"
   cfg_path.write_text(
      yaml.safe_dump(
         {
            **MINIMAL,
            "working_dir": str(tmp_path / "wd"),
            "web": {"database_url": url},
         }
      )
   )
   cfg = str(cfg_path)
   result = run(
      "history",
      "--user",
      "alice",
      "--from",
      (interval_start - timedelta(minutes=5)).isoformat(),
      "--to",
      (interval_start + timedelta(hours=1)).isoformat(),
      "--format",
      "json",
      "--config",
      cfg,
   )
   assert result.exit_code == 0, result.output
   data = json.loads(result.output)
   # Should have at least one row with ISO timestamp in minute/interval_start
   assert len(data) >= 1
   for row in data:
      # every row has a datetime-like field that parses as ISO8601
      ts_val = row.get("minute") or row.get("interval_start") or row.get("ts") or row.get("bucket")
      assert ts_val is not None, f"no timestamp field in {row}"
      # must be ISO8601 parseable
      datetime.fromisoformat(ts_val.replace("Z", "+00:00"))


def test_history_node_outputs_iso_timestamps_json(migrated_db, tmp_path):
   from datetime import datetime, timedelta, timezone

   from node_monitor.database.connect import connect

   url = migrated_db
   now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
   minute = now - timedelta(hours=1)
   with connect(url, autocommit=True) as conn:
      from node_monitor.database.maintenance import ensure_partition

      ensure_partition(conn, "node_metrics_1m", minute)
      conn.execute(
         "INSERT INTO node_metrics_1m"
         " (system, node, minute, load1, mem_used_kb, procs_total, sample_count, expected_count)"
         " VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
         ("polaris", "n1.example", minute, 1.5, 1024, 10, 5, 5),
      )
   cfg_path = tmp_path / "config.yaml"
   cfg_path.write_text(
      yaml.safe_dump({**MINIMAL, "working_dir": str(tmp_path / "wd"), "web": {"database_url": url}})
   )
   cfg = str(cfg_path)
   result = run(
      "history",
      "--node",
      "n1.example",
      "--from",
      (minute - timedelta(minutes=5)).isoformat(),
      "--to",
      (minute + timedelta(hours=1)).isoformat(),
      "--format",
      "json",
      "--config",
      cfg,
   )
   assert result.exit_code == 0, result.output
   data = json.loads(result.output)
   assert len(data) >= 1
   for row in data:
      ts_val = row.get("minute") or row.get("interval_start") or row.get("ts") or row.get("bucket")
      assert ts_val is not None, f"no timestamp in {row}"
      datetime.fromisoformat(ts_val.replace("Z", "+00:00"))


def test_history_csv_header(migrated_db, tmp_path):
   from datetime import datetime, timedelta, timezone

   from node_monitor.database.connect import connect

   url = migrated_db
   now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
   interval_start = now - timedelta(hours=2)
   with connect(url, autocommit=True) as conn:
      from node_monitor.database.maintenance import ensure_partition

      ensure_partition(conn, "user_usage_5m", interval_start)
      conn.execute(
         "INSERT INTO user_usage_5m"
         " (system, node, username, interval_start, cpu_seconds, rss_kb_avg,"
         "  process_count_avg, sample_count, expected_count)"
         " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
         ("polaris", "n1", "alice", interval_start, 10.0, 1024, 2.0, 5, 5),
      )
   cfg_path = tmp_path / "config.yaml"
   cfg_path.write_text(
      yaml.safe_dump({**MINIMAL, "working_dir": str(tmp_path / "wd"), "web": {"database_url": url}})
   )
   result = run(
      "history",
      "--user",
      "alice",
      "--from",
      (interval_start - timedelta(minutes=5)).isoformat(),
      "--to",
      (interval_start + timedelta(hours=1)).isoformat(),
      "--format",
      "csv",
      "--config",
      str(cfg_path),
   )
   assert result.exit_code == 0, result.output
   assert (
      "minute" in result.output.splitlines()[0] or "interval_start" in result.output.splitlines()[0]
   )


# ---------------------------------------------------------------------------
# commands search
# ---------------------------------------------------------------------------


def test_commands_search_bad_regex_exits_2(tmp_path):
   cfg_path = tmp_path / "config.yaml"
   # need a web url even if bad — we validate regex before DB
   cfg_path.write_text(
      yaml.safe_dump(
         {
            **MINIMAL,
            "working_dir": str(tmp_path / "wd"),
            "web": {"database_url": "postgresql://localhost:1/x"},
         }
      )
   )
   result = run("commands", "search", "[invalid", "--regex", "--config", str(cfg_path))
   assert result.exit_code == 2, result.output
   assert "Traceback" not in result.output


def test_commands_search_requires_pattern(tmp_path):
   cfg_path = tmp_path / "config.yaml"
   cfg_path.write_text(
      yaml.safe_dump(
         {
            **MINIMAL,
            "working_dir": str(tmp_path / "wd"),
            "web": {"database_url": "postgresql://localhost:1/x"},
         }
      )
   )
   result = run("commands", "search", "--config", str(cfg_path))
   assert result.exit_code != 0


def test_commands_search_json_with_db(migrated_db, tmp_path):
   from datetime import datetime, timedelta, timezone

   from node_monitor.database.connect import connect

   url = migrated_db
   now = datetime.now(timezone.utc)
   with connect(url, autocommit=True) as conn:
      conn.execute(
         "INSERT INTO command_lines"
         " (system, username, cmdline_hash, cmdline, activity, category, tags,"
         "  first_seen, last_seen, sample_count, nodes)"
         " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
         (
            "polaris",
            "alice",
            "h-cli-1",
            "python train.py --lr 0.01",
            "python-generic",
            "python",
            [],
            now - timedelta(days=1),
            now,
            5,
            ["n1"],
         ),
      )
   cfg_path = tmp_path / "config.yaml"
   cfg_path.write_text(
      yaml.safe_dump({**MINIMAL, "working_dir": str(tmp_path / "wd"), "web": {"database_url": url}})
   )
   result = run("commands", "search", "train.py", "--format", "json", "--config", str(cfg_path))
   assert result.exit_code == 0, result.output
   body = json.loads(result.output)
   assert body["total"] >= 1
   assert any("train.py" in it["cmdline"] for it in body["items"])


def test_commands_search_missing_web_url_is_one_line(tmp_path):
   cfg = _config_no_web(tmp_path)
   result = run("commands", "search", "hello", "--config", cfg)
   assert result.exit_code == 1
   assert "web.database_url" in result.output
   assert "Traceback" not in result.output


# ---------------------------------------------------------------------------
# reclassify
# ---------------------------------------------------------------------------


def test_reclassify_missing_database_url_is_one_line(tmp_path):
   cfg = _config_no_web(tmp_path)
   result = run("reclassify", "--dry-run", "--config", cfg)
   assert result.exit_code == 1
   assert "database.url" in result.output
   assert "Traceback" not in result.output


def test_reclassify_dry_run_reports_delta_without_writing(migrated_db, tmp_path):
   from datetime import datetime, timedelta, timezone

   from node_monitor.database.connect import connect

   url = migrated_db
   now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
   # Seed a process_samples row classified as 'other' but whose cmdline should be ai_agent today
   # and a command_lines row with stale category.
   with connect(url, autocommit=True) as conn:
      from node_monitor.database.maintenance import ensure_daily_partition

      ensure_daily_partition(conn, "process_samples", now)
      conn.execute(
         "INSERT INTO process_samples"
         " (system, node, ts, pid, start_time, username, uid, comm, exe, cmdline, cmdline_hash,"
         "  category, activity, tags, state, threads, cpu_seconds, rss_kb, hourly)"
         " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
         (
            "polaris",
            "n1",
            now,
            9991,
            100,
            "alice",
            1000,
            "python",
            "/usr/bin/python",
            "python -m claude_code.cli --help",
            "h-old-1",
            "other",
            "python-generic",
            [],
            "S",
            1,
            1.0,
            1024,
            False,
         ),
      )
      conn.execute(
         "INSERT INTO command_lines"
         " (system, username, cmdline_hash, cmdline, exe, activity, category, tags,"
         "  first_seen, last_seen, sample_count, nodes)"
         " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
         (
            "polaris",
            "alice",
            "h-old-1",
            "python -m claude_code.cli --help",
            "/usr/bin/python",
            "python-generic",
            "other",
            [],
            now - timedelta(days=1),
            now,
            3,
            ["n1"],
         ),
      )

   cfg_path = tmp_path / "config.yaml"
   cfg_path.write_text(
      yaml.safe_dump({**MINIMAL, "working_dir": str(tmp_path / "wd"), "database": {"url": url}})
   )
   cfg = str(cfg_path)

   result = run("reclassify", "--dry-run", "--config", cfg)
   assert result.exit_code == 0, result.output
   assert "changed:" in result.output.lower()
   # dry-run must not have written
   with connect(url, autocommit=True) as conn:
      row = conn.execute("SELECT category FROM process_samples WHERE pid=9991").fetchone()
      assert row[0] == "other", "dry-run must not write"
      row2 = conn.execute(
         "SELECT category FROM command_lines WHERE cmdline_hash='h-old-1'"
      ).fetchone()
      assert row2[0] == "other"


def test_reclassify_apply_writes(migrated_db, tmp_path):
   from datetime import datetime, timedelta, timezone

   from node_monitor.database.connect import connect

   url = migrated_db
   now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
   with connect(url, autocommit=True) as conn:
      from node_monitor.database.maintenance import ensure_daily_partition

      ensure_daily_partition(conn, "process_samples", now)
      conn.execute(
         "INSERT INTO process_samples"
         " (system, node, ts, pid, start_time, username, uid, comm, exe, cmdline, cmdline_hash,"
         "  category, activity, tags, state, threads, cpu_seconds, rss_kb, hourly)"
         " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
         (
            "polaris",
            "n1",
            now,
            9992,
            101,
            "bob",
            1000,
            "python",
            "/usr/bin/python",
            "python -m claude_code.cli --help",
            "h-old-2",
            "other",
            "python-generic",
            [],
            "S",
            1,
            1.0,
            1024,
            False,
         ),
      )
      conn.execute(
         "INSERT INTO command_lines"
         " (system, username, cmdline_hash, cmdline, exe, activity, category, tags,"
         "  first_seen, last_seen, sample_count, nodes)"
         " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
         (
            "polaris",
            "bob",
            "h-old-2",
            "python -m claude_code.cli --help",
            "/usr/bin/python",
            "python-generic",
            "other",
            [],
            now - timedelta(days=1),
            now,
            2,
            ["n1"],
         ),
      )

   cfg_path = tmp_path / "config.yaml"
   cfg_path.write_text(
      yaml.safe_dump({**MINIMAL, "working_dir": str(tmp_path / "wd"), "database": {"url": url}})
   )
   cfg = str(cfg_path)

   result = run("reclassify", "--apply", "--config", cfg)
   assert result.exit_code == 0, result.output
   assert "changed:" in result.output.lower()
   with connect(url, autocommit=True) as conn:
      row = conn.execute("SELECT category FROM process_samples WHERE pid=9992").fetchone()
      # reclassified — should now be ai_agent (contains claude_code)
      assert row[0] == "ai_agent", f"expected ai_agent after apply, got {row[0]}"
      row2 = conn.execute(
         "SELECT category FROM command_lines WHERE cmdline_hash='h-old-2'"
      ).fetchone()
      assert row2[0] == "ai_agent"


def test_reclassify_dry_run_is_default(migrated_db, tmp_path):
   from datetime import datetime, timezone

   from node_monitor.database.connect import connect

   url = migrated_db
   now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
   with connect(url, autocommit=True) as conn:
      from node_monitor.database.maintenance import ensure_daily_partition

      ensure_daily_partition(conn, "process_samples", now)
      conn.execute(
         "INSERT INTO process_samples"
         " (system, node, ts, pid, start_time, username, uid, comm, exe, cmdline, cmdline_hash,"
         "  category, activity, tags, state, threads, cpu_seconds, rss_kb, hourly)"
         " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
         (
            "polaris",
            "n1",
            now,
            9993,
            102,
            "carol",
            1000,
            "sleep",
            "/bin/sleep",
            "sleep 100",
            "h-old-3",
            "other",
            "sleep",
            [],
            "S",
            1,
            0.0,
            512,
            False,
         ),
      )
   cfg_path = tmp_path / "config.yaml"
   cfg_path.write_text(
      yaml.safe_dump({**MINIMAL, "working_dir": str(tmp_path / "wd"), "database": {"url": url}})
   )
   cfg = str(cfg_path)
   result = run("reclassify", "--config", cfg)
   assert result.exit_code == 0, result.output
   assert "changed:" in result.output.lower()
   with connect(url, autocommit=True) as conn:
      row = conn.execute("SELECT category FROM process_samples WHERE pid=9993").fetchone()
      # dry-run default: DB unchanged
      assert row[0] == "other", "default must be dry-run (no write)"
