"""Analytics API tests: category-mix, agent-adoption, dormancy, leaderboard, coverage."""

from datetime import datetime, timedelta, timezone

import yaml

from tests.database import conftest as pg
from tests.database.live_fixture import SYSTEM
from tests.test_config import MINIMAL

migrated_db = pg.migrated_db
pg_admin_url = pg.pg_admin_url


def _client(url, tmp_path):
   from fastapi.testclient import TestClient

   from node_monitor.config import load_config
   from node_monitor.web.app import create_app

   base = tmp_path / "config.yaml"
   base.write_text(
      yaml.safe_dump({**MINIMAL, "working_dir": str(tmp_path / "wd"), "web": {"database_url": url}})
   )
   cfg = load_config(str(base))
   return TestClient(create_app(cfg))


def _seed_process_samples(url, base, hours=24, step_min=60):
   """Insert process_samples rows: 2 categories x 1 user, hourly points."""
   from node_monitor.database.connect import connect
   from node_monitor.database.maintenance import ensure_daily_partition

   step = timedelta(minutes=step_min)
   count = int(timedelta(hours=hours) / step)
   with connect(url, autocommit=True) as conn:
      cur = base
      for i in range(count):
         ensure_daily_partition(conn, "process_samples", cur)
         for cat in ("ai_agent", "python"):
            conn.execute(
               "INSERT INTO process_samples (system, node, ts, pid, start_time, username,"
               " category, activity, rss_kb, cpu_seconds, cpu_state, idle_for_sec,"
               " owner_presence, hourly) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
               " ON CONFLICT DO NOTHING",
               (
                  SYSTEM,
                  "n1.example",
                  cur,
                  100 + (0 if cat == "ai_agent" else 1),
                  1000 + i * 10 + (0 if cat == "ai_agent" else 1),
                  "alice" if cat == "ai_agent" else "bob",
                  cat,
                  "claude-code" if cat == "ai_agent" else "python-generic",
                  1024 * (100 + i),
                  float(i * 10),
                  "busy",
                  None,
                  "interactive",
                  cur.minute == 0,
               ),
            )
         cur += step


def _seed_dormant_samples(url, base, count=10):
   from node_monitor.database.connect import connect
   from node_monitor.database.maintenance import ensure_daily_partition

   with connect(url, autocommit=True) as conn:
      for i in range(count):
         ts = base + timedelta(hours=i)
         ensure_daily_partition(conn, "process_samples", ts)
         # dormant: idle>1h + absent
         conn.execute(
            "INSERT INTO process_samples (system, node, ts, pid, start_time, username,"
            " category, rss_kb, cpu_seconds, cpu_state, idle_for_sec, owner_presence, hourly)"
            " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (
               SYSTEM,
               "n1.example",
               ts,
               200 + i,
               2000 + i,
               "alice",
               "python",
               2048,
               5.0,
               "idle",
               7200,
               "absent",
               False,
            ),
         )
         # non-dormant: busy
         conn.execute(
            "INSERT INTO process_samples (system, node, ts, pid, start_time, username,"
            " category, rss_kb, cpu_seconds, cpu_state, idle_for_sec, owner_presence, hourly)"
            " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (
               SYSTEM,
               "n1.example",
               ts,
               300 + i,
               3000 + i,
               "bob",
               "shell",
               512,
               1.0,
               "busy",
               None,
               "interactive",
               False,
            ),
         )


def _seed_node_metrics(url, start, days, interval="1d"):
   from node_monitor.database.connect import connect
   from node_monitor.database.maintenance import ensure_partition

   step = {"1m": timedelta(minutes=1), "1h": timedelta(hours=1), "1d": timedelta(days=1)}[interval]
   count = int(timedelta(days=days) / step)
   with connect(url, autocommit=True) as conn:
      cur = start
      for _ in range(count):
         ensure_partition(conn, "node_metrics_1m", cur)
         conn.execute(
            "INSERT INTO node_metrics_1m (system, node, minute, load1, mem_used_kb,"
            " procs_total, sample_count, expected_count) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
            " ON CONFLICT DO NOTHING",
            (SYSTEM, "n1.example", cur, 1.5, 1024, 10, 6, 6),
         )
         cur += step


# ---------------------------------------------------------------------------
# 2y caps to <=730
# ---------------------------------------------------------------------------


def test_two_year_category_mix_capped_to_730(migrated_db, tmp_path):
   url = migrated_db
   base = datetime(2024, 1, 1, tzinfo=timezone.utc)
   _seed_process_samples(url, base, hours=730 * 24, step_min=60)
   client = _client(url, tmp_path)
   resp = client.get(
      "/api/analytics/category-mix",
      params={"from": "2024-01-01T00:00:00Z", "to": "2026-01-01T00:00:00Z"},
   )
   assert resp.status_code == 200, resp.text
   data = resp.json()
   assert len(data["items"]) <= 730, f"got {len(data['items'])} > 730"
   assert len(data["items"]) > 0


def test_two_year_dormancy_capped_to_730(migrated_db, tmp_path):
   url = migrated_db
   base = datetime(2024, 1, 1, tzinfo=timezone.utc)
   _seed_dormant_samples(url, base, count=730)
   client = _client(url, tmp_path)
   resp = client.get(
      "/api/analytics/dormancy",
      params={"from": "2024-01-01T00:00:00Z", "to": "2026-01-01T00:00:00Z"},
   )
   assert resp.status_code == 200, resp.text
   assert len(resp.json()["items"]) <= 730


# ---------------------------------------------------------------------------
# Dormancy sum matches manual SQL
# ---------------------------------------------------------------------------


def test_dormancy_sum_matches_manual_sql(migrated_db, tmp_path):
   url = migrated_db
   base = datetime(2026, 2, 1, tzinfo=timezone.utc)
   _seed_dormant_samples(url, base, count=24)
   client = _client(url, tmp_path)
   resp = client.get(
      "/api/analytics/dormancy",
      params={"from": "2026-02-01T00:00:00Z", "to": "2026-02-02T00:00:00Z"},
   )
   assert resp.status_code == 200, resp.text
   data = resp.json()
   # Manual SQL
   from node_monitor.database.connect import connect as db_connect

   with db_connect(url, autocommit=True) as conn:
      with conn.cursor() as cur:
         cur.execute(
            "SELECT coalesce(sum(rss_kb),0) FROM process_samples"
            " WHERE system=%s AND ts >= %s AND ts < %s"
            " AND ((cpu_state='idle' AND coalesce(idle_for_sec,0) > 3600)"  # noqa: E501
            " OR owner_presence='absent')",
            (SYSTEM, base, base + timedelta(days=1)),
         )
         expected = int(cur.fetchone()[0])
   api_total = sum(cat["rss_kb"] for item in data["items"] for cat in item["categories"].values())
   assert api_total == expected, f"api {api_total} != manual {expected}"


# ---------------------------------------------------------------------------
# Leaderboard order descending
# ---------------------------------------------------------------------------


def test_leaderboard_order(migrated_db, tmp_path):
   url = migrated_db
   base = datetime(2026, 3, 1, tzinfo=timezone.utc)
   _seed_process_samples(url, base, hours=24, step_min=60)
   client = _client(url, tmp_path)
   for metric in ("cpu", "mem"):
      resp = client.get(
         "/api/analytics/leaderboard",
         params={"metric": metric, "from": "2026-03-01T00:00:00Z", "to": "2026-03-02T00:00:00Z"},
      )
      assert resp.status_code == 200, resp.text
      items = resp.json()["items"]
      assert len(items) >= 2, items
      values = [x["value"] for x in items]
      assert values == sorted(values, reverse=True), f"not descending for {metric}: {values}"


def test_leaderboard_bad_metric_400(migrated_db, tmp_path):
   client = _client(migrated_db, tmp_path)
   resp = client.get("/api/analytics/leaderboard", params={"metric": "bogus"})
   assert resp.status_code == 400


def test_leaderboard_bad_bucket_400(migrated_db, tmp_path):
   client = _client(migrated_db, tmp_path)
   resp = client.get("/api/analytics/category-mix", params={"bucket": "nope"})
   assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Coverage gap detection
# ---------------------------------------------------------------------------


def test_coverage_gap_detection(migrated_db, tmp_path):
   url = migrated_db
   base = datetime(2026, 4, 1, tzinfo=timezone.utc)
   _seed_node_metrics(url, base, days=3, interval="1d")
   # Insert a gap event
   from node_monitor.database.connect import connect as db_connect
   from node_monitor.database.maintenance import ensure_partition

   with db_connect(url, autocommit=True) as conn:
      ensure_partition(conn, "collection_events", base + timedelta(days=1))
      conn.execute(
         "INSERT INTO collection_events (ts, system, node, event_type, detail)"  # noqa: E501
         " VALUES (%s,%s,%s,%s,%s::jsonb)",
         (base + timedelta(days=1), SYSTEM, "n1.example", "gap", '{"reason":"timeout"}'),
      )
   client = _client(url, tmp_path)
   resp = client.get(
      "/api/analytics/coverage",
      params={"from": "2026-04-01T00:00:00Z", "to": "2026-04-04T00:00:00Z"},
   )
   assert resp.status_code == 200, resp.text
   data = resp.json()
   assert "gaps" in data
   assert "uptime_ratio" in data
   assert len(data["gaps"]) >= 1
   assert data["gaps"][0]["event_type"] == "gap"


def test_agent_adoption_basic(migrated_db, tmp_path):
   url = migrated_db
   base = datetime(2026, 5, 1, tzinfo=timezone.utc)
   _seed_process_samples(url, base, hours=48, step_min=60)
   client = _client(url, tmp_path)
   resp = client.get(
      "/api/analytics/agent-adoption",
      params={"from": "2026-05-01T00:00:00Z", "to": "2026-05-03T00:00:00Z"},
   )
   assert resp.status_code == 200, resp.text
   data = resp.json()
   assert "items" in data
   assert "tag_share" in data
