"""Task 5: history roll-up API tests."""

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


def _seed_node_metrics(url, start, days, interval="1m"):
   from node_monitor.database.connect import connect
   from node_monitor.database.maintenance import ensure_partition

   step = {"1m": timedelta(minutes=1), "1h": timedelta(hours=1), "1d": timedelta(days=1)}[interval]
   count = int(timedelta(days=days) / step)
   with connect(url, autocommit=True) as conn:
      cur = start
      for _ in range(count):
         ensure_partition(conn, "node_metrics_1m", cur)
         conn.execute(
            "INSERT INTO node_metrics_1m (system, node, minute, load1, mem_used_kb, procs_total, sample_count, expected_count) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",  # noqa: E501
            (SYSTEM, "n1.example", cur, 1.5, 1024, 10, 6, 6),
         )
         cur += step


def test_bucket_auto_select(migrated_db, tmp_path):
   url = migrated_db
   _seed_node_metrics(url, datetime(2026, 1, 1, tzinfo=timezone.utc), days=2)
   client = _client(url, tmp_path)
   # <=24h -> 1m (should not 400)
   resp = client.get(
      "/api/history/node/n1.example",
      params={"from": "2026-01-01T00:00:00Z", "to": "2026-01-01T12:00:00Z"},
   )
   assert resp.status_code == 200
   # <=30d -> 1h auto
   resp = client.get(
      "/api/history/node/n1.example",
      params={"from": "2026-01-01T00:00:00Z", "to": "2026-01-10T00:00:00Z"},
   )
   assert resp.status_code == 200
   # >30d -> 1d auto
   resp = client.get(
      "/api/history/node/n1.example",
      params={"from": "2026-01-01T00:00:00Z", "to": "2026-02-15T00:00:00Z"},
   )
   assert resp.status_code == 200


def test_raw_range_over_7d_rejected(migrated_db, tmp_path):
   url = migrated_db
   _seed_node_metrics(url, datetime(2026, 2, 1, tzinfo=timezone.utc), days=10)
   client = _client(url, tmp_path)
   resp = client.get(
      "/api/history/node/n1.example",
      params={"from": "2026-02-01T00:00:00Z", "to": "2026-02-10T00:00:00Z", "bucket": "1m"},
   )
   assert resp.status_code == 400


def test_utc_boundaries_respected(migrated_db, tmp_path):
   url = migrated_db
   base = datetime(2026, 3, 1, tzinfo=timezone.utc)
   _seed_node_metrics(url, base, days=1)
   client = _client(url, tmp_path)
   resp = client.get(
      "/api/history/node/n1.example",
      params={
         "from": base.isoformat(),
         "to": (base + timedelta(hours=2)).isoformat(),
         "bucket": "1m",
      },
   )
   assert resp.status_code == 200
   data = resp.json()
   # All returned minutes should be >= from and < to in UTC
   for item in data["items"]:
      ts = datetime.fromisoformat(item["minute"])
      assert ts.tzinfo is not None
      assert ts >= base
      assert ts < base + timedelta(hours=2)


def test_hourly_avg_matches_manual(migrated_db, tmp_path):
   url = migrated_db
   from node_monitor.database.connect import connect
   from node_monitor.database.maintenance import ensure_partition

   base = datetime(2026, 4, 1, tzinfo=timezone.utc)
   with connect(url, autocommit=True) as conn:
      for i in range(6):
         cur = base + timedelta(minutes=i * 10)
         ensure_partition(conn, "node_metrics_1m", cur)
         conn.execute(
            "INSERT INTO node_metrics_1m (system, node, minute, load1, mem_used_kb, sample_count, expected_count) VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",  # noqa: E501
            (SYSTEM, "n1.example", cur, float(i), 100 + i, 6, 6),
         )
   client = _client(url, tmp_path)
   resp = client.get(
      "/api/history/node/n1.example",
      params={
         "from": base.isoformat(),
         "to": (base + timedelta(hours=1)).isoformat(),
         "bucket": "1h",
      },
   )
   assert resp.status_code == 200
   data = resp.json()
   assert len(data["items"]) == 1
   # avg of 0..5 = 2.5
   assert abs(data["items"][0]["load1"] - 2.5) < 0.01


def test_two_year_range_capped(migrated_db, tmp_path):
   url = migrated_db
   # Seed 2 years of daily points (730)
   _seed_node_metrics(url, datetime(2024, 1, 1, tzinfo=timezone.utc), days=730, interval="1d")
   client = _client(url, tmp_path)
   resp = client.get(
      "/api/history/node/n1.example",
      params={"from": "2024-01-01T00:00:00Z", "to": "2026-01-01T00:00:00Z"},
   )
   assert resp.status_code == 200
   data = resp.json()
   assert len(data["items"]) <= 730
   assert len(data["items"]) > 0


def test_user_history_404_if_unknown(migrated_db, tmp_path):
   client = _client(migrated_db, tmp_path)
   resp = client.get(
      "/api/history/user/nonexistent_xyz",
      params={"from": "2026-01-01T00:00:00Z", "to": "2026-01-02T00:00:00Z"},
   )
   assert resp.status_code == 404


def test_from_must_be_before_to(migrated_db, tmp_path):
   client = _client(migrated_db, tmp_path)
   resp = client.get(
      "/api/history/node/n1.example",
      params={"from": "2026-01-02T00:00:00Z", "to": "2026-01-01T00:00:00Z"},
   )
   assert resp.status_code == 400
