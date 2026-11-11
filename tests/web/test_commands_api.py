"""Command search API tests: substring escapes, regex, filters, paging, XSS."""

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


def _seed_cmdlines(url):
   from datetime import datetime, timedelta, timezone

   from node_monitor.database.connect import connect

   now = datetime.now(timezone.utc)
   rows = [
      {
         "username": "alice",
         "cmdline": "python train.py --lr 0.01",
         "cmdline_hash": "h1",
         "activity": "python-generic",
         "category": "python",
         "tags": ["python.debugger"],
      },
      {
         "username": "bob",
         "cmdline": "claude --dangerously-skip-permissions do thing",
         "cmdline_hash": "h2",
         "activity": "claude-code",
         "category": "ai_agent",
         "tags": ["agent.skip_permissions"],
      },
      {
         "username": "carol",
         "cmdline": "ls %_special_% pattern",
         "cmdline_hash": "h3",
         "activity": "shell",
         "category": "shell",
         "tags": [],
      },
      {
         "username": "alice",
         "cmdline": "<script>alert(1)</script>",
         "cmdline_hash": "h4",
         "activity": "shell",
         "category": "shell",
         "tags": [],
      },
   ]
   with connect(url, autocommit=True) as conn:
      for r in rows:
         conn.execute(
            "INSERT INTO command_lines"
            " (system, username, cmdline_hash, cmdline, activity, category, tags,"
            "  first_seen, last_seen, sample_count, nodes)"
            " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
               SYSTEM,
               r["username"],
               r["cmdline_hash"],
               r["cmdline"],
               r["activity"],
               r["category"],
               r["tags"],
               now - timedelta(days=1),
               now,
               5,
               ["n1"],
            ),
         )


def test_search_substring_escapes_percent_underscore(migrated_db, tmp_path):
   url = migrated_db
   _seed_cmdlines(url)
   client = _client(url, tmp_path)
   resp = client.get("/api/commands/search", params={"q": "%_special"})
   assert resp.status_code == 200, resp.text
   body = resp.json()
   assert body["total"] == 1, body
   assert body["items"][0]["cmdline"] == "ls %_special_% pattern"


def test_search_regex_valid(migrated_db, tmp_path):
   url = migrated_db
   _seed_cmdlines(url)
   client = _client(url, tmp_path)
   resp = client.get("/api/commands/search", params={"regex": "python.*train"})
   assert resp.status_code == 200, resp.text
   body = resp.json()
   assert body["total"] >= 1
   assert any("train.py" in it["cmdline"] for it in body["items"])


def test_search_bad_regex_returns_400(migrated_db, tmp_path):
   url = migrated_db
   _seed_cmdlines(url)
   client = _client(url, tmp_path)
   resp = client.get("/api/commands/search", params={"regex": "[invalid"})
   assert resp.status_code == 400, resp.text


def test_search_filter_by_tag(migrated_db, tmp_path):
   url = migrated_db
   _seed_cmdlines(url)
   client = _client(url, tmp_path)
   resp = client.get("/api/commands/search", params={"tag": "agent.skip_permissions"})
   assert resp.status_code == 200, resp.text
   body = resp.json()
   assert body["total"] == 1, body
   assert body["items"][0]["username"] == "bob"


def test_search_unknown_filter_returns_empty(migrated_db, tmp_path):
   url = migrated_db
   _seed_cmdlines(url)
   client = _client(url, tmp_path)
   for params in [
      {"user": "no_such_user_xyz"},
      {"activity": "no_such_activity"},
      {"tag": "no_such_tag"},
   ]:
      resp = client.get("/api/commands/search", params=params)
      assert resp.status_code == 200, resp.text
      assert resp.json()["total"] == 0
      assert resp.json()["items"] == []


def test_search_paging_total(migrated_db, tmp_path):
   url = migrated_db
   _seed_cmdlines(url)
   client = _client(url, tmp_path)
   resp = client.get("/api/commands/search", params={"limit": "1", "offset": "0"})
   assert resp.status_code == 200, resp.text
   body = resp.json()
   assert body["total"] >= 4
   assert len(body["items"]) == 1
   resp2 = client.get("/api/commands/search", params={"limit": "1", "offset": "1"})
   assert resp2.status_code == 200
   assert len(resp2.json()["items"]) == 1
   assert body["items"][0]["cmdline"] != resp2.json()["items"][0]["cmdline"]


def test_search_xss_cmdline_returned_as_text(migrated_db, tmp_path):
   url = migrated_db
   _seed_cmdlines(url)
   client = _client(url, tmp_path)
   resp = client.get("/api/commands/search", params={"q": "script"})
   assert resp.status_code == 200, resp.text
   body = resp.json()
   assert body["total"] >= 1
   hit = next((it for it in body["items"] if "script" in it["cmdline"]), None)
   assert hit is not None
   assert "<script>" in hit["cmdline"]


def test_search_returns_expected_fields(migrated_db, tmp_path):
   url = migrated_db
   _seed_cmdlines(url)
   client = _client(url, tmp_path)
   resp = client.get("/api/commands/search", params={"q": "train"})
   assert resp.status_code == 200, resp.text
   it = resp.json()["items"][0]
   for key in (
      "cmdline",
      "username",
      "activity",
      "category",
      "tags",
      "first_seen",
      "last_seen",
      "nodes",
      "sample_count",
   ):
      assert key in it, "missing " + key + " in " + str(it)


def test_search_100k_user_still_pages(migrated_db, tmp_path):
   from datetime import datetime, timedelta, timezone

   from node_monitor.database.connect import connect

   url = migrated_db
   now = datetime.now(timezone.utc)
   with connect(url, autocommit=True) as conn:
      for i in range(200):
         conn.execute(
            "INSERT INTO command_lines"
            " (system, username, cmdline_hash, cmdline, activity, category, tags,"
            "  first_seen, last_seen, sample_count, nodes)"
            " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
               SYSTEM,
               "heavy",
               "heavy-" + str(i).zfill(6),
               "python job_" + str(i).zfill(5) + ".py --seed " + str(i),
               "python-generic",
               "python",
               [],
               now - timedelta(hours=1),
               now,
               1,
               ["n1"],
            ),
         )
   client = _client(url, tmp_path)
   resp = client.get("/api/commands/search", params={"user": "heavy", "limit": "50", "offset": "0"})
   assert resp.status_code == 200, resp.text
   body = resp.json()
   assert body["total"] == 200
   assert len(body["items"]) == 50
   resp2 = client.get(
      "/api/commands/search", params={"user": "heavy", "limit": "50", "offset": "50"}
   )
   assert resp2.status_code == 200
   assert len(resp2.json()["items"]) == 50


def test_search_user_filter(migrated_db, tmp_path):
   url = migrated_db
   _seed_cmdlines(url)
   client = _client(url, tmp_path)
   resp = client.get("/api/commands/search", params={"user": "alice"})
   assert resp.status_code == 200, resp.text
   body = resp.json()
   assert body["total"] == 2
