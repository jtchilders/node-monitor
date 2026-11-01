import psycopg
import pytest
import yaml
from click.testing import CliRunner

from node_monitor.cli.main import cli
from node_monitor.config import load_config
from node_monitor.pipeline.replay import replay
from tests.database.conftest import pg_admin_url, pg_db  # noqa: F401 - fixtures
from tests.pipeline.test_replay import build_soak
from tests.test_config import MINIMAL

SUMMARY_TABLES = ("node_hardware", "node_metrics_1m", "user_usage_5m", "node_usage_5m")


@pytest.fixture(autouse=True)
def _no_env(monkeypatch):
   monkeypatch.delenv("NODE_MONITOR_DB_URL", raising=False)
   monkeypatch.delenv("NODE_MONITOR_CONFIG", raising=False)


def _config(tmp_path, url):
   data = dict(MINIMAL)
   data["nodes"] = [{"host": f"{n}.example", "label": n} for n in ("n1", "n2")]
   data["working_dir"] = str(tmp_path)
   if url:
      data["database"] = {"url": url, "connect_timeout_sec": 1}
   path = tmp_path / "config.yaml"
   path.write_text(yaml.safe_dump(data))
   path.chmod(0o600)
   return str(path)


def _counts(url):
   with psycopg.connect(url) as conn:
      return {
         t: conn.execute(f"SELECT count(*) FROM node_monitor.{t}").fetchone()[0]
         for t in SUMMARY_TABLES
      }


def test_replay_write_db_round_trip(migrated_db, tmp_path):
   soak = build_soak(tmp_path)
   cfg = _config(tmp_path, migrated_db)
   expected = replay(soak, load_config(cfg))

   result = CliRunner().invoke(cli, ["soak", "replay", str(soak), "--config", cfg, "--write-db"])
   assert result.exit_code == 0, result.output
   assert "connected" in result.output  # the final status() line

   first = _counts(migrated_db)
   for table in SUMMARY_TABLES:
      assert first[table] == sum(c[table] for c in expected.values()), table
   assert first["node_metrics_1m"] > 0 and first["user_usage_5m"] > 0

   again = CliRunner().invoke(cli, ["soak", "replay", str(soak), "--config", cfg, "--write-db"])
   assert again.exit_code == 0, again.output
   assert _counts(migrated_db) == first


def test_replay_write_db_without_url_is_a_clean_error(tmp_path):
   soak = build_soak(tmp_path)
   cfg = _config(tmp_path, None)
   result = CliRunner().invoke(cli, ["soak", "replay", str(soak), "--config", cfg, "--write-db"])
   assert result.exit_code == 1
   assert "database.url" in result.output
   assert "Traceback" not in result.output
