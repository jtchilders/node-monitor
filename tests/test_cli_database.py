import secrets

import psycopg
import pytest
import yaml
from click.testing import CliRunner
from psycopg import sql

from node_monitor.cli.main import cli
from tests.database import conftest as pg
from tests.test_config import MINIMAL

# The pg fixtures live in tests/database/conftest.py; re-exported for this module.
pg_admin_url = pg.pg_admin_url
pg_db = pg.pg_db


def _config(tmp_path, url):
   path = tmp_path / "config.yaml"
   data = dict(MINIMAL)
   if url:
      data["database"] = {"url": url, "connect_timeout_sec": 1}
   path.write_text(yaml.safe_dump(data))
   return str(path)


@pytest.fixture(autouse=True)
def _no_env_urls(monkeypatch):
   monkeypatch.delenv("NODE_MONITOR_DB_URL", raising=False)
   monkeypatch.delenv("NODE_MONITOR_CONFIG", raising=False)


def run(*args):
   return CliRunner().invoke(cli, ["database", *args])


def test_missing_url_is_a_clean_error(tmp_path):
   result = run("migrate", "--config", _config(tmp_path, None))
   assert result.exit_code == 1
   assert "database.url" in result.output
   assert "Traceback" not in result.output


def test_unavailable_database_is_a_clean_error(tmp_path):
   cfg = _config(tmp_path, "postgresql://nobody@127.0.0.1:1/x")
   for cmd in ("migrate", "status"):
      result = run(cmd, "--config", cfg)
      assert result.exit_code == 1
      assert "unavailable" in result.output
      assert result.exception is None or isinstance(result.exception, SystemExit)


def test_status_and_migrate_flow(pg_db, tmp_path):
   cfg = _config(tmp_path, pg_db)
   before = run("status", "--config", cfg)
   assert before.exit_code == 1
   assert "current version: 0" in before.output
   assert "pending: 0001" in before.output

   first = run("migrate", "--config", cfg)
   assert first.exit_code == 0 and "applied: 0001" in first.output
   second = run("migrate", "--config", cfg)
   assert second.exit_code == 0 and "up to date" in second.output

   after = run("status", "--config", cfg)
   assert after.exit_code == 0
   assert "pending: none" in after.output
   assert "partition node_metrics_1m: none" in after.output.splitlines()

   with psycopg.connect(pg_db, autocommit=True) as conn:
      conn.execute(
         "CREATE TABLE node_monitor.node_metrics_1m_y2026m11"
         " PARTITION OF node_monitor.node_metrics_1m"
         " FOR VALUES FROM ('2026-11-01') TO ('2026-12-01')"
      )
   with_partition = run("status", "--config", cfg)
   lines = with_partition.output.splitlines()
   line = next(
      x for x in lines if x.startswith("partition node_metrics_1m.node_metrics_1m_y2026m11")
   )
   assert "2026-11-01" in line and "2026-12-01" in line
   assert "partition node_metrics_1m: none" not in lines


def test_bootstrap_command(pg_admin_url):
   hexid = secrets.token_hex(4)
   db, owner, reader = (
      f"node_monitor_test_{hexid}",
      f"nm_test_owner_{hexid}",
      f"nm_test_reader_{hexid}",
   )
   try:
      args = [
         "bootstrap",
         "--admin-url",
         pg_admin_url,
         "--database",
         db,
         "--owner",
         owner,
         "--reader",
         reader,
      ]
      first = run(*args)
      assert first.exit_code == 0 and f"created database {db}" in first.output
      assert "nothing to do" in run(*args).output
   finally:
      pg._drop_test_db(pg_admin_url, db)
      with pg._admin_connect(pg_admin_url) as conn:
         for role in (owner, reader):
            conn.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role)))
