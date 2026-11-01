import os
import subprocess
import sys
import time

import pytest
import yaml
from click.testing import CliRunner

from node_monitor.cli.main import cli
from tests.database import conftest as pg
from tests.database.test_bootstrap import _url, names  # noqa: F401
from tests.test_config import MINIMAL

pg_admin_url = pg.pg_admin_url
pg_db = pg.pg_db
migrated_db = pg.migrated_db

_HOLDER = """
import sys, time
from pathlib import Path
from node_monitor.collector.instance import InstanceLock
InstanceLock(Path(sys.argv[1])).acquire()
print("ready", flush=True)
time.sleep(120)
"""


@pytest.fixture(autouse=True)
def _no_env(monkeypatch):
   for var in ("NODE_MONITOR_DB_URL", "NODE_MONITOR_WEB_DB_URL", "NODE_MONITOR_CONFIG"):
      monkeypatch.delenv(var, raising=False)
   monkeypatch.delenv("INVOCATION_ID", raising=False)


@pytest.fixture
def workdir(tmp_path):
   return tmp_path / "wd"


def _config(tmp_path, workdir, **extra):
   path = tmp_path / "config.yaml"
   path.write_text(yaml.safe_dump({**MINIMAL, "working_dir": str(workdir), **extra}))
   return str(path)


@pytest.fixture
def holder(workdir):
   procs = []

   def start(env=None):
      proc = subprocess.Popen(
         [sys.executable, "-c", _HOLDER, str(workdir)],
         stdout=subprocess.PIPE,
         text=True,
         env={**os.environ, **(env or {})},
      )
      procs.append(proc)
      assert proc.stdout.readline().strip() == "ready"
      return proc

   yield start
   for proc in procs:
      proc.terminate()
      try:
         proc.wait(timeout=10)
      except subprocess.TimeoutExpired:
         proc.kill()
         proc.wait(timeout=10)
      proc.stdout.close()


def run(*args):
   return CliRunner().invoke(cli, list(args))


def test_daemon_start_refuses_when_running(tmp_path, workdir, holder):
   proc = holder()
   for extra in ([], ["--foreground"]):
      result = run("daemon", "start", *extra, "--config", _config(tmp_path, workdir))
      assert result.exit_code == 1
      assert f"pid {proc.pid}" in result.output


def test_daemon_stop_not_running_exits_0(tmp_path, workdir):
   result = run("daemon", "stop", "--config", _config(tmp_path, workdir))
   assert result.exit_code == 0
   assert "not running" in result.output


def test_daemon_stop_refuses_systemd_managed_without_force(tmp_path, workdir, holder):
   proc = holder({"INVOCATION_ID": "x"})
   result = run("daemon", "stop", "--config", _config(tmp_path, workdir))
   assert result.exit_code == 1
   assert "systemctl --user stop node-monitor-daemon" in result.output
   assert proc.poll() is None


def test_daemon_stop_terminates_a_foreground_child(tmp_path, workdir, holder):
   proc = holder()
   result = run("daemon", "stop", "--timeout", "10", "--config", _config(tmp_path, workdir))
   assert result.exit_code == 0, result.output
   assert proc.wait(timeout=5) != 0  # killed by SIGTERM


def test_daemon_stop_force_overrides_systemd(tmp_path, workdir, holder):
   proc = holder({"INVOCATION_ID": "x"})
   result = run(
      "daemon", "stop", "--force", "--timeout", "10", "--config", _config(tmp_path, workdir)
   )
   assert result.exit_code == 0, result.output
   proc.wait(timeout=5)


def test_daemon_status_not_running_without_database(tmp_path, workdir):
   result = run("daemon", "status", "--config", _config(tmp_path, workdir))
   assert result.exit_code == 1
   assert "not running" in result.output
   assert "not configured" in result.output


def test_daemon_status_running_exits_0(tmp_path, workdir, holder):
   proc = holder()
   result = run("daemon", "status", "--config", _config(tmp_path, workdir))
   assert result.exit_code == 0
   assert str(proc.pid) in result.output


def test_daemon_status_reports_unreachable_database(tmp_path, workdir):
   cfg = _config(
      tmp_path,
      workdir,
      web={"database_url": "postgresql://nobody@127.0.0.1:1/x"},
      database={"url": "postgresql://nobody@127.0.0.1:1/x", "connect_timeout_sec": 1},
   )
   result = run("daemon", "status", "--config", cfg)
   assert result.exit_code == 1
   assert "database unreachable" in result.output


def test_daemon_status_shows_heartbeat(tmp_path, workdir, migrated_db):
   from datetime import datetime, timezone

   from node_monitor.database.connect import connect
   from node_monitor.database.maintenance import write_heartbeat

   now = datetime.now(timezone.utc)
   with connect(migrated_db, autocommit=True) as conn:
      write_heartbeat(
         conn,
         system="polaris",
         collector_host="h",
         version="9.9",
         started_at=now,
         now=now,
         status={
            "state": "running",
            "sessions": {"login-01": "up"},
            "connected": 1,
            "spooled_batches": 3,
         },
      )
   cfg = _config(tmp_path, workdir, web={"database_url": migrated_db})
   result = run("daemon", "status", "--config", cfg)
   assert result.exit_code == 0, result.output  # fresh heartbeat, though no local process
   assert "9.9" in result.output
   assert "login-01" in result.output
   assert "spooled" in result.output


def test_database_wait_times_out_on_unreachable_url(tmp_path, workdir):
   cfg = _config(
      tmp_path,
      workdir,
      database={"url": "postgresql://nobody@127.0.0.1:1/x", "connect_timeout_sec": 1},
   )
   start = time.monotonic()
   result = run("database", "wait", "--timeout", "3", "--config", cfg)
   assert result.exit_code == 1
   assert time.monotonic() - start < 6


def test_database_wait_succeeds_when_reachable(tmp_path, workdir, pg_db):
   cfg = _config(tmp_path, workdir, database={"url": pg_db, "connect_timeout_sec": 1})
   assert run("database", "wait", "--timeout", "5", "--config", cfg).exit_code == 0


def test_daemon_start_reports_early_exit_with_log_tail(tmp_path, workdir):
   # No database.url, so the spawned foreground daemon fails at once.
   result = run("daemon", "start", "--config", _config(tmp_path, workdir))
   assert result.exit_code == 1
   assert "database.url" in result.output
   assert (workdir / "logs").stat().st_mode & 0o777 == 0o700
   assert (workdir / "logs" / "daemon.log").stat().st_mode & 0o777 == 0o600
   assert not (workdir / "state" / "daemon.json").exists()


def test_foreground_start_tightens_shared_state_and_logs(tmp_path, workdir):
   for name in ("state", "logs"):
      (workdir / name).mkdir(parents=True)
      os.chmod(workdir / name, 0o775)
   # No database.url: the daemon refuses to start, but only after the directories are fixed.
   result = run("daemon", "start", "--foreground", "--config", _config(tmp_path, workdir))
   assert result.exit_code == 1
   assert "database.url" in result.output
   for name in ("state", "logs"):
      assert (workdir / name).stat().st_mode & 0o777 == 0o700, name


def test_background_start_tightens_shared_state(tmp_path, workdir):
   (workdir / "state").mkdir(parents=True)
   os.chmod(workdir / "state", 0o775)
   result = run("daemon", "start", "--config", _config(tmp_path, workdir))
   assert "database.url" in result.output
   assert (workdir / "state").stat().st_mode & 0o777 == 0o700


@pytest.fixture
def stale_info(workdir):
   """A crashed daemon's leftover daemon.json whose pid now belongs to an unrelated process."""
   import json
   import socket

   proc = subprocess.Popen(["sleep", "30"])
   state = workdir / "state"
   state.mkdir(parents=True)
   (state / "daemon.json").write_text(
      json.dumps(
         {
            "pid": proc.pid,
            "host": socket.gethostname(),
            "started_at": "2026-01-01T00:00:00+00:00",
            "systemd": False,
         }
      )
   )
   yield proc
   proc.kill()
   proc.wait(timeout=10)


def test_stale_info_with_recycled_pid_is_not_signalled_by_stop(tmp_path, workdir, stale_info):
   result = run("daemon", "stop", "--config", _config(tmp_path, workdir))
   assert result.exit_code == 0
   assert "not running" in result.output
   assert stale_info.poll() is None
   assert not (workdir / "state" / "daemon.json").exists()


def test_stale_info_does_not_make_status_healthy(tmp_path, workdir, stale_info):
   result = run("daemon", "status", "--config", _config(tmp_path, workdir))
   assert result.exit_code == 1
   assert "not running" in result.output
   assert stale_info.poll() is None


def test_stale_info_does_not_block_start(tmp_path, workdir, stale_info):
   # The spawned daemon dies at once (no database.url); what matters is that start tried.
   result = run("daemon", "start", "--config", _config(tmp_path, workdir))
   assert "already running" not in result.output
   assert "database.url" in result.output
   assert stale_info.poll() is None
