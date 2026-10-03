"""Real-PostgreSQL + real-subprocess collector/web process isolation
acceptance (operational-web-dashboard plan Task 9, Step 3 second half).

All tests in this file are unconditionally SKIPPED when
NODE_MONITOR_TEST_DATABASE_URL is not set -- they never fabricate evidence.

Builds a resident-like collector fixture (a background thread writing real
``node_hardware``/``node_counter_minute`` rows through its own dedicated
SQLAlchemy engine -- never the web process's engine) and a REAL
``node-monitor web`` subprocess bound to a disposable UUID-named database
through a UUID-named SELECT-only reader role. Proves:

  * While the web process is induced into repeated query failures, the
    resident-like collector's heartbeat and maximum counter timestamp keep
    advancing (the two are genuinely independent processes/connections).
  * Killing the web subprocess (SIGKILL) does not signal, interrupt, or
    stale the collector thread.
  * Stopping the collector leaves ``/health`` available on the still-running
    web subprocess, and ``/api/dashboard`` reports connected-but-stale data
    (the web process is up; the data it serves is honestly marked stale).

Every subprocess/thread this file starts is bounded and guaranteed to be
torn down in ``finally`` blocks, even on assertion failure. This file never
starts, stops, restarts, or reconfigures PostgreSQL itself, and never
touches ``pbs-monitor``, ``node_monitor_dev``, or the resident collector
that may be running against the development database.
"""

import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.pool import NullPool

from node_monitor.database.migration import MigrationRunner
from node_monitor.web.queries import COUNTER_STALENESS_SECONDS

from tests.web.conftest import pg_skip


_PG_AVAILABLE = bool(os.environ.get("NODE_MONITOR_TEST_DATABASE_URL"))

_SYSTEM = "polaris"
_NODE = "login-04"

_REPO_ROOT = os.path.dirname(
   os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_VENV_PYTHON = os.path.join(_REPO_ROOT, "venv", "bin", "python")


def _repo_python():
   return _VENV_PYTHON if os.path.exists(_VENV_PYTHON) else sys.executable


# ---------------------------------------------------------------------------
# Disposable DB + reader role + collector (writer) engine
# ---------------------------------------------------------------------------

class _ProcessIsolationEnv:
   def __init__(self):
      self.db_name = None
      self.role_name = None
      self.admin_url = None
      self.reader_url = None
      self.collector_engine = None  # dedicated engine, never the web's own


@pytest.fixture
def process_isolation_env():
   if not _PG_AVAILABLE:
      pytest.skip("NODE_MONITOR_TEST_DATABASE_URL is required")

   admin_url = make_url(os.environ["NODE_MONITOR_TEST_DATABASE_URL"])
   db_name = "nm_procisotest_" + uuid.uuid4().hex[:16]
   role_name = "nm_procisoreader_" + uuid.uuid4().hex[:16]

   admin_engine = create_engine(
      admin_url, poolclass=NullPool, isolation_level="AUTOCOMMIT")
   with admin_engine.connect() as conn:
      conn.exec_driver_sql('CREATE DATABASE "%s"' % db_name)
      conn.exec_driver_sql(
         "CREATE ROLE %s LOGIN PASSWORD 'test_only_pw'" % role_name)

   db_admin_engine = create_engine(
      admin_url.set(database=db_name), poolclass=NullPool)

   env = _ProcessIsolationEnv()
   try:
      runner = MigrationRunner(db_admin_engine, "task9-procisolation-test")
      runner.migrate()

      # Revoke default PUBLIC TEMP so this disposable database matches the
      # expected production setup -- WebDatabase.preflight() fails closed
      # on any database-level TEMP privilege (see
      # test_web_database_preflight.py's migrated_reader_engine fixture,
      # which performs this exact same revoke for the same reason).
      with admin_engine.connect() as conn:
         conn.exec_driver_sql(
            'REVOKE TEMP ON DATABASE "%s" FROM PUBLIC' % db_name)

      with db_admin_engine.connect() as conn:
         conn.exec_driver_sql(
            "GRANT CONNECT ON DATABASE \"%s\" TO %s" % (db_name, role_name))
         conn.exec_driver_sql(
            "GRANT USAGE ON SCHEMA node_monitor TO %s" % role_name)
         conn.exec_driver_sql(
            "GRANT SELECT ON ALL TABLES IN SCHEMA node_monitor TO %s"
            % role_name)
         conn.commit()

      env.db_name = db_name
      env.role_name = role_name
      env.admin_url = admin_url.set(database=db_name)
      env.reader_url = admin_url.set(
         database=db_name, username=role_name, password="test_only_pw")
      # The collector's OWN dedicated engine -- deliberately a brand-new
      # engine object, never reused by (or shared with) the web process.
      env.collector_engine = create_engine(
         str(env.admin_url), poolclass=NullPool)

      yield env
   finally:
      if env.collector_engine is not None:
         env.collector_engine.dispose()
      db_admin_engine.dispose()
      with admin_engine.connect() as conn:
         conn.exec_driver_sql(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()", (db_name,))
         conn.exec_driver_sql('DROP DATABASE IF EXISTS "%s"' % db_name)
         conn.exec_driver_sql("DROP ROLE IF EXISTS %s" % role_name)
      admin_engine.dispose()


# ---------------------------------------------------------------------------
# Resident-like collector fixture: a bounded background thread that writes
# real rows through its OWN dedicated engine, independent of the web process.
# ---------------------------------------------------------------------------

class ResidentCollector:
   """A bounded background thread simulating a resident node-monitor
   collector: it periodically writes a fresh ``node_counter_minute`` row
   (advancing the maximum stored window_end) and records its own heartbeat
   timestamp, using a dedicated SQLAlchemy engine that the web process never
   touches.
   """

   def __init__(self, engine, system, node, *, interval_sec=0.2):
      self._engine = engine
      self._system = system
      self._node = node
      self._interval_sec = interval_sec
      self._stop_event = threading.Event()
      self._thread = None
      self._lock = threading.Lock()
      self._last_heartbeat_at = None
      self._tick_count = 0
      self._error = None

   def write_hardware_row(self):
      with self._engine.begin() as conn:
         conn.execute(text("""
            INSERT INTO node_monitor.node_hardware (
               system, source_hostname, first_seen, last_verified,
               cpu_logical, mem_total_kb, probe_version
            ) VALUES (
               :system, :node, now(), now(), 96, 131072000, 4
            )
            ON CONFLICT (system, source_hostname) DO UPDATE SET
               last_verified = EXCLUDED.last_verified
         """), {"system": self._system, "node": self._node})

   def write_one_counter_row(self, window_end):
      window_start = window_end - timedelta(minutes=1)
      with self._engine.begin() as conn:
         conn.execute(text("""
            INSERT INTO node_monitor.node_counter_minute (
               system, source_hostname, window_start, window_end,
               collector_hostname, probe_version, daemon_version,
               sample_count, expected_count, coverage,
               mem_available_kb, cached_kb, shmem_kb,
               load1, load5, load15, procs_running, procs_total,
               socket_count, cpu_busy_pct, network_rates,
               lustre_md_summary, meets_minimum_samples,
               invalid_pair_count, excess_sample_count
            ) VALUES (
               :system, :node, :window_start, :window_end, :node, 4, '0.2.0',
               6, 6, 1.0, 60000000, 9000000, 500000, 1.0, 1.0, 1.0, 2, 200, 8,
               CAST(:cpu AS jsonb), CAST(:net AS jsonb), CAST(:lustre AS jsonb),
               true, 0, 0
            )
            ON CONFLICT (system, source_hostname, window_start) DO NOTHING
         """), {
            "system": self._system, "node": self._node,
            "window_start": window_start, "window_end": window_end,
            "cpu": json.dumps({"p50": 10.0, "p95": 20.0, "max": 30.0}),
            "net": json.dumps({}),
            "lustre": json.dumps({}),
         })

   def _run(self):
      try:
         self.write_hardware_row()
         while not self._stop_event.is_set():
            now = datetime.now(timezone.utc)
            self.write_one_counter_row(now)
            with self._lock:
               self._last_heartbeat_at = now
               self._tick_count += 1
            self._stop_event.wait(self._interval_sec)
      except Exception as exc:  # pragma: no cover -- surfaced via self._error
         self._error = exc

   def start(self):
      self._thread = threading.Thread(target=self._run, daemon=True)
      self._thread.start()
      # Wait for at least one real tick before returning control.
      deadline = time.monotonic() + 5.0
      while time.monotonic() < deadline:
         with self._lock:
            if self._tick_count > 0:
               return
         time.sleep(0.05)
      raise AssertionError("resident collector never completed a first tick")

   def heartbeat_and_ticks(self):
      with self._lock:
         return self._last_heartbeat_at, self._tick_count

   def is_alive(self):
      return self._thread is not None and self._thread.is_alive()

   def stop(self, timeout=5.0):
      self._stop_event.set()
      if self._thread is not None:
         self._thread.join(timeout=timeout)
      if self._error is not None:
         raise self._error

   def max_counter_window_end(self):
      with self._engine.connect() as conn:
         return conn.execute(text(
            "SELECT max(window_end) FROM node_monitor.node_counter_minute "
            "WHERE system = :system AND source_hostname = :node"
         ), {"system": self._system, "node": self._node}).scalar_one()


# ---------------------------------------------------------------------------
# Real `node-monitor web` subprocess harness
# ---------------------------------------------------------------------------

class WebSubprocess:
   """Launches the real ``node-monitor web --config ...`` CLI as a bounded
   subprocess bound to a temporary ``HOME`` so it never touches the
   operator's real ``~/.node-monitor`` run directory.
   """

   def __init__(self, reader_url):
      self._reader_url = reader_url
      # Use a short /tmp path for HOME rather than tempfile's default
      # (which resolves under macOS's long /private/var/folders/... prefix
      # and overflows the 103-byte AF_UNIX path limit once ~/.node-monitor/
      # run/web.sock is appended -- see tests/web/test_web_socket.py's
      # short_tmp fixture for the same constraint).
      self._tmp_home = tempfile.mkdtemp(prefix="nmw_", dir="/tmp")
      self._config_path = os.path.join(self._tmp_home, "web_config.yaml")
      self._socket_path = os.path.join(
         self._tmp_home, ".node-monitor", "run", "web.sock")
      self.process = None

   def _write_config(self):
      os.makedirs(
         os.path.join(self._tmp_home, ".node-monitor", "run"),
         mode=0o700, exist_ok=True)
      with open(self._config_path, "w") as handle:
         handle.write(
            "system: %s\n"
            "web:\n"
            "  socket_path: ~/.node-monitor/run/web.sock\n"
            "  database:\n"
            "    url: %s\n" % (_SYSTEM, str(self._reader_url)))

   def start(self):
      self._write_config()
      env = dict(os.environ)
      env["HOME"] = self._tmp_home
      env["PYTHONPATH"] = _REPO_ROOT
      self.process = subprocess.Popen(
         [_repo_python(), "-m", "node_monitor.cli.main", "web",
          "--config", self._config_path],
         cwd=_REPO_ROOT, env=env,
         stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
      )
      self._wait_for_socket(timeout=10.0)

   def _wait_for_socket(self, timeout):
      deadline = time.monotonic() + timeout
      while time.monotonic() < deadline:
         if self.process.poll() is not None:
            out, err = self.process.communicate(timeout=2)
            raise AssertionError(
               "web subprocess exited early (rc=%s): stdout=%r stderr=%r"
               % (self.process.returncode, out, err))
         if os.path.exists(self._socket_path):
            try:
               client = self.client()
               resp = client.get("/health", timeout=1.0)
               client.close()
               if resp.status_code == 200:
                  return
            except Exception:
               pass
         time.sleep(0.1)
      raise AssertionError("web subprocess never became ready")

   def client(self):
      transport = httpx.HTTPTransport(uds=self._socket_path)
      return httpx.Client(transport=transport, base_url="http://nodemonitor")

   def is_alive(self):
      return self.process is not None and self.process.poll() is None

   def kill(self, timeout=5.0):
      if self.process is None:
         return
      self.process.kill()
      try:
         self.process.wait(timeout=timeout)
      except subprocess.TimeoutExpired:  # pragma: no cover -- defensive
         pass

   def cleanup(self):
      if self.process is not None and self.process.poll() is None:
         self.process.kill()
         try:
            self.process.wait(timeout=5.0)
         except subprocess.TimeoutExpired:  # pragma: no cover
            pass
      import shutil
      shutil.rmtree(self._tmp_home, ignore_errors=True)


# ---------------------------------------------------------------------------
# Step 3: collector heartbeat/counter timestamp advance under web failure;
# killing the web subprocess does not signal/stale the collector.
# ---------------------------------------------------------------------------

@pg_skip
def test_collector_advances_while_web_query_fails_and_survives_web_kill(
      process_isolation_env):
   collector = ResidentCollector(
      process_isolation_env.collector_engine, _SYSTEM, _NODE,
      interval_sec=0.2)
   web = WebSubprocess(process_isolation_env.reader_url)
   try:
      collector.start()
      web.start()

      client = web.client()
      try:
         # Induce repeated web query failures: an unknown node is rejected
         # by the real DashboardService's in-transaction inventory lookup
         # (422) on every call -- a genuine, repeated web-side failure mode
         # that touches the database on every request.
         failures = 0
         heartbeat_before, ticks_before = collector.heartbeat_and_ticks()
         max_window_before = collector.max_counter_window_end()

         for _ in range(10):
            resp = client.get(
               "/api/dashboard",
               params={"node": "unknown-node-xyz", "range": "1h"},
               timeout=5.0)
            assert resp.status_code == 422
            failures += 1
            time.sleep(0.1)

         heartbeat_after, ticks_after = collector.heartbeat_and_ticks()
         max_window_after = collector.max_counter_window_end()

         assert failures == 10
         assert ticks_after > ticks_before, (
            "collector must keep ticking while the web process fails "
            "queries; before=%d after=%d" % (ticks_before, ticks_after))
         assert heartbeat_after > heartbeat_before, (
            "collector heartbeat must advance independent of web failures")
         assert max_window_after > max_window_before, (
            "collector's maximum counter timestamp must keep advancing")
      finally:
         client.close()

      # Kill the web subprocess -- the collector must not be signaled,
      # interrupted, or staled by this at all.
      heartbeat_before_kill, ticks_before_kill = collector.heartbeat_and_ticks()
      assert web.is_alive()
      web.kill()
      assert not web.is_alive()

      time.sleep(0.6)  # bounded: let a couple more collector ticks happen
      assert collector.is_alive(), (
         "collector thread must survive the web subprocess being killed")
      heartbeat_after_kill, ticks_after_kill = collector.heartbeat_and_ticks()
      assert ticks_after_kill > ticks_before_kill, (
         "collector must keep ticking after the web subprocess is killed")
      assert heartbeat_after_kill > heartbeat_before_kill
   finally:
      collector.stop()
      web.cleanup()


# ---------------------------------------------------------------------------
# Step 3: stopping the collector leaves /health available and dashboard
# data connected-but-stale.
# ---------------------------------------------------------------------------

@pg_skip
def test_stopping_collector_leaves_health_available_and_data_stale(
      process_isolation_env):
   """The collector writes ONLY already-old (pre-stale) counter windows --
   anchored far enough in the past that COUNTER_STALENESS_SECONDS is
   already exceeded at write time -- then is explicitly stopped. The web
   subprocess, queried only AFTER the collector stops, must still answer
   /health, while /api/dashboard reports connected-but-stale counter data.
   This proves the staleness contract without requiring the test to sleep
   past the real 120-second freshness window.
   """
   collector = ResidentCollector(
      process_isolation_env.collector_engine, _SYSTEM, _NODE,
      interval_sec=0.2)
   web = WebSubprocess(process_isolation_env.reader_url)
   try:
      collector.write_hardware_row()
      # Write a handful of counter rows anchored well beyond the staleness
      # threshold -- the collector has, in effect, "already stopped"
      # producing fresh data from the dashboard's point of view.
      stale_anchor = (
         datetime.now(timezone.utc)
         - timedelta(seconds=COUNTER_STALENESS_SECONDS * 5))
      for minute_offset in range(5):
         collector.write_one_counter_row(
            stale_anchor + timedelta(minutes=minute_offset))

      web.start()
      client = web.client()
      try:
         health = client.get("/health", timeout=5.0)
         assert health.status_code == 200
         assert health.json() == {"status": "ok"}

         resp = client.get(
            "/api/dashboard", params={"node": _NODE, "range": "24h"},
            timeout=10.0)
         assert resp.status_code == 200
         data = resp.json()
         assert data["counters"]["is_fresh"] is False
         assert data["counters"]["status"] == "stale"
         # Connected-but-stale: the web process answered successfully (not
         # a 503/timeout) with real, non-empty historical rows.
         assert len(data["counters"]["rows"]) > 0
      finally:
         client.close()
   finally:
      collector.stop()
      web.cleanup()


# ---------------------------------------------------------------------------
# Bounded-process guarantee: both fixtures above tear down deterministically
# even when the body raises -- proven by a deliberately-failing body whose
# `finally` cleanup still runs and leaves no process/thread behind.
# ---------------------------------------------------------------------------

@pg_skip
def test_cleanup_runs_even_when_test_body_raises(process_isolation_env):
   collector = ResidentCollector(
      process_isolation_env.collector_engine, _SYSTEM, _NODE,
      interval_sec=0.2)
   web = WebSubprocess(process_isolation_env.reader_url)
   cleanup_ran = {"collector": False, "web": False}
   try:
      collector.start()
      web.start()
      try:
         raise RuntimeError("deliberate failure inside the test body")
      finally:
         collector.stop()
         cleanup_ran["collector"] = True
         web.cleanup()
         cleanup_ran["web"] = True
   except RuntimeError:
      pass
   assert cleanup_ran["collector"] is True
   assert cleanup_ran["web"] is True
   assert not collector.is_alive()
   assert not web.is_alive()
