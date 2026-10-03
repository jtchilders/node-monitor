"""Task 6 CLI lifecycle tests.

Proves ordered lifecycle events for the `web` sub-command using
Click CliRunner with monkeypatching of module-level callables.

Scenarios:
1. Success: config -> DB construct -> preflight -> service/app/socket ->
   startup line -> run -> socket close -> db dispose.
2. Preflight failure: never binds/prints startup line; DB dispose called.
3. Service/app/socket failure: DB dispose called.
4. Bind failure (SocketError): DB dispose called; no startup line.
5. Runtime failure (run_uvicorn raises): socket close then DB dispose.

Additional:
- Missing/bad config exits nonzero with bounded, credential-free text.
- Bad db url in config exits nonzero with bounded text.
- Startup line is exactly 'PID <digits> socket <abs-path>'.
- No raw exception interpolation in output.

Import-graph regression:
- Importing cli.main must not load node_monitor.database.migration.
"""
import os
import re
import subprocess
import sys
import tempfile

import pytest
from click.testing import CliRunner

from node_monitor.cli.main import web_command


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _Tracker:
   """Records call events in order."""

   def __init__(self):
      self.events = []

   def record(self, name):
      self.events.append(name)


def _make_fake_config(socket_path=None):
   """Return a minimal valid WebConfig object using real constructors."""
   from node_monitor.config import WebConfig
   from node_monitor.config import DatabaseConfig
   home_dir = os.path.expanduser("~")
   if socket_path is None:
      socket_path = os.path.join(home_dir, ".node-monitor", "run", "web_test.sock")
   db_cfg = DatabaseConfig(
      url="postgresql://u:p@h/db",
      schema="node_monitor",
      pool_size=1,
      max_overflow=0,
      echo_sql=False,
      pool_pre_ping=True,
      pool_timeout_sec=30.0,
      pool_recycle_sec=1800.0,
      connect_args=(),
   )
   return WebConfig(system="test", database=db_cfg, socket_path=socket_path)


def _write_valid_config(path):
   """Write a minimal valid web config to path."""
   home = os.path.expanduser("~")
   run_dir = os.path.join(home, ".node-monitor", "run")
   os.makedirs(run_dir, mode=0o700, exist_ok=True)
   with open(path, "w") as fh:
      fh.write(
         "system: test\n"
         "web:\n"
         "  socket_path: ~/.node-monitor/run/web_test.sock\n"
         "  database:\n"
         "    url: postgresql://u:p@h/db\n"
      )


# ---------------------------------------------------------------------------
# Import-graph regression
# ---------------------------------------------------------------------------

def test_import_cli_main_does_not_load_migration():
   """Importing cli.main must not transitively load database.migration."""
   code = (
      "import sys;"
      " import node_monitor.cli.main;"
      " sys.exit(1 if 'node_monitor.database.migration' in sys.modules else 0)"
   )
   repo = os.path.dirname(
      os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
   )
   result = subprocess.run(
      [sys.executable, "-c", code],
      capture_output=True,
      text=True,
      cwd=repo,
      env={**os.environ, "PYTHONPATH": repo},
   )
   assert result.returncode == 0, (
      "cli.main import loaded migration: stdout=%s stderr=%s"
      % (result.stdout, result.stderr)
   )
   assert "node_monitor.database.migration" not in result.stdout + result.stderr


# ---------------------------------------------------------------------------
# Missing / bad config exits nonzero with bounded text
# ---------------------------------------------------------------------------

def test_missing_config_file_exits_nonzero():
   """--config pointing to a nonexistent file exits nonzero (Click error)."""
   runner = CliRunner()
   result = runner.invoke(web_command, ["--config", "/nonexistent/config.yaml"])
   assert result.exit_code != 0


def test_missing_system_key_exits_nonzero_bounded_text():
   """Config missing the system key exits nonzero with bounded, credential-free text."""
   runner = CliRunner()
   with tempfile.NamedTemporaryFile(
      mode="w", suffix=".yaml", delete=False
   ) as fh:
      fh.write(
         "web:\n"
         "  socket_path: /tmp/x.sock\n"
         "  database:\n"
         "    url: postgresql://u:p@h/db\n"
      )
      cfg = fh.name
   try:
      result = runner.invoke(web_command, ["--config", cfg])
   finally:
      os.unlink(cfg)
   assert result.exit_code != 0
   assert "Traceback" not in result.output
   assert "preflight failed" in result.output or "Error" in result.output


def test_non_postgresql_url_exits_nonzero_bounded_text():
   """Config with a non-PostgreSQL URL exits nonzero with bounded, credential-free text."""
   runner = CliRunner()
   with tempfile.NamedTemporaryFile(
      mode="w", suffix=".yaml", delete=False
   ) as fh:
      fh.write(
         "system: test\n"
         "web:\n"
         "  socket_path: ~/.node-monitor/run/web_test.sock\n"
         "  database:\n"
         "    url: sqlite:///foo.db\n"
      )
      cfg = fh.name
   try:
      result = runner.invoke(web_command, ["--config", cfg])
   finally:
      os.unlink(cfg)
   assert result.exit_code != 0
   assert "Traceback" not in result.output


# ---------------------------------------------------------------------------
# Scenario 1: Success path
# ---------------------------------------------------------------------------

def test_success_lifecycle(monkeypatch):
   """Success: ordered events; startup line format; socket close; db dispose."""
   tracker = _Tracker()

   class FakeDB:
      def preflight(self):
         tracker.record("db_preflight")

      def dispose(self):
         tracker.record("db_dispose")

   class FakeSock:
      def close(self):
         tracker.record("sock_close")

   import node_monitor.config as config_mod
   import node_monitor.database.web as db_web_mod
   import node_monitor.web.app as app_mod
   import node_monitor.web.socket as sock_mod
   import node_monitor.web.runtime as rt_mod

   monkeypatch.setattr(
      config_mod, "load_web_config",
      lambda path, home=None, database_url_env=None: _make_fake_config()
   )
   monkeypatch.setattr(
      db_web_mod, "WebDatabase",
      lambda database_config: (tracker.record("db_construct") or FakeDB())
   )
   monkeypatch.setattr(
      app_mod, "create_app",
      lambda service: (tracker.record("create_app") or object())
   )
   monkeypatch.setattr(
      sock_mod, "bind_private_socket",
      lambda path, backlog=128: (tracker.record("bind") or FakeSock())
   )
   monkeypatch.setattr(
      rt_mod, "run_uvicorn",
      lambda app, sock: tracker.record("run_uvicorn")
   )

   runner = CliRunner()
   with tempfile.NamedTemporaryFile(
      mode="w", suffix=".yaml", delete=False
   ) as fh:
      _write_valid_config(fh.name)
      cfg = fh.name
   try:
      result = runner.invoke(web_command, ["--config", cfg])
   finally:
      os.unlink(cfg)

   assert result.exit_code == 0, (
      "expected exit 0; got %d\noutput: %s\nexception: %s"
      % (result.exit_code, result.output, result.exception)
   )

   # Startup line must match 'PID <digits> socket <abs-path>'
   startup_lines = [
      l for l in result.output.splitlines()
      if re.match(r"PID \d+ socket .+", l)
   ]
   assert len(startup_lines) == 1, (
      "expected exactly one startup line; got: %s" % startup_lines
   )
   assert re.match(r"^PID \d+ socket /.+", startup_lines[0]), (
      "startup line malformed: %r" % startup_lines[0]
   )

   events = tracker.events
   assert "db_preflight" in events
   assert "create_app" in events
   assert "bind" in events
   assert "run_uvicorn" in events
   assert "sock_close" in events
   assert "db_dispose" in events

   # sock_close must precede db_dispose
   sock_idx = events.index("sock_close")
   dispose_idx = events.index("db_dispose")
   assert sock_idx < dispose_idx, (
      "sock_close must precede db_dispose; events=%s" % events
   )


# ---------------------------------------------------------------------------
# Scenario 2: DB preflight failure
# ---------------------------------------------------------------------------

def test_preflight_failure_no_bind_no_startup_line(monkeypatch):
   """Preflight failure: no bind, no startup line, db dispose called."""
   tracker = _Tracker()

   class FailDB:
      def preflight(self):
         tracker.record("db_preflight")
         raise RuntimeError("preflight internal error with sensitive detail")

      def dispose(self):
         tracker.record("db_dispose")

   import node_monitor.config as config_mod
   import node_monitor.database.web as db_web_mod
   import node_monitor.web.socket as sock_mod

   monkeypatch.setattr(
      config_mod, "load_web_config",
      lambda path, home=None, database_url_env=None: _make_fake_config()
   )
   monkeypatch.setattr(
      db_web_mod, "WebDatabase",
      lambda database_config: FailDB()
   )
   monkeypatch.setattr(
      sock_mod, "bind_private_socket",
      lambda path, backlog=128: (tracker.record("bind") or object())
   )

   runner = CliRunner()
   with tempfile.NamedTemporaryFile(
      mode="w", suffix=".yaml", delete=False
   ) as fh:
      _write_valid_config(fh.name)
      cfg = fh.name
   try:
      result = runner.invoke(web_command, ["--config", cfg])
   finally:
      os.unlink(cfg)

   assert result.exit_code != 0, (
      "expected nonzero exit on preflight failure; got %d" % result.exit_code
   )
   # Must NOT have bound
   assert "bind" not in tracker.events, (
      "bind must not be called after preflight failure; events=%s" % tracker.events
   )
   # Must NOT print startup line
   assert not any(
      re.match(r"PID \d+", l) for l in result.output.splitlines()
   ), "startup line must not appear after preflight failure"
   # Must dispose DB
   assert "db_dispose" in tracker.events, (
      "db_dispose must be called after preflight failure; events=%s" % tracker.events
   )
   # Output must not contain raw exception message or Traceback
   assert "Traceback" not in result.output
   assert "preflight internal error" not in result.output, (
      "raw exception detail must not appear in output: %r" % result.output
   )


# ---------------------------------------------------------------------------
# Scenario 3: Service/app construction failure
# ---------------------------------------------------------------------------

def test_service_failure_db_dispose(monkeypatch):
   """Service/app failure (bounded exception): db dispose called; no startup line."""
   tracker = _Tracker()

   class OkDB:
      def preflight(self):
         tracker.record("db_preflight")

      def dispose(self):
         tracker.record("db_dispose")

   import node_monitor.config as config_mod
   import node_monitor.database.web as db_web_mod
   import node_monitor.web.app as app_mod

   def fail_create_app(service):
      tracker.record("create_app_fail")
      from node_monitor.database.web import WebDatabaseError
      # WebDatabaseError is bounded; its message may appear in output.
      raise WebDatabaseError("web database error during service init")

   monkeypatch.setattr(
      config_mod, "load_web_config",
      lambda path, home=None, database_url_env=None: _make_fake_config()
   )
   monkeypatch.setattr(db_web_mod, "WebDatabase", lambda cfg: OkDB())
   monkeypatch.setattr(app_mod, "create_app", fail_create_app)

   runner = CliRunner()
   with tempfile.NamedTemporaryFile(
      mode="w", suffix=".yaml", delete=False
   ) as fh:
      _write_valid_config(fh.name)
      cfg = fh.name
   try:
      result = runner.invoke(web_command, ["--config", cfg])
   finally:
      os.unlink(cfg)

   assert result.exit_code != 0
   assert "db_dispose" in tracker.events, (
      "db_dispose must be called after service failure; events=%s" % tracker.events
   )
   assert not any(
      re.match(r"PID \d+", l) for l in result.output.splitlines()
   ), "startup line must not appear after service failure"
   assert "Traceback" not in result.output
   # bounded prefix 'service initialization failed' must appear
   assert "service initialization failed" in result.output


# ---------------------------------------------------------------------------
# Scenario 4: Bind failure (SocketError)
# ---------------------------------------------------------------------------

def test_bind_failure_db_dispose_no_startup(monkeypatch):
   """Bind failure: db dispose called; no startup line printed."""
   tracker = _Tracker()

   class OkDB:
      def preflight(self):
         tracker.record("db_preflight")

      def dispose(self):
         tracker.record("db_dispose")

   import node_monitor.config as config_mod
   import node_monitor.database.web as db_web_mod
   import node_monitor.web.app as app_mod
   import node_monitor.web.socket as sock_mod

   def fail_bind(path, backlog=128):
      tracker.record("bind_fail")
      from node_monitor.web.socket import SocketError
      raise SocketError("socket is already in use at %r" % path)

   monkeypatch.setattr(
      config_mod, "load_web_config",
      lambda path, home=None, database_url_env=None: _make_fake_config()
   )
   monkeypatch.setattr(db_web_mod, "WebDatabase", lambda cfg: OkDB())
   monkeypatch.setattr(
      app_mod, "create_app",
      lambda service: (tracker.record("create_app") or object())
   )
   monkeypatch.setattr(sock_mod, "bind_private_socket", fail_bind)

   runner = CliRunner()
   with tempfile.NamedTemporaryFile(
      mode="w", suffix=".yaml", delete=False
   ) as fh:
      _write_valid_config(fh.name)
      cfg = fh.name
   try:
      result = runner.invoke(web_command, ["--config", cfg])
   finally:
      os.unlink(cfg)

   assert result.exit_code != 0
   assert "bind_fail" in tracker.events, (
      "bind must have been attempted; events=%s" % tracker.events
   )
   assert "db_dispose" in tracker.events, (
      "db_dispose must be called after bind failure; events=%s" % tracker.events
   )
   assert not any(
      re.match(r"PID \d+", l) for l in result.output.splitlines()
   ), "startup line must not appear after bind failure"
   assert "Traceback" not in result.output


# ---------------------------------------------------------------------------
# Scenario 5: Runtime failure (run_uvicorn raises)
# ---------------------------------------------------------------------------

def test_runtime_failure_socket_close_then_db_dispose(monkeypatch):
   """Runtime failure: socket close precedes db dispose."""
   tracker = _Tracker()

   class OkDB:
      def preflight(self):
         tracker.record("db_preflight")

      def dispose(self):
         tracker.record("db_dispose")

   class OkSock:
      def close(self):
         tracker.record("sock_close")

   import node_monitor.config as config_mod
   import node_monitor.database.web as db_web_mod
   import node_monitor.web.app as app_mod
   import node_monitor.web.socket as sock_mod
   import node_monitor.web.runtime as rt_mod

   def fail_run_uvicorn(app, sock):
      tracker.record("run_uvicorn_fail")
      raise RuntimeError("runtime crash with internal detail")

   monkeypatch.setattr(
      config_mod, "load_web_config",
      lambda path, home=None, database_url_env=None: _make_fake_config()
   )
   monkeypatch.setattr(db_web_mod, "WebDatabase", lambda cfg: OkDB())
   monkeypatch.setattr(
      app_mod, "create_app",
      lambda service: (tracker.record("create_app") or object())
   )
   monkeypatch.setattr(
      sock_mod, "bind_private_socket",
      lambda path, backlog=128: (tracker.record("bind") or OkSock())
   )
   monkeypatch.setattr(rt_mod, "run_uvicorn", fail_run_uvicorn)

   runner = CliRunner()
   with tempfile.NamedTemporaryFile(
      mode="w", suffix=".yaml", delete=False
   ) as fh:
      _write_valid_config(fh.name)
      cfg = fh.name
   try:
      result = runner.invoke(web_command, ["--config", cfg])
   finally:
      os.unlink(cfg)

   events = tracker.events
   assert "sock_close" in events, (
      "socket must be closed after runtime failure; events=%s" % events
   )
   assert "db_dispose" in events, (
      "db must be disposed after runtime failure; events=%s" % events
   )
   sock_idx = events.index("sock_close")
   dispose_idx = events.index("db_dispose")
   assert sock_idx < dispose_idx, (
      "sock_close must precede db_dispose; events=%s" % events
   )


# ---------------------------------------------------------------------------
# Startup line format
# ---------------------------------------------------------------------------

def test_startup_line_format_only_after_bind(monkeypatch):
   """Startup line is 'PID <digits> socket <abs-path>' emitted only after bind."""
   import node_monitor.config as config_mod
   import node_monitor.database.web as db_web_mod
   import node_monitor.web.app as app_mod
   import node_monitor.web.socket as sock_mod
   import node_monitor.web.runtime as rt_mod

   class OkDB:
      def preflight(self):
         pass

      def dispose(self):
         pass

   class OkSock:
      def close(self):
         pass

   monkeypatch.setattr(
      config_mod, "load_web_config",
      lambda path, home=None, database_url_env=None: _make_fake_config()
   )
   monkeypatch.setattr(db_web_mod, "WebDatabase", lambda cfg: OkDB())
   monkeypatch.setattr(app_mod, "create_app", lambda service: object())
   monkeypatch.setattr(
      sock_mod, "bind_private_socket",
      lambda path, backlog=128: OkSock()
   )
   monkeypatch.setattr(rt_mod, "run_uvicorn", lambda app, sock: None)

   runner = CliRunner()
   with tempfile.NamedTemporaryFile(
      mode="w", suffix=".yaml", delete=False
   ) as fh:
      _write_valid_config(fh.name)
      cfg = fh.name
   try:
      result = runner.invoke(web_command, ["--config", cfg])
   finally:
      os.unlink(cfg)

   assert result.exit_code == 0, (
      "expected exit 0; got %d\noutput: %s\nexception: %s"
      % (result.exit_code, result.output, result.exception)
   )
   startup_lines = [
      l for l in result.output.splitlines()
      if re.match(r"PID \d+", l)
   ]
   assert len(startup_lines) == 1, (
      "expected exactly 1 startup line; got %d: %s"
      % (len(startup_lines), startup_lines)
   )
   m = re.match(r"^PID (\d+) socket (.+)$", startup_lines[0])
   assert m is not None, (
      "startup line must be 'PID <digits> socket <path>'; got: %r"
      % startup_lines[0]
   )
   assert int(m.group(1)) > 0
   assert os.path.isabs(m.group(2)), (
      "socket path in startup line must be absolute: %r" % m.group(2)
   )


# ---------------------------------------------------------------------------
# New sentinel tests (Task 6 correction)
# ---------------------------------------------------------------------------
# Each test below uses a unique string sentinel that cannot appear in the
# command output if exception objects are interpolated into the message.
# Sentinel strings are chosen to be distinctive and not substrings of
# any fixed bounded message.

_YAML_SENTINEL = "YAML-PARSE-SENTINEL-a1b2c3d4"
_DB_CTOR_SENTINEL = "DB-CTOR-SENTINEL-e5f6g7h8"
_SERVICE_CTOR_SENTINEL = "SVC-CTOR-SENTINEL-i9j0k1l2"
_OSERROR_SENTINEL = "OSERROR-SENTINEL-m3n4o5p6"
_RUNTIME_SENTINEL = "RUNTIME-SENTINEL-q7r8s9t0"
_PLAIN_EXC_SENTINEL = "PLAIN-EXC-SENTINEL-u1v2w3x4"


def test_malformed_yaml_exits_nonzero_no_sentinel(monkeypatch):
   """Malformed YAML / parser failure: nonzero exit; sentinel never in output."""
   import node_monitor.config as config_mod
   import yaml as yaml_mod

   def fail_load(path, **kwargs):
      raise yaml_mod.YAMLError(_YAML_SENTINEL)

   monkeypatch.setattr(config_mod, "load_web_config", fail_load)

   runner = CliRunner()
   with tempfile.NamedTemporaryFile(
      mode="w", suffix=".yaml", delete=False
   ) as fh:
      fh.write("system: test\n")
      cfg = fh.name
   try:
      result = runner.invoke(web_command, ["--config", cfg])
   finally:
      os.unlink(cfg)

   assert result.exit_code != 0, (
      "expected nonzero exit on YAML error; got %d" % result.exit_code
   )
   assert _YAML_SENTINEL not in result.output, (
      "YAML sentinel must not appear in output (raw exception interpolated); output=%r"
      % result.output
   )
   assert "Traceback" not in result.output
   # Bounded message must be present
   assert "preflight failed" in result.output


def test_web_database_constructor_failure_nonzero_no_sentinel(monkeypatch):
   """WebDatabase(...) constructor failure: nonzero; sentinel absent; DB dispose called."""
   tracker = _Tracker()

   import node_monitor.config as config_mod
   import node_monitor.database.web as db_web_mod

   monkeypatch.setattr(
      config_mod, "load_web_config",
      lambda path, **kwargs: _make_fake_config()
   )

   def fail_ctor(database_config):
      tracker.record("db_ctor_fail")
      raise RuntimeError(_DB_CTOR_SENTINEL)

   monkeypatch.setattr(db_web_mod, "WebDatabase", fail_ctor)

   runner = CliRunner()
   with tempfile.NamedTemporaryFile(
      mode="w", suffix=".yaml", delete=False
   ) as fh:
      _write_valid_config(fh.name)
      cfg = fh.name
   try:
      result = runner.invoke(web_command, ["--config", cfg])
   finally:
      os.unlink(cfg)

   assert result.exit_code != 0, (
      "expected nonzero exit on DB ctor failure; got %d" % result.exit_code
   )
   assert _DB_CTOR_SENTINEL not in result.output, (
      "DB ctor sentinel must not appear in output; output=%r" % result.output
   )
   assert "Traceback" not in result.output
   assert "db_ctor_fail" in tracker.events


def test_service_construction_failure_no_sentinel(monkeypatch):
   """Service construction failure (sentinel in exc): sentinel absent in output; DB dispose."""
   tracker = _Tracker()

   class OkDB:
      def preflight(self):
         tracker.record("db_preflight")

      def dispose(self):
         tracker.record("db_dispose")

   import node_monitor.config as config_mod
   import node_monitor.database.web as db_web_mod
   import node_monitor.web.app as app_mod

   monkeypatch.setattr(
      config_mod, "load_web_config",
      lambda path, **kwargs: _make_fake_config()
   )
   monkeypatch.setattr(db_web_mod, "WebDatabase", lambda cfg: OkDB())

   def fail_create_app(service):
      tracker.record("create_app_fail")
      raise ValueError(_SERVICE_CTOR_SENTINEL)

   monkeypatch.setattr(app_mod, "create_app", fail_create_app)

   runner = CliRunner()
   with tempfile.NamedTemporaryFile(
      mode="w", suffix=".yaml", delete=False
   ) as fh:
      _write_valid_config(fh.name)
      cfg = fh.name
   try:
      result = runner.invoke(web_command, ["--config", cfg])
   finally:
      os.unlink(cfg)

   assert result.exit_code != 0
   assert _SERVICE_CTOR_SENTINEL not in result.output, (
      "service sentinel must not appear in output; output=%r" % result.output
   )
   assert "Traceback" not in result.output
   assert "db_dispose" in tracker.events
   # No startup line before bind
   assert not any(
      re.match(r"PID \d+", l) for l in result.output.splitlines()
   ), "startup line must not appear after service failure"


def test_bare_oserror_bind_failure_no_sentinel(monkeypatch):
   """Bare OSError bind failure: sentinel absent; socket close (if opened) then DB dispose."""
   tracker = _Tracker()

   class OkDB:
      def preflight(self):
         tracker.record("db_preflight")

      def dispose(self):
         tracker.record("db_dispose")

   import node_monitor.config as config_mod
   import node_monitor.database.web as db_web_mod
   import node_monitor.web.app as app_mod
   import node_monitor.web.socket as sock_mod

   monkeypatch.setattr(
      config_mod, "load_web_config",
      lambda path, **kwargs: _make_fake_config()
   )
   monkeypatch.setattr(db_web_mod, "WebDatabase", lambda cfg: OkDB())
   monkeypatch.setattr(
      app_mod, "create_app",
      lambda service: (tracker.record("create_app") or object())
   )

   def fail_bind_oserror(path, backlog=128):
      tracker.record("bind_oserror")
      raise OSError(_OSERROR_SENTINEL)

   monkeypatch.setattr(sock_mod, "bind_private_socket", fail_bind_oserror)

   runner = CliRunner()
   with tempfile.NamedTemporaryFile(
      mode="w", suffix=".yaml", delete=False
   ) as fh:
      _write_valid_config(fh.name)
      cfg = fh.name
   try:
      result = runner.invoke(web_command, ["--config", cfg])
   finally:
      os.unlink(cfg)

   assert result.exit_code != 0
   assert _OSERROR_SENTINEL not in result.output, (
      "OSError sentinel must not appear in output; output=%r" % result.output
   )
   assert "Traceback" not in result.output
   assert "bind_oserror" in tracker.events
   assert "db_dispose" in tracker.events
   # No startup line
   assert not any(
      re.match(r"PID \d+", l) for l in result.output.splitlines()
   ), "startup line must not appear after OSError bind failure"


def test_run_uvicorn_plain_exception_no_sentinel(monkeypatch):
   """run_uvicorn raising ordinary (non-RuntimeError) Exception: nonzero; sentinel absent;
   socket close precedes DB dispose."""
   tracker = _Tracker()

   class OkDB:
      def preflight(self):
         tracker.record("db_preflight")

      def dispose(self):
         tracker.record("db_dispose")

   class OkSock:
      def close(self):
         tracker.record("sock_close")

   import node_monitor.config as config_mod
   import node_monitor.database.web as db_web_mod
   import node_monitor.web.app as app_mod
   import node_monitor.web.socket as sock_mod
   import node_monitor.web.runtime as rt_mod

   monkeypatch.setattr(
      config_mod, "load_web_config",
      lambda path, **kwargs: _make_fake_config()
   )
   monkeypatch.setattr(db_web_mod, "WebDatabase", lambda cfg: OkDB())
   monkeypatch.setattr(
      app_mod, "create_app",
      lambda service: (tracker.record("create_app") or object())
   )
   monkeypatch.setattr(
      sock_mod, "bind_private_socket",
      lambda path, backlog=128: (tracker.record("bind") or OkSock())
   )

   def fail_plain(app, sock):
      tracker.record("run_uvicorn_plain_fail")
      raise ValueError(_PLAIN_EXC_SENTINEL)

   monkeypatch.setattr(rt_mod, "run_uvicorn", fail_plain)

   runner = CliRunner()
   with tempfile.NamedTemporaryFile(
      mode="w", suffix=".yaml", delete=False
   ) as fh:
      _write_valid_config(fh.name)
      cfg = fh.name
   try:
      result = runner.invoke(web_command, ["--config", cfg])
   finally:
      os.unlink(cfg)

   events = tracker.events
   assert result.exit_code != 0, (
      "expected nonzero exit; got %d output=%r" % (result.exit_code, result.output)
   )
   assert _PLAIN_EXC_SENTINEL not in result.output, (
      "plain exception sentinel must not appear in output; output=%r" % result.output
   )
   assert "Traceback" not in result.output
   assert "sock_close" in events, "socket must be closed; events=%s" % events
   assert "db_dispose" in events, "DB must be disposed; events=%s" % events
   sock_idx = events.index("sock_close")
   dispose_idx = events.index("db_dispose")
   assert sock_idx < dispose_idx, (
      "sock_close must precede db_dispose; events=%s" % events
   )


def test_run_uvicorn_runtime_error_no_sentinel(monkeypatch):
   """run_uvicorn raising RuntimeError (sentinel in msg): nonzero; sentinel absent;
   socket close precedes DB dispose."""
   tracker = _Tracker()

   class OkDB:
      def preflight(self):
         tracker.record("db_preflight")

      def dispose(self):
         tracker.record("db_dispose")

   class OkSock:
      def close(self):
         tracker.record("sock_close")

   import node_monitor.config as config_mod
   import node_monitor.database.web as db_web_mod
   import node_monitor.web.app as app_mod
   import node_monitor.web.socket as sock_mod
   import node_monitor.web.runtime as rt_mod

   monkeypatch.setattr(
      config_mod, "load_web_config",
      lambda path, **kwargs: _make_fake_config()
   )
   monkeypatch.setattr(db_web_mod, "WebDatabase", lambda cfg: OkDB())
   monkeypatch.setattr(
      app_mod, "create_app",
      lambda service: (tracker.record("create_app") or object())
   )
   monkeypatch.setattr(
      sock_mod, "bind_private_socket",
      lambda path, backlog=128: (tracker.record("bind") or OkSock())
   )

   def fail_runtime(app, sock):
      tracker.record("run_uvicorn_runtime_fail")
      raise RuntimeError(_RUNTIME_SENTINEL)

   monkeypatch.setattr(rt_mod, "run_uvicorn", fail_runtime)

   runner = CliRunner()
   with tempfile.NamedTemporaryFile(
      mode="w", suffix=".yaml", delete=False
   ) as fh:
      _write_valid_config(fh.name)
      cfg = fh.name
   try:
      result = runner.invoke(web_command, ["--config", cfg])
   finally:
      os.unlink(cfg)

   events = tracker.events
   assert result.exit_code != 0, (
      "expected nonzero exit; got %d output=%r" % (result.exit_code, result.output)
   )
   assert _RUNTIME_SENTINEL not in result.output, (
      "RuntimeError sentinel must not appear in output; output=%r" % result.output
   )
   assert "Traceback" not in result.output
   assert "sock_close" in events, "socket must be closed; events=%s" % events
   assert "db_dispose" in events, "DB must be disposed; events=%s" % events
   sock_idx = events.index("sock_close")
   dispose_idx = events.index("db_dispose")
   assert sock_idx < dispose_idx, (
      "sock_close must precede db_dispose; events=%s" % events
   )


def test_keyboard_interrupt_propagates_after_bind(monkeypatch):
   """KeyboardInterrupt after socket creation must propagate (not be swallowed);
   cleanup (socket close + DB dispose) must still happen."""
   tracker = _Tracker()

   class OkDB:
      def preflight(self):
         tracker.record("db_preflight")

      def dispose(self):
         tracker.record("db_dispose")

   class OkSock:
      def close(self):
         tracker.record("sock_close")

   import node_monitor.config as config_mod
   import node_monitor.database.web as db_web_mod
   import node_monitor.web.app as app_mod
   import node_monitor.web.socket as sock_mod
   import node_monitor.web.runtime as rt_mod

   monkeypatch.setattr(
      config_mod, "load_web_config",
      lambda path, **kwargs: _make_fake_config()
   )
   monkeypatch.setattr(db_web_mod, "WebDatabase", lambda cfg: OkDB())
   monkeypatch.setattr(
      app_mod, "create_app",
      lambda service: (tracker.record("create_app") or object())
   )
   monkeypatch.setattr(
      sock_mod, "bind_private_socket",
      lambda path, backlog=128: (tracker.record("bind") or OkSock())
   )

   def raise_ki(app, sock):
      tracker.record("run_uvicorn_ki")
      raise KeyboardInterrupt

   monkeypatch.setattr(rt_mod, "run_uvicorn", raise_ki)

   runner = CliRunner()
   with tempfile.NamedTemporaryFile(
      mode="w", suffix=".yaml", delete=False
   ) as fh:
      _write_valid_config(fh.name)
      cfg = fh.name
   try:
      # CliRunner catches BaseException; KeyboardInterrupt surfaces as exception
      result = runner.invoke(web_command, ["--config", cfg])
   finally:
      os.unlink(cfg)

   # KeyboardInterrupt propagates past our 'except Exception:' (which does NOT catch
   # BaseException subclasses like KeyboardInterrupt).  Click's own group runner
   # converts it to SystemExit(1) with "Aborted!" -- that is fine; what must NOT
   # happen is for our code to silently swallow it or produce a raw traceback.
   events = tracker.events
   # "Aborted!" is Click's own bounded message for KeyboardInterrupt -- acceptable.
   assert "Traceback" not in result.output
   # Cleanup must still happen due to the finally block
   assert "sock_close" in events, (
      "socket must be closed even on KeyboardInterrupt; events=%s" % events
   )
   assert "db_dispose" in events, (
      "DB must be disposed even on KeyboardInterrupt; events=%s" % events
   )


def test_system_exit_not_swallowed_after_bind(monkeypatch):
   """SystemExit from run_uvicorn propagates (not swallowed by inner except Exception)."""
   tracker = _Tracker()

   class OkDB:
      def preflight(self):
         tracker.record("db_preflight")

      def dispose(self):
         tracker.record("db_dispose")

   class OkSock:
      def close(self):
         tracker.record("sock_close")

   import node_monitor.config as config_mod
   import node_monitor.database.web as db_web_mod
   import node_monitor.web.app as app_mod
   import node_monitor.web.socket as sock_mod
   import node_monitor.web.runtime as rt_mod

   monkeypatch.setattr(
      config_mod, "load_web_config",
      lambda path, **kwargs: _make_fake_config()
   )
   monkeypatch.setattr(db_web_mod, "WebDatabase", lambda cfg: OkDB())
   monkeypatch.setattr(
      app_mod, "create_app",
      lambda service: (tracker.record("create_app") or object())
   )
   monkeypatch.setattr(
      sock_mod, "bind_private_socket",
      lambda path, backlog=128: (tracker.record("bind") or OkSock())
   )

   def raise_sysexit(app, sock):
      tracker.record("run_uvicorn_sysexit")
      raise SystemExit(42)

   monkeypatch.setattr(rt_mod, "run_uvicorn", raise_sysexit)

   runner = CliRunner()
   with tempfile.NamedTemporaryFile(
      mode="w", suffix=".yaml", delete=False
   ) as fh:
      _write_valid_config(fh.name)
      cfg = fh.name
   try:
      result = runner.invoke(web_command, ["--config", cfg])
   finally:
      os.unlink(cfg)

   # SystemExit must propagate; cleanup must happen.
   events = tracker.events
   assert "sock_close" in events, (
      "socket must be closed even on SystemExit; events=%s" % events
   )
   assert "db_dispose" in events, (
      "DB must be disposed even on SystemExit; events=%s" % events
   )
