"""TCP web CLI lifecycle regression tests (TCP --host/--port/--no-browser).

Migrated from original Unix-socket test file (git 8790425) to TCP listener.
Removed only Unix filesystem/stale-socket specific assertions; preserved
lifecycle, import isolation, DB preflight, bounded errors, sanitization,
disposal, startup/shutdown and signal behavior.
"""
import os
import re
import subprocess
import sys
import tempfile

import pytest
from click.testing import CliRunner

from node_monitor.cli.main import web_command


class _Tracker:

   def __init__(self):
      self.events = []

   def record(self, name):
      self.events.append(name)


def _make_fake_config():
   from node_monitor.config import WebConfig, DatabaseConfig
   db_cfg = DatabaseConfig(
      url="postgresql://u:***@h/db",
      schema="node_monitor",
      pool_size=1,
      max_overflow=0,
      echo_sql=False,
      pool_pre_ping=True,
      pool_timeout_sec=30.0,
      pool_recycle_sec=1800.0,
      connect_args=(),
   )
   return WebConfig(system="test", nodes=(), database=db_cfg)


def _write_valid_config(path):
   with open(path, "w") as fh:
      fh.write(
         "system: test\n"
         "web:\n"
         "  database:\n"
         "    url: postgresql://u:***@h/db\n"
      )


# ------------------------------------------------------------------
# Import isolation regression
# ------------------------------------------------------------------

def test_import_cli_main_does_not_load_migration():
   code = (
      "import sys; import node_monitor.cli.main;"
      " sys.exit(1 if 'node_monitor.database.migration' in sys.modules else 0)")
   repo = os.path.dirname(
      os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
   result = subprocess.run(
      [sys.executable, "-c", code],
      capture_output=True, text=True,
      cwd=repo,
      env={**os.environ, "PYTHONPATH": repo},
   )
   assert result.returncode == 0, (
      "cli.main import loaded migration: stdout=%s stderr=%s"
      % (result.stdout, result.stderr))
   assert "node_monitor.database.migration" not in result.stdout + result.stderr


# ------------------------------------------------------------------
# Default TCP binding (explicit default 127.0.0.1:8080)
# ------------------------------------------------------------------

def test_default_host_and_port(monkeypatch):
   import node_monitor.config as config_mod
   import node_monitor.database.web as db_web_mod
   import node_monitor.web.app as app_mod
   import node_monitor.web.runtime as rt_mod
   monkeypatch.setattr(
      config_mod, "load_web_config",
      lambda path, **kw: _make_fake_config())
   class FakeDB:
      def preflight(self): pass
      def dispose(self): pass
   monkeypatch.setattr(db_web_mod, "WebDatabase", lambda cfg: FakeDB())
   monkeypatch.setattr(app_mod, "create_app", lambda s: object())

   calls = {}

   def capture_run(app, host, port):
      calls["host"] = host
      calls["port"] = port
   monkeypatch.setattr(rt_mod, "run_uvicorn", capture_run)

   runner = CliRunner()
   with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as fh:
      _write_valid_config(fh.name)
      cfg = fh.name
   try:
      result = runner.invoke(web_command, ["--config", cfg, "--no-browser"])
   finally:
      os.unlink(cfg)
   assert result.exit_code == 0, result.output
   assert calls.get("host") == "127.0.0.1"
   assert calls.get("port") == 8080


# ------------------------------------------------------------------
# Custom host/port wiring
# ------------------------------------------------------------------

def test_custom_host_port_and_no_browser(monkeypatch):
   import node_monitor.config as config_mod
   import node_monitor.database.web as db_web_mod
   import node_monitor.web.app as app_mod
   import node_monitor.web.runtime as rt_mod

   monkeypatch.setattr(
      config_mod, "load_web_config", lambda p, **kw: _make_fake_config())

   tracker = _Tracker()

   class FakeDB:
      def preflight(self): tracker.record("preflight")
      def dispose(self): tracker.record("dispose")

   monkeypatch.setattr(db_web_mod, "WebDatabase", lambda cfg: FakeDB())
   monkeypatch.setattr(app_mod, "create_app", lambda s: object())

   calls = {}

   def capture_run(app, host, port):
      calls["host"], calls["port"] = host, port
      tracker.record("run_uvicorn:%s:%s" % (host, port))
   monkeypatch.setattr(rt_mod, "run_uvicorn", capture_run)

   runner = CliRunner()
   with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as fh:
      _write_valid_config(fh.name)
      cfg = fh.name
   try:
      result = runner.invoke(
         web_command,
         ["--config", cfg, "--host", "0.0.0.0", "--port", "9000", "--no-browser"])
   finally:
      os.unlink(cfg)
   assert result.exit_code == 0, result.output
   assert calls.get("host") == "0.0.0.0"
   assert calls.get("port") == 9000
   assert "run_uvicorn:0.0.0.0:9000" in tracker.events


# ------------------------------------------------------------------
# Wildcard browser URL normalization (monkeypatch webbrowser.open)
# ------------------------------------------------------------------

def test_wildcard_browser_url_normalization(monkeypatch):
   import webbrowser
   import node_monitor.config as config_mod
   import node_monitor.database.web as db_web_mod
   import node_monitor.web.app as app_mod
   import node_monitor.web.runtime as rt_mod

   monkeypatch.setattr(
      config_mod, "load_web_config", lambda p, **kw: _make_fake_config())
   class FakeDB:
      def preflight(self): pass
      def dispose(self): pass
   monkeypatch.setattr(db_web_mod, "WebDatabase", lambda cfg: FakeDB())
   monkeypatch.setattr(app_mod, "create_app", lambda s: object())

   opened_urls = []

   def fake_open(url, new=0, autoraise=True):
      opened_urls.append(url)
   monkeypatch.setattr(webbrowser, "open", fake_open)

   monkeypatch.setattr(
      rt_mod, "run_uvicorn", lambda app, host, port: None)

   runner = CliRunner()
   with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as fh:
      _write_valid_config(fh.name)
      cfg = fh.name
   try:
      result = runner.invoke(
         web_command, ["--config", cfg, "--host", "0.0.0.0", "--port", "8080"])
   finally:
      os.unlink(cfg)
   assert result.exit_code == 0, result.output
   assert len(opened_urls) == 1
   # Wildcard 0.0.0.0 normalized to 127.0.0.1 for browser URL
   assert opened_urls[0] == "http://127.0.0.1:8080"


def test_no_browser_suppresses_browser_open(monkeypatch):
   import webbrowser
   import node_monitor.config as config_mod
   import node_monitor.database.web as db_web_mod
   import node_monitor.web.app as app_mod
   import node_monitor.web.runtime as rt_mod

   monkeypatch.setattr(
      config_mod, "load_web_config", lambda p, **kw: _make_fake_config())
   class FakeDB:
      def preflight(self): pass
      def dispose(self): pass
   monkeypatch.setattr(db_web_mod, "WebDatabase", lambda cfg: FakeDB())
   monkeypatch.setattr(app_mod, "create_app", lambda s: object())
   monkeypatch.setattr(rt_mod, "run_uvicorn", lambda app, host, port: None)

   opened = []

   def fake_open(url, new=0, autoraise=True):
      opened.append(url)
   monkeypatch.setattr(webbrowser, "open", fake_open)

   runner = CliRunner()
   with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as fh:
      _write_valid_config(fh.name)
      cfg = fh.name
   try:
      result = runner.invoke(
         web_command, ["--config", cfg, "--host", "127.0.0.1", "--port", "8080", "--no-browser"])
   finally:
      os.unlink(cfg)
   assert result.exit_code == 0, result.output
   assert opened == []


# ------------------------------------------------------------------
# Invalid host / NUL and invalid port (before DB construction)
# ------------------------------------------------------------------

def test_invalid_host_empty_before_db():
   runner = CliRunner()
   with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as fh:
      _write_valid_config(fh.name)
      cfg = fh.name
   try:
      result = runner.invoke(web_command, ["--config", cfg, "--host", ""])
   finally:
      os.unlink(cfg)
   assert result.exit_code != 0
   assert "invalid host" in result.output


def test_invalid_host_nul_before_db():
   runner = CliRunner()
   with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as fh:
      _write_valid_config(fh.name)
      cfg = fh.name
   try:
      result = runner.invoke(
         web_command, ["--config", cfg, "--host", "bad\x00host"])
   finally:
      os.unlink(cfg)
   assert result.exit_code != 0
   assert "invalid host" in result.output


def test_invalid_port_bounds_before_db():
   runner = CliRunner()
   with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as fh:
      _write_valid_config(fh.name)
      cfg = fh.name
   try:
      result = runner.invoke(web_command, ["--config", cfg, "--port", "0"])
   finally:
      os.unlink(cfg)
   assert result.exit_code != 0
   assert "invalid port" in result.output


def test_invalid_port_above_max_before_db():
   runner = CliRunner()
   with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as fh:
      _write_valid_config(fh.name)
      cfg = fh.name
   try:
      result = runner.invoke(
         web_command, ["--config", cfg, "--port", "70000"])
   finally:
      os.unlink(cfg)
   assert result.exit_code != 0
   assert "invalid port" in result.output


# ------------------------------------------------------------------
# Config / DB / service failure lifecycle (bounded; DB disposed)
# ------------------------------------------------------------------

def test_config_failure_bounded_no_traceback(monkeypatch):
   import node_monitor.config as config_mod

   def fail_load(path, **kw):
      from node_monitor.config import ConfigError
      raise ConfigError("bad config")

   monkeypatch.setattr(config_mod, "load_web_config", fail_load)
   runner = CliRunner()
   with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as fh:
      _write_valid_config(fh.name)
      cfg = fh.name
   try:
      result = runner.invoke(web_command, ["--config", cfg])
   finally:
      os.unlink(cfg)
   assert result.exit_code != 0
   assert "Traceback" not in result.output
   assert ("preflight failed" in result.output
           or "invalid" in result.output.lower())


def test_db_preflight_failure_bounded_with_dispose(monkeypatch):
   import node_monitor.config as config_mod
   import node_monitor.database.web as db_web_mod
   monkeypatch.setattr(
      config_mod, "load_web_config", lambda p, **kw: _make_fake_config())

   tracker = _Tracker()

   class FailDB:
      def preflight(self):
         tracker.record("preflight")
         raise RuntimeError("secret internal detail")
      def dispose(self):
         tracker.record("dispose")

   monkeypatch.setattr(db_web_mod, "WebDatabase", lambda cfg: FailDB())
   runner = CliRunner()
   with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as fh:
      _write_valid_config(fh.name)
      cfg = fh.name
   try:
      result = runner.invoke(web_command, ["--config", cfg])
   finally:
      os.unlink(cfg)
   assert result.exit_code != 0
   assert "preflight" in tracker.events
   assert "dispose" in tracker.events
   assert "secret internal detail" not in result.output
   assert "Traceback" not in result.output


def test_service_init_failure_db_dispose(monkeypatch):
   import node_monitor.config as config_mod
   import node_monitor.database.web as db_web_mod
   import node_monitor.web.app as app_mod
   monkeypatch.setattr(
      config_mod, "load_web_config", lambda p, **kw: _make_fake_config())

   tracker = _Tracker()

   class OkDB:
      def preflight(self): tracker.record("preflight")
      def dispose(self): tracker.record("dispose")

   monkeypatch.setattr(db_web_mod, "WebDatabase", lambda cfg: OkDB())

   def fail_create(service):
      tracker.record("service_fail")
      raise ValueError("service init detail")

   monkeypatch.setattr(app_mod, "create_app", fail_create)
   runner = CliRunner()
   with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as fh:
      _write_valid_config(fh.name)
      cfg = fh.name
   try:
      result = runner.invoke(web_command, ["--config", cfg])
   finally:
      os.unlink(cfg)
   assert result.exit_code != 0
   assert "service_fail" in tracker.events
   assert "dispose" in tracker.events
   assert "service init detail" not in result.output
   assert "service initialization failed" in result.output


# ------------------------------------------------------------------
# Startup line format (TCP: PID <digits> http://...)
# ------------------------------------------------------------------

def test_startup_line_format(monkeypatch):
   import node_monitor.config as config_mod
   import node_monitor.database.web as db_web_mod
   import node_monitor.web.app as app_mod
   import node_monitor.web.runtime as rt_mod
   monkeypatch.setattr(
      config_mod, "load_web_config", lambda p, **kw: _make_fake_config())

   class OkDB:
      def preflight(self): pass
      def dispose(self): pass

   monkeypatch.setattr(db_web_mod, "WebDatabase", lambda cfg: OkDB())
   monkeypatch.setattr(app_mod, "create_app", lambda s: object())
   monkeypatch.setattr(rt_mod, "run_uvicorn", lambda app, host, port: None)

   runner = CliRunner()
   with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as fh:
      _write_valid_config(fh.name)
      cfg = fh.name
   try:
      result = runner.invoke(
         web_command,
         ["--config", cfg, "--host", "127.0.0.1", "--port", "9999", "--no-browser"])
   finally:
      os.unlink(cfg)
   assert result.exit_code == 0, result.output
   startup_lines = [
      l for l in result.output.splitlines()
      if re.match(r"PID \d+ http://", l)
   ]
   assert len(startup_lines) == 1
   assert re.match(r"^PID \d+ http://127\.0\.0\.1:9999$", startup_lines[0])


# ------------------------------------------------------------------
# Runtime failure: bounded; DB disposed
# ------------------------------------------------------------------

def test_runtime_failure_bounded_and_dispose(monkeypatch):
   import node_monitor.config as config_mod
   import node_monitor.database.web as db_web_mod
   import node_monitor.web.app as app_mod
   import node_monitor.web.runtime as rt_mod

   monkeypatch.setattr(
      config_mod, "load_web_config", lambda p, **kw: _make_fake_config())

   tracker = _Tracker()

   class OkDB:
      def preflight(self): tracker.record("preflight")
      def dispose(self): tracker.record("dispose")

   monkeypatch.setattr(db_web_mod, "WebDatabase", lambda cfg: OkDB())
   monkeypatch.setattr(app_mod, "create_app", lambda s: object())

   def fail_run(app, host, port):
      tracker.record("runtime_fail")
      raise RuntimeError("runtime crash with internal detail")

   monkeypatch.setattr(rt_mod, "run_uvicorn", fail_run)

   runner = CliRunner()
   with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as fh:
      _write_valid_config(fh.name)
      cfg = fh.name
   try:
      result = runner.invoke(web_command, ["--config", cfg, "--no-browser"])
   finally:
      os.unlink(cfg)
   assert result.exit_code != 0
   assert "runtime_fail" in tracker.events
   assert "dispose" in tracker.events
   assert "Traceback" not in result.output
   assert "runtime crash with internal detail" not in result.output


# ------------------------------------------------------------------
# Signal / exception propagation with disposal
# ------------------------------------------------------------------

def test_keyboard_interrupt_propagates_and_disposes(monkeypatch):
   import node_monitor.config as config_mod
   import node_monitor.database.web as db_web_mod
   import node_monitor.web.app as app_mod
   import node_monitor.web.runtime as rt_mod
   monkeypatch.setattr(
      config_mod, "load_web_config", lambda p, **kw: _make_fake_config())

   tracker = _Tracker()

   class OkDB:
      def preflight(self): tracker.record("preflight")
      def dispose(self): tracker.record("dispose")

   monkeypatch.setattr(db_web_mod, "WebDatabase", lambda cfg: OkDB())
   monkeypatch.setattr(app_mod, "create_app", lambda s: object())

   def raise_ki(app, host, port):
      tracker.record("ki")
      raise KeyboardInterrupt

   monkeypatch.setattr(rt_mod, "run_uvicorn", raise_ki)

   runner = CliRunner()
   with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as fh:
      _write_valid_config(fh.name)
      cfg = fh.name
   try:
      result = runner.invoke(web_command, ["--config", cfg, "--no-browser"])
   finally:
      os.unlink(cfg)
   assert "ki" in tracker.events
   assert "dispose" in tracker.events
   assert "Traceback" not in result.output


def test_system_exit_propagates_and_disposes(monkeypatch):
   import node_monitor.config as config_mod
   import node_monitor.database.web as db_web_mod
   import node_monitor.web.app as app_mod
   import node_monitor.web.runtime as rt_mod
   monkeypatch.setattr(
      config_mod, "load_web_config", lambda p, **kw: _make_fake_config())

   tracker = _Tracker()

   class OkDB:
      def preflight(self): tracker.record("preflight")
      def dispose(self): tracker.record("dispose")

   monkeypatch.setattr(db_web_mod, "WebDatabase", lambda cfg: OkDB())
   monkeypatch.setattr(app_mod, "create_app", lambda s: object())

   def raise_sysexit(app, host, port):
      tracker.record("sysexit")
      raise SystemExit(42)

   monkeypatch.setattr(rt_mod, "run_uvicorn", raise_sysexit)

   runner = CliRunner()
   with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as fh:
      _write_valid_config(fh.name)
      cfg = fh.name
   try:
      result = runner.invoke(web_command, ["--config", cfg, "--no-browser"])
   finally:
      os.unlink(cfg)
   assert "sysexit" in tracker.events
   assert "dispose" in tracker.events


# ------------------------------------------------------------------
# Credential sanitization / bounded error assertions
# ------------------------------------------------------------------

def test_no_raw_exception_interpolation_in_output(monkeypatch):
   import node_monitor.config as config_mod
   import node_monitor.database.web as db_web_mod
   monkeypatch.setattr(
      config_mod, "load_web_config", lambda p, **kw: _make_fake_config())

   sentinel = "SECRET-DB-SENTINEL-x7y8z9"

   class BadDB:
      def preflight(self):
         raise ValueError(sentinel)
      def dispose(self):
         pass

   monkeypatch.setattr(db_web_mod, "WebDatabase", lambda cfg: BadDB())

   runner = CliRunner()
   with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as fh:
      _write_valid_config(fh.name)
      cfg = fh.name
   try:
      result = runner.invoke(web_command, ["--config", cfg])
   finally:
      os.unlink(cfg)
   assert result.exit_code != 0
   assert sentinel not in result.output
   assert "Traceback" not in result.output
