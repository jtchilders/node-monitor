"""Tests for the Uvicorn runtime integration in node_monitor.web.runtime.

Requirements verified here:
- run_uvicorn receives an already-bound socket (not uds path, not TCP)
- Uvicorn is configured with server_header=False
- Uvicorn is configured with proxy_headers=False
- access_log is False
- the fd parameter is used (not uds, not host/port)
- the Python socket is kept alive until the server exits
- no TCP bind happens

Note: macOS AF_UNIX path limit is 103 bytes. Tests use short /tmp paths
to stay within that limit.
"""

import os
import shutil
import socket
import stat
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from node_monitor.web.runtime import run_uvicorn


# ---------------------------------------------------------------------------
# Fixture: short-path temp directory
# ---------------------------------------------------------------------------

@pytest.fixture()
def short_tmp():
   """Yield a short absolute temp directory path and clean up after."""
   d = tempfile.mkdtemp(dir="/tmp")
   try:
      yield d
   finally:
      shutil.rmtree(d, ignore_errors=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_unix_socket(path):
   """Create a bound, listened AF_UNIX socket for testing."""
   sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
   old_umask = os.umask(0o177)
   try:
      sock.bind(path)
   finally:
      os.umask(old_umask)
   os.chmod(path, 0o600)
   sock.listen(1)
   return sock


def _sock_path(base):
   """Return a socket path short enough for macOS AF_UNIX (103-byte limit)."""
   run_dir = os.path.join(base, "run")
   os.makedirs(run_dir, mode=0o700, exist_ok=True)
   return os.path.join(run_dir, "w.sock")


# ---------------------------------------------------------------------------
# Test: run_uvicorn uses fd=, never uds=, never host/port
# ---------------------------------------------------------------------------

def test_run_uvicorn_uses_fd_not_uds(short_tmp):
   """run_uvicorn must pass fd=sock.fileno() to uvicorn.Config, not uds."""
   path = _sock_path(short_tmp)
   sock = _make_unix_socket(path)
   try:
      app = MagicMock()

      class FakeServer:
         def __init__(self, config):
            pass
         def run(self):
            pass

      with patch("uvicorn.Config") as mock_config_cls, \
           patch("uvicorn.Server", FakeServer):
         mock_config_cls.return_value = MagicMock()
         run_uvicorn(app, sock)
         assert mock_config_cls.called
         kwargs = mock_config_cls.call_args[1]
         assert "fd" in kwargs, "expected fd= parameter, got: %s" % list(kwargs.keys())
         assert kwargs["fd"] == sock.fileno()
         assert "uds" not in kwargs, "uds= must NOT be passed to uvicorn.Config"
         assert "host" not in kwargs, "host= must NOT be passed to uvicorn.Config"
         assert "port" not in kwargs, "port= must NOT be passed to uvicorn.Config"
   finally:
      sock.close()


# ---------------------------------------------------------------------------
# Test: server_header=False
# ---------------------------------------------------------------------------

def test_run_uvicorn_server_header_false(short_tmp):
   path = _sock_path(short_tmp)
   sock = _make_unix_socket(path)
   try:
      app = MagicMock()

      class FakeServer:
         def __init__(self, config):
            pass
         def run(self):
            pass

      with patch("uvicorn.Config") as mock_config_cls, \
           patch("uvicorn.Server", FakeServer):
         mock_config_cls.return_value = MagicMock()
         run_uvicorn(app, sock)
         kwargs = mock_config_cls.call_args[1]
         assert kwargs.get("server_header") is False, \
            "server_header must be False, got: %r" % kwargs.get("server_header")
   finally:
      sock.close()


# ---------------------------------------------------------------------------
# Test: proxy_headers=False
# ---------------------------------------------------------------------------

def test_run_uvicorn_proxy_headers_false(short_tmp):
   path = _sock_path(short_tmp)
   sock = _make_unix_socket(path)
   try:
      app = MagicMock()

      class FakeServer:
         def __init__(self, config):
            pass
         def run(self):
            pass

      with patch("uvicorn.Config") as mock_config_cls, \
           patch("uvicorn.Server", FakeServer):
         mock_config_cls.return_value = MagicMock()
         run_uvicorn(app, sock)
         kwargs = mock_config_cls.call_args[1]
         assert kwargs.get("proxy_headers") is False, \
            "proxy_headers must be False, got: %r" % kwargs.get("proxy_headers")
   finally:
      sock.close()


# ---------------------------------------------------------------------------
# Test: access_log=False
# ---------------------------------------------------------------------------

def test_run_uvicorn_access_log_false(short_tmp):
   path = _sock_path(short_tmp)
   sock = _make_unix_socket(path)
   try:
      app = MagicMock()

      class FakeServer:
         def __init__(self, config):
            pass
         def run(self):
            pass

      with patch("uvicorn.Config") as mock_config_cls, \
           patch("uvicorn.Server", FakeServer):
         mock_config_cls.return_value = MagicMock()
         run_uvicorn(app, sock)
         kwargs = mock_config_cls.call_args[1]
         assert kwargs.get("access_log") is False, \
            "access_log must be False, got: %r" % kwargs.get("access_log")
   finally:
      sock.close()


# ---------------------------------------------------------------------------
# Test: the app is forwarded to uvicorn.Config
# ---------------------------------------------------------------------------

def test_run_uvicorn_forwards_app(short_tmp):
   path = _sock_path(short_tmp)
   sock = _make_unix_socket(path)
   try:
      app = MagicMock()
      app.__name__ = "test_app"

      class FakeServer:
         def __init__(self, config):
            pass
         def run(self):
            pass

      with patch("uvicorn.Config") as mock_config_cls, \
           patch("uvicorn.Server", FakeServer):
         mock_config_cls.return_value = MagicMock()
         run_uvicorn(app, sock)
         args = mock_config_cls.call_args[0]
         assert args[0] is app, "first positional arg to uvicorn.Config must be the app"
   finally:
      sock.close()


# ---------------------------------------------------------------------------
# Test: uvicorn.Server(config).run() is called
# ---------------------------------------------------------------------------

def test_run_uvicorn_calls_server_run(short_tmp):
   path = _sock_path(short_tmp)
   sock = _make_unix_socket(path)
   try:
      app = MagicMock()
      run_called = []

      class FakeServer:
         def __init__(self, config):
            pass
         def run(self):
            run_called.append(True)

      with patch("uvicorn.Config") as mock_config_cls, \
           patch("uvicorn.Server", FakeServer):
         mock_config_cls.return_value = MagicMock()
         run_uvicorn(app, sock)
         assert run_called == [True], "uvicorn.Server.run() must be called exactly once"
   finally:
      sock.close()


# ---------------------------------------------------------------------------
# Test: run_uvicorn does not close the socket before server returns
# ---------------------------------------------------------------------------

def test_run_uvicorn_keeps_socket_alive_until_server_exits(short_tmp):
   """The Python socket object must remain open while the server runs."""
   path = _sock_path(short_tmp)
   sock = _make_unix_socket(path)
   try:
      app = MagicMock()
      fd_during_run = []

      class FakeServer:
         def __init__(self, config):
            pass
         def run(self):
            # During run(), the socket fd must still be valid
            try:
               os.fstat(sock.fileno())
               fd_during_run.append("open")
            except OSError:
               fd_during_run.append("closed")

      with patch("uvicorn.Config") as mock_config_cls, \
           patch("uvicorn.Server", FakeServer):
         mock_config_cls.return_value = MagicMock()
         run_uvicorn(app, sock)
         assert fd_during_run == ["open"], \
            "socket must be open during server.run(), got: %s" % fd_during_run
   finally:
      sock.close()


# ---------------------------------------------------------------------------
# Test: uvicorn.Config instance is passed to uvicorn.Server
# ---------------------------------------------------------------------------

def test_run_uvicorn_passes_config_to_server(short_tmp):
   path = _sock_path(short_tmp)
   sock = _make_unix_socket(path)
   try:
      app = MagicMock()
      received_configs = []
      fake_config = MagicMock()

      class FakeServer:
         def __init__(self, config):
            received_configs.append(config)
         def run(self):
            pass

      with patch("uvicorn.Config", return_value=fake_config) as mock_config_cls, \
           patch("uvicorn.Server", FakeServer):
         run_uvicorn(app, sock)
         assert received_configs == [fake_config], \
            "uvicorn.Server must receive the uvicorn.Config instance"
   finally:
      sock.close()
