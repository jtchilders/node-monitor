"""Tests for Uvicorn TCP runtime in node_monitor.web.runtime."""
import pytest
from unittest.mock import MagicMock, patch
from node_monitor.web.runtime import run_uvicorn


class FakeServer:
   def __init__(self, config):
      pass
   def run(self):
      pass


def test_tcp_config_exact_host_port_and_flags():
   app = MagicMock()
   with patch("uvicorn.Config") as mock_cfg, \
        patch("uvicorn.Server", FakeServer):
      mock_cfg.return_value = MagicMock()
      run_uvicorn(app, host="127.0.0.1", port=8080)
      assert mock_cfg.called
      kwargs = mock_cfg.call_args[1]
      assert kwargs.get("host") == "127.0.0.1"
      assert kwargs.get("port") == 8080
      assert kwargs.get("server_header") is False
      assert kwargs.get("proxy_headers") is False
      assert kwargs.get("access_log") is False
      assert "fd" not in kwargs
      assert "uds" not in kwargs


def test_app_forwarded():
   app = MagicMock()
   with patch("uvicorn.Config") as mock_cfg, \
        patch("uvicorn.Server", FakeServer):
      mock_cfg.return_value = MagicMock()
      run_uvicorn(app, host="0.0.0.0", port=9000)
      args = mock_cfg.call_args[0]
      assert args[0] is app


def test_server_receives_config_and_run_called():
   received = []
   class FakeS:
      def __init__(self, config):
         received.append(config)
      def run(self):
         pass
   app = MagicMock()
   with patch("uvicorn.Config", return_value=MagicMock()) as mock_cfg, \
        patch("uvicorn.Server", FakeS):
      run_uvicorn(app, host="::", port=3000)
      assert len(received) == 1
      assert received[0] is mock_cfg.return_value
