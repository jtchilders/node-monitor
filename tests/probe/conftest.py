import importlib.util
import os
from pathlib import Path

import pytest

PROBE_PATH = Path(__file__).resolve().parents[2] / "node_monitor" / "probe" / "probe.py"


@pytest.fixture(scope="session")
def probe():
   spec = importlib.util.spec_from_file_location("probe", str(PROBE_PATH))
   module = importlib.util.module_from_spec(spec)
   spec.loader.exec_module(module)
   return module


@pytest.fixture
def fake_bin(tmp_path, monkeypatch):
   """Return fake_bin(name, shell_body): an executable stand-in first on PATH."""
   bin_dir = tmp_path / "fake-bin"
   bin_dir.mkdir()
   monkeypatch.setenv("PATH", str(bin_dir) + os.pathsep + os.environ.get("PATH", ""))

   def make(name, body):
      path = bin_dir / name
      path.write_text("#!/bin/sh\n" + body + "\n")
      path.chmod(0o755)
      return path

   return make
