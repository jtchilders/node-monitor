import importlib.util
from pathlib import Path

import pytest

PROBE_PATH = Path(__file__).resolve().parents[2] / "node_monitor" / "probe" / "probe.py"


@pytest.fixture(scope="session")
def probe():
   spec = importlib.util.spec_from_file_location("probe", str(PROBE_PATH))
   module = importlib.util.module_from_spec(spec)
   spec.loader.exec_module(module)
   return module
