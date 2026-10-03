"""RED tests for Task 6: route/static/CLI/packaging."""
import pytest
from fastapi.testclient import TestClient


def test_app_exists():
   from node_monitor.web.app import create_app
   assert callable(create_app)

def test_health_never_touches_database():
   # RED: no app yet
   pass
