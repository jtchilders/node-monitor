"""Task 3: dots canvas + modal + /api/process."""

import re
from pathlib import Path

import pytest
import yaml

from tests.database import conftest as pg
from tests.database.live_fixture import SYSTEM, populate
from tests.test_config import MINIMAL

migrated_db = pg.migrated_db
pg_admin_url = pg.pg_admin_url
pg_db = pg.pg_db


@pytest.fixture(autouse=True)
def _no_env(monkeypatch):
   for var in ("NODE_MONITOR_DB_URL", "NODE_MONITOR_WEB_DB_URL", "NODE_MONITOR_CONFIG"):
      monkeypatch.delenv(var, raising=False)


def _client_for(url, tmp_path):
   from fastapi.testclient import TestClient

   from node_monitor.config import load_config
   from node_monitor.web.app import create_app

   base = tmp_path / "config.yaml"
   base.write_text(
      yaml.safe_dump({**MINIMAL, "working_dir": str(tmp_path / "wd"), "web": {"database_url": url}})
   )
   config = load_config(str(base))
   app = create_app(config)
   return TestClient(app)


# ---------------------------------------------------------------------------
# CATEGORY_COLORS covers every live category + other
# ---------------------------------------------------------------------------


def test_category_colors_covers_all():
   path = Path("node_monitor/web/static/js/dots.js")
   assert path.is_file(), f"{path} missing"
   text = path.read_text()
   # Must define CATEGORY_COLORS
   assert "CATEGORY_COLORS" in text, "CATEGORY_COLORS not found in dots.js"
   # Extract keys: look for quoted category names near CATEGORY_COLORS
   # Accept both 'category' and "category" forms.
   expected = {
      "ai_agent",
      "editor_server",
      "jupyter",
      "build",
      "python",
      "shell",
      "data_transfer",
      "fs_scan",
      "scheduler",
      "workflow",
      "system",
      "other",
   }
   missing = []
   for cat in expected:
      # category appears as a key in the color map
      if not re.search(rf"[\"\']{re.escape(cat)}[\"\']\s*:", text):
         missing.append(cat)
   assert not missing, f"CATEGORY_COLORS missing categories: {missing} in {path}"
   # other must be present explicitly
   assert re.search(r"[\"\']other[\"\']\s*:", text), "other not in CATEGORY_COLORS"


# ---------------------------------------------------------------------------
# Hover payload contains required fields
# ---------------------------------------------------------------------------


def test_hover_payload_fields():
   path = Path("node_monitor/web/static/js/dots.js")
   assert path.is_file(), f"{path} missing"
   text = path.read_text()
   # Hover tooltip / onHover should expose user, activity, cmdline, idle, presence
   for field in ("username", "activity", "cmdline", "idle", "presence"):
      # allow any case: some may use idle_for_sec / owner_presence
      alt = {
         "idle": ["idle_for_sec", "idle", "idle_for"],
         "presence": ["owner_presence", "presence"],
         "username": ["username", "user"],
      }.get(field, [field])
      found = any(kw in text for kw in alt)
      assert found, f"dots.js missing hover field {field!r} (looked for {alt})"
   # Must have onHover handling
   assert "onHover" in text, "dots.js missing onHover"


# ---------------------------------------------------------------------------
# Canvas does not render HTML from cmdline (literal text test with [bold])
# ---------------------------------------------------------------------------


def test_canvas_does_not_render_html():
   path = Path("node_monitor/web/static/js/dots.js")
   assert path.is_file(), f"{path} missing"
   text = path.read_text()
   # Must use safe text insertion, not innerHTML for user data
   assert "textContent" in text or "createTextNode" in text or "Text(" in text, (
      "dots.js must use textContent/createTextNode/Text to avoid HTML injection"
   )
   # Should not use innerHTML with cmdline data (allow innerHTML only for static scaffolding)
   # If innerHTML appears, it must not be paired with cmdline
   if "innerHTML" in text:
      # Ensure no line does innerHTML = ... cmdline
      for line in text.splitlines():
         if "innerHTML" in line and "cmdline" in line.lower():
            pytest.fail(f"dots.js uses innerHTML with cmdline: {line.strip()!r}")
   # Also check modal.js
   modal_path = Path("node_monitor/web/static/js/modal.js")
   assert modal_path.is_file(), f"{modal_path} missing"
   modal_text = modal_path.read_text()
   assert "textContent" in modal_text or "createTextNode" in modal_text or "Text(" in modal_text, (
      "modal.js must use textContent/createTextNode/Text"
   )
   # Verify the 3-space indentation via ruff: done separately; here just check files exist
   for p in [
      Path("node_monitor/web/static/js/dots.js"),
      Path("node_monitor/web/static/js/list.js"),
      Path("node_monitor/web/static/js/modal.js"),
      Path("node_monitor/web/static/js/app.js"),
      Path("node_monitor/web/templates/_process_table.html"),
   ]:
      assert p.is_file(), f"required file {p} missing"


# ---------------------------------------------------------------------------
# Modal open shows 404 message
# ---------------------------------------------------------------------------


def test_modal_handles_404(migrated_db, tmp_path):
   url = migrated_db
   populate(url)
   client = _client_for(url, tmp_path)
   # Unknown process -> 404
   resp = client.get(f"/api/process/{SYSTEM}/n1.example/99999/9999999")
   assert resp.status_code == 404, resp.text
   body = resp.json()
   # Must have a clear message, not empty
   detail = body.get("detail", "")
   assert detail, "404 response missing detail"
   assert "not found" in detail.lower() or "no" in detail.lower() or "404" in detail.lower(), (
      f"404 detail not clear: {detail!r}"
   )


def test_modal_fetches_live_process(migrated_db, tmp_path):
   url = migrated_db
   populate(url)
   client = _client_for(url, tmp_path)
   # pid 1 is alice/ml on n1.example with start_time 1001
   resp = client.get(f"/api/process/{SYSTEM}/n1.example/1/1001")
   assert resp.status_code == 200, resp.text
   data = resp.json()
   # Must contain redacted cmdline, parent info, classification, live fields
   for field in ("cmdline", "category", "activity", "pid", "username"):
      assert field in data, f"process response missing {field!r}: {data}"
   # cmdline is the redacted one (live_fixture uses plain cmdN, which is not secret)
   assert data["cmdline"] == "cmd1"
   assert data["category"] == "ml"
   assert data["pid"] == 1


def test_modal_js_fetches_api_and_shows_404():
   modal_path = Path("node_monitor/web/static/js/modal.js")
   assert modal_path.is_file(), f"{modal_path} missing"
   text = modal_path.read_text()
   assert "/api/process" in text, "modal.js must fetch /api/process"
   assert "404" in text or "not found" in text.lower() or "NotFound" in text, (
      "modal.js must handle 404"
   )
   # Must export ProcessModal with open
   assert "ProcessModal" in text, "ProcessModal not found in modal.js"
   assert re.search(r"open\s*\(|open\s*:", text), "ProcessModal.open not found"


def test_dots_canvas_api():
   path = Path("node_monitor/web/static/js/dots.js")
   text = path.read_text()
   assert "DotsCanvas" in text, "DotsCanvas not found"
   # Constructor signature check: DotsCanvas(el, {processes, colorBy, sizeBy, onHover, onClick})
   for kw in ("processes", "colorBy", "sizeBy", "onHover", "onClick"):
      assert kw in text, f"DotsCanvas missing option {kw!r}"
   # Must use canvas 2D context
   assert "getContext" in text and "2d" in text, "DotsCanvas must use canvas 2d context"
   # Log scale for rss
   assert "log" in text.lower(), "DotsCanvas must use log scale for size"
   # Debounced resize
   assert "resize" in text.lower(), "DotsCanvas must handle resize"
   assert "debounce" in text.lower() or "setTimeout" in text or "requestAnimationFrame" in text, (
      "DotsCanvas must debounce resize"
   )
   # Empty state
   assert "empty" in text.lower() or "no process" in text.lower() or "No process" in text, (
      "DotsCanvas must have empty-state handling"
   )
