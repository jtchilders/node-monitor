"""Complete browser acceptance matrix for the node-monitor dashboard
(operational-web-dashboard plan Task 9, Step 4).

Reuses the real production static bytes and the existing browser harness
from ``tests/browser/conftest.py`` (``browser_page``, ``live_web``,
``snapshot_complete``) rather than duplicating a fake implementation --
every assertion here drives the actual served ``index.html``/``app.js``/
``styles.css``/``chart.umd.min.js`` through a real Chromium page.

``browser_page``'s own request router already aborts every request whose
host is not ``127.0.0.1``/``localhost`` (see conftest.py's ``route_request``)
and appends aborted hosts to ``external`` -- every test below asserts
``external == []`` as its abort/fail-on-non-local-request proof.

Isolation note: ``browser_page`` and ``live_web`` (tests/browser/conftest.py)
are both plain function-scoped fixtures (no ``scope=`` override) -- every
test in this module gets its own fresh Playwright browser context/page and
its own fresh ``LiveWeb`` HTTP server instance and port. There is no shared
mutable state across tests (the deliberate external-request abort list,
``external``, and the server's mutable ``WebState`` are both created fresh
per fixture instantiation), so no additional per-test reset/isolation
assertion is added here -- it would be ceremonial rather than meaningful.

Covers the full Task 9 Step 4 matrix:
  * all states (loading, connected/current, stale, partial, empty,
    connection failure, disconnect-after-first-load)
  * all five time ranges (1h, 3h, 6h, 12h, 24h)
  * two distinct nodes (switching node preserves range/controls)
  * username match / no-match
  * network/Lustre toggle controls and process toggle controls
  * desktop and 400px narrow layouts
  * local age advancement (no extra network requests from the age ticker)
  * first-load failure vs. later disconnect (distinct messaging)
  * stale / partial / empty data states
  * packaged static assets (served bytes, not literal source-tree reads)
  * abort/fail on every non-local external request
"""

import copy

from playwright.sync_api import expect


CONNECTED_TIMEOUT = 10000  # ms


def open_dashboard(browser_page, live_web):
   page, errors, external = browser_page
   page.goto(live_web.url + "/")
   expect(page.locator('[data-testid="connectivity-status"]')).to_have_text(
      "Connected", timeout=CONNECTED_TIMEOUT)
   return page, errors, external


# ---------------------------------------------------------------------------
# States: loading -> connected/current
# ---------------------------------------------------------------------------

def test_state_loading_then_connected_current(browser_page, live_web):
   page, errors, external = browser_page
   gate = live_web.state.delay()
   page.goto(live_web.url + "/", wait_until="domcontentloaded")
   expect(page.locator('[data-testid="connectivity-status"]')).to_have_text(
      "Loading…", timeout=CONNECTED_TIMEOUT)
   gate.set()
   expect(page.locator('[data-testid="connectivity-status"]')).to_have_text(
      "Connected", timeout=CONNECTED_TIMEOUT)
   expect(page.locator('[data-testid="counter-freshness"]')).to_have_text(
      "Current")
   assert errors == []
   assert external == []


# ---------------------------------------------------------------------------
# States: first-load failure vs. later disconnect (distinct messaging)
# ---------------------------------------------------------------------------

def test_state_first_load_failure_shows_retry(browser_page, live_web):
   page, errors, external = browser_page
   live_web.state.fail()
   page.goto(live_web.url + "/")
   expect(page.locator('[data-testid="connectivity-status"]')).to_have_text(
      "Connection failure", timeout=CONNECTED_TIMEOUT)
   expect(page.locator('[data-testid="retry-btn"]')).to_be_visible()
   assert external == []


def test_state_later_disconnect_differs_from_first_load_failure(
      browser_page, live_web):
   page, errors, external = open_dashboard(browser_page, live_web)
   before_cpu = page.locator('[data-testid="cpu-busy"]').inner_text()
   live_web.state.fail()
   page.locator('[data-range="3h"]').click()
   expect(page.locator('[data-testid="connectivity-status"]')).to_have_text(
      "Web server disconnected", timeout=CONNECTED_TIMEOUT)
   # Distinct from the first-load failure message.
   assert page.locator(
      '[data-testid="connectivity-status"]').inner_text() != "Connection failure"
   # Last-known data is retained, not cleared, on a later disconnect.
   assert page.locator('[data-testid="cpu-busy"]').inner_text() == before_cpu
   assert external == []


# ---------------------------------------------------------------------------
# States: stale / partial / empty
# ---------------------------------------------------------------------------

def test_state_stale(browser_page, live_web, snapshot_complete):
   snap = copy.deepcopy(snapshot_complete)
   snap["counters"]["status"] = "stale"
   snap["counters"]["is_fresh"] = False
   live_web.state.set_snapshot(snap)
   page, errors, external = open_dashboard(browser_page, live_web)
   expect(page.locator('[data-testid="counter-freshness"]')).to_have_text(
      "Stale")
   assert errors == []
   assert external == []


def test_state_partial(browser_page, live_web, snapshot_complete):
   snap = copy.deepcopy(snapshot_complete)
   snap["counters"]["status"] = "partial"
   snap["usage"]["status"] = "partial"
   live_web.state.set_snapshot(snap)
   page, errors, external = open_dashboard(browser_page, live_web)
   expect(page.locator('[data-testid="counter-freshness"]')).to_have_text(
      "Partial")
   expect(page.locator('[data-testid="usage-freshness"]')).to_have_text(
      "Partial")
   assert errors == []
   assert external == []


def test_state_empty_does_not_invent_zero(browser_page, live_web,
                                           snapshot_complete):
   empty = copy.deepcopy(snapshot_complete)
   empty["counters"].update({
      "rows": [], "latest": None, "newest_window_end": None,
      "status": "empty", "is_fresh": False})
   empty["usage"].update({
      "grains": [], "newest_interval_end": None,
      "status": "empty", "is_fresh": False})
   live_web.state.set_snapshot(empty)
   page, errors, external = open_dashboard(browser_page, live_web)
   expect(page.locator('[data-testid="counter-freshness"]')).to_have_text(
      "Empty")
   assert page.locator('[data-testid="cpu-busy"]').inner_text() == "-"
   assert errors == []
   assert external == []


# ---------------------------------------------------------------------------
# All five time ranges
# ---------------------------------------------------------------------------

def test_all_five_ranges_issue_requests_and_activate(browser_page, live_web):
   page, errors, external = open_dashboard(browser_page, live_web)
   for value in ("1h", "3h", "6h", "12h", "24h"):
      button = page.locator('[data-range="%s"]' % value)
      button.click()
      expect(button).to_have_attribute("aria-pressed", "true")
      assert live_web.state.requests[-1]["range"] == [value]
   assert errors == []
   assert external == []


# ---------------------------------------------------------------------------
# Two distinct nodes
# ---------------------------------------------------------------------------

def test_two_nodes_node_switch_preserves_range(
      browser_page, live_web, snapshot_complete):
   live_web.state.set_inventory([
      {"id": "login-04", "label": "login-04", "configured": True,
       "role": "local"},
      {"id": "login-05", "label": "login-05", "configured": True,
       "role": "remote"},
   ])
   page, errors, external = open_dashboard(browser_page, live_web)
   page.locator('[data-range="6h"]').click()

   switched = copy.deepcopy(snapshot_complete)
   switched["node"] = "login-05"
   switched["hardware"]["source_hostname"] = "login-05"
   live_web.state.set_snapshot(switched)
   page.locator('[data-node-id="login-05"]').click()
   expect(page.locator('[data-testid="node-name"]')).to_have_text("login-05")
   assert live_web.state.requests[-1]["node"] == ["login-05"]
   assert live_web.state.requests[-1]["range"] == ["6h"]
   assert errors == []
   assert external == []


# ---------------------------------------------------------------------------
# Username match / no-match
# ---------------------------------------------------------------------------

def test_username_match_and_no_match(browser_page, live_web,
                                      snapshot_complete):
   page, errors, external = open_dashboard(browser_page, live_web)

   page.locator("#user-input").fill("alice")
   page.locator('[data-testid="user-submit"]').click()
   expect(page.locator('[data-testid="current-username"]')).to_have_text(
      "alice")
   assert live_web.state.requests[-1]["username"] == ["alice"]
   expect(page.locator('[data-testid="username-result"]')).to_have_text(
      "Username matched")

   no_match = copy.deepcopy(snapshot_complete)
   no_match["usage"]["grains"] = []
   live_web.state.set_snapshot(no_match)
   page.locator('[data-testid="user-submit"]').click()
   expect(page.locator('[data-testid="username-result"]')).to_have_text(
      "No matching usage")
   assert errors == []
   assert external == []


# ---------------------------------------------------------------------------
# Network/Lustre and process toggle controls
# ---------------------------------------------------------------------------

def test_network_lustre_and_process_toggle_controls(browser_page, live_web):
   page, errors, external = open_dashboard(browser_page, live_web)
   page.wait_for_function(
      "() => window.__nodeMonitorTest "
      "&& window.__nodeMonitorTest.chartData "
      "&& window.__nodeMonitorTest.chartData.cpu.labels.length > 0",
      timeout=CONNECTED_TIMEOUT)

   for testid, expected_mode in (
      ("nl-network-p95", "nl-network-p95"),
      ("nl-network-max", "nl-network-max"),
      ("nl-network-p50", "nl-network-p50"),
      ("nl-mode-lustre-p50", "nl-mode-lustre-p50"),
      ("nl-mode-lustre-p95", "nl-mode-lustre-p95"),
      ("nl-mode-lustre-peak", "nl-mode-lustre-peak"),
      ("nl-mode-lustre-targets", "nl-mode-lustre-targets"),
   ):
      page.locator('[data-testid="%s"]' % testid).click()
      page.wait_for_function(
         "() => window.__nodeMonitorTest.getChartRenderState()"
         ".networkLustre.mode === '%s'" % expected_mode,
         timeout=CONNECTED_TIMEOUT)
      assert page.locator(
         '[data-testid="%s"]' % testid).get_attribute("aria-pressed") == "true"

   for testid, expected_mode in (
      ("proc-mode-max", "proc-mode-max"),
      ("proc-mode-rss", "proc-mode-rss"),
      ("proc-mode-cpusum", "proc-mode-cpusum"),
   ):
      page.locator('[data-testid="%s"]' % testid).click()
      page.wait_for_function(
         "() => window.__nodeMonitorTest.getChartRenderState()"
         ".process.mode === '%s'" % expected_mode,
         timeout=CONNECTED_TIMEOUT)
      assert page.locator(
         '[data-testid="%s"]' % testid).get_attribute("aria-pressed") == "true"

   assert errors == []
   assert external == []


# ---------------------------------------------------------------------------
# Desktop / 400px narrow layouts
# ---------------------------------------------------------------------------

def test_desktop_layout_keeps_controls_visible(browser_page, live_web):
   page, errors, external = open_dashboard(browser_page, live_web)
   page.set_viewport_size({"width": 1280, "height": 900})
   for testid in ("counter-quality", "counter-freshness", "usage-freshness",
                  "hardware-context", "cpu-busy"):
      expect(page.locator('[data-testid="%s"]' % testid)).to_be_visible()
   assert errors == []
   assert external == []


def test_narrow_400px_layout_keeps_controls_accessible(browser_page, live_web):
   page, errors, external = open_dashboard(browser_page, live_web)
   page.set_viewport_size({"width": 400, "height": 800})
   for testid in ("counter-quality", "counter-freshness", "usage-freshness",
                  "hardware-context", "cpu-busy"):
      expect(page.locator('[data-testid="%s"]' % testid)).to_be_visible()
   page.locator('[data-range="1h"]').focus()
   assert page.locator('[data-range="1h"]').evaluate(
      "el => getComputedStyle(el).outlineStyle") != "none"
   no_overflow = page.evaluate(
      "() => document.body.scrollWidth <= document.body.clientWidth + 2")
   assert no_overflow
   assert errors == []
   assert external == []


# ---------------------------------------------------------------------------
# Local age advancement -- no extra network request from the age ticker
# ---------------------------------------------------------------------------

def test_local_age_advancement_causes_no_network_request(
      browser_page, live_web):
   page, errors, external = open_dashboard(browser_page, live_web)
   count_before = len(live_web.state.requests)

   def age_seconds():
      import re
      text = page.locator('[data-testid="data-age-seconds"]').inner_text()
      return int(re.search(r"(\d+)s", text).group(1))

   first = age_seconds()
   page.wait_for_timeout(3100)
   assert age_seconds() >= first + 2
   assert len(live_web.state.requests) == count_before
   assert errors == []
   assert external == []


# ---------------------------------------------------------------------------
# Packaged static assets: served, real production bytes
# ---------------------------------------------------------------------------

def test_packaged_static_assets_are_served_and_match_production_bytes(
      browser_page, live_web):
   """The dashboard's served static assets are byte-identical to the real
   packaged ``node_monitor/web/static/*`` files -- proving this harness
   exercises the real production static payload, not a duplicated fixture.
   """
   from pathlib import Path

   page, errors, external = browser_page
   static_dir = (Path(__file__).resolve().parents[2]
                 / "node_monitor" / "web" / "static")

   for name, content_type_fragment in (
      ("index.html", "text/html"),
      ("styles.css", "text/css"),
      ("app.js", "javascript"),
      ("chart.umd.min.js", "javascript"),
   ):
      response = page.request.get(live_web.url + "/static/" + name)
      assert response.ok, "static asset %s must be served" % name
      assert content_type_fragment in response.headers.get(
         "content-type", "")
      served_bytes = response.body()
      real_bytes = (static_dir / name).read_bytes()
      assert served_bytes == real_bytes, (
         "served %s bytes differ from the real packaged asset" % name)

   assert errors == []
   assert external == []


def test_root_serves_real_index_html(browser_page, live_web):
   from pathlib import Path

   page, errors, external = browser_page
   static_dir = (Path(__file__).resolve().parents[2]
                 / "node_monitor" / "web" / "static")
   response = page.request.get(live_web.url + "/")
   assert response.ok
   assert response.body() == (static_dir / "index.html").read_bytes()
   assert errors == []
   assert external == []


# ---------------------------------------------------------------------------
# Abort/fail on every non-local external request
# ---------------------------------------------------------------------------

def test_abort_on_external_request_is_enforced_by_harness(
      browser_page, live_web):
   """The ``browser_page`` fixture's own router aborts any request to a
   non-local host and records it in ``external`` -- demonstrate that this
   abort mechanism actually fires (not merely never triggered) by having
   the page itself attempt one external fetch, then assert it was aborted
   and never silently succeeded.
   """
   page, errors, external = open_dashboard(browser_page, live_web)

   result = page.evaluate("""
      () => fetch('https://example.invalid/should-be-aborted')
         .then(() => 'unexpectedly-succeeded')
         .catch((e) => 'aborted:' + e.name)
   """)
   assert result.startswith("aborted:"), (
      "external fetch must be aborted by the harness, got: %r" % result)
   assert any("example.invalid" in url for url in external), (
      "aborted external URL must be recorded: %r" % external)


def test_no_external_requests_on_full_interaction_sequence(
      browser_page, live_web, snapshot_complete):
   """Across the full interaction sequence (ranges, node switch, username
   filter, toggle controls, narrow layout), zero non-local requests occur.
   """
   live_web.state.set_inventory([
      {"id": "login-04", "label": "login-04", "configured": True,
       "role": "local"},
      {"id": "login-05", "label": "login-05", "configured": True,
       "role": "remote"},
   ])
   page, errors, external = open_dashboard(browser_page, live_web)

   for value in ("1h", "3h", "6h", "12h", "24h"):
      page.locator('[data-range="%s"]' % value).click()

   switched = copy.deepcopy(snapshot_complete)
   switched["node"] = "login-05"
   live_web.state.set_snapshot(switched)
   page.locator('[data-node-id="login-05"]').click()

   page.locator("#user-input").fill("alice")
   page.locator('[data-testid="user-submit"]').click()

   page.wait_for_function(
      "() => window.__nodeMonitorTest "
      "&& window.__nodeMonitorTest.chartData "
      "&& window.__nodeMonitorTest.chartData.cpu.labels.length > 0",
      timeout=CONNECTED_TIMEOUT)
   page.locator('[data-testid="nl-network-p95"]').click()
   page.locator('[data-testid="proc-mode-max"]').click()

   page.set_viewport_size({"width": 400, "height": 800})

   assert external == [], "no external requests must occur: %r" % external
