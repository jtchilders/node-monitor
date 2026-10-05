"""Visual-system/state/responsive acceptance assertions (Task 2 + 3): responsive layout, focus outline, reduced motion, lifecycle stability, disconnect state retention."""
import copy
from playwright.sync_api import expect

# Import open_dashboard from sibling module
from tests.browser.test_dashboard_states import open_dashboard

CONNECTED_TIMEOUT = 10000

def wait_connected_and_charts(page):
   expect(page.locator('[data-testid="connectivity-status"]')).to_have_text("Connected", timeout=CONNECTED_TIMEOUT)
   page.wait_for_function("() => window.__nodeMonitorTest && window.__nodeMonitorTest.getChartLifecycle && window.__nodeMonitorTest.getChartLifecycle().createCount === 4", timeout=10000)
   lc = page.evaluate("() => window.__nodeMonitorTest.getChartLifecycle()")
   assert lc["createCount"] == 4, f"expected exact createCount 4, got {lc}"


def test_semantic_state_stale_usage_current_counters(browser_page, live_web, snapshot_complete):
   page, _, _ = open_dashboard(browser_page, live_web)
   wait_connected_and_charts(page)
   snapshot = copy.deepcopy(snapshot_complete)
   snapshot["usage"]["status"] = "stale"
   snapshot["usage"]["is_fresh"] = False
   live_web.state.set_snapshot(snapshot)
   page.goto(live_web.url + "/")
   wait_connected_and_charts(page)
   # Header connected
   header = page.locator('.dashboard-header')
   cls_header = header.get_attribute('class') or ''
   assert 'is-connected' in cls_header
   # Counter card exact state-current
   counter_card = page.locator('[data-testid="counter-card"]')
   expect(counter_card).to_have_class(r"state-current")
   # Usage card exact state-stale (not broad any)
   usage_card = page.locator('[data-testid="usage-card"]')
   cls_usage = usage_card.get_attribute('class') or ''
   assert 'state-stale' in cls_usage, f"usage class must be exactly state-stale, got: {cls_usage}"
   assert 'state-current' not in cls_usage
   # Freshness text exact
   expect(page.locator('[data-testid="counter-freshness"]')).to_have_text("Current")
   expect(page.locator('[data-testid="usage-freshness"]')).to_have_text("Stale")


def test_disconnect_retains_stale_classes(browser_page, live_web, snapshot_complete):
   page, _, _ = open_dashboard(browser_page, live_web)
   wait_connected_and_charts(page)
   # Load stale usage snapshot first
   snapshot = copy.deepcopy(snapshot_complete)
   snapshot["usage"]["status"] = "stale"
   snapshot["usage"]["is_fresh"] = False
   live_web.state.set_snapshot(snapshot)
   page.goto(live_web.url + "/")
   wait_connected_and_charts(page)
   # Record CPU text and usage stale class
   before_cpu = page.locator('[data-testid="cpu-busy"]').inner_text()
   usage_card = page.locator('[data-testid="usage-card"]')
   cls_usage_before = usage_card.get_attribute('class') or ''
   assert 'state-stale' in cls_usage_before
   # Capture lifecycle baseline BEFORE disconnect
   lc_before = page.evaluate("() => window.__nodeMonitorTest.getChartLifecycle()")
   assert lc_before["createCount"] == 4
   assert lc_before.get("destroyCount", 0) == 0, f"unexpected destroy before disconnect: {lc_before}"
   # Fail server and trigger different range click
   live_web.state.fail()
   page.locator('[data-range="3h"]').click()
   # Wait exact disconnect text
   expect(page.locator('[data-testid="connectivity-status"]')).to_have_text("Web server disconnected", timeout=CONNECTED_TIMEOUT)
   # Header exact disconnected
   header = page.locator('.dashboard-header')
   cls_header = header.get_attribute('class') or ''
   assert 'is-disconnected' in cls_header
   assert 'is-connected' not in cls_header
   # Usage remains state-stale, CPU unchanged, retry NOT visible
   cls_usage_after = page.locator('[data-testid="usage-card"]').get_attribute('class') or ''
   assert 'state-stale' in cls_usage_after
   assert page.locator('[data-testid="cpu-busy"]').inner_text() == before_cpu
   retry_btn = page.locator('[data-testid="retry-btn"]')
   assert not retry_btn.is_visible()
   # After disconnect, lifecycle unchanged (compare full projection)
   lc_after = page.evaluate("() => window.__nodeMonitorTest.getChartLifecycle()")
   assert lc_after["createCount"] == lc_before["createCount"], f"createCount changed after disconnect: {lc_after}"
   assert lc_after.get("destroyCount", 0) == lc_before.get("destroyCount", 0), f"destroyCount changed after disconnect: {lc_after}"


def test_partial_and_empty_states(browser_page, live_web, snapshot_complete):
   page, _, _ = open_dashboard(browser_page, live_web)
   wait_connected_and_charts(page)
   partial = copy.deepcopy(snapshot_complete)
   partial["counters"]["status"] = "partial"
   partial["usage"]["status"] = "partial"
   live_web.state.set_snapshot(partial)
   page.goto(live_web.url + "/")
   wait_connected_and_charts(page)
   # Wait exact Partial text
   expect(page.locator('[data-testid="counter-freshness"]')).to_have_text("Partial")
   expect(page.locator('[data-testid="usage-freshness"]')).to_have_text("Partial")
   # Exact partial classes
   assert 'state-partial' in (page.locator('[data-testid="counter-card"]').get_attribute('class') or '')
   assert 'state-partial' in (page.locator('[data-testid="usage-card"]').get_attribute('class') or '')
   # Then set empty
   empty = copy.deepcopy(snapshot_complete)
   empty["counters"].update({
      "rows": [], "latest": None, "newest_window_end": None,
      "status": "empty", "is_fresh": False})
   empty["usage"].update({
      "grains": [], "newest_interval_end": None,
      "status": "empty", "is_fresh": False})
   live_web.state.set_snapshot(empty)
   page.goto(live_web.url + "/")
   wait_connected_and_charts(page)
   # Wait exact Empty text
   expect(page.locator('[data-testid="counter-freshness"]')).to_have_text("Empty")
   expect(page.locator('[data-testid="usage-freshness"]')).to_have_text("Empty")
   # Exact empty classes
   for card in ('counter-card', 'usage-card'):
      cls = page.locator(f'[data-testid="{card}"]').get_attribute('class') or ''
      assert 'state-empty' in cls, f"expected state-empty on {card}, got {cls}"
   # CPU '-' and header connected
   assert page.locator('[data-testid="cpu-busy"]').inner_text() == "-"
   header_cls = page.locator('.dashboard-header').get_attribute('class') or ''
   assert 'is-connected' in header_cls
   retry_btn = page.locator('[data-testid="retry-btn"]')
   assert not retry_btn.is_visible()


def test_first_load_failure_header_disconnected_and_retry_visible(browser_page, live_web):
   page, _, _ = browser_page
   live_web.state.fail()
   page.goto(live_web.url + "/")
   # Wait exact connection failure text
   expect(page.locator('[data-testid="connectivity-status"]')).to_have_text("Connection failure", timeout=CONNECTED_TIMEOUT)
   # Header exact disconnected
   header = page.locator('.dashboard-header')
   cls_header = header.get_attribute('class') or ''
   assert 'is-disconnected' in cls_header
   assert 'is-connected' not in cls_header
   # Retry visible
   retry_btn = page.locator('[data-testid="retry-btn"]')
   expect(retry_btn).to_be_visible()
   # No card state class required before any snapshot


def test_desktop_geometry_4_cards_and_2_metric_columns_and_chart_height(browser_page, live_web):
   page, _, _ = open_dashboard(browser_page, live_web)
   wait_connected_and_charts(page)
   page.set_viewport_size({"width": 1440, "height": 1000})
   cards = [page.locator('[data-testid="counter-card"]'),
            page.locator('[data-testid="usage-card"]'),
            page.locator('[data-testid="poll-card"]'),
            page.locator('[data-testid="mem-card"]')],
   for c in cards:
      expect(c).to_be_visible()
   tops = []
   for c in cards:
      bb = c.bounding_box()
      assert bb is not None, f"bounding_box missing for {c}"
      tops.append(bb["y"])
   assert len(tops) == 4
   assert max(tops) - min(tops) < 4, f"status cards not aligned: {tops}"
   panels = page.locator('.metric-panel').all()
   xs = []
   for p in panels:
      expect(p).to_be_visible()
      bb = p.bounding_box()
      assert bb is not None, "metric-panel bounding_box None"
      xs.append(bb["x"])
   distinct_x = len({round(x / 10) * 10 for x in xs})
   assert distinct_x == 2, f"expected 2 metric x columns, got {distinct_x} at {xs}"
   for w in page.locator('.chart-canvas-wrap').all():
      expect(w).to_be_visible()
      bb = w.bounding_box()
      assert bb is not None, "chart wrapper bounding_box None (geometry missing)"
      h = bb["height"]
      assert 280 <= h <= 300, f"chart wrapper height {h} out of [280,300]"


def test_narrow_400px_no_overflow_and_content_visible_and_focus_and_chart_height(browser_page, live_web):
   page, _, _ = open_dashboard(browser_page, live_web)
   wait_connected_and_charts(page)
   page.set_viewport_size({"width": 400, "height": 800})
   scroll = page.evaluate("() => document.documentElement.scrollWidth <= document.documentElement.clientWidth + 2")
   assert scroll, "narrow layout overflows horizontally"
   # Required content assertions using expect visibility (not count>0)
   expect(page.locator('.header-identity').first).to_be_visible()
   expect(page.locator('.header-hero').first).to_be_visible()
   expect(page.locator('.header-stats').first).to_be_visible()
   expect(page.locator('.control-panel').first).to_be_visible()
   expect(page.locator('.range-buttons button').first).to_be_visible()
   expect(page.locator('#node-form').first).to_be_visible()
   expect(page.locator('#user-form').first).to_be_visible()
   expect(page.locator('.metric-panel').first).to_be_visible()
   page.locator('[data-range="1h"]').focus()
   outline_style = page.locator('[data-range="1h"]').evaluate("el => getComputedStyle(el).outlineStyle")
   assert outline_style != 'none', f"focus outline missing (outlineStyle={outline_style})"
   outline_width = page.locator('[data-range="1h"]').evaluate("el => parseFloat(getComputedStyle(el).outlineWidth) || 0")
   assert outline_width > 0, f"focus outline width expected >0, got {outline_width}"
   # Root mask absence checked implicitly by no overflow hidden/clip; keep existing check
   overflow_hidden = page.evaluate("() => { const s=getComputedStyle(document.documentElement); return s.overflow==='hidden'||s.overflow==='clip'; }")
   assert not overflow_hidden, "overflow hidden/clip must not be set on root"
   for w in page.locator('.chart-canvas-wrap').all():
      expect(w).to_be_visible()
      bb = w.bounding_box()
      assert bb is not None, "chart wrapper bounding_box None in narrow layout"
      assert 280 <= bb["height"] <= 300


def test_reduced_motion_disables_pulse(browser_page, live_web):
   page, _, _ = browser_page
   page.emulate_media(reduced_motion="reduce")
   page.goto(live_web.url + "/")
   wait_connected_and_charts(page)
   live_web.state.fail()
   page.locator('[data-range="3h"]').click()
   expect(page.locator('[data-testid="connectivity-status"]')).to_have_text("Web server disconnected", timeout=CONNECTED_TIMEOUT)
   # Retain disconnected class; do not require removal
   header = page.locator('.dashboard-header')
   assert 'is-disconnected' in (header.get_attribute('class') or '')
   dot = page.locator('.dashboard-header.is-disconnected .state-dot')
   expect(dot).to_be_visible()
   anim = dot.evaluate("el => getComputedStyle(el).animationName")
   assert anim == "none", f"expected animation none under reduced motion, got {anim}"
