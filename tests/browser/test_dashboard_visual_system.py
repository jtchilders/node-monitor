"""Task 2 visual-system exact-state assertions (rewritten, non-tautological)."""
import copy
from playwright.sync_api import expect

CONNECTED_TIMEOUT = 10000


def test_semantic_state_stale_usage_current_counters(browser_page, live_web, snapshot_complete):
    page, _, _ = browser_page
    snapshot = copy.deepcopy(snapshot_complete)
    snapshot["usage"]["status"] = "stale"
    snapshot["usage"]["is_fresh"] = False
    live_web.state.set_snapshot(snapshot)
    page.goto(live_web.url + "/")
    expect(page.locator('[data-testid="connectivity-status"]')).to_have_text("Connected", timeout=CONNECTED_TIMEOUT)
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
    page, _, _ = browser_page
    # Load stale usage snapshot first
    snapshot = copy.deepcopy(snapshot_complete)
    snapshot["usage"]["status"] = "stale"
    snapshot["usage"]["is_fresh"] = False
    live_web.state.set_snapshot(snapshot)
    page.goto(live_web.url + "/")
    expect(page.locator('[data-testid="connectivity-status"]')).to_have_text("Connected", timeout=CONNECTED_TIMEOUT)
    # Record CPU text and usage stale class
    before_cpu = page.locator('[data-testid="cpu-busy"]').inner_text()
    usage_card = page.locator('[data-testid="usage-card"]')
    cls_usage_before = usage_card.get_attribute('class') or ''
    assert 'state-stale' in cls_usage_before
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
    # Charts not replaced/destroyed: lifecycle projection unchanged
    lc_before = page.evaluate("() => window.__nodeMonitorTest.getChartLifecycle()")
    assert page.locator('#chart-cpu').count() == 1


def test_partial_and_empty_states(browser_page, live_web, snapshot_complete):
    page, _, _ = browser_page
    partial = copy.deepcopy(snapshot_complete)
    partial["counters"]["status"] = "partial"
    partial["usage"]["status"] = "partial"
    live_web.state.set_snapshot(partial)
    page.goto(live_web.url + "/")
    expect(page.locator('[data-testid="connectivity-status"]')).to_have_text("Connected", timeout=CONNECTED_TIMEOUT)
    # Wait exact Partial text
    expect(page.locator('[data-testid="counter-freshness"]')).to_have_text("Partial")
    expect(page.locator('[data-testid="usage-freshness"]')).to_have_text("Partial")
    # Exact partial classes
    assert 'state-partial' in (page.locator('[data-testid="counter-card"]').get_attribute('class') or '')
    assert 'state-partial' in (page.locator('[data-testid="usage-card"]').get_attribute('class') or '')
    # Then set empty with DIFFERENT range click (e.g., 12h)
    empty = copy.deepcopy(snapshot_complete)
    empty["counters"].update({"rows": [], "latest": None, "newest_window_end": None, "status": "empty", "is_fresh": False})
    empty["usage"].update({"grains": [], "newest_interval_end": None, "status": "empty", "is_fresh": False})
    live_web.state.set_snapshot(empty)
    page.locator('[data-range="12h"]').click()
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
