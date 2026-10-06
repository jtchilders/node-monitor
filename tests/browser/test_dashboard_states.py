"""Task 7 real-browser acceptance for dashboard states and controls."""
import copy
import re
import time

from playwright.sync_api import expect


def open_dashboard(browser_page, live_web):
   page, errors, external = browser_page
   page.goto(live_web.url + "/")
   expect(page.locator('[data-testid="connectivity-status"]')).to_have_text(
      "Connected")
   return page, errors, external


def age_seconds(page):
   text = page.locator('[data-testid="data-age-seconds"]').inner_text()
   return int(re.search(r"(\d+)s", text).group(1))


def test_initial_loading_then_connected_current(browser_page, live_web):
   page, errors, external = browser_page
   gate = live_web.state.delay()
   page.goto(live_web.url + "/", wait_until="domcontentloaded")
   expect(page.locator('[data-testid="connectivity-status"]')).to_have_text(
      re.compile("Loading"))
   gate.set()
   expect(page.locator('[data-testid="connectivity-status"]')).to_have_text(
      "Connected")
   expect(page.locator('[data-testid="counter-freshness"]')).to_have_text(
      "Current")
   expect(page.locator('[data-testid="usage-freshness"]')).to_have_text(
      "Current")
   expect(page.locator('[data-testid="counter-quality"]')).to_contain_text(
      "Coverage 100.0%")
   assert errors == []
   assert external == []


def test_first_failure_retry_succeeds(browser_page, live_web):
   page, errors, _external = browser_page
   live_web.state.fail()
   page.goto(live_web.url + "/")
   expect(page.locator('[data-testid="connectivity-status"]')).to_have_text(
      "Connection failure")
   expect(page.locator('[data-testid="retry-btn"]')).to_be_visible()
   live_web.state.status = 200
   page.locator('[data-testid="retry-btn"]').click()
   expect(page.locator('[data-testid="connectivity-status"]')).to_have_text(
      "Connected")
   assert all("503" in error for error in errors)


def test_current_counter_does_not_freshen_stale_usage(
      browser_page, live_web, snapshot_complete):
   snapshot = copy.deepcopy(snapshot_complete)
   snapshot["usage"]["status"] = "stale"
   snapshot["usage"]["is_fresh"] = False
   live_web.state.set_snapshot(snapshot)
   page, errors, _external = open_dashboard(browser_page, live_web)
   assert page.locator('[data-testid="counter-freshness"]').inner_text() == "Current"
   assert page.locator('[data-testid="usage-freshness"]').inner_text() == "Stale"
   assert errors == []


def test_disconnect_retains_snapshot_and_age_advances(browser_page, live_web):
   page, errors, _external = open_dashboard(browser_page, live_web)
   before_cpu = page.locator('[data-testid="cpu-busy"]').inner_text()
   before_age = age_seconds(page)
   live_web.state.fail()
   page.locator('[data-range="3h"]').click()
   expect(page.locator('[data-testid="connectivity-status"]')).to_have_text(
      "Web server disconnected")
   assert page.locator('[data-testid="cpu-busy"]').inner_text() == before_cpu
   page.wait_for_timeout(3100)
   assert age_seconds(page) >= before_age + 2
   assert all("503" in error for error in errors)


def test_empty_and_partial_states_do_not_invent_zero(
      browser_page, live_web, snapshot_complete):
   empty = copy.deepcopy(snapshot_complete)
   empty["counters"].update({
      "rows": [], "latest": None, "newest_window_end": None,
      "status": "empty", "is_fresh": False})
   empty["usage"].update({
      "grains": [], "newest_interval_end": None,
      "status": "empty", "is_fresh": False})
   live_web.state.set_snapshot(empty)
   page, errors, _external = open_dashboard(browser_page, live_web)
   assert page.locator('[data-testid="counter-freshness"]').inner_text() == "Empty"
   assert page.locator('[data-testid="cpu-busy"]').inner_text() == "-"
   partial = copy.deepcopy(snapshot_complete)
   partial["counters"]["status"] = "partial"
   partial["usage"]["status"] = "partial"
   live_web.state.set_snapshot(partial)
   page.locator('[data-range="3h"]').click()
   expect(page.locator('[data-testid="counter-freshness"]')).to_have_text("Partial")
   expect(page.locator('[data-testid="usage-freshness"]')).to_have_text("Partial")
   assert errors == []


def test_all_ranges_issue_requests_and_mark_active(browser_page, live_web):
   page, errors, _external = open_dashboard(browser_page, live_web)
   for value in ("1h", "3h", "6h", "12h", "24h"):
      button = page.locator('[data-range="%s"]' % value)
      button.click()
      expect(button).to_have_attribute("aria-pressed", "true")
      assert live_web.state.requests[-1]["range"] == [value]
   timer_state = page.evaluate("window.__nodeMonitorTest.getTimerState()")
   assert timer_state == {"refreshIntervals": 1, "ageIntervals": 1}
   assert errors == []


def test_node_and_username_controls_preserve_range(
      browser_page, live_web, snapshot_complete):
   live_web.state.set_inventory([
      {"id": "login-04", "label": "login-04", "configured": True,
       "role": "local"},
      {"id": "login-05", "label": "login-05", "configured": True,
       "role": "remote"},
   ])
   page, errors, _external = open_dashboard(browser_page, live_web)
   page.locator('[data-range="6h"]').click()
   switched = copy.deepcopy(snapshot_complete)
   switched["node"] = "login-05"
   switched["hardware"]["source_hostname"] = "login-05"
   live_web.state.set_snapshot(switched)
   page.locator('[data-node-id="login-05"]').click()
   expect(page.locator('[data-testid="node-name"]')).to_have_text("login-05")
   assert live_web.state.requests[-1]["node"] == ["login-05"]
   assert live_web.state.requests[-1]["range"] == ["6h"]
   page.locator('#user-input').fill("alice")
   page.locator('[data-testid="user-submit"]').click()
   expect(page.locator('[data-testid="current-username"]')).to_have_text("alice")
   assert live_web.state.requests[-1]["username"] == ["alice"]
   expect(page.locator('[data-testid="username-result"]')).to_have_text(
      "Username matched")
   no_match = copy.deepcopy(switched)
   no_match["usage"]["grains"] = []
   live_web.state.set_snapshot(no_match)
   page.locator('[data-testid="user-submit"]').click()
   expect(page.locator('[data-testid="username-result"]')).to_have_text(
      "No matching usage")
   assert errors == []


def test_local_age_tick_causes_no_request(browser_page, live_web):
   page, errors, _external = open_dashboard(browser_page, live_web)
   count = len(live_web.state.requests)
   first = age_seconds(page)
   page.wait_for_timeout(3100)
   assert age_seconds(page) >= first + 2
   assert len(live_web.state.requests) == count
   assert page.evaluate("window.__nodeMonitorTest.getTimerState()") == {
      "refreshIntervals": 1, "ageIntervals": 1}
   assert errors == []


def test_skewed_wall_clock_does_not_change_server_age(browser_page, live_web):
   page, errors, _external = browser_page
   page.add_init_script("""
      (() => {
         const NativeDate = Date;
         class SkewedDate extends NativeDate {
            constructor(...args) {
               super(...(args.length ? args : [4102444800000]));
            }
            static now() { return 4102444800000; }
         }
         window.Date = SkewedDate;
      })();
   """)
   page.goto(live_web.url + "/")
   expect(page.locator('[data-testid="connectivity-status"]')).to_have_text(
      "Connected")
   assert 60 <= age_seconds(page) <= 62
   assert errors == []


def test_memory_total_variants(browser_page, live_web, snapshot_complete):
   page, errors, _external = open_dashboard(browser_page, live_web)
   text = page.locator('[data-testid="mem-used"]').inner_text()
   assert "GiB" in text and "%" in text
   for total in (None, 0):
      snapshot = copy.deepcopy(snapshot_complete)
      snapshot["hardware"]["mem_total_kb"] = total
      live_web.state.set_snapshot(snapshot)
      page.locator('[data-range="3h"]').click()
      expect(page.locator('[data-testid="hardware-context"]')).to_contain_text(
         "memory total " + ("unavailable" if total is None else "0 KiB"))
      text = page.locator('[data-testid="mem-used"]').inner_text()
      assert "GiB" in text and "percentage unavailable" in text
      assert "NaN" not in text and "Infinity" not in text
   assert errors == []


def test_dstate_hotspot_and_bounded_failures(
      browser_page, live_web, snapshot_complete):
   snapshot = copy.deepcopy(snapshot_complete)
   snapshot["usage"]["grains"].append({
      "interval_end": "2026-10-03T11:45:00+00:00",
      "d_state_fraction": 0.40,
      "d_state_username": "bob",
      "complete": True,
   })
   snapshot["poll_failures"] = [{
      "recorded_at": "2026-10-03T11:%02d:00+00:00" % index,
      "loop": "counter", "failure_type": "timeout",
      "detail": "failure-%d" % index, "breaker_state": "open",
   } for index in range(12)]
   live_web.state.set_snapshot(snapshot)
   page, errors, _external = open_dashboard(browser_page, live_web)
   assert page.locator('[data-testid="d-state"]').inner_text() == "40.0%"
   assert page.locator('[data-testid="usage-username"]').inner_text() == "bob"
   assert page.locator('[data-testid="usage-timestamp"]').inner_text().endswith(
      "11:45:00+00:00")
   assert page.locator('[data-testid="poll-failures"]').inner_text() == "10"
   assert page.locator('[data-testid="poll-breaker"]').inner_text() == "open"
   assert "failure-9" in page.locator(
      '[data-testid="poll-failures-text"]').inner_text()
   assert "failure-10" not in page.locator(
      '[data-testid="poll-failures-text"]').inner_text()
   assert errors == []


def test_narrow_layout_keeps_quality_and_controls_accessible(
      browser_page, live_web):
   page, errors, external = open_dashboard(browser_page, live_web)
   page.set_viewport_size({"width": 400, "height": 800})
   for testid in ("counter-quality", "counter-freshness", "usage-freshness",
                  "hardware-context", "cpu-busy"):
      expect(page.locator('[data-testid="%s"]' % testid)).to_be_visible()
   page.locator('[data-range="1h"]').focus()
   assert page.locator('[data-range="1h"]').evaluate(
      "el => getComputedStyle(el).outlineStyle") != "none"
   assert page.locator('button').count() >= 8
   assert errors == []
   assert external == []

def test_inventory_first_selection_no_guessed_input(
      browser_page, live_web, snapshot_complete):
   page, errors, _ = open_dashboard(browser_page, live_web)
   assert page.locator('[data-testid="node-btn"]').count() >= 1
   assert page.locator('#node-input').count() == 0
   assert page.locator(
      '[data-testid="node-btn"][aria-pressed="true"]').count() == 1
   assert errors == []


def test_empty_inventory_shows_distinct_text(browser_page, live_web):
   live_web.state.set_inventory([])
   page, errors, _ = browser_page
   page.goto(live_web.url + "/", wait_until="domcontentloaded")
   expect(page.locator('#node-status')).to_have_text(
      re.compile("No monitored nodes available"))
   expect(page.locator('[data-testid="connectivity-status"]')).not_to_have_text(
      "Connection failure")
   assert live_web.state.requests == []
   assert errors == []


def test_inventory_failure_distinguished_from_empty(browser_page, live_web):
   live_web.state.fail_inventory()
   page, errors, _ = browser_page
   page.goto(live_web.url + "/", wait_until="domcontentloaded")
   expect(page.locator('#node-status')).to_contain_text("Connection failure")
   assert live_web.state.requests == []
   assert all("503" in error for error in errors)


def test_node_buttons_use_exact_id_and_exclusive_pressed(
      browser_page, live_web, snapshot_complete):
   exact_local = "polaris-login-01.hsn.cm.polaris.alcf.anl.gov"
   exact_remote = "polaris-login-04.hsn.cm.polaris.alcf.anl.gov"
   snapshot_complete["node"] = exact_local
   snapshot_complete["hardware"]["source_hostname"] = exact_local
   live_web.state.set_snapshot(snapshot_complete)
   live_web.state.set_inventory([
      {"id": exact_local, "label": "login-01", "configured": True,
       "role": "local"},
      {"id": exact_remote, "label": "login-04", "configured": False,
       "role": None},
   ])
   page, errors, _ = open_dashboard(browser_page, live_web)
   buttons = page.locator('[data-testid="node-btn"]')
   assert buttons.count() == 2
   assert live_web.state.requests[0]["node"] == [exact_local]
   switched = copy.deepcopy(snapshot_complete)
   switched["node"] = exact_remote
   switched["hardware"]["source_hostname"] = exact_remote
   live_web.state.set_snapshot(switched)
   buttons.nth(1).click()
   expect(page.locator('[data-testid="node-name"]')).to_have_text(exact_remote)
   assert live_web.state.requests[-1]["node"] == [exact_remote]
   pressed = page.locator('[data-testid="node-btn"][aria-pressed="true"]')
   assert pressed.count() == 1
   assert pressed.get_attribute("data-node-id") == exact_remote
   assert errors == []


def test_unavailable_node_refreshes_inventory_and_selects_valid_exact_id(
      browser_page, live_web, snapshot_complete):
   stale = "stale.example"
   valid = "valid.example"
   live_web.state.set_inventory([
      {"id": stale, "label": "stale", "configured": True, "role": "local"},
   ])
   live_web.state.set_dashboard_status_once(422)
   page, errors, _ = browser_page

   def replace_inventory(request):
      if request.url.endswith("/api/nodes") and live_web.state.inventory_requests:
         live_web.state.set_inventory([
            {"id": valid, "label": "valid", "configured": True,
             "role": "local"},
         ])

   page.on("request", replace_inventory)
   page.goto(live_web.url + "/", wait_until="domcontentloaded")

   expect(page.locator('[data-testid="connectivity-status"]')).to_have_text(
      "Connected")
   assert len(live_web.state.inventory_requests) >= 2
   assert live_web.state.requests[-1]["node"] == [valid]
   assert all("422" in error for error in errors)


def test_persistent_unavailable_node_stops_after_one_inventory_refresh(
      browser_page, live_web):
   live_web.state.set_inventory([
      {"id": "gone.example", "label": "gone", "configured": True,
       "role": "local"},
   ])
   live_web.state.fail(status=422)
   page, errors, _ = browser_page
   page.goto(live_web.url + "/", wait_until="domcontentloaded")

   expect(page.locator('#node-status')).to_have_text("Node unavailable")
   assert len(live_web.state.inventory_requests) == 2
   assert len(live_web.state.requests) == 2
   assert all("422" in error for error in errors)


def test_malformed_inventory_is_a_connection_failure_without_dashboard_request(
      browser_page, live_web):
   live_web.state.inventory_body = {
      "system": "polaris",
      "nodes": [{"label": "missing exact id", "configured": True}],
   }
   page, errors, _ = browser_page
   page.goto(live_web.url + "/", wait_until="domcontentloaded")

   expect(page.locator('#node-status')).to_have_text("Connection failure")
   assert live_web.state.requests == []
   assert errors == []

def test_gap_warning_shown_exact_content(browser_page, live_web, snapshot_complete):
    snapshot = copy.deepcopy(snapshot_complete)
    snapshot["counters"]["gaps"] = {"missing_count": 7, "max_gap_minutes": 4, "intervals": [{"location":"internal"}]}
    live_web.state.set_snapshot(snapshot)
    page, errors, _ = open_dashboard(browser_page, live_web)
    expect(page.locator('#gap-warning-strip')).not_to_be_hidden()
    text = page.locator('[data-testid="gap-warning-text"]').inner_text()
    assert "7 missing one-minute counter window(s)" in text
    assert "maximum gap 4 minutes" in text
    assert errors == []

def test_gap_warning_hidden_at_zero_gaps(browser_page, live_web, snapshot_complete):
    snapshot = copy.deepcopy(snapshot_complete)
    snapshot["counters"]["gaps"] = {"missing_count": 0, "max_gap_minutes": 0, "intervals": []}
    live_web.state.set_snapshot(snapshot)
    page, errors, _ = open_dashboard(browser_page, live_web)
    expect(page.locator('#gap-warning-strip')).to_be_hidden()
    assert errors == []

def test_gap_warning_retained_after_failed_refresh(browser_page, live_web, snapshot_complete):
    snapshot = copy.deepcopy(snapshot_complete)
    snapshot["counters"]["gaps"] = {"missing_count": 3, "max_gap_minutes": 2, "intervals": [{"location":"trailing"}]}
    live_web.state.set_snapshot(snapshot)
    page, errors, _ = open_dashboard(browser_page, live_web)
    live_web.state.fail()
    page.locator('[data-range="3h"]').click()
    assert not page.locator('#gap-warning-strip').is_hidden()
    assert "3 missing" in page.locator('[data-testid="gap-warning-text"]').inner_text()

def test_desktop_header_max_64px_and_typography_unchanged(browser_page, live_web):
    page, errors, _ = open_dashboard(browser_page, live_web)
    header = page.locator('.dashboard-header')
    height = header.evaluate('el => el.getBoundingClientRect().height')
    assert height <= 64, "header height %d exceeds 64px" % height
    # Confirm header children are actually contained (no overflow interception)
    header_box = header.bounding_box()
    for child_sel in ('.header-identity', '.header-hero', '.header-stats', '.connection-line'):
        child_box = page.locator(child_sel).first.bounding_box()
        assert child_box is not None, child_sel + " missing"
        assert child_box['y'] >= header_box['y'] - 1, child_sel + " overflows top"
        assert child_box['y'] + child_box['height'] <= header_box['y'] + header_box['height'] + 1, child_sel + " overflows bottom"
    # Typography unchanged against accepted base values
    assert page.locator('.dashboard-header h1').evaluate('el => getComputedStyle(el).fontSize') == '16px'  # 1rem
    assert 'font-size' in page.locator('.connection-line').evaluate('el => getComputedStyle(el).fontSize')
    assert page.locator('.header-hero').evaluate('el => getComputedStyle(el).fontSize') == '13.6px'  # ~.85rem
    assert page.locator('.header-stat .metric-label').first.evaluate('el => getComputedStyle(el).fontSize') == '11.2px'  # .7rem
    # Range buttons remain clickable (not intercepted by header overflow)
    btn = page.locator('[data-range="3h"]')
    btn.click(force=False)
    expect(btn).to_have_attribute("aria-pressed", "true")
    assert errors == []

def test_narrow_no_overflow_and_warning_visible(browser_page, live_web, snapshot_complete):
    snapshot = copy.deepcopy(snapshot_complete)
    snapshot["counters"]["gaps"] = {"missing_count": 2, "max_gap_minutes": 1, "intervals": [{"location":"internal"}]}
    live_web.state.set_snapshot(snapshot)
    page, errors, _ = open_dashboard(browser_page, live_web)
    page.set_viewport_size({"width": 400, "height": 800})
    expect(page.locator('#gap-warning-strip')).to_be_visible()
    overflow = page.evaluate('() => document.body.scrollWidth > window.innerWidth')
    assert not overflow
    assert errors == []
