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
   page, errors, _external = open_dashboard(browser_page, live_web)
   page.locator('[data-range="6h"]').click()
   switched = copy.deepcopy(snapshot_complete)
   switched["node"] = "login-05"
   switched["hardware"]["source_hostname"] = "login-05"
   live_web.state.set_snapshot(switched)
   page.locator('#node-input').fill("login-05")
   page.locator('[data-testid="node-submit"]').click()
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
