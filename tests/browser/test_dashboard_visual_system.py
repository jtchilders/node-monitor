"""Visual-system acceptance for the production node-monitor dashboard."""
from playwright.sync_api import expect

CONNECTED_TIMEOUT = 10000


def open_dashboard(browser_page, live_web):
   page, errors, external = browser_page
   page.goto(live_web.url + "/")
   expect(page.locator('[data-testid="connectivity-status"]')).to_have_text(
      "Connected", timeout=CONNECTED_TIMEOUT)
   page.wait_for_function(
      "() => window.__nodeMonitorTest "
      "&& window.__nodeMonitorTest.getChartLifecycle().createCount === 4",
      timeout=CONNECTED_TIMEOUT)
   return page, errors, external


def css_value(page, selector, property_name):
   return page.locator(selector).first.evaluate(
      "(el, name) => getComputedStyle(el).getPropertyValue(name).trim()",
      property_name)


def test_uses_pbs_monitor_visual_tokens(browser_page, live_web):
   page, errors, external = open_dashboard(browser_page, live_web)
   assert css_value(page, "body", "background-color") == "rgb(26, 26, 46)"
   assert css_value(page, ".dashboard-header", "background-color") == "rgb(22, 33, 62)"
   assert css_value(page, ".metric-panel", "border-color") == "rgb(45, 55, 72)"
   assert css_value(page, "body", "color") == "rgb(224, 224, 224)"
   assert errors == []
   assert external == []


def test_dashboard_has_operational_hierarchy(browser_page, live_web):
   page, errors, external = open_dashboard(browser_page, live_web)
   expect(page.locator(".dashboard-header")).to_be_visible()
   expect(page.locator(".control-panel")).to_be_visible()
   assert page.locator(".status-grid > article").count() == 4
   assert page.locator(".metric-grid > article.metric-panel").count() == 4
   for testid in (
      "system-name", "node-name", "connectivity-status", "data-age-value",
      "cpu-busy", "procs-running", "procs-total",
   ):
      expect(page.locator('[data-testid="%s"]' % testid)).to_be_visible()
   assert errors == []
   assert external == []
