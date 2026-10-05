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


def test_narrow_400px_no_masked_overflow(browser_page, live_web):
   page, errors, external = open_dashboard(browser_page, live_web)
   page.set_viewport_size({"width": 400, "height": 800})
   body_ox = css_value(page, "body", "overflow-x")
   shell_ox = css_value(page, ".dashboard-shell", "overflow-x")
   assert body_ox not in ("hidden", "clip"), "body overflow-x masked: %s" % body_ox
   assert shell_ox not in ("hidden", "clip"), ".dashboard-shell overflow-x masked: %s" % shell_ox
   sw = page.evaluate("""() => {
       const b = document.body; return {sw: b.scrollWidth, cw: b.clientWidth, sh: b.scrollHeight};
   }""")
   overflowers = page.evaluate("""() => {
       const all = document.querySelectorAll('*');
       const overs = [];
       for (const el of all) {
           if (el.scrollWidth > el.clientWidth + 2) {
               overs.push({tag: el.tagName, cls: el.className, id: el.id, sw: el.scrollWidth, cw: el.clientWidth, r: el.getBoundingClientRect().right});
           }
       }
       return overs.slice(0, 10);
   }""")
   assert sw["sw"] <= sw["cw"] + 2, "body scrollWidth %d > clientWidth %d" % (sw["sw"], sw["cw"])
   for sel in (".dashboard-header", ".control-panel", ".metric-panel", ".status-grid > article"):
       expect(page.locator(sel).first).to_be_visible()
   assert errors == []
   assert external == []


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
