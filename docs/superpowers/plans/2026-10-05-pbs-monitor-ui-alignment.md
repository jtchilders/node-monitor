# PBS Monitor Visual Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restyle node-monitor's production dashboard so it uses PBS Monitor's approved dark operational-dashboard visual language while preserving every telemetry semantic, request contract, accessibility hook, and chart behavior.

**Architecture:** Keep the existing framework-free HTML/CSS/JavaScript frontend and four-asset static allowlist. Add stable semantic structure and local design tokens in `index.html` and `styles.css`; make `app.js` apply explicit state classes derived from response semantics so connectivity and telemetry freshness remain independent. Extend the existing real-Chromium fixture with visual-system, responsive, and screenshot acceptance tests that serve the production static bytes.

**Tech Stack:** Static HTML5, CSS custom properties and responsive grid, vanilla JavaScript, Chart.js 4 (vendored), pytest, Playwright Chromium.

**Approved base:** `be0bb68ee7133c07acbfad31cb6a640472ef1e0a` (`origin/main` when the design was approved).

**Design spec:** `docs/superpowers/specs/2026-10-05-pbs-monitor-ui-alignment-design.md`

**Development rules:** Use 3-space Python indentation. Use the project venv (`venv/bin/python -m pytest`). Follow strict RED-GREEN-REFACTOR for every behavior. Do not touch collector, database, daemon, listener, configuration, or deployment code. Do not add static assets, remote dependencies, diagnostic claims, or new API fields.

---

## File map

- Modify `node_monitor/web/static/index.html`: semantic PBS-style header, control panel, status grid, metric panels, and subordinate detail panels; retain all current `data-testid`, labels, tables, canvases, forms, and scripts.
- Replace `node_monitor/web/static/styles.css`: local PBS-aligned tokens, component styling, state styling, responsive layout, reduced-motion behavior, and bounded chart wrappers.
- Modify `node_monitor/web/static/app.js`: derive presentation classes from existing connection/counter/usage state and keep header process summaries synchronized. Do not alter fetch, timer, chart transformation, or API semantics.
- Create `tests/browser/test_dashboard_visual_system.py`: production-browser visual tokens, hierarchy, semantic state classes, responsive geometry, focus, reduced motion, and screenshot acceptance.
- Modify `tests/browser/test_dashboard_acceptance.py` only if an existing layout assertion needs a stricter geometry check shared with the new visual suite; do not duplicate state-contract coverage.
- Modify `tests/browser/conftest.py` only to add a reusable screenshot output-directory fixture if needed; do not change request routing or production fixture payloads.

---

### Task 1: Lock the approved visual tokens and structural hierarchy

**Files:**
- Create: `tests/browser/test_dashboard_visual_system.py`
- Modify: `node_monitor/web/static/index.html`
- Modify: `node_monitor/web/static/styles.css`

- [ ] **Step 1: Write failing browser tests for PBS-aligned tokens and structure**

Create `tests/browser/test_dashboard_visual_system.py` with production-byte assertions through the existing `browser_page` and `live_web` fixtures:

```python
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
   return page.locator(selector).evaluate(
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
```

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
venv/bin/python -m pytest \
  tests/browser/test_dashboard_visual_system.py::test_uses_pbs_monitor_visual_tokens \
  tests/browser/test_dashboard_visual_system.py::test_dashboard_has_operational_hierarchy -v
```

Expected: FAIL because `.dashboard-header`, `.control-panel`, `.status-grid`, `.metric-grid`, and PBS token colors do not yet exist.

- [ ] **Step 3: Restructure the static HTML without changing contracts**

In `node_monitor/web/static/index.html`:

1. Wrap the visible dashboard in `<div class="dashboard-shell">`.
2. Replace the generic `<header>` with:

```html
<header class="dashboard-header">
  <div class="header-identity">
    <h1><span data-testid="system-name">system</span> Node Monitor</h1>
    <div class="header-node" data-testid="node-name">node</div>
    <div class="connection-line">
      <span class="state-dot" aria-hidden="true"></span>
      <span aria-live="polite" id="connectivity-status"
            data-testid="connectivity-status">Loading…</span>
      <span aria-hidden="true">·</span>
      <span data-testid="data-age">
        <span data-testid="data-age-value">—</span>
        <span data-testid="data-age-seconds" data-age-seconds="0">—</span>
      </span>
    </div>
  </div>
  <div class="header-hero">
    <span class="metric-label">CPU busy p50</span>
    <strong data-testid="cpu-busy">—</strong>
  </div>
  <div class="header-stats">
    <div class="header-stat">
      <span class="metric-label">Running</span>
      <strong data-testid="procs-running">—</strong>
    </div>
    <div class="header-stat">
      <span class="metric-label">Processes</span>
      <strong data-testid="procs-total">—</strong>
    </div>
  </div>
</header>
```

3. Give the controls section `class="control-panel"`; keep the same forms, IDs, names, labels, buttons, and five `data-range` values.
4. Give the status section `class="status-grid"`. Its four direct articles are Counter, Usage, Memory, and Poll failures. Move the existing memory article into this grid without altering `mem-used` or `mem-note`.
5. Remove duplicate visible CPU/process values from their old generic cards after moving those exact test IDs to the header. Keep load values in a subordinate `.current-context-panel`.
6. Give the chart group `class="metric-grid"`; each existing chart article also gets `class="metric-panel"` and a `.panel-header` wrapping its existing heading.
7. Place the existing D-state hotspot, hardware context, current selection, timestamps, quality details, and fallback tables in `.detail-panel` containers. Preserve every current test ID and semantically protective sentence.
8. Keep the same four local script/style references and do not add an asset.

- [ ] **Step 4: Implement the local PBS-aligned stylesheet**

Replace `node_monitor/web/static/styles.css` with a structured stylesheet containing these exact tokens and component foundations:

```css
:root {
  --bg-color: #1a1a2e;
  --panel-bg: #16213e;
  --inset-bg: #0f172a;
  --text-color: #e0e0e0;
  --text-dim: #94a3b8;
  --accent-blue: #3b82f6;
  --accent-current: #4ade80;
  --accent-warning: #f59e0b;
  --accent-danger: #ef4444;
  --accent-border: #2d3748;
  --header-height: 72px;
}

* { box-sizing: border-box; }
html { color-scheme: dark; }
body {
  margin: 0;
  background: var(--bg-color);
  color: var(--text-color);
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto,
               Helvetica, Arial, sans-serif;
  overflow-x: hidden;
}
.dashboard-shell { min-height: 100vh; }
.dashboard-header {
  position: sticky;
  top: 0;
  z-index: 100;
  min-height: var(--header-height);
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto minmax(0, 1fr);
  align-items: center;
  gap: 1rem;
  padding: .75rem 1.5rem;
  background: var(--panel-bg);
  border-bottom: 1px solid var(--accent-border);
  box-shadow: 0 4px 6px -1px rgba(0, 0, 0, .3);
}
.control-panel,
.status-grid,
.metric-grid,
.detail-grid { margin: 1rem; }
.control-panel,
.status-card,
.metric-panel,
.detail-panel {
  background: var(--panel-bg);
  border: 1px solid var(--accent-border);
  border-radius: 8px;
}
.status-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 1rem;
}
.metric-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 1rem;
  align-items: start;
}
.panel-header,
.metric-label {
  color: var(--text-dim);
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: .05em;
}
.chart-canvas-wrap {
  position: relative;
  width: 100%;
  height: 18rem;
  max-height: 18rem;
  background: var(--inset-bg);
}
.chart-canvas-wrap canvas {
  display: block;
  width: 100% !important;
  height: 100% !important;
}
```

Complete the stylesheet with PBS-style inputs, buttons, tables, chart notes, selected `[aria-pressed="true"]` controls, disabled states, hover states, and visible focus:

```css
button:focus-visible,
input:focus-visible,
summary:focus-visible {
  outline: 2px solid #60a5fa;
  outline-offset: 2px;
}
.range-buttons,
#mem-controls,
#proc-controls,
#nl-controls {
  display: flex;
  flex-wrap: wrap;
  gap: 2px;
  padding: 2px;
  border-radius: 6px;
  background: var(--inset-bg);
}
button[aria-pressed="true"] {
  color: #fff;
  background: var(--accent-blue);
  border-color: var(--accent-blue);
}
```

- [ ] **Step 5: Run the focused tests and verify GREEN**

Run:

```bash
venv/bin/python -m pytest tests/browser/test_dashboard_visual_system.py -v
```

Expected: both new tests PASS; no console errors or external requests.

- [ ] **Step 6: Run existing structural/browser regressions**

Run:

```bash
venv/bin/python -m pytest \
  tests/browser/test_dashboard_acceptance.py \
  tests/browser/test_dashboard_states.py \
  tests/browser/test_dashboard_charts.py -q
```

Expected: PASS. If a failure is caused by a moved test ID, fix the HTML while retaining exactly one element with that ID; do not weaken the existing assertion.

- [ ] **Step 7: Commit the structural visual system**

```bash
git add node_monitor/web/static/index.html \
        node_monitor/web/static/styles.css \
        tests/browser/test_dashboard_visual_system.py
git commit -m "feat(web): adopt PBS Monitor dashboard visual system"
```

---

### Task 2: Make operational state styling semantic and independent

**Files:**
- Modify: `tests/browser/test_dashboard_visual_system.py`
- Modify: `node_monitor/web/static/app.js`
- Modify: `node_monitor/web/static/styles.css`

- [ ] **Step 1: Add failing tests for connection and telemetry state classes**

Append:

```python
import copy


def class_names(locator):
   return set((locator.get_attribute("class") or "").split())


def test_connection_and_freshness_states_are_independent(
      browser_page, live_web, snapshot_complete):
   snapshot = copy.deepcopy(snapshot_complete)
   snapshot["usage"]["status"] = "stale"
   snapshot["usage"]["is_fresh"] = False
   live_web.state.set_snapshot(snapshot)
   page, errors, external = open_dashboard(browser_page, live_web)

   assert "is-connected" in class_names(page.locator(".dashboard-header"))
   assert "state-current" in class_names(
      page.locator('[data-testid="counter-card"]'))
   assert "state-stale" in class_names(
      page.locator('[data-testid="usage-card"]'))

   live_web.state.fail()
   page.locator('[data-range="3h"]').click()
   expect(page.locator('[data-testid="connectivity-status"]')).to_have_text(
      "Web server disconnected")
   assert "is-disconnected" in class_names(page.locator(".dashboard-header"))
   assert "state-stale" in class_names(
      page.locator('[data-testid="usage-card"]'))
   assert external == []
   assert all("503" in error for error in errors)


def test_empty_and_partial_have_text_and_semantic_classes(
      browser_page, live_web, snapshot_complete):
   partial = copy.deepcopy(snapshot_complete)
   partial["counters"]["status"] = "partial"
   partial["usage"]["status"] = "empty"
   partial["usage"]["grains"] = []
   partial["usage"]["newest_interval_end"] = None
   live_web.state.set_snapshot(partial)
   page, errors, external = open_dashboard(browser_page, live_web)
   expect(page.locator('[data-testid="counter-freshness"]')).to_have_text(
      "Partial")
   expect(page.locator('[data-testid="usage-freshness"]')).to_have_text(
      "Empty")
   assert "state-partial" in class_names(
      page.locator('[data-testid="counter-card"]'))
   assert "state-empty" in class_names(
      page.locator('[data-testid="usage-card"]'))
   assert errors == []
   assert external == []
```

- [ ] **Step 2: Run the state tests and verify RED**

Run:

```bash
venv/bin/python -m pytest \
  tests/browser/test_dashboard_visual_system.py::test_connection_and_freshness_states_are_independent \
  tests/browser/test_dashboard_visual_system.py::test_empty_and_partial_have_text_and_semantic_classes -v
```

Expected: FAIL because state classes are absent.

- [ ] **Step 3: Add narrow state-class helpers to `app.js`**

Near `freshnessLabel`, add:

```javascript
function normalizedSectionState(section) {
   const status = section && section.status ? section.status : 'empty';
   if (status === 'empty') return 'empty';
   if (status === 'partial') return 'partial';
   if (status === 'stale' || !section.is_fresh) return 'stale';
   return 'current';
}

function setExclusiveStateClass(element, prefix, value, allowed) {
   allowed.forEach(function(candidate) {
      element.classList.remove(prefix + candidate);
   });
   element.classList.add(prefix + value);
}

function renderPresentationState(counters, usage, connected) {
   const header = qs('.dashboard-header');
   setExclusiveStateClass(
      header, 'is-', connected ? 'connected' : 'disconnected',
      ['connected', 'disconnected']);
   setExclusiveStateClass(
      qs('[data-testid="counter-card"]'), 'state-',
      normalizedSectionState(counters),
      ['current', 'partial', 'stale', 'empty']);
   setExclusiveStateClass(
      qs('[data-testid="usage-card"]'), 'state-',
      normalizedSectionState(usage),
      ['current', 'partial', 'stale', 'empty']);
}
```

Call `renderPresentationState(counters, usage, true)` in `render()` after the text values are assigned. In `renderFailure(firstLoad)`, set only the header connection class to disconnected; do not recompute or erase retained counter/usage classes.

Do not derive class state by parsing human-readable DOM text. Do not modify `freshnessLabel`, `renderAges`, fetch handling, timers, or retained-snapshot behavior.

- [ ] **Step 4: Style semantic states without relying on color alone**

Add CSS:

```css
.state-dot { width: 8px; height: 8px; border-radius: 50%; }
.is-connected .state-dot { background: var(--accent-current); }
.is-disconnected .state-dot {
  background: var(--accent-danger);
  animation: connection-pulse 1.2s ease-in-out infinite;
}
.is-disconnected .connection-line { color: var(--accent-danger); font-weight: 600; }
.state-current { border-top: 3px solid var(--accent-current); }
.state-partial,
.state-stale { border-top: 3px solid var(--accent-warning); }
.state-empty { border-top: 3px solid var(--text-dim); }
@keyframes connection-pulse {
  0%, 100% { opacity: 1; }
  50% { opacity: .35; }
}
@media (prefers-reduced-motion: reduce) {
  .is-disconnected .state-dot { animation: none; }
}
```

Ensure visible text (`Connected`, `Web server disconnected`, `Current`, `Partial`, `Stale`, `Empty`) remains present.

- [ ] **Step 5: Run new and existing state suites and verify GREEN**

Run:

```bash
venv/bin/python -m pytest \
  tests/browser/test_dashboard_visual_system.py \
  tests/browser/test_dashboard_states.py \
  tests/browser/test_dashboard_acceptance.py -q
```

Expected: PASS.

- [ ] **Step 6: Commit semantic operational states**

```bash
git add node_monitor/web/static/app.js \
        node_monitor/web/static/styles.css \
        tests/browser/test_dashboard_visual_system.py
git commit -m "feat(web): style dashboard operational states"
```

---

### Task 3: Prove responsive geometry, focus, reduced motion, and chart bounds

**Files:**
- Modify: `tests/browser/test_dashboard_visual_system.py`
- Modify: `node_monitor/web/static/styles.css`
- Modify: `node_monitor/web/static/index.html` only if a wrapper/class is missing

- [ ] **Step 1: Add failing geometry and accessibility tests**

Append:

```python
def test_desktop_grid_and_chart_geometry(browser_page, live_web):
   page, errors, external = open_dashboard(browser_page, live_web)
   page.set_viewport_size({"width": 1440, "height": 1000})
   cards = page.locator(".status-grid > article")
   panels = page.locator(".metric-grid > article.metric-panel")
   assert len({round(cards.nth(index).bounding_box()["y"])
               for index in range(cards.count())}) == 1
   assert len({round(panels.nth(index).bounding_box()["x"])
               for index in range(panels.count())}) == 2
   for index in range(page.locator(".chart-canvas-wrap").count()):
      height = page.locator(".chart-canvas-wrap").nth(index).bounding_box()["height"]
      assert 280 <= height <= 300
   assert errors == []
   assert external == []


def test_narrow_layout_has_no_overflow_and_keeps_controls_visible(
      browser_page, live_web):
   page, errors, external = open_dashboard(browser_page, live_web)
   page.set_viewport_size({"width": 400, "height": 800})
   assert page.evaluate(
      "() => document.documentElement.scrollWidth "
      "<= document.documentElement.clientWidth + 2")
   for selector in (
      ".header-identity", ".header-hero", ".header-stats", ".control-panel",
      '[data-testid="range-btn"]', '[data-testid="node-input"]',
      '[data-testid="user-input"]', ".metric-panel",
   ):
      expect(page.locator(selector).first).to_be_visible()
   page.locator('[data-range="1h"]').focus()
   assert css_value(page, '[data-range="1h"]', "outline-style") != "none"
   assert errors == []
   assert external == []


def test_reduced_motion_disables_disconnect_animation(
      browser_page, live_web):
   page, errors, external = browser_page
   page.emulate_media(reduced_motion="reduce")
   page.goto(live_web.url + "/")
   expect(page.locator('[data-testid="connectivity-status"]')).to_have_text(
      "Connected", timeout=CONNECTED_TIMEOUT)
   live_web.state.fail()
   page.locator('[data-range="3h"]').click()
   expect(page.locator('[data-testid="connectivity-status"]')).to_have_text(
      "Web server disconnected")
   assert css_value(page, ".state-dot", "animation-name") == "none"
   assert external == []
   assert all("503" in error for error in errors)
```

- [ ] **Step 2: Run geometry tests and verify RED where layout is incomplete**

Run:

```bash
venv/bin/python -m pytest \
  tests/browser/test_dashboard_visual_system.py::test_desktop_grid_and_chart_geometry \
  tests/browser/test_dashboard_visual_system.py::test_narrow_layout_has_no_overflow_and_keeps_controls_visible \
  tests/browser/test_dashboard_visual_system.py::test_reduced_motion_disables_disconnect_animation -v
```

Expected: at least one FAIL until desktop columns, narrow stacking, bounded wrappers, and reduced-motion styling exactly satisfy the contract.

- [ ] **Step 3: Complete responsive CSS**

Add or refine:

```css
@media (max-width: 1024px) {
  .dashboard-header {
    position: static;
    grid-template-columns: 1fr 1fr;
  }
  .header-stats { justify-content: flex-end; }
  .status-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
}

@media (max-width: 640px) {
  .dashboard-header,
  .status-grid,
  .metric-grid,
  .detail-grid { grid-template-columns: minmax(0, 1fr); }
  .dashboard-header,
  .control-panel,
  .status-grid,
  .metric-grid,
  .detail-grid { margin-left: .5rem; margin-right: .5rem; }
  .dashboard-header { margin: 0; padding: .75rem; }
  .header-hero { text-align: left; }
  .header-stats { justify-content: flex-start; }
  .control-panel { align-items: stretch; }
  .control-panel form { display: grid; grid-template-columns: 1fr; }
  .range-buttons { width: 100%; }
  .range-buttons button { flex: 1 1 auto; }
  .chart-canvas-wrap { height: 18rem; max-height: 18rem; }
  .chart-alt { display: block; max-width: 100%; overflow-x: auto; }
}
```

Adjust selectors to the actual final markup. Do not hide required content on narrow screens.

- [ ] **Step 4: Verify geometry GREEN and exercise the chart semantics**

Run:

```bash
venv/bin/python -m pytest tests/browser/test_dashboard_visual_system.py -q
venv/bin/python -m pytest \
  tests/browser/test_dashboard_charts.py \
  tests/browser/test_dashboard_charts_aggregation.py \
  tests/browser/test_dashboard_task8_blockers.py -q
```

Expected: PASS, including exact chart arrays, null gaps, modes, attribution, lifecycle counts, and canvas pixels.

- [ ] **Step 5: Perform RED-proof on the no-overflow assertion**

Temporarily add `min-width: 700px` to `.metric-grid`, run:

```bash
venv/bin/python -m pytest \
  tests/browser/test_dashboard_visual_system.py::test_narrow_layout_has_no_overflow_and_keeps_controls_visible -q
```

Expected: FAIL on document overflow. Remove the temporary line and rerun the same test; expected PASS. Confirm `git diff` contains no temporary mutation.

- [ ] **Step 6: Commit responsive and accessibility acceptance**

```bash
git add node_monitor/web/static/index.html \
        node_monitor/web/static/styles.css \
        tests/browser/test_dashboard_visual_system.py
git commit -m "test(web): enforce responsive dashboard presentation"
```

---

### Task 4: Capture and inspect the required visual-state matrix

**Files:**
- Modify: `tests/browser/test_dashboard_visual_system.py`
- Optionally modify: `tests/browser/conftest.py`
- Generated, uncommitted evidence: `.artifacts/ui-alignment/*.png`

- [ ] **Step 1: Add a deterministic screenshot test**

Append a test that uses `tmp_path` for test isolation and also accepts an explicit owner-only evidence directory through `NODE_MONITOR_SCREENSHOT_DIR`:

```python
import os
from pathlib import Path


def screenshot_dir(tmp_path):
   configured = os.environ.get("NODE_MONITOR_SCREENSHOT_DIR")
   output = Path(configured) if configured else tmp_path
   output.mkdir(parents=True, exist_ok=True)
   return output


def test_capture_visual_acceptance_matrix(
      browser_page, live_web, snapshot_complete, tmp_path):
   page, errors, external = open_dashboard(browser_page, live_web)
   output = screenshot_dir(tmp_path)

   page.set_viewport_size({"width": 1440, "height": 1000})
   page.screenshot(path=str(output / "desktop-current.png"), full_page=True)

   live_web.state.fail()
   page.locator('[data-range="3h"]').click()
   expect(page.locator('[data-testid="connectivity-status"]')).to_have_text(
      "Web server disconnected")
   page.screenshot(path=str(output / "desktop-disconnected.png"), full_page=True)

   partial = copy.deepcopy(snapshot_complete)
   partial["counters"]["status"] = "partial"
   partial["usage"]["status"] = "stale"
   partial["usage"]["is_fresh"] = False
   live_web.state.set_snapshot(partial)
   page.locator('[data-testid="retry-btn"]').click() if page.locator(
      '[data-testid="retry-btn"]:visible').count() else page.locator(
      '[data-range="1h"]').click()
   expect(page.locator('[data-testid="connectivity-status"]')).to_have_text(
      "Connected")
   page.set_viewport_size({"width": 400, "height": 800})
   page.screenshot(path=str(output / "narrow-partial-stale.png"), full_page=True)

   for name in (
      "desktop-current.png", "desktop-disconnected.png",
      "narrow-partial-stale.png",
   ):
      assert (output / name).stat().st_size > 10000
   assert external == []
   assert all("503" in error for error in errors)
```

If the retained-disconnect page has no visible retry control by design, use a successful range click after restoring the fixture, as shown. Do not add a production test hook.

- [ ] **Step 2: Run the screenshot test into a durable local evidence directory**

```bash
mkdir -p .artifacts/ui-alignment
chmod 700 .artifacts .artifacts/ui-alignment
NODE_MONITOR_SCREENSHOT_DIR="$PWD/.artifacts/ui-alignment" \
  venv/bin/python -m pytest \
  tests/browser/test_dashboard_visual_system.py::test_capture_visual_acceptance_matrix -v
```

Expected: PASS and three PNG files larger than 10 KB. Confirm `.artifacts/` is ignored before proceeding; never commit screenshots unless Taylor explicitly requests them.

- [ ] **Step 3: Inspect all screenshots visually**

Open each PNG with the available vision tool and check:

- desktop current: PBS-family dark palette, four aligned status cards, two metric columns, readable header and controls;
- desktop disconnected: retained charts and values, red connection treatment, independently preserved telemetry state;
- narrow partial/stale: one-column flow, visible controls and state text, no clipping/overlap, bounded chart dimensions;
- all images: no blank or transparent charts, absurd full-page dimensions, horizontal overflow, low-contrast labels, or accidental light browser-default surfaces.

If any defect appears, add a failing geometry/style test reproducing it before changing production CSS, then rerun this task.

- [ ] **Step 4: Verify static packaging still contains exactly four assets**

Run:

```bash
venv/bin/python -m pytest \
  tests/web/test_web_packaging.py::test_chart_checksum_and_size \
  tests/browser/test_dashboard_acceptance.py::test_packaged_static_assets_are_served_and_match_production_bytes \
  tests/browser/test_dashboard_acceptance.py::test_no_external_requests_on_full_interaction_sequence -v
```

Expected: PASS; no fifth asset and no external request.

- [ ] **Step 5: Commit screenshot acceptance code only**

```bash
git add tests/browser/test_dashboard_visual_system.py tests/browser/conftest.py
git commit -m "test(web): add dashboard visual acceptance matrix"
```

Do not stage `.artifacts/`.

---

### Task 5: Full verification, independent review, and integration handoff

**Files:**
- No planned production changes; review/fix commits only if findings require them.

- [ ] **Step 1: Run focused web and browser suites**

```bash
venv/bin/python -m pytest tests/web tests/browser -q
```

Expected: PASS with no collection errors, console errors, external requests, or warnings introduced by this branch.

- [ ] **Step 2: Run the full repository suite**

```bash
venv/bin/python -m pytest -q
```

Expected: PASS. Classify a failure as pre-existing only by reproducing it at accepted base `be0bb68ee7133c07acbfad31cb6a640472ef1e0a`; an intermediate branch failure is not a baseline.

- [ ] **Step 3: Inspect scope and static security boundary**

```bash
git diff --check be0bb68ee7133c07acbfad31cb6a640472ef1e0a..HEAD
git diff --stat be0bb68ee7133c07acbfad31cb6a640472ef1e0a..HEAD
git status --short --branch
```

Expected changed implementation scope:

- `node_monitor/web/static/index.html`
- `node_monitor/web/static/styles.css`
- `node_monitor/web/static/app.js` only for presentation state classes
- `tests/browser/test_dashboard_visual_system.py`
- optionally `tests/browser/conftest.py`
- approved spec and this plan

Reject unexpected collector, database, daemon, listener, config, deployment, dependency, or vendored-JavaScript changes.

- [ ] **Step 4: Dispatch an independent exact-head review**

Give Vera the absolute base SHA, exact HEAD SHA, design spec path, plan path, and these blocking questions:

1. Does the diff preserve connectivity versus telemetry-freshness independence?
2. Are all existing telemetry meanings, chart transforms, attribution, null gaps, units, and request parameters unchanged?
3. Does every state remain textually identifiable without color?
4. Are assets local and static-resource protections unchanged?
5. Do desktop and 400-pixel layouts avoid clipping, overflow, and Chart.js resize feedback?
6. Do tests genuinely exercise the production static bytes and fail if the visual contract is broken?

Critical or Important findings block integration. Vera reviews only and does not implement fixes.

- [ ] **Step 5: Resolve findings with fresh RED-GREEN cycles**

For each reproduced finding, add the smallest failing test, observe RED, implement the minimal correction, rerun focused and full suites, commit, and request a new exact-head review. Do not waive or downgrade a Critical/Important finding.

- [ ] **Step 6: Prepare integration evidence**

Record:

- base and final SHAs;
- focused and full test commands with observed counts;
- screenshot paths and visual-inspection result;
- independent-review verdict and finding disposition;
- `git diff --stat` and clean status;
- explicit note that no remote deployment or Polaris action was performed.

Do not deploy to Polaris. Remote deployment remains paused until a separate resource-bounded deployment plan is reviewed and explicitly approved.
