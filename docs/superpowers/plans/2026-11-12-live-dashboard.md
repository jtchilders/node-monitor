# Live dashboard (replace dots) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the dots canvas on `/` with a dashboard of Chart.js visuals — a pie (doughnut) breaking down live processes by a selectable group and two horizontal bar charts showing current RSS and CPU summed per group — all filtered by the existing All-nodes/node tabs and text filter, with the process table moved below.

**Architecture:** Pure front-end change: add `live_charts.js` that groups the already-fetched `GET /api/live/processes` rows in-memory (no new API), exposes `GROUP_DEFS` + `aggregateForGroup()` + deterministic `colorFor()` and Chart.js wrappers; `index.js` wires a shared `group` state to both the pie and the resource bars and to `applyFilter()`; `index.html`/`dashboard.css` lay out the dashboard grid above the full-width process list. No DB, no API, no Python changes.

**Tech Stack:** Python ≥3.10 (unchanged), FastAPI + vanilla JS modules, vendored `chart.umd.min.js@4.4.9` + `vue.global.prod.js@3.4.38` already in `node_monitor/web/static/vendor/`, pytest/httpx, ruff.

**Spec:** User request 2026-11-12: replace dots, pie by users/categories/activity (+suggestions), memory & CPU plots color-coded by same group, keep All-nodes/node-0N filter, shift process list below.

## Global Constraints

- Repo `~/node-monitor/node-monitor`, branch `feat/live-dashboard` from `main` (`0797d31`), venv `~/node-monitor/venv` editable, live commands `env -u LD_PRELOAD`.
- 3-space indent, `ruff check . && ruff format --check .` must pass, `docs/` never formatted, Python ≥3.10, no `asyncio.wait_for`.
- No CDN — reuse `node_monitor/web/static/vendor/chart.umd.min.js@4.4.9` (and Vue if needed); no new npm deps.
- Every visible text via `textContent`/`createTextNode`, never `innerHTML` with user data; cmdline slices stay 80 chars in table, full cmdline only in modal.
- Existing APIs unchanged: `GET /api/live/processes|nodes|health|events` remain the sole data source; no new backend route in this plan.
- Collector/DB/migrate untouched.

## Review Focus

- 0 processes after filtering (node with no procs, or text filter matches nothing) — charts must show an empty/friendly state, not throw, and recover when data returns.
- 60 distinct users on one node — pie and bars must cap slices/bars and bucket remainder as “Other” so the page stays readable and Chart.js stays fast.
- Rapid node-tab clicks while a `/api/live/processes` fetch is in flight — previous fetch must be aborted and stale data must never overwrite newer data.
- User-supplied process fields contain HTML/JS (`<img onerror=…>`) — every label, legend, and tooltip rendered from process data must be plain text and not execute.
- Group choice must survive refresh/filter/node change within the page session and not reset spuriously; pie, both bars, and legend must always agree on the same group and colors.

Each gets a test in its owning task (see Task steps).

---

### Task 1: Shared grouping and palette utilities

**Files:**
- Create: `node_monitor/web/static/js/live_charts.js`
- Create: `tests/web/test_live_dashboard_groups.py` (JS exercised via node smoke or Python mirror of grouping logic if needed; primary JS unit tests live in `tests/web/test_live_dashboard_js.py` — see note)
- Test: `tests/web/test_live_dashboard_js.py` (or `tests/web/test_live_charts.py` — JS unit via `node --test` shim; if harness lacks node, use Python-equivalent grouping tests + DOM smoke via `httpx` page fetch)

**Interfaces:**
- Consumes: `CATEGORY_COLORS` map from `index.js` (moved/shared) and live process shape `{username, category, activity, node/label, owner_presence, rss_kb, cpu_seconds}`.
- Produces:
  - `export const GROUP_DEFS = {category:{label:"Category", key:(p)=>p.category||"other"}, activity:{label:"Activity", key:(p)=>p.activity||"other"}, user:{label:"User", key:(p)=>p.username||"unknown"}, node:{label:"Node", key:(p)=>p.label||p.node||"unknown"}, presence:{label:"Presence", key:(p)=>p.owner_presence||"unknown"}}`
  - `export function aggregateForGroup(processes, group) -> {groups:[{key,count,rss,cpu}], total, capped}` — counts + sums per distinct key, sorted by count desc, caps at `MAX_SLICES=12` (pie) or `MAX_BARS=12` (bars) with an `Other` bucket summing the tail; 0-length input returns `{groups:[], total:0, capped:false}`.
  - `export function colorFor(group, key, index) -> string` — for `category` reuses `CATEGORY_COLORS[key]||"#9ca3af"`; otherwise deterministic hash → `PALETTE[index % PALETTE.length]` where `PALETTE` is 12-color accessible set (Tailwind-inspired, dark-bg legible).
  - `export const PALETTE: string[]` — 12 hex colors, no two adjacent within 30deg hue on dark bg.
  - `export function buildPieDataset(agg, group) -> {labels, data, backgroundColor}` and `export function buildBarDatasets(agg, group) -> {labels, rssData, cpuData, backgroundColor}`

- [ ] **Step 1: Write failing tests**
  ```python
  # tests/web/test_live_charts_groups.py — Python mirror or JS shim:
  def test_aggregate_empty_returns_empty():
      assert aggregateForGroup([], "category") == {"groups": [], "total": 0, "capped": False}

  def test_aggregate_counts_and_sums():
      procs = [{"category":"ai_agent","rss_kb":100,"cpu_seconds":1.0},{"category":"ai_agent","rss_kb":50,"cpu_seconds":2.0},{"category":"python","rss_kb":10,"cpu_seconds":0.1}]
      agg = aggregateForGroup(procs, "category")
      assert agg["groups"][0]["key"]=="ai_agent" and agg["groups"][0]["count"]==2 and agg["groups"][0]["rss"]==150

  def test_aggregate_caps_at_12_with_other():
      procs = [{"username":f"u{i}","rss_kb":1,"cpu_seconds":1} for i in range(20)]
      agg = aggregateForGroup(procs, "user")
      assert len(agg["groups"])==12 and agg["groups"][-1]["key"]=="Other" and agg["capped"] is True

  def test_colorFor_category_reuses_CATEGORY_COLORS():
      assert colorFor("category","ai_agent",0)=="#8b5cf6"

  def test_colorFor_unknown_group_is_deterministic():
      assert colorFor("user","alice",3)==colorFor("user","alice",99)  # index ignored for same key when group!=category? or deterministic per key

  def test_buildPieDataset_uses_text_labels():
      agg = aggregateForGroup([{"category":"<img onerror=alert(1)>","rss_kb":1,"cpu_seconds":1}], "category")
      ds = buildPieDataset(agg, "category")
      assert "<img" in ds["labels"][0]  # label is raw text, not HTML; rendering test checks textContent
  ```
- [ ] **Step 2: Run tests to verify they fail**
  Run: `env -u LD_PRELOAD /home/parton/node-monitor/venv/bin/python -m pytest tests/web/test_live_charts_groups.py -v`
  Expected: FAIL — module `live_charts.js` not found / functions not defined
- [ ] **Step 3: Implement `live_charts.js`**
  - Define `CATEGORY_COLORS` centrally here and re-export for `index.js` (or keep copy in both and assert equality in tests). Prefer single source: move `CATEGORY_COLORS` into `live_charts.js` and have `index.js` import it.
  - `GROUP_DEFS` as above; `MAX_SLICES=12`, `MAX_BARS=12`.
  - `aggregateForGroup`: Map key→{count,rss,cpu}, sort by count desc then rss desc, slice top N-1, bucket tail as `Other` with summed count/rss/cpu.
  - `PALETTE` 12 colors: `#8b5cf6,#06b6d4,#f59e0b,#ef4444,#3b82f6,#10b981,#ec4899,#14b8a6,#f97316,#a78bfa,#22c55e,#e879f9` (or similar accessible set).
  - `colorFor`: if group==="category" lookup `CATEGORY_COLORS`, else `PALETTE[hash(key) % len]` where hash is simple djb2 over string; memoize per key so same label always same color within page session.
  - `buildPieDataset`/`buildBarDatasets` produce Chart.js-ready arrays; no DOM.
  - 3-space indent, `export` all.
- [ ] **Step 4: Run tests to verify they pass**
  Run: `env -u LD_PRELOAD /home/parton/node-monitor/venv/bin/python -m pytest tests/web/test_live_charts_groups.py -v` (+ `ruff check .`)
  Expected: PASS, ruff clean
- [ ] **Step 5: Commit**
  ```bash
  git add node_monitor/web/static/js/live_charts.js tests/web/test_live_charts*.py
  git commit -m "feat(web): grouping and palette utilities for live dashboard

Co-Authored-By: Claude Code <noreply@anthropic.com>"
  ```

### Task 2: Pie chart — breakdown by selectable group

**Files:**
- Modify: `node_monitor/web/static/js/live_charts.js` (add Chart.js wrappers if not in Task 1)
- Modify: `node_monitor/web/static/js/index.js`
- Modify: `node_monitor/web/templates/index.html` (add dashboard slot for pie)
- Modify: `node_monitor/web/static/css/dashboard.css` (dashboard card sizing)
- Test: `tests/web/test_live_dashboard_page.py`

**Interfaces:**
- Consumes: `GROUP_DEFS`, `aggregateForGroup`, `colorFor`, `buildPieDataset` from Task 1; `allProcesses`/`filtered` from `index.js:applyFilter`; `selectedNode` filter.
- Produces:
  - `export function createPieChart(canvasEl, agg, group) -> Chart` — Chart.js `type:"doughnut"`, `cutout:"55%"`, `responsive:true`, `plugins.legend.position="right"` with `labels.generateLabels` using plain text, `plugins.tooltip.callbacks.label` returning `label + ": " + value + " (" + pct + "%)"` via `textContent`-safe strings.
  - `export function updatePieChart(chart, agg, group)` — diff update without destroy (set `data.labels`/`datasets[0].data`/`backgroundColor`, `chart.update()`).
  - DOM: `index.html` gains `<div id="live-dashboard" class="dashboard-grid"><div class="dashboard-card" id="pie-card"><div class="panel-header"><h2>Breakdown</h2><select id="group-select" aria-label="Group by"><option value="category" selected>Category</option><option value="activity">Activity</option><option value="user">User</option><option value="node">Node</option><option value="presence">Presence</option></select></div><canvas id="pie-canvas" aria-label="Process breakdown pie chart"></canvas><div id="pie-empty" hidden>No processes to display</div></div> ... </div>` replacing `#dots-container` slot.

- [ ] **Step 1: Write failing tests**
  ```python
  def test_index_page_has_pie_canvas_and_group_select(client):
      html = client.get("/").text
      assert 'id="pie-canvas"' in html
      assert 'id="group-select"' in html
      assert 'value="category"' in html and 'value="user"' in html and 'value="activity"' in html

  def test_pie_empty_state_hidden_when_data_present(client):
      html = client.get("/").text
      assert 'id="pie-empty"' in html
      # JS unit: aggregateForGroup([], "category") renders empty div visible, chart hidden
  ```
  JS unit (in `tests/web/test_live_charts_groups.py` or new file):
  ```js
  test_updatePieChart_empty_shows_placeholder: build empty agg → chart hidden, #pie-empty not hidden
  test_pie_uses_textContent_for_html_category: category="<img onerror=x>" appears as literal label, not element
  test_pie_caps_at_12_slices: 20 users → chart data has 12 entries, last label "Other"
  ```
- [ ] **Step 2: Run tests to verify they fail**
  Run: `env -u LD_PRELOAD /home/parton/node-monitor/venv/bin/python -m pytest tests/web/test_live_dashboard_page.py -v`
  Expected: FAIL — `pie-canvas` not in HTML
- [ ] **Step 3: Implement pie chart wiring**
  - In `live_charts.js`: implement `createPieChart`/`updatePieChart` using `Chart` global from `chart.umd.min.js` (vendored, loaded via `base.html` `<script src="/static/vendor/chart.umd.min.js">`). Do not `import` Chart; use `window.Chart`. Handle empty `agg.groups.length===0` by hiding canvas (`canvas.hidden=true`) and showing `#pie-empty`.
  - In `index.js`: import `{GROUP_DEFS, aggregateForGroup, buildPieDataset, createPieChart, updatePieChart, PALETTE, CATEGORY_COLORS}` from `./live_charts.js`; remove `DotsCanvas` import and all `dotsCanvas`/`#dots-container` refs. Add `let group="category"` persisted in `sessionStorage` under `nm:live:group`; on `DOMContentLoaded` read it, set `<select>` value. On `#group-select` `change`, update `group`, persist, call `applyFilter()`. In `applyFilter()`, after recomputing `filtered`, compute `agg=aggregateForGroup(filtered, group)` and call `updatePieChart(pieChart, agg, group)` (lazy-create on first call). Also update pie legend colors via `colorFor`.
  - In `index.html`: remove `<div id="dots-container">`; insert `#live-dashboard` grid structure inside the former `.node-map-panel` or as new sibling above process list; keep `#node-tabs` as global filter at top of dashboard (move it out of the old dots panel so it filters all charts + table). Keep `#category-legend` and `.category-bar` but make them reflect current `group` (see Task 4) — for this task they may stay category-only; Task 4 generalizes.
  - In `dashboard.css`: add `.dashboard-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:1rem}` and `.dashboard-card{background:#0f172a;border:1px solid #1e293b;border-radius:8px;padding:0.75rem;min-height:220px}` plus `#pie-canvas{max-height:280px}`.
  - All user labels via `textContent`; tooltip strings are plain text.
- [ ] **Step 4: Run tests to verify they pass**
  Run: `env -u LD_PRELOAD /home/parton/node-monitor/venv/bin/python -m pytest tests/web/test_live_dashboard_page.py tests/web/test_live_charts_groups.py -q` and `ruff check .`
  Expected: PASS
- [ ] **Step 5: Commit**
  ```bash
  git add node_monitor/web/static/js/live_charts.js node_monitor/web/static/js/index.js node_monitor/web/templates/index.html node_monitor/web/static/css/dashboard.css tests/web/test_live_dashboard_page.py
  git commit -m "feat(web): pie breakdown by group on live dashboard

Co-Authored-By: Claude Code <noreply@anthropic.com>"
  ```

### Task 3: Resource usage bars — RSS and CPU per group, color-coded

**Files:**
- Modify: `node_monitor/web/static/js/live_charts.js`
- Modify: `node_monitor/web/static/js/index.js`
- Modify: `node_monitor/web/templates/index.html`
- Test: `tests/web/test_live_dashboard_page.py` (extend)

**Interfaces:**
- Consumes: `GROUP_DEFS`, `aggregateForGroup`, `colorFor`, `buildBarDatasets`, shared `group` state from Task 2.
- Produces:
  - `export function createBarChart(canvasEl, which) -> Chart` where `which` is `"rss"` or `"cpu"`: `type:"bar"`, `indexAxis:"y"` (horizontal), `responsive:true`, `scales.x.beginAtZero:true`, `scales.x.ticks.callback` formats `rss` via `formatRss` (`"12.3 GiB"`) and `cpu` via `Math.round` + `"s"`, `plugins.tooltip` plain text.
  - `export function updateBarChart(chart, agg, group, which)` — sets `labels` and `datasets[0].data` (`rss` or `cpu`), `backgroundColor` via `colorFor(group, key)` per bar, `chart.update()`.
  - DOM: inside `#live-dashboard`, after pie card, two cards: `<div class="dashboard-card" id="rss-card"><h3>Memory (RSS)</h3><canvas id="rss-canvas"></canvas><div id="rss-empty" hidden>No processes</div></div>` and `<div class="dashboard-card" id="cpu-card"><h3>CPU time</h3><canvas id="cpu-canvas"></canvas><div id="cpu-empty" hidden>No processes</div></div>`. Both share the same `#group-select` so changing group recolors and regroups all three charts.

- [ ] **Step 1: Write failing tests**
  ```python
  def test_index_has_rss_and_cpu_canvases(client):
      html = client.get("/").text
      assert 'id="rss-canvas"' in html
      assert 'id="cpu-canvas"' in html

  def test_bar_chart_labels_are_text_not_html(client):
      # Seed a process with username="<b>evil</b>" via fixture; assert page renders label as text (httpx fetch + JS unit checks buildBarDatasets labels contain raw string)
      pass
  ```
  JS unit:
  ```js
  test_buildBarDatasets_sums_rss_and_cpu: agg from 2 procs same group → rss sum, cpu sum
  test_bar_colors_match_pie_colors_for_same_group: colorFor(group,key) same in pie and bars
  test_updateBarChart_empty_hides_canvas: empty agg → canvas hidden, empty div visible
  ```
- [ ] **Step 2: Run tests to verify they fail**
  Run: `env -u LD_PRELOAD /home/parton/node-monitor/venv/bin/python -m pytest tests/web/test_live_dashboard_page.py::test_index_has_rss_and_cpu_canvases -v`
  Expected: FAIL — `rss-canvas` not found
- [ ] **Step 3: Implement bar charts**
  - In `live_charts.js`: implement `createBarChart`/`updateBarChart`; import `formatRss` helper (either duplicate small formatter or import from `list.js`; prefer local `fmtRss` to avoid cycle — keep `formatRss` in `list.js` and also export a tiny `fmtRss` from `live_charts.js`). Use `backgroundColor` array 1:1 with labels via `colorFor`. Cap bars at 12 + Other (reuse `aggregateForGroup` already capped). Use `borderWidth:1, borderColor:"#1e293b"`.
  - In `index.js`: on init, create `rssChart=createBarChart(...)` and `cpuChart=createBarChart(...)`; in `applyFilter()` after pie update, call `updateBarChart(rssChart, agg, group, "rss")` and `updateBarChart(cpuChart, agg, group, "cpu")`. Ensure `group` change updates all three charts atomically.
  - In `index.html`: add the two canvas cards inside `#live-dashboard`; keep overall grid responsive (3 cards: pie + rss + cpu).
  - CSS: ensure bar canvases have `max-height:320px; width:100%`.
- [ ] **Step 4: Run tests to verify they pass**
  Run: `env -u LD_PRELOAD /home/parton/node-monitor/venv/bin/python -m pytest tests/web/test_live_dashboard_page.py -q` + `ruff check .`
  Expected: PASS
- [ ] **Step 5: Commit**
  ```bash
  git add node_monitor/web/static/js/live_charts.js node_monitor/web/static/js/index.js node_monitor/web/templates/index.html tests/web/test_live_dashboard_page.py
  git commit -m "feat(web): RSS and CPU bar charts per group on live dashboard

Co-Authored-By: Claude Code <noreply@anthropic.com>"
  ```

### Task 4: Layout restructure — dashboard on top, process list below, node filter preserved

**Files:**
- Modify: `node_monitor/web/templates/index.html`
- Modify: `node_monitor/web/static/js/index.js`
- Modify: `node_monitor/web/static/css/dashboard.css`
- Modify: `node_monitor/web/templates/base.html` if needed (no changes expected)
- Test: `tests/web/test_live_dashboard_page.py`

**Interfaces:**
- Consumes: `#node-tabs` (All nodes / login-01|02|04), `#live-dashboard` (pie+rss+cpu), `#process-table-container`, `#category-legend`, `#category-mix` bar.
- Produces: final page structure:
  ```
  header-bar (Polaris Login Nodes + heartbeat)
  #node-tabs (global filter, sticky below header)
  #live-dashboard.dashboard-grid (pie-card | rss-card | cpu-card) — 3 cols desktop, 1 col ≤1024px, 2 cols ≤640px stacked
  .main-content reflowed: process list is full-width section below dashboard (not side-by-side with old dots)
  #category-legend + #category-mix now reflect current `group` (not only category): legend items are group keys with matching pie/bar colors, counts shown
  footer: per-node cards + dormancy + recent events unchanged but below process list
  ```
  Node tabs filter all charts + table; text filter (`#process-filter`) also filters all; both respect `AbortController` cancel of in-flight fetch.

- [ ] **Step 1: Write failing tests**
  ```python
  def test_node_tabs_still_present_and_filter_dashboard(client):
      html = client.get("/").text
      assert 'data-node="__all"' in html and 'data-node="login-01"' in html
      assert 'id="live-dashboard"' in html
      assert 'id="process-table-container"' in html
      # JS unit: selecting login-01 with 2 nodes' data filters pie agg to only that node's procs

  def test_process_list_is_below_dashboard(client):
      html = client.get("/").text
      assert html.index('live-dashboard') < html.index('process-table-container')

  def test_dots_container_removed(client):
      html = client.get("/").text
      assert 'dots-container' not in html and 'dots.js' not in html

  def test_legend_reflects_current_group(client):
      # After switching group to "user", legend items are usernames, not categories (JS unit)
      pass
  ```
- [ ] **Step 2: Run tests to verify they fail**
  Run: `env -u LD_PRELOAD /home/parton/node-monitor/venv/bin/python -m pytest tests/web/test_live_dashboard_page.py -v`
  Expected: FAIL — `live-dashboard` before `process-table-container` ordering or `dots-container` still present
- [ ] **Step 3: Implement layout**
  - `index.html`: move `#node-tabs` out of old `.node-map-panel` to a sticky bar under `header-bar` (e.g., `<div id="node-tabs" class="tab-bar" style="...sticky...">`). Remove `.main-content` two-column grid that housed dots + jobs side-by-side; replace with single-column flow: first `#live-dashboard` grid, then `<section class="panel jobs-panel" style="margin:1rem">` full-width. Keep `#category-legend` and `#category-mix` inside `pie-card` header or as sibling row above dashboard — make them group-aware: in `applyFilter()` rebuild `legend` from `agg.groups` (not just category) using `colorFor(group, key)` and show `key — count (rss sum)`; bar stays as thin segmented strip under tabs if kept, or merged into dashboard cards — decide to keep thin strip as overview but recolor by group.
  - `index.js`: remove any remaining `dotsCanvas`/`DotsCanvas`/`dots-container` code; ensure `applyFilter()` is the single place that updates pie, bars, legend, and table from `filtered`; ensure `selectedNode` change triggers same path and `sessionStorage` persists `selectedNode` too (`nm:live:node`). Ensure `refresh()` still aborts prior fetch via `AbortController` and never applies stale data (check `signal.aborted` before `applyFilter()`).
  - `dashboard.css`: replace `.main-content{grid-template-columns:minmax(280px,420px) 1fr; grid-template-rows:80vh}` with single column; add `.dashboard-grid` responsive breakpoints; ensure `header-bar` remains sticky, `#node-tabs` sticky at `top: var(--header-height)`; cards have `overflow:hidden`.
  - Do not add new JS deps; reuse existing `dashboard.css` variables.
- [ ] **Step 4: Run tests to verify they pass**
  Run: `env -u LD_PRELOAD /home/parton/node-monitor/venv/bin/python -m pytest tests/web/test_live_dashboard_page.py -q` + `ruff check .` + manual `curl -s http://127.0.0.1:9998/ | grep -E "live-dashboard|pie-canvas|rss-canvas|cpu-canvas"`
  Expected: PASS, HTML contains all three canvases in correct order
- [ ] **Step 5: Commit**
  ```bash
  git add node_monitor/web/templates/index.html node_monitor/web/static/js/index.js node_monitor/web/static/css/dashboard.css tests/web/test_live_dashboard_page.py
  git commit -m "feat(web): dashboard layout with node filter and process list below

Co-Authored-By: Claude Code <noreply@anthropic.com>"
  ```

### Task 5: Polish and edge-case hardening

**Files:**
- Modify: `node_monitor/web/static/js/live_charts.js`
- Modify: `node_monitor/web/static/js/index.js`
- Modify: `node_monitor/web/templates/index.html` (a11y labels, empty states)
- Modify: `node_monitor/web/static/css/dashboard.css` (responsive tweaks)
- Test: `tests/web/test_live_dashboard_page.py` + `tests/web/test_live_charts_groups.py`

**Interfaces:**
- Consumes: all prior tasks' DOM and chart instances.
- Produces:
  - Empty states: when `filtered.length===0`, all three canvases hidden, their `#*-empty` divs shown with text “No processes match filter” (pie) / “No processes” (bars); legend shows “—” and category bar width 0.
  - “Other” bucket tooltip: `Other (N groups)` with summed rss/cpu in tooltip.
  - Group persistence: `group` and `selectedNode` restored from `sessionStorage` on load; invalid values fall back to `category` / `__all`.
  - Click-through: clicking a pie slice or bar sets `#process-filter` to that key (e.g., `alice` or `ai_agent`) and re-applies filter; clicking `Other` clears filter.
  - A11y: `aria-label` on `<select>`, `role="img"` + `aria-label` on canvases, keyboard focus on `#group-select` and `#node-tabs` buttons.
  - Responsive: at ≤640px dashboard stacks vertically, bar chart y-labels truncate to 14 chars with full label in tooltip.

- [ ] **Step 1: Write failing tests**
  ```python
  def test_empty_filter_shows_empty_state(client):
      # JS unit: filtered=[] → pie-empty visible, rss-empty visible, cpu-empty visible

  def test_other_bucket_tooltip_includes_group_count():
      procs = [{"username":f"u{i}","rss_kb":1,"cpu_seconds":1} for i in range(30)]
      agg = aggregateForGroup(procs, "user")
      assert any(g["key"]=="Other" and g["count"]==19 for g in agg["groups"])  # 12 cap → 11 + Other(19)

  def test_clicking_pie_slice_sets_filter():
      # JS unit: simulate chart onClick → process-filter value == slice label, table refilters

  def test_group_persists_in_sessionStorage():
      # JS unit: set group to "user", reload init → select value still "user"
  ```
- [ ] **Step 2: Run tests to verify they fail**
  Run: `env -u LD_PRELOAD /home/parton/node-monitor/venv/bin/python -m pytest tests/web/test_live_dashboard_page.py tests/web/test_live_charts_groups.py -v`
  Expected: FAIL — empty states not shown, Other count off, click handler missing
- [ ] **Step 3: Implement polish**
  - `live_charts.js`: add `onClick` handler factory for Chart.js that maps clicked element index → `agg.groups[idx].key` → calls `onSliceClick(key)` callback; handle `Other` specially (clear filter). Ensure tooltip callbacks produce plain strings, never HTML. Truncate y-axis labels for bars: `ticks.callback:(v,i)=>labels[i].length>14 ? labels[i].slice(0,14)+'…' : labels[i]` but tooltip shows full.
  - `index.js`: wire `onSliceClick` for all three charts to set `#process-filter` value and dispatch `input` event (debounced applyFilter picks it up); ensure `applyFilter()` shows/hides empty divs. Add `sessionStorage` persist for `group` and `selectedNode` with fallback. Add `aria-live="polite"` region for dashboard updates if needed.
  - `index.html`: add `aria-label`/`role` attributes, ensure empty divs have `role="status"`.
  - `dashboard.css`: media queries for ≤640px single column, ensure cards don't overflow; style `#group-select` as pill matching existing `.tab-btn` aesthetics.
- [ ] **Step 4: Run tests to verify they pass**
  Run: `env -u LD_PRELOAD /home/parton/node-monitor/venv/bin/python -m pytest tests/web/test_live_dashboard_page.py tests/web/test_live_charts_groups.py -q` + `ruff check . && ruff format --check .` + manual verify `curl -s http://127.0.0.1:9998/ | grep -c 'aria-label'`
  Expected: PASS
- [ ] **Step 5: Commit**
  ```bash
  git add node_monitor/web/static/js/live_charts.js node_monitor/web/static/js/index.js node_monitor/web/templates/index.html node_monitor/web/static/css/dashboard.css tests/web/test_live_dashboard_page.py tests/web/test_live_charts_groups.py
  git commit -m "feat(web): polish live dashboard empty states and interactions

Co-Authored-By: Claude Code <noreply@anthropic.com>"
  ```

---

## Self-review

- **Spec coverage:** Pie by users/categories/activity + extra groups (node, presence) in Task 1–2; RSS & CPU plots color-coded by same group in Task 3; All-nodes/node filter preserved in Task 4; process list below dashboard in Task 4; all use existing live API with no backend change.
- **Type consistency:** `GROUP_DEFS` keys `category|activity|user|node|presence` consistent across Task 1→2→3→4→5; `aggregateForGroup` signature `(processes: Process[], group: string) -> {groups, total, capped}` unchanged; `colorFor(group,key,index)` stable; Chart instances `Chart` from `window.Chart` throughout.
- **Review Focus pinned:** empty → Task 5; 60-user cap → Task 1 & 2 & 5; abort race → Task 4; XSS → Task 2 & 3; group persistence/consistency → Task 4 & 5.
- **Proportion:** ~40% of spec length; no transcribed bodies — only signatures, HTML skeletons, and assertions; each task has its own failing test before code.
