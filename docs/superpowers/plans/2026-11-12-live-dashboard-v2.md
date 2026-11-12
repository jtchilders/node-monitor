# Live dashboard v2 polish Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Polish the live dashboard: (1) memory & CPU resource pies with total-vs-available labels, (2) breakdown legend text fix, (3) health strip always color-coded, (4) group-select moved to node-tabs bar and process table made scrollable.

**Architecture:** Pure front-end change on top of v1 dashboard. Extend `live_charts.js` with `buildResourcePieDataset` for memory/CPU pies (weighted by rss/cpu, not count) and helpers to format total labels from already-fetched node capacities. `index.js` applies the same group filter to resource pies, renders totals from node `mem_total_kb`/`mem_used_kb` and `load1`/`cpu_logical` already in health/nodes payload. `index.html` moves `#group-select` into sticky `#node-tabs` bar; limits `#process-table-container` height. `dashboard.css` fixes legend contrast and health colors.

**Tech Stack:** Python ≥3.10, FastAPI + vanilla JS modules, vendored `chart.umd.min.js@4.4.9`, pytest/httpx, ruff.

**Spec:** User follow-up 2026-11-12: memory pie with "total used out of available", CPU pie with total utilization percent, category dropdown moved to node tabs, process list scrollable, legend text white on dark, health always green/red.

## Global Constraints

- Repo `~/node-monitor/node-monitor`, branch `feat/live-dashboard-v2` from `main` (`bbe7bd0`), venv `~/node-monitor/venv` editable, live commands `env -u LD_PRELOAD`.
- 3-space indent, `ruff check . && ruff format --check .` must pass.
- No CDN — reuse `chart.umd.min.js@4.4.9`; no new npm deps.
- Every visible text via `textContent`/`createTextNode`, never `innerHTML` with user data.
- Existing APIs unchanged: `GET /api/live/processes|nodes|health|events` sole data source; no new backend route.
- Commit or push only when user asks.

## Review Focus

- Empty filter (0 processes) must not throw on resource pies; show "No processes" and totals still from node capacity.
- Single-node vs All-nodes filtering must change totals (capacity sums only for visible nodes) and pies weight correctly.
- RSS not available (null) must not NaN the total; treat as 0.
- Health strip must show green when ok and red when stale/disconnected, never uncolored.
- Legend text must be readable on dark bg at all group values including XSS payloads.

---

### Task 1: Fix legend contrast and health color-coding

**Files:**
- Modify: `node_monitor/web/static/css/dashboard.css`
- Modify: `node_monitor/web/static/js/index.js`
- Modify: `node_monitor/web/templates/index.html`
- Test: `tests/web/test_live_dashboard_page.py`

**Interfaces:**
- Consumes: `#category-legend` items, `#health-strip` heartbeat data.
- Produces: legend items with `color: var(--text-color)` (white-readable) and health strip dot+text always colored green/yellow/red based on heartbeat age and `disconnected` state.

- [ ] **Step 1: Write failing tests**
  ```python
  def test_legend_text_is_white_readable(client):
      html = client.get("/").text
      # CSS must set legend-item color to white/var(--text-color), not black
      assert "legend-item" in html
      css = (Path(__file__).parents[2] / "node_monitor/web/static/css/dashboard.css").read_text()
      assert "legend-item" in css and ("#e2e8f0" in css or "var(--text-color)" in css or "#fff" in css or "white" in css)

  def test_health_strip_color_coded(client):
      html = client.get("/").text
      assert 'id="health-strip"' in html
      js = (Path(__file__).parents[2] / "node_monitor/web/static/js/index.js").read_text()
      assert "freshness-indicator" in js and "green" in js.lower() or "disconnected" in js
  ```
- [ ] **Step 2: Run tests to verify they fail**
  Run: `env -u LD_PRELOAD /home/parton/node-monitor/venv/bin/python -m pytest tests/web/test_live_dashboard_page.py::test_legend_text_is_white_readable tests/web/test_live_dashboard_page.py::test_health_strip_color_coded -v`
  Expected: FAIL
- [ ] **Step 3: Implement**
  - `dashboard.css`: add `.legend-item { color: var(--text-color); }` (or `#e2e8f0`). Ensure `.color-box` keeps its background. Verify no black `color:#000` on legend.
  - `index.js` `refresh()`: after health fetch, set `strip.className` based on health: if `!health.ok` or `heartbeat_age_s > 120` → `freshness-indicator red` (or `yellow` for stale), else `freshness-indicator green`. Also handle disconnected case (already has `disconnected` class on error). Ensure dot gets correct class so header shows green/red continuously.
  - `index.html`: ensure `#health-strip` has initial class reflecting server `ok` (e.g., `class="freshness-indicator {% if ok %}green{% else %}red{% endif %}"`) so SSR also color-coded before JS loads.
- [ ] **Step 4: Run tests to verify they pass**
  Run: `env -u LD_PRELOAD /home/parton/node-monitor/venv/bin/python -m pytest tests/web/test_live_dashboard_page.py -q && env -u LD_PRELOAD /home/parton/node-monitor/venv/bin/python -m ruff check .`
  Expected: PASS
- [ ] **Step 5: Commit**
  ```bash
  git add node_monitor/web/static/css/dashboard.css node_monitor/web/static/js/index.js node_monitor/web/templates/index.html tests/web/test_live_dashboard_page.py
  git commit -m "fix(web): legend contrast and health color-coding

Co-Authored-By: Claude Code <noreply@anthropic.com>"
  ```

### Task 2: Move group-select to node-tabs bar and make process table scrollable

**Files:**
- Modify: `node_monitor/web/templates/index.html`
- Modify: `node_monitor/web/static/css/dashboard.css`
- Modify: `node_monitor/web/static/js/index.js` (selector still `#group-select`, just DOM moved)
- Test: `tests/web/test_live_dashboard_page.py`

**Interfaces:**
- Consumes: `#node-tabs` bar, `#group-select`, `#process-table-container`.
- Produces: `#node-tabs` sticky bar now contains both node filter buttons AND the group-select dropdown (right-aligned). Process table container has `max-height: 45vh` (or 400px) with `overflow-y:auto` so page doesn't grow with 1000s of procs.

- [ ] **Step 1: Write failing tests**
  ```python
  def test_group_select_inside_node_tabs(client):
      html = client.get("/").text
      assert html.index('id="node-tabs"') < html.index('id="group-select"') < html.index('id="live-dashboard"')

  def test_process_table_scrollable(client):
      html = client.get("/").text
      css = (Path(__file__).parents[2] / "node_monitor/web/static/css/dashboard.css").read_text()
      assert "process-table-container" in css and ("max-height" in css or "45vh" in css)
  ```
- [ ] **Step 2: Run tests to verify they fail**
  Run: `env -u LD_PRELOAD /home/parton/node-monitor/venv/bin/python -m pytest tests/web/test_live_dashboard_page.py::test_group_select_inside_node_tabs tests/web/test_live_dashboard_page.py::test_process_table_scrollable -v`
  Expected: FAIL
- [ ] **Step 3: Implement**
  - `index.html`: move `<select id="group-select">` from inside `#pie-card .panel-header` to inside `#node-tabs` bar, e.g., after the node buttons with `style="margin-left:auto"`. Keep same id/options. Leave pie-card header as just `<h2>Breakdown</h2>`.
  - `dashboard.css`: add `#node-tabs { display:flex; align-items:center; gap:0.5rem; } #group-select { margin-left:auto; }` and `#process-table-container { max-height: 45vh; overflow-y: auto; }` (or `.table-wrapper#process-table-container`).
  - `index.js`: no logic change needed—selector id unchanged, still wired in `init()`. Verify `applyFilter` still updates pie.
- [ ] **Step 4: Run tests to verify they pass**
  Run: `env -u LD_PRELOAD /home/parton/node-monitor/venv/bin/python -m pytest tests/web/test_live_dashboard_page.py -q && ruff check .`
  Expected: PASS
- [ ] **Step 5: Commit**
  ```bash
  git add node_monitor/web/templates/index.html node_monitor/web/static/css/dashboard.css tests/web/test_live_dashboard_page.py
  git commit -m "feat(web): move group-select to node tabs and scrollable process table

Co-Authored-By: Claude Code <noreply@anthropic.com>"
  ```

### Task 3: Memory and CPU resource pies with total labels

**Files:**
- Modify: `node_monitor/web/static/js/live_charts.js`
- Modify: `node_monitor/web/static/js/index.js`
- Modify: `node_monitor/web/templates/index.html`
- Test: `tests/web/test_live_charts_groups.py` + `tests/web/test_live_dashboard_page.py`

**Interfaces:**
- Consumes: `aggregateForGroup` (weighted by rss/cpu), `colorFor`, node capacities from `GET /api/health` nodes payload (mem_total_kb, mem_used_kb, load1, cpu_logical) stored in JS.
- Produces:
  - `export function buildResourcePieDataset(agg, group, weightKey) -> {labels,data,backgroundColor}` where weightKey is `"rss"` or `"cpu"` (slices weighted by rss/cpu sum, not count). Empty agg returns empty dataset.
  - `export function formatResourceLabel(totalUsed, totalAvailable, kind) -> string` e.g., `"4.2 GiB used of 64 GiB (6%)"` for memory, `"12,340s CPU"` for CPU. For memory, totalAvailable = sum of mem_total_kb for visible nodes. For CPU, show total cpu_seconds with count.
  - `index.html`: rss-card and cpu-card each gain a `<div class="resource-total" id="rss-total">` / `id="cpu-total"` label above canvas, and canvases get `role="img"` aria-labels for resource pies.
  - `index.js`: on refresh, store `nodeCapacities = health.nodes` (map label→{mem_total_kb, load1, cpu_logical}). In `applyFilter`, compute `totalAvailableMem = sum of mem_total for selectedNode(s)` and `totalUsedMem = sum of rss for filtered procs`; build rss pie via `buildResourcePieDataset(agg, group, "rss")` weighted. Update `rss-total` textContent with formatted label. Similarly for CPU: total cpu_seconds sum, show label.

- [ ] **Step 1: Write failing tests**
  ```python
  def test_buildResourcePieDataset_weights_by_rss():
      procs = [{"category":"a","rss_kb":100,"cpu_seconds":1},{"category":"b","rss_kb":900,"cpu_seconds":9}]
      agg = aggregateForGroup(procs, "category")
      ds = buildResourcePieDataset(agg, "category", "rss")
      assert ds["data"][0] == 900  # b first (sorted by count then rss, but weight is rss)

  def test_formatResourceLabel_memory():
      assert "GiB" in formatResourceLabel(4096*1024, 64*1024*1024, "memory")

  def test_resource_pies_present(client):
      html = client.get("/").text
      assert 'id="rss-total"' in html and 'id="cpu-total"' in html
      assert 'id="rss-canvas"' in html and 'role="img"' in html
  ```
- [ ] **Step 2: Run tests to verify they fail**
  Run: `env -u LD_PRELOAD /home/parton/node-monitor/venv/bin/python -m pytest tests/web/test_live_charts_groups.py::test_buildResourcePieDataset_weights_by_rss tests/web/test_live_dashboard_page.py::test_resource_pies_present -v`
  Expected: FAIL
- [ ] **Step 3: Implement**
  - `live_charts.js`: add `buildResourcePieDataset` and `formatResourceLabel` (and `fmtRss` already exists, reuse). For resource pie, weight by `g.rss` or `g.cpu` instead of `g.count`. Keep colorFor parity.
  - `index.js`: store `window._nodeCaps` or module var `nodeCaps` from health fetch. In `applyFilter`, compute `selectedNodes = selectedNode === "__all" ? allNodeLabels : [selectedNode]`; `totalAvail = sum(nodeCaps[n].mem_total_kb for n in selectedNodes if exists)`; `totalUsed = filtered.reduce((s,p)=>s+(p.rss_kb||0),0)`. Update `document.getElementById("rss-total").textContent = formatResourceLabel(totalUsed, totalAvail, "memory")`. Similarly CPU: `totalCpu = sum(cpu_seconds)`.
  - Create/update rss/cpu charts as doughnut pies (reuse createPieChart logic or new helper) weighted by resource. On empty filtered, still show totals from node caps.
  - `index.html`: add total divs in rss/cpu cards.
- [ ] **Step 4: Run tests to verify they pass**
  Run: `env -u LD_PRELOAD /home/parton/node-monitor/venv/bin/python -m pytest tests/web/test_live_dashboard_page.py tests/web/test_live_charts_groups.py -q && ruff check .`
  Expected: PASS
- [ ] **Step 5: Commit**
  ```bash
  git add node_monitor/web/static/js/live_charts.js node_monitor/web/static/js/index.js node_monitor/web/templates/index.html tests/web/test_live_charts_groups.py tests/web/test_live_dashboard_page.py
  git commit -m "feat(web): memory and CPU resource pies with total labels

Co-Authored-By: Claude Code <noreply@anthropic.com>"
  ```
