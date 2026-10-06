# Memory Chart Category RSS Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the available-memory chart series with total capacity and non-additive p50 RSS overlays grouped by process classification.

**Architecture:** Reuse the existing atomic dashboard response. Extend the frontend projection in `app.js` to derive category p50 RSS points from usage grains and render those alongside the one-minute system-used series and a dotted total-memory reference. No SQL, schema, collector, or API changes are required.

**Tech Stack:** Vanilla JavaScript, Chart.js, pytest, Playwright.

---

### Task 1: Specify the chart projection

**Files:**
- Modify: `tests/browser/test_dashboard_charts_aggregation.py`
- Modify: `node_monitor/web/static/app.js`

- [ ] Add a browser test fixture containing multiple categories, two activities for one category at one interval, and counter rows at one-minute timestamps.
- [ ] Assert `memoryUsedSeries` equals `MemTotal - MemAvailable` at each counter timestamp.
- [ ] Assert `memoryTotalSeries` repeats `mem_total_kb` at real counter timestamps and preserves null gap slots.
- [ ] Assert `memoryAvailableSeries` is absent.
- [ ] Assert category p50 RSS selects the largest activity grain per `(category, interval_end)`, remains separated by category, and aligns missing timestamps as null rather than zero.
- [ ] Run the exact test and confirm RED because total/category projections do not exist and Available remains.
- [ ] Implement the minimal projection in `buildChartData`.
- [ ] Run the exact test and confirm GREEN.

### Task 2: Specify rendering semantics

**Files:**
- Modify: `tests/browser/test_dashboard_charts.py`
- Modify: `node_monitor/web/static/app.js`
- Modify: `node_monitor/web/static/index.html`

- [ ] Add rendering assertions for exactly one solid System used dataset with visible circular points.
- [ ] Assert Total memory is dotted and has no visible points.
- [ ] Assert no dataset label contains Available.
- [ ] Assert each category dataset label is `<category> RSS p50 (non-additive)` and datasets are not stacked or filled.
- [ ] Assert the visible explanatory note states that RSS is non-additive and does not reconcile to system-used memory.
- [ ] Run the exact test and confirm RED.
- [ ] Implement the minimal Chart.js datasets and chart note.
- [ ] Run the exact test and confirm GREEN.

### Task 3: Validate behavior and sensitivity

**Files:**
- Test: `tests/browser/test_dashboard_charts.py`
- Test: `tests/browser/test_dashboard_charts_aggregation.py`
- Test: `tests/web/`

- [ ] Run `/Users/jchilders/workspaces/node_monitor/venv/bin/python -m pytest tests/web tests/browser -q` and require success.
- [ ] Temporarily reintroduce Available or disable category grouping and prove the targeted test fails; restore byte-for-byte.
- [ ] Capture a real-browser screenshot only after chart pixels and Connected state are verified; inspect dimensions and visual semantics.
- [ ] Run `/Users/jchilders/workspaces/node_monitor/venv/bin/python -m pytest -q` and require success.
- [ ] Run `git diff --check` and inspect the complete base-to-head diff.

### Task 4: Review and integrate

**Files:**
- Review all files changed from base `14578dae241c86881a7156b96cc799e7f068559b`.

- [ ] Commit implementation with a conventional commit.
- [ ] Obtain independent exact-head review; Critical or Important findings block progress.
- [ ] Push the branch and open a GitHub pull request.
- [ ] Merge only after local verification and review pass.
- [ ] Verify local main, origin/main, and GitHub main have the same merge SHA.
- [ ] Remove the feature worktree and branch.
