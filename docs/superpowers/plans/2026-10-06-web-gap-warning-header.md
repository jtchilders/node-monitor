# Web Gap Warning and Compact Header Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Warn operators about missing one-minute counter windows across the full selected wall-clock range and reduce empty header space without changing typography.

**Architecture:** Extend service enrichment so counter gap metadata includes leading, internal, and trailing gaps bounded by the selected range and response timestamp. Render that canonical metadata in a compact amber warning strip, and tighten only header height, padding, and gaps. Preserve null gaps and all existing range semantics.

**Tech Stack:** Python 3.11, FastAPI service contract, vanilla JavaScript/CSS, pytest, Playwright.

---

### Task 1: Boundary-aware counter gap metadata

**Files:**
- Modify: `node_monitor/web/service.py`
- Test: `tests/web/test_web_snapshot_contract.py`

- [ ] Add failing tests proving a one-hour range with six recent rows reports the leading missing windows, an internal discontinuity reports its missing windows, a stale last row reports trailing missing windows, and a complete range reports zero.
- [ ] Run each new test and confirm it fails because current enrichment only compares adjacent rows.
- [ ] Pass the requested range boundary into enrichment and compute non-overlapping leading, internal, and trailing missing one-minute windows. Return `missing_count`, `max_gap_minutes`, and bounded interval records with a location field; do not synthesize rows.
- [ ] Run `tests/web/test_web_snapshot_contract.py` and confirm green.
- [ ] Commit the service and contract tests.

### Task 2: PBS Monitor-style warning and compact header

**Files:**
- Modify: `node_monitor/web/static/index.html`
- Modify: `node_monitor/web/static/app.js`
- Modify: `node_monitor/web/static/styles.css`
- Test: `tests/browser/test_dashboard_states.py`

- [ ] Add failing browser tests proving the amber warning is visible with exact missing-window and maximum-gap text, hidden when no gaps exist, retained after refresh failure with the last complete snapshot, and responsive without horizontal overflow.
- [ ] Add a layout test proving desktop header height is no more than 64px while computed font sizes for existing header text remain unchanged.
- [ ] Run the focused browser tests and confirm RED.
- [ ] Add an `aria-live="polite"` warning strip below the header; render canonical server metadata only. Hide it for zero gaps.
- [ ] Change desktop header height to 64px and reduce padding/gaps only. Preserve existing font-size declarations and responsive behavior.
- [ ] Run `tests/browser/test_dashboard_states.py` and confirm green.
- [ ] Commit the frontend and browser tests.

### Task 3: Verification and review

**Files:**
- Verify all changed production and test files.

- [ ] Run `venv/bin/python -m pytest tests/web tests/browser` using the maintained repository environment.
- [ ] Run `venv/bin/python -m pytest` for the full suite.
- [ ] Temporarily disable boundary inclusion and confirm the boundary regression test fails; restore byte-for-byte and rerun green.
- [ ] Inspect the production dashboard in a real browser at desktop and narrow widths; confirm warning visibility, sane dimensions, painted charts, and no overflow.
- [ ] Obtain independent exact-head review; resolve every Critical or Important finding and repeat verification/review.
- [ ] Merge to `main`, push, and verify local main SHA equals origin/main and GitHub main SHA.
