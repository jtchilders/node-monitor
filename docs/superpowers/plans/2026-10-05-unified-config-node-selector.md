# Unified Config and Node Selector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement strict-TDD unified `~/.node_monitor.yml` canonical config, database-backed `database` writer / `web.database` reader separation, `--config` CLI option, POSIX mode policy, fixed `/api/nodes`, browser node buttons with bootstrapping/error distinction, removal of `webbrowser` and `--no-browser`, PBS-like `node-monitor web --port`, loopback default, and migration/compatibility tests.

**Architecture:** Extend `NodeMonitorConfig` with a web projection (`database` writer / `web.database` reader) without letting the web layer construct a writer DB. Keep strict loading; never merge representations. All CLI commands (`daemon`, `database`, `web`) accept optional `--config`. Default bind is loopback (`127.0.0.1`). Remove browser launch paths entirely.

**Tech Stack:** Python 3.11, SQLAlchemy 2, FastAPI, pytest, YAML, click.

---

## File Map

- Modify: `node_monitor/config.py` (add web projection, POSIX mode, `--config` support)
- Modify: `node_monitor/cli/main.py` (add `--config`; remove `--no-browser`/webbrowser)
- Modify: `node_monitor/web/app.py` (`/api/nodes` bounds/labels/config ordering)
- Modify: `node_monitor/web/service.py` (node-button bootstrapping/distinction)
- Modify: `node_monitor/web/static_impl.py` (button state, error distinction)
- Modify: `tests/test_config_nested.py`, `tests/web/test_web_config.py`
- Create: `tests/test_unified_config_migration.py`
- Plan: `docs/superpowers/plans/2026-10-05-unified-config-node-selector.md` (this file)

---

### Task 1: Config Extension (Web Projection, POSIX Mode, --config)

**Files:** `node_monitor/config.py`; tests: `tests/test_config_nested.py`

- [ ] **Step 1: Write failing test** — `test_unified_web_projection_separates_writer_reader()` asserts `NodeMonitorConfig.web_database` exists and does not share identity with `database` writer.

```python
def test_unified_web_projection_separates_writer_reader():
   cfg = load_nested_config(...)
   assert cfg.database.url != cfg.web_database.url
```

- [ ] **Step 2: Verify FAIL**

Run: `python -m pytest tests/test_config_nested.py::test_unified_web_projection_separates_writer_reader -v`
Expected: FAIL — `AttributeError: 'NodeMonitorConfig' object has no attribute 'web_database'`

- [ ] **Step 3: Minimal GREEN** — add `web_database: DatabaseConfig` frozen field; enforce POSIX mode (`0o600`) on default config path; never merge flat/nested representations.

- [ ] **Step 4: Verify PASS**

- [ ] **Step 5: Commit**

```bash
git add -f docs/superpowers/plans/2026-10-05-unified-config-node-selector.md
git commit -m "docs: plan unified config and node selector"
```

---

### Task 2: CLI `--config` and Removal of `--no-browser` / `webbrowser`

**Files:** `node_monitor/cli/main.py`; `tests/test_cli_config.py`

- [ ] **Step 1:** `test_cli_web_accepts_config_and_rejects_no_browser()` asserts `click.UsageError` on `--no-browser`; asserts `--config` loads `NodeMonitorConfig`.
- [ ] **Step 2:** Verify FAIL (current `--no-browser` accepted).
- [ ] **Step 3:** Remove `webbrowser` import, `--no-browser` param; wire `--config` through `load_config_file_any`.
- [ ] **Step 4:** Verify PASS.
- [ ] **Step 5:** Commit.

---

### Task 3: PBS-like `node-monitor web --port`; Default Loopback

**Files:** `node_monitor/web/app.py`, `node_monitor/web/service.py`

- [ ] **Step 1:** `test_web_port_pbs_like()` asserts `service.run(host='127.0.0.1', port=8080)` starts; `port` must be int `1..65535`; default loopback.
- [ ] **Step 2:** Verify FAIL.
- [ ] **Step 3:** Implement host/port binding with loopback default; bind `port` to Uvicorn; reject non-int/zero/65536.
- [ ] **Step 4:** Verify PASS.
- [ ] **Step 5:** Commit.

---

### Task 4: `/api/nodes` Fixed Inventory with Bounds/Labels/Config Ordering

**Files:** `node_monitor/web/app.py`, `tests/web/test_web_app.py`

- [ ] **Step 1:** `test_api_nodes_inventory_fixed_order()` asserts response contains ordered nodes with `label`, `config_order`, bounds `[min, max]`, empty inventory `[]`.
- [ ] **Step 2:** Verify FAIL.
- [ ] **Step 3:** Add route; return ordered JSON; enforce exact fields; never construct writer DB.
- [ ] **Step 4:** Verify PASS.
- [ ] **Step 5:** Commit.

---

### Task 5: Browser Node Buttons / Bootstrapping / Error Distinction

**Files:** `node_monitor/web/static_impl.py`, `tests/browser/test_dashboard_states.py`

- [ ] **Step 1:** `test_button_state_distinguishes_error_from_bootstrapping()` asserts `button-state=bootstrapping` vs `button-state=error`; `request_id` present.
- [ ] **Step 2:** Verify FAIL.
- [ ] **Step 3:** Implement button state machine; separate bootstrapping from error; include exact `request_id`.
- [ ] **Step 4:** Verify PASS.
- [ ] **Step 5:** Commit.

---

### Task 6: Compatibility / Migration / Security Tests

- [ ] Write `tests/test_unified_config_migration.py`: multiple nodes, empty inventory, 422 invalidation, network/5xx failure, exact post-click state, reader/writer credential separation, no browser launch, mutation-sensitive negative controls.
- [ ] Verify full suite: `venv/bin/python -m pytest -q` passes.
- [ ] Verify scope: no deployment steps; no `webbrowser`; no `--no-browser`; `database` writer never exposed to `web.database` reader.
- [ ] Verify docs/examples preserved.

---

### Deployment Boundary (Excluded)

Deployment (packaging, systemd, supervisor) is explicitly excluded. A separately approved resource-bounded plan will cover it.

---

## Self-Review Checklist (Before Commit)

- [ ] Spec sections covered: unified config (`~/.node_monitor.yml`), legacy fallbacks, `--config`, database/web separation, POSIX mode (`0o600`), `/api/nodes` fixed inventory, button states, removal of `webbrowser`/`--no-browser`, PBS-like `--port`, loopback default.
- [ ] No placeholders (`TBD`, `TODO`, `similar to above`).
- [ ] Type names consistent (`NodeMonitorConfig`, `DatabaseConfig`, `WebDatabase`).
- [ ] 3-space Python indentation in examples.
- [ ] Subagent-driven-development referenced in header.
- [ ] Concrete assertions and exact commands included.
- [ ] No deployment included.

**Verification commands:**

```bash
git log --oneline -1
grep -rE 'TBD|TODO|similar to' docs/superpowers/plans/ || echo "No placeholders found"
venv/bin/python -m pytest tests/web tests/browser -q
```

Expected results: SHA of new plan commit; zero placeholder hits; `tests/web` and `tests/browser` PASS; full `pytest -q` PASS.

**Ambiguities resolved:**
- `database` writer and `web.database` reader are separate frozen `DatabaseConfig` instances; reader never constructs writer.
- POSIX mode applies to `~/.node_monitor.yml` default path only (`0o600`).
- Browser launch removed; `--no-browser` removed; loopback (`127.0.0.1`) is default bind.
- Deployment excluded from this plan.

Plan saved to `docs/superpowers/plans/2026-10-05-unified-config-node-selector.md`. Commit message: `docs: plan unified config and node selector`.
