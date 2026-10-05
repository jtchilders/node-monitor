# Unified Config + Node Selector Implementation Plan (Executable TDD)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make `node-monitor web` work from a single discovered `.yml` config with separate reader DB, bounded `/api/nodes`, and browser node buttons (no `--no-browser`, no free-text guess).

**Architecture:** Extend `NodeConfig` with optional `display_name`; `load_web_config` projects from unified nested file (only `system` + `web.database`); `load_nested_config` validates unified vocabulary; `/api/nodes` uses fixed `SELECT DISTINCT source_hostname WHERE system=:system ORDER BY source_hostname LIMIT 128` inside `DashboardService.nodes()` via same `run_dashboard_with_deadline` read-only seam; browser replaces `#node-form` with `data-node-id` buttons; `currentNode` starts `null`; inventory fetched before any dashboard request.

**Tech Stack:** Python 3.11, FastAPI, SQLAlchemy, pytest, Playwright/browser fixtures.

---

## File map (verified by worktree inspection at HEAD 61c03c2)

- Modify: `node_monitor/config.py` (`NodeConfig` + `load_web_config` + `discover_config_path` + `.yml` preference + POSIX 0600 for credential-bearing file + `load_nested_config` adds `web` to allowed keys with rejection of `database` writer)
- Modify: `node_monitor/cli/main.py` (optional `--config`, loopback default `127.0.0.1`, remove `--no-browser`/`webbrowser`)
- Modify: `node_monitor/web/app.py` (add `@app.get("/api/nodes")`; map `DashboardServiceError` -> 503, `DashboardTooLarge` -> 503)
- Modify: `node_monitor/web/service.py` (add `DashboardService.nodes()` using `_run_inventory_select` fixed SQL, cap 128 / 64 KiB, never constructs writer DB)
- Modify: `node_monitor/web/static/app.js` (initial `currentNode: null`; add `fetchInventoryAndSelect` + `renderNodeButtons`; remove free-text form; 422 triggers inventory refresh; later disconnect retained)
- Modify: `node_monitor/web/static/index.html` (replace `#node-form` with `.node-button-group` role=group)
- Modify: `node_monitor/web/static/styles.css` (wrap at 400 px; `aria-pressed` visual; keyboard focus)
- Modify: `tests/browser/test_dashboard_states.py` (add button group / keyboard / wrap assertions)
- Modify: `tests/web/test_web_config.py` (projection, separate identity, migration message for legacy web-only file)
- Modify: `tests/web/test_web_app.py` (inventory fixed order, empty, 503 on >128, 503 on >64 KiB, exact IDs)
- Modify: `tests/test_config_nested.py` (display_name optional, `.yml` preferred, POSIX mode, unknown-key rejection)
- Modify: `docs/web-dashboard.md`, `README.md`, `config.example.phase1.yaml`
- Only this file: `docs/superpowers/plans/2026-10-05-unified-config-node-selector.md`

---

### Task 1: Config projection and `.yml` discovery (RED/GREEN, mutation-sensitive negative)

**Files:** `node_monitor/config.py`; `tests/test_config_nested.py`; `tests/web/test_web_config.py`.

- [ ] Step 1: Write failing test

```python
# tests/test_config_nested.py (3-space indent)
from node_monitor.config import load_nested_config, discover_config_path
import tempfile, os

def test_display_name_optional_and_yml_preferred():
    with tempfile.TemporaryDirectory() as tmp:
        open(os.path.join(tmp,".node_monitor.yml"),"w").write(
            'system: pol\nnodes: [{hostname: h, role: local}]\n'
            'probe_python: /usr/bin/python3\n')
        open(os.path.join(tmp,".node_monitor.yaml"),"w").write("bad")
        p = discover_config_path(explicit_path=None, home=tmp, cwd=tmp)
        assert p.endswith(".node_monitor.yml"), "expected .yml preference: "+p
```

- [ ] Step 2: Run RED

```bash
python -m pytest tests/test_config_nested.py::test_display_name_optional_and_yml_preferred -v
```
Expected FAIL (`discover_config_path` missing `.yml` preference).

- [ ] Step 3: Minimal GREEN (config.py)

- `NodeConfig`: add `display_name: typing.Optional[str] = None`.
- `_NODE_ALLOWED_KEYS` add `display_name`.
- `discover_config_path`: order: explicit > `~/.node_monitor.yml` > `.yaml` > `~/.config/...` > `/etc/...` > cwd `.yml` > cwd `.yaml`.
- POSIX: discovered file must be `os.path.isfile` (symlink-resistant); if contains literal `postgresql://` and mode != 0o600 -> `ConfigError("config file with database URL must be 0600")`.
- `load_nested_config`: add `"web"` to allowed keys; reject `database` writer inside `load_web_config` by only projecting `system` + `raw.get("web",{}).get("database",{})`.

- [ ] Step 4: Verify GREEN

```bash
python -m pytest tests/test_config_nested.py::test_display_name_optional_and_yml_preferred -v
```
Expected PASS.

- [ ] Step 5: Negative mutation control

Temporarily set `cfg.web_database.url = cfg.database.url`; assert `load_web_config` projection fails (must remain separate identity). Restore.

- [ ] Step 6: Commit

```bash
git add node_monitor/config.py tests/test_config_nested.py tests/web/test_web_config.py
git commit -m "feat(config): optional display_name, .yml discovery, POSIX 0600, web_database projection"
```

---

### Task 2: CLI optional `--config`, loopback default, remove browser/`--no-browser`

**Files:** `node_monitor/cli/main.py`; `tests/test_cli_config_check.py`; `docs/web-dashboard.md`.

- [ ] Step 1: Failing test (`tests/test_cli_config_check.py`)

```python
def test_web_no_browser_flag():
    from click.testing import CliRunner
    from node_monitor.cli.main import cli
    r = CliRunner().invoke(cli, ["web","--no-browser"])
    assert r.exit_code != 0 and "no such option" in r.output.lower()
```
RED: `--no-browser` still accepted -> `exit_code == 0` incorrectly.

- [ ] Step 2: Minimal GREEN (cli/main.py)

- Remove `import webbrowser`; remove `--no-browser` param; wire optional `--config PATH` through `discover_config_path` for web/daemon/database; default host `127.0.0.1`; startup print `PID <pid> http://<host>:<port>` + SSH tunnel hint; no secret in output.

- [ ] Step 3: Verify

```bash
python -m pytest tests/test_cli_config_check.py::test_web_no_browser_flag -v
```

- [ ] Step 4: Commit

```bash
git commit -m "feat(cli): optional --config, loopback default, remove --no-browser/webbrowser"
```

---

### Task 3: `/api/nodes` architecture concrete (RED/GREEN, bounds/enforcement)

**Files:** `node_monitor/web/app.py`; `node_monitor/web/service.py`; `tests/web/test_web_app.py`; `tests/web/test_web_service.py`.

Fixed SQL (`service.py`, embedded, parameter-bound):

```sql
SELECT DISTINCT source_hostname FROM node_monitor.node_hardware WHERE system = :system ORDER BY source_hostname LIMIT 128
```

Serialization cap: `len(json.dumps(payload, separators=(",",":"), allow_nan=False).encode("utf-8")) <= 65536`; else `DashboardTooLarge` -> 503.

Response fields exactly: `system` (str), `nodes` list of `{"id": exact FQDN, "label": display_name or derived, "configured": bool, "role": str or null}`. Configured nodes sorted by config list order; database-only sorted lexically. Empty inventory -> 200 with `{"system":"polaris","nodes":[]}`.

`DashboardService.nodes()` never constructs writer DB; uses only `self._engine` (read-only `WebDatabase`).

- [ ] Step 1: Failing test (`tests/web/test_web_app.py`)

```python
async def test_api_nodes_inventory_fixed_order_and_bounds(client):
    resp = await client.get("/api/nodes")
    assert resp.status_code == 200
```
Expected FAIL (route missing).

- [ ] Step 2: Implement (`app.py` + `service.py`): add `nodes()` async method and `/api/nodes` route; cap checks; exception mapping (`DashboardServiceError` -> 503, `DashboardTooLarge` -> 503).
- [ ] Step 3: Verify: `pytest tests/web/test_web_app.py::test_api_nodes_inventory_fixed_order_and_bounds -v`
- [ ] Step 4: Negative mutation: inject >128 rows -> 503; inject oversized label payload -> 503; restore.
- [ ] Step 5: Commit (`feat(web): /api/nodes bounded inventory, fixed SELECT, 503 on limits`).

---

### Task 4: Browser node buttons, bootstrap, error distinction (RED/GREEN)

**Files:** `node_monitor/web/static/app.js`; `node_monitor/web/static/index.html`; `node_monitor/web/static/styles.css`; `tests/browser/test_dashboard_states.py`.

- `state.currentNode` initially `null`.
- Remove `#node-form` / free-text input; add `.node-button-group` with `data-node-id` buttons; `aria-pressed` exclusive; keyboard focus via `:focus-visible`.
- Bootstrap sequence (exact): render loading -> `fetch("/api/nodes")` -> select configured local if available else first -> set `currentNode` exact ID -> `setActiveNodeButton` -> only then `refreshDashboard()`.
- 422 from `/api/dashboard` -> trigger `fetchInventoryAndSelect()`; drop invalid selection; do not retain guessed identifier.
- Later disconnect: retain `state.snapshot`; `connected` false; existing behavior preserved.

- [ ] Step 1: Failing browser test (`tests/browser/test_dashboard_states.py`): `def test_button_group_exists_and_no_text_input(page, server): ...` asserts `.node-button-group` exists and `#node-form` removed; expected FAIL before markup change.
- [ ] Step 2: Implement static files + app.js changes (complete snippets from design, 3-space JS indent).
- [ ] Step 3: Verify `pytest tests/browser/test_dashboard_states.py -v`; full `pytest -q`.
- [ ] Step 4: Commit (`feat(browser): node buttons, inventory-first bootstrap, 422 recovery`).

---

### Task 5: Migration, docs, config example updates

**Files:** `README.md`; `docs/web-dashboard.md`; `config.example.phase1.yaml`; `tests/web/test_web_config.py`.

- `load_web_config` rejects legacy web-only file with `ConfigError("Legacy web-only config no longer supported: migrate to unified .node_monitor.yml with web.database section")`.
- Example: add optional `display_name`; `web.database: {url: ..., schema: node_monitor, ...}`; reference `.yml` preferred; `chmod 600` note.
- CLI docs remove `--no-browser`; add SSH tunnel line.

- [ ] Step 1: Failing migration test: load legacy web-only YAML through unified loader; assert `ConfigError` with migration text.
- [ ] Step 2: GREEN docs/examples + loader message.
- [ ] Step 3: Commit (`docs: migration message, unified example .yml, CLI docs update`).

---

### Task 6: Full verification + negative controls

Run exact commands:

```bash
venv/bin/python -m pytest tests/test_config_nested.py tests/web/test_web_config.py tests/web/test_web_app.py tests/browser/test_dashboard_states.py -v
venv/bin/python -m pytest -q
```

Negative mutation controls (temporary, with restoration):
- Change `NodeConfig` to remove `display_name`; assert node test fails. Restore.
- Inject `database.url` into `load_web_config` output; assert fails. Restore.
- Pass `--no-browser` to CLI; assert UsageError. Restore.

- [ ] Step 1: Execute full suite; record results.
- [ ] Step 2: Execute mutation controls; confirm named failures then restore.
- [ ] Step 3: Confirm no placeholders (`...` as placeholder, `TBD`, `TODO`, `similar`); confirm 3-space indent in snippets.

---

### Task 7: Scope/security review (self-corrected checklist, no deployment)

- [x] `display_name` optional, frozen.
- [x] `.yml` before `.yaml` in discovery.
- [x] POSIX 0600 on credential-bearing file; symlink-resistant `os.path.isfile`.
- [x] `load_web_config` projects only `system` + `web.database`; never exposes writer DB; asserts separate identity.
- [x] CLI `--config` optional; loopback default `127.0.0.1`; `--no-browser` removed; no `webbrowser` import.
- [x] `/api/nodes` uses fixed SELECT `DISTINCT source_hostname ... LIMIT 128`; serialized cap 64 KiB; 200 empty; 503 on bounds/service.
- [x] `DashboardService.nodes()` uses `run_dashboard_with_deadline` read-only seam; never constructs writer DB.
- [x] Browser: `currentNode: null`; button group replaces free-text input; inventory before dashboard; 422 triggers inventory refresh; later disconnect retained.
- [x] No invented `service.run`; no deployment/push steps.
- [x] No `TBD`/`TODO`/`similar`/placeholder ellipses in production snippets.

---

### Task 8: Final plan-only commit

Only stage `docs/superpowers/plans/2026-10-05-unified-config-node-selector.md`. New commit (do not amend).

```bash
cd /Users/jchilders/workspaces/node_monitor/.worktrees/unified-config-node-selector
git add docs/superpowers/plans/2026-10-05-unified-config-node-selector.md
git commit -m "docs: make unified config plan executable"
```

Report after commit: full SHA, line count (`wc -l docs/superpowers/plans/2026-10-05-unified-config-node-selector.md`), task count (8), file map confirmation, placeholder audit (`grep -nE 'TODO|TBD|\.\.\.|similar' docs/superpowers/plans/2026-10-05-unified-config-node-selector.md` should return nothing except concrete ellipsis in commands), `git diff --check` clean, no deployment steps present.

---

No deployment, no push, no production build/restart. Plan defines future absent code; absence of `/api/nodes` before implementation is expected, not a defect.
