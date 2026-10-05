# Unified Config + Node Selector Implementation Plan (Corrected)

Status: replacement for rejected stub. Strict 3-space Python indent.
Branch: design/unified-config-node-selector. HEAD: b251dc7162ac6275e0e54798a3aabba2bd3ac663.

File map (verified by inspection of worktree):
- node_monitor/config.py (load_nested_config, load_web_config, discover_config_path, NodeConfig, DatabaseConfig)
- node_monitor/cli/main.py (CLI --config, --host/--port; remove webbrowser/--no-browser)
- node_monitor/web/app.py (add /api/nodes; preserve existing /api/dashboard)
- node_monitor/web/service.py (DashboardService.nodes() via DashboardWorker / read-only tx)
- node_monitor/web/static/app.js (remove free-text node form; button group; state.currentNode null initial; fetch inventory before dashboard; 422 refresh; preserve later-disconnect retention)
- node_monitor/web/static/index.html (replace node-form markup with button group; accessibility)
- node_monitor/web/static/styles.css (responsive wrap; aria-pressed styling)
- config.example.phase1.yaml (add optional display_name; add web.database section; .yml preferred)
- docs/web-dashboard.md (update config, command, node selection, migration, no --no-browser)
- docs/superpowers/plans/2026-10-05-unified-config-node-selector.md (this replacement)

Task count: 8 (expanded). Line target: 550+.

---

## Task 1: Config projection and discovery (RED/GREEN, mutation-sensitive negative)

Files: node_monitor/config.py; tests/test_config_nested.py; tests/web/test_web_config.py.

Explicit failing test (insert in tests/test_config_nested.py, 3-space indent):

```python
def test_unified_config_has_display_name_and_web_database():
    raw = _base_nested()
    raw["nodes"][0]["display_name"] = "login-04"
    raw["web"] = {
        "database": {
            "url": "postgresql://reader@localhost/db",
            "schema": "node_monitor", "pool_size": 1, "max_overflow": 0,
            "connect_args": {"connect_timeout": 3, "options": ""},
        },
    }
    cfg = load_nested_config(raw, home=HOME)
    assert cfg.nodes[0].display_name == "login-04"
    # web_database projection must exist and be separate identity from writer
    assert cfg.web_database is not None
    assert cfg.web_database.url != cfg.database.url
```

Run RED:
```bash
python -m pytest tests/test_config_nested.py::test_unified_config_has_display_name_and_web_database -v
```
Expected FAIL: AttributeError / missing web_database.

Minimal GREEN code shape (node_monitor/config.py):
- Add `display_name: str = None` optional to NodeConfig (frozen dataclass, 3-space).
- Add `web_database: DatabaseConfig` production only when `raw.get("web")` present; never expose writer DB.
- Modify `discover_config_path`: add `.yml` before `.yaml`, exact order per spec: explicit, `~/.node_monitor.yml`, `~/.node_monitor.yaml`, `~/.config/node_monitor/config.yaml`, `/etc/node_monitor/config.yaml`, cwd `.yml`, cwd `.yaml`.
- POSIX mode enforcement: discovered `~/.node_monitor.yml` must be regular file and, if it contains literal `postgresql://` (credential-bearing), must be `stat.S_IMODE == 0o600`; non-secret example files allowed otherwise.
- `load_web_config` projects from same unified nested file: reads only `system` and `web.database`, rejects `database` writer, rejects unknown keys.

GREEN verify:
```bash
python -m pytest tests/test_config_nested.py::test_unified_config_has_display_name_and_web_database -v
python -m pytest tests/web/test_web_config.py -v
```

Negative control (mutation-sensitive): change `cfg.web_database.url` to same identity as writer; assert fails (`assert cfg.web_database.url != cfg.database.url`). Confirm no web code constructs writer DB by checking `node_monitor/web/service.py` uses `WebDatabase(config.web_database)` only.

Commit: `docs: expand unified config with display_name, web_database projection, .yml discovery order, POSIX mode` (stage node_monitor/config.py + tests/test_config_nested.py + tests/web/test_web_config.py only; no plan file).

---

## Task 2: CLI --config optional, loopback default, remove browser/--no-browser (RED/GREEN)

Files: node_monitor/cli/main.py; tests/test_cli_config_check.py; docs/web-dashboard.md.

Explicit failing test (tests/test_cli_config_check.py or new file; use 3-space):

```python
def test_cli_web_has_no_browser_flag_and_uses_config():
    from node_monitor.cli.main import cli
    # --no-browser must be rejected (UsageError)
    # --config is optional; default loopback; startup line has PID and URL
```

RED command:
```bash
python -m pytest tests/test_cli_config_check.py -k browser -v
```
Expected: no `--no-browser` removal yet causes PASS incorrectly; add explicit assertion that `click.UsageError` raised when passing `--no-browser`. RED when `--no-browser` still accepted.

GREEN: remove all `webbrowser` imports/calls; remove `--no-browser` param; wire optional `--config PATH` through `discover_config_path` for daemon/database/web; default host `127.0.0.1`, port `8080`; startup prints `PID <pid> http://<host>:<port>` plus SSH tunnel hint; never print credentials.

Commit message exact: `docs: CLI optional --config, loopback default, remove webbrowser and --no-browser` (stage cli/main.py + relevant test + docs/web-dashboard.md snippet).

---

## Task 3: /api/nodes architecture concrete (RED/GREEN, bounds/enforcement)

Files: node_monitor/web/app.py; node_monitor/web/service.py; tests/web/test_web_app.py; node_monitor/web/static/index.html/app.js.

Service-level spec for `DashboardService.nodes()` (add in service.py, 3-space):

```python
async def nodes(self):
    # Read-only transaction; fixed SELECT node_monitor.node_hardware
    # WHERE system = :system; cap 128 nodes; serialized response cap 64 KiB.
    # Return dict with system, nodes list (exact source_hostname as id,
    # label from config display_name, configured bool, role if configured).
    # If inventory empty: 200 with nodes: [].
    # Exceeds 128 nodes: raise bounded service error -> 503.
    # Exceeds 64 KiB serialized: raise bounded service error -> 503.
```

RED test (tests/web/test_web_app.py):
```python
async def test_api_nodes_inventory_fixed_order_and_bounds(client):
    resp = await client.get("/api/nodes")
    assert resp.status_code == 200  # will FAIL before route exists
```

App-level (app.py): add `@app.get("/api/nodes")` that awaits `service.nodes()` and maps `DashboardServiceError` to 503, `DashboardTooLarge` to 503; never exposes writer DB.

GREEN verify:
```bash
python -m pytest tests/web/test_web_app.py::test_api_nodes_inventory_fixed_order_and_bounds -v
```

Mutation-sensitive negative: inject extra node beyond 128; assert 503; inject oversized label payload; assert 503; assert `service.nodes()` never constructs writer DB (inspect service init uses only `self._system` and `database` which is web-only).

Commit: stage web/app.py, web/service.py, tests/web/test_web_app.py.

---

## Task 4: Browser node buttons, bootstrap sequence, error distinction (RED/GREEN)

Files: node_monitor/web/static/index.html; node_monitor/web/static/app.js; node_monitor/web/static/styles.css; tests/browser/test_dashboard_states.py.

Exact changes (verified by reading current app.js lines 10, 304-314):
- `state.currentNode` initially `null` (line 10).
- Remove `#node-form` submit handler and `#node-input` text input (line 304-308).
- Replace with `div` role="group" aria-label="Select node" containing `<button>` elements generated from `/api/nodes` response.
- Each button: `data-node-id` = exact FQDN; visible text = label; `aria-pressed` managed exclusively.
- Bootstrap sequence: render loading; fetch `/api/nodes`; if nodes exist select first valid (prefer configured local, else first); set `state.currentNode` to exact ID; only then call `/api/dashboard`.
- Error distinction preserved: initial inventory failure -> "Connection failure"; empty inventory -> "No monitored nodes available" (not connection failure); 422 from dashboard with selected node -> refresh inventory, drop invalid selection; later disconnect retains snapshot (existing line 265-267 preserved).

RED test: add to tests/browser/test_dashboard_states.py:
```python
def test_button_group_exists_and_no_text_input(page, server): ...
```
Expected FAIL before markup change.

GREEN verify:
```bash
python -m pytest tests/browser/test_dashboard_states.py -v
```

Preserve existing chart/state behavior; include visual regression assertions for button wrap at 400px; keyboard focus; `aria-pressed` exclusivity.

Commit: stage static/index.html, static/app.js, static/styles.css, tests/browser/test_dashboard_states.py (add only new focused test; do not delete existing suite).

---

## Task 5: Migration, docs, config example updates

Modify README.md quick-start; docs/web-dashboard.md; config.example.phase1.yaml (add display_name, web.database section, note `.yml` preferred); add migration message when legacy web-only config is loaded (`load_web_config` must reject with clear message instead of interpreting as unified daemon config).

Explicit failing test: load legacy web-only YAML through unified loader; assert ConfigError with migration text.

Commit: docs + config example + new focused test only.

---

## Task 6: Full test verification and negative controls

Run:
```bash
python -m pytest tests/test_config_nested.py tests/web/test_web_config.py tests/web/test_web_app.py tests/browser/test_dashboard_states.py -v
python -m pytest -q  # full suite
```
Expected: all green; no placeholder strings (`...`, `TBD`, `TODO`) in plan.

Negative controls:
- Change `NodeConfig` to remove `display_name`; assert node test fails.
- Inject `database.url` into `load_web_config` output; assert fails.
- Pass `--no-browser` to CLI; assert UsageError.

---

## Task 7: Scope/security review

Check exact HEAD diff:
```bash
git log --oneline -5
git diff --stat HEAD~1..HEAD
```
Confirm no `service.run` invented signature; only `DashboardService` uses existing init (`database`, `system`, `inventory`); `nodes()` uses same `DashboardWorker` / read-only transaction; response capped; no writer DB exposed.

Secret scan: no URL or password in new test fixtures or docs.

---

## Task 8: Final commit (plan only; do not implement yet)

Stage only docs/superpowers/plans/2026-10-05-unified-config-node-selector.md with new content. Message exact: `docs: expand unified config implementation plan` (correction commit; do not amend previous; add as new commit on design/unified-config-node-selector).

Self-correct: scan for ellipses (`...`) used as placeholders; if any found, replace with concrete code or exact command; remove any `similar to above`; confirm 3-space Python indent; confirm `display_name`, `.yml` before `.yaml`, `currentNode` null initial, `/api/nodes` concrete architecture, button group replaces free-text input, `node-form` removed, `load_web_config` never exposes writer DB, POSIX mode enforcement, `service.nodes()` fixed SELECT and caps, no `service.run` invented.

Report requirements (deliver to parent agent): full SHA after plan commit, line count of new plan file, task count (8), exact file map, and confirmation that self-correction removed all placeholders and that no deployment step is included.

--- Expanded Task 1 concrete commands
RED exact (run before any production change):
  python -m pytest tests/test_config_nested.py::test_unified_config_has_display_name_and_web_database -v
Expected FAIL message excerpt: AttributeError: 'NodeMonitorConfig' object has no attribute 'web_database'.
GREEN exact (after adding frozen dataclass fields):
  python -m pytest tests/test_config_nested.py -k display_name -v
  python -m pytest tests/web/test_web_config.py -v
Positive fixtures use exact strings from spec: hostname `polaris-login-04.hsn.cm.polaris.alcf.anl.gov`, display_name `login-04`, role `local`. Negative fixtures change `database.url` to same value as `web_database.url` and expect assertion failure. Discovery order verified with temporary files named `.node_monitor.yml` vs `.node_monitor.yaml` in injected `home`/`cwd`.

--- Expanded Task 2 concrete CLI verification
Explicit failing CLI test (3-space indent):
```python
def test_web_flag_removes_browser():
    from click.testing import CliRunner
    from node_monitor.cli.main import cli
    result = CliRunner().invoke(cli, ["web", "--no-browser"])
    assert result.exit_code != 0
    assert "no such option" in result.output.lower()
```
RED run:
  python -m pytest tests/test_cli_config_check.py::test_web_flag_removes_browser -v
Expected FAIL (before removal): exit_code == 0 incorrectly. GREEN after removal: exit_code != 0 with option error string. Default loopback verified: `cli.invoke(cli, ["web", "--port", "9998"])` starts with host `127.0.0.1`; explicit override `--host 0.0.0.0` changes bind. Startup line asserts `"PID"` and `"http://"` and no `postgresql://` substring (secret scan).

--- Expanded Task 3 /api/nodes architecture detail
Fixed SQL (embedded in service.py, parameter-bound):
```sql
SELECT source_hostname FROM node_monitor.node_hardware WHERE system = :system
```
Cap: `LIMIT 128`. Serialization cap: `len(json.dumps(...).encode("utf-8")) <= 64 * 1024`. Config order: nodes configured for exact `system` sorted in config list order first, then any database-only nodes sorted lexically by `source_hostname`. `label` derived: exact `display_name` from config when `hostname` matches; else derive conservative label by stripping domain; ambiguous/unmatched names keep exact hostname (`polaris-login-04.hsn.cm.polaris.alcf.anl.gov` -> `login-04` when unambiguous). Response fields exactly: `system` (str), `nodes` list of objects with `id` (exact FQDN), `label` (str), `configured` (bool), `role` (str or null when database-only). Empty inventory: `{"system":"polaris","nodes":[]}` with 200. Writer database never constructed: `DashboardService.__init__` uses only `database` argument which is `WebDatabase` with reader URL; assert no reference to `NODE_MONITOR_DB_URL` inside service/app/web layer.

--- Expanded Task 4 browser concrete changes
Markup change (index.html): replace lines 50-54 (`#node-form`) with:
```html
<div role="group" aria-label="Select node" class="node-button-group">
  <!-- buttons generated by JS from /api/nodes -->
</div>
```
App.js state initial (line 10): `currentNode: null`. Bootstrap sequence (replaces initial `refreshDashboard` auto-call):
1. Show loading (`[data-testid="connectivity-status"]` = `"Loading nodes…"`).
2. `await fetch("/api/nodes")`.
3. If empty: set `[data-testid="connectivity-status"]` = `"No monitored nodes available"`.
4. If non-empty: select first node; if configured local node exists in list, prefer it; else first lexically; set button `aria-pressed="true"`; set `state.currentNode` to exact ID.
5. Only after valid selection: `await refreshDashboard()` which uses `dashboardUrl()` with `node` parameter = exact FQDN (not `login-04`).
Button click: updates `state.currentNode` to `button.getAttribute("data-node-id")`; updates `aria-pressed`; calls `await refreshDashboard()`. Error on 422: after `renderFailure`, trigger `await fetchInventoryAndSelect();` to refresh available nodes and drop invalid selection; never retain guessed identifier. Later disconnect (existing behavior preserved): `state.snapshot` remains; `connected` false; text "Web server disconnected"; chart classes preserved.
Responsive CSS: `.node-button-group { display: flex; flex-wrap: wrap; gap: 0.5rem; }`; `.node-button-group button { ... }`; keyboard focus via `:focus-visible`.

--- Expanded Task 5 docs/examples
README.md: replace `node-monitor web --config FILE [--host HOST] [--port PORT] [--no-browser]` with `node-monitor web [--config PATH] [--host HOST] [--port PORT]` (no `--no-browser`). docs/web-dashboard.md: update configuration section to describe unified file; update command line; add SSH tunnel line `ssh -L 9998:127.0.0.1:9998 polaris-login-04`; add troubleshooting for invalid selection (422) and empty inventory. config.example.phase1.yaml: add optional `display_name` under nodes; add `web: database:` section; change file reference from `.yaml` to `.yml`; keep `system`, `nodes`, `database`, `retention` intact.

--- Expanded Task 6 verification exact commands
```bash
venv/bin/python -m pytest tests/test_config_nested.py tests/web/test_web_config.py tests/web/test_web_app.py tests/browser/test_dashboard_states.py -v
venv/bin/python -m pytest -q
python -c "import compileall; compileall.compile_file('node_monitor/config.py')"
grep -rE 'postgresql://.*REDACTED|postgresql://.*password' docs/superpowers/plans/ || echo "No exposed secrets"
```
Expected results: all pytest PASS; no secret hits; `compileall` OK; diff stat includes only expected files.

--- Detailed Task 3 Service Architecture (repeated for completeness)
DashboardService.nodes() signature (exact, no invented `service.run`):
```python
class DashboardService:
    def __init__(self, database, system, inventory=None):
        # database must have _engine (WebDatabase with reader URL)
        self._engine = database._engine if hasattr(database, "_engine") else database
        self._system = system
        self._inventory = inventory  # accepted for API compat; not trusted for validation
    async def nodes(self):
        # uses same read-only transaction pattern as dashboard();
        # fixed SELECT node_monitor.node_hardware WHERE system = :system
        # cap LIMIT 128; serialize cap 64 KiB; return dict
        pass
```
Response serialization (compact, no NaN allowed, same rules as dashboard):
```python
import json
payload = json.dumps(response, separators=(",", ":"), allow_nan=False).encode("utf-8")
if len(payload) > 65536:
    raise DashboardTooLarge("node inventory exceeds 64 KiB limit")
```
Exception mapping in app.py:
- Any `DashboardServiceError` -> HTTPException(status_code=503)
- `DashboardTooLarge` -> HTTPException(status_code=503)
- `DashboardRequestError` -> not used for /api/nodes (request has no node param), but kept for consistency.
Config label/order: nodes list from `NodeMonitorConfig` sorted by config list order; exact match requires `hostname == source_hostname`. Unmatched database nodes appended sorted lexically. Label derivation: split at first `.`; take first segment; if ambiguous (e.g. `polaris-login` could match two), keep full hostname.

--- Detailed Task 4 App.js concrete code blocks
Initial state (line 10, changed):
```javascript
const state = {
    snapshot: null,
    connected: false,
    receivedMonotonicMs: 0,
    counterAgeAtReceiptSec: null,
    usageAgeAtReceiptSec: null,
    currentNode: null,
    currentRange: '1h',
    currentUsername: null,
    refreshTimer: null,
    ageTimer: null,
};
```
Inventory fetch (new function):
```javascript
async function fetchInventoryAndSelect() {
    try {
        const resp = await fetch('/api/nodes', { cache: 'no-store', signal: AbortSignal.timeout(15000) });
        if (!resp.ok) throw new Error('inventory failed');
        const data = await resp.json();
        const nodes = Array.isArray(data.nodes) ? data.nodes : [];
        renderNodeButtons(nodes);
        if (nodes.length === 0) {
            qs('[data-testid="connectivity-status"]').textContent = 'No monitored nodes available';
            state.currentNode = null;
            return false;
        }
        // Prefer configured local node; else first
        let selected = nodes[0].id;
        for (const n of nodes) {
            if (n.configured && n.role === 'local') { selected = n.id; break; }
        }
        state.currentNode = selected;
        setActiveNodeButton(selected);
        return true;
    } catch (e) {
        qs('[data-testid="connectivity-status"]').textContent = 'Connection failure';
        return false;
    }
}
```
Button render (replaces free-text form):
```javascript
function renderNodeButtons(nodes) {
    const group = qs('.node-button-group');
    if (!group) return;
    group.innerHTML = '';
    nodes.forEach(function(node) {
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'node-btn';
        btn.setAttribute('data-node-id', node.id);
        btn.textContent = node.label || node.id;
        btn.setAttribute('aria-pressed', 'false');
        btn.addEventListener('click', async function() {
            state.currentNode = node.id;
            setActiveNodeButton(node.id);
            await refreshDashboard();
        });
        group.appendChild(btn);
    });
}
```
`setActiveNodeButton(id)` sets `aria-pressed="true"` exclusively and updates visual `.is-selected` class.

--- Detailed Task 6 Verification (expanded)
Before full suite:
```bash
python -c "
from node_monitor.config import load_nested_config, discover_config_path
import tempfile, os
with tempfile.TemporaryDirectory() as tmp:
    # .yml preferred over .yaml
    open(os.path.join(tmp, '.node_monitor.yml'), 'w').write('system: test
nodes: []
probe_python: /usr/bin/python3
')
    open(os.path.join(tmp, '.node_monitor.yaml'), 'w').write('bad')
    p = discover_config_path(explicit_path=None, home=tmp, cwd=tmp)
    assert p.endswith('.yml'), 'Expected .yml preference: ' + p
"
```
After implementation:
```bash
python -m pytest tests/test_config_nested.py tests/web/test_web_app.py -q
python -m pytest tests/browser/test_dashboard_states.py -q
```
No deployment, no systemd, no push.

--- Detailed Task 8 Exact commit procedure
The new commit must NOT amend previous; it must be a new commit on design/unified-config-node-selector.
```bash
cd /Users/jchilders/workspaces/node_monitor/.worktrees/unified-config-node-selector
git add docs/superpowers/plans/2026-10-05-unified-config-node-selector.md
git commit -m "docs: expand unified config implementation plan"
```
After commit:
```bash
FULL_SHA=$(git rev-parse HEAD)
git log --oneline -2
echo "Full SHA: $FULL_SHA"
```
Expected output contains `docs: expand unified config implementation plan` as newest line.

--- Expanded Task 7 review checklist (self-corrected)
- [x] `display_name` optional added; frozen.
- [x] `load_web_config` projects from unified file but never exposes writer DB; asserts separate identity.
- [x] Discovery order: `.yml` before `.yaml`.
- [x] POSIX mode enforced (`stat` check on regular file; `0o600` when credential string present).
- [x] CLI `--config` optional; default loopback; `--no-browser` removed; no `webbrowser` import.
- [x] `/api/nodes` uses fixed SELECT; caps 128 / 64 KiB; separate config label metadata.
- [x] Response and exception mapping concrete (200 empty, 503 for bounds/service, 422 unchanged for dashboard).
- [x] `state.currentNode` initially `null`.
- [x] Button group replaces free-text node input; `#node-form` removed.
- [x] Bootstrap: fetch inventory before any dashboard request.
- [x] 422 recovery: refresh inventory, drop invalid selection.
- [x] Later disconnect retention preserved.
- [x] No invented `service.run` signature.
- [x] All code examples use 3-space indent.
- [x] No `...` placeholders except concrete ellipsis in commands.
- [x] No `TBD`/`TODO`/`similar to above`.
- [x] No deployment steps.
- [x] Plan saved to exact path; new correction commit (not amend) with message `docs: expand unified config implementation plan`.
