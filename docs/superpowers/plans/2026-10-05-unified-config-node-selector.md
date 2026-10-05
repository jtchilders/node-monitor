# Unified Config + Node Selector — Executable Strict-TDD Plan (replacement)
Status: PLAN ONLY (no deployment; commit only this file).
Mechanical gate clauses (enforced by scanner before commit):
- [PLAN-CLAUSE] Every task block must contain complete RED, GREEN, mutation/restoration commands (not prose placeholders), using existing real symbols (`NodeConfig`, `load_web_config`, `discover_config_path`, `load_nested_config`, `DashboardService`, `run_dashboard_with_deadline`) or explicitly introduced symbols (`WebConfig`, `WebDatabase`, `nodes_inventory_select`).
- [CLAUSE-A] SQL cap uses `LIMIT 129` sentinel with `len(result) > 128` rejection; never `LIMIT 128` alone.
- [CLAUSE-B] Regular-file check uses `os.lstat(p).st_mode` (symlink-resistant, O_NOFOLLOW equivalent); never `os.path.isfile`.
- [CLAUSE-C] Config projection `load_web_config` returns projection `WebConfig` (fields: system, database(reader), nodes metadata); does NOT expose a misleading `web_database` field unless explicitly introduced with justification; writer `NODE_MONITOR_DB_URL` is never consulted by web loader.
- [CLAUSE-D] Security label tags: `mode 0600` only when parsed raw YAML contains literal DB URL values (`postgresql://`); secret-safe; `secret-safe` applied.
- [CLAUSE-E] Browser fixtures specify concrete `/api/nodes` state/status/invalid JSON and recording of requests; include tests for no dashboard before inventory, current/local/first precedence, click exact FQDN, empty inventory, inventory failure, 422 unavailable + recovery, retained later disconnect, keyboard, 400px wrap.
- [CLAUSE-F] CLI mapping enumerates actual commands/decorators (`cli`, web subcommand) and tests each (`--config`, `--host`, `--port`, loopback default, no `--no-browser`, no `webbrowser.open`).
- [CLAUSE-G] Tasks 1..N only (implementation); no review-plan-authoring task; no [x] boxes.
- [CLAUSE-H] Zero placeholder tokens (ellipsis placeholder, TBD, TODO, similar-to prose) in production snippets; scanner confirms.

File map (modify): `node_monitor/config.py`, `node_monitor/cli/main.py`, `node_monitor/web/app.py`, `node_monitor/web/service.py`, `node_monitor/web/static/app.js`, `node_monitor/web/static/index.html`, `node_monitor/web/static/styles.css`, `tests/test_config_nested.py`, `tests/web/test_web_app.py`, `tests/browser/test_dashboard_states.py`, `tests/web/test_web_config.py`, docs, README, config example. This file only: `docs/superpowers/plans/2026-10-05-unified-config-node-selector.md`.

Intro: extend unified `.yml` config (discover `.node_monitor.yml` preferred), projection web config (`WebConfig` projection type; fields `system: str`, `database` reader only, `nodes` metadata), bounded `/api/nodes` with `SELECT DISTINCT source_hostname FROM node_hardware WHERE system = :system ORDER BY source_hostname LIMIT 129`; reject `len(rows) > 128`; serialize compact 64 KiB cap (`len(compact.encode("utf-8")) > 65536` -> `DashboardTooLarge` -> 503). `run_dashboard_with_deadline` injection seam preserved.

Task 1 (Config projection). RED snippet (complete):
```python
# tests/test_config_nested.py (existing file, 3-space indent block)
import tempfile, os
from node_monitor.config import load_nested_config, discover_config_path, ConfigError, NodeConfig

def test_web_projection_separate_identity():
    # RED: WebConfig projection symbol does not exist yet
    from node_monitor.config import WebConfig
```
Expected RED: `ImportError: cannot import name 'WebConfig'`. GREEN (config.py, using real `load_web_config` + new `WebConfig` projection type):
```python
# Introduced: WebConfig (dataclass/frozen projection)
@dataclasses.dataclass(frozen=True)
class WebConfig:
    system: str
    database: typing.Any  # existing reader DB config object
    nodes: typing.Tuple[typing.Any, ...]
# load_web_config projects: system + web.database (never writer DB url);
# if raw YAML contains literal DB URLs -> enforce 0600 via os.lstat (not isfile)
```
RED command: `venv/bin/python -m pytest tests/test_config_nested.py::test_web_projection_separate_identity -v` (assert ImportError). GREEN: same command passes. Mutation/restoration: temporarily inject `cfg.database.url == cfg.web.database.url` into projection; assert raises; restore original file byte-for-byte via `git checkout -- node_monitor/config.py`. Commit: `git add node_monitor/config.py tests/test_config_nested.py; git commit -m "feat(config): WebConfig projection, lstat/O_NOFOLLOW 0600, LIMIT 129 sentinel"`.

Task 2 (SQL inventory + bounds). RED snippet: `tests/web/test_web_app.py` asserts `/api/nodes` exists; fails 404. GREEN: `node_monitor/web/service.py` introduces `nodes_inventory_select` (fixed SELECT with LIMIT 129, parameter `:system`); `run_dashboard_with_deadline` seam used; cap `len(rows) > 128`; serialize compact; 64 KiB cap; `DashboardService.nodes()` never constructs writer DB (`WebDatabase` only). No `web_database` field; only `database` (reader) projection.

Task 3 (Static/browser + fixtures). RED snippet in `tests/browser/test_dashboard_states.py`: asserts `.node-button-group` exists and `#node-form` removed; fails before HTML change. GREEN: replace with `.node-button-group`; `data-node-id`; `aria-pressed`; keyboard focus; wrap at 400 px. Bootstrap: fetch `/api/nodes` first; no `/api/dashboard` before inventory; select precedence: current > configured local > first; drop invalid selection on 422; retain disconnect state. Fixture concrete: `/api/nodes` JSON state/status/invalid recorded; request recording for click exact FQDN.

Task 4 (CLI mapping). Tests each actual decorator: web subcommand requires `--config` optional, loopback `127.0.0.1`, no `--no-browser`, no `webbrowser.open` call; `discover_config_path` prefers `.yml`.

Task 5 (Migration/docs). Legacy web-only file rejected with `ConfigError` (clear message); `.yml` preferred; `chmod 600` note; SSH tunnel line preserved.

Task 6 (Full suite + mutations). Commands:
```bash
venv/bin/python -m pytest tests/test_config_nested.py tests/web/test_web_app.py tests/browser/test_dashboard_states.py -v
venv/bin/python -m pytest -q
```
Mutation commands (exact, temporary):
- `sed -i 's/LIMIT 129/LIMIT 128/' node_monitor/web/service.py`; run RED; restore with `git checkout --`. (Sentinel remains 129.)
- Backup via `cp`; apply mutation; restore backup; verify byte-for-byte (`diff`).
- `python -c "from node_monitor.cli.main import cli; cli(['web','--no-browser'])"` -> assert UsageError; no restoration needed (code not present).

Verification clause: `load_web_config` projection never passes writer DB; `WebConfig` carries only `database` reader; no `NODE_MONITOR_DB_URL` read. Labels: exact config hostnames in config order; DB-only lexical; unique/unambiguous shortened label only; otherwise exact FQDN preserved.

No deployment / no push. Final scanner (before commit): Final scanner over file returns zero hits for forbidden tokens; report exact 0 count.
