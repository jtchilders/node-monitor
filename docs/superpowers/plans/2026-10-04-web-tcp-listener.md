# Web TCP Listener Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace node-monitor's Unix-socket web listener with PBS Monitor-compatible TCP host/port options while preserving read-only database and bounded-failure guarantees.

**Architecture:** Keep web data/configuration and database preflight unchanged except that `socket_path` disappears from `WebConfig`. The CLI validates `--host`/`--port`, constructs the existing app, and invokes a focused TCP Uvicorn runtime. Browser launch is best-effort and separately testable; database disposal remains deterministic.

**Tech Stack:** Python 3.9+, Click, FastAPI, Uvicorn, pytest, Playwright, PostgreSQL/SQLAlchemy.

---

### Task 1: Web configuration becomes database-only

**Files:**
- Modify: `node_monitor/config.py:959-1140`
- Test: `tests/web/test_web_config.py`

- [ ] Add failing tests proving a config with only `web.database` succeeds, `WebConfig` has no `socket_path`, and legacy `socket_path` is rejected.
- [ ] Run `venv/bin/python -m pytest tests/web/test_web_config.py -q` and confirm failure is due to the old required socket contract.
- [ ] Remove socket-path validation and state from `WebConfig`/`load_web_config`; restrict `web` to `database` only.
- [ ] Re-run the focused tests and commit the green increment.

### Task 2: TCP Uvicorn runtime and CLI contract

**Files:**
- Modify: `node_monitor/web/runtime.py`
- Modify: `node_monitor/cli/main.py:1354-1447`
- Test: `tests/web/test_web_runtime.py`
- Test: `tests/web/test_web_cli.py`

- [ ] Add failing runtime tests proving `uvicorn.Config` receives `host`/`port` and never `fd`/`uds`.
- [ ] Add failing CLI tests for defaults (`127.0.0.1:8080`), custom host/port, `--no-browser`, wildcard browser URL normalization, invalid host/port rejection before DB construction, bounded runtime failure, and guaranteed DB disposal.
- [ ] Run focused tests and confirm expected RED failures.
- [ ] Implement `run_uvicorn(app, host, port)` and CLI options `--host`, `--port`, `--no-browser`; validate host and port at Click boundary.
- [ ] Implement a small pure browser URL helper; make browser-open failure non-fatal.
- [ ] Emit exactly `PID <pid> http://<host>:<port>` and preserve sanitized failure handling.
- [ ] Run focused tests and commit the green increment.

### Task 3: Remove obsolete socket surface and update packaging/docs

**Files:**
- Delete: `node_monitor/web/socket.py`
- Modify: `docs/web-dashboard.md`
- Modify: `README.md`
- Modify: `tests/web/test_web_socket.py`
- Modify: `tests/web/test_web_packaging.py`
- Modify: any tests importing `node_monitor.web.socket`
- Modify: `config.example.phase1.yaml` only if it contains web socket material

- [ ] Add/adjust failing packaging and import-surface tests proving no production or documented Unix-socket interface remains.
- [ ] Search the full repository for `socket_path`, `bind_private_socket`, `--uds`, and Unix-socket runbook commands; classify every hit as removal or historical design text.
- [ ] Delete the obsolete module and migrate all relevant tests/docs to TCP host/port behavior.
- [ ] Run `venv/bin/python -m pytest tests/web -q` and commit the green increment.

### Task 4: Real TCP integration acceptance

**Files:**
- Modify/Create: `tests/web/test_web_tcp_integration.py`
- Modify: existing browser fixture only if it can reuse the production runtime without weakening isolation

- [ ] Add a failing integration test that allocates an ephemeral loopback port, starts the real production web command/runtime with a controlled production-shaped service/database seam, and verifies HTTP 200 `/health` and parsed HTTP 200 `/api/dashboard`.
- [ ] Add occupied-port coverage requiring nonzero exit, bounded output, and database disposal.
- [ ] Run the test to prove RED against incomplete behavior.
- [ ] Make the minimum runtime/fixture changes needed for GREEN without production test hooks.
- [ ] Run the integration test and commit.

### Task 5: Documentation and configuration migration

**Files:**
- Modify: `docs/web-dashboard.md`
- Modify: `README.md`
- Modify: `node-monitor` Polaris deployment configuration after merge only

- [ ] Document foreground TCP operation, defaults, custom host/port, `--no-browser`, `screen`, health/API checks, shared-network exposure, logs, duplicate bind behavior, and removal of SSH-tunnel/Unix-socket instructions.
- [ ] Ensure examples use `--host 0.0.0.0 --port 9998 --no-browser` for shared access and explicitly state the unauthenticated read-only exposure boundary.
- [ ] Run documentation regression tests and search for stale operational instructions.
- [ ] Commit documentation.

### Task 6: Full verification and independent gates

**Files:** all changed files

- [ ] Run `venv/bin/python -m pytest -q`.
- [ ] Run `venv/bin/python -m pytest -q tests/web tests/browser` explicitly.
- [ ] Run a sabotage check: temporarily restore the old runtime invocation or remove host/port forwarding and prove the new focused test fails; restore and rerun GREEN.
- [ ] Independently inspect the complete diff for credential leakage, authentication assumptions, wildcard exposure semantics, bounded errors, lifecycle cleanup, and unrelated changes.
- [ ] Obtain independent code-review and security-review verdicts; any Critical/Important finding blocks merge.
- [ ] Resolve findings test-first and rerun all gates.

### Task 7: PR, merge, and Polaris deployment

**Files:** deployment state only after reviewed merge

- [ ] Push with the `jtchilders-ai-assistant` GitHub identity and open a PR against `jtchilders/node-monitor:main`.
- [ ] Verify PR head/base/files and live checks; merge only after required reviews and green exact-head verification.
- [ ] Verify local `main`, `origin/main`, and GitHub merged SHA match.
- [ ] On Polaris, preserve the current installation/config, update `~/node-monitor` to the exact merged SHA, rebuild/install its venv if required, remove `socket_path` from `web.config.dev.yaml`, and retain mode `0600`.
- [ ] Do not disturb the running collector daemon; verify its PID/start ticks and heartbeat before and after deployment.
- [ ] Start the web process only if Taylor explicitly asks for that production side effect. Otherwise report the exact command:

```bash
cd ~/node-monitor
screen -S node-monitor-web
venv/bin/node-monitor web --config web.config.dev.yaml --host 0.0.0.0 --port 9998 --no-browser
```

- [ ] If started with approval, verify the exact process, port listener, HTTP 200 `/health`, parsed HTTP 200 `/api/dashboard` for a stored hostname, and duplicate-bind rejection.
