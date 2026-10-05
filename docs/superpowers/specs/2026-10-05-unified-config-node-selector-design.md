# Unified Configuration and Node Selection Design

## Status

Approved direction from Taylor Childers on 2026-10-05. This design follows the PBS Monitor operator experience while preserving Node Monitor's read-only web database boundary.

## Problem

Node Monitor currently imposes three avoidable operator and user failures:

1. the daemon and web server require different YAML files;
2. `node-monitor web` requires an explicit config path and includes browser-launch behavior, making the normal command substantially more complicated than `pbs-monitor web --port <port>`;
3. the browser guesses `login-04`, while the API validates the exact `node_hardware.source_hostname`. On Polaris the stored value is `polaris-login-04.hsn.cm.polaris.alcf.anl.gov`, so the initial dashboard request returns HTTP 422 and the UI reports `Connection failure` even though `/health` and the database are healthy.

The live diagnosis established this distinction directly: `/health` returned 200, shorthand `node=login-04` returned 422, and the exact stored hostname returned a populated HTTP-200 dashboard response.

## Goals

- One user-managed configuration file for daemon, database operations, and web.
- Default discovery of `~/.node_monitor.yml`, so ordinary commands need no `--config` argument.
- Separate daemon-writer and web-reader database credentials inside that one file.
- A PBS Monitor-like command: `node-monitor web --port 9998`.
- No automatic browser launch and no `--no-browser` option.
- Database-backed clickable node controls; users never need to type or remember source FQDNs.
- Correctly distinguish web/API failure, no monitored nodes, invalid selection, stale/partial/empty data, and successful connection.
- Preserve all existing telemetry semantics, read-only web preflight, API bounds, static-resource protections, and daemon behavior.

## Non-goals

- No database migration or schema change.
- No collector, scheduler, probe, aggregation, or telemetry-math change.
- No automatic deployment or restart on Polaris.
- No weakening of the web database's SELECT-only privilege checks.
- No automatic hostname rewriting in dashboard requests.
- No requirement that configured nodes and database inventory be identical.

## Configuration contract

### Canonical path and discovery

The canonical per-user file is:

```text
~/.node_monitor.yml
```

All commands that consume configuration accept an optional `--config PATH`. Resolution order is:

1. explicit `--config PATH`;
2. `~/.node_monitor.yml`;
3. legacy compatibility fallback `~/.node_monitor.yaml`;
4. `~/.config/node_monitor/config.yaml`;
5. `/etc/node_monitor/config.yaml`;
6. `node_monitor.yml` in the current directory;
7. legacy `node_monitor.yaml` in the current directory.

An explicit missing path fails clearly. Discovery never silently substitutes another file after an explicit path was supplied. Documentation and generated examples use `.yml`; `.yaml` remains compatibility-only.

The file contains credentials and SHOULD have mode `0600`. Commands reject a discovered or explicit config that is not a regular file. On POSIX, a group/world-readable file that contains a literal database URL fails closed with a fixed, non-secret error. Environment-only credential deployments may use a non-secret example file without that requirement.

### Unified shape

The existing nested daemon configuration remains the base shape. A `web` section is added rather than introducing a second file:

```yaml
system: polaris

nodes:
  - hostname: polaris-login-04.hsn.cm.polaris.alcf.anl.gov
    display_name: login-04
    role: local
  - hostname: polaris-login-01.hsn.cm.polaris.alcf.anl.gov
    display_name: login-01
    role: remote
    ssh_target: polaris-login-01.head

probe_python: /usr/bin/python3.13

output: {}
collection: {}
ssh: {}
safety: {}
retention: {}

database:
  # Daemon/operator writer credential.
  url: postgresql+psycopg2://node_monitor_writer:REDACTED@localhost/node_monitor_dev
  schema: node_monitor
  pool_size: 1
  max_overflow: 0

web:
  database:
    # Separate SELECT-only web credential.
    url: postgresql+psycopg2://node_monitor_reader:REDACTED@localhost/node_monitor_dev
    schema: node_monitor
    pool_size: 1
    max_overflow: 0
```

`database` is the existing daemon/operator database configuration. `web.database` is a distinct `DatabaseConfig` used only by `node-monitor web`.

Credential resolution remains independent:

- writer: explicit `database.url`, otherwise `NODE_MONITOR_DB_URL`;
- reader: explicit `web.database.url`, otherwise `NODE_MONITOR_WEB_DB_URL`;
- reader never falls back to writer URL or `NODE_MONITOR_DB_URL`;
- writer never reads `NODE_MONITOR_WEB_DB_URL`.

This preserves database-enforced read-only operation despite the single file. The web OS process can read the unified file and therefore both literal credentials if both are embedded; that process-level secret-isolation trade-off is explicit and accepted. Database grants remain the enforcement boundary for web writes.

### Consumer-specific validation

One file does not mean every command requires every section:

- daemon and database commands validate the existing nested daemon fields and use `database`;
- web validates `system` and `web.database`, ignores daemon-only sections after the top-level unified schema has been validated, and never constructs the writer database;
- unknown keys still fail closed;
- the deprecated flat Phase-0 layout remains supported only where currently supported and does not gain a web mode.

`load_web_config` becomes a projection from the unified config rather than a disjoint web-only file parser. It returns only `system` and the read-only database configuration to web startup code.

## CLI contract

Normal operation becomes:

```bash
node-monitor daemon start
node-monitor daemon status
node-monitor daemon stop
node-monitor database status
node-monitor web --port 9998
```

Every relevant command retains optional `--config PATH` for overrides.

The web command is:

```text
node-monitor web [--config PATH] [--host HOST] [--port PORT]
```

Defaults:

- host: `127.0.0.1`;
- port: `8080`;
- config: discovered via the canonical search order.

`--host 0.0.0.0` remains available for deliberate non-loopback exposure, but is never implied by `--port`.

Browser launch is removed entirely:

- remove `--no-browser`;
- remove imports/calls to `webbrowser`;
- startup prints the PID and listening URL plus an SSH-tunnel hint, following PBS Monitor's operator-facing command behavior;
- startup never opens a local or remote browser.

## Node inventory API

Add a bounded endpoint:

```http
GET /api/nodes
```

Successful response:

```json
{
  "system": "polaris",
  "nodes": [
    {
      "id": "polaris-login-04.hsn.cm.polaris.alcf.anl.gov",
      "label": "login-04",
      "configured": true,
      "role": "local"
    }
  ]
}
```

### Source of truth and labels

- Availability is database-backed: select exact `source_hostname` values from `node_hardware` for the configured system in a read-only transaction.
- `id` is always the exact stored hostname and is the value sent to `/api/dashboard`.
- Labels come from matching config `nodes[].display_name` when provided.
- Without an explicit label, derive a conservative display label from the exact hostname: `polaris-login-04.hsn.cm.polaris.alcf.anl.gov` becomes `login-04`; ambiguous or unmatched names remain the exact hostname.
- Include configured metadata only when an exact configured hostname matches the database ID. Configuration cannot invent an available database node.
- Sort configured nodes in config order, then any database-only nodes lexically.
- Cap the response at 128 nodes and 64 KiB. Exceeding either bound fails closed with a fixed service error.

The query is a fixed SELECT against `node_hardware`; it does not expose credentials, free-form SQL, or write capability.

## Browser bootstrap and controls

The free-text node input and Switch form are replaced by a node-button group.

Bootstrap sequence:

1. render loading state without requesting a guessed dashboard node;
2. fetch `/api/nodes`;
3. if nodes exist, choose in order:
   - current selection if still available;
   - configured local node if available;
   - first returned node;
4. render clickable buttons using friendly labels and exact IDs in internal state;
5. request `/api/dashboard` only after a valid selection exists.

Clicking a button:

- updates `state.currentNode` to the exact ID;
- sets `aria-pressed` and visible selected styling exclusively;
- requests the current range and username for that node;
- preserves existing chart/state behavior.

The node group is keyboard accessible, wraps at narrow widths, and exposes an accessible label. Button text never substitutes for the exact request ID.

## Error-state semantics

The UI must stop conflating distinct failures:

- **Loading nodes**: initial inventory request is in flight; no dashboard request yet.
- **No monitored nodes available**: `/api/nodes` returned 200 with an empty list. This is not a connection failure.
- **Connection failure**: `/api/nodes` or the initial dashboard request could not be completed because of network failure, timeout, HTTP 5xx, or invalid response.
- **Node unavailable**: a previously selected exact node returns HTTP 422 or disappears from refreshed inventory. Refresh inventory and select a valid node; do not retain a guessed identifier.
- **Connected/current, partial, stale, empty**: existing telemetry state semantics remain unchanged.
- **Later disconnect**: retain the last complete dashboard values/charts and independently mark web connectivity disconnected, as today.

The frontend may log bounded diagnostic status to the browser console for development, but user-visible text remains fixed and does not expose server exception details.

## Security and failure handling

- Web startup constructs only `WebDatabase(config.web.database)` and retains the existing SELECT-only privilege/schema preflight.
- The unified loader must prove the writer URL is never passed into web construction.
- Config-related errors remain sanitized and never print URLs or passwords.
- `/api/nodes` uses the same one-connection read-only database boundary and deadlines as dashboard reads.
- Static-resource route protections and local-only asset policy remain unchanged.
- No automatic browser invocation or OS shell command is introduced.
- TCP defaults to loopback. Public binding remains an explicit operator choice.

## Compatibility and migration

- Existing explicit web-only files are no longer the primary contract. To avoid a silent credential-boundary mistake, they fail with a clear migration message rather than being interpreted as a unified daemon config.
- Existing nested daemon configs can migrate by adding `web.database`.
- Existing commands with explicit `--config` continue to work with the unified file.
- Default discovery applies consistently to daemon start/run/smoke/dry-run, daemon status/stop where configuration is required by their existing contract, database status/migrate, config check, and web.
- Existing scripts may keep passing `--config`; this change removes the requirement, not the option.

## Documentation

Update:

- `README.md` quick start;
- `docs/web-dashboard.md` configuration, command, node selection, and troubleshooting;
- `config.example.phase1.yaml` as the unified example;
- CLI help text.

Document the ordinary path as:

```bash
cp config.example.phase1.yaml ~/.node_monitor.yml
chmod 600 ~/.node_monitor.yml
node-monitor web --port 9998
```

For SSH-tunnel use with default loopback binding:

```bash
ssh -L 9998:127.0.0.1:9998 polaris-login-04
```

Then open `http://localhost:9998` manually.

## Test strategy

Follow strict RED-GREEN TDD.

### Configuration tests

- canonical `.yml` discovery and legacy fallbacks in exact order;
- optional `--config` across relevant commands;
- unified config accepts separate writer/reader credentials;
- reader URL never falls back to writer URL;
- web projection rejects missing `web.database` and unknown keys;
- config errors redact both credentials;
- explicit credential-bearing config permissions are owner-only on POSIX.

### CLI tests

- `node-monitor web --port 9998` loads discovered config;
- default host is `127.0.0.1`;
- explicit host/port reach runtime unchanged;
- CLI exposes no `--no-browser` option;
- monkeypatched `webbrowser.open` is never called because browser-launch code is absent;
- startup output provides URL/tunnel instructions without printing credentials.

### API/database tests

- `/api/nodes` returns exact IDs and friendly labels;
- inventory is database-backed and system-scoped;
- config order and database-only fallback order are deterministic;
- empty inventory returns 200 with `nodes: []`;
- row/serialized-size bounds fail closed;
- reader transaction is read-only and uses only reader credentials;
- dashboard exact-host validation remains unchanged.

### Browser tests

- no dashboard request occurs before node inventory succeeds;
- first valid node is selected automatically;
- configured local node is preferred;
- button clicks send exact FQDN, range, and username;
- no shorthand hostname is guessed;
- node buttons are visible, keyboard accessible, wrapped at 400 px, and have exclusive `aria-pressed` state;
- empty inventory, inventory failure, invalidated selection, initial dashboard failure, and later disconnect render distinct expected text;
- all existing telemetry, chart, operational-state, screenshot, focus, reduced-motion, and no-external-request tests remain green.

### Verification

Run:

```bash
venv/bin/python -m pytest tests/web tests/browser -q
venv/bin/python -m pytest -q
```

Before integration, perform an independent exact-head review and a live bounded Polaris smoke using a reviewed immutable release. The smoke must verify:

- `node-monitor web --port <test-port>` with discovered `~/.node_monitor.yml`;
- no browser process is launched;
- `/health` returns 200;
- `/api/nodes` returns the exact database inventory;
- the initial browser dashboard request uses an inventory ID and returns 200;
- node-button switching works when multiple database nodes exist;
- no production daemon or PostgreSQL process is stopped or restarted by the smoke.

## Acceptance criteria

The work is complete when:

1. one `~/.node_monitor.yml` config supports daemon and web with separate writer/reader credentials;
2. `node-monitor web --port 9998` is sufficient under normal setup;
3. browser-launch behavior and `--no-browser` are absent;
4. users choose available nodes through buttons and never need to enter FQDNs;
5. the initial page cannot issue a guessed `login-04` dashboard request;
6. healthy server/database state does not render `Connection failure` because of an invalid default node;
7. all security, telemetry semantics, responsive behavior, and existing full-suite tests remain intact;
8. deployment remains a separate reviewed action.
