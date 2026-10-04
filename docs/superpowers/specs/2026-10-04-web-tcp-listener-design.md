# Web TCP Listener Design

## Goal

Replace node-monitor's private Unix-domain-socket-only web listener with the TCP listener model used by PBS Monitor. Operators can choose a bind address and port, including an externally reachable listener when facility networking permits it.

## Command contract

```text
node-monitor web --config FILE [--host HOST] [--port PORT] [--no-browser]
```

Defaults match PBS Monitor:

- `--host 127.0.0.1`
- `--port 8080`
- browser launch enabled unless `--no-browser` is supplied

Examples:

```bash
node-monitor web --config web.config.dev.yaml --port 9998
node-monitor web --config web.config.dev.yaml --host 0.0.0.0 --port 9998 --no-browser
```

`--host` must be a non-empty value without NUL bytes. `--port` must be an integer in `1..65535`; boolean values are not integers for this contract. Click rejects malformed CLI values before database construction.

## Configuration

The web-only YAML keeps `system` and `web.database`. `web.socket_path` is removed and rejected as an unknown key. Host and port are invocation-time operational choices, as in PBS Monitor, rather than persisted web configuration.

## Runtime

Uvicorn receives `host` and `port` directly. There is no Unix-socket creation, stale-socket cleanup, or socket filesystem state. The service remains foreground-only; operators use `screen`, `tmux`, or another supervisor for persistence.

On successful startup, node-monitor prints exactly one bounded line:

```text
PID <pid> http://<display-host>:<port>
```

The display host preserves the selected host. Runtime and preflight errors remain fixed, bounded, credential-free messages without tracebacks. Database disposal remains guaranteed on all ordinary failure and shutdown paths.

The `--no-browser` flag suppresses browser launch. Without it, node-monitor follows PBS Monitor and attempts to open the selected URL. Browser-launch failure is non-fatal and does not prevent serving. For wildcard binds (`0.0.0.0` or `::`), the local browser URL uses loopback (`127.0.0.1` or `[::1]`) while the startup line still reports the actual bind host.

## Security boundary

TCP mode is intentionally unauthenticated and read-only. `127.0.0.1` remains the safe default. Selecting `0.0.0.0` or another non-loopback address explicitly exposes the dashboard to every source that facility routing and firewalls allow to reach that port. The existing SELECT-only PostgreSQL role, privilege preflight, bounded responses, and read-only transactions remain unchanged.

## Tests and acceptance

Test-first implementation must cover:

1. Default host, port, and browser behavior.
2. Custom host and port passed unchanged to Uvicorn.
3. `--no-browser` suppression.
4. Wildcard bind browser URL normalization.
5. Invalid/zero/out-of-range ports rejected before database construction.
6. Empty/NUL host rejection.
7. Occupied-port and runtime failures produce bounded output and dispose the database.
8. Successful shutdown disposes the database.
9. Web config accepts database-only shape and rejects legacy `socket_path`.
10. Runtime config uses `host`/`port`, never `fd` or `uds`.
11. Real ephemeral-port integration verifies `/health` and `/api/dashboard` over TCP.
12. Focused web, browser, and full repository suites pass.

Deployment acceptance on Polaris requires the exact reviewed SHA, an updated web config without `socket_path`, startup on an operator-selected port, HTTP 200 from `/health`, and a parsed HTTP 200 dashboard response for an exact stored node hostname. No production web process is started without operator approval.