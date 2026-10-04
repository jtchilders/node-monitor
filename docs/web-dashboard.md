# node-monitor web dashboard

This document is the public operator runbook for the read-only
node-monitor web dashboard process (`node-monitor web`). It describes the
process lifecycle, configuration shape, health/monitoring, and the
PostgreSQL boundary. It intentionally contains no credentials, no internal
host names, and no private database details -- the YAML example below is
disjoint from the collector configuration and is safe to publish.

## Scope and boundary

The web process is entirely separate from the collector/daemon:

- It opens exactly one read-only PostgreSQL connection (`pool_size: 1`,
  `max_overflow: 0`) and never writes to the database.
- It never runs schema migrations and never issues DDL.
- It never starts, stops, restarts, or reconfigures the shared PostgreSQL
  server. Creating a dedicated PostgreSQL reader role and granting it
  `SELECT`-only access to the `node_monitor` schema is an **operator**
  responsibility, performed with the operator's own PostgreSQL tooling
  (`psql`, `CREATE ROLE ... LOGIN`, `GRANT USAGE ON SCHEMA node_monitor`,
  `GRANT SELECT ON ALL TABLES IN SCHEMA node_monitor`) -- node-monitor
  itself never creates roles, grants privileges, or manages PostgreSQL
  lifecycle in any way.
- It never manages the collector/daemon process. The collector and the web
  process may run on different hosts, at different times, and independent
  of one another; the web dashboard simply reads whatever has already been
  written.

## Configuration (web-only YAML)

The web process loads a dedicated YAML file that is completely disjoint
from the collector's configuration -- it accepts exactly `system` and
`web` at the top level, and every collector-only key (`nodes`,
`probe_python`, `output`, `collection`, `ssh`, `safety`, `retention`) is
rejected on sight.

```yaml
# web-config.yaml -- contains NO credential. The database URL below is
# illustrative; see "Supplying the database URL" for how the real
# connection string is injected without ever being committed to this file.
system: polaris

web:
  database:
    schema: node_monitor
    pool_size: 1
    max_overflow: 0
```

Notes on this shape:

- Listener address and port are CLI settings, not YAML fields. The defaults
  are `--host 127.0.0.1 --port 8080`; use those options to override either
  value. There is no Unix-socket mode; `socket_path` has been removed and any
  reference to it is rejected.
- `--host 0.0.0.0` exposes the unauthenticated read-only dashboard to every
  network source able to reach the selected port. Use it only when that
  exposure is intentional; otherwise retain loopback and use SSH forwarding.
- `web.database.schema` must be exactly `node_monitor`.
- `pool_size` must be exactly `1`; `max_overflow` must be exactly `0`.
- No password, connection string, internal hostname, or other credential
  belongs in a committed copy of this file.

### Supplying the database URL

The connection URL is resolved in this strict order, and an explicit value
always wins over an injected one:

1. `web.database.url` in the YAML file itself (only appropriate for an
   untracked, permission-restricted local copy of the config -- never
   commit a URL with a real password).
2. The `NODE_MONITOR_WEB_DB_URL` environment variable.
3. Otherwise the process refuses to start with a bounded configuration
   error.

`NODE_MONITOR_DB_URL` (the collector/writer's own credential) is **never**
read by the web process under any circumstance -- the two processes use
entirely separate environment variables so a writer credential can never
leak into the read-only web process by accident.

## Foreground run command

```console
node-monitor web --config /path/to/web-config.yaml [--host HOST] [--port PORT] [--no-browser]
```

Defaults: `--host 127.0.0.1` `--port 8080`. `--no-browser` suppresses the
best-effort browser open. The process always runs in the foreground,
bound to TCP; there is no `--daemonize` mode (the collector's separate
`daemon start --foreground` controls a different lifecycle).

On successful startup the process prints exactly one bounded line to
stdout before serving:

```
PID <pid> http://<host>:<port>
```

No other startup banner, and no credential, URL, or SQL text, is ever
printed. On any failure (bad config, database preflight failure, TCP
bind failure, runtime crash) the process prints one bounded,
credential-free line to stderr and exits nonzero -- it never prints a raw
traceback or interpolates an underlying exception's message.

## TCP-only mode (no Unix sockets)

The web process serves HTTP exclusively over TCP -- there is no `AF_UNIX`
mode. The legacy `socket_path` parameter has been removed; configuring it
is rejected at startup. A duplicate-start against the same `host:port`
while the first is still running fails closed: the TCP bind detects the
occupied port and the second process exits nonzero with a bounded message.

Because the listener is TCP, remote access can go either directly
(`curl http://127.0.0.1:8080/health`) when the bind is loopback, or
through an optional SSH port-forward (`ssh -L 8080:localhost:8080 <host>`)
when the service is bound to loopback only. A `0.0.0.0` bind exposes the
dashboard directly; the operator must confirm that is intentional.

For intentional shared access, the PBS Monitor-style invocation is:

```console
node-monitor web --config /path/to/web-config.yaml --host 0.0.0.0 --port 9998 --no-browser
```

This service has no HTTP authentication. Its database role is constrained to
read-only access, but all clients that can reach the listener can view the
dashboard data.

## Running detached with `screen`

The web process has no built-in daemonization, so an operator who wants it
to survive a disconnected terminal session runs it inside `screen` (or
`tmux`):

```console
screen -S node-monitor-web
node-monitor web --config /path/to/web-config.yaml
# Detach with Ctrl-A, D -- process keeps running inside screen.
```

Reattach: `screen -r node-monitor-web`. Stop: reattach and press Ctrl-C,
or send SIGTERM directly (`kill <pid>`); there is no separate `stop`
subcommand.

## No automatic restart

The web process does not restart itself after a crash, and node-monitor
does not ship a supervisor, systemd unit, or watchdog for it. If the
process exits (crash, OOM, transport-level panic inside Uvicorn, or an
operator-initiated stop), it stays down until an operator explicitly
starts it again -- inside the same `screen` session, a fresh one, or
under a process supervisor the operator chooses separately.

## Health check

The process exposes `GET /health`, returning `{"status": "ok"}` with
HTTP 200 whenever it is accepting TCP connections -- it does **not**
re-run the database preflight or touch PostgreSQL. A 200 from `/health`
means the web process is alive; it does not by itself prove the database
is reachable or that dashboard data is fresh (see the note at the end).

Because the API is TCP-only, `/health` is checked directly over TCP:

```console
curl http://localhost:8080/health
```

(When bound to loopback only, reach it through SSH port-forwarding:
`ssh -L 8080:localhost:8080 <host>` then `curl http://localhost:8080/health`.)

A dashboard that is "connected but stale" (healthy and reachable, but
most recent data older than the freshness threshold) is a separate,
honestly-reported condition surfaced by `/api/dashboard`'s
`is_fresh`/`status` fields, never by `/health`.

## SSH tunnel (optional, loopback-bound services)

When the service is bound to `127.0.0.1` (the safe default) and must be
reached from another host, use SSH port forwarding rather than exposing
`0.0.0.0`:

```console
ssh -L 8080:localhost:8080 <host>
# then open http://localhost:8080/ in a browser locally
```

This is optional: a `0.0.0.0` bind requires no tunnel, but exposes the
listener to all interfaces reachable from the host.

## Log location

The web process does not write its own log file. All bounded
startup/shutdown/failure output goes to stdout/stderr, exactly as
inherited from whatever launched it (`screen` scrollback, supervisor
capture, or an operator redirect such as
`node-monitor web --config ... >> ~/web.log 2>&1`). There is no separate
structured log file the process manages on its own.

## Duplicate-start behavior

Starting a second `node-monitor web` against the same `host:port` while
the first is still running fails closed: the TCP bind detects the
occupied port (`EADDRINUSE`) and the second process exits nonzero before
it serves. The first process's listener is never disturbed by the
failed second attempt.

## PostgreSQL reader role: operator responsibility

node-monitor never creates, modifies, or grants privileges to any
PostgreSQL role, and never creates or drops any database. Before running
`node-monitor web`, an operator with appropriate PostgreSQL
administrative access must, using their own tooling, create a dedicated
read-only role and grant exactly:

- `CONNECT` on the target database.
- `USAGE` on the `node_monitor` schema.
- `SELECT` on every table in the `node_monitor` schema.

And must ensure the role does **not** hold any of: `INSERT`, `UPDATE`,
`DELETE`, `TRUNCATE`, `REFERENCES`, or `TRIGGER` on any table; database-level
`TEMP` or `CREATE`; or schema-level `CREATE`. The web process's own
preflight verifies every grant and forbidden-privilege absence before
serving a request, and refuses to start if any check fails -- but the role
must already exist with the correct grants before the preflight can
succeed.

## node-monitor never manages the shared PostgreSQL server

To restate plainly: node-monitor -- neither the collector/daemon nor the
web process -- ever starts, stops, restarts, reconfigures, backs up, or
manages the PostgreSQL server lifecycle. The server may be shared (e.g.
with a `pbs-monitor` installation); node-monitor reads and writes only
its `node_monitor` schema and never touches any other schema, database,
or role. Backups, server upgrades, `pg_ctl` actions, and PostgreSQL
configuration changes are all operator responsibilities.

## Migration note: `socket_path` removed/rejected

Earlier releases used `web.socket_path` (Unix domain socket). That
parameter has been removed; any config that still references it is
rejected at startup with a bounded error message. Operators migrating
from a Unix-socket deployment should switch to `--host 127.0.0.1` (or an
explicit bind) and `--port 8080`, update health checks to direct TCP
(`curl http://localhost:8080/health`), and replace Unix-socket SSH
tunnels with TCP port-forwarding or direct TCP reach when `0.0.0.0` is used.

# Chart.js asset documentation

- Pinned upstream version: 4.4.1
- Source URL (official minified UMD): https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js
- Download/verification method: verified generically: download with pinned URL, compare SHA-256 and size, confirm real JS content (must contain Chart.js code, not placeholder)
- Verified SHA-256: d2af8974e95271638772e9e9524db5b9a6f58d6ec2d5d781400447b4a31c681e
- Verified size: 205399 bytes
- Installed at: node_monitor/web/static/chart.umd.min.js
- Content gate: the build/test pipeline verifies exact size (205399), exact SHA-256, and non-placeholder JS content (must contain real Chart.js code, not placeholder string) before accepting the package.
- Status: VERIFIED and installed.
