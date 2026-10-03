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
  socket_path: ~/.node-monitor/run/web.sock
  database:
    schema: node_monitor
    pool_size: 1
    max_overflow: 0
```

Notes on this shape:

- `web.socket_path` must resolve (after `~` expansion against the
  operator's own home directory) to a path inside
  `~/.node-monitor/run/`. Any other location is rejected before the
  process attempts to bind.
- `web.database.schema` must be exactly `node_monitor`.
- `web.database.pool_size` must be exactly `1`; `max_overflow` must be
  exactly `0`. The web process is a single bounded reader, never a pool of
  connections.
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
node-monitor web --config /path/to/web-config.yaml
```

This is the only supported invocation shape: the web process always runs
in the foreground, bound to the terminal or process supervisor that
started it. There is no `--daemonize` or background-fork mode for the web
process (unlike the collector's `daemon start --foreground` flag, which
controls a separate detached-daemon lifecycle entirely).

On successful startup the process prints exactly one bounded line to
stdout before serving:

```
PID <pid> socket <absolute socket path>
```

No other startup banner, and no credential, URL, or SQL text, is ever
printed. On any failure (bad config, database preflight failure, socket
bind failure, runtime crash) the process prints one bounded,
credential-free line to stderr and exits nonzero -- it never prints a raw
traceback or interpolates an underlying exception's message.

## Run and socket modes

The web process serves HTTP exclusively over a Unix domain socket --
it never binds a TCP port. Exactly one mode is supported:

- **Unix socket, foreground, single process.** `web.socket_path` names an
  `AF_UNIX` `SOCK_STREAM` socket under `~/.node-monitor/run/`. The parent
  run directory is created (or verified) mode `0700`, owned by the
  invoking user; the socket itself is created mode `0600` and verified
  before `listen()` is ever called. A pre-existing socket at the same path
  is probed: if it is still live (a real connect succeeds) the new process
  refuses to start ("socket is already in use"); if it is conclusively
  stale (the probe connect fails with `ECONNREFUSED` or `ENOENT`) the old
  inode is removed and the new process binds cleanly. Any other probe
  outcome (including `EACCES`) is treated conservatively as "still live"
  and the new process refuses to start rather than risk clobbering a
  working socket.

There is no TCP listening mode and no option to expose the dashboard
directly to a network interface -- every remote access path goes through
an SSH tunnel (see below).

## Running detached with `screen`

The web process has no built-in daemonization, so an operator who wants it
to survive a disconnected terminal session runs it inside `screen` (or an
equivalent terminal multiplexer such as `tmux`):

```console
screen -S node-monitor-web
node-monitor web --config /path/to/web-config.yaml
# Detach with Ctrl-A, D -- the process keeps running inside the screen session.
```

To reattach later and check on it:

```console
screen -r node-monitor-web
```

To stop it, reattach and press Ctrl-C (or send SIGTERM/SIGINT to the
process directly with `kill`) -- the process has no separate `stop`
subcommand of its own.

## No automatic restart

The web process does not restart itself after a crash, and node-monitor
does not ship a supervisor, systemd unit, or watchdog for it. If the
process exits (crash, OOM, a transport-level panic inside Uvicorn, or an
operator-initiated stop), it stays down until an operator explicitly
starts it again -- inside the same `screen` session, a fresh one, or
under a process supervisor the operator chooses to configure separately.
This is a known operational limitation, not an oversight: automatic
restart is explicitly out of scope for this increment.

## Health check

The process exposes `GET /health`, which returns `{"status": "ok"}` with
HTTP 200 whenever the process is accepting connections -- it does **not**
re-run the database preflight or touch PostgreSQL at all. A 200 from
`/health` means the web process itself is alive; it does not by itself
prove the database is reachable or that dashboard data is fresh (see the
note at the end of this section).

Because the API is served only over the Unix socket, `/health` must be
checked either directly on the host (over the socket) or through an SSH
tunnel (see the next section) -- never over a bare TCP connection, because
none exists.

### Checking `/health` over the socket directly on the host

```console
curl --unix-socket ~/.node-monitor/run/web.sock http://localhost/health
```

### Checking `/health` over an SSH tunnel

```console
ssh -L 8080:/home/<operator>/.node-monitor/run/web.sock <host>
curl http://localhost:8080/health
```

(Modern OpenSSH supports tunneling a local TCP port straight to a remote
Unix socket with `-L local_port:/path/to/socket`; older OpenSSH may
require `socat` or `nc -U` as a small relay on the remote side instead.)

Note that `/health` returning 200 only proves the web process is up and
accepting connections -- it does not query PostgreSQL. A dashboard that is
"connected but stale" (the web process is healthy and reachable, but the
most recent stored counter/usage data is older than the freshness
threshold) is a distinct, honestly-reported condition surfaced by
`/api/dashboard`'s own `is_fresh`/`status` fields, never by `/health`.

## SSH tunnel to the dashboard

The dashboard UI and `/api/dashboard` JSON endpoint are reached the same
way as `/health`: tunnel a local TCP port to the exact path named by
`web.socket_path` in the running process's configuration, then browse to
the tunneled local port.

```console
ssh -L 8080:/home/<operator>/.node-monitor/run/web.sock <host>
# then open http://localhost:8080/ in a browser on the local machine
```

The tunnel target is always the live process's own `web.socket_path` --
never a hard-coded path -- so an operator who changes `socket_path` in
their config must update the tunnel command to match.

## Log location

The web process does not write its own log file. All of its bounded
startup/shutdown/failure output goes to stdout/stderr, exactly as
inherited from whatever launched it (a `screen` session's scrollback, a
process supervisor's captured output, or a redirected file the operator
sets up themselves, e.g. `node-monitor web --config ... >>
~/web.log 2>&1`). There is no separate structured log file the process
manages on its own, unlike the collector/daemon, which does maintain
`~/.node_monitor_daemon.log`.

## Duplicate-start behavior

Starting a second `node-monitor web` process against the same
`web.socket_path` while the first is still running fails closed: the
socket-bind probe detects the first process is still live (a real connect
to the existing socket succeeds) and the second process exits nonzero
with a bounded "socket is already in use" message before it ever calls
`listen()`. The first process's socket inode is never disturbed by the
failed second attempt.

If the first process has genuinely exited without cleaning up its own
socket file (e.g. it was killed with `SIGKILL`), the stale socket is
detected the same way: the connect probe fails conclusively
(`ECONNREFUSED`/`ENOENT`), the old inode is unlinked, and the new process
binds cleanly in its place.

## Manual cross-UID socket-denial check

The socket is created mode `0600` and owned by the user that started the
process; the kernel itself enforces that no other UID can connect to it.
To manually verify this on a multi-user host:

```console
# As the operator who started the web process:
node-monitor web --config /path/to/web-config.yaml &

# As a DIFFERENT user on the same host:
curl --unix-socket /home/<operator>/.node-monitor/run/web.sock http://localhost/health
# Expected: a permission-denied connection failure (curl reports
# "Couldn't connect to server" / a kernel-level EACCES), never a 200.
```

If the second command ever succeeds for a foreign UID, that is a serious
regression in the socket security contract and must be treated as a
blocking defect, not a configuration quirk.

## PostgreSQL reader role: operator responsibility

node-monitor never creates, modifies, or grants privileges to any
PostgreSQL role, and never creates or drops any database. Before running
`node-monitor web`, an operator with appropriate PostgreSQL administrative
access must, using their own tooling, create a dedicated read-only role
and grant it exactly the following, against the already-migrated
`node_monitor` schema:

- `CONNECT` on the target database.
- `USAGE` on the `node_monitor` schema.
- `SELECT` on every table in the `node_monitor` schema.

And must ensure the role does **not** hold any of: `INSERT`, `UPDATE`,
`DELETE`, `TRUNCATE`, `REFERENCES`, or `TRIGGER` on any table in the
schema; database-level `TEMP` or `CREATE`; or schema-level `CREATE`. The
web process's own startup preflight independently verifies every one of
these grants and forbidden-privilege absences before it ever serves a
request, and refuses to start if any check fails -- but the role itself
must already exist with the correct grants before that preflight can
succeed. Creating that role, and deciding which PostgreSQL server and
database it lives on, remains entirely the operator's decision and
action; node-monitor never performs it automatically.

## node-monitor never manages the shared PostgreSQL server

To restate plainly, because it is the single most important operational
boundary in this document: node-monitor -- neither the collector/daemon
nor the web process -- ever starts, stops, restarts, reconfigures, backs
up, or otherwise manages the lifecycle of the PostgreSQL server itself.
The server may be shared with other applications (for example, a
`pbs-monitor` installation on the same host); node-monitor reads and
writes only its own `node_monitor` schema and never touches any other
schema, database, or role on that server. Database backups, server
upgrades, `pg_ctl`/service-manager actions, and PostgreSQL server
configuration changes are all operator responsibilities performed outside
of node-monitor entirely.

# Chart.js asset documentation

- Pinned upstream version: 4.4.1
- Source URL (official minified UMD): https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js
- Download/verification method: verified generically: download with pinned URL, compare SHA-256 and size, confirm real JS content (must contain Chart.js code, not placeholder)
- Verified SHA-256: d2af8974e95271638772e9e9524db5b9a6f58d6ec2d5d781400447b4a31c681e
- Verified size: 205399 bytes
- Installed at: node_monitor/web/static/chart.umd.min.js
- Content gate: the build/test pipeline verifies exact size (205399), exact SHA-256, and non-placeholder JS content (must contain real Chart.js code, not placeholder string) before accepting the package.
- Status: VERIFIED and installed.
