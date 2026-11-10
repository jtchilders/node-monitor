# Running the collector daemon

ALCF login nodes give users no systemd, so the collector is operated by hand: start it
once, and start it again after each login-node maintenance (as pbs_monitor is run).

## One-time setup

```bash
mkdir -m 700 -p ~/node-monitor/logs
chmod -R g-w,o-w ~/node-monitor
chmod 700 ~/node-monitor/{state,logs}
```

`state/` holds the ssh config the collector runs ssh with, and `logs/` and the spool hold
other users' command lines, so nobody else may write to the tree or read those two
directories. The daemon also tightens `state/` and `logs/` at every start.

The SSH key must be usable without a passphrase and without an agent: ssh runs in batch
mode (`BatchMode`) and cannot prompt. An encrypted key fails even when its path is set as
`ssh.identity_file`; use a dedicated unencrypted key (set its path as `ssh.identity_file`
if it is not a default key name).

## Start

```bash
node-monitor daemon start
```

It runs in the background, logs to `~/node-monitor/logs/daemon.log`, and refuses to start
if a daemon is already running.

## Check

```bash
node-monitor daemon status     # exit 0 if healthy, 1 if not
node-monitor status            # fleet view; non-zero exit when the heartbeat is stale
tail -f ~/node-monitor/logs/daemon.log
```

## Stop

```bash
node-monitor daemon stop [--timeout 60]
```

Sends SIGTERM and waits up to `--timeout` seconds (default 60) for the daemon to exit.

## After maintenance

```bash
node-monitor database wait --timeout 60    # PostgreSQL is up
node-monitor daemon start
```

A daemon started while PostgreSQL is still down is fine: it spools and catches up.

## Web

```bash
node-monitor web --port 9998 &
curl -s http://127.0.0.1:9998/api/health | python3 -m json.tool
# stop: kill %1  or  pkill -f "node-monitor web"
```

Loopback only, read-only role (`web.database_url`). Vendor pins `vue@3.4.38` + `chart.js@4.4.9` (no CDN).

## Pitfalls

- Do not run a soak and the daemon at the same time: they take over each other's probes on
  the nodes.
- There is no automatic restart after a crash. Check `node-monitor daemon status` after
  maintenance, or whenever the dashboard shows a stale heartbeat.
- `logs/daemon.log` grows without rotation; truncate it when needed.
