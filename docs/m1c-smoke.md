# M1c smoke on polaris-login-04

Date: 2026-11-01, 21:30–22:07 UTC. The collector daemon ran from the `feat/m1c-daemon` checkout
(`0.1.0+v0.1.0-alpha.3-22-ga45c75e`), started by hand with `node-monitor daemon start`. ALCF
login nodes give users no systemd (see `deploy/README.md`). Counts only: no usernames or command
lines are recorded here.

## Before starting

| Step | Result |
|---|---|
| Sessions on `node_monitor` before migrating | 0 |
| Stray probes on login-01/02/04 | none |
| `node-monitor database migrate` | `applied: 0002`, schema v2 |
| `collection_events` | 972 → 748: exactly the 224 duplicates from the M1b re-replay removed |
| `user_usage_5m` | all 2,701 rows kept; `by_category`/`by_dormancy` dropped |
| `collection_events_natural_key` | present |
| Working tree | `chmod -R g-w,o-w ~/node-monitor`; `state/` and `logs/` 0700. Before this they were group-writable (see the M1c final review, I2) |

## 30-minute run

The daemon started at 21:30:10 UTC. Samples were taken every 5 minutes for 31 minutes.

| Measure | Result |
|---|---|
| Heartbeat | always fresh (≤ 50 s old), `connected`, spool 0, queue 0 |
| Sessions | login-01/02/04 `up` throughout; 3 `session_started`, no `session_lost` |
| `node_metrics_1m` | 3 rows per minute (93 in 31 minutes), every minute `sample_count = expected_count = 6` |
| `user_usage_5m` | 684–687 rows per 5-minute interval, all 3 nodes, every row `sample_count = expected_count` |
| Live tables | about 4,950 processes over 3 nodes; latest snapshot always < 60 s old |
| Daemon | RSS 44–46 MiB (flat); CPU 78 s in 31 min (≈ 4 % of one core) |
| Probe RSS on the nodes | 19.7–20.6 MiB, flat |
| `live_processes` autovacuum | 34 runs in 31 min; dead tuples cycle back to 0 |
| Events | `collector_started` 1, `session_started` 3, `probe_error` 15 |
| `daemon.log` | 0 bytes: a healthy daemon logs nothing |

All 15 `probe_error` events were `nvidia-smi` exiting 255 ("Failed to initialize NVML") on all
three login nodes. After 3 identical failures the probe suspends GPU queries for 600 s, so this
is about one event per node every 10 minutes. It is expected on login nodes.

## CLI on real data

| Command | Result |
|---|---|
| `status` | collector block plus a table of 3 nodes `up`; exit 0 |
| `nodes` | load, memory used/total/% , processes, user processes (≈1,480–1,510 per node), users (222–238 per node) |
| `users --limit 0` | 362 users system-wide |
| `top --limit 5`, `users --limit 5` | 5 rows each; JSON keys as specified |
| `idle` (default `--min-idle 1h`) | 0 rows, as expected: idle time is counted from when the probe first saw a process, and the probes were about 32 minutes old |
| `idle --min-idle 10m` | 2,602 owner-absent idle processes (2,343 of them lower bounds); 3,224 with `--any-presence` |

## Crash recovery (`kill -9`)

1. The daemon was SIGKILLed at 22:02:42.
2. `daemon status` then reported "not running (stale daemon.json … removed)".
3. No local ssh processes were left behind, and the remote probes exited once their stdin closed.
4. The daemon was restarted by hand at 22:03:05: one probe per node, then `collector_started` and 3 `session_started`.
5. The interrupted interval was written with reduced coverage: `user_usage_5m` and `node_usage_5m` at 22:00 have `sample_count 2 / expected 5` on every node. Minute 22:02 is a visible gap in `node_metrics_1m`. All minutes from 22:03 on are complete (6/6).

## Clean stop (`daemon stop`)

`daemon stop` took 1.6 s. It wrote `collector_stopped`, and a final heartbeat with
`state = stopped` and every session `stopped`. Both `daemon status` and `status` then exit 1.
No ssh processes remained.

## Follow-ups found

- **`daemon status` after a crash.** It exits 0 for up to 180 s after a crash: the heartbeat is still fresh, even though the same command reports the local process as not running. On the collector host, a free instance lock should override heartbeat freshness.
- **No startup log line.** `daemon.log` stays empty on a healthy start. A one-line "started (version, nodes)" and "stopped" would make the log useful after maintenance restarts.
- **`nodes` "Procs" column.** It shows `procs_total` from `/proc/loadavg` (about 11–13k), which counts scheduling entities (threads), not processes. Rename the column or document it.
- **Runbook gaps.**
  - `deploy/README.md` doesn't say that `daemon stop` exits 1 at its timeout and leaves the daemon running.
  - `chmod 700 ~/node-monitor/{state,logs}` fails on a fresh install before `state/` exists.
