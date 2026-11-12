# node-monitor

Monitor for ALCF login nodes: a stdlib-only remote probe streamed over SSH, with a
collector that records process and node activity.

## Development setup

The repo is a subfolder of the `~/node-monitor` working directory, so the venv lives one
level up.

```bash
python3 -m venv ../venv && ../venv/bin/pip install -e '.[dev]' && ../venv/bin/pytest
```

## M0 soak

Record every configured node's probe stream, then summarize it:

```bash
node-monitor soak run --duration-hours 24    # omit the duration to run until Ctrl-C
node-monitor soak report ~/node-monitor/logs/soak/<timestamp> [--format json]
```

`soak run` writes `manifest.json` and one `<label>-<YYYYMMDD>.jsonl` per node per UTC day
into the out dir (default `<working_dir>/logs/soak/<UTC timestamp>`, or `--out-dir`).
Censuses are stored as summaries unless `--full-census` is given. The config comes from
`--config` or the usual search path (`node-monitor config --create` writes a commented template).

`soak report` prints one row per node: coverage of expected counter messages, largest gap,
sessions, losses, rotations, reboots, probe CPU per hour, peak probe RSS, census scan p95
and average process count. `rss_MB` is the exact peak (smaps_rollup); `maxrss_MB` comes
from `ru_maxrss`, which drifts tens of MiB on many-core kernels, so it is only a secondary
check. `--format json` adds the per-session RSS series and rotations by reason.

## Replaying a capture through the pipeline

```bash
node-monitor soak replay ~/node-monitor/logs/soak/<timestamp> [--config PATH] \
   [--write-records /tmp/records.jsonl] [--write-db]
```

`soak replay` feeds every recorded line through the per-node pipeline (classification,
redaction, minute and 5-minute aggregation) and prints the storage record counts per node.
`--write-records` writes each record as a `{kind, row}` JSON line; the rows hold redacted
but still sensitive command lines, so the file is owner-only. A capture recorded without
`--full-census` has no process rows, so it yields node metrics but no process or usage
records.

`--write-db` also writes the records to PostgreSQL through `database.url`, exactly as the
collector will. The schema must be migrated first (see [Database](#database)); missing
monthly partitions are created. Retention is measured back from the current time, not the
capture's, so rows older than `retention.*_days` are counted as `rows_beyond_retention`
and not written. If the database becomes unavailable partway, the remaining batches are
spooled to `<working_dir>/state/spool-replay` and the command exits 1; running it again
replays the spool first.

## Configuration keys added for M1

- `collection.summary_interval_sec` (default 300): length of the per-user and per-node
  usage intervals; a multiple of `census_interval_sec` that divides 3600.
- `collection.probe_rss_limit_mb` (default 128): above this resident size the probe says
  `bye` and exits, and the collector starts a fresh session.
- `classification.min_user_uid` (default 1000): processes owned by lower uids are
  classified as `system` and left out of usage summaries.
- `redaction.extra_patterns` (default `[]`): extra regexes applied after the built-in
  command-line redaction rules; the first capture group (or the whole match) is masked.

## CLI

Read commands query the live tables through `web.database_url`; every one takes
`--config PATH` and `--format table|json|csv` (JSON and CSV carry raw values and full
command lines).

| Command | Shows |
| --- | --- |
| `node-monitor status` | collector heartbeat and per-node session state (exit 1 if stale or missing) |
| `node-monitor nodes` | per-node load, memory, process counts, census age |
| `node-monitor top [-u USER]` | user processes, busiest first |
| `node-monitor users` | per-user totals |
| `node-monitor user USERNAME` | every process of one user, with a summary line |
| `node-monitor idle [--min-idle 1h] [--any-presence]` | processes idle that long whose owner is absent |
| `node-monitor daemon start\|stop\|status` | run and control the collector daemon |

`top`, `users`, `user` and `idle` also take `-n/--node`, `-c/--category`, `--sort`,
`--reverse` and `--limit` (default 20, 0 for all). `--min-idle` accepts `30m`, `2h`, `1d`,
`90s` or plain seconds.

```bash
node-monitor top --sort rss --limit 10
node-monitor idle --min-idle 2h --format csv
```

## History and analytics (M3)

Collector stores notable processes (cpu≥0.1s/rss≥1 GiB/ai_agent/tagged) each minute and every user process at minute 0 into `process_samples` (daily partitions, 30d), and distinct redacted cmdlines into `command_lines` (730d). Tags (e.g. `agent.skip_permissions`) are table-driven in `pipeline/tags.py`.

| Command | Shows |
| --- | --- |
| `node-monitor history --user X\|--node Y [--bucket 5m\|1h\|1d] [--format table\|json\|csv]` | 5m/1m roll-ups for a user or node (ISO timestamps) |
| `node-monitor commands search PATTERN [--regex] [--user --activity --tag] [--limit]` | search `command_lines` (ILIKE escaped / `~` regex); bad regex exits 2 |
| `node-monitor reclassify [--dry-run] [--apply]` | re-run classify+tags over `process_samples`+`command_lines` batched, report delta; writes only with `--apply` |

`history` reads via `web.database_url`; `reclassify` writes via `database.url` (owner).

```bash
node-monitor commands search "claude" --limit 20
node-monitor commands search "claude.*skip" --regex
node-monitor reclassify --dry-run
```

## Web dashboard

```bash
node-monitor web --port 9998   # loopback only, read-only role
```

Opens `http://127.0.0.1:9998/` — live overview (`/`), per-node (`/node/{node}`), per-user (`/user/{username}`), history (`/analytics`, `/commands`), all polling every 30 s with a disconnected banner on failure. `web.database_url` must be set (reader role). Reach it from your laptop via `ssh -L 9998:localhost:9998 polaris-login-04`.

Vendor: Vue `3.4.38` + Chart.js `4.4.9` under `node_monitor/web/static/vendor/` (no CDN).

## Database

The collector writes to PostgreSQL (database and schema `node_monitor`). Two roles are
used: `node_monitor_owner` (owns the schema; the collector and migrations) and
`node_monitor_reader` (SELECT only; web and CLI). Configure them in the config file:

- `database.url`: owner-role URL, e.g. `postgresql://node_monitor_owner@localhost/node_monitor`
- `web.database_url`: reader-role URL
- `database.connect_timeout_sec` (default 10): connection timeout, rounded up to whole
  seconds; must be a finite positive number
- `retention.*_days` (defaults 730 / 730 / 180) and `spool.max_bytes` (default 1 GiB)

`NODE_MONITOR_DB_URL` and `NODE_MONITOR_WEB_DB_URL` override the two URLs. If a URL in
the file contains a password, the file must have mode 0600 or loading fails.

Usage:

```
node-monitor database bootstrap --admin-url postgresql://localhost/postgres  # roles + database
node-monitor database migrate    # run with the owner-role URL from database.url
node-monitor database status     # schema version and connectivity
```

`database migrate` must run with the owner-role URL (`database.url`), so that tables and
partitions get the reader's default privilege; it refuses to run as any other role.
`database bootstrap` creates the database from `template0` with the builtin `C.UTF-8`
locale (PostgreSQL 17+), so its text indexes do not depend on the host's glibc.

### Spool

While PostgreSQL is unreachable, record batches are written to an on-disk spool and
replayed, oldest first, once it answers again. The collector daemon (M1c) spools to
`<working_dir>/state/spool`; `soak replay --write-db` uses `<working_dir>/state/spool-replay`
so a replay never mixes with live data. Each batch is one owner-only
`<UTC time>-<sequence>.jsonl` file (`-L` before `.jsonl` marks a batch of live records
only). Above `spool.max_bytes`, live-only batches, which newer ones supersede, are dropped
first, then the oldest; every drop is recorded as a `spool_dropped` collection event.

A batch that can never be written goes to the spool's `corrupt/` subdirectory, with a
`spool_batch_quarantined` collection event naming the file and the error: an unreadable
spool file, a spooled batch the database rejects, or a fresh batch the database rejects
(the collector then also logs and raises the error). `corrupt/` is not counted towards
`spool.max_bytes` and is never cleaned up automatically. To retry a batch once the cause is
fixed, move its file back into the spool directory; names sort in write order, so it is
replayed in its original place on the next write.
