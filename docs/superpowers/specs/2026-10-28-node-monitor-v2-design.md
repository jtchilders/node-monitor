# node-monitor v2 — Design

Date: 2026-10-28
Status: approved in brainstorming, pending written-spec review

## 1. Purpose

node-monitor observes login nodes on ALCF systems: what is running, who is
running it, what it costs, and how that changes over time. It should be to
login nodes what pbs_monitor is to PBS jobs — a tool used daily for:

1. **Live triage** — who and what is loading a node right now; drill from
   node → user → process.
2. **Trends** — weeks to years of history for operations and capacity
   decisions (e.g. ALCF-4 login-node sizing).
3. **Behavior research** — the database is a research corpus: AI coding
   agents, editor servers, Jupyter, builds, dormant/abandoned processes,
   command-line options in use (e.g. agents run with permission checks
   disabled).
4. **Alerts and intelligence** — rules and, later, analysis built on the same
   data.

### Non-goals

- Not a profiler or bug-hunting tool.
- No root, no sudo, nothing installed on monitored nodes.
- Does not manage the PostgreSQL server (pbs_monitor's units keep it up).
- No standby collector in v2; a collector-host outage is a visible gap.

### Relationship to the previous implementation

The previous repo (`~/node-monitor-20261028`, same GitHub remote) grew out of
a finite 24-hour Phase 0 canary. Its finite-run model (run directories,
acceptance thresholds, `DONE` markers, fatal-on-DB-error) is replaced by an
always-on, restart-safe collector. Specific pieces are reused (§12). The old
`node_monitor_dev` database is left untouched as an archive.

## 2. Decisions

| Topic | Decision |
|---|---|
| Systems | Machine-agnostic; `system:` set in config. Develop on Polaris, deploy to Aurora later as a second instance. |
| Database | Shared PostgreSQL server with pbs_monitor (PostgreSQL 18 on Polaris), independent database `node_monitor`. |
| Collection | One collector per system; persistent SSH session per node running a streaming stdlib probe. |
| Robustness | Collector never exits due to SSH, node, or DB failure; gaps are recorded and visible. |
| Detail | Live (all processes, current), recent (per-process, 30 days, full redacted command lines), long-term (5-minute per-user and per-node summaries, per-minute node metrics, 2 years). |
| Command lines | Kept in recent history and a distinct-command-line table; secret *values* redacted, flags always preserved. |
| Dormancy | First-class: per-process CPU state, idle duration, owner presence; summarized long-term. |
| Presentation | pbs_monitor model: FastAPI + Vue 3 + Chart.js multi-page dashboard, rich-table CLI. |
| Layout | `~/node-monitor/` is a working directory; the repo is `~/node-monitor/node-monitor/`. Fresh git history on the existing GitHub repo. |

## 3. Architecture

```
                   one login node per system (e.g. polaris-login-04)
 ┌──────────────────────────────────────────────────────────────────────┐
 │ collector daemon (systemd user unit, Restart=always)                 │
 │                                                                      │
 │  NodeSession ×N ──lines──► Aggregator ──records──► Writer ──► Postgres│
 │  (ssh + probe)             (live, 1m, 5m,          (retry,     ▲     │
 │                             samples, cmdlines,      spool)     │     │
 │                             events)                            │     │
 │  Maintenance task: partitions, retention, heartbeat ───────────┘     │
 └──────────────────────────────────────────────────────────────────────┘
        │ ssh (one long-lived session per node, incl. own host)
        ▼
 login nodes: python3 -u -c <bootstrap>  ← probe source sent on stdin

 web server (loopback, read-only role) ──► Postgres ◄── CLI (read-only role)
```

### Working directory and repo layout

```
~/node-monitor/                    working directory (not a repo)
├── venv/
├── node-monitor/                  git repo
│   ├── node_monitor/
│   │   ├── probe/                 remote probe (Python 3.6, stdlib only) + rules
│   │   ├── collector/             sessions, aggregation, writer, spool, maintenance
│   │   ├── database/              models, SQL migrations, repositories
│   │   ├── web/                   FastAPI server + static pages
│   │   ├── notifications/         (M4) alert rules + Slack
│   │   ├── cli/                   click commands
│   │   └── config.py
│   ├── deploy/systemd/            user units, rearm.sh
│   ├── docs/
│   └── tests/
├── logs/                          daemon and web logs
└── state/                         ssh config + control sockets, DB-outage spool
```

### Configuration

Search order (first found wins): `--config PATH`, `$NODE_MONITOR_CONFIG`,
`~/.node_monitor.yaml`, `~/.config/node_monitor/config.yaml`. Database URLs
may be overridden by `NODE_MONITOR_DB_URL` / `NODE_MONITOR_WEB_DB_URL`.
Credential-bearing config files must be mode 0600 (enforced with a clear
error). `node-monitor config --create` writes a commented template.

```yaml
system: polaris
nodes:                               # host = node identity; ssh_target = how to reach it
  - host: polaris-login-01.hsn.cm.polaris.alcf.anl.gov
    label: login-01
    ssh_target: polaris-login-01.head  # optional; defaults to host
  - host: polaris-login-02.hsn.cm.polaris.alcf.anl.gov
    label: login-02
    ssh_target: polaris-login-02.head
  - host: polaris-login-04.hsn.cm.polaris.alcf.anl.gov
    label: login-04
    ssh_target: polaris-login-04.head
ssh:
  identity_file: ~/.ssh/id_ed25519_polaris   # optional; written into the project ssh_config
  connect_timeout_sec: 10
probe_python: /usr/bin/python3.11
working_dir: ~/node-monitor          # logs/ and state/ live here
database:
  url: postgresql://node_monitor_owner@host:port/node_monitor
web:
  database_url: postgresql://node_monitor_reader@host:port/node_monitor
  host: 127.0.0.1
  port: 9998
collection:
  counter_interval_sec: 10
  census_interval_sec: 60
  summary_interval_sec: 300
  hourly_census: true
  session_silence_timeout_sec: 30
  session_max_lifetime_sec: 21600
  reconnect_backoff_sec: [5, 300]
classification:
  min_user_uid: 1000
  active_cpu_seconds_per_min: 0.1
  notable_cpu_seconds_per_min: 3.0     # ≈5% of one core
  notable_rss_mb: 1024
redaction:
  extra_patterns: []                   # appended to built-in rules
retention:
  process_samples_days: 30
  node_metrics_days: 730
  usage_summary_days: 730
  command_lines_days: 730              # measured from last_seen
  collection_events_days: 180
spool:
  max_bytes: 1073741824
```

All thresholds, intervals, and retention periods are configuration so they
can be tuned once real data is flowing.

### Database roles

- `node_monitor_owner` — owns the schema. Used by `node-monitor database
  migrate` and by the collector, which also creates future partitions and
  drops expired ones for its own tables.
- `node_monitor_reader` — SELECT only. Used by the web server and CLI.

Migrations are numbered plain-SQL files applied by a small runner
(`node-monitor database migrate | status`). The collector refuses to start
against a schema older than it expects, with a message naming the command to
run.

## 4. Collection

### Session

For each configured node (including the collector's own host), the
collector runs:

```
ssh -F <working_dir>/state/ssh_config -o BatchMode=yes \
    -o ServerAliveInterval=10 -o ServerAliveCountMax=3 \
    <ssh_target> env -u LD_PRELOAD <probe_python> -u -c '<bootstrap>'
```

The project-owned `ssh_config` uses ControlMaster with sockets under
`state/`, and `IdentityFile`/`IdentitiesOnly` when `ssh.identity_file` is
set (on Polaris, login-node SSH only succeeds via the `.head` names with the
user's Polaris key). The bootstrap (≈20 lines, Python 3.6) reads a length-prefixed probe
source from stdin, compiles and runs it. stdin then stays open as the
liveness channel. The probe source is sent on every connection, so the
remote always runs the collector's version.

### Probe protocol

One JSON object per line on stdout, each with `type`, `seq`, `ts` (node
clock, UTC):

| `type` | Cadence | Content |
|---|---|---|
| `hello` | once per session | probe version, FQDN, `boot_id`, `btime`, hardware inventory |
| `counters` | every 10 s | node-level counters: CPU jiffies, loadavg, meminfo, process counts, network, Lustre metadata ops, sockets, per-GPU utilization/memory |
| `census` | every 60 s | full process list (below) with per-process CPU deltas and dormancy fields |
| `error` | as needed | bounded, non-fatal probe-side errors (e.g. `nvidia-smi` failed) |

Per-process census fields: pid, start time, uid/username, comm, exe, argv0,
full cmdline, ppid and parent comm/exe, parent kind, state, threads, RSS,
utime/stime deltas since the previous census, tty, session leader,
under screen/tmux, GPU memory (from `nvidia-smi --query-compute-apps` where
available), and the dormancy fields in §5.3. `/proc/<pid>/{cwd,environ,fd,io}`
are unreadable for other users and are not collected.

The probe is long-lived and keeps per-process state (previous CPU ticks,
last-active time) keyed by `(pid, start_time)`.

### Probe self-termination

The probe exits when any of these holds: stdin reaches EOF; a write to
stdout fails; its parent becomes PID 1. On start it takes a per-node lock in
`/dev/shm/node-monitor-<uid>.lock` holding its PID; if another probe holds
it, the new probe signals the old one and takes over, so at most one probe
runs per node.

### Failure handling

| Failure | Detection | Response |
|---|---|---|
| SSH drop / network blip | EOF, or no line for `session_silence_timeout_sec` | Kill the session; reconnect with exponential backoff and jitter within `reconnect_backoff_sec`; record `session_lost` / `session_started` events. |
| Stale probe on node | stdin EOF, failed stdout write, reparented to init | Probe exits; lock takeover guarantees one per node. |
| Long-lived session | `session_max_lifetime_sec` | Collector rotates the session proactively. |
| Node reboot | session failure; on reconnect `hello.boot_id` differs | New `node_hardware` row; reset delta baselines; `node_rebooted` event. |
| Overloaded / hung node | silence timeout; per-sample latency | Treated as a drop; latency recorded in `node_metrics_1m`. |
| Collector crash | systemd | `Restart=always`. Partially filled intervals are written with reduced coverage. |
| Collector host reboot | systemd + linger | Starts at boot; `rearm.sh` re-enables linger if maintenance cleared it. Heartbeat row shows staleness on the dashboard. |
| PostgreSQL outage | write failure | Retry with backoff; records buffer in memory, then in `state/spool/` (capped at `spool.max_bytes`; superseded live snapshots dropped first); replayed in order on recovery; `spool_replayed` event. The collector never exits on DB errors. |

Intervals are bucketed on the collector's clock (UTC, wall-clock aligned) so
all nodes share one clock; each node's clock offset is recorded per minute.

### Soak test gate

Before building beyond the probe and session layer (M0), the streaming probe
runs on login-01, -02, and -04 for at least 24 hours to confirm login-node
policy tolerates a long-lived process, to measure probe CPU/RSS overhead, and
to exercise drops and reconnects.

## 5. Classification, redaction, dormancy

Classification and redaction run in the collector (not the probe) so rules
are configuration-driven and unit-testable. Rules live in the repo as data
tables reviewed with examples.

### 5.1 Vocabulary

| Field | Granularity | Examples |
|---|---|---|
| `category` | coarse, ~12 values | `ai_agent`, `editor_server`, `jupyter`, `build`, `python`, `shell`, `data_transfer`, `workflow`, `system`, `other` |
| `activity` | fine, ~80 values (seeded from the old tuned rules) | `claude-code`, `cursor-server`, `pytest`, `globus-compute`, `python-training` |
| `tags` | flag-derived, zero or more per process | `agent.skip_permissions`, `agent.full_auto`, `python.debugger` |

Tag rules match a tool (by activity) plus command-line options, e.g.
activity `claude-code` with `--dangerously-skip-permissions` →
`agent.skip_permissions`.

A **user process** is one with `uid >= min_user_uid` and `category != system`.

### 5.2 Redaction

Applied to every command line before storage. Flags and option names are
always preserved; only values matching secret patterns are replaced with
`***`. Built-in patterns cover: values of options named like
`password|passwd|token|secret|api[-_]?key|auth`; `KEY=value` assignments with
those names; `user:pass@` in URLs; `Authorization:`/`Bearer` header values;
well-known key formats (AWS access keys, GitHub `ghp_`/`gho_`/`github_pat_`,
`sk-` style API keys). `redaction.extra_patterns` appends site rules.

Example: `claude --dangerously-skip-permissions --api-key sk-abc123` →
`claude --dangerously-skip-permissions --api-key ***`.

### 5.3 Dormancy

Computed for every user process each census:

| Field | Definition |
|---|---|
| `cpu_state` | `active` if CPU used in the last minute ≥ `active_cpu_seconds_per_min`, else `idle`. |
| `idle_for_sec` | seconds since the process was last `active`. Tracked by the long-lived probe; after a session (re)start it is a lower bound and `idle_for_is_lower_bound` is set. |
| `idle_bucket` | `<5m`, `5m-1h`, `1h-1d`, `>1d`. |
| `owner_presence` | `interactive` if the user has an `sshd: <user>@pts/*` session on the node; `connected` if only `sshd: <user>@notty` sessions (e.g. editor remote connections); `absent` otherwise. |

## 6. Data model

All timestamps are `timestamptz` (UTC). Time-series tables are range-
partitioned; retention is enforced by dropping partitions. Every summary row
carries `sample_count` and `expected_count`.

| Tier | Table | Key / grain | Partitioning | Retention |
|---|---|---|---|---|
| Live | `live_processes` | (system, node, pid, start_time); every process, replaced per node per census in one transaction | — | current |
| Live | `live_nodes` | (system, node); latest metrics, session state, last sample time, probe version, boot_id | — | current |
| Recent | `process_samples` | (system, node, ts, pid, start_time) | daily | 30 days |
| Long-term | `node_metrics_1m` | (system, node, minute) | monthly | 2 years |
| Long-term | `user_usage_5m` | (system, node, username, interval_start) | monthly | 2 years |
| Long-term | `node_usage_5m` | (system, node, interval_start) | monthly | 2 years |
| Long-term | `command_lines` | (system, username, cmdline_hash) | — | 2 years from last_seen |
| Inventory | `nodes` | (system, node); label, first/last seen, current boot_id | — | forever |
| Inventory | `node_hardware` | (system, node, boot_id) | — | forever |
| Ops | `collection_events` | id; ts, system, node, event_type, detail jsonb | monthly | 180 days |
| Ops | `collector_heartbeat` | (system); collector host, version, started_at, last_seen | — | current |

### Table contents

**`process_samples`** — rows written for (a) every *notable* user process
each minute: CPU ≥ `notable_cpu_seconds_per_min` or RSS ≥ `notable_rss_mb`,
or category `ai_agent`, or any tag; and (b) every user process once per hour
at minute 0 (`hourly_census`), flagged `hourly=true`. Columns: identity
(pid, start_time, username, uid), comm, exe, argv0, redacted cmdline,
cmdline_hash, parent comm/exe/kind, category, activity, tags (text[]),
state, threads, cpu_seconds (minute), rss_kb, gpu_mem_mb, interactive,
under_screen_or_tmux, cpu_state, idle_for_sec, idle_for_is_lower_bound,
owner_presence.

**`node_metrics_1m`** — CPU busy breakdown (user, system, iowait, irq,
steal, idle %), load1/5/15, memory (total, used, available, cached, shmem,
swap used), procs running/total, network rx/tx bytes/s per interface class,
Lustre metadata ops/s, socket count, per-GPU utilization and memory (jsonb),
probe sample latency, node clock offset, sample_count/expected_count.

**`user_usage_5m`** — process_count (avg, max), cpu_seconds, rss_kb (avg,
max), gpu_mem_mb (avg, max), threads (avg), d_state_fraction,
interactive_fraction, plus jsonb maps:
- `by_category`, `by_activity`, `by_tag`: `{key: {count, cpu_s, rss_kb}}`
- `by_dormancy`: `{"<cpu_state>|<idle_bucket>|<owner_presence>": {count, cpu_s, rss_kb}}`
- `by_category_dormancy`: same dormancy key nested under category

`count` values are interval averages of per-census counts.

**`node_usage_5m`** — process_count, user_count, active_user_count (users
with any `active` process), the same totals and jsonb maps aggregated across
all users on the node, sample_count/expected_count.

**`command_lines`** — redacted cmdline, cmdline_hash, exe, activity,
category, tags, first_seen, last_seen, nodes (text[]), sample_count.
Covers every user process seen in any census, not only notable ones;
sightings are accumulated in memory and upserted once per summary interval.

### Reclassification

Stored rows keep the labels assigned at collection time; improved rules
apply to new data. `node-monitor reclassify` re-labels `process_samples` and
`command_lines` (which retain redacted command lines) and reports how label
counts would change, so new rules can be tested against history.

### Expected volume (Polaris, 3 login nodes)

From the previous corpus: ~1,800 processes, ~200 users, ~490
user×activity groups per node; distinct (user, cmdline) pairs grow on the
order of thousands per node per day.

| Table | Approx. volume |
|---|---|
| `process_samples` | ~15–20M rows over the 30-day window |
| `user_usage_5m` | ~175k rows/day, ~60M rows/year |
| `node_metrics_1m`, `node_usage_5m` | small (thousands of rows/day) |
| `command_lines` | low millions of rows/year |

## 7. Web dashboard

FastAPI server, loopback by default (`node-monitor web`), reached through an
SSH tunnel. Static pages use Vue 3 and Chart.js, vendored at pinned versions
under `web/static/vendor/` so pages work without CDN access. Live pages poll
every 30 s and show an explicit "web server disconnected" state. Long ranges
query the 1-minute/5-minute tables with SQL-side hourly/daily aggregation.

| Page | Content |
|---|---|
| `/` Live overview | Collector health strip (heartbeat, per-node session state, last gap); per-node cards (CPU, memory, load, GPU, users, processes, active/idle/dormant-unattended counts); category mix per node; sortable/filterable "top now" table by user or process; dormancy panel (idle > 1h with owner absent, by category); recent events. |
| `/node/{node}` | Time series (CPU breakdown, memory, load, network, Lustre ops, GPU) with ranges 1h–2y; category/activity stacked areas; users over time; live process table. |
| `/user/{username}` | Current processes across nodes; 5-minute history of CPU/memory/process count by category; dormancy profile; tags seen; searchable distinct command lines. |
| `/analytics` | Users and processes per node (typical vs peak); category/activity mix over time; AI-agent adoption (users per day, tag share); dormancy trends; leaderboards (CPU-hours, memory-hours, dormant memory-hours); collector coverage and gaps. |
| `/commands` | Command-line explorer: substring/regex search over `command_lines`, filters by user/activity/tag, first/last seen; used to develop and check classification and tag rules. |

A shared process modal (opened from any process row) shows the redacted
command line, parent chain, classification, and per-minute history from
`process_samples`.

JSON APIs back every page under `/api/...`; the web role is read-only.

## 8. CLI

`node-monitor <command>`; rich tables; `--format table|json|csv`; common
filters `-u/--user`, `-n/--node`, `-c/--category`, `--sort`, `--reverse`.

| Area | Commands |
|---|---|
| Live state | `status`, `nodes`, `top`, `users`, `user USERNAME`, `idle` |
| History and search | `history` (user or node 5-minute history), `commands search PATTERN` |
| Operations | `daemon start [--foreground] / stop / status`, `database migrate / status`, `web`, `config --create`, `reclassify` |

## 9. Deployment

`deploy/systemd/` provides user units following pbs_monitor's proven
pattern:

- `node-monitor-daemon.service`: `ExecStart=… node-monitor daemon start
  --foreground`, `Restart=always`, `RestartSec=15`,
  `StartLimitIntervalSec=0`, explicit `PATH`, logs appended under
  `~/node-monitor/logs/`. It waits for PostgreSQL reachability in
  `ExecStartPre` with a bounded loop but does **not** `Requires=` pbs_monitor's
  Postgres unit, and the daemon itself tolerates DB absence.
- `node-monitor-web.service` (optional).
- `rearm.sh`: re-enables linger and restarts units after maintenance.

Units are installed into `~/.config/systemd/user/` (on `$HOME`, which
survives login-node reimaging). A README covers install, linger, and the
clean-environment pitfalls documented in pbs_monitor.

## 10. Alerts and intelligence (M4)

Modeled on pbs_monitor `notifications/`: rules in config, Slack webhook
delivery, content-aware dedupe. Rules evaluate each collection cycle against
live tables and `node_metrics_1m`. Initial rule types: node memory above a
threshold for a duration; session down longer than a duration; user holding
more than N GB of dormant, owner-absent memory; stale collector heartbeat.
Anomaly detection and reports are later work on the same data.

## 11. Testing and quality

- **Probe**: collectors and parsers tested against fixture `/proc` trees;
  probe tests also run under Python 3.6 in CI.
- **Classification and redaction**: table-driven tests from a curated,
  hand-scrubbed sample of real command lines; every rule change ships with
  examples.
- **Aggregation**: summaries, dormancy tracking, and CPU deltas with a fake
  clock.
- **Robustness integration test**: collector against a fake `ssh` that runs
  the probe locally; kill a session mid-stream, simulate a `boot_id` change,
  make the database unavailable; assert reconnect, recorded gap, and spool
  replay without loss.
- **Database and web**: tests against a disposable PostgreSQL (service
  container in CI; skipped locally when unavailable); API tests with httpx.
- Tests describe behavior; no tests that assert on README/source text; test
  files named by component, not by task.
- Test dependencies are declared and installed by the documented dev setup;
  `pytest` passes on a clean checkout.
- **CI**: GitHub Actions running `ruff` (3-space indentation) and `pytest`.
- Code style: 3-space indentation (matching pbs_monitor and the previous
  repo); comments explain *why*; history lives in git and specs, not
  comments. Python ≥ 3.10 everywhere except the probe (≥ 3.6, stdlib only,
  no f-strings).

## 12. Reuse from the previous repo

Copied and cleaned rather than imported wholesale:

- Probe collectors (`/proc` parsing for loadavg, CPU jiffies, meminfo,
  network, Lustre metadata, sockets, hardware, GPUs, processes) and the
  process/activity classification rule tables (tuned on ~400k
  observations).
- CPU/network/Lustre delta math and window-accumulation logic.
- Usage-grain aggregation ideas and schema column definitions.
- `fake_proc` test fixtures.

Not carried forward: the finite-run harness (JSONL sink, acceptance
evaluation, `DONE` markers, deploy screen scripts), the per-poll subprocess
transport, the thread-per-request web DB worker, and the existing test
suite.

## 13. Delivery

One implementation plan per milestone; each milestone runs in production
before the next begins.

| Milestone | Delivers | Exit criterion |
|---|---|---|
| M0 Soak | Streaming probe, bootstrap, minimal collector logging lines to a file | ≥ 24 h on login-01/02/04: tolerated by login-node policy, overhead measured, drop/reconnect behaves |
| M1 Collector + DB | Full collector, migrations, live and long-term tables (`live_*`, `node_metrics_1m`, `*_usage_5m`, inventory, events, heartbeat), classification, redaction, dormancy, spool, maintenance, systemd units; CLI `status`, `nodes`, `top`, `users`, `user`, `idle`, `daemon`, `database`, `config` | One week unattended under systemd, including a deliberate PostgreSQL restart and a node reboot |
| M2 Live web | `/`, `/node/{node}`, `/user/{username}`, process modal | Used for day-to-day triage |
| M3 History and research | `process_samples` + hourly census, `command_lines`, tags, `/analytics`, `/commands`, `history`, `commands search`, `reclassify` | Dormancy and agent-option questions answerable from the UI and SQL |
| M4 Alerts + Aurora | Slack rules; Aurora configuration and deployment | Running on both systems |

## 14. Risks and open items

- **Login-node process policy** may kill long-lived processes. Mitigated by
  the M0 soak gate; fallback is the stateless per-poll probe with the same
  record formats (dormancy `idle_for` would then be derived across polls).
- **Rule drift**: new tools appear constantly. Mitigated by `command_lines`,
  `/commands`, and `reclassify`.
- **Command-line sensitivity**: redaction is pattern-based and can miss
  secrets. Mitigated by bounded retention of `process_samples`, the
  independent database, read-only web role, and loopback-only web binding.
- **Single collector host** per system: accepted; gaps are visible.
- Thresholds, retention periods, and the summary interval are expected to be
  tuned after M1 data is available.
