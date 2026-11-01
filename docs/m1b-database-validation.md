# M1b database validation on polaris-login-04

Date: 2026-11-01. Server: PostgreSQL 18.4 on polaris-login-04, shared with pbs_monitor. The data
directory is on `/lus/eagle` (139 TB free at the time). Branch `feat/m1b-database`.

## Setup

```bash
node-monitor database bootstrap --admin-url postgresql://localhost/postgres
#   created role node_monitor_owner / node_monitor_reader, database node_monitor, schema node_monitor
node-monitor database migrate          # applied: 0001 (as node_monitor_owner)
node-monitor database status           # current 1, required 1, pending none
```

Before the run, the only pre-existing objects named `node_monitor*` were the v1 database
`node_monitor_dev` and the v1 role `node_monitor_web`. Neither was touched.

`~/.node_monitor.yaml` (mode 0600) gained:

```yaml
database:
  url: postgresql://node_monitor_owner@localhost/node_monitor
web:
  database_url: postgresql://node_monitor_reader@localhost/node_monitor
```

Database properties after bootstrap:

| Property | Value |
|---|---|
| Locale provider / locale | `builtin` / `C.UTF-8` |
| Encoding | UTF8 |
| ACL | owner `CTc`, reader `c`; no PUBLIC entry |
| Database settings | `search_path=node_monitor, public`, `TimeZone=UTC` |
| Bootstrap re-run | prints `nothing to do` |

## Replays

| Capture | Mode | Wall time | `node_metrics_1m` per node | Other records per node |
|---|---|---|---|---|
| M0 `20261028T230908Z` (26 h) | summary | 37 s | 1,561 (expected ≈1,561) | 168–171 events, 6 hello |
| M1a `20261030T200954Z` (12 h) | summary | 36 s | 721 (expected ≈721) | 74–75 events, 2 hello |
| 15-min `--full-census` capture | full census | 38 s | 16 | 15 census, 4 `node_usage_5m`, 848–972 `user_usage_5m` |

- None of the replays had rows beyond retention or anything spooled.
- The latest `node_metrics_1m` rows from the M1a capture have `probe_rss_kb` populated (20–26 MB).
- The census capture was deleted after replay because it holds other users' command lines. After
  validation, `live_processes` was emptied (4,918 rows) for the same reason; the M1c collector
  repopulates it.

## Row counts per table and node (after all replays)

| Table | login-01 | login-02 | login-04 |
|---|---|---|---|
| `node_metrics_1m` | 2,298 | 2,298 | 2,298 |
| `user_usage_5m` | 881 | 848 | 972 |
| `node_usage_5m` | 4 | 4 | 4 |
| `collection_events` | 323 | 324 | 325 |
| `live_processes` (before clearing) | 1,656 | 1,551 | 1,711 |
| `live_nodes` | 1 | 1 | 1 |
| `node_hardware` | 1 | 1 | 1 |

`node_hardware` has one row per node because the key is `(system, node, boot_id)` and no node
rebooted between the soaks. The same `boot_id` appears in both captures.

## Read-back queries (as `node_monitor_reader`)

| Query | Time | Result |
|---|---|---|
| Latest `node_metrics_1m` per node | 220 ms | 3 rows, 2026-10-31 08:09 UTC: load1 178 / 123 / 251, mem_used 336 / 323 / 412 GB |
| Users per 5-min interval | 428 ms | 675 rows per interval (≈225 users per node); 356 distinct users system-wide |
| Top users by `rss_kb_avg`, last full interval | 130 ms | top 5 (anonymised): 23.6, 23.5, 23.3, 23.3, 15.9 GB across 3 nodes |
| Top users by dormant memory (`idle\|1h-1d\|absent`, `idle\|>1d\|absent`) | 417 ms | 0 rows; see note below |
| Live AI-agent processes by owner presence | 12 ms | absent 271 (84 users), connected 39 (15), interactive 29 (9) |
| Partition list | 189 ms | 14 monthly partitions (`*_y2026m10` … `*_y2027m01`) |
| Reader `INSERT` into `collector_heartbeat` | — | denied, SQLSTATE 42501 |

These timings are on a 37 MB database with a cold Python process, so they are not representative
at scale.

**Dormancy note:** the probe measures `idle_for_sec` from when it first saw a process
(`probe.py:627`). A 15-minute capture therefore can't produce idle buckets over 1 hour; the
observed buckets were `<5m` and `5m-1h`, plus `unknown` on first sight. The query is correct but
needs M1c's long-running collector before it returns rows.

AI-agent breakdown: 271 of 339 live agent processes (80%) belong to users with no session on the
node (`absent`).

## Idempotency

A second `soak replay --write-db` of the M1a capture:

- Summary tables were unchanged: `node_metrics_1m` 6,846, `node_hardware` 3.
- `collection_events` grew by 224 (733 → 957). It has no natural key (deferred T3 item), so a
  replay or a resend after a lost acknowledgement duplicates events. Recorded for M1c in
  `docs/m1b-followups.md`.

## Storage

Measured on the census data. On-disk figures include indexes and are inflated by small-table
overhead.

| Table | Rows | Bytes per row (on disk) |
|---|---|---|
| `user_usage_5m` | 2,701 | 1,386 |
| `node_metrics_1m` | 6,894 | 2,301 |
| `live_processes` | 4,918 | 1,374 |
| `collection_events` | 972 | 506 |

Average `user_usage_5m` tuple: 1,090 B. Of that, the jsonb maps take:

| Column | Bytes |
|---|---|
| `by_category` | 162 |
| `by_activity` | 198 |
| `by_dormancy` | 218 |
| `by_category_dormancy` | 315 |

**Projection for `user_usage_5m` on Polaris** (3 login nodes):

- 675 rows every 5 minutes is about 194k rows/day.
- At 1.2–1.4 KB per row that is **≈ 170–200 GB over the 730-day retention**.
- `node_metrics_1m` adds about 7 GB.
- Aurora has more login nodes, so it scales proportionally.

`by_category` and `by_dormancy` can be derived exactly from `by_category_dormancy`, because the
sums are linear. Dropping them would save about 35% of each row. This is an owner decision to make
before M1c starts accumulating data.

## Database size

37 MB after all replays (`pg_database_size('node_monitor')`).
