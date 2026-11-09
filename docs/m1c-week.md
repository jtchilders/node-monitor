# M1c one-week run — polaris-login-04

Period: 2026-11-01 22:15 UTC → 2026-11-09 04:18 UTC (7d 18h shown; 7d required). Daemon `0.1.0+v0.1.0-alpha.3-24-ge58c1e8` started by hand from `main`, no systemd.

## Coverage

| Table | Rows per node | Partial | Gaps |
|---|---|---|---|
| `node_metrics_1m` | 11,166 (1/min, 3 nodes = 33,498) | 2 at the 22:15 start minute (4/6, daemon started mid-minute) | none since start |
| `node_usage_5m` | 2,232 per node | 0 | — |
| `user_usage_5m` | ~2,470 per node (7,400 total) | 0 since start | — |

DB growth: 228 MB at 22h → 1.6 GB at 7d (~190 MB/day, ~1.48 GB `user_usage_5m`). On track for ~135 GB/year (was 130 GB projection).

## Events

- `collector_started` 1, `session_started` 96 (initial + 14 rotations/node every 6h), `session_rotated` 93 (3/node/day) — all clean, no `session_lost`, no `node_rebooted`.
- `probe_error` 3,353 — all `nvidia-smi: Failed to initialize NVML` on login nodes (no GPUs). Backed off correctly.
- No DB outages, no spool, no `corrupt/`.

A Nov 7 maintenance was expected but no reboot occurred.

## Resources

- Daemon: RSS 77–135 MiB flat for last 5 days (VmHWM 169 MiB, VmPeak 2.0 GB from Python allocator, stable). CPU 4h in 7.7d (~2% core). `daemon.log` 0 bytes.
- Probes: 19–25 MiB per node, stable. Sample latency p99 ~0.52s, clock offset <30ms.
- `live_processes` autovacuum: ~1,360 runs, keeping dead tuples ~5k.

## Users

- Distinct users per day: 368 → 411 → 426 → 433 → 445 → 430 → 373 → 375 (Nov 1–8). Flat 7-day pattern.
- Editor servers dominate CPU (1,315 CPU-h/24h), AI agents next (753 CPU-h) with 169 `active|absent` (owner gone, agent still active).
- Dormant `idle>1h|absent` steady ~48 GiB (python 21 GiB, editor_server 16 GiB).

## Verdict

Exit criterion met: one week unattended, including deliberate rotation load; no gaps. The deferred DB-outage test and node reboot remain open and are covered by Task 6 tests.

Follow-ups in `docs/m1c-followups.md` still apply (stale `daemon status` after crash, startup log line, Procs column).
