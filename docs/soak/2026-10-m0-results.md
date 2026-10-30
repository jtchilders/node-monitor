# M0 streaming-probe soak results

## Decision: GO (conditional) - proceed to M1 with streaming

Conditions: fix the probe memory growth early in M1 (see "Probe memory growth"); keep the 6-hour rotation until it is fixed. If the fix is not in the first M1 iteration, shorten rotation to 4 h.

Reasons:

- Coverage outside fault windows is 100.0% on all three nodes. The largest gap outside a fault window is 10.6 s (one 10 s counter interval).
- The run had no unexplained probe terminations. There were 3 losses, one per node, and every one was a deliberate fault check. There were no session crashes, malformed lines or probe stderr lines.
- Every reconnect recovered in 3.5-4.6 s of backoff.
- Probe CPU is 26-30 s/h per node against a limit of 60. The census scan p95 is 0.42-0.48 s.
- Probe max RSS is 98-165 MB against a limit of 200 MB. **This passes only because of the 6-hour rotation** and is a defect (below).

## Run

Units: "MB" in this document means MiB (kB/1024), as in the report table.

| Item | Value |
|---|---|
| Window | 2026-10-28T23:09:06Z to 2026-10-30T01:09Z (26 h, collector exited normally) |
| Hosts | polaris-login-01, -02, -04 (collector ran on -04) |
| git_describe | `4dd1458` (git_commit `4dd14584febea940387708b3f2489d326b50d6c2`, not dirty) |
| probe_sha256 | `57be7e8e8d4d688d08e0c6fc8f0d40834285a90535c3d0ef3d8bca517de4d21c` |
| Data | `~/node-monitor/logs/soak/20261028T230908Z` (about 720 MB JSONL, plus `report.json`) |
| Intervals | counters 10 s, census 60 s, rotation 6 h |

## Report table (verbatim)

```
node      hours  coverage%  census%  errors  max_gap_s  sessions  losses  rotations  reboots  cpu_s/h  maxrss_MB  scan_p95_s  procs
login-01  26.00      100.0    100.1     156         11         6       1          4        0     29.8      141.0        0.48   1805
login-02  26.00      100.0    100.1     159         10         6       1          4        0     26.2      164.5        0.42   1523
login-04  26.00      100.0    100.0     158         43         6       1          4        0     29.6       98.2        0.47   1776
```

Further figures from `report.json`:

| node | counters recv/expected | census coverage % | errors_by_where | gpu_status_counts | cpu_seconds_min / max | idle_for_max_sec | scan p50 / p95 / max (s) |
|---|---|---|---|---|---|---|---|
| login-01 | 9360 / 9359.4 | 100.07 | gpu 156 | error 156, suspended 1405 | 0.0 / 302.59 | 21540 | 0.39 / 0.48 / 0.57 |
| login-02 | 9360 / 9359.2 | 100.07 | gpu 158, signal 1 | error 158, suspended 1403 | 0.0 / 423.30 | 21540 | 0.34 / 0.42 / 0.49 |
| login-04 | 9357 / 9359.1 | 100.01 | gpu 158 | error 158, suspended 1402 | 0.0 / 319.74 | 21540 | 0.39 / 0.48 / 0.76 |

Coverage outside the fault windows (fault windows below removed from both received and expected): 100.006% (login-01), 100.006% (login-02), 100.002% (login-04). The login-04 coverage of 99.977% (in `report.json`; the verbatim table rounds it to 100.0) is the c' window, about 50 s. Coverage above 100% is clock-granularity noise.

Census-delta sanity (Review Focus 3): `cpu_seconds_min` is 0.0 on every node, so there are no negative deltas. `cpu_seconds_max` is 303-423 CPU-seconds for one 60 s census interval, about 5-7 busy cores for one process on a 256-thread node (`cpu_logical` 256 in the probe hello). That is plausible for a multithreaded user job. `idle_for_max_sec` is 21540 s (5.98 h) on every node. That equals the 6 h session length less the first census, so it is a lower bound capped by probe lifetime and not an absurd value. All three checks are sane.

## Fault checks

Times are UTC on 2026-10-28.

| Check | Expected | Observed | Result |
|---|---|---|---|
| (a) Local SSH kill, login-01, 23:09:56 | session_lost then session_started within about 10 s | session_lost exit 255, backoff 4.56 s, session_started 23:10:01 (about 4.6 s) | PASS |
| (b) Remote probe SIGTERM, login-02, 23:10:22 | error `where`=signal, session_lost exit 143, reconnect | error "terminated by SIGTERM (15)", session_lost exit 143, backoff 3.47 s, session_started 23:10:29 | PASS |
| (c) SIGSTOP session ssh client, login-04, 23:11:00-23:14:55 | session_lost (silence) after about 30 s | No loss. The client was stopped for 3 m 55 s and data kept flowing. | INEFFECTIVE, not a probe defect |
| (c') SIGSTOP the remote probe, login-04, 23:16:41 (controller replacement for c) | session_lost (silence), reconnect, one probe left | session_lost reason silence at 23:17:14.7 (exit 255), backoff 3.9 s, session_started 23:17:18.6. The stopped probe is gone. Exactly one probe, and it matches the lock. | PASS |
| (d) Lock safety, about 23:15:40 | lock pid equals the one precise-pgrep probe on each node | 01: 3012028, 02: 1170921, 04: 2532256, all matching, mode 600 | PASS |

Why (c) is ineffective (probable cause, not verified): with ControlMaster/ControlPersist the session's ssh process is probably a mux client that hands its stdio to the master, so stopping it does not stall the data path. (c) as written cannot test silence detection. (c') freezes the probe itself and exercises the silence timeout.

Fault windows used for coverage: login-01 23:09:56-23:10:02, login-02 23:10:25-23:10:30, login-04 23:16:30-23:17:20.

## Losses not caused by fault checks

None. Each node's `loss_timeline` has exactly one entry, and each entry matches a fault check:

| node | recv (UTC) | reason | exit_code | stderr_tail | cause |
|---|---|---|---|---|---|
| login-01 | 2026-10-28T23:09:56.7 | exit | 255 | empty | check (a) |
| login-02 | 2026-10-28T23:10:25.7 | exit | 143 | empty | check (b) |
| login-04 | 2026-10-28T23:17:14.7 | silence | 255 | empty | check (c') |

The 4 rotations per node (`session_rotated`) were clean and are not losses. `probe_stderr_count` is 0 and `session_crashed_count` is 0 on all nodes. There were no node reboots and no maintenance windows (reboots 0).

## Probe overhead

| node | CPU s/h | max RSS (MB) | scan p95 (s) | census bytes avg | processes avg |
|---|---|---|---|---|---|
| login-01 | 29.8 | 141.0 | 0.48 | 1.23 MB | 1805 |
| login-02 | 26.2 | 164.5 | 0.42 | 1.05 MB | 1523 |
| login-04 | 29.6 | 98.2 | 0.47 | 1.31 MB | 1776 |

GPU metrics are unavailable to non-root users on Polaris login nodes (NVML initialization fails, so `gpu_status` is `error` then `suspended`). The overhead figures therefore exclude successful nvidia-smi calls.

**Collector RSS** (`~/node-monitor/logs/soak-rss.log`, 51 lines: 25 RSS values, the initial 24.0 MB plus 24 hourly samples, and a final timestamp with no value because the collector had already exited): 24.0 MB at start, range 24.0-29.5 MB, 29.5 MB at the last sample. It ratchets up in small steps (24.0, 28.1, 25.9, 29.0, 27.4, 29.5 MB): +5 MB over 26 h, not conclusively flat. Growth of this size is not a concern for the collector, but it was not shown to be zero.

## Probe memory growth (defect, fix early in M1)

`counters.probe_self.maxrss_kb` is `ru_maxrss`, a high-water mark. It climbs within every session and resets at rotation. Probe RSS (MB) at hourly offsets from session start; one row per node and session. S1 and S6 are partial: S1 was ended by the fault checks and S6 was cut off by the end of the run, so neither is representative.

| node | session | 0 h | 1 h | 2 h | 3 h | 4 h | 5 h | end (6 h) | slope MB/h (whole / last 2 h) |
|---|---|---|---|---|---|---|---|---|---|
| login-01 | S2 | 14 | 43 | 60 | 76 | 86 | 96 | 104 | 12.7 / 8.9 |
| login-01 | S3 | 14 | 52 | 78 | 92 | 116 | 136 | 141 | 21.4 / 13.1 |
| login-01 | S4 | 14 | 34 | 65 | 94 | 106 | 123 | 141 | 20.2 / 19.4 |
| login-01 | S5 | 12 | 51 | 65 | 77 | 99 | 116 | 120 | 16.7 / 11.4 |
| login-01 | S6 (1.98 h) | 14 | 60 | | | | | 81 | 32.7 |
| login-02 | S2 | 16 | 51 | 86 | 109 | 124 | 144 | 162 | 22.5 / 21.1 |
| login-02 | S3 | 14 | 53 | 79 | 94 | 105 | 129 | 145 | 18.9 / 19.8 |
| login-02 | S4 | 14 | 49 | 73 | 108 | 132 | 149 | 164 | 25.7 / 17.2 |
| login-02 | S5 | 14 | 50 | 88 | 105 | 128 | 141 | 157 | 21.9 / 12.0 |
| login-02 | S6 (1.97 h) | 16 | 57 | | | | | 86 | 36.1 |
| login-04 | S2 | 14 | 39 | 49 | 49 | 49 | 49 | 49 | 2.0 / 0.0 |
| login-04 | S3 | 14 | 26 | 43 | 66 | 69 | 69 | 71 | 10.1 / 1.4 |
| login-04 | S4 | 14 | 50 | 72 | 83 | 98 | 98 | 98 | 12.1 / 0.0 |
| login-04 | S5 | 14 | 37 | 54 | 54 | 81 | 81 | 81 | 10.7 / 0.0 |
| login-04 | S6 (1.86 h) | 14 | 37 | | | | | 37 | 11.2 |

S1 values are not tabled: they ran 1 s to 8 min and peaked at 18-21 MB.

Findings:

- **Start:** 12-16 MB on every session on every node.
- **Rate:** the first hour adds 12-50 MB. Averaged over a full session, login-01 and login-02 grow 13-26 MB/h. login-04 grows 2-12 MB/h.
- **Plateau:** there is no true plateau. login-01 and login-02 are still growing 9-21 MB/h in the last 2 h, and every one of their sessions ends at its maximum. login-04 grows in steps, with flat stretches of 1-3 h (S2 holds 49 MB from 2 h on). As a high-water mark it only rises, so these are steps and not a decay in the underlying memory.
- **Correlation with census:** none visible. After removing each session's linear trend, the correlation of RSS with `process_count`, census `bytes`, `user_count` and `gpu_status` is within +-0.12 on every node. Raw correlation of RSS with process_count is -0.13 (login-01), 0.07 (login-02) and -0.23 (login-04). The growth is not driven by anything in the census summaries. login-04 is the node that gets the least growth, but its process count is the same as login-01's, so load alone does not explain it.
- **Extrapolation to 200 MB without rotation:** from each session's end RSS, using its whole-session and last-2-h rates, the time to reach 200 MB from session start is about 13.5-17 h for login-01 S2, 9-10.5 h for S3, about 9 h for S4 and 11-13 h for S5, so about 9-17 h on login-01. For login-02 it is about 7.5-10 h across S2-S5 (S2 about 7.7 h). login-04 is undetermined: its tails are flat in 3 of 4 sessions, so a rate cannot be fitted. These are linear extrapolations of a high-water mark, with wide error bars.
- **Cause:** not diagnosed. Hypothesis: state that grows with the number of distinct pids seen over the session, for example per-pid idle tracking or CPU-delta tables not pruned when a pid exits, or allocator fragmentation from building 1-1.4 MB census payloads each minute. Neither was tested.

Assessment of the "max RSS < 200 MB" criterion: it passes (worst 164.5 MB on login-02, 36 MB or 18% headroom), but only because rotation caps the session at 6 h. Without rotation login-02 would pass 200 MB in about 7.5-10 h and login-01 in about 9-17 h (login-04 undetermined). The criterion is meant to prove the probe has a bounded footprint, and it does not: it shows that rotation bounds it. Treat this as a defect with evidence above.

**Does it block GO?** No, in my judgment. The footprint is bounded in practice by a mechanism already in the design (rotation), the 6 h sessions were repeated 4 times per node with similar peaks (141, 145-164, 71-98 MB), and nothing else misbehaved: CPU is half the limit and no session was lost. It would block GO if the 6 h rotation were optional or if headroom were slim, but 18% headroom on a login node with 256 threads is thin enough that I attach conditions: (1) open an M1 task to find and fix the growth first, before adding features that grow the census payload; (2) until it is fixed, keep rotation at 6 h or lower it to 4 h (about 132 MB worst case here); (3) add an RSS self-limit in the probe so it exits and restarts when it crosses a threshold such as 150 MB, instead of relying on the clock.

## Required statements

- GPU metrics are unavailable to non-root users on Polaris login nodes (NVML initialization fails, so `gpu_status` is `error` then `suspended`), so the overhead figures exclude successful nvidia-smi calls. The `gpu` errors (156-158 per node) are this failure and not a probe fault.
- SIGKILL terminations are unattributable: only SIGTERM, SIGHUP and SIGINT leave an `error` with `where` "signal". Check (b) used SIGTERM and left one, as expected on login-02.
- Simultaneous silence losses on every node point to a collector-side stall (for example /home I/O), not to the nodes. This run had none: the one silence loss is the deliberate login-04 check (c').
- `coverage.parent_unresolved` is always at least 1, because pid 1's parent is pid 0, which has no /proc entry.

## GO criteria (spec section 13, M0)

| Criterion | Result | Verdict |
|---|---|---|
| Coverage >= 99% outside fault windows | 100.006 / 100.006 / 100.002% | PASS |
| No unexplained probe terminations | 3 losses, all deliberate fault checks | PASS |
| Reconnects within backoff limits | 3.5-4.6 s | PASS |
| Probe CPU < 60 s/h per node | 26.2-29.8 | PASS |
| Max RSS < 200 MB | 98-165 MB, with growth until rotation | PASS, with defect |

Not covered by this soak: the silence timeout was exercised only by (c'), once, on login-04. Silence detection through the mux client path (check c) is untested. No node reboot occurred, so reboot detection was not exercised live.
