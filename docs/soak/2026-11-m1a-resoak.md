# M1a re-soak and pipeline replay results

## Verdict: PASS - M0 GO condition "fix probe memory growth" is met

- Exact probe RSS (`probe_self.rss_kb`) plateaus in all 6 full sessions. The last hour is 0.0-4.65 MiB above the second hour (limit 10 MiB).
- Max exact RSS is 25.8 MiB (limit 64 MiB). M0's `maxrss` peaked at 164.5 MiB.
- `rotations_by_reason` is `{"max_lifetime": 1}` per node. There is no `rss_limit`, and `bye_count` is 0.
- Coverage is 100.0% on all nodes, with 0 losses.
- Probe CPU rose 17-20% against M0, which is below the expected 25-30% and is not a regression.

## Run

| Item | Value |
|---|---|
| Window | 2026-10-30T20:09:54Z to 2026-10-31T08:09:45Z (12 h, collector exited normally) |
| Hosts | polaris-login-01, -02, -04 (collector on -04) |
| git_describe | `v0.1.0-alpha.1-20-g43950f5` (commit `43950f59b04a949bad220c5ec9eeaa8681073ae6`, not dirty) |
| probe_sha256 | `4e97c75bec2d4e2205ecee8db6bd5e69505b55cea4d90a1c25334c4a7eb2d663` |
| Mode | default summary, counters 10 s, census 60 s, 6 h rotation, no fault checks |
| Data | `~/node-monitor/logs/soak/20261030T200954Z` (about 325 MB, plus `report.json`) |

Units: "MiB" is kB/1024. "MB" in the report table also means MiB.

## Report table (verbatim)

```
node      hours  coverage%  census%  errors  max_gap_s  sessions  losses  rotations  reboots  cpu_s/h  rss_MB  maxrss_MB  scan_p95_s  procs
login-01  12.00      100.0    100.0      72         10         2       0          1        0     35.4    20.7       14.0        0.50   1739
login-02  12.00      100.0    100.0      71         10         2       0          1        0     31.5    20.0       14.0        0.46   1601
login-04  12.00      100.0    100.0      72         10         2       0          1        0     34.6    25.8       26.6        0.50   1820
```

## Probe memory (pass metric)

Exact RSS (`probe_self.rss_kb`, from smaps_rollup), MiB, at hourly offsets from session start. Each node ran two full 6 h sessions, with no short first session. The "2nd hr" column is the maximum between 1 h and 2 h. The "last hr" column is the maximum over the final hour. Delta is last hr minus 2nd hr.

| node | session | 0 h | 1 h | 2 h | 3 h | 4 h | 5 h | end | max | 2nd hr | last hr | delta | limit |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| login-01 | S1 | 16.2 | 20.6 | 20.6 | 20.7 | 20.7 | 20.7 | 20.7 | 20.7 | 20.65 | 20.70 | +0.05 | PASS |
| login-01 | S2 | 16.1 | 20.2 | 20.2 | 20.2 | 20.3 | 20.3 | 20.3 | 20.3 | 20.23 | 20.30 | +0.07 | PASS |
| login-02 | S1 | 16.1 | 19.9 | 19.9 | 20.0 | 20.0 | 20.0 | 20.0 | 20.0 | 19.92 | 20.00 | +0.09 | PASS |
| login-02 | S2 | 16.2 | 19.6 | 19.6 | 20.0 | 20.0 | 20.0 | 20.0 | 20.0 | 19.61 | 19.96 | +0.36 | PASS |
| login-04 | S1 | 16.1 | 20.4 | 25.8 | 25.8 | 25.8 | 25.8 | 25.6 | 25.8 | 25.84 | 25.84 | 0.00 | PASS |
| login-04 | S2 | 16.1 | 20.6 | 19.9 | 25.3 | 25.3 | 25.3 | 25.3 | 25.3 | 20.63 | 25.28 | +4.65 | PASS |

Shape of the curve:

- Every session starts at 16.1-16.6 MiB, steps to 19.6-21.1 MiB in the first hour, and then is flat or nearly so.
- login-01 and login-02 add only 0.07-0.4 MiB between 1 h and 6 h.
- login-04 has one or two step rises of about 5 MiB (S1 at about 1-2 h, S2 at about 2-3 h), then is flat. It adds no more after the step. login-04 S2 passes with 4.65 of the 10 MiB allowance only because its step falls in hour 3, so the criterion is sensitive to when a step lands. These look like one-off allocations (such as a first large census) rather than growth. Not diagnosed.
- Total growth from 1 h to the end of a session is 0.1-0.4 MiB on login-01/-02 and 4.7-5.4 MiB on login-04. M0's `maxrss` grew about 60-110 MiB (login-01/-02) and about 10-60 MiB (login-04) from 1 h to session end.

## Secondary: `maxrss_kb` (`ru_maxrss`) against M0

| node | session | 0 h | 1 h | 2 h | 3 h | end | max | M0 end (S2-S5) | M0 max |
|---|---|---|---|---|---|---|---|---|---|
| login-01 | S1 | 14.0 | 14.0 | 14.0 | 14.0 | 14.0 | 14.0 | 104-141 | 141.0 |
| login-01 | S2 | 14.0 | 14.0 | 14.0 | 14.0 | 14.0 | 14.0 | 104-141 | 141.0 |
| login-02 | S1 | 14.0 | 14.0 | 14.0 | 14.0 | 14.0 | 14.0 | 145-164 | 164.5 |
| login-02 | S2 | 14.0 | 14.0 | 14.0 | 14.0 | 14.0 | 14.0 | 145-164 | 164.5 |
| login-04 | S1 | 14.0 | 23.4 | 26.6 | 26.6 | 26.6 | 26.6 | 49-98 | 98.2 |
| login-04 | S2 | 14.0 | 14.0 | 14.0 | 25.5 | 25.5 | 25.5 | 49-98 | 98.2 |

Counter drift, in plain terms:

- M0 recorded only `maxrss`. It has no exact RSS, so M0's growth cannot be split between real growth and counter drift. The plan notes these counters can drift tens of MiB on this 256-CPU kernel.
- In this run the two counters disagree by up to 6 MiB. On login-01 and -02 `maxrss` sits at its 14.0 MiB start value while exact RSS is 20-21 MiB, so it under-reads by about 6 MiB. On login-04 it matches the exact value to within 1 MiB at session end (it differed more mid-session, for example 23.4 against 20.4 MiB at 1 h in S1).
- That gap comes from the fixed probe, whose footprint is flat, so it does not bound drift in M0's growing-footprint probe.
- The evidence that the fix works is the exact-RSS plateau of the fixed probe above, not a comparison of the two counters.

## Rotations, coverage and losses

| node | coverage % | census cov % | max gap (s) | losses | rotations_by_reason | bye_count | sessions | errors (gpu) | stderr lines | crashes | malformed |
|---|---|---|---|---|---|---|---|---|---|---|---|
| login-01 | 100.02 | 100.02 | 10.3 | 0 | `max_lifetime`: 1 | 0 | 2 | 72 | 0 | 0 | 0 |
| login-02 | 100.0 | 100.0 | 10 | 0 | `max_lifetime`: 1 | 0 | 2 | 71 | 0 | 0 | 0 |
| login-04 | 100.0 | 100.0 | 10 | 0 | `max_lifetime`: 1 | 0 | 2 | 72 | 0 | 0 | 0 |

- All nodes rotated at 02:09:54Z, 6 h after start. Each new session started within 0.02 s.
- There were no losses, so no exit codes to report. `loss_timeline` is empty on every node.
- `bye_count` is 0 because the probe only says `bye` for `rss_limit`, which never triggered.
- The `gpu` errors are the known NVML failure for non-root users on the login nodes, as in M0.
- There were no reboots. No fault checks were run, so reconnect and silence handling were not exercised here (M0 covered them).

## Probe CPU and collector

| node | CPU s/h, M0 | CPU s/h, re-soak | change |
|---|---|---|---|
| login-01 | 29.8 | 35.4 | +18.8% |
| login-02 | 26.2 | 31.5 | +20.2% |
| login-04 | 29.6 | 34.6 | +16.9% |

The rise is 17-20% (19% on the pooled sum), a little under the expected 25-30% from the chunked streaming encoder. The cause is not isolated: process count (1601-1820) and census size differ a bit between runs. It fits the expectation (no regression) and is far below the 60 s/h limit. Scan p95 is 0.46-0.50 s (M0 0.42-0.48).

Collector RSS (`~/node-monitor/logs/soak-rss.log`, run lines only): 28.0 MiB at +90 s, 29.7 MiB at +1 h, then 29.9 MiB (30588 kB) for the next seven hourly samples, then 27.5 MiB (28176 kB) from 05:10Z to the end. The final sample after exit was blank. The range is 27.5-29.9 MiB and it ends 0.5 MiB below its start, so it is flat. M0 ratcheted from 24.0 to 29.5 MiB.

## Pipeline replay spot-checks (Task 9 Step 6)

Capture `20261030T195301Z` (0.25 h, full census, HEAD 43950f5, clean tree). `soak replay` ran cleanly. The capture dir and the records file were deleted afterwards, because they held unredacted command lines.

| Record kind | login-01 | login-02 | login-04 |
|---|---|---|---|
| collection_event | 5 | 5 | 5 |
| node_hardware | 1 | 1 | 1 |
| live_node | 90 | 90 | 90 |
| live_processes | 15 | 15 | 15 |
| node_metrics_1m | 15 | 15 | 15 |
| node_usage_5m | 4 | 4 | 4 |
| user_usage_5m | 924 | 881 | 960 |

`node_usage_5m` is 4 and not 3 because the window crossed four 5-minute boundaries. There were 220-240 user rows per 5-minute bucket per node, and 364 distinct users in `live_processes`. `probe_rss_kb` is populated in 45 of 45 `node_metrics_1m` rows. `gpu_status` shows suspended 33 and error 12, as expected for non-root.

Process categories (80,744 process rows):

| category | rows | category | rows |
|---|---|---|---|
| editor_server | 32315 | build | 1050 |
| shell | 18359 | workflow | 1009 |
| other | 8182 | scheduler | 414 |
| system | 7886 | data_transfer | 120 |
| python | 5996 | jupyter | 30 |
| ai_agent | 5378 | fs_scan | 5 |

| Check | Result |
|---|---|
| `activity: null` share of user processes | 7357 of 72858 = 10.10% (target under 15%, v1 9%): PASS |
| `owner_presence` | absent 58674, connected 10743, interactive 11327 |
| `idle_bucket` | unknown 7327, under 5 min 33943, 5 min to 1 h 39474 |
| Top activities | vscode-remote-server 31138, shell 10662, python-generic 5441, ssh-session 2908, claude-code 2697, codex-cli 2546, sleep 2250 |

Three random redacted command lines (cut at 200 characters):

1. `/home/mabdallah/.cursor-server/bin/linux-x64/f0df...67c080/node /home/mabdallah/.cursor-server/bin/linux-x64/f0df...67c080/out/bootstrap-fork --ty`
2. `-bash`
3. `/home/akum/.vscode-server/code-645f29cc... --cli-data-dir /home/akum/.vscode-server/cli agent host`

Findings for follow-up:

- "Redacted" command lines still contain home-directory paths with usernames (samples 1 and 3). Decide whether that is acceptable before M1 stores them.
- Six distinct usernames are unresolved uids (`uid:39232`, `uid:41086`, `uid:42898`, `uid:36454`, `uid:36088`, `uid:42890`), on 210 process rows and in `user_usage_5m`. They are probably missing from the login-node passwd/nss view.

## M0 GO condition

| Condition | Result |
|---|---|
| Fix probe memory growth: exact RSS plateaus, last hour within 10 MiB of 2nd hour, max under 64 MiB | PASS: max delta +4.65 MiB, max 25.8 MiB |
| No `rss_limit` rotation | PASS: only `max_lifetime` |
| Coverage at least 99% | PASS: 100.0% |
| Probe CPU under 60 s/h | PASS: 31.5-35.4 |

The M0 condition to keep the 6 h rotation until the fix lands can now be reviewed. This soak covered only 6 h sessions, so it does not show that a longer session would stay flat.
