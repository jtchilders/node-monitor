# M0 follow-ups for M1

Deferred minor findings and parked items from the M0 task reviews and final review (branch feat/m0-streaming-probe). Triage for M1 planning; the final review marked all of these DEFER-TO-M1 except the two it fixed before the soak.

## Parked (real, deferred)

- emit() lock has no timeout: signal pending + stalled reader + watchdog holding lock can block both threads past the 5 s grace — Ruling: real but bounded (EPIPE on transport teardown; new probe's takeover SIGKILL after 5 s); defer to M1 (acquire with timeout when _signum set) — cost if wrong: a wedged probe lingers until its ssh dies.
- test_signal_during_blocked_census_write_does_not_split_the_line timing-sensitive (0.7 s) — Ruling: test-only flake risk; defer to M1 — cost if wrong: occasional CI flake.

## Must-do in M1 (from the soak, docs/soak/2026-10-m0-results.md)

- Probe memory grows 2–26 MiB/h within a session (no plateau on login-01/02); fix before relying on rotation; keep session_max_lifetime_sec ≤ 6 h (4 h if unfixed); add an RSS self-limit.
- GPU metrics unavailable to non-root on Polaris login nodes (NVML init fails); decide whether GPU collection stays in the probe.
- Plan Task 8 check (c) is ineffective under ControlMaster; use the remote-probe SIGSTOP check (c′) in future soak procedures.

## Deferred minors

- [Task 1] config numeric fields not type-checked (string/null backoff → raw TypeError, not ConfigError)
- [Task 1] config read OSError/UnicodeDecodeError not wrapped in ConfigError
- [Task 1] test_example_config_loads unused tmp_path; asserts real-$HOME-dependent path
- [Task 1] version 0.1.0 duplicated in pyproject.toml and __init__.py (plan-mandated)
- [Task 1] ruff still includes root README.md (only docs/ excluded) — a future ```python block in README would be reformatted; consider "*.md" in extend-exclude
- [Task 2] fake_proc.py imports shutil inside a loop
- [Task 2] no missing-file tolerance test for collect_hardware/boot_identity
- [Task 2] hardware GPU fallback calls host nvidia-smi in tests when fake tree lacks /proc/driver/nvidia (env-dependent)
- [Task 2] broad except around read_text in net_ifaces lacks a why-comment
- [Task 3] ProcessTracker later sighting with interval_sec=None → threshold 0 → all "active"
- [Task 3] unreadable cmdline (zombies) counted as kernel_thread in coverage
- [Task 3] ppid 0 (pid 1's parent) triggers /proc/0 read and inflates parent_unresolved
- [Task 3] no tests for stat_unreadable/stat_unparseable/vanished coverage paths
- [Task 3] compute-apps query failure discards valid gpus data (matches brief)
- [Task 3] shared-budget GPU test margin small (2.5 s bound vs ~2.0 s), may flake under load
- [Task 4] test_nonblocking_stdin_does_not_end_probe cannot fail (select never reports empty pipe readable)
- [Task 4] main-loop parent-change `return 0` untested in isolation (watchdog exits first)
- [Task 4] empty cmdline of mid-exit holder treated as non-probe → new probe exits 3 instead of waiting (self-heals via session backoff; watch in soak after rotations)
- [Task 4] stale pid in lock file between flock and rewrite (narrow; comment)
- [Task 4] abandon/reap GPU paths not exercised end-to-end
- [Task 4] fd-less emit path doesn't catch ValueError on closed file (not production)
- [Task 4] lock file not hardened (no O_NOFOLLOW/ownership check); another user can pre-create & hold it → exit 3
- [Task 4] suspended GPU queries indistinguishable from no GPUs in census (no gpu_suspended flag)
- [Task 4] interval ≤ 0 busy-loops (collector validates config)
- [Task 4] nvidia-smi exits 255 on polaris-login-04 → gpu error every census (~1.4k/day); consider suspending on any GPU failure, not only timeouts (M1)
- [Task 5] e2e bootstrap test reads only hello before closing stdin — doesn't prove streaming continues with stdin open (plan-inherited)
- [Task 5] ssh_config paths unquoted (spaces would break); no socket-path length check
- [Task 5] ConnectTimeout {:g} on fractional value rejected by ssh
- [Task 5] remote cmd with newlines requires bash/sh login shell (breaks under tcsh) — comment it
- [Task 5] no test asserting BOOTSTRAP_CODE starts with the marker line
- [Task 6] callback exception propagates out of run() (ends node monitoring) — undocumented in class docstring
- [Task 6] on_event raising at session_started (emitted before try) leaks the spawned process group
- [Task 6] raising probe_stderr callback kills the stderr drainer silently (Review Focus #2 via callback bug)
- [Task 6] oversize-line malformed_line event has bytes=STREAM_LIMIT, empty sample, plus a second event for the fragment; no test
- [Task 6] no tests for cancelling run() or callback-exception cleanup
- [Task 6] brief's 2 MiB stderr flood test passes trivially (plan-mandated; covered by 80 MiB test)
- [Task 7] coverage fencepost (expected=span/interval; received includes both ends) → slightly >100%
- [Task 7] summarize omits manifest nodes that wrote no lines (should appear with 0 coverage)
- [Task 7] recorder _shape outside the try → malformed census entry could raise into a session
- [Task 7] summarize sorts by recv string; non-dict/missing-recv line raises TypeError/KeyError
- [Task 7] test gaps — date rollover, truncated last line, supervise-crash, signal path, lock suffix, session/rotation/reboot counts; e2e timing mildly flaky
- [Task 7] run_soak returns 0 even if all sessions crashed; second Ctrl-C ignored during stop
- [Final review] make_private_dir chmods an explicitly named existing --out-dir to 0700
- [Final review] Task 8 fault (d) hard-codes uid 31112 in lock path (correct for this user)
- [Task 8] results doc line 87 mixes kB/1000 and kB/1024 for collector RSS steps (top is 28.8 MiB, not 29.5)
- [Task 8] results doc line 119 reason for login-04 "undetermined" contradicts its own slope table
- [Task 8] results doc line 57/61 still state the mux premise unhedged ("cannot test silence detection")
- [Task 8] collector RSS first sample is at +1 h; 24 hourly values span 24 h not 26 h; 99.977 vs 99.978 rounding
