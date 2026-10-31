# M1a follow-ups for M1b/M1c

Deferred minor findings, parked items and notes from the M1a task reviews, final review and re-soak (branch feat/m1a-pipeline). Results: docs/soak/2026-11-m1a-resoak.md (PASS).

## Notes for the M1b plan

- `live_node` records are partial updates: a missing key means "unchanged". The writer must upsert without nulling the last-known gauges.
- `node_hardware` is re-emitted on every session's `hello`. Upsert on (system, node, boot_id) and keep the first `collected_at`.
- `username` values of the form `uid:<N>` are synthetic keys for owners the probe could not resolve, not real accounts.
- GPU fields are None when unmeasured (gpu_status != ok); on Polaris they are always unmeasured for non-root users.
- Redacted command lines still contain home-directory paths, and with them usernames. Usernames are stored deliberately (spec §1), so this is expected.

## Parked (real, deferred)

- [Final review] redaction over-masks non-secret values: bare `key` short keyword (--sort-key name, --key path, openssl -key file.pem) and -D build defines (cmake -DENABLE_AUTH=ON, gcc -DUSE_TOKEN=1); sshpass -pSecret masks the next word — Ruling: real, fails safe (no secret leaks), deferred to an M1b redaction follow-up (drop bare `key`, restrict -D to Java property forms / known secret names, allowlist) — cost if wrong: some non-secret CLI detail masked in live rows until fixed. The bare-`key` keyword came from the controller's findings file.
- [Final review] keytool -storepass/-keypass and openssl -passout pass:x not masked (pre-existing) — Ruling: add in the M1b redaction follow-up — cost if wrong: those secrets appear in live rows until then.

## Deferred minors

- [Task 1] a signal arriving in the µs gap after `bye` can add a signal error line + exit 143 after bye — Task 2 session must not assume bye is last / exit 0
- [Task 1] EMIT_CHUNK_BYTES comment overstates the bound (a single huge cmdline piece can exceed it)
- [Task 1] probe doesn't validate probe_rss_limit_mb (0/negative → bye loop) — must be validated in config (Task 2)
- [Task 1] while_waiting grace test uses a fake signal via _on_signal
- [Task 1] test_own_rss_is_none_when_nothing_is_readable relies on chmod(0) — fails if suite runs as root; add skip for uid 0
- [Task 2] `.inf` intervals pass _positive and crash summary divisibility with OverflowError — add math.isfinite
- [Task 2] bool/NaN rejection untested for active_cpu_seconds_per_min and connect_timeout_sec; reconnect_backoff_sec entries not type-checked (pre-existing)
- [Task 2] no isolated test for bye followed by nonzero exit without an error line
- [Task 2] no test at uptime strictly between 0 and healthy_after / equality boundary; "growing delay" test asserts failure counts only
- [Task 3] rules.py docstring says "never list position" but ties go to the earlier rule
- [Task 4] multi-word Authorization header value only masked to the first space
- [Task 4] KEY=value rule masks PWD=/x (env working dir, not a password) because pwd is a keyword; quoted -u 'bob:a b' not masked
- [Task 4] `sudo mysql -pX` not masked (argv0-basename check only)
- [Task 5] no test proving classification uses the unredacted cmdline (a secret-bearing classifying token)
- [Task 5] sshd regex untested for pts/NN, [net] forms; trailing space rejected by $ anchor (strip before match)
- [Task 5] no test for idle_bucket(idle, None) → unknown or float boundaries
- [Task 6] malformed counters (missing rx_bytes, None probe values, non-dict ops) can raise
- [Task 6] _parse_ts strict (no fraction / nanoseconds → dropped from offset median)
- [Task 6] one Lustre target reset skips the whole filesystem for that pair
- [Task 6] _census can grow unbounded if censuses arrive with no counters (no open minute)
- [Task 6] no single-pair zero-elapsed test (no-crash path for a minute holding only that pair)
- [Task 7] map/fraction/gpu/threads averaging tested only at samples == 1 (wrong denominator in cell() would pass); add a multi-census case
- [Task 7] _CensusTally inherits unused fold/row (composition cleaner); late-census fold untested
- [Task 8] full census whose minute is closed but interval open isn't late → its gpu/scan metrics silently dropped by MinuteAggregator (no event)
- [Task 8] reconnect hello re-emits NODE_HARDWARE each session (intended; untested)
- [Task 9a] label glob `{label}-*.jsonl` collides for labels like gpu / gpu-a (use -????????.jsonl)
- [Task 9a] interval override untested; non-UTC-offset recv not normalized/tested
- [Task 9a] records sink os.open outside try (traceback on bad path); broad KeyError/ValueError catch may mask pipeline bugs; node with no files prints bare line; skip message on stdout
- [Task 9b] results doc says counters disagree "up to 6 MiB" (two pairs ~6.6–6.7); login-04 S2 "only because" step in hour 3 slightly overstated

## Rulings made during execution

- Ruling: branch feat/m1a-pipeline from local main with PR #6's fix commit cherry-picked (eb5543c) instead of waiting for the user to merge PR #6 — merging is the user's decision; Task 2 edits session.py which the fix also touches; a rebase onto main after PR #6 merges drops the duplicate patch — cost if wrong: one rebase conflict in session.py.
- Ruling R1: Task 3 CASES — expected for `("bash", "/home/u/scripts/my_windsurf_notes.sh", 31112)` is `("other", None)`, not `("shell", "shell")` — v1's rules (verified) give other/None because the cmdline does not start with a shell name; the case exists to prove the windsurf lookalike does not match — cost if wrong: one test expectation.
- Ruling R2: NodePipeline treats a `census` message without a `processes` key (soak summary mode) as metrics-only: it feeds MinuteAggregator.add_census (gpus, scan_seconds) and emits no live_processes/usage records — needed for Task 9 replay of default-mode soak data — cost if wrong: replay counts differ.
- Ruling: accept the adjusted peak-allocation test (subtracting what the BytesIO sink retains) — the plan's version measured the test's own buffer, not the emitter — cost if wrong: none.
- Ruling: the RSS self-limit reads exact `Rss:` from /proc/self/smaps_rollup (fallback VmRSS when absent), and counters' probe_self gains `rss_kb` (exact, same source) so the Task 9 re-soak judges real memory — VmRSS/ru_maxrss are approximate per-CPU counters on kernel 6.4 with 256 CPUs and can drift tens of MiB — cost if wrong: one extra small /proc read per census/counters.
- Ruling: a bye rotation whose session uptime < healthy_after counts as a failure for backoff (reconnect_wait + delay) while still reported as session_rotated; bye after ≥ healthy_after and max-lifetime rotations reconnect immediately — spec §4 only requires the EOF after bye be a rotation, not a loss; plan's "no backoff" would hammer SSH on a node whose baseline RSS exceeds the limit — cost if wrong: slower recovery after a legitimately early bye.
- Ruling: accept 42 activity rules (verbatim port; plan's 41 was a miscount) — cost if wrong: none.
- Ruling: accept category precedence = a CATEGORY_RULES match with strictly higher priority than the matched activity overrides the activity's category (activity label kept), e.g. `python -m claude_code.cli` → (ai_agent, python-generic) — v1 computed category independently and its tuned tests require it; plan's "activity category first" broke a v1 expectation — cost if wrong: a few processes' category differs from the activity's mapped category.
- Ruling: mask an option/KEY value only when the name's LAST segment is a secret keyword (add pass, passphrase, pat), never for names starting no-/no_ — owner wants non-secret CLI detail kept; spec §5.2 masks values of options "named like" secrets — cost if wrong: an option like --foo-token-dir with a secret value leaks (unlikely).
- Ruling: short-option secrets only in unambiguous forms: `-u/--user user:pass` (mask after colon) and `-p<attached>` for mysql/mariadb/mysqldump/mysqladmin; Bearer outside Authorization masks only ≥16-char tokens — a blanket -p rule would mangle mkdir -p / ssh -p — cost if wrong: other tools' short-option secrets leak (e.g. docker login -p value).
- Ruling: idle_bucket returns "unknown" when cpu_state is set but idle_for_sec is None — unspecified edge; unknown is the honest value — cost if wrong: none.
- Ruling: emit node_usage_5m for every interval with ≥1 census even with zero user processes — a measured zero is data, distinct from a gap (sample_count shows coverage) — cost if wrong: a few all-zero rows.
- Ruling: split Task 9 — 9a (replay code, Steps 1–5) → final whole-branch review + fix wave → 9b (live full-census capture/replay, 12 h re-soak, results doc, Steps 6–8) — the re-soak must exercise the code being kept (M0 precedent) — cost if wrong: none.
- Ruling: replay uses the manifest's counter/census intervals over the config's — the capture's actual cadence must drive bucketing and expected_count — cost if wrong: none.
- Ruling: fix wave includes I1–I4, minors M1–M5 and M7–M9, and the three FIX-BEFORE-M1B items; M6 (encoder CPU cost) is documented in the plan's Step 8 note instead of changed — the chunked encoder is the memory fix; ~0.13 CPU-s/min extra is acceptable vs the 60 s/h limit — cost if wrong: re-soak CPU ~25–30% above M0.
- Ruling: unresolved owners get username "uid:<uid>" (stable per-uid key) instead of None — keeps the (system,node,username,interval) key unique and never merges users — cost if wrong: M1b sees synthetic "uid:N" names it must not treat as real accounts.
- Ruling: GPU memory is None (unmeasured) unless the census gpu_status is "ok" — a measured zero is data, an unmeasured value is a gap (Task 7 principle) — cost if wrong: none.
