# M1a — Probe Memory Fix and Data Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the probe's per-session RSS growth, then build the pure, in-memory pipeline that turns probe messages into the records M1b will store: classified, redacted, dormancy-annotated live process rows, per-minute node metrics, and 5-minute per-user and per-node usage summaries.

**Architecture:** The probe streams each JSON message in bounded chunks (no whole-line buffers) and exits with a `bye` message if its RSS ever exceeds a limit; the session treats that as a rotation. A new `node_monitor/pipeline/` package holds pure transforms — classification rules ported from v1, value-only redaction, census enrichment, a minute aggregator for counters, a 5-minute usage aggregator — behind one per-node facade, `NodePipeline`, that maps `(message | session event, receive time)` to a list of `Record`s. No database or daemon code in this plan.

**Tech Stack:** Python ≥ 3.10 (collector, venv `~/node-monitor/venv`), Python ≥ 3.6 stdlib (probe), pytest, ruff.

**Spec:** `docs/superpowers/specs/2026-10-28-node-monitor-v2-design.md` — §3 config, §4 probe protocol (incl. `bye`), §5 classification/redaction/dormancy, §6 table contents, §13 M1a. Inputs: `docs/soak/2026-10-m0-results.md` (GO conditions), `docs/m0-followups.md`.

## Global Constraints

- Repo `~/node-monitor/node-monitor/`; branch `feat/m1a-pipeline` from `main` **after PR #6 (`fix/py310-swallowed-cancel`) is merged** (Task 2 edits `session.py`, which PR #6 also changes).
- 3-space indentation; ruff `indent-width = 3`, `line-length = 100`; `ruff check . && ruff format --check .` pass; never format `docs/`.
- `node_monitor/probe/probe.py` stays Python 3.6 + stdlib: no f-strings, no `capture_output`/`text=`, no walrus, no dataclasses. Probe tests also pass with `NODE_MONITOR_PROBE_PYTHON=/usr/bin/python3.6`.
- Collector code runs on Python 3.10 and 3.13 (CI matrix); never use `asyncio.wait_for` (use `session._within`) and no 3.11+-only APIs.
- The probe never classifies or redacts. Everything in `node_monitor/pipeline/` is pure: no I/O, no clocks (time is always passed in), no asyncio.
- Spec values: categories exactly `ai_agent, editor_server, jupyter, build, python, shell, data_transfer, fs_scan, scheduler, workflow, system, other`; idle buckets `<5m`, `5m-1h`, `1h-1d`, `>1d` (plus `unknown` when not yet measured); owner presence `interactive`, `connected`, `absent`; dormancy key `"<cpu_state>|<idle_bucket>|<owner_presence>"` with `cpu_state` ∈ `active`, `idle`, `unknown`; a *user process* has `uid >= min_user_uid` (default 1000) and `category != "system"`.
- Intervals are bucketed on the collector's receive time (UTC), aligned to the wall clock: minutes for `node_metrics_1m`, `summary_interval_sec` (default 300) for usage.
- Every summary row carries `sample_count` and `expected_count`.
- Tags (`by_tag`), `process_samples`, `command_lines` and the hourly census are M3 — do not build them.
- Comments explain why; no task IDs or history in code; tests named by component.
- Commit after each task with a conventional-commit message ending `Co-Authored-By: Claude Code <noreply@anthropic.com>`. Do not push.
- Run live commands with `env -u LD_PRELOAD` (site XALT preload aborts Python at exit).

## Review Focus

1. **Cumulative counters going backwards** (node reboot, new probe session, interface reset, Lustre target remount) — must yield no negative or huge rates; that sample pair is skipped and the next pair is a new baseline. Test: Task 6 `test_counter_reset_skips_pair_without_negative_rates`.
2. **A user who appears in only some censuses of an interval** — their averages count the censuses where they had no processes as 0, rather than averaging only the censuses they appear in; users absent for the whole interval get no row. Test: Task 7 `test_user_absent_from_some_censuses_averages_zero`.
3. **Secrets in both `--opt=value` and `--opt value` forms, plus `KEY=value` and URLs** — masked; flags and option names untouched; non-ASCII and 100 KB command lines don't crash or slow down catastrophically. Test: Task 4 `test_redacts_both_option_forms_and_keeps_flags`, `test_long_and_non_ascii_cmdlines`.
4. **A census arriving after its interval was already emitted** (session reconnect replaying a late census, collector clock stepped backwards) — never emits a duplicate or earlier interval row; the late census is dropped and reported as a `late_sample` event. Test: Task 8 `test_late_census_for_closed_interval_is_dropped_with_event`.
5. **Owner presence from mixed sshd forms** — `sshd: alice [priv]` (root-owned) must not count; `sshd: alice@pts/3` → `interactive`; `sshd: alice@notty` only → `connected`; both → `interactive`; usernames containing `.`/`-`. Test: Task 5 `test_owner_presence_forms`.

---

## File Structure

```
node_monitor/probe/probe.py           MODIFY: chunked streaming emitter, RSS self-limit, bye
node_monitor/config.py                MODIFY: min_user_uid, summary_interval_sec, probe_rss_limit_mb,
                                              redaction.extra_patterns, numeric validation
node_monitor/collector/bootstrap.py   MODIFY: probe_config passes probe_rss_limit_mb
node_monitor/collector/session.py     MODIFY: `bye` → rotation
node_monitor/pipeline/__init__.py
node_monitor/pipeline/rules.py        activity/category rule tables (data, ported from v1)
node_monitor/pipeline/classify.py     classify()
node_monitor/pipeline/redact.py       Redactor
node_monitor/pipeline/enrich.py       owner presence, idle bucket, live process rows
node_monitor/pipeline/node_metrics.py MinuteAggregator (counters → node_metrics_1m)
node_monitor/pipeline/usage.py        UsageAggregator (enriched censuses → *_usage_5m)
node_monitor/pipeline/records.py      Record type + kind constants
node_monitor/pipeline/node_pipeline.py NodePipeline facade
node_monitor/cli/main.py              MODIFY: `soak replay`
tests/probe/test_emitter_memory.py
tests/pipeline/__init__.py, test_classify.py, test_redact.py, test_enrich.py,
tests/pipeline/test_node_metrics.py, test_usage.py, test_node_pipeline.py
tests/collector/test_session.py       MODIFY: bye test
tests/test_config.py                  MODIFY
docs/soak/2026-11-m1a-resoak.md       written in Task 9
```

---

### Task 1: Probe — chunked streaming emitter and RSS self-limit

Root cause (measured during planning on polaris-login-04, 150–300 census cycles in-process): scanning `/proc` alone and the dormancy tracker alone are flat; building each census as one JSON string reproduces the growth (18 → 31 MiB); the Python heap stays flat (0.8 MiB) throughout, and neither `MALLOC_MMAP_THRESHOLD_` (worse: 15 → 85 MiB) nor `malloc_trim(0)` fixes it. The emitter makes three full-size copies per census line (`json.dumps`, `+ "\n"`, `.encode`), 1–5 MB each and a different size every minute, so glibc's heap high-water mark keeps rising.

**Files:**
- Modify: `node_monitor/probe/probe.py` (`_Emitter.emit`, `_write_all`, `stream`, config defaults)
- Test: `tests/probe/test_emitter_memory.py`, `tests/probe/test_stream.py` (add tests)

**Interfaces:**
- Produces:
  - `EMIT_CHUNK_BYTES = 65536` — the most bytes of one message held as encoded output at once.
  - `_Emitter.emit(kind, fields)` keeps its signature and envelope (`type`, `seq`, `ts`, compact separators, one line, flushed). It encodes with `json.JSONEncoder(separators=(",", ":")).iterencode(msg)`, accumulates pieces up to `EMIT_CHUNK_BYTES`, ASCII-encodes and writes each chunk via `_write_all` while holding the lock, and finishes with `b"\n"`. Output bytes are identical to `json.dumps(msg, separators=(",", ":")) + "\n"`.
  - PROBE_CONFIG key `probe_rss_limit_mb` (default 128). After each census, `stream()` reads its own `VmRSS` from `/proc/self/status`. If it exceeds the limit, it emits `{"type": "bye", "reason": "rss_limit", "rss_kb": <int>}` and returns 0.
  - The emitter's signal-report path acquires the lock with `acquire(timeout=SIGNAL_REPORT_GRACE_SEC)` and exits with 128+signum unreported if that times out. This closes the parked follow-up "emit() lock has no timeout".

- [ ] **Step 1: Write the failing tests**

```python
def test_census_emission_peak_allocation_is_bounded(probe):
   # 2,000 processes with 1 KiB command lines -> a ~2.5 MB line
   rows = [dict(fake_row(pid=i), cmdline="x" * 1024) for i in range(2000)]
   out = io.BytesIO()                                # fd-less path
   em = probe._Emitter(out)
   tracemalloc.start(); tracemalloc.reset_peak()
   base = tracemalloc.get_traced_memory()[0]
   em.emit("census", {"processes": rows})
   peak = tracemalloc.get_traced_memory()[1] - base
   tracemalloc.stop()
   line = out.getvalue()
   assert json.loads(line)["processes"][1999]["pid"] == 1999
   assert peak < 1024 * 1024                         # was > 3 x line size before the fix

def test_streamed_bytes_match_compact_json(probe):
   msg_fields = {"processes": [fake_row(pid=1), fake_row(pid=2, cmdline="é ünïcode")]}
   out = io.BytesIO(); probe._Emitter(out).emit("census", msg_fields)
   msg = json.loads(out.getvalue())
   expected = json.dumps(msg, separators=(",", ":")) + "\n"
   assert out.getvalue().decode("ascii") == expected

def test_rss_limit_emits_bye_and_exits(tmp_path):        # in test_stream.py
   p = run_stream(cfg(tmp_path, probe_rss_limit_mb=1))
   msgs = read_messages(p, until=lambda ms: any(m["type"] == "bye" for m in ms), timeout=10)
   bye = [m for m in msgs if m["type"] == "bye"][0]
   assert bye["reason"] == "rss_limit" and bye["rss_kb"] > 1024
   assert p.wait(timeout=5) == 0
```

`fake_row(pid, cmdline="python x.py")` is a test helper building a census row with the fields `scan_processes` produces.

- [ ] **Step 2: Run them to verify they fail**

Run: `pytest tests/probe/test_emitter_memory.py tests/probe/test_stream.py -k "peak or streamed or rss_limit" -v`
Expected: the peak test fails (peak ≈ 3× line size), the rss test fails (no `bye`), and the bytes test passes. The bytes test is a guard for the rewrite.

- [ ] **Step 3: Implement the chunked emitter, the RSS check, `bye`, and the lock timeout in `probe.py`**

- [ ] **Step 4: Verify**

Run: `pytest tests/probe -q && NODE_MONITOR_PROBE_PYTHON=/usr/bin/python3.6 pytest tests/probe/test_stream.py -q`
Expected: all pass, including the existing signal-not-splitting-a-line and takeover tests.

Then measure, and record the output in the commit message body:

```bash
env -u LD_PRELOAD python3 - <<'EOF'
# runs stream() against real /proc at census_interval 1 s for 300 censuses, gpu off,
# lock under /tmp, samples VmRSS every 30 s; print the series
EOF
```

Expected: VmRSS stays within 8 MiB of its value at census 30 for the rest of the run. Write the script as `tests/probe/manual_rss_series.py` (not collected by pytest: no `test_` prefix).

- [ ] **Step 5: Commit** — `fix(probe): stream messages in bounded chunks; add RSS self-limit with bye`

---

### Task 2: Config additions and session handling of `bye`

**Files:**
- Modify: `node_monitor/config.py`, `node_monitor/collector/bootstrap.py`, `node_monitor/collector/session.py`, `config.example.yaml`
- Test: `tests/test_config.py`, `tests/collector/test_session.py`, `tests/collector/test_bootstrap.py`

**Interfaces:**
- Produces:
  - `CollectionConfig` gains `summary_interval_sec: float = 300` and `probe_rss_limit_mb: float = 128`.
  - `ClassificationConfig` gains `min_user_uid: int = 1000`.
  - New `RedactionConfig(extra_patterns: tuple[str, ...] = ())`, as `Config.redaction`.
  - `load_config` validates, raising `ConfigError` naming the key, that:
    - every interval, timeout and limit is a positive number;
    - `session_silence_timeout_sec > counter_interval_sec`;
    - `summary_interval_sec` is a multiple of `census_interval_sec` and divides 3600;
    - `reconnect_backoff_sec` is an increasing pair (existing check);
    - every `extra_patterns` entry compiles as a regex.
  - `notable_cpu_seconds_per_min`, `notable_rss_mb` and `hourly_census` stay accepted and ignored until M3.
  - `probe_config(config)` adds `"probe_rss_limit_mb"`.
  - NodeSession: a `bye` message is passed to `on_message`, and it marks the session as ending deliberately. The EOF that follows ends the session with outcome `"rotate"`. That means no `session_lost`, no backoff, and a `session_rotated` event whose detail is `{"uptime_sec": ..., "reason": <bye reason>}`. Max-lifetime rotations get `"reason": "max_lifetime"`.

- [ ] **Step 1: Write the failing tests**

```python
def test_m1a_config_defaults(tmp_path):
   cfg = load_config(str(write_yaml(tmp_path, MINIMAL)))
   assert cfg.collection.summary_interval_sec == 300
   assert cfg.collection.probe_rss_limit_mb == 128
   assert cfg.classification.min_user_uid == 1000
   assert cfg.redaction.extra_patterns == ()

@pytest.mark.parametrize("section,key,value,match", [
   ("collection", "counter_interval_sec", 0, "counter_interval_sec"),
   ("collection", "census_interval_sec", "abc", "census_interval_sec"),
   ("collection", "session_silence_timeout_sec", 5, "session_silence_timeout_sec"),  # < counter 10
   ("collection", "summary_interval_sec", 90, "summary_interval_sec"),               # not multiple of 60
   ("redaction", "extra_patterns", ["(unclosed"], "extra_patterns"),
])
def test_invalid_values_are_rejected(tmp_path, section, key, value, match): ...

async def test_bye_ends_session_as_rotation(tmp_path):            # test_session.py
   rig = make_session(tmp_path, probe_cfg={"probe_rss_limit_mb": 1})
   async with rig:
      await rig.wait(lambda m, e: count_events(e, "session_rotated") >= 1, 15)
   rotated = details(rig.events, "session_rotated")[0]
   assert rotated["reason"] == "rss_limit"
   assert not [e for e in rig.events if e[1] in ("session_lost", "reconnect_wait")]
   assert count(rig.msgs, "bye") >= 1
```

- [ ] **Step 2: Run them to verify they fail** — `pytest tests/test_config.py tests/collector -q`. Expected: the new tests fail.

- [ ] **Step 3: Implement the config fields and validation, the `probe_config` key, session `bye` handling and rotation reasons, and the `config.example.yaml` additions** (spec §3 keys)

- [ ] **Step 4: Verify** — full suite on the venv. Also run the session tests on a Python 3.10 interpreter if one is available. CI covers 3.10 regardless.

- [ ] **Step 5: Commit** — `feat(collector): validate config; treat probe bye as rotation`

---

### Task 3: Classification rules and `classify()`

**Files:**
- Create: `node_monitor/pipeline/__init__.py`, `node_monitor/pipeline/rules.py`, `node_monitor/pipeline/classify.py`
- Test: `tests/pipeline/__init__.py`, `tests/pipeline/test_classify.py`

**Interfaces:**
- Produces:
  - `rules.ACTIVITY_RULES: tuple[tuple[str, str, str, int], ...]` of `(activity, category, regex, priority)`.
  - `rules.CATEGORY_RULES: tuple[tuple[str, str, int], ...]` of `(category, regex, priority)`. This is the fallback when no activity matches.
  - `Classification = namedtuple("Classification", "category activity")`.
  - `classify(comm: str, cmdline: str, uid: int, min_user_uid: int) -> Classification`:
    - Match against `cmdline` if non-empty, otherwise `comm`. The highest priority wins; on a tie, the earlier rule wins.
    - `category` is the matched activity's category, else the highest-priority `CATEGORY_RULES` match, else `"other"`. `activity` is `None` when nothing matches.
    - Any process with `uid < min_user_uid` gets category `"system"`, whatever matched, but keeps its activity.
  - Patterns are compiled once at import, with a module-level cache.

Port all 41 activity rules verbatim (labels, regexes, priorities) from `~/node-monitor-20261028/node_monitor/collector/remote_probe.py:170-311`. Port the category rules from lines 113-154 as fallbacks. Keep only short why-comments. Category mapping:

| v1 category / activity group | v2 category |
|---|---|
| `ai-coding-agent` (`claude-code`, `codex-cli`, `windsurf`, `cursor-cli`, `hermes`, `openclaw`, `roo`, `continue`, `copilot-agent`, `agent-like-unclassified`) | `ai_agent` |
| `ide-remote` (`vscode-remote-server`, `cursor-remote-server`) | `editor_server` |
| `jupyter` (`jupyter-kernel`, `jupyter-server`) | `jupyter` |
| `compute/build` (`compiler-invocation`, `build-driver`, `package-install`) | `build` |
| `python-training`, `python-inference`, `python-generic`, `pytest`, `python-http-server`, `torch-distributed` | `python` |
| `shell/session` (`shell`, `ssh-session`, `terminal-multiplexer`, `ssh-agent`, `sleep`) | `shell` |
| `data-xfer/vcs` (`git-operation`, `data-transfer`) | `data_transfer` |
| `fs-scan` (`filesystem-scan`) | `fs_scan` |
| `pbs-client` (`pbs-query`, `mpi-launch`) | `scheduler` |
| `globus-compute`, `balsam`, `parsl`, `argo` | `workflow` |
| `postgres`, `dbus`, `fail2ban` | `system` |
| `reuse-lint`, `starccm` | `other` |

For the v1 category fallback rules, map `ide-remote`→`editor_server`, `ai-coding-agent`→`ai_agent`, `jupyter`→`jupyter`, `pbs-client`→`scheduler`, `data-xfer/vcs`→`data_transfer`, `fs-scan`→`fs_scan`, `compute/build`→`build` and `shell/session`→`shell`. In the `compute/build` fallback, split bare `python`/`python[0-9.]*` out into its own `python` fallback rule.

- [ ] **Step 1: Write the failing tests**

Port the v1 classification examples from `~/node-monitor-20261028/tests/test_remote_probe.py`: every `_classify(...)` call, 27 in all. Rewrite each as a table case, with v1 categories mapped through the table above. Add these cases:

```python
CASES = [
   # (comm, cmdline, uid, expected_category, expected_activity)
   ("node", "/home/u/.vscode-server/extensions/anthropic.claude-code-2.1.0/bin/claude", 31112, "ai_agent", "claude-code"),
   ("node", "/home/u/.cursor-server/bin/xyz/node server.js", 31112, "editor_server", "cursor-remote-server"),
   ("bash", "/home/u/scripts/my_windsurf_notes.sh", 31112, "shell", "shell"),
   ("python3", "python3 train.py --epochs 3", 31112, "python", "python-training"),
   ("sshd", "sshd: alice@pts/3", 0, "system", "ssh-session"),
   ("weird", "", 31112, "other", None),
   ("make", "make -j 32", 500, "system", "build-driver"),           # uid below min_user_uid
]

@pytest.mark.parametrize("comm,cmdline,uid,cat,act", CASES)
def test_classify_cases(comm, cmdline, uid, cat, act):
   assert classify(comm, cmdline, uid, 1000) == (cat, act)

def test_every_activity_maps_to_a_spec_category():
   assert {c for _, c, _, _ in ACTIVITY_RULES} <= SPEC_CATEGORIES
   assert len(ACTIVITY_RULES) == 41

def test_narrow_agent_rule_beats_broad_editor_rule():
   prio = {a: p for a, _, _, p in ACTIVITY_RULES}
   assert prio["claude-code"] > prio["vscode-remote-server"]
```

Run the ported v1 expectations exactly as v1 wrote them. If one fails, the port is wrong, so don't edit the expectation.

- [ ] **Step 2: Run them to verify they fail** — `pytest tests/pipeline/test_classify.py -q`. Expected: ImportError.

- [ ] **Step 3: Implement `rules.py` and `classify.py`**

- [ ] **Step 4: Verify** — `pytest tests/pipeline -q`. Expected: pass.

- [ ] **Step 5: Commit** — `feat(pipeline): port v1 classification rules to the v2 category vocabulary`

---

### Task 4: Redaction

**Files:**
- Create: `node_monitor/pipeline/redact.py`
- Test: `tests/pipeline/test_redact.py`

**Interfaces:**
- Produces: `class Redactor(extra_patterns: Iterable[str] = ())`, with `redact(cmdline: str) -> str`. Masked values become `***`. Built-in rules (spec §5.2):
  - values of options whose name matches `password|passwd|pwd|token|secret|api[-_]?key|auth|credential`, case-insensitive, in both `--opt=value` and `--opt value` forms;
  - `KEY=value` assignments where KEY matches the same names, e.g. `GITHUB_TOKEN=…` → `GITHUB_TOKEN=***`;
  - `user:pass@` in URLs → `user:***@`;
  - `Authorization: <value>` and `Bearer <value>`;
  - well-known key formats anywhere: `AKIA[0-9A-Z]{16}`, `gh[pousr]_[A-Za-z0-9]{20,}`, `github_pat_[A-Za-z0-9_]{20,}`, `sk-[A-Za-z0-9_-]{16,}`.

  Each `extra_patterns` regex masks its first capture group if it has one, otherwise the whole match. Option names and flags are never altered.

- [ ] **Step 1: Write the failing tests**

```python
R = Redactor()

def test_redacts_both_option_forms_and_keeps_flags():
   assert R.redact("claude --dangerously-skip-permissions --api-key sk-abc123def456ghi789") == \
      "claude --dangerously-skip-permissions --api-key ***"
   assert R.redact("tool --password=hunter2 --verbose") == "tool --password=*** --verbose"
   assert R.redact("tool --token abc --model opus") == "tool --token *** --model opus"

def test_redacts_assignments_urls_headers_and_known_keys():
   assert R.redact("env GITHUB_TOKEN=ghp_" + "a" * 36 + " make") == "env GITHUB_TOKEN=*** make"
   assert R.redact("git clone https://bob:s3cret@host/repo.git") == "git clone https://bob:***@host/repo.git"
   assert R.redact("curl -H 'Authorization: Bearer xyz.abc' url") == "curl -H 'Authorization: ***' url"
   assert R.redact("aws AKIAABCDEFGHIJKLMNOP") == "aws ***"

def test_leaves_ordinary_commands_untouched():
   for c in ("python3 train.py --epochs 3 --lr 0.1", "ssh -o BatchMode=yes host", "make -j32 author=me"):
      assert R.redact(c) == c

def test_extra_patterns_mask_group_or_match():
   r = Redactor([r"--site-key\s+(\S+)", r"SITE[0-9]{6}"])
   assert r.redact("x --site-key k1 SITE123456") == "x --site-key *** ***"

def test_long_and_non_ascii_cmdlines():
   long = "python " + " ".join("--opt%d=v" % i for i in range(10000))      # ~100 KB
   t = time.perf_counter(); out = R.redact(long)
   assert out == long and time.perf_counter() - t < 0.5
   assert R.redact("runé --password=pässwörd") == "runé --password=***"
```

- [ ] **Step 2: Run them to verify they fail.** Expected: ImportError.
- [ ] **Step 3: Implement `Redactor`.** Precompile all patterns, and apply them in a fixed order (known key formats last, so they don't fire inside text another rule has already masked).
- [ ] **Step 4: Verify** — `pytest tests/pipeline/test_redact.py -q`.
- [ ] **Step 5: Commit** — `feat(pipeline): value-only command-line redaction`

---

### Task 5: Census enrichment — owner presence, idle bucket, live process rows

**Files:**
- Create: `node_monitor/pipeline/enrich.py`
- Test: `tests/pipeline/test_enrich.py`

**Interfaces:**
- Consumes: `classify`, `Classification` (Task 3); `Redactor` (Task 4). Census row fields come from the probe: `pid, ppid, start_time, uid, username, comm, exe, argv0, cmdline, state, threads, rss_kb, tty_nr, interactive, session_leader, parent_comm, parent_exe, parent_kind, under_screen_or_tmux, gpu_mem_mb, cpu_seconds, cpu_state, idle_for_sec, idle_for_is_lower_bound`.
- Produces:
  - `owner_presence_by_user(rows: list[dict]) -> dict[str, str]`. It scans the cmdlines of *all* rows for `^sshd: (?P<user>[^@\s\[]+)@(?P<tty>pts/\d+|notty)$`. A user with any `pts` match is `"interactive"`, a user with only `notty` matches is `"connected"`, and users not in the map are `"absent"`.
  - `idle_bucket(cpu_state: str | None, idle_for_sec: float | None) -> str`. It returns `"unknown"` if `cpu_state` is None, `"<5m"` if `idle_for_sec` is below 300, `"5m-1h"` below 3600, `"1h-1d"` below 86400, and `">1d"` otherwise. An active process has `idle_for_sec = 0`, so it falls in `"<5m"`.
  - `enrich_census(rows: list[dict], *, redactor: Redactor, min_user_uid: int) -> list[dict]`. It returns new dicts (inputs are not mutated) with the keys of the `LIVE_PROCESS_COLUMNS` tuple, in this order:
    - **Copied unchanged:** `pid, start_time, uid, username, comm, exe, argv0, ppid, parent_comm, parent_exe, parent_kind, state, threads, rss_kb, cpu_seconds, gpu_mem_mb, interactive, under_screen_or_tmux, idle_for_sec, idle_for_is_lower_bound`.
    - **Computed:**
      - `cmdline` (redacted)
      - `category` and `activity` (classification runs on the *unredacted* cmdline, before redaction)
      - `cpu_state` (`"unknown"` when the probe sent None)
      - `idle_bucket`
      - `owner_presence` (the user's presence on this node, or `"absent"`)
      - `is_user_process`

- [ ] **Step 1: Write the failing tests**

```python
def test_owner_presence_forms():
   rows = [r(uid=0, cmdline="sshd: alice [priv]"), r(uid=1001, username="alice", cmdline="sshd: alice@notty"),
           r(uid=1002, username="bob.x-y", cmdline="sshd: bob.x-y@pts/3"),
           r(uid=1003, username="carol", cmdline="sshd: carol@notty"), r(uid=1003, username="carol", cmdline="sshd: carol@pts/9")]
   assert owner_presence_by_user(rows) == {"alice": "connected", "bob.x-y": "interactive", "carol": "interactive"}

@pytest.mark.parametrize("state,idle,bucket", [(None, None, "unknown"), ("active", 0, "<5m"), ("idle", 299, "<5m"),
   ("idle", 300, "5m-1h"), ("idle", 3599, "5m-1h"), ("idle", 3600, "1h-1d"), ("idle", 86400, ">1d")])
def test_idle_bucket(state, idle, bucket):
   assert idle_bucket(state, idle) == bucket

def test_enrich_census_rows():
   rows = [r(pid=7, uid=31112, username="parton", cmdline="claude --api-key sk-abcdefghijklmnopqrst --resume",
             cpu_state=None, idle_for_sec=None),
           r(pid=8, uid=31112, username="parton", cmdline="sshd: parton@pts/2"),
           r(pid=9, uid=0, username="root", cmdline="/usr/sbin/sshd -D")]
   out = {x["pid"]: x for x in enrich_census(rows, redactor=Redactor(), min_user_uid=1000)}
   assert tuple(out[7]) == LIVE_PROCESS_COLUMNS
   assert out[7]["cmdline"] == "claude --api-key *** --resume"
   assert (out[7]["category"], out[7]["activity"]) == ("ai_agent", "claude-code")
   assert (out[7]["cpu_state"], out[7]["idle_bucket"], out[7]["owner_presence"]) == ("unknown", "unknown", "interactive")
   assert out[7]["is_user_process"] and not out[9]["is_user_process"]
   assert rows[0]["cmdline"].startswith("claude --api-key sk-")          # input not mutated
```

`r(**overrides)` is a helper building a full census row with defaults for every probe field.

- [ ] **Step 2: Run them to verify they fail.**
- [ ] **Step 3: Implement `enrich.py`**, with `LIVE_PROCESS_COLUMNS` defined there.
- [ ] **Step 4: Verify.**
- [ ] **Step 5: Commit** — `feat(pipeline): census enrichment with owner presence and dormancy buckets`

---

### Task 6: Minute aggregator — counters → `node_metrics_1m`

**Files:**
- Create: `node_monitor/pipeline/node_metrics.py`
- Test: `tests/pipeline/test_node_metrics.py`

**Interfaces:**
- Produces:
  - `class MinuteAggregator(counter_interval_sec: float)`. Each counters message is handled with `add_counters(msg: dict, recv: datetime, session_id: int) -> list[dict]`, which returns rows for any minutes that closed. It buckets by `recv` floored to the minute, and a minute closes when a sample for a later minute arrives.
  - `add_census(msg: dict, recv: datetime) -> None` records `gpus` and `scan_seconds` for the minute.
  - `flush() -> list[dict]` emits the open minute, if any.

  Rows have exactly the keys of `NODE_METRICS_COLUMNS`:
  - `minute`: tz-aware UTC datetime.
  - `cpu_pct`: dict over `user, nice, system, iowait, irq, softirq, steal, idle`. Each value is the share of the summed jiffy deltas of pairs whose later sample is in this minute, as a percentage rounded to 0.01.
  - `load1, load5, load15, procs_running, procs_total, socket_count`: last sample in the minute.
  - `mem_total_kb`, `mem_available_kb`, `mem_cached_kb`, `mem_shmem_kb`: last sample. `mem_used_kb` = total − available. `swap_used_kb` = swap_total − swap_free.
  - `network_rates`: `{iface: {"rx_bytes_per_sec", "tx_bytes_per_sec"}}`, where each value is the summed byte deltas ÷ summed elapsed seconds for the minute. Rates are per interface; do not sum interfaces, because `bond0` carries its `ens*` members.
  - `lustre_md_rates`: `{fs: ops_per_sec}`, where fs is the target name before `-MDT` and the value is the summed `total` deltas across that filesystem's targets ÷ elapsed seconds.
  - `gpus`: the latest census `gpus` list in the minute, or `[]`.
  - `sample_latency_sec`: the max census `scan_seconds` in the minute, or None.
  - `clock_offset_sec`: the median of `recv − parse(msg.ts)` over the minute's samples, in seconds rounded to 0.001.
  - `probe_cpu_seconds`: the summed deltas of `probe_self.cpu_seconds + children_cpu_seconds`.
  - `probe_maxrss_kb`: last sample.
  - `sample_count`: counters samples received in the minute. `expected_count` = `round(60 / counter_interval_sec)`.
- Delta rule: compute a delta between consecutive samples only when both have the same `session_id` and no cumulative value decreased. Otherwise, skip that pair for all rates and start a new baseline. A minute whose pairs were all skipped gets `cpu_pct = None` and empty rate dicts, but still gets its gauge values.

- [ ] **Step 1: Write the failing tests**

```python
# tests/pipeline/__init__.py: at(seconds) -> datetime(2026, 11, 1, 12, 0, tzinfo=UTC) + timedelta(seconds),
# shared by all pipeline tests.
def counters(t, jiffies, rx=0, md=0, cpu=0.0, ts=None): ...   # helper building a counters msg; t = seconds after 12:00:00Z

def test_minute_row_from_six_samples():
   agg = MinuteAggregator(10)
   for i in range(7):                                         # 12:00:00 .. 12:01:00
      out = agg.add_counters(counters(i * 10, jiffies={"user": 100 * i, "idle": 300 * i}, rx=1000 * i,
                                      md=50 * i, cpu=0.1 * i), at(i * 10), session_id=1)
   (row,) = out                                               # minute 12:00 closed by the 12:01:00 sample
   assert row["minute"] == at(0) and row["sample_count"] == 6 and row["expected_count"] == 6
   assert row["cpu_pct"]["user"] == 25.0 and row["cpu_pct"]["idle"] == 75.0
   assert row["network_rates"]["hsn0"]["rx_bytes_per_sec"] == 100.0
   assert row["lustre_md_rates"] == {"eagle": 5.0}
   assert row["probe_cpu_seconds"] == pytest.approx(0.5)

def test_counter_reset_skips_pair_without_negative_rates():
   agg = MinuteAggregator(10)
   agg.add_counters(counters(0, {"user": 10_000, "idle": 10_000}, rx=10**9), at(0), 1)
   agg.add_counters(counters(10, {"user": 5, "idle": 5}, rx=10), at(10), 2)          # new session, small values
   agg.add_counters(counters(20, {"user": 105, "idle": 305}, rx=1010), at(20), 2)
   (row,) = agg.flush()
   assert row["cpu_pct"]["user"] == 25.0                                            # only the 10->20 pair
   assert row["network_rates"]["hsn0"]["rx_bytes_per_sec"] == 100.0
   assert all(v >= 0 for v in row["cpu_pct"].values())

def test_minute_with_only_skipped_pairs_has_gauges_but_no_rates(): ...
def test_census_gpus_and_scan_seconds_attach_to_minute(): ...
def test_clock_offset_is_median_recv_minus_ts(): ...
```

- [ ] **Step 2: Run them to verify they fail.**
- [ ] **Step 3: Implement `MinuteAggregator`**, with `NODE_METRICS_COLUMNS` defined in the module.
- [ ] **Step 4: Verify.**
- [ ] **Step 5: Commit** — `feat(pipeline): minute aggregation of node counters`

---

### Task 7: Usage aggregator — enriched censuses → `user_usage_5m`, `node_usage_5m`

**Files:**
- Create: `node_monitor/pipeline/usage.py`
- Test: `tests/pipeline/test_usage.py`

**Interfaces:**
- Consumes: enriched rows (`LIVE_PROCESS_COLUMNS`, Task 5).
- Produces:
  - `class UsageAggregator(summary_interval_sec: float, census_interval_sec: float)`, with `add_census(rows: list[dict], recv: datetime) -> list[tuple[str, dict]]` and `flush() -> list[tuple[str, dict]]`. Both return `("user_usage_5m", row)` and `("node_usage_5m", row)` tuples for closed intervals. Buckets are `recv` floored to `summary_interval_sec` relative to the epoch, and only user processes (`is_user_process`) count.
  - **Per-census tallies, per user and for the whole node:**
    - `n`: process count
    - `cpu`: sum of non-null `cpu_seconds`
    - `unmeasured`: count of null `cpu_seconds`
    - `rss`: sum of `rss_kb`
    - `gpu`: sum of non-null `gpu_mem_mb`
    - `threads`: sum
    - `d`: count of `state == "D"`
    - `inter`: count of `interactive`
    - For each key in `by_category`, `by_activity` (activity None becomes `"unknown"`), `by_dormancy` (`cpu_state|idle_bucket|owner_presence`) and `by_category_dormancy` (`{category: {dormancy_key: …}}`): `{count, cpu_s, rss_kb}`.
  - **`user_usage_5m` row (`USER_USAGE_COLUMNS`):**
    - `username` and `interval_start`
    - `process_count_avg` and `process_count_max`
    - `cpu_seconds` (sum over censuses)
    - `rss_kb_avg` and `rss_kb_max`
    - `gpu_mem_mb_avg` and `gpu_mem_mb_max`
    - `threads_avg`
    - `d_state_fraction` and `interactive_fraction` (process-sample weighted)
    - `by_category`, `by_activity`, `by_dormancy` and `by_category_dormancy`. In each map entry, `count` is averaged over censuses, `cpu_s` is summed, and `rss_kb` is averaged.
    - `sample_count` (censuses received in the interval for this node), `expected_count` = `summary_interval_sec / census_interval_sec`, and `unmeasured_count` (summed).
    - **Averages divide by `sample_count`,** so censuses where the user had no processes contribute 0. A user with no processes in the whole interval gets no row.
  - **`node_usage_5m` row (`NODE_USAGE_COLUMNS`):** the same fields, without `username`, aggregated over all user processes. It adds `user_count_avg` and `user_count_max` (distinct users with at least one user process), plus `active_user_count_avg` (users with at least one `active` process).

- [ ] **Step 1: Write the failing tests**

```python
def test_user_absent_from_some_censuses_averages_zero():
   agg = UsageAggregator(300, 60)
   rows_a = [e(username="alice", rss_kb=1000, cpu_seconds=2.0)]
   for i in range(5):                                          # 12:00 .. 12:04
      agg.add_census(rows_a if i < 2 else [], at(60 * i))
   out = dict((k, r) for k, r in agg.add_census([], at(300)) if k == "user_usage_5m")  # closes 12:00
   row = out["user_usage_5m"]
   assert row["username"] == "alice" and row["sample_count"] == 5 and row["expected_count"] == 5
   assert row["process_count_avg"] == 0.4 and row["process_count_max"] == 1
   assert row["rss_kb_avg"] == 400 and row["rss_kb_max"] == 1000 and row["cpu_seconds"] == 4.0

def test_maps_and_fractions():
   rows = [e(username="bob", category="ai_agent", activity="claude-code", cpu_state="idle", idle_bucket="1h-1d",
             owner_presence="absent", state="S", rss_kb=500, cpu_seconds=0.0),
           e(username="bob", category="python", activity=None, cpu_state="active", idle_bucket="<5m",
             owner_presence="absent", state="D", interactive=True, rss_kb=300, cpu_seconds=30.0)]
   agg = UsageAggregator(300, 60); agg.add_census(rows, at(0))
   user = dict(agg.flush())["user_usage_5m"]
   assert user["by_category"]["ai_agent"] == {"count": 1.0, "cpu_s": 0.0, "rss_kb": 500.0}
   assert user["by_activity"]["unknown"]["count"] == 1.0
   assert user["by_dormancy"]["idle|1h-1d|absent"]["rss_kb"] == 500.0
   assert user["by_category_dormancy"]["ai_agent"]["idle|1h-1d|absent"]["count"] == 1.0
   assert user["d_state_fraction"] == 0.5 and user["interactive_fraction"] == 0.5

def test_node_row_counts_users_and_active_users(): ...
def test_system_processes_are_excluded(): ...                 # is_user_process False -> no contribution
def test_partial_interval_on_flush_has_reduced_sample_count(): ...
```

`e(**overrides)` builds an enriched row with defaults (`is_user_process=True`, `category="other"`, ...).

- [ ] **Step 2: Run them to verify they fail.**
- [ ] **Step 3: Implement `UsageAggregator`**, with the column tuples defined in the module.
- [ ] **Step 4: Verify.**
- [ ] **Step 5: Commit** — `feat(pipeline): 5-minute per-user and per-node usage summaries`

---

### Task 8: `NodePipeline` facade and records

**Files:**
- Create: `node_monitor/pipeline/records.py`, `node_monitor/pipeline/node_pipeline.py`
- Test: `tests/pipeline/test_node_pipeline.py`

**Interfaces:**
- Consumes: Tasks 3–7. Probe messages (`hello`, `counters`, `census`, `error`, `bye`) and NodeSession events (`session_started`, `session_lost`, `session_rotated`, `node_rebooted`, `probe_stderr`, `malformed_line`, `reconnect_wait`).
- Produces:
  - `Record = namedtuple("Record", "kind row")`. Kind constants: `LIVE_PROCESSES = "live_processes"`, `LIVE_NODE = "live_node"`, `NODE_METRICS_1M = "node_metrics_1m"`, `USER_USAGE_5M = "user_usage_5m"`, `NODE_USAGE_5M = "node_usage_5m"`, `NODE_HARDWARE = "node_hardware"`, `COLLECTION_EVENT = "collection_event"`.
  - Every row starts with the keys `system` and `node` (the node's configured `host`).
  - `class NodePipeline(*, system: str, node: NodeConfig, collection: CollectionConfig, classification: ClassificationConfig, redaction: RedactionConfig)`:
    - **`on_message(msg: dict, recv: datetime) -> list[Record]`:**
      - `hello`: `NODE_HARDWARE` with `{boot_id, btime, collected_at: recv, probe_version, python_version, hostname_fqdn, **hardware}`. If `hostname_fqdn != node.host`, also a `COLLECTION_EVENT` of type `hostname_mismatch`.
      - `counters`: minute rows, plus `LIVE_NODE` with `{label: node.label, updated_at: recv, session_state: "up", boot_id, probe_version, load1, mem_used_kb, mem_total_kb, procs_total, last_census_at}`. `boot_id` and `probe_version` are remembered from the session's `hello`.
      - `census`:
        1. Enrich the rows.
        2. Emit `LIVE_PROCESSES` with `{ts: recv, processes: [...]}`, a full snapshot that replaces the node's previous one.
        3. Emit the usage rows.
        4. Feed the minute aggregator.
      - `error`: a `COLLECTION_EVENT` of type `probe_error`.
      - `bye`: nothing; the session reports it.
    - **`on_event(kind: str, detail: dict, recv: datetime) -> list[Record]`:**
      - Every event becomes a `COLLECTION_EVENT` with `{ts, event_type, detail}`.
      - `session_started` increments the internal `session_id` used for minute deltas.
      - `session_lost` adds `LIVE_NODE` with `session_state: "down"`.
    - **`flush() -> list[Record]`** emits all open minute and usage intervals, for shutdown.
    - **Late samples:** a census or counters message whose bucket is earlier than an already-emitted interval is dropped. It produces one `COLLECTION_EVENT` of type `late_sample`, with `{type, recv}`.

- [ ] **Step 1: Write the failing tests**

```python
def test_end_to_end_over_a_scripted_session():
   p = make_pipeline()                                         # system "polaris", node host "n1.example"
   recs = p.on_event("session_started", {"attempt": 1, "pid": 9}, at(0))
   recs += p.on_message(hello(fqdn="n1.example", boot_id="b1"), at(0))
   for i in range(31):                                         # 12:00:00 .. 12:05:00, counters every 10 s
      recs += p.on_message(counters(i * 10), at(i * 10))
      if i % 6 == 0:
         recs += p.on_message(census([row(username="alice", cmdline="claude --token t1")]), at(i * 10))
   kinds = Counter(r.kind for r in recs)
   assert kinds[NODE_HARDWARE] == 1 and kinds[NODE_METRICS_1M] == 5
   assert kinds[USER_USAGE_5M] == 1 and kinds[NODE_USAGE_5M] == 1 and kinds[LIVE_PROCESSES] == 6
   live = [r for r in recs if r.kind == LIVE_PROCESSES][-1].row
   assert live["system"] == "polaris" and live["node"] == "n1.example"
   assert live["processes"][0]["cmdline"] == "claude --token ***"
   assert all(r.row["system"] == "polaris" for r in recs)

def test_hostname_mismatch_event(): ...
def test_late_census_for_closed_interval_is_dropped_with_event():
   p = make_pipeline()
   p.on_message(census([row()]), at(0)); p.on_message(census([row()]), at(360))   # closes 12:00 interval
   recs = p.on_message(census([row()]), at(120))                                   # belongs to closed 12:00
   assert [r.row["event_type"] for r in recs] == ["late_sample"]
def test_session_restart_starts_new_delta_baseline(): ...
def test_flush_emits_partial_intervals(): ...
```

- [ ] **Step 2: Run them to verify they fail.**
- [ ] **Step 3: Implement `records.py` and `NodePipeline`.**
- [ ] **Step 4: Verify** — full suite, `ruff check . && ruff format --check .`.
- [ ] **Step 5: Commit** — `feat(pipeline): per-node pipeline producing storage records`

---

### Task 9: `soak replay`, 12-hour re-soak, results

**Files:**
- Modify: `node_monitor/cli/main.py`; create `node_monitor/pipeline/replay.py`
- Create: `docs/soak/2026-11-m1a-resoak.md`
- Test: `tests/pipeline/test_replay.py`

**Interfaces:**
- Consumes: `NodePipeline` (Task 8); the soak JSONL layout (`<label>-<YYYYMMDD>.jsonl` lines `{recv, kind: "msg"|"event", msg|event+detail}`) and `manifest.json`.
- Produces:
  - `replay(out_dir: Path, config: Config) -> dict[str, Counter]`, keyed by node label: the record-kind counts from feeding every line of every node file, in receipt order, through a `NodePipeline`, then `flush()`. Census lines summarized without processes (the default soak mode) contribute only `gpus`/`scan_seconds`.
  - CLI `node-monitor soak replay DIR [--config PATH] [--write-records FILE]` prints one line per node with record counts. `--write-records` writes every record as JSONL `{kind, row}`, with datetimes in ISO format.

- [ ] **Step 1: Write the failing test** — `test_replay_counts_records_from_a_small_soak_dir`: build a soak dir with the M0 test helpers (2 nodes, scripted lines including a full census) and assert the per-node `Counter` values.
- [ ] **Step 2–4: Implement and verify** (suite + ruff).
- [ ] **Step 5: Commit** — `feat(cli): replay soak captures through the pipeline`
- [ ] **Step 6: Short full-census capture plus replay on real nodes**

```bash
env -u LD_PRELOAD node-monitor soak run --duration-hours 0.25 --full-census
env -u LD_PRELOAD node-monitor soak replay <that out dir> --write-records /tmp/m1a-records.jsonl
```

Expected: per node, about 15 `node_metrics_1m`, 3 `node_usage_5m` and 15 `live_processes` records, and user rows for about 150–250 users. Spot-check, and record in the results doc:
- the distribution of `category` across live processes;
- the share of `activity: null`, which should be under 15%, consistent with v1's 9%;
- three random redacted command lines;
- `owner_presence` counts.

Delete the full-census out dir afterwards. It holds unredacted command lines.

- [ ] **Step 7: 12-hour re-soak** with the procedure from M0 Task 8 Steps 1 and 3 (default summary mode, `--duration-hours 12`, `env -u LD_PRELOAD`, a clean tree, and one probe per account). Use the remote-probe `kill -STOP` check (c′) if any fault check is run.
  - **Pass:** every node's per-session exact RSS (`probe_self.rss_kb`, from smaps_rollup; `probe_rss_series` in `soak report --format json`) plateaus. The last hour of each full 6-hour session is within 10 MiB of its second hour, and the max exact RSS (`probe_rss_kb_max`, the `rss_MB` column) is < 64 MiB.
  - `probe_maxrss_kb` (`ru_maxrss`, the `maxrss_MB` column) is reported as a secondary figure only. Like VmRSS it is an approximate per-CPU counter that drifts tens of MiB on this 256-CPU kernel, so it does not decide the verdict.
  - `rotations_by_reason` has no `rss_limit`, and coverage is at least 99%.
- [ ] **Step 8: Write `docs/soak/2026-11-m1a-resoak.md`:**
  - the replay spot-checks;
  - a per-session exact-RSS table (start, hourly, end, from `probe_rss_series`) per node, compared with the M0 numbers;
  - the coverage and loss table;
  - the verdict on the M0 GO condition "fix probe memory growth".

  Note: the chunked streaming encoder raises probe CPU by roughly 25–30% compared with M0. That rise is expected, not a regression.

  Commit: `docs: M1a re-soak and pipeline replay results`.
