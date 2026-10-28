# M0 — Streaming Probe Soak Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the streaming probe, its SSH bootstrap, and a robust per-node session layer, then soak them on polaris-login-01/02/04 for ≥ 24 h and record a go/no-go.

**Architecture:** A single-file, stdlib-only Python 3.6 probe (`node_monitor/probe/probe.py`) is sent over SSH on every connection by a small bootstrap and streams JSON lines (`hello`, `counters`, `census`, `error`). An asyncio `NodeSession` per node owns one SSH subprocess, reads lines with a silence timeout, and reconnects with backoff. For M0 the messages go to per-node JSONL files and a `soak report` command summarizes them; M1 replaces the file recorder with the aggregator and database.

**Tech Stack:** Python ≥ 3.10 (collector, venv at `~/node-monitor/venv`, currently 3.13), Python ≥ 3.6 stdlib (probe), asyncio, click, PyYAML, pytest, ruff, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-10-28-node-monitor-v2-design.md` (§3 config, §4 collection, §5.3 dormancy, §11 testing, §13 M0).

## Global Constraints

- Repo root is `~/node-monitor/node-monitor/`; the working directory `~/node-monitor/` holds `venv/`, `logs/`, `state/`. Run all commands from the repo root with `~/node-monitor/venv/bin` first on `PATH`.
- 3-space indentation everywhere; ruff `indent-width = 3`, `line-length = 100`.
- `node_monitor/probe/probe.py` must run on Python 3.6 with stdlib only: no f-strings, no `dataclasses`, no `subprocess.run(capture_output=..., text=...)` (3.7+), no walrus, no `from __future__ import annotations`. Everything else: Python ≥ 3.10.
- Probe protocol version: `PROBE_VERSION = 1`.
- Defaults (spec §3): `counter_interval_sec: 10`, `census_interval_sec: 60`, `session_silence_timeout_sec: 30`, `session_max_lifetime_sec: 21600`, `reconnect_backoff_sec: [5, 300]`, `active_cpu_seconds_per_min: 0.1`, `ssh.connect_timeout_sec: 10`.
- Per-node lock path default: `/dev/shm/node-monitor-<uid>.lock`. Bootstrap marker string: `node-monitor-probe`.
- Config search order: `--config PATH`, `$NODE_MONITOR_CONFIG`, `~/.node_monitor.yaml`, `~/.config/node_monitor/config.yaml`.
- The probe never redacts or classifies; it ships raw fields. Redaction/classification are M1, collector-side.
- Comments explain why; no task IDs or review history in code. Tests are named by component and assert behavior, never README/source text.
- Commit after each task with a conventional-commit message ending in `Co-Authored-By: Claude Code <noreply@anthropic.com>`. Do not push.

## Review Focus

1. **Census lines larger than 64 KiB** (asyncio's default `StreamReader` limit; a real census is ~600 KB) — the session must read them intact. Test: Task 6 `test_large_census_line_is_read`.
2. **Probe stderr flood** (warnings, `nvidia-smi` noise) filling the stderr pipe — must not stall stdout processing. Test: Task 6 `test_stderr_flood_does_not_block`.
3. **PID reuse between censuses** (same pid, new start time) — must be treated as a new process, never a negative or huge CPU delta. Test: Task 3 `test_pid_reuse_is_new_process`.
4. **Stale lock naming a recycled PID** that now belongs to an unrelated process — takeover must never signal a process whose cmdline lacks the marker. Test: Task 4 `test_lock_refuses_to_signal_non_probe`.
5. **`nvidia-smi` missing, failing, or hanging** — census still arrives (`gpus: []`), an `error` message is emitted, and the stall is bounded by a 5 s timeout. Tests: Task 3 `test_gpu_query_failure_returns_empty`, `test_gpu_query_timeout_is_bounded`.

---

## File Structure

```
pyproject.toml                       package metadata, deps, ruff + pytest config
.gitignore
README.md                            dev setup + M0 soak usage
.github/workflows/ci.yml             ruff + pytest (3.10, 3.13) + probe on 3.6
config.example.yaml                  commented template (spec §3 keys)
node_monitor/__init__.py             __version__ = "0.1.0"
node_monitor/config.py               load_config, dataclasses, ConfigError
node_monitor/probe/__init__.py       (empty; probe.py is shipped as source, not imported by the collector)
node_monitor/probe/probe.py          the remote probe (py3.6, stdlib)
node_monitor/collector/__init__.py
node_monitor/collector/bootstrap.py  BOOTSTRAP_CODE, build_payload, remote_command, probe_config, probe_source
node_monitor/collector/ssh.py        write_ssh_config, ssh_argv
node_monitor/collector/session.py    NodeSession
node_monitor/collector/soak.py       SoakRecorder, run_soak, summarize
node_monitor/cli/__init__.py
node_monitor/cli/main.py             click group: soak run | soak report
tests/probe/fake_proc.py             synthetic /proc + /sys trees
tests/probe/test_counters.py
tests/probe/test_census.py
tests/probe/test_stream.py
tests/collector/fake_ssh.py          executable ssh stand-in (runs remote command locally)
tests/collector/test_bootstrap.py
tests/collector/test_session.py
tests/collector/test_soak.py
tests/test_config.py
docs/soak/2026-10-m0-results.md      written in Task 8
```

Probe unit tests load the probe as a module with `importlib.util.spec_from_file_location("probe", "node_monitor/probe/probe.py")`, exposed as a `probe` fixture in `tests/probe/conftest.py`.

---

### Task 1: Project scaffold and configuration

**Files:**
- Create: `pyproject.toml`, `.gitignore`, `README.md`, `.github/workflows/ci.yml`, `config.example.yaml`, `node_monitor/__init__.py`, `node_monitor/config.py`, `node_monitor/cli/__init__.py`, `node_monitor/collector/__init__.py`, `node_monitor/probe/__init__.py`
- Test: `tests/test_config.py`

**Interfaces:**
- Produces:
  - `class ConfigError(Exception)`
  - frozen dataclasses: `NodeConfig(host: str, label: str, ssh_target: str)`; `SshConfig(identity_file: str | None = None, connect_timeout_sec: float = 10)`; `CollectionConfig(counter_interval_sec: float = 10, census_interval_sec: float = 60, session_silence_timeout_sec: float = 30, session_max_lifetime_sec: float = 21600, reconnect_backoff_sec: tuple[float, float] = (5, 300))`; `ClassificationConfig(active_cpu_seconds_per_min: float = 0.1)`; `Config(system: str, nodes: tuple[NodeConfig, ...], probe_python: str, working_dir: Path, ssh: SshConfig, collection: CollectionConfig, classification: ClassificationConfig, source_path: Path)`
  - `find_config_path(explicit: str | None = None) -> Path` (search order in Global Constraints; raises `ConfigError` listing the paths tried)
  - `load_config(path: str | None = None) -> Config`

- [ ] **Step 1: Write `pyproject.toml`**

setuptools build; `name = "node-monitor"`, `requires-python = ">=3.10"`, `dependencies = ["click>=8.1", "pyyaml>=6.0"]`, `[project.optional-dependencies] dev = ["pytest>=8", "ruff>=0.6"]`, `[project.scripts] node-monitor = "node_monitor.cli.main:cli"`; package data includes `node_monitor/probe/probe.py`. `[tool.ruff] indent-width = 3, line-length = 100, target-version = "py310"`; `[tool.ruff.lint] select = ["E", "F", "W", "I", "B"]`. `[tool.pytest.ini_options] testpaths = ["tests"]`. `.gitignore` covers `__pycache__/`, `*.egg-info/`, `build/`, `dist/`, `.pytest_cache/`, `.ruff_cache/`, `*.log`, and local configs `config.yaml`, `*.local.yaml`.

- [ ] **Step 2: Write the failing config tests**

```python
def test_minimal_config_gets_spec_defaults(tmp_path):
   path = write_yaml(tmp_path, {"system": "polaris", "probe_python": "/usr/bin/python3.11",
                                "nodes": [{"host": "n1.example", "label": "n1"}]})
   cfg = load_config(str(path))
   assert cfg.nodes[0].ssh_target == "n1.example"          # defaults to host
   assert cfg.collection.counter_interval_sec == 10
   assert cfg.collection.census_interval_sec == 60
   assert cfg.collection.reconnect_backoff_sec == (5, 300)
   assert cfg.classification.active_cpu_seconds_per_min == 0.1
   assert cfg.working_dir == Path("~/node-monitor").expanduser()

def test_spec_sections_not_used_yet_are_accepted(tmp_path):
   # database/web/redaction/retention/spool belong to M1+ and must not break M0 loading
   ...

def test_unknown_top_level_key_is_rejected(tmp_path):
   with pytest.raises(ConfigError, match="unknown key.*colection"):
      load_config(str(write_yaml(tmp_path, {**MINIMAL, "colection": {}})))

def test_duplicate_node_labels_rejected(tmp_path): ...
def test_empty_nodes_rejected(tmp_path): ...
def test_backoff_must_be_increasing_pair(tmp_path): ...       # [300, 5] -> ConfigError

def test_search_order(tmp_path, monkeypatch):
   monkeypatch.setenv("HOME", str(tmp_path))
   monkeypatch.delenv("NODE_MONITOR_CONFIG", raising=False)
   xdg = tmp_path / ".config/node_monitor/config.yaml"; write_minimal(xdg)
   assert find_config_path() == xdg
   home = tmp_path / ".node_monitor.yaml"; write_minimal(home)
   assert find_config_path() == home
   env = tmp_path / "env.yaml"; write_minimal(env)
   monkeypatch.setenv("NODE_MONITOR_CONFIG", str(env))
   assert find_config_path() == env
   assert find_config_path(str(xdg)) == xdg

def test_missing_config_lists_paths_tried(tmp_path, monkeypatch): ...
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `pip install -e '.[dev]' && pytest tests/test_config.py -v`
Expected: FAIL (`ModuleNotFoundError: node_monitor.config`).

- [ ] **Step 4: Implement `node_monitor/config.py`**

Allowed top-level keys: `system, nodes, ssh, probe_python, working_dir, collection, classification, database, web, redaction, retention, spool`. Keys of the M1-only sections are not validated. Unknown keys inside `nodes[]`, `ssh`, `collection`, `classification` raise `ConfigError` naming the key. `~` is expanded in `working_dir` and `ssh.identity_file`.

- [ ] **Step 5: Write `config.example.yaml`, `README.md`, and CI**

`config.example.yaml` is the spec §3 example, commented. README: dev setup (`python3 -m venv ../venv && ../venv/bin/pip install -e '.[dev]' && pytest`) and a placeholder "M0 soak" section filled in by Task 7. `.github/workflows/ci.yml`: job `test` on `ubuntu-latest`, matrix Python `3.10`, `3.13`: `pip install -e '.[dev]'`, `ruff check .`, `ruff format --check .`, `pytest -q`. (Task 4 adds the Python 3.6 job.)

- [ ] **Step 6: Run tests and lint**

Run: `pytest -q && ruff check . && ruff format --check .`
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "chore: scaffold project and config loading"
```

---

### Task 2: Probe readers, node counters, hardware

**Files:**
- Create: `node_monitor/probe/probe.py` (readers, counters, hardware sections), `tests/probe/conftest.py`, `tests/probe/fake_proc.py`
- Test: `tests/probe/test_counters.py`

**Interfaces:**
- Produces (module-level functions in `probe.py`; `roots` is a dict `{"proc": str, "sys": str}` so tests point at fake trees):
  - `read_text(path) -> str | None` — returns `None` for EACCES, EPERM, ENOENT, ESRCH, EINVAL, EIO, ENXIO
  - `read_link(path) -> str | None` — `None` on any OSError
  - `collect_counters(roots) -> dict` with keys `uptime_sec, load1, load5, load15, procs_running, procs_total, cpu_jiffies{user,nice,system,idle,iowait,irq,softirq,steal,guest,guest_nice}, clk_tck, mem{total_kb,available_kb,free_kb,cached_kb,shmem_kb,swap_total_kb,swap_free_kb}, net{iface:{rx_bytes,tx_bytes}}, md_ops{target:{op:count,...,total}}, socket_count`
  - `collect_hardware(roots) -> dict` with keys `cpu_model, cpu_logical, sockets, cores_per_socket, cpu_max_freq_khz, numa_nodes, mem_total_kb, swap_total_kb, hugepage_size_kb, kernel_release, os_pretty_name, net_fs_mounts, net_ifaces, gpus`
  - `boot_identity(roots) -> tuple[str | None, int | None]` — `(boot_id, btime)`
  - `fake_proc.write_tree(root, pids=(), *, loadavg=None, stat_cpu=None, btime=1760000000, uptime="340091.15 1234567.00\n", meminfo=None, net_dev=None, md_stats=None, sockstat_used=None, boot_id="boot-a", cpuinfo_processors=2, cpu_model="Test CPU", omit=()) -> dict` returning `{"proc": ..., "sys": ...}`. `meminfo` is `{key: kB}`, `net_dev` is `{iface: (rx, tx)}`, `md_stats` is `{target: {op: count}}`, and `omit` is a tuple of relative paths left out; `fake_proc.make_stat(pid, comm, state="S", ppid=1, tty_nr=0, utime=100, stime=50, starttime=98765, rss_pages=1024, sid=0) -> str`. Each pid dict has `pid, comm, cmdline (None = kernel thread), uid, threads` plus optional `exe`, stat overrides; `write_tree` writes `stat`, `cmdline` (NUL-separated), `status` (`Uid:\t<uid>\t...` and `Threads:\t<n>`), and an `exe` symlink when given.

Port the reader and collector logic from `~/node-monitor-20261028/node_monitor/collector/remote_probe.py:578-1044`, with these changes: every path is built from `roots`; GPU inventory uses `subprocess.run(..., stdout=PIPE, stderr=PIPE, universal_newlines=True, timeout=5)` (the old code used 3.7-only `capture_output`/`text`); drop the SIGALRM/`ProbeTimeout` machinery; `uptime_sec` moves into counters.

- [ ] **Step 1: Write the failing tests**

```python
def test_counters_parse_fake_tree(probe, tmp_path):
   roots = fake_proc.write_tree(tmp_path,
      loadavg="1.50 2.00 2.50 7/6616 3371728\n",
      stat_cpu="cpu  100 5 50 1000 20 1 2 0 0 0\n",
      meminfo={"MemTotal": 528000000, "MemAvailable": 400000000},
      net_dev={"hsn0": (1000, 2000), "lo": (5, 5)},
      md_stats={"eagle-MDT0000-mdc-ff": {"open": 10, "close": 9}},
      sockstat_used=812)
   c = probe.collect_counters(roots)
   assert (c["load1"], c["procs_running"], c["procs_total"]) == (1.5, 7, 6616)
   assert c["cpu_jiffies"]["iowait"] == 20
   assert c["mem"]["available_kb"] == 400000000
   assert c["net"] == {"hsn0": {"rx_bytes": 1000, "tx_bytes": 2000}}   # lo excluded
   assert c["md_ops"]["eagle-MDT0000-mdc-ff"]["total"] == 19
   assert c["socket_count"] == 812
   assert c["uptime_sec"] == 340091.15

def test_counters_tolerate_missing_files(probe, tmp_path):
   roots = fake_proc.write_tree(tmp_path, omit=("loadavg", "net/dev", "fs/lustre"))
   c = probe.collect_counters(roots)
   assert "load1" not in c and c["net"] == {} and c["md_ops"] == {}

def test_hardware_and_boot_identity(probe, tmp_path):
   roots = fake_proc.write_tree(tmp_path, boot_id="abc-123", btime=1760000000,
                                cpuinfo_processors=4, cpu_model="AMD EPYC 7543P")
   hw = probe.collect_hardware(roots)
   assert hw["cpu_logical"] == 4 and hw["cpu_model"] == "AMD EPYC 7543P"
   assert probe.boot_identity(roots) == ("abc-123", 1760000000)

def test_read_text_returns_none_for_vanished(probe, tmp_path):
   assert probe.read_text(str(tmp_path / "nope")) is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/probe/test_counters.py -v` — Expected: FAIL (functions not defined).

- [ ] **Step 3: Implement the readers, `collect_counters`, `collect_hardware`, `boot_identity` in `probe.py` and the `fake_proc` helpers**

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/probe -v` — Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(probe): node counters and hardware inventory"
```

---

### Task 3: Probe process census and dormancy tracker

**Files:**
- Modify: `node_monitor/probe/probe.py` (census section)
- Test: `tests/probe/test_census.py`

**Interfaces:**
- Consumes: `read_text`, `read_link`, `fake_proc.write_tree` (Task 2)
- Produces:
  - `parse_stat(raw: str) -> tuple[str, list[str]] | None` — comm bounded by first `(` and last `)`; `None` if fewer than 22 tail fields
  - `scan_processes(roots, uid_names: dict) -> tuple[list[dict], dict]` — rows and coverage. `uid_names` is a cache dict filled via `pwd.getpwuid` (unresolvable → `None`). Kernel threads (missing or empty cmdline) are excluded. Each row has `pid, ppid, start_time, uid, username, comm, exe, argv0, cmdline` (NULs → spaces, stripped), `state, threads, rss_kb, utime, stime, tty_nr, interactive (tty_nr != 0), session_leader (pid == sid), parent_comm, parent_exe, parent_kind ("init"|"unknown"|"multiplexer"|"sshd"|"shell"|"other"), under_screen_or_tmux`. uid and threads come from `/proc/<pid>/status`. Coverage keys: `pids_seen, stat_unreadable, stat_unparseable, kernel_thread, vanished, owner_unresolved, exe_unreadable, parent_unresolved`. Parent resolution and `parent_kind` port `remote_probe.py:1143-1225`.
  - `class ProcessTracker(active_cpu_seconds_per_min: float, clk_tck: int)` with `update(rows: list[dict], now: float, interval_sec: float | None) -> None`. It mutates each row in place, removing `utime`/`stime` and adding `cpu_seconds`, `cpu_state`, `idle_for_sec`, `idle_for_is_lower_bound`. It prunes state for processes no longer present.
  - `query_gpus(timeout: float = 5.0) -> tuple[list[dict], dict, str | None]` — `(gpus, gpu_mem_by_pid, error)`; `gpus` items are `{index, util_pct, mem_used_mb, mem_total_mb}` from `nvidia-smi --query-gpu=index,utilization.gpu,memory.used,memory.total --format=csv,noheader,nounits`. Per-process memory comes from `nvidia-smi --query-compute-apps=pid,used_memory --format=csv,noheader,nounits`. If `nvidia-smi` isn't on `PATH`, returns `([], {}, None)`. A non-zero exit, timeout, or unparseable output returns `([], {}, "<short reason>")`.

Tracker algorithm (spec §5.3). State is keyed by `(pid, start_time)`:

```
first sighting (key not in state):
   cpu_seconds = None; cpu_state = None; idle_for_sec = None
   remember ticks, first_seen=now, last_active=None,
            existed_at_start = (this is the tracker's first update call)
later sighting:
   cpu_seconds = (utime+stime - prev_ticks) / clk_tck
   threshold   = active_cpu_seconds_per_min * interval_sec / 60
   if cpu_seconds >= threshold: cpu_state="active"; last_active=now; idle_for_sec=0
   else: cpu_state="idle"; idle_for_sec = now - (last_active or first_seen)
   idle_for_is_lower_bound = (last_active is None and existed_at_start)
```

- [ ] **Step 1: Write the failing tests**

```python
def test_parse_stat_handles_spaces_and_parens(probe):
   comm, tail = probe.parse_stat(fake_proc.make_stat(7, "tmux: server (x)"))
   assert comm == "tmux: server (x)" and tail[0] == "S"

def test_scan_excludes_kernel_threads_and_reads_status(probe, tmp_path):
   roots = fake_proc.write_tree(tmp_path, pids=[
      {"pid": 2, "comm": "kthreadd", "cmdline": None, "uid": 0},
      {"pid": 10, "comm": "bash", "cmdline": ["-bash"], "uid": 31112, "threads": 1,
       "tty_nr": 34817, "sid": 10},
      {"pid": 11, "comm": "node", "cmdline": ["node", "/home/u/.cursor-server/x.js", "--port", "1"],
       "uid": 31112, "threads": 11, "ppid": 10, "rss_pages": 2048, "exe": "/usr/bin/node"}])
   rows, cov = probe.scan_processes(roots, {31112: "parton"})
   by_pid = {r["pid"]: r for r in rows}
   assert set(by_pid) == {10, 11} and cov["kernel_thread"] == 1
   assert by_pid[11]["cmdline"] == "node /home/u/.cursor-server/x.js --port 1"
   assert by_pid[11]["threads"] == 11 and by_pid[11]["username"] == "parton"
   assert by_pid[11]["parent_comm"] == "bash" and by_pid[11]["parent_kind"] == "shell"
   assert by_pid[10]["interactive"] and by_pid[10]["session_leader"]
   assert by_pid[11]["exe"] == "/usr/bin/node"

def test_unresolvable_uid_is_null_not_invented(probe, tmp_path): ...   # username None, owner_unresolved == 1

def test_tracker_active_idle_and_lower_bound(probe):
   t = probe.ProcessTracker(active_cpu_seconds_per_min=0.1, clk_tck=100)
   r = row(pid=5, start=1, utime=100, stime=0)
   t.update([r], now=0, interval_sec=None)
   assert r["cpu_seconds"] is None and r["cpu_state"] is None and "utime" not in r
   r = row(pid=5, start=1, utime=150, stime=0)           # 0.5 cpu-s in 60 s -> active
   t.update([r], now=60, interval_sec=60)
   assert (r["cpu_seconds"], r["cpu_state"], r["idle_for_sec"]) == (0.5, "active", 0)
   r = row(pid=5, start=1, utime=150, stime=0)           # no CPU for 120 s
   t.update([r], now=180, interval_sec=120)
   assert (r["cpu_state"], r["idle_for_sec"], r["idle_for_is_lower_bound"]) == ("idle", 120, False)

def test_never_active_process_from_session_start_is_lower_bound(probe):
   ...  # seen at first update, idle at t=60 and t=120 -> idle_for_sec == 120, lower bound True

def test_process_started_after_tracking_is_not_lower_bound(probe):
   ...  # first appears on second update, idle on third -> lower bound False

def test_pid_reuse_is_new_process(probe):
   t = probe.ProcessTracker(0.1, 100)
   t.update([row(pid=5, start=1, utime=10_000, stime=0)], now=0, interval_sec=None)
   r = row(pid=5, start=999, utime=3, stime=0)            # same pid, new start time
   t.update([r], now=60, interval_sec=60)
   assert r["cpu_seconds"] is None                         # treated as first sighting

def test_vanished_processes_are_pruned(probe): ...        # internal state size returns to 0

def test_gpu_query_parses_output(probe, fake_bin): ...    # fake nvidia-smi on PATH, both queries
def test_gpu_query_missing_binary_is_silent(probe, monkeypatch): ...   # ([], {}, None)
def test_gpu_query_failure_returns_empty(probe, fake_bin): ...         # exit 1 -> ([], {}, "nvidia-smi exit 1")
def test_gpu_query_timeout_is_bounded(probe, fake_bin):
   fake_bin("nvidia-smi", "sleep 30")
   start = time.monotonic()
   gpus, by_pid, err = probe.query_gpus(timeout=1.0)
   assert gpus == [] and "timeout" in err and time.monotonic() - start < 3
```

`row(pid, start, utime, stime)` is a test helper returning `{"pid", "start_time", "utime", "stime"}`. `fake_bin` is a `tests/probe/conftest.py` fixture: `fake_bin(name, shell_body)` writes an executable `#!/bin/sh` script into a tmp dir and prepends that dir to `PATH`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/probe/test_census.py -v` — Expected: FAIL.

- [ ] **Step 3: Implement `parse_stat`, `scan_processes`, `ProcessTracker`, `query_gpus` in `probe.py`**

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/probe -v` — Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(probe): process census with dormancy tracking"
```

---

### Task 4: Probe streaming loop, lock, self-termination

**Files:**
- Modify: `node_monitor/probe/probe.py` (main section), `.github/workflows/ci.yml` (add the 3.6 job)
- Test: `tests/probe/test_stream.py`

**Interfaces:**
- Consumes: everything from Tasks 2–3
- Produces:
  - `PROBE_VERSION = 1`; `MARKER = "node-monitor-probe"`
  - Config keys read from the `PROBE_CONFIG` dict (all optional; defaults shown): `counter_interval_sec: 10`, `census_interval_sec: 60`, `active_cpu_seconds_per_min: 0.1`, `lock_path: "/dev/shm/node-monitor-<uid>.lock"`, `proc_root: "/proc"`, `sys_root: "/sys"`, `gpu: True`
  - `class ProbeLock(path: str)`, `acquire() -> None`. Uses `fcntl.flock(LOCK_EX | LOCK_NB)` and writes its PID into the file. If the lock is held, it reads the holder's PID. If `/proc/<pid>/cmdline` contains `MARKER`, it sends SIGTERM, waits up to 5 s (polling every 0.1 s) for the lock, then sends SIGKILL and retries once. Otherwise it raises `LockHeldByOther(pid)`.
  - `stream(config: dict, out=sys.stdout, stdin=sys.stdin) -> int` — the main loop:
    1. Take the lock. On `LockHeldByOther`, emit `error` (`where="lock"`) and return 3.
    2. Start a daemon thread that blocks on `stdin.buffer.read(1)` until EOF, then calls `os._exit(0)`.
    3. Emit `hello`.
    4. Loop: emit `counters` every `counter_interval_sec` and `census` every `census_interval_sec`, scheduled on `time.monotonic()`.
    5. Return 0 if `os.getppid()` differs from the parent PID recorded at startup (checked each iteration). On login nodes orphans are re-parented to `systemd --user`, not PID 1, so "ppid == 1" would miss them. A `BrokenPipeError`/`OSError` on write calls `os._exit(0)`.
  - `once(config: dict, out=sys.stdout) -> int` — emits `hello`, `counters`, `census` once, with no lock and no stdin watcher. Used by the CLI `--once` and the CI 3.6 job.
  - Entry points at the bottom of the file:
    - `if __name__ == "__node_monitor_probe__": sys.exit(stream(PROBE_CONFIG))` — set by the bootstrap
    - `if __name__ == "__main__":` supports `--once [--proc-root DIR] [--sys-root DIR]`
  - Message envelope: every line is `json.dumps(obj, separators=(",", ":"))`, with `type`, `seq` (int, from 0 per process) and `ts` (UTC `"%Y-%m-%dT%H:%M:%S.mmmZ"`), and is flushed immediately.
    - `hello`: `probe_version, python_version, hostname_fqdn, pid, boot_id, btime, hardware`
    - `counters`: the `collect_counters` keys plus `probe_self: {cpu_seconds, children_cpu_seconds, maxrss_kb}` from `resource.getrusage`
    - `census`: `interval_sec` (None on the first census), `scan_seconds`, `coverage`, `gpus`, and `processes` (rows from `scan_processes` after `ProcessTracker.update`, plus `gpu_mem_mb` from `gpu_mem_by_pid` or None)
    - `error`: `where, message` (message truncated to 500 chars). Errors are emitted for a GPU query error and for any exception inside a counters/census collection; the loop continues after them.

- [ ] **Step 1: Write the failing tests**

The tests run the probe as a subprocess. `run_stream(config)` starts `sys.executable -u -c "import runpy,sys,json; g=runpy.run_path(<probe>, run_name='x'); sys.exit(g['stream'](json.loads(sys.argv[1])))" <config-json>`, with `stdin=PIPE, stdout=PIPE`. Configs use a fake tree, `lock_path` in `tmp_path`, `gpu: False`, `counter_interval_sec: 0.2`, `census_interval_sec: 0.5`.

```python
def test_stream_emits_hello_then_counters_and_census(tmp_path):
   p = run_stream(cfg(tmp_path))
   msgs = read_messages(p, until=lambda ms: any(m["type"] == "census" for m in ms), timeout=5)
   assert msgs[0]["type"] == "hello" and msgs[0]["probe_version"] == 1
   assert [m["seq"] for m in msgs] == list(range(len(msgs)))
   assert any(m["type"] == "counters" and "probe_self" in m for m in msgs)

def test_second_census_has_cpu_deltas(tmp_path): ...       # 2nd census rows have cpu_seconds not None

def test_stdin_eof_terminates_probe(tmp_path):
   p = run_stream(cfg(tmp_path)); read_messages(p, count=1, timeout=5)
   p.stdin.close()
   assert p.wait(timeout=3) == 0

def test_closed_stdout_terminates_probe(tmp_path):
   p = run_stream(cfg(tmp_path)); read_messages(p, count=1, timeout=5)
   p.stdout.close()
   assert p.wait(timeout=3) == 0

def test_parent_death_terminates_probe(tmp_path):
   # wrap the probe in `sh -c '<probe cmd> & wait'`, SIGKILL the sh; the test still
   # holds the probe's stdin open, so only the parent-change check can end it
   wrapper, probe_pid = run_stream_under_shell(cfg(tmp_path))
   os.kill(wrapper.pid, signal.SIGKILL)
   assert wait_for_pid_exit(probe_pid, timeout=5)

def test_new_probe_takes_over_stale_probe(tmp_path):
   old = run_stream(cfg(tmp_path)); read_messages(old, count=1, timeout=5)
   new = run_stream(cfg(tmp_path))
   assert read_messages(new, count=1, timeout=8)[0]["type"] == "hello"
   assert old.wait(timeout=8) is not None                    # old one was signalled

def test_lock_refuses_to_signal_non_probe(tmp_path):
   # a non-probe process holds the lock (flock via a helper whose cmdline lacks the marker)
   holder = hold_lock_without_marker(tmp_path / "lock")
   p = run_stream(cfg(tmp_path))
   msgs = read_messages(p, count=1, timeout=5)
   assert msgs[0]["type"] == "error" and msgs[0]["where"] == "lock"
   assert p.wait(timeout=3) == 3 and holder.poll() is None   # holder untouched

def test_once_mode_on_real_proc():
   out = subprocess.run([sys.executable, PROBE, "--once"], stdout=PIPE, check=True, timeout=60)
   types = [json.loads(l)["type"] for l in out.stdout.splitlines()]
   assert types == ["hello", "counters", "census"]
```

Because `run_stream` starts the probe through `-c` code, it can only take over a stale probe if that cmdline contains the marker. `run_stream` therefore puts `#node-monitor-probe` as the first line of its `-c` code, which is how the real bootstrap does it (Task 5).

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/probe/test_stream.py -v` — Expected: FAIL.

- [ ] **Step 3: Implement `ProbeLock`, `stream`, `once`, emit helpers, and the entry points**

- [ ] **Step 4: Add the CI job `probe-py36`**

Runs in `container: python:3.6-slim` and checks out the repo. It runs `python -m py_compile node_monitor/probe/probe.py`, then `python node_monitor/probe/probe.py --once > out.jsonl`, then `python -c "import json; ts=[json.loads(l)['type'] for l in open('out.jsonl')]; assert ts == ['hello','counters','census'], ts"`.

- [ ] **Step 5: Run tests and verify the probe on the login nodes' oldest interpreter locally**

Run: `pytest tests/probe -v && /usr/bin/python3.6 node_monitor/probe/probe.py --once | head -c 300`
Expected: tests PASS; three JSON lines from Python 3.6, starting with `{"type":"hello"`.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(probe): streaming loop with lock takeover and self-termination"
```

---

### Task 5: Bootstrap and SSH command construction

**Files:**
- Create: `node_monitor/collector/bootstrap.py`, `node_monitor/collector/ssh.py`
- Test: `tests/collector/test_bootstrap.py`

**Interfaces:**
- Consumes: `Config`, `SshConfig`, `NodeConfig` (Task 1); the probe file and its `PROBE_CONFIG` keys (Task 4)
- Produces:
  - `BOOTSTRAP_CODE: str` — exactly this (Python 3.6, first line is the marker):

    ```python
    #node-monitor-probe
    import sys, json
    h = json.loads(sys.stdin.buffer.readline().decode("utf-8"))
    src = sys.stdin.buffer.read(h["len"]).decode("utf-8")
    g = {"__name__": "__node_monitor_probe__", "PROBE_CONFIG": h["config"]}
    exec(compile(src, "node_monitor_probe", "exec"), g)
    ```
  - `probe_source() -> str` — the text of `node_monitor/probe/probe.py`, via `importlib.resources.files("node_monitor.probe")`
  - `probe_config(config: Config) -> dict` — `{"counter_interval_sec", "census_interval_sec", "active_cpu_seconds_per_min", "gpu": True}` taken from config
  - `build_payload(source: str, probe_cfg: dict) -> bytes` — `json.dumps({"len": len(source_bytes), "config": probe_cfg}) + "\n"` followed by the UTF-8 source bytes
  - `remote_command(probe_python: str) -> str` — `shlex.join(["env", "-u", "LD_PRELOAD", probe_python, "-u", "-c", BOOTSTRAP_CODE])`
  - `write_ssh_config(state_dir: Path, ssh: SshConfig) -> Path` — writes `state_dir/ssh_config` with mode 0600 and creates `state_dir/ssh/` with mode 0700. Contents (`Host *` block): `BatchMode yes`, `ControlMaster auto`, `ControlPersist 60s`, `ControlPath <state_dir>/ssh/cm-%C`, `ConnectTimeout <n>`, `ServerAliveInterval 10`, `ServerAliveCountMax 3`, plus `IdentityFile <path>` and `IdentitiesOnly yes` when `identity_file` is set.
  - `ssh_argv(ssh_binary: str, ssh_config_path: Path, target: str, command: str) -> list[str]` — `[ssh_binary, "-F", str(path), "-o", "BatchMode=yes", target, command]`

- [ ] **Step 1: Write the failing tests**

```python
def test_bootstrap_runs_probe_end_to_end(tmp_path):
   roots = fake_proc.write_tree(tmp_path / "t", pids=[...one user process...])
   cfg = {"counter_interval_sec": 0.2, "census_interval_sec": 0.4, "gpu": False,
          "lock_path": str(tmp_path / "lock"), "proc_root": roots["proc"], "sys_root": roots["sys"]}
   p = subprocess.Popen([sys.executable, "-u", "-c", BOOTSTRAP_CODE], stdin=PIPE, stdout=PIPE)
   p.stdin.write(build_payload(probe_source(), cfg)); p.stdin.flush()   # stdin stays open
   first = json.loads(p.stdout.readline())
   assert first["type"] == "hello"
   p.stdin.close()
   assert p.wait(timeout=3) == 0

def test_payload_length_counts_bytes_not_chars():
   payload = build_payload("x = 'é'\n", {})
   header, body = payload.split(b"\n", 1)
   assert json.loads(header)["len"] == len(body) == len("x = 'é'\n".encode())

def test_remote_command_round_trips_through_a_shell():
   argv = shlex.split(remote_command("/usr/bin/python3.11"))
   assert argv == ["env", "-u", "LD_PRELOAD", "/usr/bin/python3.11", "-u", "-c", BOOTSTRAP_CODE]

def test_ssh_config_contents_and_modes(tmp_path):
   path = write_ssh_config(tmp_path, SshConfig(identity_file="/home/u/.ssh/k", connect_timeout_sec=10))
   text = path.read_text()
   for line in ("BatchMode yes", "ControlMaster auto", f"ControlPath {tmp_path}/ssh/cm-%C",
                "ServerAliveInterval 10", "ServerAliveCountMax 3", "ConnectTimeout 10",
                "IdentityFile /home/u/.ssh/k", "IdentitiesOnly yes"):
      assert line in text
   assert stat.S_IMODE(path.stat().st_mode) == 0o600

def test_ssh_config_without_identity_omits_identities_only(tmp_path): ...
def test_ssh_argv_shape(): ...
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/collector/test_bootstrap.py -v` — Expected: FAIL.

- [ ] **Step 3: Implement `bootstrap.py` and `ssh.py`**

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/collector -v` — Expected: PASS.

- [ ] **Step 5: Verify against one real node**

Run (from the repo root, with a temporary state dir):

```bash
python - <<'EOF'
import json, subprocess, tempfile
from pathlib import Path
from node_monitor.config import SshConfig
from node_monitor.collector import bootstrap, ssh
state = Path(tempfile.mkdtemp())
cfgp = ssh.write_ssh_config(state, SshConfig(identity_file=str(Path("~/.ssh/id_ed25519_polaris").expanduser())))
argv = ssh.ssh_argv("ssh", cfgp, "polaris-login-01.head", bootstrap.remote_command("/usr/bin/python3.11"))
p = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
p.stdin.write(bootstrap.build_payload(bootstrap.probe_source(), {"census_interval_sec": 5})); p.stdin.flush()
for _ in range(3):
   m = json.loads(p.stdout.readline()); print(m["type"], m.get("hostname_fqdn", ""), len(m.get("processes", [])))
p.stdin.close(); print("exit", p.wait(timeout=15))
EOF
```

Expected: `hello polaris-login-01.hsn.cm.polaris.alcf.anl.gov 0`, then `counters`/`census` lines, with the census showing roughly 1,500–2,500 processes, then `exit 0`. Afterwards, `ssh polaris-login-01.head pgrep -f node-monitor-probe` prints nothing (the probe exited remotely).

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(collector): probe bootstrap and ssh command construction"
```

---

### Task 6: NodeSession with reconnect, silence timeout, rotation

**Files:**
- Create: `node_monitor/collector/session.py`, `tests/collector/fake_ssh.py` (executable)
- Test: `tests/collector/test_session.py`

**Interfaces:**
- Consumes: `build_payload`, `remote_command`, `ssh_argv` (Task 5)
- Produces:

  ```python
  class NodeSession:
     def __init__(self, label: str, *, argv: list[str], payload: bytes,
                  on_message: Callable[[str, dict], None],
                  on_event: Callable[[str, str, dict], None],
                  silence_timeout: float, max_lifetime: float,
                  backoff: tuple[float, float],
                  healthy_after: float = 60.0) -> None: ...
     async def run(self) -> None      # returns only after stop()
     def stop(self) -> None
  ```

  - `on_message(label, msg)` is called for every parsed JSON line.
  - `on_event(label, event_type, detail)`. Event types and their details:
    - `session_started` `{attempt}`
    - `session_lost` `{reason: "eof"|"silence"|"exit"|"spawn_failed", exit_code, uptime_sec, stderr_tail}`
    - `session_rotated` `{uptime_sec}`
    - `node_rebooted` `{old_boot_id, new_boot_id}`
    - `probe_stderr` `{line}` (each line truncated to 500 chars; at most 20 per session, then one `probe_stderr` with `{suppressed: n}` when the session ends)
    - `malformed_line` `{bytes, sample}` (sample = first 200 chars)
    - `reconnect_wait` `{delay_sec, failures}`

  Behaviour:
  - Spawn with `asyncio.create_subprocess_exec(*argv, stdin=PIPE, stdout=PIPE, stderr=PIPE, start_new_session=True, limit=32 * 1024 * 1024)`, write `payload`, and keep stdin open.
  - A separate task drains stderr continuously.
  - Each `readline()` is wrapped in `asyncio.wait_for(..., silence_timeout)`.
  - Session end, for any reason: SIGTERM the process group, wait up to 5 s, then SIGKILL, then reap.
  - Rotation: once `max_lifetime` has elapsed, close stdin (the probe exits on EOF), wait up to 10 s for EOF, kill, emit `session_rotated`, and reconnect immediately with no backoff.
  - Backoff after a loss:
    `delay = min(backoff[1], backoff[0] * 2 ** (failures - 1)) * (0.5 + random.random() / 2)`.
    `failures` resets to 0 when a session stays up for at least `healthy_after` seconds.
  - Boot ID: remember the last `hello.boot_id`. If a later `hello` carries a different non-null value, emit `node_rebooted`.
  - `stop()` ends the current session as in rotation and makes `run()` return.

- [ ] **Step 1: Write `tests/collector/fake_ssh.py`**

An executable `#!/usr/bin/env python3` that mimics ssh:
- It skips `-F <x>`, `-o <x>` and other single-dash options, takes the next argument as the target and joins the rest with spaces.
- It honours `FAKE_SSH_STDERR_BYTES` (writes that many bytes of `x` to stderr first) and `FAKE_SSH_FAIL=1` (prints `Permission denied` to stderr and exits 255).
- It then `os.execvp("/bin/sh", ["sh", "-c", command])`.

Tests set `PATH` so that `env` and the test's `sys.executable` resolve, and use `remote_command(sys.executable)`.

- [ ] **Step 2: Write the failing tests**

All tests use a fake tree, a `lock_path` under `tmp_path`, `gpu: False`, short intervals (`counter 0.2`, `census 0.5`), `silence_timeout=2`, `backoff=(0.05, 0.4)`, and `healthy_after=1`. A helper `run_until(session, predicate, timeout)` runs `session.run()` until `predicate(messages, events)` holds, then calls `stop()`.

```python
async def test_streams_messages(tmp_path):
   msgs, events = await run_until(make_session(tmp_path), lambda m, e: count(m, "census") >= 2, 10)
   assert msgs[0]["type"] == "hello" and events[0][1] == "session_started"

async def test_reconnects_after_ssh_process_is_killed(tmp_path):
   # kill the fake ssh pid after the first census; expect session_lost then a second session_started
   ...
   assert reasons(events, "session_lost")[0] in ("eof", "exit")
   assert count_events(events, "session_started") >= 2

async def test_silence_timeout_kills_and_reconnects(tmp_path):
   s = make_session(tmp_path, probe_cfg={"counter_interval_sec": 100, "census_interval_sec": 100},
                    silence_timeout=1)
   ...
   assert "silence" in reasons(events, "session_lost")
   # the silenced probe process is gone (its pid from hello no longer exists)

async def test_rotation_reconnects_without_backoff(tmp_path):
   s = make_session(tmp_path, max_lifetime=1.0)
   ...
   assert count_events(events, "session_rotated") >= 1
   assert not [e for e in events if e[1] == "reconnect_wait"]

async def test_node_reboot_detected_on_boot_id_change(tmp_path):
   # rewrite <proc>/sys/kernel/random/boot_id between sessions, kill the session
   ...
   assert ("node_rebooted", {"old_boot_id": "boot-a", "new_boot_id": "boot-b"}) in kinds_details(events)

async def test_spawn_failure_backs_off_with_growing_delay(tmp_path, monkeypatch):
   monkeypatch.setenv("FAKE_SSH_FAIL", "1")
   ...
   delays = [d["delay_sec"] for _, k, d in events if k == "reconnect_wait"]
   assert len(delays) >= 3 and max(delays) <= 0.4
   assert reasons(events, "session_lost")[0] == "exit"
   assert "Permission denied" in first_lost(events)["stderr_tail"]

async def test_large_census_line_is_read(tmp_path):
   # 600 fake processes with 1 KiB cmdlines -> census line > 600 KiB
   msgs, _ = await run_until(make_session(tmp_path, pids=big_tree(600)), lambda m, e: count(m, "census") >= 1, 15)
   assert len(next(m for m in msgs if m["type"] == "census")["processes"]) == 600

async def test_stderr_flood_does_not_block(tmp_path, monkeypatch):
   monkeypatch.setenv("FAKE_SSH_STDERR_BYTES", str(2 * 1024 * 1024))
   msgs, events = await run_until(make_session(tmp_path), lambda m, e: count(m, "census") >= 1, 15)
   assert count_events(events, "probe_stderr") <= 21

async def test_malformed_line_is_reported_and_stream_continues(tmp_path): ...
async def test_stop_terminates_remote_probe(tmp_path): ...   # after stop(), probe pid from hello is gone within 5 s
```

Mark the module with `pytestmark = pytest.mark.asyncio` and add `pytest-asyncio>=0.23` to the `dev` extras, with `asyncio_mode = "auto"` in `[tool.pytest.ini_options]`.

- [ ] **Step 3: Run tests to verify they fail**

Run: `pytest tests/collector/test_session.py -v` — Expected: FAIL.

- [ ] **Step 4: Implement `NodeSession`**

- [ ] **Step 5: Run tests to verify they pass, repeatedly**

Run: `for i in 1 2 3 4 5; do pytest -q tests/collector/test_session.py || break; done`
Expected: PASS on all five runs (these tests are timing-based; a flaky failure is a bug to fix, not to retry).

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(collector): resilient per-node streaming session"
```

---

### Task 7: Soak recorder, runner, report, CLI

**Files:**
- Create: `node_monitor/collector/soak.py`, `node_monitor/cli/main.py`
- Modify: `README.md` (M0 soak section)
- Test: `tests/collector/test_soak.py`

**Interfaces:**
- Consumes: `Config`/`load_config` (Task 1); `probe_source`, `probe_config`, `build_payload`, `remote_command`, `write_ssh_config`, `ssh_argv` (Task 5); `NodeSession` (Task 6)
- Produces:
  - `class SoakRecorder(out_dir: Path, full_census: bool = False)`
    - `on_message(label, msg)` and `on_event(label, kind, detail)` append one JSON line to `out_dir/<label>-<YYYYMMDD>.jsonl` (UTC date of receipt). Each line is `{"recv": <iso>, "kind": "msg"|"event", ...}`: messages put the message under `"msg"`, events add `"event"` and `"detail"`.
    - With `full_census=False`, a census is stored as `{type, seq, ts, interval_sec, scan_seconds, coverage, gpus, process_count, user_count, bytes}`, where `bytes` is the serialized size of the original message.
    - `close()`.
  - `async def run_soak(config: Config, out_dir: Path, duration_sec: float | None, full_census: bool = False, ssh_binary: str = "ssh", probe_overrides: dict | None = None) -> int` — `probe_overrides` is merged over `probe_config(config)`; tests use it for `proc_root`, `sys_root`, `lock_path`, `gpu`.
    1. Writes the ssh config under `config.working_dir / "state"`.
    2. Starts one `NodeSession` per node (argv from `ssh_argv(ssh_binary, ..., node.ssh_target, remote_command(config.probe_python))`, plus the config's silence timeout, lifetime and backoff).
    3. Writes `out_dir/manifest.json` (`started`, `system`, node labels and targets, intervals, probe `PROBE_VERSION`, git commit if available).
    4. Runs until `duration_sec` elapses or SIGINT/SIGTERM arrives, then calls `stop()` on all sessions and returns 0.
  - `summarize(out_dir: Path) -> dict` — keyed by node label:
    - `first`, `last`, `hours`
    - `counters_received`, `counters_expected` (`hours * 3600 / counter_interval_sec` from the manifest), `coverage_pct`
    - `max_gap_sec` (largest gap between consecutive `counters` receipts)
    - `sessions_started`, `losses_by_reason`, `rotations`, `reboots`
    - `probe_cpu_sec_per_hour` (computed per session from the first and last `counters.probe_self.cpu_seconds + children_cpu_seconds`, summed, divided by hours), `probe_maxrss_kb`
    - `census_scan_p50`, `census_scan_p95`, `census_scan_max`, `census_bytes_avg`, `process_count_avg`
  - CLI (`node_monitor/cli/main.py`): click group `cli`, with group `soak`:
    - `soak run [--config PATH] [--duration-hours FLOAT] [--full-census] [--out-dir DIR]`. The default out dir is `<working_dir>/logs/soak/<UTC %Y%m%dT%H%M%SZ>`; the command prints the out dir on start.
    - `soak report DIR [--format table|json]`. The table format is one aligned text row per node with these columns: `node, hours, coverage%, max_gap_s, sessions, losses, rotations, reboots, cpu_s/h, maxrss_MB, scan_p95_s, procs`.

- [ ] **Step 1: Write the failing tests**

```python
def test_recorder_summarizes_census_by_default(tmp_path):
   r = SoakRecorder(tmp_path)
   r.on_message("n1", census_msg(processes=[proc("a"), proc("a"), proc("b")]))
   r.close()
   line = json.loads(only_file(tmp_path).read_text().splitlines()[0])
   assert line["msg"]["process_count"] == 3 and line["msg"]["user_count"] == 2
   assert "processes" not in line["msg"] and line["msg"]["bytes"] > 0

def test_recorder_full_census_keeps_processes(tmp_path): ...

def test_summarize_coverage_gaps_losses_and_overhead(tmp_path):
   write_manifest(tmp_path, counter_interval_sec=10)
   write_lines(tmp_path, "n1", [
      counters_at(0, cpu=1.0), counters_at(10, cpu=1.1), counters_at(20, cpu=1.2),
      event_at(25, "session_lost", {"reason": "silence"}),
      event_at(60, "session_started", {"attempt": 2}),
      counters_at(60, cpu=0.0), counters_at(3600, cpu=3.0)])
   s = summarize(tmp_path)["n1"]
   assert s["max_gap_sec"] == 3540 and s["losses_by_reason"] == {"silence": 1}
   assert s["counters_received"] == 5
   assert s["probe_cpu_sec_per_hour"] == pytest.approx((0.2 + 3.0) / 1.0, rel=1e-3)

async def test_run_soak_end_to_end_with_fake_ssh(tmp_path, monkeypatch):
   cfg = config_for_fake_tree(tmp_path, nodes=2, counter=0.2, census=0.5)
   rc = await run_soak(cfg, tmp_path / "out", duration_sec=3, ssh_binary=FAKE_SSH)
   assert rc == 0
   s = summarize(tmp_path / "out")
   assert set(s) == {"n1", "n2"} and all(v["counters_received"] >= 5 for v in s.values())

def test_cli_report_table(tmp_path): ...   # CliRunner: `soak report DIR` exit 0, header contains "coverage%"
```

`config_for_fake_tree` builds a `Config` directly (`probe_python=sys.executable`, `working_dir=tmp_path`) and the test passes the fake roots and lock path as `probe_overrides`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/collector/test_soak.py -v` — Expected: FAIL.

- [ ] **Step 3: Implement `soak.py` and the CLI; document `soak run` / `soak report` in the README**

- [ ] **Step 4: Run the whole suite and lint**

Run: `pytest -q && ruff check . && ruff format --check .` — Expected: all pass.

- [ ] **Step 5: Short live run against the real nodes**

Create `~/.node_monitor.yaml` from `config.example.yaml` with the three Polaris nodes (`ssh_target: polaris-login-0N.head`, `identity_file: ~/.ssh/id_ed25519_polaris`), then run:

```bash
node-monitor soak run --duration-hours 0.05 && node-monitor soak report "$(ls -d ~/node-monitor/logs/soak/* | tail -1)"
```

Expected: three rows, coverage ≥ 95%, `losses` 0, `procs` ≈ 1,500–2,500 per node.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(collector): soak recorder, runner, and report CLI"
```

---

### Task 8: 24-hour soak on Polaris, fault checks, results

**Files:**
- Create: `docs/soak/2026-10-m0-results.md`

This task is operational. It runs on polaris-login-04 as `parton`.

**Matching probes precisely.** `pgrep -fa node-monitor-probe` also matches the collector's ssh clients, the ControlPersist masters and the very ssh command doing the check, so it over-counts. Everywhere in this task, list probes on a node with:

```bash
ssh polaris-login-0N.head "pgrep -u \$USER -fa '^/usr/bin/python3.11 -u -c #node-monitor-probe'"
```

or compare against the pid in `/dev/shm/node-monitor-$(id -u).lock` on that node.

**One probe per account.** No other default-lock probe may run on this account during the soak: no second `node-monitor soak run`, and no Task 5-style live check. Any of them takes over the soak's lock on that node and shows up as a loss.

- [ ] **Step 1: Start the soak detached**

Before launch, confirm `git status --short` prints nothing: the code soaked must be the committed code.

```bash
mkdir -p ~/node-monitor/logs
screen -dmS node-monitor-soak bash -lc 'PATH=~/node-monitor/venv/bin:$PATH env -u LD_PRELOAD node-monitor soak run --duration-hours 26 >> ~/node-monitor/logs/soak-console.log 2>&1'
```

`env -u LD_PRELOAD` is needed because login shells set the site's XALT LD_PRELOAD, which can abort the interpreter at exit.

Then check:

- `tail ~/node-monitor/logs/soak-console.log` shows the out dir (`OUT` below).
- `grep -E '"git_(commit|describe)"|probe_sha256' $OUT/manifest.json`: `git_describe` matches `git describe --always --dirty` and has no `-dirty` suffix. If it does, stop the soak (`screen -S node-monitor-soak -X quit`), commit or revert, and relaunch.
- `ls -ld $OUT; ls -l $OUT`: the dir is `drwx------` and the files are `-rw-------`.
- The precise pgrep above shows exactly one probe on each node.

Sample the collector's RSS hourly into a log, from a second window of the same screen session:

```bash
COLLECTOR=$(pgrep -u $USER -f '^[^ ]*python[^ ]* [^ ]*/node-monitor soak run')   # must be exactly one pid
screen -S node-monitor-soak -X screen bash -c "while sleep 3600; do date -u; ps -o rss= -p $COLLECTOR; done >> ~/node-monitor/logs/soak-rss.log"
```

- [ ] **Step 2: Fault checks (in the first 2 h; record times in the results doc)**

Target a node's ssh client by the pid in its latest `session_started` event, not by a pattern:

```bash
ssh_pid() { grep -h '"event": "session_started"' $OUT/$1-*.jsonl | tail -1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["detail"]["pid"])'; }
```

Cross-check with `pgrep -P $COLLECTOR -f polaris-login-0N`: the session's ssh clients are the collector's direct children. Never signal the ControlPersist master (`ssh: .../cm-... [mux]`, detached from the collector): it carries every session to that node.

  a. **Local SSH kill:** `kill $(ssh_pid login-01)` → expect `session_lost` then `session_started` for login-01 within about 10 s.
  b. **Remote probe kill:** `ssh polaris-login-02.head "pkill -u \$USER -f '^/usr/bin/python3.11 -u -c #node-monitor-probe'"` → expect an `error` message with `where` "signal" ("terminated by SIGTERM (15)"), then `session_lost` with reason `exit` and `exit_code` 143, and a reconnect. A probe killed with SIGKILL leaves no such message (`eof` or `exit` with a bare exit code) and is unattributable.
  c. **Hung transport:** `P=$(ssh_pid login-04); kill -STOP $P` → after about 30 s, expect `session_lost (silence)`. Then make sure the stopped client is gone: the session's SIGKILL normally ends it; if `ps -o pid,stat -p $P` still lists it, run `kill -CONT $P` (the session's pending SIGTERM then ends it) or `kill -KILL $P`. Then the precise pgrep on login-04 shows exactly one probe, the new one, because the stale probe was taken over or exited.
  d. **Lock safety:** confirm `/dev/shm/node-monitor-31112.lock` holds the current probe's PID on each node, and that it is the pid the precise pgrep shows.

- [ ] **Step 3: After ≥ 24 h, generate the report**

Run: `node-monitor soak report <out_dir>` and `node-monitor soak report <out_dir> --format json > <out_dir>/report.json`

The JSON report carries what the table does not: census coverage, `errors_by_where`, `loss_timeline` (each loss with its exit code and stderr tail), session crash, malformed-line and probe-stderr counts, `gpu_status_counts`, and the per-process delta extremes (`cpu_seconds_min`/`max`, `idle_for_max_sec`). Also stop the RSS sampler (`screen -S node-monitor-soak -X quit` once the run has ended) and keep `~/node-monitor/logs/soak-rss.log`.

- [ ] **Step 4: Write `docs/soak/2026-10-m0-results.md`**

Sections:
- Dates and hosts, plus the manifest's `git_describe` and `probe_sha256`
- The report table, verbatim
- Fault-check outcomes (expected vs observed)
- Any session losses not caused by the fault checks, with their reasons, exit codes and the `probe_stderr` lines
- Probe overhead: CPU seconds per hour and max RSS per node, plus census scan p95; collector RSS over time from `soak-rss.log`
- Node reboots or maintenance windows observed
- **Decision:** GO (proceed to M1 with streaming) or NO-GO (fall back to per-poll probe, spec §14), with the reasons

The results doc must also state:
- GPU metrics are unavailable to non-root users on Polaris login nodes (NVML initialization fails, so `gpu_status` is `error` then `suspended`), so the overhead figures exclude successful nvidia-smi calls.
- SIGKILL terminations are unattributable: only SIGTERM, SIGHUP and SIGINT leave an `error` with `where` "signal".
- Simultaneous silence losses on every node point to a collector-side stall (for example /home I/O), not to the nodes.
- `coverage.parent_unresolved` is always at least 1, because pid 1's parent is pid 0, which has no /proc entry.

GO criteria (spec §13 M0):
- coverage ≥ 99% outside the fault-check windows
- no unexplained probe terminations by site policy
- reconnects recover within backoff limits
- probe CPU < 60 CPU-seconds per hour per node and max RSS < 200 MB

- [ ] **Step 5: Commit**

```bash
git add docs/soak && git commit -m "docs: M0 soak results and go/no-go"
```
