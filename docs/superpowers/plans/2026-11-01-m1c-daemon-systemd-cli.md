# M1c — Daemon, systemd and CLI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run the collector unattended. A daemon streams every node through the M1a pipeline into the M1b database, survives SSH drops, node reboots and PostgreSQL outages, runs under a systemd user unit, and is inspected with read-only CLI commands.

**Architecture:**
- **Event loop thread:** `NodeSession`s run on the asyncio loop and feed per-node `NodePipeline`s. Their records go into a thread-safe `RecordQueue` that coalesces superseded live snapshots.
- **Writer thread:** a single thread owns the `BufferedWriter` (database or spool) and an `Upkeep` helper (heartbeat, daily partitions and retention). It submits durable records and live records as *separate* batches, so spool eviction can drop live-only batches first.
- **Shared session setup:** `soak` and `daemon` both use a `fleet` module that builds the sessions and handles signals.
- **CLI:** the read-side commands query the live tables through the reader role.

**Tech Stack:**
- Python ≥ 3.10 with asyncio and threading.
- psycopg 3 against PostgreSQL 18.
- click, plus `rich` for tables (as pbs_monitor uses).
- systemd user units; pytest; ruff.

**Spec:** `docs/superpowers/specs/2026-10-28-node-monitor-v2-design.md`, specifically:
- §3 Architecture and configuration
- §4 Collection and failure handling
- §6 Data model
- §8 CLI
- §9 Deployment
- §11 Robustness integration test
- §13 M1c exit criterion

**Inputs:**
- `docs/m1b-followups.md`, in particular its must-dos for M1c.
- `docs/m1b-database-validation.md`.

**Owner decision (2026-11-01):** `user_usage_5m` drops `by_category` and `by_dormancy`, because both can be derived exactly from `by_category_dormancy`. `node_usage_5m` keeps all its maps.

## Global Constraints

**Repo and environment**
- Repo: `~/node-monitor/node-monitor/`. Branch: `feat/m1c-daemon` from `main`.
- Venv: `~/node-monitor/venv`, an editable install of the repo. Prefix live commands with `env -u LD_PRELOAD`.
- 3-space indentation. `ruff check . && ruff format --check .` must pass, and `docs/` is never formatted.
- Python ≥ 3.10. Never use `asyncio.wait_for`; use the `_within` helper pattern in `collector/session.py`. The database layer stays synchronous; the daemon calls it only from the writer thread.

**Database**
- Never change `pg_hba.conf`, `postgresql.conf`, or any pbs_monitor database or role. Never touch `node_monitor_dev` or `node_monitor_web`.
- The collector writes with `database.url` (role `node_monitor_owner`). The CLI reads with `web.database_url` (role `node_monitor_reader`).
- PostgreSQL tests use the existing `pg_db` and `migrated_db` fixtures. They skip unless `NODE_MONITOR_TEST_ADMIN_URL` is set, and they create only `node_monitor_test_*` databases and `nm_test_*` roles.

**Collector behaviour (spec §4)**
- Intervals are bucketed on the collector's UTC clock: `recv = datetime.now(timezone.utc)` at line receipt.
- The collector never exits on database errors.
- Partially filled intervals are written at shutdown with reduced coverage (`sample_count` < `expected_count`).

**Files, dependencies and process**
- Spool directory: `<working_dir>/state/spool`.
- Daemon lock and info files: `<working_dir>/state/daemon.lock` and `<working_dir>/state/daemon.json` (mode 0600).
- Daemon log under systemd: `<working_dir>/logs/daemon.log`.
- New dependency: `rich>=13` in `pyproject.toml` `dependencies`.
- `process_samples`, `command_lines`, tags, the hourly census, the web server and its unit are M2/M3. Don't build them.
- Comments explain why, never task history. Tests describe behaviour and never assert on README or source text.
- Each commit message ends with `Co-Authored-By: Claude Code <noreply@anthropic.com>`. Never push.

## Review Focus

The five failure modes the spec implies that are most likely to bite, each pinned by a test in the task that owns it:

1. **PostgreSQL down when the daemon starts** (a boot race with pbs-postgres). The daemon must start, spool, and catch up once the database answers, without a restart. Test in Task 6.
2. **SIGTERM during an outage with records still queued.** The daemon must exit within 30 s, with every durable record either written or in the spool. Test in Task 6.
3. **A node whose session has died, or that was removed from the config.** Its `live_nodes` row stays. `status` and `nodes` must show it as `stale` rather than as healthy. Test in Task 8.
4. **A second daemon started by hand while the systemd one runs.** It must refuse with the running instance's pid and host, and must not take over the probes. Test in Task 7.
5. **CLI with an unreachable database or a missing `web.database_url`.** The CLI must give a one-line error and exit 1, with no traceback. Test in Task 8.

---

### Task 1: Migration 0002 — per-user maps and an event natural key

**Files:**
- Create: `node_monitor/database/migrations/0002_user_maps_and_event_key.sql`
- Modify:
  - `node_monitor/database/migrate.py` (`REQUIRED_VERSION = 2`)
  - `node_monitor/pipeline/usage.py`
  - `node_monitor/database/writer.py` (event insert)
  - `docs/superpowers/specs/2026-10-28-node-monitor-v2-design.md` §6
- Test: `tests/database/test_migrate.py`, `tests/database/test_writer.py`, `tests/pipeline/test_usage.py`

**Interfaces:**
- `USER_USAGE_COLUMNS` no longer contains `by_category` or `by_dormancy`. `NODE_USAGE_COLUMNS` is unchanged.
- User rows from `UsageAggregator` omit those two keys; node rows keep them.
- Constraint name: `collection_events_natural_key`.

- [ ] **Step 1: Write the failing tests.**
  - `test_migration_0002_drops_user_maps_and_dedupes_events`:
    - Migrate a `pg_db` to version 1 only. Apply 0001 through the runner with `MIGRATIONS` truncated by monkeypatch, or by calling the SQL directly; pick whichever matches how `test_migrate.py` already builds versions.
    - Insert two identical `collection_events` rows (same system, node, ts and event_type, both with `node` NULL), plus one that differs only in `event_type`.
    - Run `migrate`.
    - Assert `user_usage_5m` has no `by_category` or `by_dormancy` column, and `node_usage_5m` still has both.
    - Assert two event rows remain and the lower `id` of the duplicate pair survived.
  - `test_writer_events_are_idempotent`: write the same batch of two events twice, one with node `h1` and one with node `None`. Assert exactly 2 rows.
  - `test_user_rows_omit_derivable_maps`: user rows from `UsageAggregator` have no `by_category`/`by_dormancy` keys, and node rows still do. Summing `by_category_dormancy[cat]` over its dormancy keys equals the node row's `by_category[cat]` for `count`, `cpu_s` and `rss_kb` (within 1e-9) on a two-user census fixture.
  - Update the existing table-columns-versus-pipeline-tuples test so it holds after the change.
- [ ] **Step 2: Run the tests and confirm they fail** (`pytest tests/database tests/pipeline/test_usage.py -q` with PG).
- [ ] **Step 3: Implement.** The migration:
  1. `ALTER TABLE node_monitor.user_usage_5m DROP COLUMN by_category, DROP COLUMN by_dormancy`.
  2. Delete duplicate events, keeping the lowest `id` per (system, node, ts, event_type), with `node` compared using `IS NOT DISTINCT FROM`.
  3. `ALTER TABLE node_monitor.collection_events ADD CONSTRAINT collection_events_natural_key UNIQUE NULLS NOT DISTINCT (system, node, ts, event_type)`.

  The writer inserts events with `ON CONFLICT ON CONSTRAINT collection_events_natural_key DO NOTHING`. The migration names no roles (ruling R1).
- [ ] **Step 4: Update the spec.**
  - §6 `user_usage_5m`: its maps are `by_activity`, `by_tag` and `by_category_dormancy`. Add one sentence saying `by_category` and `by_dormancy` are the per-key sums of `by_category_dormancy`.
  - §6 Ops row: `collection_events` has the natural key (system, node, ts, event_type).
- [ ] **Step 5: Run the full suite with PG, plus ruff. Commit** `feat(database): migration 0002 drops derivable per-user maps and dedupes events`.

### Task 2: Head-of-line cap for spooled batches

A spooled batch that fails deterministically with an error classed as an outage (53200 out of memory, 53400 `temp_file_limit`) blocks replay forever while the spool fills.

**Files:**
- Modify: `node_monitor/database/writer.py`, `node_monitor/database/buffered.py`
- Test: `tests/database/test_buffered.py`, `tests/database/test_writer.py`

**Interfaces:**
- `DatabaseWriter.ping() -> bool`: runs `SELECT 1` on the writer's connection, reconnecting as needed. Never raises.
- `BufferedWriter(writer, spool, *, system, backoff=(5, 300), max_replay_attempts: int = 3)`.

- [ ] **Step 1: Write the failing tests.**
  - `test_spooled_batch_failing_while_database_answers_is_quarantined_after_3_attempts`:
    - Use a fake writer whose `write` raises the outage error for batch B only, and whose `ping()` returns True.
    - Spool batches A, B and C.
    - Call `submit` repeatedly, advancing `now` past each `next_retry`.
    - After the third failure of B: B's file is in `corrupt/`, and A, C and the fresh batch are written in order.
    - The events include `spool_batch_quarantined` with `detail["reason"] == "repeated failure while the database answered"` and `detail["attempts"] == 3`.
  - `test_spooled_batch_is_never_quarantined_while_ping_fails`: as above, but `ping()` returns False. After 10 failing attempts nothing is in `corrupt/` and B is still pending.
  - `test_ping_reports_reachability` (PG): True against `migrated_db`, False against `postgresql://localhost:1/x` with `connect_timeout_sec=1`.
- [ ] **Step 2: Run the tests and confirm they fail.**
- [ ] **Step 3: Implement.**
  - In `_replay`, an outage-class failure of a spooled batch calls `writer.ping()`.
    - If the ping succeeds, it counts an attempt for that file (in memory, keyed by path). It quarantines the file at `max_replay_attempts`, then continues replay with the next file in the same submit.
    - Otherwise it is an outage, exactly as today.
  - The attempt count resets when the file is written or removed.
- [ ] **Step 4: Run the full suite with PG, plus ruff. Commit** `fix(database): quarantine a spooled batch that keeps failing while the database answers`.

### Task 3: Shared session runner (`fleet`) and version string

**Files:**
- Create: `node_monitor/collector/fleet.py`, `node_monitor/version.py`
- Modify: `node_monitor/collector/soak.py` (use both; behaviour unchanged)
- Test: `tests/collector/test_fleet.py`; the existing `tests/collector/test_soak.py` must pass unchanged, except for imports.

**Interfaces:**
- `build_sessions(config: Config, *, on_message: Callable[[str, dict], None], on_event: Callable[[str, str, dict], None], ssh_binary: str = "ssh", probe_overrides: dict | None = None) -> dict[str, NodeSession]`
  - Writes `state/ssh_config` and builds one session per node, keyed by label.
  - A `lock_path` override gets a `.<label>` suffix, as `run_soak` does today.
- `async run_sessions(sessions: dict[str, NodeSession], *, duration_sec: float | None = None, stop: asyncio.Event | None = None, on_crash: Callable[[str, BaseException], None]) -> None`
  - Runs every session.
  - Returns when `duration_sec` elapses, when `stop` is set, when SIGINT or SIGTERM arrives (handlers are installed and removed as in `run_soak`), or when all sessions end.
  - Then calls `stop()` on every session and gathers them.
  - A session whose `run()` raises is reported through `on_crash` and doesn't stop the others.
- `version.describe() -> str`: `"<__version__>+<git describe --always --dirty>"`, or `__version__` alone outside a checkout. The `_git`/`_git_describe` helpers move here from `soak.py`.

- [ ] **Step 1: Write the failing tests.**
  - `test_run_sessions_returns_when_stop_is_set`: use two fake sessions whose `run()` waits until their `stop()` is called. Set `stop` after 0.2 s. `run_sessions` returns within 2 s, both fakes were stopped, and no signal handler is left installed (`signal.getsignal(SIGTERM)` is restored).
  - `test_run_sessions_reports_a_crashed_session`: one fake raises `RuntimeError("boom")` from `run()`. `on_crash` receives `("n1", exc)`, and the other session keeps running until `stop`.
  - `test_build_sessions_suffixes_lock_path_per_node`.
  - `test_describe_outside_checkout_is_bare_version`.
- [ ] **Step 2: Run the tests and confirm they fail.**
- [ ] **Step 3: Implement both modules and switch `run_soak` to them.**
- [ ] **Step 4: Run the full suite, plus ruff. Commit** `refactor(collector): share session setup and signal handling between soak and daemon`.

### Task 4: `RecordQueue` — thread-safe hand-off with live coalescing

**Files:**
- Create: `node_monitor/collector/record_queue.py`
- Test: `tests/collector/test_record_queue.py`

**Interfaces:**
- `RecordQueue()` with:
  - `put(records: list[Record]) -> None`
  - `take(timeout: float) -> tuple[list[Record], list[Record]]`
  - `close() -> None`
  - `closed: bool`
  - `depth() -> dict`
- `take` returns `(durable, live)`:
  - It blocks until records are available, `close()` is called, or `timeout` elapses.
  - `durable` holds every non-live record in put order.
  - `live` holds at most one `LIVE_PROCESSES` record per node (the latest put) and one `LIVE_NODE` record per node. The `LIVE_NODE` record merges that node's partial rows in put order, so later keys win.
  - Node identity is `row["node"]`.
- After `close()`, `take` returns whatever is left without blocking, and then `([], [])`.
- `depth()` returns `{"durable": int, "live": int}`.

- [ ] **Step 1: Write the failing tests.**
  - `test_live_processes_coalesce_to_latest_per_node`
  - `test_live_node_partials_merge_in_order`: a full row at t1, then a session-lost partial at t2 `{updated_at: t2, session_state: "down"}`. The result keeps t1's gauges with `session_state == "down"` and `updated_at == t2`.
  - `test_durable_records_keep_put_order`
  - `test_take_times_out_empty`: returns `([], [])` after about the timeout.
  - `test_put_from_another_thread_wakes_take`: within 0.5 s.
  - `test_close_wakes_a_blocked_take_and_drains`
- [ ] **Step 2: Run the tests and confirm they fail.**
- [ ] **Step 3: Implement** with a `threading.Condition`.
- [ ] **Step 4: Run the full suite, plus ruff. Commit** `feat(collector): record queue that coalesces superseded live snapshots`.

### Task 5: `Upkeep` and `WriterThread`

**Files:**
- Create: `node_monitor/collector/upkeep.py`, `node_monitor/collector/writer_thread.py`
- Test: `tests/collector/test_upkeep.py` (PG), `tests/collector/test_writer_thread.py` (fakes, no PG)

**Interfaces:**
- `Upkeep(url: str, *, system: str, retention: RetentionConfig, connect_timeout_sec: float, collector_host: str, version: str, started_at: datetime, heartbeat_interval_sec: float = 60, maintenance_interval_sec: float = 86400, retry_interval_sec: float = 3600)`
  - **`tick(now: datetime, status: dict) -> list[Record]`:**
    - When due, it writes the heartbeat with `maintenance.write_heartbeat`.
    - When due, it runs `maintenance.run`. Maintenance is due at the first tick, then every `maintenance_interval_sec`. When the last run raised or returned a non-empty `skipped`, it is due again after `retry_interval_sec` instead.
    - It returns collector-level `collection_event` records (`node` None):
      - `partitions_maintained` with `{created, dropped, skipped}`, when any list is non-empty.
      - `maintenance_failed` with `{error}`.
    - A failed heartbeat returns no event, because `BufferedWriter` already reports outages. It is logged once per outage.
    - Never raises.
    - It uses its own autocommit connection from `connect()`, reopened after any error.
  - **`close(now: datetime, status: dict) -> None`:** writes a last heartbeat with `status["state"] = "stopped"`, then closes. Never raises.
- `WriterThread(queue: RecordQueue, buffered: BufferedWriter, upkeep: Upkeep, *, status_fn: Callable[[], dict], clock: Callable[[], datetime], flush_interval_sec: float = 5.0)`, a daemon-less `threading.Thread`.
  - **Each loop:**
    1. `take(flush_interval_sec)`.
    2. Submit `carried_events + durable` as one batch, if non-empty.
    3. Submit `live` as a separate batch, if non-empty.
    4. Collect the events returned by both submits, plus those from `upkeep.tick(clock(), status_fn())`, into the next round's `carried_events`.
  - **Errors:** an exception from `submit` is logged with its traceback and never ends the thread. The `BufferedWriter` already carries its events.
  - **Exit:** once the queue is closed and drained, it runs at most 3 more rounds to write carried events, then calls `upkeep.close(clock(), status_fn())` and `buffered`'s writer `close()`.
  - **`status() -> dict`:** `buffered.status()` plus `{"queue": queue.depth(), "last_write_at": iso | None}`.

- [ ] **Step 1: Write the failing tests.**
  - `test_tick_writes_heartbeat_and_maintains_partitions_once` (PG):
    - The first tick upserts `collector_heartbeat` with the given status and returns a `partitions_maintained` event.
    - A second tick 10 s later neither changes `last_seen` nor returns an event.
    - A tick 61 s later updates `last_seen`.
  - `test_tick_with_unreachable_database_returns_nothing_and_retries`: url `postgresql://localhost:1/x`, `connect_timeout_sec=1`.
  - `test_skipped_retention_is_retried_after_retry_interval`: monkeypatch `maintenance.run` to return `skipped: ["x"]`. It runs again at +3600 s, not +86400 s.
  - `test_durable_and_live_are_separate_submits_durable_first` (fake `BufferedWriter` recording its calls).
  - `test_returned_events_are_submitted_next_round`
  - `test_submit_exception_does_not_stop_the_thread`
  - `test_close_drains_queue_and_stops_within_5s`
- [ ] **Step 2: Run the tests and confirm they fail.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run the full suite with PG, plus ruff. Commit** `feat(collector): writer thread with heartbeat and daily maintenance`.

### Task 6: `Daemon` — wiring, startup and shutdown; robustness integration test

**Files:**
- Create: `node_monitor/collector/daemon.py`
- Test: `tests/collector/test_daemon.py` (PG; fake ssh from `tests/collector/fake_ssh.py`; fake `/proc` from `tests/probe/fake_proc.py`)

**Interfaces:**
- `Daemon(config: Config, *, ssh_binary: str = "ssh", probe_overrides: dict | None = None, record_tap: Callable[[Record], None] | None = None, db_backoff: tuple[float, float] = (5, 300), flush_interval_sec: float = 5.0)`
  - `db_backoff` goes to `BufferedWriter`, and `flush_interval_sec` to `WriterThread`. Tests pass `(0.5, 2)` and `0.5`, so outages and recoveries resolve within seconds.
  - It builds every component in `__init__`:
    - one `NodePipeline` per node
    - `RecordQueue`
    - `DatabaseWriter(config.database.url, retention=…, connect_timeout_sec=…)`
    - `Spool(<working_dir>/state/spool, max_bytes=config.spool.max_bytes)`
    - `BufferedWriter`
    - `Upkeep` (with `version.describe()` and `socket.getfqdn()`)
    - `WriterThread`
    - sessions via `build_sessions`

    So a test can change `daemon.database_writer.url` before or during `run`.
  - A missing `config.database.url` raises `ConfigError("database.url is not set")`.
- Public attributes: `database_writer: DatabaseWriter` and `sessions: dict[str, NodeSession]`.
- `request_stop() -> None` (call it from the loop thread).
- `async run(duration_sec: float | None = None) -> int`:
  1. **Schema check.** `check_schema` with `config.database.url`. `SchemaOutOfDate` logs an error naming `node-monitor database migrate` and returns 2. `DatabaseUnavailable` logs a warning and continues: the spool covers the outage.
  2. **Start.** Start the writer thread and put a `collector_started` event `{version, host, pid}`.
  3. **Run** `run_sessions(...)`.
     - `on_message(label, msg)` stamps `recv = now UTC` and runs `pipelines[label].on_message`. The records go to `record_tap`, if set, and then to `queue.put`.
     - `on_event` works the same way through `pipelines[label].on_event`.
     - An exception from a pipeline is logged and becomes a `pipeline_error` event `{type, error}` for that node. It never propagates into the session.
     - Per-label session state (`up` after `session_started`, `down` after `session_lost`) feeds the heartbeat `status`: `{"state": "running", "sessions": {...}, **writer_thread.status()}`.
     - `on_crash` puts a `session_crashed` event.
  4. **Shutdown.** Flush every pipeline into the queue and put `collector_stopped`. Close the queue, join the writer thread (`asyncio.to_thread`, at most 60 s), and return 0.

- [ ] **Step 1: Write the failing tests.**
  - **Shared setup:** two nodes on fake ssh with `counter=0.2`, `census=1.0`, `summary=2.0`, `silence=5` and `backoff=(0.05, 0.4)`. Each node gets its own fake tree, so its `boot_id` file can change. `database.url` is the `migrated_db` URL. `record_tap` collects records.
  - **`test_daemon_survives_kill_reboot_and_outage`**, in this order:
    1. Wait (polling, at most 20 s) for `live_nodes` rows for both nodes.
    2. SIGKILL n1's ssh process group (`os.killpg(daemon.sessions["n1"].current_pid, SIGKILL)`). Wait for a second `session_started` event for n1 in the database.
    3. Rewrite n2's `boot_id`, then kill n2's ssh. Wait for a `node_rebooted` event for n2.
    4. Set `daemon.database_writer.url = "postgresql://localhost:1/x"`. Wait until the spool has a batch, then restore the URL. Wait for `database_recovered`.
    5. `request_stop()`. `run` returns 0 within 30 s.
    6. **Assert:**
       - Every tapped `node_metrics_1m`, `user_usage_5m` and `node_usage_5m` record has a database row with the same key. This is "no loss through the outage".
       - The spool holds no batches.
       - `collection_events` contains `collector_started`, `session_lost` (n1), `node_rebooted` (n2), `database_unavailable`, `database_recovered` and `collector_stopped`.
       - `collector_heartbeat` has `status->>'state' = 'stopped'`.
  - **`test_daemon_started_while_database_down_catches_up`** (Review Focus 1):
    - Construct the daemon from a config whose `database.url` is unreachable (`dataclasses.replace`), so the schema check and the writer both fail.
    - After about 3 s, set `daemon.database_writer.url` to the `migrated_db` URL.
    - Wait for rows in `node_metrics_1m` and an empty spool, then stop. `run` returned 0 and never exited early.
  - **`test_sigterm_during_outage_spools_everything`** (Review Focus 2):
    - Writer URL unreachable from the start.
    - Run for about 4 s, then `request_stop()`. `run` returns within 30 s.
    - The durable records found in `Spool.pending()` batches equal, as a multiset of (kind, key), the tapped durable records.
  - **`test_out_of_date_schema_refuses_to_start`:** use a `pg_db` with no migrations. `run` returns 2, and the log names `node-monitor database migrate`.
- [ ] **Step 2: Run the tests and confirm they fail.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run the full suite with PG, plus ruff. The daemon tests must take under 90 s together. Commit** `feat(collector): daemon wiring sessions, pipelines and the buffered writer`.

### Task 7: Daemon CLI, `config --create`, `database wait`, systemd units

**Files:**
- Create:
  - `node_monitor/collector/instance.py`
  - `node_monitor/config_template.yaml` (moved from `config.example.yaml`, which is deleted; add it to package data)
  - `deploy/systemd/node-monitor-daemon.service`
  - `deploy/systemd/rearm.sh` (executable)
  - `deploy/systemd/README.md`
- Modify:
  - `node_monitor/cli/main.py`: `daemon` group, `config` group, `database wait`, and an `if __name__ == "__main__": cli()` guard
  - `README.md`
  - the spec, §9
- Test: `tests/collector/test_instance.py`, `tests/test_cli_daemon.py`, `tests/test_cli_config.py`

**Interfaces:**
- **`InstanceLock(working_dir: Path)`:**
  - `acquire() -> None`:
    - Takes `fcntl.flock(LOCK_EX | LOCK_NB)` on `state/daemon.lock`.
    - Writes `state/daemon.json` (mode 0600): `{pid, host, started_at, systemd}`, where `systemd` is `"INVOCATION_ID" in os.environ`.
    - Raises `AlreadyRunning(info: dict)` if the lock is held.
  - `release() -> None`: removes `daemon.json` and unlocks.
- **`read_info(working_dir) -> dict | None`** and **`is_alive(info) -> bool`**: true only for the same host and a live pid.
- **CLI:**
  - **`daemon start [--foreground] [--config PATH]`.**
    - *With `--foreground`:* configure logging to stderr as `%(asctime)s %(levelname)s %(name)s: %(message)s` with UTC times. Acquire the lock, run `asyncio.run(Daemon(config).run())`, release, and exit with its code. `AlreadyRunning` prints `node-monitor daemon already running (pid P on HOST)` and exits 1.
    - *Without `--foreground`:* refuse the same way if alive. Otherwise spawn `[sys.executable, "-m", "node_monitor.cli.main", "daemon", "start", "--foreground", (--config PATH)]` with `start_new_session=True` and stdin `/dev/null`, appending stdout and stderr to `<working_dir>/logs/daemon.log` (the directory is created with mode 0700). Wait 2 s. If the child has exited, print the log's last 20 lines and exit 1; otherwise print its pid.
  - **`daemon stop [--timeout 60] [--force] [--config]`:**
    - Not running: say so and exit 0.
    - Another host: error and exit 1.
    - `systemd` true and no `--force`: print `managed by systemd: systemctl --user stop node-monitor-daemon` and exit 1.
    - Otherwise send SIGTERM and wait for exit. A timeout exits 1.
  - **`daemon status [--config]`:**
    - Shows the local process (pid, host, age, systemd) or "not running".
    - Shows the heartbeat from `web.database_url`: last_seen age, version, connected, spooled batches and per-node sessions. "database unreachable" when it is.
    - Exit 0 when the local process is alive or the heartbeat is younger than 180 s; else 1.
  - **`config --create [PATH]`:** writes the template to PATH (default `~/.node_monitor.yaml`) with mode 0600. Refuses with exit 1 if the file exists.
  - **`database wait [--timeout 60] [--config]`:** tries to connect with `database.url` every 2 s. Exit 0 on the first success, 1 at the timeout.
- **`node-monitor-daemon.service`:**
  - `Type=simple`
  - `ExecStartPre=-%h/node-monitor/venv/bin/node-monitor database wait --timeout 60`. The leading `-` means a database that stays down never blocks the start (spec §9: the daemon tolerates its absence).
  - `ExecStart=%h/node-monitor/venv/bin/node-monitor daemon start --foreground`
  - `WorkingDirectory=%h/node-monitor`
  - `Environment=PATH=%h/node-monitor/venv/bin:/usr/local/bin:/usr/bin:/bin`
  - `Restart=always`, `RestartSec=15`, `StartLimitIntervalSec=0`, `TimeoutStopSec=90`, `KillMode=mixed`
  - `StandardOutput=append:%h/node-monitor/logs/daemon.log`, and the same for `StandardError`
  - `After=network-online.target pbs-postgres.service`, and no `Requires=`
  - `WantedBy=default.target`
- **`rearm.sh`:** pbs_monitor's pattern, reduced to this unit: `loginctl enable-linger`, `daemon-reload`, `enable` and `start` `node-monitor-daemon.service`, then print the `Loaded:`/`Active:` lines and the linger state. Idempotent, `set -euo pipefail`.
- **`deploy/systemd/README.md`:**
  - Install: `mkdir -p ~/node-monitor/logs`, copy the unit into `~/.config/systemd/user/`, `daemon-reload`, `enable --now`, then check linger.
  - Checking: `systemctl --user status`, `node-monitor daemon status`, tail the log.
  - Stopping.
  - Pitfalls: systemd's clean environment, so `PATH` is explicit; the SSH key must work without an agent; a soak and the daemon must not run at once, because they take over each other's probes; log growth.

- [ ] **Step 1: Write the failing tests.**
  - `test_second_acquire_raises_already_running_with_info` (Review Focus 4): acquire in a child process (`multiprocessing`), then acquire in the test. `AlreadyRunning.info["pid"]` is the child's pid.
  - `test_release_removes_info_file`
  - `test_daemon_start_refuses_when_running` (CliRunner, lock held by a child): exit 1, and the message contains the pid.
  - `test_daemon_stop_not_running_exits_0`
  - `test_daemon_stop_refuses_systemd_managed_without_force`
  - `test_daemon_stop_terminates_a_foreground_child`: start a `sleep`-like child that holds the lock and writes info. `stop` ends it within the timeout.
  - `test_config_create_writes_0600_and_refuses_overwrite`: the written file loads with `load_config`.
  - `test_database_wait_times_out_on_unreachable_url`: `--timeout 3`, exit 1, within 6 s.
- [ ] **Step 2: Run the tests and confirm they fail.**
- [ ] **Step 3: Implement.** Then run `systemd-analyze --user verify deploy/systemd/node-monitor-daemon.service` and record the output in the task report. Warnings about `%h` paths that don't exist yet are acceptable.
- [ ] **Step 4: Update the spec, §9:** the `ExecStartPre` uses `database wait` with a `-` prefix, and the web unit arrives with M2.
- [ ] **Step 5: Run the full suite, plus ruff. Commit** `feat(cli): daemon start/stop/status, config --create, systemd unit`.

### Task 8: Read queries, output formatting, `status` and `nodes`

**Files:**
- Create: `node_monitor/database/queries.py`, `node_monitor/cli/output.py`, `node_monitor/cli/live.py`
- Modify: `node_monitor/cli/main.py` (register the live commands), `pyproject.toml` (`rich>=13`)
- Test: `tests/database/test_queries.py` (PG), `tests/test_cli_output.py`, `tests/test_cli_live.py` (PG)

**Interfaces:**
- **`queries.py`.** Each function takes a psycopg connection and returns plain dicts. Every value is a bound parameter, and sort keys come from a whitelist (`ValueError` otherwise).
  - `heartbeat(conn, system) -> dict | None`
  - `node_states(conn, system) -> list[dict]`:
    - One per `live_nodes` row, ordered by label.
    - Keys: `node, label, session_state, updated_at, last_census_at, load1, mem_used_kb, mem_total_kb, procs_total, probe_version, boot_id, user_processes, users`.
    - The label comes from `live_nodes.label`, falling back to `nodes.label`, then the host.
    - The last two keys are counted from `live_processes` with `is_user_process`.
  - `processes(conn, system, *, user=None, node=None, category=None, user_only=True, idle_min_sec=None, presence=None, sort="cpu", reverse=False, limit=None) -> list[dict]`:
    - `node` matches either the host or the label.
    - Sorts: `cpu→cpu_seconds`, `rss→rss_kb`, `idle→idle_for_sec`, `threads→threads`, `gpu→gpu_mem_mb`. Descending by default, NULLs last; `reverse` flips the order.
    - `idle_min_sec` also requires `cpu_state = 'idle'`.
    - Keys: `node, label, username, pid, category, activity, cpu_seconds, rss_kb, gpu_mem_mb, threads, state, cpu_state, idle_for_sec, idle_for_is_lower_bound, idle_bucket, owner_presence, cmdline, ts`.
  - `users(conn, system, *, node=None, category=None, sort="rss", reverse=False, limit=None) -> list[dict]`:
    - Covers user processes only.
    - Sorts: `rss, cpu, procs, gpu, idle`.
    - Keys: `username, processes, nodes, cpu_seconds, rss_kb, gpu_mem_mb, idle_processes, top_category`. `top_category` is the category holding the most of that user's RSS.
- **`output.py`.**
  - `Column(key: str, header: str, fmt: Callable[[Any], str] | None = None, justify: str = "left")`.
  - `render(rows: list[dict], columns: list[Column], fmt: str, *, title: str | None = None, file=None) -> None`:
    - `table`: a rich `Table`, with columns formatted.
    - `json`: a JSON list of the rows restricted to the column keys, raw values, datetimes as ISO.
    - `csv`: a header of column keys, raw values.
  - Helpers:
    - `fmt_kb(kb)`: binary units, e.g. `512 KiB`, `1.5 GiB`.
    - `fmt_age(ts, now)`: e.g. `42s`, `3m`, `2h05m`, `4d`.
    - `fmt_duration(sec, lower_bound=False)`: same units, prefixed `≥` for a lower bound.
    - `fmt_pct(x)`.
- **`live.py`.**
  - A shared decorator adds `--config` and `--format [table|json|csv]`.
  - `_reader(config)`:
    - Missing `web.database_url` raises `click.ClickException("web.database_url is not set in <config path>")`.
    - `DatabaseUnavailable` raises `click.ClickException("database unreachable: <reason>")`.
  - `STALE_AFTER_SEC = 120` for node rows, `HEARTBEAT_STALE_SEC = 180` for the collector.
  - **`status`:**
    - A collector block: host, version, last_seen age with `STALE` past 180 s, connected, spooled batches, outage_since.
    - A node table: label; state, which is `stale` when `updated_at` is older than 120 s and `session_state` otherwise; updated age; census age; probe version.
    - Exit 1 when the heartbeat is missing or stale, else 0.
    - JSON: `{"collector": {...}, "nodes": [...]}`.
  - **`nodes`:** label, state (same rule), load1, memory used/total with %, processes, user processes, users, census age.

- [ ] **Step 1: Write the failing tests.**
  - **Fixture:** populate `migrated_db` with `DatabaseWriter`, using two nodes, six processes across three users with mixed categories and idle states, and a heartbeat.
  - **Queries:**
    - `test_node_states_counts_users_and_user_processes`
    - `test_processes_filters_by_label_or_host_user_category`
    - `test_processes_sort_reverse_limit_nulls_last`
    - `test_processes_idle_filter_requires_idle_state`
    - `test_users_aggregates_and_top_category`
    - `test_unknown_sort_raises`
  - **Output:**
    - `test_render_json_restricts_to_column_keys_and_isoformats`
    - `test_render_csv_header_and_raw_values`
    - `test_render_table_contains_headers_and_formatted_values`: use a rich Console with a fixed width and `record=True`, or capture the file.
    - `test_fmt_helpers`: `fmt_kb(1536) == "1.5 MiB"`, `fmt_duration(3600, True) == "≥1h00m"`, `fmt_age` at its boundaries.
  - **CLI:**
    - `test_status_fresh_heartbeat_exit_0`
    - `test_status_stale_heartbeat_exit_1`
    - `test_status_and_nodes_mark_stale_node` (Review Focus 3): one `live_nodes` row has `updated_at` 10 minutes old and `session_state='up'`, and shows as `stale`.
    - `test_cli_unreachable_database_is_one_line_exit_1` (Review Focus 5): URL `postgresql://localhost:1/x`. The output contains `database unreachable` and has no `Traceback`.
    - `test_cli_missing_web_url_exit_1`
- [ ] **Step 2: Run the tests and confirm they fail.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run the full suite with PG, plus ruff. Commit** `feat(cli): status and nodes from the live tables`.

### Task 9: `top`, `users`, `user`, `idle`

**Files:**
- Modify: `node_monitor/cli/live.py`, `README.md` (CLI section)
- Test: `tests/test_cli_live.py`

**Interfaces** (Task 8 provides `queries.processes`, `queries.users`, `render`, `Column`, the format helpers and `_reader`):
- **Common options:** `-n/--node`, `-c/--category`, `--sort`, `--reverse`, `--limit` (default 20; 0 means all), and `--format`.
- **`top [-u/--user]`:**
  - Covers user processes, sorted by `cpu` by default.
  - Columns: node (label), user, pid, category, activity, CPU s/min (`cpu_seconds`), RSS, state, idle (`fmt_duration` with the lower-bound mark), presence, command.
  - The command is truncated to 60 characters in table output and kept whole in json/csv.
- **`users`:**
  - Sorted by `rss` by default.
  - Columns: user, procs, nodes, CPU s/min, RSS, GPU MiB, idle procs, top category.
- **`user USERNAME`:**
  - Shows every process of that user (`user_only=False`), whatever its category.
  - First a summary line: processes, nodes and total RSS.
  - Exit 1 with `no live processes for USERNAME` when there are none.
- **`idle [--min-idle 1h] [--any-presence]`:**
  - Shows user processes that are idle for at least `--min-idle` (accepts `30m`, `2h`, `1d`, or plain seconds) and whose owner is `absent`, unless `--any-presence` is given.
  - Sorted by `rss` by default.
  - Columns: node, user, pid, category, activity, idle, RSS, presence, command.

- [ ] **Step 1: Write the failing tests** against the Task 8 fixture:
  - `test_top_default_sort_and_limit`
  - `test_top_user_filter`
  - `test_top_json_keeps_full_command`
  - `test_users_sorted_by_rss`
  - `test_user_shows_all_categories_and_summary`
  - `test_user_unknown_exit_1`
  - `test_idle_threshold_and_absent_only`
  - `test_idle_any_presence`
  - `test_min_idle_parsing_rejects_garbage`: click error, exit 2.
- [ ] **Step 2: Run the tests and confirm they fail.**
- [ ] **Step 3: Implement, and add the CLI section to the README.**
- [ ] **Step 4: Run the full suite with PG, plus ruff. Commit** `feat(cli): top, users, user and idle`.

### Task 10: Real-server smoke on polaris-login-04

Run these on polaris-login-04 with `env -u LD_PRELOAD`. Delete every artefact that holds other users' command lines once it has been read.

- [ ] **Step 1: Migrate.** Run `node-monitor database migrate` (expect `applied: 0002`), then `database status`. The duplicate `collection_events` from the M1b re-replay must be gone.
- [ ] **Step 2: No stray probes.** Confirm no soak or probe is running: check `ps` on each node via the project ssh_config, and the probe lock in `/dev/shm`.
- [ ] **Step 3: Install and start under systemd from the branch checkout.** Follow `deploy/systemd/README.md`: `systemctl --user enable --now node-monitor-daemon`. Confirm `Linger=yes`.
- [ ] **Step 4: 30-minute smoke.** Record each of these:
  - `node-monitor daemon status` and `status`: fresh heartbeat, 3 nodes `up`.
  - `nodes`, `users --limit 5` (anonymise when recording), `idle --limit 5` and `top --limit 5`. Record counts only, no command lines.
  - `node_metrics_1m` grows by 3 rows per minute. `user_usage_5m` gets about 675 rows per 5-minute interval, with `sample_count == expected_count`.
  - Daemon RSS and CPU (`ps -o rss,etime,time`), probe RSS on each node, and how fast the log grows.
  - `pg_stat_user_tables` `n_dead_tup` and `last_autovacuum` for `live_processes`.
- [ ] **Step 5: Crash recovery.** `systemctl --user kill -s SIGKILL node-monitor-daemon`. Confirm it restarts within about 20 s, emits `collector_started`, and that the interrupted interval's rows have `sample_count < expected_count` or were completed by the new process.
- [ ] **Step 6: Stop and disable.** `systemctl --user disable --now node-monitor-daemon`, because the unit runs whatever is checked out. It is re-enabled from `main` after the merge (see below).
- [ ] **Step 7: Write `docs/m1c-smoke.md`** with the measurements (no usernames, no command lines). Commit `docs: M1c smoke on polaris-login-04`.

### After the merge: the one-week exit criterion (spec §13)

These are operations, not tasks for an implementer.

1. On `main`, run `systemctl --user enable --now node-monitor-daemon`.
2. Check daily: heartbeat, gaps (`collection_events` `session_lost`), spool, daemon/probe RSS, `live_processes` dead tuples and log size.
3. **Deliberate PostgreSQL outage: deferred (owner, 2026-11-01).** It must not disrupt PBS Monitor for now, so it is not part of this week. Task 6's integration test covers the behaviour in code. If `pbs-postgres` restarts during the week for another reason, record what the daemon did: `database_unavailable`, `database_recovered`, and any gaps in `node_metrics_1m`. Run the deliberate test later, when the owner chooses. One option is the `node_monitor`-only cut-off (`ALTER DATABASE node_monitor ALLOW_CONNECTIONS false` plus terminating its backends), after confirming that SQLSTATE 55000 is classed as an outage.
4. **Node reboot.** Login nodes can't be rebooted on demand. Record any maintenance reboot during the week (`node_rebooted`). If none happens, record that and keep watching.
5. After 7 days, write `docs/m1c-week.md` and tag the next prerelease.
