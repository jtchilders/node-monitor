# M1b — Database Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist the M1a pipeline's records in PostgreSQL. Build a versioned, partitioned schema, an idempotent writer, a disk spool that survives database outages without losing data, and retention maintenance. Then validate the whole thing on the real server with real captured data.

**Architecture:** A new `node_monitor/database/` package (sync, psycopg 3) contains:
- the migration runner and SQL migrations;
- `DatabaseWriter`, which maps each `Record` kind to idempotent SQL in one transaction per batch;
- `Spool`, append-only JSONL batch files;
- `BufferedWriter`, which writes, falls back to the spool on failure, and replays in order on recovery;
- `Maintenance`, which handles monthly partitions and retention.

M1c will run `BufferedWriter` in a thread beside the asyncio sessions. The plan also lands the redaction fixes parked from M1a, because this is the first milestone that stores command lines.

**Tech Stack:** Python ≥ 3.10, `psycopg[binary]>=3.2,<4`, PostgreSQL 18 (polaris-login-04, localhost:5432), pytest, ruff, GitHub Actions `postgres` service.

**Spec:** `docs/superpowers/specs/2026-10-28-node-monitor-v2-design.md`, specifically:
- §3 Database roles and migrations
- §4 Failure handling (PostgreSQL outage)
- §6 Data model
- §11 Testing
- §13 M1b

**Inputs:**
- `docs/m1a-followups.md`: record-contract notes and parked redaction items.
- The record shapes in `node_monitor/pipeline/` (`LIVE_PROCESS_COLUMNS`, `NODE_METRICS_COLUMNS`, `USER_USAGE_COLUMNS`, `NODE_USAGE_COLUMNS`, plus the `records.py` kinds).

## Global Constraints

**Repo, environment and tooling**
- Repo: `~/node-monitor/node-monitor/`. Branch: `feat/m1b-database` from `main`. Venv: `~/node-monitor/venv`. Run live commands with `env -u LD_PRELOAD`.
- 3-space indentation; `ruff check . && ruff format --check .` must pass; never format `docs/`.
- Python ≥ 3.10. Never use `asyncio.wait_for`. The database package is synchronous.
- Dependency: add `psycopg[binary]>=3.2,<4` to `pyproject.toml` `dependencies`.

**Database names and access**
- Database `node_monitor`; schema `node_monitor`.
- Roles: `node_monitor_owner` (owns the schema; used by migrate and the collector) and `node_monitor_reader` (SELECT only; used by web and CLI). Both are `LOGIN NOSUPERUSER`.
- Known issue, accepted by the owner for now: the server's `pg_hba` trusts every local connection. Roles are created anyway so that tightening access later needs no schema change. Do **not** change `pg_hba.conf`, `postgresql.conf`, or any pbs_monitor database or role.
- Never touch the old v1 database `node_monitor_dev` or the old v1 role `node_monitor_web`.
- The collector runs no DDL except creating and dropping partitions of its own tables (spec §3).

**Data format and partitioning**
- All timestamps are `timestamptz`. Rows carry tz-aware UTC datetimes from the pipeline.
- Time-series tables are range-partitioned by month. Partitions are named `<table>_yYYYYmMM` and cover `[first of month, first of next month)` in UTC.
- Retention defaults (spec §3):
  - `node_metrics_days: 730`
  - `usage_summary_days: 730`
  - `collection_events_days: 180`
  - `process_samples_days` and `command_lines_days` are M3; accept them and ignore them.
- Spool default: `max_bytes: 1073741824`. Files are mode 0600 in a directory with mode 0700.

**Tests and CI**
- Tests that need PostgreSQL use the `pg` fixtures (Task 3). They **skip** unless `NODE_MONITOR_TEST_ADMIN_URL` is set. CI sets it to a `postgres:18` service with trust auth.
- Locally, tests use `NODE_MONITOR_TEST_ADMIN_URL=postgresql://localhost/postgres`. They create and drop only databases named `node_monitor_test_<random hex>`.
- `process_samples`, `command_lines`, tags and the hourly census are M3. Do not create those tables.

**Code and commits**
- Comments explain why. No task IDs or history in code.
- Commit after each task. Each message ends with `Co-Authored-By: Claude Code <noreply@anthropic.com>`. Do not push.

## Review Focus

1. **Replaying an old spooled live snapshot after newer data was written** (spool replay order, or a collector restart). It must never overwrite newer live state: `live_processes` and `live_nodes` only change when the incoming `ts`/`updated_at` is greater than or equal to the stored one.
   Test: Task 5 `test_stale_live_snapshot_does_not_overwrite_newer`.
2. **A record whose month has no partition.** This happens at month boundaries or when replaying old data.
   - The partition is created on demand and the batch succeeds.
   - A row older than its table's retention is dropped and counted in a `rows_beyond_retention` event; it never fails the batch.
   Test: Task 5 `test_missing_partition_created_and_expired_rows_dropped`.
3. **Non-finite floats (NaN, ±inf) inside jsonb maps or numeric columns.** They must not abort the batch. They are stored as SQL NULL or JSON null.
   Test: Task 5 `test_non_finite_floats_stored_as_null`.
4. **The spool cannot be written** (disk full, permission denied, or over `max_bytes`). It must never raise into the collector. The oldest batches are dropped, superseded live-only batches first, and the drops are counted in a `spool_dropped` event.
   Test: Task 6 `test_spool_write_failure_never_raises`, `test_cap_drops_live_only_batches_first`.
5. **Replaying the same batch twice** (a commit succeeded but the acknowledgement was lost). This must not duplicate any summary or inventory row.
   Test: Task 5 `test_writing_a_batch_twice_is_idempotent`.

---

## File Structure

```
node_monitor/pipeline/redact.py            MODIFY: parked M1a redaction fixes (Task 1)
node_monitor/config.py                     MODIFY: database, web.database_url, retention, spool sections
config.example.yaml, README.md             MODIFY
node_monitor/database/__init__.py
node_monitor/database/connect.py           connect(url) helper, error classification
node_monitor/database/migrations/0001_initial.sql
node_monitor/database/migrate.py           migration runner, schema version check
node_monitor/database/bootstrap.py         roles + database creation (admin)
node_monitor/database/codec.py             row → SQL params; jsonb encoding; spool (de)serialization
node_monitor/database/writer.py            DatabaseWriter
node_monitor/database/spool.py             Spool
node_monitor/database/buffered.py          BufferedWriter
node_monitor/database/maintenance.py       partitions, retention, heartbeat
node_monitor/cli/main.py                   MODIFY: `database bootstrap|migrate|status`, `soak replay --write-db`
.github/workflows/ci.yml                   MODIFY: postgres service, env
tests/database/__init__.py, conftest.py    pg fixtures
tests/database/test_*.py
docs/m1b-database-validation.md            Task 9
```

---

### Task 1: Redaction fixes parked from M1a

**Files:**
- Modify: `node_monitor/pipeline/redact.py`
- Test: `tests/pipeline/test_redact.py`

**Interfaces:**
- Produces: `Redactor` keeps its existing API. Only the behaviour changes, as follows:
  - **Bare `key`** is no longer a short secret keyword. Compound forms stay: `api-key`, `api_key`, `apikey`, `access-key`/`access_key`/`accesskey`, `secret-key`/`secret_key`/`secretkey`, `private-key`/`private_key`, `ssh-key`.
  - **Java `-D<name>=value`** masks only when the name's last segment (split on `.`, `_`, `-`) or its glued/camelCase ending is a **long** keyword (`password`, `passwd`, `secret`, `token`, `apikey`, `accesskey`, `secretkey`). Short keywords (`auth`, `pass`, `pat`, `pwd`, `credential`) never trigger in `-D` form.
  - **keytool** `-storepass X`, `-keypass X`, `-srcstorepass X`, `-deststorepass X` → `***`.
  - **openssl** `-passin pass:X`, `-passout pass:X` → `pass:***`.
  - **sshpass** `-p<attached>` and `-p X` → `***`, only when argv0's basename is `sshpass`.

- [ ] **Step 1: Write the failing tests.** Add these to the existing MASKED and UNTOUCHED tables.
  - MASKED:
    - `keytool -genkey -storepass s3 -keypass k4` → `keytool -genkey -storepass *** -keypass ***`
    - `openssl pkcs12 -passout pass:hunter2 -in a` → `openssl pkcs12 -passout pass:*** -in a`
    - `sshpass -pSecret ssh h` → `sshpass -p*** ssh h`
    - `sshpass -p Secret ssh h` → `sshpass -p *** ssh h`
    - `java -Djavax.net.ssl.keyStorePassword=pw X` → `…keyStorePassword=*** X`
    - `--api-key x` → `--api-key ***`
  - UNTOUCHED:
    - `--sort-key name`
    - `--key path/obj`
    - `openssl req -key file.pem`
    - `cmake -DENABLE_AUTH=ON ..`
    - `gcc -DUSE_TOKEN=1 x.c`
    - `-Dfile.encoding=UTF-8`
    - `--primary-key id`
  - Every existing test must still pass.
- [ ] **Step 2: Run the tests to verify they fail.** `pytest tests/pipeline/test_redact.py -q`
- [ ] **Step 3: Implement the changes.** Keep everything free of catastrophic backtracking. The existing 100 KB test must pass.
- [ ] **Step 4: Verify.** Run the full suite and ruff.
- [ ] **Step 5: Commit.** `fix(pipeline): stop masking non-secret key options; mask keytool, openssl and sshpass secrets`

---

### Task 2: Config — database, web reader, retention, spool

**Files:**
- Modify: `node_monitor/config.py`, `config.example.yaml`, `README.md`
- Test: `tests/test_config.py`

**Interfaces:**
- Produces:
  - `DatabaseConfig(url: str | None = None, connect_timeout_sec: float = 10)` as `Config.database`.
  - `WebConfig(database_url: str | None = None, host: str = "127.0.0.1", port: int = 9998)` as `Config.web`.
  - `RetentionConfig(node_metrics_days: int = 730, usage_summary_days: int = 730, collection_events_days: int = 180)` as `Config.retention`.
  - `SpoolConfig(max_bytes: int = 1073741824)` as `Config.spool`.
  - All new fields have defaults, so existing `Config(` constructions keep working.
  - Validation, each failure raising `ConfigError` that names the key:
    - Retention values and `max_bytes` are positive ints. Reject bool, NaN and inf.
    - URLs must start with `postgresql://` or `postgres://`.
  - **Credential rule (spec §3).** If any configured database URL contains a password (`user:pass@`), `load_config` raises `ConfigError("… must have mode 0600 …")` when the file's mode allows group or other access.
  - Env overrides: `NODE_MONITOR_DB_URL` replaces `database.url`; `NODE_MONITOR_WEB_DB_URL` replaces `web.database_url`.
  - `retention.process_samples_days` and `retention.command_lines_days` stay accepted and ignored (M3).

- [ ] **Step 1: Write the failing tests.**
  - Defaults.
  - Each invalid value.
  - Env overrides.
  - A password-bearing URL with file mode 0644 is rejected; with mode 0600 it is accepted.
  - A URL without a password with mode 0644 is accepted.
- [ ] **Step 2: Run them to verify they fail.**
- [ ] **Step 3: Implement.**
  - Update `config.example.yaml` with these values:
    - `database.url: postgresql://node_monitor_owner@localhost/node_monitor`
    - `web.database_url: postgresql://node_monitor_reader@localhost/node_monitor`
  - Add a README section, "Database", covering bootstrap, migrate and status.
- [ ] **Step 4: Verify.**
- [ ] **Step 5: Commit.** `feat(config): database, web reader, retention and spool settings`

---

### Task 3: Schema migration, runner, and test fixtures

**Files:**
- Create:
  - `node_monitor/database/__init__.py`
  - `node_monitor/database/connect.py`
  - `node_monitor/database/migrate.py`
  - `node_monitor/database/migrations/__init__.py`
  - `node_monitor/database/migrations/0001_initial.sql`
  - `tests/database/__init__.py`
  - `tests/database/conftest.py`
- Modify: `pyproject.toml` (dependency; package data `node_monitor.database.migrations: ["*.sql"]`), `.github/workflows/ci.yml`
- Test: `tests/database/test_migrate.py`

**Interfaces:**

`connect.py`
- `connect(url: str, *, connect_timeout_sec: float = 10, autocommit: bool = False) -> psycopg.Connection`. It sets `search_path` to `node_monitor, public` and `TimeZone` to `UTC`.
- `class DatabaseUnavailable(Exception)`
- `is_unavailable(exc: BaseException) -> bool`. True for `psycopg.OperationalError` and its subclasses, including `AdminShutdown`, `ConnectionTimeout` and connection failures. False for programming and data errors.

`migrate.py`
- `MIGRATIONS: list[tuple[int, str]]`: `(version, filename)`, discovered from the package's `NNNN_*.sql` files via `importlib.resources`.
- `REQUIRED_VERSION: int`: the highest version.
- `current_version(conn) -> int`: 0 when `node_monitor.schema_migrations` doesn't exist.
- `migrate(conn) -> list[int]`:
  - Takes `pg_advisory_lock(hashtext('node_monitor.migrate'))`.
  - Applies each pending migration in its own transaction.
  - Records `(version, name, applied_at)` in `node_monitor.schema_migrations(version int PRIMARY KEY, name text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())`.
  - Returns the versions it applied.
- `class SchemaOutOfDate(Exception)`
- `check_schema(conn) -> None`: raises `SchemaOutOfDate("schema version N, need M: run `node-monitor database migrate`")` when the schema is behind.

`0001_initial.sql`

The schema `node_monitor` is created by bootstrap (Task 4), so this file creates tables only. Column types:

| Value | SQL type |
|---|---|
| datetime | `timestamptz` |
| int | `bigint` (but `pid`, `ppid`, `uid` are `integer`) |
| float | `double precision` |
| bool | `boolean` |
| str | `text` |
| dict or list | `jsonb` |

Every table starts with `system text NOT NULL, node text NOT NULL`, except where noted.

| Table | Columns after system, node | Key | Partitioned by |
|---|---|---|---|
| `live_processes` | `ts timestamptz NOT NULL` + `LIVE_PROCESS_COLUMNS` | PK (system, node, pid, start_time) | — |
| `live_snapshots` | `ts timestamptz NOT NULL`, the time of the node's current `live_processes` snapshot. It is kept separately so that an empty snapshot still has a timestamp to compare against. | PK (system, node) | — |
| `live_nodes` | `label, updated_at, session_state, boot_id, probe_version, load1, mem_used_kb, mem_total_kb, procs_total, last_census_at` | PK (system, node) | — |
| `nodes` | `label text, first_seen timestamptz NOT NULL, last_seen timestamptz NOT NULL, boot_id text` | PK (system, node) | — |
| `node_hardware` | `boot_id text NOT NULL, btime bigint, collected_at timestamptz NOT NULL, probe_version integer, python_version text, hostname_fqdn text, cpu_model text, cpu_logical integer, sockets integer, cores_per_socket integer, cpu_max_freq_khz bigint, numa_nodes integer, mem_total_kb bigint, swap_total_kb bigint, hugepage_size_kb bigint, kernel_release text, os_pretty_name text, net_fs_mounts integer, net_ifaces jsonb, gpus jsonb` | PK (system, node, boot_id) | — |
| `node_metrics_1m` | `NODE_METRICS_COLUMNS` (`minute` NOT NULL; `cpu_pct`, `network_rates`, `lustre_md_rates`, `gpus` jsonb) | PK (system, node, minute) | `minute` |
| `user_usage_5m` | `USER_USAGE_COLUMNS` (`username` NOT NULL; `by_*` jsonb) | PK (system, node, username, interval_start) | `interval_start` |
| `node_usage_5m` | `NODE_USAGE_COLUMNS` | PK (system, node, interval_start) | `interval_start` |
| `collection_events` | `id bigint GENERATED ALWAYS AS IDENTITY, ts timestamptz NOT NULL, system text NOT NULL, node text NULL, event_type text NOT NULL, detail jsonb NOT NULL` | PK (id, ts) | `ts` |
| `collector_heartbeat` | `system text PRIMARY KEY, collector_host text NOT NULL, version text NOT NULL, started_at timestamptz NOT NULL, last_seen timestamptz NOT NULL, status jsonb NOT NULL` | PK (system) | — |

- `collection_events.node` is NULL for collector-level events.
- **Indexes:**
  - `node_metrics_1m (system, minute DESC)`
  - `user_usage_5m (system, interval_start DESC)`
  - `user_usage_5m (system, username, interval_start DESC)`
  - `node_usage_5m (system, interval_start DESC)`
  - `collection_events (system, ts DESC)`
  - `live_processes (system, username)`
- **Partitions:** none are created by the migration (Task 8 does that). Partitioned parents have no DEFAULT partition, so an insert into a missing month fails loudly. The writer handles that case (Task 5).
- **Grants:** `GRANT SELECT ON ALL TABLES IN SCHEMA node_monitor TO node_monitor_reader` and `ALTER DEFAULT PRIVILEGES FOR ROLE node_monitor_owner IN SCHEMA node_monitor GRANT SELECT ON TABLES TO node_monitor_reader`. Default privileges are what make future partitions readable.

`tests/database/conftest.py`
- `pg_admin_url` (session) is `os.environ["NODE_MONITOR_TEST_ADMIN_URL"]`. Without it, `pytest.skip`.
- `pg_db` (function scope) creates `node_monitor_test_<8 hex>` owned by the admin user, runs `CREATE SCHEMA node_monitor`, yields its URL, then terminates its backends and drops it.
- `migrated_db` is `pg_db` with `migrate()` applied.

CI
- Add `services.postgres` (`image: postgres:18`, `env: POSTGRES_HOST_AUTH_METHOD: trust`, `ports: 5432:5432`, health check `pg_isready`) to the `test` job.
- Set `NODE_MONITOR_TEST_ADMIN_URL: postgresql://postgres@localhost:5432/postgres` in the job env.

- [ ] **Step 1: Write the failing tests.**
  - `test_migrate_applies_all_and_is_idempotent`: the first run returns `[1]`, the second returns `[]`, and `current_version == REQUIRED_VERSION`.
  - `test_check_schema_raises_until_migrated`.
  - `test_tables_and_primary_keys_exist`: query `information_schema` for every table above and its PK columns.
  - `test_partitioned_tables_are_partitioned`: query `pg_partitioned_table` for the 4 partitioned parents.
  - `test_concurrent_migrate_is_serialized`: two connections run `migrate` in threads, and exactly one applies version 1.
  - `test_is_unavailable_classification`: connecting to `postgresql://localhost:1/x` gives `is_unavailable` True. A syntax error gives False.
- [ ] **Step 2: Run them to verify they fail.** Run locally with `NODE_MONITOR_TEST_ADMIN_URL=postgresql://localhost/postgres`.
- [ ] **Step 3: Implement** the runner, the SQL, the fixtures and CI.
- [ ] **Step 4: Verify.**
  - Run the full suite with the env var set (database tests run) and without it (database tests skip).
  - Confirm no `node_monitor_test_*` database remains: `psql -l`, or a `pg_database` query from Python.
- [ ] **Step 5: Commit.** `feat(database): versioned schema, migration runner and PostgreSQL test fixtures`

---

### Task 4: Bootstrap and the `database` CLI

**Files:**
- Create: `node_monitor/database/bootstrap.py`
- Modify: `node_monitor/cli/main.py`
- Test: `tests/database/test_bootstrap.py`, `tests/test_cli_database.py`

**Interfaces:**

`bootstrap(admin_url: str, *, database: str = "node_monitor", owner: str = "node_monitor_owner", reader: str = "node_monitor_reader") -> list[str]`

It is idempotent and returns a description of each action taken. It does this:
1. Creates `owner` and `reader` as `LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE` if they are missing.
2. Creates `database` with owner `owner` if it is missing.
3. In that database, runs `CREATE SCHEMA IF NOT EXISTS node_monitor AUTHORIZATION owner`, `GRANT CONNECT ON DATABASE … TO reader` and `GRANT USAGE ON SCHEMA node_monitor TO reader`.

It never alters existing roles or other databases. Names must match `^[a-z_][a-z0-9_]*$`, and all identifiers are quoted with `psycopg.sql.Identifier`.

The `database` command group:
- `database bootstrap --admin-url URL` prints the actions.
- `database migrate [--config PATH]` uses `config.database.url`. It prints the applied versions, or "up to date".
- `database status [--config PATH]` prints the current and required version, the pending migrations, and each partitioned table's partitions (name and range). It exits 1 if the schema is behind.

Errors are `click.ClickException` messages, with no tracebacks for an unavailable database.

- [ ] **Step 1: Write the failing tests.** Use the `pg_admin_url` fixture and a unique database name, plus unique role names `nm_test_owner_<hex>` and `nm_test_reader_<hex>`, all dropped at teardown.
  - Bootstrap twice: the second run reports no actions.
  - The reader can connect and has USAGE on the schema but cannot `CREATE TABLE`.
  - After migrating as the owner, the reader can SELECT from a table and cannot INSERT into it.
  - The CLI: `database status` exits 1 before migrate and 0 after. `database migrate` prints "up to date" on its second run.
- [ ] **Step 2: Run them to verify they fail.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Verify.**
- [ ] **Step 5: Commit.** `feat(database): bootstrap roles and database; database bootstrap/migrate/status commands`

---

### Task 5: `DatabaseWriter`

**Files:**
- Create: `node_monitor/database/codec.py`, `node_monitor/database/writer.py`, and `node_monitor/database/maintenance.py` with `partition_name` and `ensure_partition` only (Task 8 adds the rest)
- Test: `tests/database/test_codec.py`, `tests/database/test_writer.py`

**Interfaces:**

`codec.py`
- `to_db_value(value) -> object`: dicts and lists become `psycopg.types.json.Jsonb` with non-finite floats replaced by `None` recursively; non-finite floats become `None`; everything else passes through unchanged.
- `encode_record(record) -> dict` and `decode_record(d) -> Record`: a JSON-safe round trip for the spool. Datetimes are encoded as `{"$dt": "<iso>"}`; everything else is plain JSON.

`writer.py`
- `class DatabaseWriter(url: str, *, retention: RetentionConfig, connect_timeout_sec: float = 10)`.
- `write(records: list[Record], now: datetime) -> WriteResult` writes one batch in one transaction.
  - It raises `DatabaseUnavailable` when `is_unavailable`. On any other exception it rolls back and re-raises.
  - It reconnects lazily: a broken connection is discarded, and the next `write` reconnects.
- `WriteResult(rows_written: dict[str, int], rows_beyond_retention: dict[str, int], partitions_created: list[str])`.
- `close()`.

Per-kind SQL:

| Kind | SQL behaviour |
|---|---|
| `live_processes` | Applied only if the incoming `ts` is ≥ the node's `live_snapshots.ts`, or the node has no `live_snapshots` row yet. When applied, it upserts `live_snapshots`, then replaces the node's processes (`DELETE … WHERE system=%s AND node=%s` followed by COPY or `executemany` INSERT). Otherwise the record is skipped. Within one batch, only the newest snapshot per node is applied. |
| `live_node` | A partial upsert of only the keys present, from an allowlist of `live_nodes` columns. Unknown keys are ignored. `ON CONFLICT (system, node) DO UPDATE SET <present cols> WHERE live_nodes.updated_at <= EXCLUDED.updated_at`. It also upserts `nodes` (label, `first_seen` = least, `last_seen` = greatest, `boot_id` when present). |
| `node_hardware` | `ON CONFLICT DO NOTHING`, which keeps the first `collected_at`. It also sets `nodes.boot_id`. |
| `node_metrics_1m`, `user_usage_5m`, `node_usage_5m` | `INSERT … ON CONFLICT (<pk>) DO UPDATE SET` every non-key column to `EXCLUDED`. |
| `collection_event` | Plain INSERT. |

Retention and partitions:
- **Expired rows.** A row whose partition-key time is older than `now − retention days` for its table is dropped before insert and counted in `rows_beyond_retention`.
- **Missing partitions.** If an insert fails with "no partition of relation … found for row" (SQLSTATE `23514`), the writer creates the missing monthly partitions for that batch's months and retries the batch once. Partitions are created with `maintenance.ensure_partition(conn, table, month_start)`, from Task 8; this task defines that function, and Task 8 reuses it.
- **Unknown record kinds** raise `ValueError` before anything is written.

- [ ] **Step 1: Write the failing tests.** These run against `migrated_db` (connected as the admin user; ownership is not under test here).
  - `test_each_kind_round_trips`: write one record of each kind built by the real `NodePipeline` from the scripted session used in `tests/pipeline/test_node_pipeline.py`, then SELECT it back and compare its columns.
  - `test_writing_a_batch_twice_is_idempotent`: write the same batch twice. Summary, inventory and hardware row counts are unchanged, and only events are appended.
  - `test_stale_live_snapshot_does_not_overwrite_newer`: write a snapshot at T2, then one at T1 < T2. The stored snapshot is still T2's processes, and `live_nodes.updated_at` stays at T2.
  - `test_live_node_partial_update_keeps_gauges`: write a counters-style `live_node`, then a session-lost partial row. The gauges survive and `session_state` becomes "down".
  - `test_missing_partition_created_and_expired_rows_dropped`: a metrics row in a month with no partition is written, and `partitions_created` names it. A row 800 days old is not written, and `rows_beyond_retention["node_metrics_1m"] == 1`.
  - `test_non_finite_floats_stored_as_null`: `cpu_pct` with a NaN value, and `load1 = inf`, are stored as JSON null and SQL NULL.
  - `test_unavailable_database_raises_database_unavailable`: a writer pointed at port 1 raises `DatabaseUnavailable`.
  - `test_unknown_kind_rejected_without_partial_write`.
  - `test_codec_round_trip` (no PG needed): every record kind encodes to JSON and decodes back to an equal Record, including tz-aware datetimes.
- [ ] **Step 2: Run them to verify they fail.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Verify.** Run the full suite with PG.
- [ ] **Step 5: Commit.** `feat(database): idempotent record writer with partition and retention handling`

---

### Task 6: `Spool`

**Files:**
- Create: `node_monitor/database/spool.py`
- Test: `tests/database/test_spool.py` (no PG needed)

**Interfaces:**
- `class Spool(directory: Path, *, max_bytes: int)`.
  - It creates `directory` with mode 0700.
  - Each batch is one file, `<UTC %Y%m%dT%H%M%S.%f>-<seq:06d>.jsonl`, mode 0600. Its lines are `encode_record` dicts.
  - Writes go to a temporary file, then `os.replace`, so a crash never leaves a half-written batch file.
- `append(records: list[Record]) -> SpoolResult` never raises. `SpoolResult(stored: bool, dropped_batches: int, dropped_records: int, error: str | None)`.
  - On an OS error it returns `stored=False` with an error message.
  - If adding the batch would exceed `max_bytes`, it first deletes the oldest batches whose records are all `live_processes` or `live_node`, which newer data supersedes. If that isn't enough, it deletes the oldest batches of any kind.
  - A batch larger than `max_bytes` on its own is not stored.
- `pending() -> Iterator[tuple[Path, list[Record]]]` yields batches oldest first. Unreadable or corrupt files are moved to `directory/corrupt/` and skipped.
- `remove(path: Path) -> None`.
- `size_bytes() -> int` and `batch_count() -> int`.

- [ ] **Step 1: Write the failing tests.**
  - `test_append_and_pending_round_trip_in_order`.
  - `test_files_are_private`: the directory is mode 0700 and files are 0600.
  - `test_cap_drops_live_only_batches_first`.
  - `test_cap_drops_oldest_when_no_live_only_batches`.
  - `test_oversized_batch_not_stored`.
  - `test_spool_write_failure_never_raises`: a read-only directory gives `stored=False` and an error.
  - `test_corrupt_file_quarantined`.
  - `test_crash_mid_write_leaves_no_partial_batch`: simulate it by leaving a temp file behind. `pending()` ignores it.
- [ ] **Step 2: Run them to verify they fail.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Verify.**
- [ ] **Step 5: Commit.** `feat(database): disk spool for database outages`

---

### Task 7: `BufferedWriter` — write, spool, replay

**Files:**
- Create: `node_monitor/database/buffered.py`
- Test: `tests/database/test_buffered.py`

**Interfaces:**
- **Constructor:** `class BufferedWriter(writer: DatabaseWriter, spool: Spool, *, system: str, backoff: tuple[float, float] = (5, 300))`.
- **`submit(records: list[Record], now: datetime) -> list[Record]`.** It returns the collector-level `COLLECTION_EVENT` records it generated, with `node=None`. The caller passes those back in its next `submit`.
- **While outside an outage, or once `now` reaches the next retry time:**
  1. Replay the spool oldest first, one batch per `writer.write`, calling `spool.remove` after each success.
  2. Then write `records`.
  3. On `DatabaseUnavailable`, enter or extend the outage: append `records` (and any batch that wasn't replayed stays on disk) to the spool, set the next retry to `now + delay` (delay doubles from `backoff[0]`, capped at `backoff[1]`), and stop.
- **During an outage, before the next retry:** append `records` to the spool without touching the database.
- **Events:**
  - `database_unavailable` `{error}`: when an outage begins.
  - `database_recovered` `{outage_sec, replayed_batches, replayed_records}`: when the spool is fully drained.
  - `spool_dropped` `{batches, records}`: from `SpoolResult`.
  - `spool_write_failed` `{error}`.
  - `rows_beyond_retention` `{table: n}`: from `WriteResult`.
- **Other exceptions:** non-unavailable errors from `writer.write` propagate. They are bugs, and M1c logs them.
- **`status() -> dict`** returns `{connected: bool, outage_since: iso|None, spooled_batches, spooled_bytes, next_retry: iso|None}`.

- [ ] **Step 1: Write the failing tests.** These run against PG (`migrated_db`), using a writer whose URL can be swapped between the real database and `postgresql://localhost:1/x` (port 1).
  - `test_outage_spools_then_replays_in_order_without_loss`:
    1. With a good URL, write batch A.
    2. Switch to the bad URL and write B and C. Both are spooled, and one `database_unavailable` event is emitted.
    3. Switch back and write D after `next_retry`.
    4. The spool is now empty, B, C and D are in the database in order, and `database_recovered` has `replayed_batches == 2`.
  - `test_no_database_attempts_before_next_retry`: count connection attempts with a stub writer.
  - `test_backend_terminated_mid_session`: write, run `pg_terminate_backend` on the writer's backend from an admin connection, then write again. Either the second write succeeds on a fresh connection, or the batch is spooled and replayed on the next submit. Nothing is lost.
  - `test_backoff_doubles_and_caps`.
  - `test_spool_drop_event_surfaces`.
- [ ] **Step 2: Run them to verify they fail.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Verify.**
- [ ] **Step 5: Commit.** `feat(database): buffered writer that spools through outages and replays in order`

---

### Task 8: Maintenance — partitions, retention, heartbeat

**Files:**
- Modify: `node_monitor/database/maintenance.py` (Task 5 created it with `partition_name` and `ensure_partition`; this task adds the rest)
- Test: `tests/database/test_maintenance.py`

**Interfaces:**
- `PARTITIONED = {"node_metrics_1m": "node_metrics_days", "user_usage_5m": "usage_summary_days", "node_usage_5m": "usage_summary_days", "collection_events": "collection_events_days"}`, mapping each table to its `RetentionConfig` field.
- `partition_name(table, month_start) -> str` gives `"<table>_y%Ym%m"`.
- `ensure_partition(conn, table, month_start: datetime) -> bool` is `CREATE TABLE IF NOT EXISTS … PARTITION OF … FOR VALUES FROM (…) TO (…)`. It returns True if the partition was created.
- `ensure_partitions(conn, now, months_ahead: int = 2) -> list[str]` covers the current month and the next `months_ahead` months for every table.
- `apply_retention(conn, now, retention: RetentionConfig) -> list[str]` detaches and then drops every partition whose upper bound ≤ `now − days`. It never drops the current month's partition.
- `write_heartbeat(conn, *, system, collector_host, version, started_at, now, status: dict) -> None` upserts `collector_heartbeat`.
- `run(conn, now, retention) -> dict` returns `{created: [...], dropped: [...]}`. M1c calls it at startup and daily.

- [ ] **Step 1: Write the failing tests.**
  - `test_ensure_partitions_creates_three_months_per_table_idempotently`.
  - `test_month_boundaries_are_utc`: a row at `2026-11-30T23:59:59Z` goes into `_y2026m11`, and a row at `2026-12-01T00:00:00Z` goes into `_y2026m12`.
  - `test_apply_retention_drops_only_expired_partitions`: with `collection_events_days=180` and `now=2026-11-15`, a May 2026 partition is dropped and a June 2026 partition is kept.
  - `test_reader_can_select_new_partition`: after bootstrap with test roles, a partition created by the owner is readable by the reader. This proves the default privileges work.
  - `test_heartbeat_upsert`.
- [ ] **Step 2: Run them to verify they fail.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Verify.**
- [ ] **Step 5: Commit.** `feat(database): monthly partitions, retention and collector heartbeat`

---

### Task 9: Replay into the database; validate on the real server

**Files:**
- Modify: `node_monitor/pipeline/replay.py`, `node_monitor/cli/main.py`
- Create: `docs/m1b-database-validation.md`
- Test: `tests/database/test_replay_db.py`

**Interfaces:**
- `soak replay DIR [--config PATH] [--write-db] [--write-records FILE]`. With `--write-db`, records go through a `BufferedWriter`:
  - It uses `config.database.url` and a spool under `<working_dir>/state/spool-replay`.
  - Batches are flushed every 500 records and at the end.
  - `maintenance.ensure_partitions(conn, now)` runs first.
  - Replay prints per-node counts, then `rows_beyond_retention`, then a final `status()`.

- [ ] **Step 1: Write the failing test.** `test_replay_write_db_round_trip`: replay a small scripted soak directory into `migrated_db`. Per-kind row counts in the database must match the record counts from the replay, and a second replay must leave summary counts unchanged.
- [ ] **Steps 2–4: Implement and verify** (suite plus ruff, with PG).
- [ ] **Step 5: Commit.** `feat(cli): replay soak captures into PostgreSQL`
- [ ] **Step 6: Real server.** Run these on polaris-login-04 with `env -u LD_PRELOAD`.

```bash
node-monitor database bootstrap --admin-url postgresql://localhost/postgres
node-monitor database migrate
node-monitor database status
node-monitor soak replay ~/node-monitor/logs/soak/20261028T230908Z --write-db     # M0 26 h, summary mode
node-monitor soak replay ~/node-monitor/logs/soak/20261030T200954Z --write-db     # M1a 12 h, summary mode
```

  - **Config change first.** Before running these, update `~/.node_monitor.yaml` (keep mode 0600) to add `database.url: postgresql://node_monitor_owner@localhost/node_monitor` and `web.database_url: postgresql://node_monitor_reader@localhost/node_monitor`.
  - **Full-census capture.** Then run a 15-minute `soak run --duration-hours 0.25 --full-census`, replay it with `--write-db`, and delete the capture directory afterwards (as in M1a).
  - **Expected results:**
    - **M0 replay:** about 1,561 `node_metrics_1m` rows per node.
    - **M1a replay:** about 721 per node, with `probe_rss_kb` populated.
    - **Full-census capture:** live tables populated, and `user_usage_5m` rows for about 220–240 users per interval.
- [ ] **Step 7: Read-back queries.** Connect as `node_monitor_reader` and run the queries below. Record each one's output, timing it with `\timing` or Python `time.perf_counter`.
  - The latest `node_metrics_1m` row per node.
  - The top 10 users by `rss_kb_avg` in the last full 5-minute interval.
  - The top 10 users by dormant memory: `sum` over `by_dormancy` keys starting with `idle|1h-1d|absent` or `idle|>1d|absent`.
  - Live AI-agent processes by owner presence.
  - The partition list.
  - The reader's INSERT fails with a permission error.
  - A second `soak replay --write-db` of the M1a capture leaves every summary table's count unchanged.
- [ ] **Step 8: Write `docs/m1b-database-validation.md`.** Include the commands, the row counts per table and node, the query outputs (keep user lists to the top 5 and include no command lines), the timings, and the size of the database (`pg_database_size`). Commit it as `docs: M1b database validation on polaris-login-04`.
