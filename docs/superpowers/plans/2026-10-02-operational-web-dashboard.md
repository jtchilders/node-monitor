# Operational Web Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and deploy a private, read-only, one-node-at-a-time operational dashboard for node-monitor with bounded PostgreSQL queries, a mode-0600 Unix socket, PBS Monitor-style status semantics, and real-browser acceptance tests.

**Architecture:** A separate `node-monitor web` process loads a web-only configuration, verifies a dedicated PostgreSQL reader's schema and effective privileges, and serves one FastAPI application through a pre-bound Unix socket. One atomic dashboard request runs database-side projections and aggregates in one repeatable-read/read-only transaction; a packaged HTML/CSS/JavaScript frontend polls it every 60 seconds and retains the last coherent response after disconnects.

**Tech Stack:** Python 3.9+, Click, SQLAlchemy 2, psycopg2, FastAPI, Uvicorn, static HTML/CSS/JavaScript, bundled Chart.js, pytest, disposable PostgreSQL databases, and Playwright with Chromium.

**Authoritative specification:** `/Users/jchilders/workspaces/node_monitor_planning/docs/superpowers/specs/2026-10-02-operational-web-dashboard-design.md` at `a4353e4df4fc16845446b44f8fd5dcc88b3ef18d`.

**Implementation base:** `8f397d2850180c0c0c843245147931c1a11a8d96`.

---

## File map and ownership boundaries

- `node_monitor/config.py`: strict collector configuration remains intact; add only web-specific immutable types and a separate `load_web_config()` entry point.
- `node_monitor/database/schema_contract.py`: pure migration/catalog contract shared by operator migrations and read-only inspection; no engine creation or DDL execution.
- `node_monitor/database/web.py`: dedicated web engine, schema/privilege preflight, and one-connection request runner with cancellation cleanup.
- `node_monitor/web/queries.py`: parameterized SQL and pure response-shaping helpers; no HTTP or socket concerns.
- `node_monitor/web/service.py`: dashboard orchestration, response bounds, and safe service-domain errors.
- `node_monitor/web/app.py`: FastAPI routes and exact static-resource allowlist.
- `node_monitor/web/socket.py`: run-directory and pre-bound Unix-socket lifecycle only.
- `node_monitor/web/runtime.py`: Uvicorn integration using the already-open socket.
- `node_monitor/web/static/{index.html,styles.css,app.js,chart.umd.min.js}`: build-free browser application and pinned package-owned Chart.js.
- `node_monitor/cli/main.py`: thin `web` command; no generic Uvicorn options and no database migration capability.
- `tests/web/`: unit, PostgreSQL, packaging, socket, API, and browser acceptance tests.
- `docs/web-dashboard.md`: public operator instructions; internal credentials and host reconnaissance remain in the private planning repository.

All Python uses three-space indentation. The remote probe and collector runtime are not modified except where a test proves web/collector isolation.

---

### Task 1: Add the independent web-only configuration boundary

**Files:**
- Modify: `node_monitor/config.py`
- Create: `tests/web/test_web_config.py`

- [ ] **Step 1: Write failing tests for the exact accepted shape and credential separation**

```python
from pathlib import Path

import pytest
import yaml

from node_monitor.config import ConfigError, load_web_config


def _write(path, value):
   path.write_text(yaml.safe_dump(value))
   return str(path)


def _valid(url="postgresql+psycopg2://reader@localhost/node_monitor_dev"):
   return {
      "system": "polaris",
      "web": {
         "database": {
            "url": url,
            "schema": "node_monitor",
            "pool_size": 1,
            "max_overflow": 0,
            "connect_args": {
               "connect_timeout": 3,
               "options": "-c statement_timeout=3000 -c lock_timeout=2000",
            },
         },
         "socket_path": "~/.node-monitor/run/web.sock",
      },
   }


def test_web_config_accepts_only_system_and_web(tmp_path):
   path = _write(tmp_path / "web.yaml", _valid())
   config = load_web_config(path, home="/home/operator")
   assert config.system == "polaris"
   assert config.socket_path == "/home/operator/.node-monitor/run/web.sock"
   assert config.database.pool_size == 1
   assert config.database.max_overflow == 0


def test_web_config_rejects_collector_fields(tmp_path):
   raw = _valid()
   raw["nodes"] = []
   path = _write(tmp_path / "web.yaml", raw)
   with pytest.raises(ConfigError, match="unknown key"):
      load_web_config(path, home="/home/operator")


def test_explicit_web_url_wins_and_writer_env_is_never_read(tmp_path, monkeypatch):
   path = _write(tmp_path / "web.yaml", _valid("postgresql:///explicit"))
   monkeypatch.setenv("NODE_MONITOR_WEB_DB_URL", "postgresql:///web_env")
   monkeypatch.setenv("NODE_MONITOR_DB_URL", "postgresql:///writer_secret")
   config = load_web_config(path, home="/home/operator")
   assert config.database.url == "postgresql:///explicit"


def test_web_env_fills_only_an_omitted_url(tmp_path, monkeypatch):
   raw = _valid()
   del raw["web"]["database"]["url"]
   path = _write(tmp_path / "web.yaml", raw)
   monkeypatch.setenv("NODE_MONITOR_WEB_DB_URL", "postgresql:///reader")
   config = load_web_config(path, home="/home/operator")
   assert config.database.url == "postgresql:///reader"
```

- [ ] **Step 2: Run the focused tests and observe RED**

Run: `venv/bin/python -m pytest tests/web/test_web_config.py -q`

Expected: collection fails because `load_web_config` does not exist.

- [ ] **Step 3: Implement immutable `WebConfig` and strict loading without calling `load_any_config()`**

```python
@dataclasses.dataclass(frozen=True)
class WebConfig:
   system: str
   database: DatabaseConfig
   socket_path: str


def load_web_config(path, *, home=None, database_url_env=None):
   resolved_home = os.path.expanduser("~") if home is None else home
   if database_url_env is None:
      database_url_env = os.environ.get("NODE_MONITOR_WEB_DB_URL")
   with open(path, "r") as handle:
      raw = yaml.safe_load(handle)
   raw = _require_mapping(raw, "config")
   _reject_unknown_keys(raw, frozenset({"system", "web"}), "config")
   if set(raw) != {"system", "web"}:
      raise ConfigError("web config requires exactly system and web")
   web = _require_mapping(raw["web"], "web")
   _reject_unknown_keys(web, frozenset({"database", "socket_path"}), "web")
   database = _require_mapping(web.get("database"), "web.database")
   explicit_url = database.get("url")
   resolved_url = explicit_url if explicit_url is not None else database_url_env
   validated_database = _validate_database_section(database, resolved_url)
   socket_path = _validate_web_socket_path(web.get("socket_path"), resolved_home)
   return WebConfig(
      system=_validate_nonempty_string(raw["system"], "system"),
      database=validated_database,
      socket_path=socket_path,
   )
```

`load_web_config()` reads `NODE_MONITOR_WEB_DB_URL` itself only when its caller
does not inject `database_url_env`; explicit YAML still wins. It must not inspect
`NODE_MONITOR_DB_URL`. Add boundary tests for unknown nested
database/connect-argument keys, non-PostgreSQL URLs, schema other than
`node_monitor`, pool size other than 1, overflow other than 0, missing
timezone-independent absolute expansion, NUL bytes, relative paths after
expansion, and socket paths outside `~/.node-monitor/run/`. The existing
`DatabaseConfig` defaults populate `pool_timeout_sec`, `pool_recycle_sec`, and
`pool_pre_ping`; they remain fixed defaults because those keys are intentionally
absent from the approved web YAML shape.

- [ ] **Step 4: Verify focused and existing configuration tests**

Run: `venv/bin/python -m pytest tests/web/test_web_config.py tests/test_config.py tests/test_config_nested.py -q`

Expected: all selected tests pass; existing collector config behavior is unchanged.

- [ ] **Step 5: Commit the increment**

```bash
git add node_monitor/config.py tests/web/test_web_config.py
git commit -m "feat(web): add isolated web configuration"
```

- [ ] **Step 6: Independent review gate**

Review `BASE..HEAD` for recursive unknown-key rejection, explicit-file precedence, URL sanitization, path confinement, and proof that writer credentials are never read. Resolve every Critical or Important finding and re-run Step 4 before proceeding.

---

### Task 2: Extract the pure schema contract and implement read-only startup preflight

**Files:**
- Create: `node_monitor/database/schema_contract.py`
- Create: `node_monitor/database/web.py`
- Modify: `node_monitor/database/migration.py`
- Create: `tests/web/test_web_database_preflight.py`
- Modify: `tests/test_database_migration.py`
- Modify: `tests/test_database_migration_runner.py`

- [ ] **Step 1: Write RED tests proving import separation and effective privilege checks**

```python
import importlib
import sys

import pytest
from sqlalchemy import text


def test_web_database_module_does_not_import_ddl_runner():
   sys.modules.pop("node_monitor.database.migration", None)
   module = importlib.import_module("node_monitor.database.web")
   assert module is not None
   assert "node_monitor.database.migration" not in sys.modules


def test_preflight_rejects_one_missing_select(migrated_reader_engine):
   with migrated_reader_engine.admin.begin() as connection:
      connection.execute(text(
         "REVOKE SELECT ON node_monitor.node_usage_intervals FROM reader_role"))
   with pytest.raises(Exception, match="web database preflight failed"):
      migrated_reader_engine.preflight()


def test_preflight_rejects_effective_write_or_create(migrated_reader_engine):
   with migrated_reader_engine.admin.begin() as connection:
      connection.execute(text(
         "GRANT INSERT ON node_monitor.node_collection_log TO reader_role"))
   with pytest.raises(Exception, match="web database preflight failed"):
      migrated_reader_engine.preflight()
```

The PostgreSQL fixture creates a UUID-named disposable database and a UUID-named login role, migrates using the operator identity, grants only CONNECT/USAGE/SELECT to the reader, and drops only those UUID-named objects in `finally`. It never starts, stops, or reconfigures PostgreSQL.

- [ ] **Step 2: Run RED tests**

Run: `NODE_MONITOR_TEST_DATABASE_URL="$NODE_MONITOR_TEST_DATABASE_URL" venv/bin/python -m pytest tests/web/test_web_database_preflight.py -q`

Expected: module/function fixtures are absent. If the environment variable is unavailable, unit tests still run and PostgreSQL cases skip explicitly; they are mandatory before deployment.

- [ ] **Step 3: Move pure migration discovery/catalog constants into `schema_contract.py`**

```python
REQUIRED_TABLES = (
   "schema_migrations",
   "node_hardware",
   "node_counter_minute",
   "node_usage_intervals",
   "node_poll_failures",
   "node_collection_log",
)


def expected_migration_rows():
   return tuple(
      (migration.version, migration.name, migration.checksum,
       migration.mode == "transactional")
      for migration in discover_migrations()
   )
```

Move only immutable migration resource discovery and exact expected catalog signatures. `MigrationRunner` imports this pure module. The web module imports the pure module but never `migration.py`. Existing migration tests must remain green.

- [ ] **Step 4: Implement `WebDatabase` with one bounded pool and standalone inspection**

```python
class WebDatabaseError(RuntimeError):
   pass


class WebDatabase:
   def __init__(self, config):
      self._engine = create_engine(
         config.url,
         pool_size=1,
         max_overflow=0,
         pool_timeout=config.pool_timeout_sec,
         pool_recycle=config.pool_recycle_sec,
         pool_pre_ping=config.pool_pre_ping,
         echo=False,
         connect_args=dict(config.connect_args),
      )

   def preflight(self):
      try:
         with self._engine.connect() as connection:
            _require_current_schema(connection)
            _require_reader_privileges(connection)
      except (SQLAlchemyError, ValueError, WebDatabaseError):
         raise WebDatabaseError("web database preflight failed") from None
```

`_require_current_schema()` reads `information_schema`, `pg_catalog`, and `schema_migrations` only. It rejects uninitialized, pending, extra, checksum-mismatched, or catalog-drifted state. `_require_reader_privileges()` requires database CONNECT, schema USAGE, SELECT on all six required tables, and no database TEMP or CREATE, schema CREATE, or table INSERT/UPDATE/DELETE/TRUNCATE/REFERENCES/TRIGGER. All SQL uses fixed identifiers from `REQUIRED_TABLES`; no role name, URL, SQL text, or driver exception appears in raised/logged text.

- [ ] **Step 5: Verify unit, migration, and real-PostgreSQL tests**

Run: `venv/bin/python -m pytest tests/web/test_web_database_preflight.py tests/test_database_migration.py tests/test_database_migration_runner.py tests/test_database_migration_postgres.py -q`

Expected: unit tests pass; real PostgreSQL tests pass when configured and otherwise show only their documented skips.

- [ ] **Step 6: Commit and independently review**

```bash
git add node_monitor/database/schema_contract.py node_monitor/database/web.py node_monitor/database/migration.py tests/web/test_web_database_preflight.py tests/test_database_migration.py tests/test_database_migration_runner.py
git commit -m "feat(web): add read-only database preflight"
```

Review must explicitly inspect the import graph, effective privileges including PUBLIC/inherited grants, catalog equivalence, credential sanitization, and absence of DDL from web paths. Critical/Important findings block Task 3.

---

### Task 3: Implement race-free Unix-socket ownership and the foreground runtime

**Files:**
- Create: `node_monitor/web/__init__.py`
- Create: `node_monitor/web/socket.py`
- Create: `node_monitor/web/runtime.py`
- Create: `tests/web/test_web_socket.py`
- Create: `tests/web/test_web_runtime.py`

- [ ] **Step 1: Write failing socket lifecycle tests**

```python
import os
import socket
import stat

import pytest

from node_monitor.web.socket import bind_private_socket


def test_socket_is_private_before_listen(tmp_path, monkeypatch):
   events = []
   real_listen = socket.socket.listen

   def inspect_then_listen(sock, backlog):
      mode = stat.S_IMODE(os.stat(sock.getsockname()).st_mode)
      events.append(mode)
      return real_listen(sock, backlog)

   monkeypatch.setattr(socket.socket, "listen", inspect_then_listen)
   bound = bind_private_socket(str(tmp_path / "run" / "web.sock"))
   try:
      assert events == [0o600]
      assert stat.S_IMODE(os.stat(tmp_path / "run").st_mode) == 0o700
      assert stat.S_IMODE(os.stat(bound.getsockname()).st_mode) == 0o600
   finally:
      bound.close()


def test_duplicate_launch_does_not_unlink_live_socket(tmp_path):
   path = str(tmp_path / "run" / "web.sock")
   first = bind_private_socket(path)
   try:
      with pytest.raises(Exception, match="socket is already in use"):
         bind_private_socket(path)
      assert stat.S_ISSOCK(os.stat(path).st_mode)
   finally:
      first.close()
```

Also test a pre-existing regular file, symlink, foreign-owned socket (with injected stat result), operator-owned stale socket removed only after a failed connect probe, run-directory mode repair refusal when ownership differs, and cleanup that unlinks only the exact inode created by this process.

- [ ] **Step 2: Run RED tests**

Run: `venv/bin/python -m pytest tests/web/test_web_socket.py tests/web/test_web_runtime.py -q`

Expected: imports fail because the web socket/runtime modules do not exist.

- [ ] **Step 3: Implement bind/chmod/verify-before-listen and fd handoff**

```python
def bind_private_socket(path, backlog=128):
   run_dir = os.path.dirname(path)
   _ensure_private_run_directory(run_dir)
   _prepare_absent_or_stale_socket(path)
   sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
   old_umask = os.umask(0o177)
   try:
      sock.bind(path)
   finally:
      os.umask(old_umask)
   try:
      os.chmod(path, 0o600)
      _verify_socket(path, expected_uid=os.geteuid(), expected_mode=0o600)
      sock.listen(backlog)
      return sock
   except BaseException:
      sock.close()
      _unlink_if_same_socket(path)
      raise
```

`run_uvicorn(app, sock)` constructs `uvicorn.Config(app, fd=sock.fileno(), access_log=False, server_header=False, proxy_headers=False)` and `uvicorn.Server(config)`. It never passes `uds`, never binds TCP, and keeps the Python socket alive until the server exits.

- [ ] **Step 4: Verify socket/runtime tests and Linux-specific observation loop**

Run: `venv/bin/python -m pytest tests/web/test_web_socket.py tests/web/test_web_runtime.py -q`

The Linux acceptance test samples `lstat().st_mode` from bind through readiness and asserts every observed mode is `0600`. The cross-UID denial test runs only where an existing unprivileged test UID can be selected without sudo; it is a mandatory Polaris deployment check if local automation skips it.

- [ ] **Step 5: Commit and independently review**

```bash
git add node_monitor/web/__init__.py node_monitor/web/socket.py node_monitor/web/runtime.py tests/web/test_web_socket.py tests/web/test_web_runtime.py
git commit -m "feat(web): add private Unix socket runtime"
```

Review must treat any observable `0666`, unsafe unlink, ownership ambiguity, TCP bind, or Uvicorn `uds` usage as blocking.

---

### Task 4: Define and verify the database-to-API metric contract

**Files:**
- Create: `node_monitor/web/queries.py`
- Create: `tests/web/test_web_queries_unit.py`
- Create: `tests/web/test_web_queries_postgres.py`
- Create: `tests/web/fixtures.py`

- [ ] **Step 1: Capture the exact stored JSONB shapes in tests before writing queries**

```python
def test_production_shaped_metric_keys():
   counter = production_counter_row()
   usage = production_usage_row()
   assert set(counter["cpu_busy_pct"]) == {"p50", "p95", "max"}
   assert set(counter["network_rates"]["eth0"]["rx_bytes_per_sec"]) == {
      "p50", "p95", "max"}
   assert set(counter["lustre_md_summary"]["open"]) == {
      "p50_sum", "p95_sum", "max_sum", "target_count"}
   assert set(usage["process_count"]) == {"p50", "p95", "max"}
   assert set(usage["rss_kb"]) == {"p50", "p95", "max"}
```

Build these fixtures from the writer/collector contracts, then compare one live `node_monitor_dev` row on Polaris before production launch. A mismatch is fixed in the adapter/query contract; it is never converted to silent nulls.

- [ ] **Step 2: Write RED tests for range, node, username, quality, and aggregation semantics**

```python
def test_unfiltered_usage_sums_only_cpu_and_attributes_hotspots(pg_snapshot):
   result = load_usage(pg_snapshot, node="login-04", start=START, username=None)
   grain = result.by_key[("ai_coding", "active", END)]
   assert grain.cpu_seconds == 9.0
   assert grain.rss_kb["p95"] == 8000
   assert grain.rss_kb["p95_username"] == "largest-user"
   assert grain.d_state_fraction == 0.40
   assert grain.d_state_username == "blocked-user"


def test_username_is_bound_data_not_sql(pg_snapshot):
   hostile = "x' OR true; --%00\n"
   result = load_usage(pg_snapshot, node="login-04", start=START, username=hostile)
   assert result.rows == []
   assert pg_snapshot.tables_intact()


def test_counter_limit_accepts_1440_and_rejects_1441(pg_snapshot):
   assert len(load_counters(pg_snapshot, "login-04", START_24H).rows) == 1440
   pg_snapshot.insert_one_older_counter()
   with pytest.raises(Exception, match="counter row limit exceeded"):
      load_counters(pg_snapshot, "login-04", START_24H_PLUS_ONE_MINUTE)
```

Include tests for the five exact ranges; exact inventory-based node validation; 256-byte UTF-8 username boundary; newest `window_end`; 120-second counter staleness; newest closed `interval_end`; 1,200-second usage staleness; partial/complete criteria; missing intervals as gaps; no zero-fill; D-state maximum grain plus category/activity/username; latest gauges; at most 10 node poll failures; system-level collection log; loopback exclusion; and `max_sum` exported as named constant `LUSTRE_PEAK_SUM_SOURCE = "max_sum"` with API label `peak-sum`.

- [ ] **Step 3: Run RED tests**

Run: `venv/bin/python -m pytest tests/web/test_web_queries_unit.py tests/web/test_web_queries_postgres.py -q`

Expected: query interfaces are missing.

- [ ] **Step 4: Implement fixed, parameterized SQL projections and database-side aggregation**

```python
USAGE_CPU_SQL = text("""
SELECT interval_end, category, activity,
       sum(cpu_seconds) AS cpu_seconds,
       bool_and(sample_count = expected_count AND unmeasured_count = 0) AS complete
FROM node_monitor.node_usage_intervals
WHERE system = :system
  AND source_hostname = :node
  AND interval_start >= :start
  AND (:username_is_null OR username = :username)
GROUP BY interval_end, category, activity
ORDER BY interval_end, category, activity
""")

USAGE_RSS_P95_HOTSPOT_SQL = text("""
SELECT DISTINCT ON (interval_end, category, activity)
       interval_end, category, activity, username,
       (rss_kb ->> 'p95')::double precision AS rss_p95_kb,
       sample_count, expected_count, unmeasured_count
FROM node_monitor.node_usage_intervals
WHERE system = :system
  AND source_hostname = :node
  AND interval_start >= :start
  AND (:username_is_null OR username = :username)
ORDER BY interval_end, category, activity,
         (rss_kb ->> 'p95')::double precision DESC, username_key
""")
```

Create one complete constant statement like the representative RSS-p95 query
above for every allowed non-additive measure/statistic. Map API metric names to
those constants with a fixed Python allowlist; never attempt to bind a SQL
identifier or expression. Value filters (`system`, `node`, `start`, `username`)
remain bound parameters. Use JSONB projection (`column ->> 'p95'`) and explicit
numeric casts only after fixture/live-shape validation. Never load complete ORM
rows. `username_key` is the real stored generated column declared by migration
0001 as `COALESCE(username, '')`; it is intentionally used as the deterministic
null-safe tiebreaker and is not a placeholder.

- [ ] **Step 5: Run `EXPLAIN (ANALYZE, BUFFERS)` against the 24-hour PostgreSQL fixture**

Run the exact counter, usage CPU, each hotspot, poll-failure, and collection-log statement with two nodes and production-shaped cardinality. Save the machine-readable plan summaries in test output artifacts, not the public repository. Add no index unless measured latency/planner evidence violates the three-second statement deadline; any required index becomes a separately reviewed operator migration rather than runtime DDL.

- [ ] **Step 6: Verify tests and commit**

Run: `venv/bin/python -m pytest tests/web/test_web_queries_unit.py tests/web/test_web_queries_postgres.py -q`

```bash
git add node_monitor/web/queries.py tests/web/test_web_queries_unit.py tests/web/test_web_queries_postgres.py tests/web/fixtures.py
git commit -m "feat(web): add bounded dashboard queries"
```

Independent review must verify mathematical semantics, JSONB mappings, no percentile-of-percentiles, no unweighted fraction averages, parameterization, row bounds, and planner evidence before Task 5.

---

### Task 5: Add the atomic transaction runner, timeout cancellation, and dashboard service

**Files:**
- Modify: `node_monitor/database/web.py`
- Create: `node_monitor/web/service.py`
- Create: `tests/web/test_web_transaction.py`
- Create: `tests/web/test_web_service.py`

- [ ] **Step 1: Write deterministic RED tests for atomicity, timeout cleanup, and size bounds**

```python
def test_snapshot_uses_one_connection_and_repeatable_read_read_only(web_service):
   result = web_service.dashboard(node="login-04", range_name="1h", username=None)
   assert result.debug_observation.connection_ids == {result.debug_observation.connection_id}
   assert result.debug_observation.transaction_isolation == "repeatable read"
   assert result.debug_observation.transaction_read_only == "on"


def test_outer_timeout_cancels_joins_rolls_back_before_return(timeout_harness):
   response = timeout_harness.request_with_blocked_statement()
   assert response.safe_error == "dashboard request timed out"
   assert timeout_harness.dbapi_cancel_called
   assert timeout_harness.worker_done_before_response
   assert timeout_harness.rollback_called
   assert timeout_harness.pool_checked_out == 0


def test_oversized_response_fails_without_truncation(web_service):
   web_service.inject_serialized_payload(b"x" * (5 * 1024 * 1024 + 1))
   with pytest.raises(Exception, match="dashboard response exceeds limit"):
      web_service.dashboard(node="login-04", range_name="24h", username=None)
```

Also test cancellation failure invalidates/poisons the connection rather than returning it, one subquery failure rejects the whole response, exact 5-MiB success, 5-MiB-plus-one failure, and `server_utc_now` with explicit UTC offset.

- [ ] **Step 2: Run RED tests**

Run: `venv/bin/python -m pytest tests/web/test_web_transaction.py tests/web/test_web_service.py -q`

Expected: runner/service interfaces are absent.

- [ ] **Step 3: Implement one dedicated worker per request with controlling-thread cancellation**

```python
async def run_dashboard_with_deadline(database, operation, timeout_sec=12.0):
   worker = DashboardWorker(database, operation)
   worker.start()
   try:
      return await asyncio.wait_for(worker.result(), timeout=timeout_sec)
   except asyncio.TimeoutError:
      worker.cancel_dbapi()
      await worker.join()
      if not worker.rollback_succeeded:
         worker.invalidate_connection()
      raise DashboardTimeout("dashboard request timed out") from None
```

`DashboardWorker` publishes the checked-out raw DBAPI connection to the controlling event-loop thread through an event-synchronized handoff before executing statements. The worker alone opens the SQLAlchemy connection, begins `REPEATABLE READ, READ ONLY`, runs every subquery, commits/rolls back, and closes. On timeout the controller calls psycopg2 `cancel()` without touching cursor state, then awaits worker termination; no HTTP error is returned first. Cleanup is idempotent under timeout/query-error/client-cancellation races.

- [ ] **Step 4: Implement `DashboardService` assembly and serialized-byte enforcement**

```python
RANGES = {"1h": 3600, "3h": 10800, "6h": 21600,
          "12h": 43200, "24h": 86400}
MAX_RESPONSE_BYTES = 5 * 1024 * 1024
MAX_USERNAME_BYTES = 256


def serialize_dashboard(snapshot):
   payload = json.dumps(snapshot, separators=(",", ":"), allow_nan=False).encode("utf-8")
   if len(payload) > MAX_RESPONSE_BYTES:
      raise DashboardTooLarge("dashboard response exceeds limit")
   return payload
```

The service validates range, username bytes, and inventory node; uses one database snapshot; includes independent counter/usage timestamps and statuses; emits no NaN/Infinity; returns gaps as null-separated series or explicit gap metadata; and never reports HTTP response time as telemetry freshness.

- [ ] **Step 5: Verify focused and real PostgreSQL tests, then commit**

Run: `venv/bin/python -m pytest tests/web/test_web_transaction.py tests/web/test_web_service.py -q`

```bash
git add node_monitor/database/web.py node_monitor/web/service.py tests/web/test_web_transaction.py tests/web/test_web_service.py
git commit -m "feat(web): add atomic dashboard service"
```

Independent review must reproduce timeout paths against real PostgreSQL (`pg_sleep`) and verify the sole pool connection is free or invalidated before response completion.

---

### Task 6: Package FastAPI, static resources, API routes, and the `web` CLI

**Files:**
- Modify: `requirements.txt`
- Modify: `setup.py`
- Create: `node_monitor/web/app.py`
- Create: `node_monitor/web/static/index.html`
- Create: `node_monitor/web/static/styles.css`
- Create: `node_monitor/web/static/app.js`
- Create: `node_monitor/web/static/chart.umd.min.js`
- Modify: `node_monitor/cli/main.py`
- Create: `tests/web/test_web_app.py`
- Create: `tests/web/test_web_cli.py`
- Create: `tests/web/test_web_packaging.py`

- [ ] **Step 1: Vendor and record a pinned Chart.js release**

Download the official minified UMD asset for a fixed Chart.js release over HTTPS, record its upstream version and SHA-256 in `docs/web-dashboard.md`, and verify the downloaded checksum before adding it. Do not copy from a CDN at runtime and do not synthesize a replacement chart library.

- [ ] **Step 2: Write RED route, static allowlist, CLI, and clean-install tests**

```python
def test_health_never_touches_database(test_client, database_spy):
   assert test_client.get("/health").json() == {"status": "ok"}
   assert database_spy.calls == []


def test_dashboard_rejects_unknown_range_with_safe_body(test_client):
   response = test_client.get("/api/dashboard", params={"node": "login-04", "range": "7d"})
   assert response.status_code == 422
   assert response.json() == {"detail": "invalid dashboard request"}


def test_static_routes_are_an_exact_allowlist(test_client):
   assert test_client.get("/").status_code == 200
   assert test_client.get("/static/styles.css").status_code == 200
   assert test_client.get("/static/app.js").status_code == 200
   assert test_client.get("/static/chart.umd.min.js").status_code == 200
   assert test_client.get("/static/../config.py").status_code == 404
   assert test_client.get("/static/config.yaml").status_code == 404
```

CLI tests require `node-monitor web --config FILE` only, no `--host`, `--port`, `--uds`, `--socket-path`, or public escape hatch. Missing/bad config, schema, privilege, and socket preflight failures are bounded and credential-free. The startup line is emitted only after all preflight and socket checks and contains only PID plus expanded socket path.

- [ ] **Step 3: Run RED tests**

Run: `venv/bin/python -m pytest tests/web/test_web_app.py tests/web/test_web_cli.py tests/web/test_web_packaging.py -q`

Expected: FastAPI/static/CLI/package behavior is absent.

- [ ] **Step 4: Add pinned runtime/test dependencies and exact package data**

```python
package_data={
   "node_monitor.database.migrations.versions": ["*.sql", "*.sql.mode"],
   "node_monitor.web": [
      "static/index.html",
      "static/styles.css",
      "static/app.js",
      "static/chart.umd.min.js",
   ],
}
```

Add compatible pinned lower/upper bounds for FastAPI and Uvicorn to runtime dependencies. Put Playwright and build tooling in a separate test requirements file so production installation does not install browsers.

- [ ] **Step 5: Implement app factory and thin CLI wiring**

```python
def create_app(service):
   app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

   @app.get("/health")
   async def health():
      return {"status": "ok"}

   @app.get("/api/dashboard")
   async def dashboard(node: str, range: str = "1h", username: str = None):
      try:
         payload = await service.dashboard(node=node, range_name=range, username=username)
         return Response(content=payload, media_type="application/json")
      except DashboardRequestError:
         raise HTTPException(status_code=422, detail="invalid dashboard request") from None
      except DashboardServiceError:
         raise HTTPException(status_code=503, detail="dashboard refresh failed") from None

   _register_allowlisted_static_routes(app)
   return app
```

The CLI resolves only `NODE_MONITOR_WEB_DB_URL`, preflights before binding/listening, and closes the web engine/socket in `finally`. Do not import `MigrationRunner` from `cli/main.py` into the web path; isolate imports inside existing operator database commands if necessary to satisfy the import-graph test.

- [ ] **Step 6: Build/install wheel and sdist and verify resources**

Run: `venv/bin/python -m build --no-isolation`

Install each artifact into a fresh temporary venv and assert the four static files and web dependencies are present, `/static/config.py` and `/static/config.yaml` return 404, and `node_monitor.web` resolves under that venv's site-packages rather than the developer tree.

- [ ] **Step 7: Verify and commit**

Run: `venv/bin/python -m pytest tests/web/test_web_app.py tests/web/test_web_cli.py tests/web/test_web_packaging.py -q`

```bash
git add requirements.txt setup.py node_monitor/web/app.py node_monitor/web/static node_monitor/cli/main.py tests/web/test_web_app.py tests/web/test_web_cli.py tests/web/test_web_packaging.py docs/web-dashboard.md
git commit -m "feat(web): expose private dashboard service"
```

Independent review blocks on any arbitrary static mount, wildcard CORS, credential reflection, database query from `/health`, generic network bind, or packaging omission.

---

### Task 7: Implement the operator shell, connection state, cards, and local age clock

**Files:**
- Modify: `node_monitor/web/static/index.html`
- Modify: `node_monitor/web/static/styles.css`
- Modify: `node_monitor/web/static/app.js`
- Create: `tests/browser/test_dashboard_states.py`

- [ ] **Step 1: Write browser acceptance tests before UI implementation**

```python
def test_disconnect_retains_last_snapshot_and_age_advances(page, live_web):
   page.goto(live_web.url)
   page.get_by_text("Connected").wait_for()
   first_value = page.locator("[data-testid=data-age]").get_attribute("data-age-seconds")
   live_web.fail_dashboard_requests()
   page.get_by_role("button", name="Refresh").click()
   page.get_by_text("Web server disconnected").wait_for()
   assert page.locator("[data-testid=cpu-busy]").is_visible()
   page.wait_for_timeout(2100)
   second_value = page.locator("[data-testid=data-age]").get_attribute("data-age-seconds")
   assert int(second_value) >= int(first_value) + 2


def test_current_counters_do_not_freshen_stale_usage(page, live_web):
   live_web.seed_current_counter_stale_usage()
   page.goto(live_web.url)
   assert page.locator("[data-testid=counter-freshness]").inner_text() == "Current"
   assert page.locator("[data-testid=usage-freshness]").inner_text() == "Stale"
```

Cover initial loading, first-load connection failure plus Retry, connected/current, connected/stale, disconnect-after-success, empty range, partial data, all five range buttons, node switching, username match/no-match, 60-second polling, and a one-second local age tick with no extra network request. Inject a deliberately skewed wall clock with Playwright `add_init_script`; assert age uses server-reported age plus monotonic elapsed rather than browser wall time.

- [ ] **Step 2: Run browser tests and observe RED**

Run: `venv/bin/python -m pytest tests/browser/test_dashboard_states.py -q`

Expected: required DOM and behavior do not exist.

- [ ] **Step 3: Implement accessible state and fetch model**

```javascript
const state = {
  snapshot: null,
  connected: false,
  receivedMonotonicMs: 0,
  serverAgeAtReceiptSec: 0,
  refreshTimer: null,
};

async function refreshDashboard() {
  try {
    const response = await fetch(buildDashboardUrl(), {
      signal: AbortSignal.timeout(15000),
      cache: "no-store",
    });
    if (!response.ok) throw new Error("refresh failed");
    const snapshot = await response.json();
    state.snapshot = snapshot;
    state.connected = true;
    state.receivedMonotonicMs = performance.now();
    state.serverAgeAtReceiptSec = Math.max(
      0,
      (Date.parse(snapshot.server_utc_now)
       - Date.parse(snapshot.counter.newest_window_end)) / 1000,
    );
    render(snapshot);
  } catch (_error) {
    state.connected = false;
    renderConnectionState();
    if (state.snapshot === null) renderInitialFailure();
  }
}

function currentCounterAgeSeconds() {
  return state.serverAgeAtReceiptSec
    + (performance.now() - state.receivedMonotonicMs) / 1000;
}
```

Start one 60-second refresh interval only after initial setup; run a separate one-second display timer that never fetches. State changes must preserve the previous complete snapshot after failures. Every control is keyboard reachable, has a visible focus state, and uses text plus shape rather than color alone.

- [ ] **Step 4: Implement cards with scientifically exact labels**

Cards show counter timestamp/age/coverage, CPU p50, load1/load5/load15, D-state per-grain hotspot with its own usage interval timestamp and attribution, system-used physical memory (`MemTotal - MemAvailable`) with kernel/cache note, latest running/total process gauges, and up to 10 recent poll failures with breaker state. Null/zero `mem_total_kb` disables percent view without NaN/Infinity while preserving GiB when possible.

- [ ] **Step 5: Verify browser states at desktop and narrow widths**

Run: `venv/bin/python -m pytest tests/browser/test_dashboard_states.py -q`

Capture failures with console logs enabled. The acceptance test asserts no uncaught console errors, every current value has a textual equivalent, and the narrow layout stacks without hidden quality metadata.

- [ ] **Step 6: Commit and independently review**

```bash
git add node_monitor/web/static/index.html node_monitor/web/static/styles.css node_monitor/web/static/app.js tests/browser/test_dashboard_states.py
git commit -m "feat(web): add operational dashboard states"
```

Review checks backend/frontend field names and types, age-clock skew, stale/disconnected distinction, timestamp provenance, responsive rendering, and accessibility before charts are added.

---

### Task 8: Implement the four bounded chart families and textual alternatives

**Files:**
- Modify: `node_monitor/web/static/index.html`
- Modify: `node_monitor/web/static/styles.css`
- Modify: `node_monitor/web/static/app.js`
- Create: `tests/browser/test_dashboard_charts.py`

- [ ] **Step 1: Write RED browser tests for chart data and semantics**

```python
def test_counter_gap_breaks_line_without_zero_fill(page, live_web):
   live_web.seed_counter_gap()
   page.goto(live_web.url)
   cpu_data = page.evaluate("window.__nodeMonitorTest.chartData.cpu")
   assert None in cpu_data
   assert 0 not in cpu_data_for_missing_interval(cpu_data)


def test_lustre_peak_sum_explains_aggregation(page, live_web):
   page.goto(live_web.url)
   page.get_by_role("button", name="Lustre").click()
   page.get_by_role("button", name="peak-sum").click()
   assert "sum of per-target maxima" in page.locator(
      "[data-testid=network-lustre-note]").inner_text()
```

Cover CPU p50/p95/max and load/D-state cadence; memory percent/GiB and missing total; process metric toggles and hotspot username attribution; network/Lustre toggle; loopback exclusion; target count; all tooltip units/aggregation/coverage; D-state/interactivity observation weighting; RSS labeled `grain total RSS`; external requests blocked; and visible nonblank canvas marks with nonempty underlying arrays. Use a test-only read-only `window.__nodeMonitorTest` projection of chart inputs, not production mutation hooks.

- [ ] **Step 2: Run RED tests**

Run: `venv/bin/python -m pytest tests/browser/test_dashboard_charts.py -q`

Expected: chart controls/data are absent.

- [ ] **Step 3: Implement chart lifecycle with explicit gap handling**

```javascript
function seriesWithGaps(points, valueKey) {
  return points.map((point) => ({
    x: point.timestamp,
    y: point.quality.gap ? null : point[valueKey],
    quality: point.quality,
  }));
}

function replaceChart(name, canvas, config) {
  if (charts[name]) charts[name].destroy();
  charts[name] = new Chart(canvas, config);
}
```

Use `spanGaps: false`, color-blind-safe colors plus distinct dash/point styles, bounded datasets only from the atomic response, and textual summary/table alternatives adjacent to each canvas.

- [ ] **Step 4: Implement exact chart semantics**

- CPU/load/D-state: one-minute counter lines plus visibly sparse closed 15-minute D-state hotspots; separate axes/toggles for unlike units.
- Memory: MemAvailable and system-used physical memory, percent/GiB toggle, no invented percentage.
- Process: CPU sum or per-username-grain maxima with contributor; cadence and observation denominator stated.
- Network/Lustre: per-interface RX/TX with p50 default; Lustre p50-sum/p95-sum/peak-sum with target count and exact caveat.

- [ ] **Step 5: Verify with browser tests and visual QA**

Run: `venv/bin/python -m pytest tests/browser/test_dashboard_charts.py -q`

Capture representative current, stale/partial, and narrow-window screenshots. Inspect each for clipped labels, blank canvases, illegible notes, status-color ambiguity, and controls hidden behind overflow. Fix and repeat until both DOM assertions and visual inspection are clean.

- [ ] **Step 6: Commit and independently review**

```bash
git add node_monitor/web/static/index.html node_monitor/web/static/styles.css node_monitor/web/static/app.js tests/browser/test_dashboard_charts.py
git commit -m "feat(web): add operational metric charts"
```

Review verifies chart claims against stored metric semantics, backend contracts, real-browser console output, and visible rendering—not merely endpoint 200s.

---

### Task 9: Add production-shaped performance, isolation, and full acceptance gates

**Files:**
- Create: `tests/web/test_web_performance_postgres.py`
- Create: `tests/web/test_web_database_isolation.py`
- Create: `tests/web/test_web_process_isolation.py`
- Create: `tests/browser/test_dashboard_acceptance.py`
- Modify: `docs/web-dashboard.md`

- [ ] **Step 1: Seed a 24-hour, two-node production-shaped PostgreSQL fixture**

```python
COUNTER_WINDOWS = 1440
USAGE_WINDOWS = 96
CATEGORIES = ("ai_coding", "shell", "editor", "scheduler", "other")
ACTIVITIES = ("active", "idle", "unknown")
USERS = tuple("user-%02d" % index for index in range(32))


def expected_usage_rows_per_node():
   return USAGE_WINDOWS * len(CATEGORIES) * len(ACTIVITIES) * len(USERS)
```

Populate valid JSONB structures, gaps, partial rows, reboots/resets, null hardware totals, failures, and collection events. The fixture must be large enough to disprove application-side raw-row loading and must remain confined to a disposable UUID-named database.

- [ ] **Step 2: Measure and assert query/response/resource bounds**

Run the complete 24-hour endpoint repeatedly after one warm-up and record median/p95 latency, inspected counter rows, aggregate rows returned, serialized bytes, and process RSS delta. Blocking acceptance thresholds are: no statement exceeds 3 seconds; complete server request under 12 seconds; serialized response at most 5 MiB; exactly no more than 1,440 counter rows; and peak process RSS delta at most 256 MiB for one request. If measured data exceeds a threshold, stop and redesign database-side aggregation rather than truncate or raise the limits silently.

- [ ] **Step 3: Prove database and collector isolation**

```python
def test_reader_cannot_mutate_or_run_ddl(reader_connection):
   statements = (
      "INSERT INTO node_monitor.node_collection_log(system, recorded_at, event, detail) VALUES ('x', now(), 'x', '{}')",
      "UPDATE node_monitor.node_hardware SET cpu_logical = 1",
      "DELETE FROM node_monitor.node_poll_failures",
      "CREATE TABLE node_monitor.forbidden(id integer)",
      "ALTER TABLE node_monitor.node_hardware ADD COLUMN forbidden integer",
   )
   for statement in statements:
      with pytest.raises(Exception):
         reader_connection.exec_driver_sql(statement)
      reader_connection.rollback()
```

While inducing web query timeouts/failures, observe the resident-like test collector heartbeat and maximum counter timestamp advance. Assert web and daemon engines/pools are distinct objects. Killing the web subprocess must not signal or stale the collector; stopping the collector fixture leaves `/health` available and dashboard data connected-but-stale.

- [ ] **Step 4: Run the complete real-browser matrix with external networking blocked**

Run all states, five ranges, two nodes, username match/no-match, network/Lustre and process toggles, desktop/narrow layouts, local age advancement, first-load/later disconnect, stale/partial/empty, and packaged static assets. Playwright aborts every request whose host is not the local tunneled test server; a blocked external request fails the test.

- [ ] **Step 5: Verify full local suite and clean distributions**

Run:

```bash
venv/bin/python -m pytest -q
venv/bin/python -m build --no-isolation
```

Then install and test both wheel and sdist in clean venvs. Record exact pass/skip/warning counts and artifact checksums. No success claim uses the system Python 3.9 run; the verified project interpreter is `venv/bin/python` 3.11.

- [ ] **Step 6: Complete the public runbook and commit**

The runbook documents the web-only YAML without credentials, foreground command, mode requirements, detached `screen` example, health check, tunnel target explicitly tied to `web.socket_path`, no-restart limitation, log location, duplicate-start behavior, and manual cross-UID socket denial check. It states that PostgreSQL role creation is an operator action and node-monitor never manages the shared server.

```bash
git add tests/web/test_web_performance_postgres.py tests/web/test_web_database_isolation.py tests/web/test_web_process_isolation.py tests/browser/test_dashboard_acceptance.py docs/web-dashboard.md
git commit -m "test(web): add production acceptance gates"
```

- [ ] **Step 7: Exact-head independent review**

Dispatch separate specification, code-quality, security/deployment, and scientific-semantics reviews over the exact `8f397d2..HEAD` diff. A truncated review is not approval. Any Critical or Important finding blocks merge; fixes require focused tests, full-suite rerun, a new commit, and exact-new-head re-review.

---

### Task 10: Merge, deploy, and verify without disturbing the resident collector

**Files:**
- No unreviewed source changes during deployment
- Private deployment config under the operator home, mode `0600`
- Immutable release under `/home/parton/node_monitor-releases/<full-sha>/`

- [ ] **Step 1: Verify integration state before merge**

Run:

```bash
git status --short
git diff --check 8f397d2850180c0c843245147931c1a11a8d96..HEAD
venv/bin/python -m pytest -q
git rev-parse HEAD
```

Require clean status, zero diff-check errors, full suite green, and explicit exact-head review approval.

- [ ] **Step 2: Merge according to repository policy and verify SHA equality**

Merge the reviewed feature branch into `main`, push, then verify local `main`, `origin/main`, and GitHub `main` all resolve to the same full SHA. Re-run the full suite on merged `main`. Retire the implementation worktree only after the merged result is verified.

- [ ] **Step 3: Pre-deployment collector baseline**

On `polaris-login-04`, verify the borrowed SSH ControlMaster with an actual command without closing it. Record two resident-collector observations at least one collection interval apart: PID/start ticks, advancing control heartbeat, advancing `node_counter_minute` count/max timestamp, diagnostic census growth, and unchanged poll/collection failure counts.

- [ ] **Step 4: Create the dedicated reader identity and web config as operator actions**

Using the existing shared PostgreSQL server without restarting/reconfiguring it, create or repair only the dedicated node-monitor reader role and grants for `node_monitor_dev`. Revoke database TEMP/CREATE, schema CREATE, and all table write/control privileges; grant database CONNECT, schema USAGE, and SELECT on the six required tables. Write `~/node_monitor/web.config.dev.yaml` mode `0600` with its dedicated URL and socket path `/home/parton/.node-monitor/run/web.sock`. Read back effective privileges through the web preflight command; do not print the URL or role name.

- [ ] **Step 5: Deploy the immutable reviewed SHA and run host acceptance**

Install the exact SHA under `/home/parton/node_monitor-releases/<sha>/` with its own Python 3.13-compatible venv, verify package imports/static resources, and run focused Linux socket, CLI, and PostgreSQL tests. Start a dedicated detached `screen` session with a mode-0600 log and no automatic restart loop.

- [ ] **Step 6: Verify the private service and absence of TCP exposure**

On the host, verify run-directory mode `0700`, socket mode `0600`, owner, live `/health`, and no node-monitor TCP listener. Attempt a duplicate launch and require a clear refusal without changing the serving PID/socket inode. From a different local UID where permitted, require socket connection denial.

- [ ] **Step 7: Verify through the actual SSH tunnel and real browser**

Use a tunnel whose Unix target exactly equals `web.socket_path`:

```bash
ssh -L 127.0.0.1:8091:/home/parton/.node-monitor/run/web.sock polaris-login-04
```

Open `http://127.0.0.1:8091`; verify `/health`, `/api/dashboard`, bundled assets, all controls/charts, current/stale semantics, and no browser console or external-network errors.

- [ ] **Step 8: Post-deployment collector-isolation verification**

Repeat two collector observations one interval apart while refreshing the dashboard and inducing one invalid dashboard request. Require collector heartbeat/count/max timestamp and census bytes to advance, no new poll/collection failures, and unchanged resident PID/start ticks. Stop only the web `screen` process as an isolation test, verify the collector continues, restart the reviewed web command once, and read back its exact PID/socket state.

- [ ] **Step 9: Record final task state**

Record the merged/deployed SHA, exact local/origin/GitHub equality, test results, web process/session/socket, measured 24-hour latency/bytes/RSS, and collector-isolation evidence in task progress. Do not store credentials, URLs with passwords, or one-off secrets.

---

## Plan self-review

- **Specification coverage:** Tasks 1–10 cover the exact web-only config, reader/schema gate, timeout hierarchy, private pre-bound socket, atomic bounded API, all four plot families, 60-second polling/local age, packaging, browser behavior, PostgreSQL and process isolation, review, and Polaris deployment.
- **Scope:** Release one remains one page, one selected node, at most 24 hours, PostgreSQL aggregates only. It adds no PID/argv view, diagnostic-census endpoint, analytics page, automated diagnosis, retention, authentication, TCP listener, or web lifecycle subcommands.
- **Type/semantic consistency:** `window_end` drives counter freshness; `interval_end` drives usage freshness. CPU seconds alone sum across users. Percentiles/fractions use attributed max grains. `max_sum` is exposed as `peak-sum`. Collection-log events remain system-level.
- **Security:** No web path imports/constructs the DDL runner, reads writer credentials, runs migrations, exposes generic static paths, or binds TCP. Startup proves effective privileges rather than comparing URL strings.
- **Measurement before schema change:** The plan runs production-shaped `EXPLAIN` and performance acceptance before proposing any index migration or additional aggregate row bound.
