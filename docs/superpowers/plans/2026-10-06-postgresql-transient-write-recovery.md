# PostgreSQL Transient Write Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Retry whole PostgreSQL write transactions after known transient server aborts, with bounded sanitized retry and recovery logs.

**Architecture:** `DatabaseWriter` remains the transaction boundary and classifies the original SQLAlchemy/DBAPI error before replacing it with a chainless domain error. Validated retry policy is passed from nested database configuration through the daemon CLI. `PostgresDaemonSink` remains the bounded async queue and fatal boundary after permanent or exhausted writer failures.

**Tech Stack:** Python 3.11, SQLAlchemy, psycopg2, pytest, PostgreSQL.

---

### Task 1: Strict retry configuration

**Files:**
- Modify: `node_monitor/config.py`
- Test: `tests/test_config_nested.py`
- Test: `tests/test_cli_daemon_postgres.py`

- [ ] Add failing tests for defaults `5/1/8`, strict positive finite validation, unknown-key rejection, and CLI handoff.
- [ ] Run the focused tests and confirm failures are caused by missing retry fields.
- [ ] Add frozen `DatabaseConfig` fields and strict validation; pass the policy explicitly when constructing `DatabaseWriter`.
- [ ] Run focused tests and commit.

### Task 2: Retry classification and transaction replay

**Files:**
- Modify: `node_monitor/database/writer.py`
- Test: `tests/test_database_writer.py`

- [ ] Add failing tests that inject SQLAlchemy/DBAPI failures carrying SQLSTATEs `57014`, `55P03`, `40001`, and `40P01` and assert complete transaction replay.
- [ ] Add failing tests proving non-allowlisted and absent SQLSTATEs fail immediately and five failed attempts exhaust with delays `1,2,4,8`.
- [ ] Run tests and observe RED.
- [ ] Implement narrow exception-chain SQLSTATE extraction, fixed category mapping, bounded retry policy, and an injected sleeper.
- [ ] Preserve the existing fixed chainless `DatabaseWriteError` at the public boundary.
- [ ] Run focused tests and commit.

### Task 3: Sanitized operational logging

**Files:**
- Modify: `node_monitor/database/writer.py`
- Test: `tests/test_database_writer.py`

- [ ] Add failing `caplog` tests for each retry and eventual recovery.
- [ ] Assert logs contain only event/category/SQLSTATE/attempt/max/delay/batch-size/record-types and exclude sentinel URL, SQL, parameters, hostnames, usernames, and driver text.
- [ ] Run tests and observe RED.
- [ ] Emit fixed-template warning records for retry and recovery.
- [ ] Run focused tests and commit.

### Task 4: Real PostgreSQL acceptance

**Files:**
- Modify: `tests/test_database_writer_postgres.py`

- [ ] Add a test-only database wrapper that raises a real SQLSTATE `57014` on its first transaction before delegating subsequent attempts to the disposable PostgreSQL engine.
- [ ] Verify the retry commits exactly one fact row and leaves no partial first-attempt data.
- [ ] Run the real-PostgreSQL shard with `NODE_MONITOR_TEST_DATABASE_URL` when available and prove disposable-database cleanup.
- [ ] Commit the acceptance test.

### Task 5: Documentation, verification, and delivery

**Files:**
- Modify: `docs/database.md`

- [ ] Document retry defaults, allowlisted transient errors, bounded logging, and why connection failures are excluded without event idempotency keys.
- [ ] Run focused writer/config/sink/CLI tests.
- [ ] Run the explicit web/browser shard and the full maintained-Python suite.
- [ ] Mutation-check the SQLSTATE classifier and exhaustion condition, restore, and re-run GREEN.
- [ ] Run `git diff --check`, inspect the full diff, and require a clean worktree.
- [ ] Obtain independent exact-head code, security, and operations approval.
- [ ] Push, merge through GitHub, verify local/origin/GitHub main SHAs, deploy exact merged SHA to Polaris, restart only node-monitor, and verify heartbeat/data advances across multiple collection intervals.
