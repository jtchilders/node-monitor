# PostgreSQL Transient Write Recovery Design

## Goal

Keep the resident node-monitor daemon alive when PostgreSQL aborts a write transaction for a known transient reason, and leave bounded, actionable evidence in the detached daemon log.

## Incident basis

On 2026-10-06 PostgreSQL canceled a `node_counter_minute` insert after the connection's 15-second `statement_timeout`. The writer converted the driver error into `DatabaseWriteError`; `PostgresDaemonSink` converted that into a fatal sink error; the daemon exited with code 2. PostgreSQL remained live. The precise server wait event was not recorded.

## Behavior

`DatabaseWriter.write_records()` will prepare and validate the complete batch once, then retry the complete transaction only when PostgreSQL reports one of these SQLSTATEs:

- `57014`: statement cancellation/statement timeout
- `55P03`: lock unavailable/lock timeout
- `40001`: serialization failure
- `40P01`: deadlock detected

These failures abort the transaction, so replay begins from a new `database.begin()` transaction. This remains safe for both upserted facts and append-only events because no part of the failed transaction commits.

Connection-class failures (`08xxx`) will not be retried in this increment. A lost connection during commit can have an ambiguous outcome; replaying append-only events could duplicate them without an idempotency key. Authentication, constraint, syntax, schema, and conversion failures also fail immediately.

Defaults:

- `database.write_retry_max_attempts: 5` (total attempts, including the first)
- `database.write_retry_initial_delay_sec: 1`
- `database.write_retry_max_delay_sec: 8`

Delay before attempts 2–5 is `1, 2, 4, 8` seconds. Validation requires positive finite values and a maximum delay not smaller than the initial delay.

## Logging

The writer logs through Python's module logger, whose warning output reaches the detached daemon's redirected stderr. Every retry warning contains only fixed/bounded fields:

- event name
- transient category
- allowlisted SQLSTATE
- failed attempt and maximum attempts
- next delay
- batch size
- sorted allowlisted record types

A recovery warning is emitted when a later attempt commits. Exhaustion and permanent failure continue through the existing fixed, chainless `DatabaseWriteError` and `PostgresDaemonSinkError` boundaries. Logs never include exception text, SQL, parameters, record values, URLs, credentials, hostnames, usernames, argv, or telemetry payloads.

## Scope and safety

- No DDL, migration, database creation, PostgreSQL lifecycle operation, or cross-schema access.
- One existing writer thread and one existing connection pool; retries are serial and bounded.
- Queue backpressure remains unchanged while the worker retries.
- Configuration is passed explicitly from the validated `database` section into `DatabaseWriter`.
- Tests inject a sleeper so retry tests do not wait in real time.

## Verification

- Unit tests prove exact SQLSTATE classification, delay sequence, whole-batch replay, recovery logging, immediate permanent failure, exhaustion, and sanitization.
- Configuration and CLI wiring tests prove strict defaults, validation, and explicit handoff.
- A real PostgreSQL test causes one transaction to receive SQLSTATE `57014`, then proves a retry commits exactly one row.
- Focused and full maintained-Python suites must pass.
- Load-bearing tests receive a mutation check.
- Independent exact-head code, security, and operations review must report no Critical or Important findings before merge and deployment.
