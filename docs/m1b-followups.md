# M1b follow-ups

M1b added the PostgreSQL layer, executed task-by-task with reviews. This page records the decisions
taken along the way, the items deliberately deferred, and what M1c must know. Validation results are
in `docs/m1b-database-validation.md`.

## Decisions (rulings)

Each ruling gives the decision, then what it costs if it turns out wrong.

| # | Decision | Cost if wrong |
|---|---|---|
| R1 | Migrations name no roles. `database bootstrap` owns roles, grants and default privileges, and `migrate` runs as the owner (`migrate` now refuses any other role). | None seen; a site with different role names just passes `--owner`/`--reader`. |
| R2 | Retention drops a monthly partition only when its upper bound ≤ `now − days`. | Up to one extra month kept per table. |
| R3 | `DatabaseWriter.url` is a public attribute that can be swapped. | None. |
| Redaction | ALL_CAPS `-D` macro defines (`-DUSE_TOKEN=1`) are never masked; only Java-style property names ending in a long keyword are. | A secret passed as an ALL_CAPS `-D` define would be stored. |
| Redaction | Bare `key` is not a keyword. Compound keywords (`api-key`, `sse-c-key`, `customer-key`, `encryption-key`, `signing-key`, …) are, plus value shapes: Hugging Face `hf_…` tokens and key-shaped values (≥ 24 characters of `[A-Za-z0-9_-]`) of `*-key` options. | A short or path-like `--something-key` secret would be stored. |
| Config | The 0600 rule applies only when the config *file* contains a password. Passwords supplied via `NODE_MONITOR_*_DB_URL` are exempt. | None. |
| `boot_id` | A missing `boot_id` is stored as `'unknown'`. | Hardware changes on a node without a boot id are merged into one row. |
| Errors | SQLSTATE class 54 and 57014 are *not* outages, so they never spool. 40P01, 40001, 55P03 and 25006 *are*, so they spool and retry. | An operator's `pg_cancel_backend` on the writer drops (quarantines) one batch. |
| Replay | A spooled batch that fails with a non-outage error moves to `corrupt/` with a `spool_batch_quarantined` event. A fresh batch that fails the same way is also written to `corrupt/` before the error propagates. | None: nothing is silently dropped; files can be moved back. |
| Summaries | Summary upserts are guarded by `WHERE EXCLUDED.sample_count >= existing`, so a restart's partial interval never overwrites a fuller one. The pipeline never emits a NULL `sample_count`. | A deliberate correction with fewer samples is ignored. |
| Database | Created from `template0` with the `builtin` `C.UTF-8` locale, closed to PUBLIC, with database-level `search_path` and UTC. | None; changing it later would need a re-bootstrap. |
| Auth | Validation proceeded under the owner's "leave trust auth for now" decision (see Security below). | Exposure persists until the owner acts. |

## Owner decisions before M1c accumulates data

1. **Trust auth.**
   - On polaris-login-04, `pg_hba` trusts local and loopback connections and the socket in `/tmp`
     is mode 0777, so any user on the node can connect as the superuser `parton`.
   - That allows reading any table and running commands as `parton` with `COPY … TO PROGRAM`,
     including reading the collector's SSH key.
   - The minimum suggested fix is `unix_socket_permissions = 0700` plus `peer` auth for `local`
     connections. It affects pbs_monitor too.
   - Once M1c runs, `live_processes` holds about 5,000 redacted command lines at any time.
2. **Storage of `user_usage_5m`.**
   - It is projected at ≈ 170–200 GB over 730 days on Polaris, and more on Aurora.
   - `by_category` and `by_dormancy` can be derived from `by_category_dormancy`. Dropping them saves
     about 35% per row.
   - Alternatives are downsampling older intervals, or a shorter retention for the per-user table.

## Must-dos for M1c

- **Batching.** Submit live records (`live_processes`, `live_node`) and summaries in separate
  batches. Spool eviction drops live-only batches first, but only if batches aren't mixed.
- **Event duplicates.** `collection_events` has no natural key. A resend after a lost
  acknowledgement, or a replay, duplicates events (measured: +224 on re-replay). Add a dedupe key,
  for example `(system, node, ts, event)` with `ON CONFLICT DO NOTHING`, in migration 0002, or
  accept duplicates.
- **Head-of-line blocking (M11).** A batch that fails deterministically with an error classed as an
  outage (53200 out of memory, 53400 `temp_file_limit`) blocks replay while the spool fills. Add a
  per-file attempt cap that applies only once a `SELECT 1` probe succeeds.
- **Retention.** `apply_retention` returns `{"created", "dropped", "skipped"}`. Partitions skipped
  on lock timeout (5 s) should be retried on the next daily pass.
- **Draining.** Draining a full 1 GiB spool blocks the writer thread. Decide whether the live path
  must keep flowing during a drain.
- **Autovacuum.** Watch it on `live_processes`: DELETE+INSERT leaves about 1,800 dead tuples per
  node per minute.
- **Probe idle time.** It is measured from the last active sample (or first sight), and that
  state lives in the probe, so a probe restart resets dormancy to zero.
  Long-dormancy queries depend on probe uptime.

## Deferred minor items

| Item | Why deferred |
|---|---|
| `ssh -pSecret` masks the hostname | Pre-existing; ssh `-p` is a port. |
| An unterminated quote after `sshpass -p` over-masks the rest of the line | Fails safe. |
| Dead NaN/inf branch in `_positive_int` | Cosmetic. |
| `_has_password` fails open on a malformed URL | Such a URL can't connect anyway. |
| Missing tests for %-encoded or empty passwords and symlinked configs | Behaviour checked by hand. |
| Migration version gaps (`max(version)`) | Only 0001 exists; fix when 0002 is added. |
| An unlock error in `finally` can mask a migration error; the advisory lock has no timeout | Manual command. |
| No test binds `live_nodes`/event columns to the pipeline's keys | Checked by hand. |
| `connect()` overrides a URL's `connect_timeout` | Harmless. |
| Bootstrap admin connections have no `connect_timeout` | Manual command. |
| Naive datetimes in non-partitioned rows aren't rejected | The pipeline enforces aware datetimes. |
| `write([])` still connects; a stale idle connection surfaces as one outage | Spooled and retried. |
| Stale `.tmp` files never swept; eviction happens before the write; no directory fsync; `corrupt/` is unbounded | Low impact; revisit in M1c. |
| A failed quarantine move re-reads the file on every `pending()` | Rare. |
| Spool names are stamp-then-seq, so a backwards clock step could reorder them | Live tables are timestamp-guarded and summaries keyed by interval. |
| Partition month 13/00 passes the name regex | Needs manual DDL to occur. |
| Test polish: status exit code, bare `StopIteration`, cross-year cutoff, `SystemExit` idiom, close-on-exit-1, replay round trip skips live/event rows | Test polish. |
| `SET LOCAL lock_timeout` in `ensure_partition` lasts until the end of an enclosing transaction | The writer is autocommit. |
| Bootstrap needs a superuser (the REVOKE and `ALTER DATABASE … SET` statements) | The CLI help now says so. |
| Bootstrap leaves `datcollate`/`datctype` at the server default (`en_US.UTF-8`); only the builtin provider locale is `C.UTF-8` | Default collation and index order use the builtin locale. For Aurora's fresh bootstrap, consider also passing `LC_COLLATE`/`LC_CTYPE 'C.UTF-8'`. |
