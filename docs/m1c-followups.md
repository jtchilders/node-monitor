# M1c follow-ups

M1c delivered the collector daemon, its manual operation on ALCF login nodes, and the read-side
CLI. This page lists the decisions taken along the way, the items deliberately deferred, and the
open work for the one-week run and for M2. Smoke results are in `docs/m1c-smoke.md`.

## Owner decisions

- **Per-user maps.** `user_usage_5m` drops `by_category` and `by_dormancy`. Both are exact per-key sums of `by_category_dormancy` (migration 0002).
- **No systemd.** ALCF login nodes give users no systemd. The daemon is started with `node-monitor daemon start` and restarted by hand after maintenance, as pbs_monitor is (`deploy/README.md`).
- **PostgreSQL outage test deferred.** The deliberate PostgreSQL outage test is deferred so PBS Monitor isn't disrupted. Task 6's integration test covers the behaviour in code.
- **Trust auth.** Trust auth on the shared PostgreSQL stays as it is for now.
- **Permissions.** `~/node-monitor` was made non-group-writable (`chmod -R g-w,o-w`). `state/` and `logs/` are now 0700, and the daemon keeps them that way.

## Decisions (rulings)

| Decision | Cost if wrong |
|---|---|
| `BufferedWriter.close()` closes its writer; `WriterThread` calls it rather than the private `_writer`. | One extra small method. |
| `queries.py` started in Task 7 with `heartbeat()` only, and grew in Task 8. | None. |
| `config.example.yaml` became the packaged `node_monitor/config_template.yaml`, used by `config --create`. | None. |
| The SIGTERM-during-outage test checks that every tapped record is in the spool, and that any extras are collector-level events. | A weaker check on the extras only. |
| `Upkeep` defers maintenance by one heartbeat interval when that tick's heartbeat failed, rather than scheduling the 1 h retry. | Maintenance starts up to 60 s later after an outage. |
| The final review and its fix wave ran *before* the real-server migration and smoke, so both used the reviewed code. | None. |
| The final fix wave covered I1–I4 and M1–M6; the reviewer's DEFER items stay deferred (below). | None seen. |
| Spool drain: a 20 s replay budget per submit; fresh records go behind the spool until it is drained; superseded live-only spool files are dropped. | A drain takes more rounds. |
| systemd deployment dropped (the unit and `rearm.sh` were removed) in favour of a manual runbook. | Re-adding a unit later is trivial. |
| The smoke ran through `daemon start`/`daemon stop`, with `kill -9` for crash recovery. | None. |

## Before or during the one-week run

- **The week starts from `main` after the merge**, with `node-monitor daemon start`. Nothing restarts it after a crash, so check `node-monitor status` daily and after any maintenance.
- **Daily checks:**
  - heartbeat age;
  - `session_lost` gaps;
  - spool and `corrupt/` size;
  - daemon RSS (≈45 MiB at the start) and probe RSS (≈20 MiB);
  - `live_processes` dead tuples and autovacuum;
  - `daemon.log` size.
- **Node reboot.** Record any maintenance reboot (`node_rebooted`) and the manual restart that follows.

## Found in the smoke (small, do soon)

- **`daemon status` after a crash.** It exits 0 for up to 180 s after a crash, because the heartbeat is still fresh, even though it reports the local process as not running. A free instance lock on the collector host should override heartbeat freshness.
- **No startup or shutdown log lines.** The daemon logs nothing when healthy, so add one line at start (version, nodes) and one at stop.
- **`nodes` "Procs" column.** It is `procs_total` from `/proc/loadavg` (threads), not processes. Rename it or document it.
- **Runbook gaps.** Say that `daemon stop` exits 1 at its timeout and leaves the daemon running. The `chmod 700 …/{state,logs}` step fails before `state/` exists on a fresh install.

## Parked from the final review (accepted for now)

- **Superseded live-only rule.** When outage batches cover different nodes, the rule can drop a node's last live snapshot or its "down" row. That node then shows `stale` until it reconnects; no durable data is lost. A better rule: drop a `-L` file only when newer `-L` files cover every (kind, node) it holds.
- **Slow shutdown after a long outage.** With a spool backlog, shutdown can exceed the 60 s writer join. A SIGKILL then loses only the in-memory final flush, `collector_stopped` and the "stopped" heartbeat; spooled data is safe. Better: once the queue is closed, spool instead of replaying.
- **Unicode format characters.** Cf/Zl/Zp characters (bidi controls, U+2028) pass through table escaping. They can reorder or misalign a cell but can't control the terminal. Escape them too.
- **A test and a notice.** The `daemon start` test of `state/` tightening doesn't discriminate, because `InstanceLock` also tightens it. The stale-node notice ignores the user, category and presence filters.

## Deferred minor items (reviewer-triaged DEFER)

| Item | Why deferred |
|---|---|
| Below-cap replay failures emit `database_unavailable` while the database answers. | Cosmetic; `database_recovered` follows. |
| The head-of-line cap counts transient errors (25006, 55P03, 40P01) that occur while the database answers. | pbs-postgres has no standby; rare. |
| Writer-thread death isn't detected; the daemon would keep collecting into memory. | Needs a BaseException; very unlikely. |
| A daemon started during an outage never re-checks the schema. | The real database is migrated first. |
| The lock is released if the 60 s writer join expires while the writer is still writing. | Short window. |
| The lock probe briefly takes LOCK_EX, so a daemon starting in that window could get a spurious "already running". | Microsecond window; just retry. |
| `run` returns 0 when every session has crashed. | Sessions loop forever unless they crash; check `status`. |
| A crashed session leaves `live_nodes` "up". | Shown `stale` after 120 s. |
| `status --format csv` with no heartbeat prints a message before the CSV. | Send it to stderr later. |
| `daemon status` and `_reader` are two connection-handling implementations. | Refactor. |
| `user` prints "no live processes" when filters exclude everything; "1 nodes" grammar. | Cosmetic. |
| Test polish: weak assertions, `rsplit` dbname parsing, duplicated holder fixture, vacuous signal-restore test, a fixture without a NULL presence row, no test of the starting/down/rotated session mapping. | Test-only. |
| Code polish: private `_SORTS` and `_git` imports, an unused `is_alive`, untyped `_replay_failures`, `probe_source` read twice. | Polish. |
