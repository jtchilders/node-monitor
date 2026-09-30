# PostgreSQL operations

Node-monitor owns only the PostgreSQL schema named `node_monitor`. It may share
a server with other applications, but it does not read, join, migrate, or
otherwise manage their schemas. It also never starts, stops, restarts, or
configures the PostgreSQL server.

Phase 0 daemon commands remain JSONL-only. The database migration and writer
components are Phase 1 infrastructure; enabling a future PostgreSQL sink is a
separate operational decision.

## Prerequisites

- PostgreSQL 12 or newer.
- A dedicated node-monitor database and role created by an administrator.
- The role must be able to create and modify objects in the `node_monitor`
  schema during migration, and read/write those objects during collection.
- A strict nested node-monitor YAML configuration. The schema value must be
  exactly `node_monitor`.
- A PostgreSQL connection URL supplied through `NODE_MONITOR_DB_URL`, or an
  explicit URL in an untracked, permission-restricted configuration file.
  Never commit credentials. An explicit file URL takes precedence over the
  environment variable.

Node-monitor does not create a database or role. Those remain administrator
responsibilities.

## Safe operator workflow

1. Take and verify a database backup according to site policy. Node-monitor
   does not create or validate backups.
2. Stop or quiesce any future database-writing node-monitor daemon before a
   schema change. Phase 0 JSONL collection is independent and may continue.
3. Inspect the migration state without changing it:

   ```console
   node-monitor database status --config /path/to/config.yaml
   ```

4. Investigate any reported drift before proceeding. Never overwrite the
   migration ledger or edit an already-applied migration to force a match.
5. Apply pending migrations explicitly:

   ```console
   node-monitor database migrate --config /path/to/config.yaml
   ```

6. Run `database status` again and verify that no versions are pending and no
   drift is reported before enabling a database-writing service.

Both commands emit bounded diagnostics and do not print the connection URL or
raw database-driver exception. The configured SQLAlchemy pool is one connection
with no overflow.

## Locking and transactions

Migration execution holds one fixed, session-level PostgreSQL advisory lock
from bootstrap through the final migration. Lock acquisition is nonblocking:
if another operator holds it, the command fails nonzero before migration DDL.
Each transactional migration executes its SQL, schema postcondition, and ledger
insert atomically. Checksums for every applied version are validated before any
pending SQL runs.

The migration ledger is `node_monitor.schema_migrations`. Do not edit it by
hand. Unknown versions, gaps, renamed migrations, changed checksums, unsupported
modes, incompatible existing tables, and failed postconditions all fail closed.

## Failure and rollback policy

There are no automatic down migrations. After a failed or incompatible change:

- keep writers stopped;
- preserve the original bounded command output and inspect PostgreSQL logs using
  site-approved access;
- restore the verified backup when data or schema must be returned to the prior
  state, or ship a separately reviewed forward fix;
- rerun `database status`, then `database migrate`, only after the discrepancy
  is understood.

Never delete ledger rows, change applied migration bytes, disable checksum
validation, or run ad hoc destructive DDL merely to make status appear clean.

## Daemon boundary

The daemon must never invoke migrations or issue schema DDL. Only the explicit
operator command `database migrate` may apply DDL. `database status` is
read-only. The compact writer performs DML only against an already-migrated
schema and fails if required objects are absent.

Node-monitor must never manage PostgreSQL lifecycle operations. Do not add
`pg_ctl`, service-manager, container-runtime, `CREATE DATABASE`, or role-management
commands to daemon, writer, or migration code.
