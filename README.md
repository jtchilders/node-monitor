# node_monitor

Login-node observability for ALCF systems. A resident daemon that SSH fans out
to a configured set of login nodes, samples process and node-level state, and
writes aggregates to PostgreSQL.

**Status: Phase 0 and the initial PostgreSQL runtime are implemented.** The
JSONL-only `daemon dry-run` and `daemon smoke` commands remain available for
canaries. The production-shaped `daemon run` command writes five compact record
types to PostgreSQL through a bounded asynchronous queue while retaining
`diagnostic_census` as JSONL only. It requires an operator-migrated, current
schema and never runs DDL itself.

The detailed design document is maintained outside this repository. It records
site-specific operational detail -- database topology, measured fleet state,
account names -- that is not appropriate for a public repo. This README plus
the docstrings are the public specification; section references in code
comments (e.g. "see design section 8.1") point at that document.

## What it is for

1. **Operational** -- distinguish login-node misuse from genuine
   under-provisioning.
2. **Capacity sizing** -- supply measured evidence for ALCF-4.
3. **Design** -- characterize login-node use in the age of AI coding agents.

The emphasis is *behavior extraction*, not application performance profiling.

## What it is not

- Not a bug-hunting or profiling tool.
- Not coupled to `pbs_monitor`. It shares that project's PostgreSQL *server*
  and nothing else: its own `node_monitor` schema, no foreign keys, no
  cross-schema joins. `node_monitor` must work if `pbs_monitor` is uninstalled.
- Not privileged. No sudo, no root, no daemon-managed Postgres.

## Data collected

Real usernames and project attribution are stored deliberately. The hard limit is file *content*, which is enforced by the kernel
rather than by policy: `/proc/<pid>/{cwd,exe,environ,fd,io}` are unreadable for
other users' processes. Raw command-line arguments are never written to the
database, because argv routinely carries credentials.

## Layout

    node_monitor/
      cli/          command-line surface
      collector/    daemon loops, SSH fan-out, remote probe
      database/     schema, models, writers
    deploy/         install and runbook scripts (never touches Postgres lifecycle)
    docs/archive/   superseded design documents
    tests/

## Conventions

- **3-space indentation**, matching `pbs_monitor`.
- `node_monitor/collector/remote_probe.py` is **quarantined at Python 3.6**:
  stdlib only, no f-strings in the shipped path, never imported by the daemon.
  Login nodes run system `python3` 3.6.15, but the probe script itself is
  compatible with and executed by any explicitly configured `probe_python`
  (e.g. `python3.9`, `python3.11`) -- see `config.example.yaml`'s
  `probe_python`. The rest of the codebase, including the daemon and CLI,
  requires **Python 3.9+** (`setup.py`'s `python_requires`); it is never run
  under the 3.6 system interpreter.

## Development

    python3 -m venv venv && source venv/bin/activate
    pip install -r requirements.txt
    pip install -e .
    pytest

## PostgreSQL daemon

Create a dedicated node-monitor database and role administratively. A development
deployment may share the PostgreSQL server used by another application, but must
use its own database (for example `node_monitor_dev`) and the schema must be
exactly `node_monitor`. Node-monitor never manages the PostgreSQL server or
another application's database.

After configuring the database URL, apply migrations explicitly and start the
database-writing daemon:

    node-monitor database status --config config.example.phase1.yaml
    node-monitor database migrate --config config.example.phase1.yaml
    node-monitor database status --config config.example.phase1.yaml
    node-monitor daemon run --config config.example.phase1.yaml

`daemon run` performs a read-only schema gate and refuses uninitialized,
pending, or drifted schemas. It writes `node_hardware`, compact counter-minute
records, usage intervals, poll failures, and collection-log events relationally.
Diagnostic censuses remain in the run directory as JSONL and have no relational
table. Sustained writer failures are fatal and cannot produce a `DONE` marker.

See [`docs/database.md`](docs/database.md) for prerequisites, advisory locking,
fail-closed drift handling, backup/restore policy, and the strict prohibition on
daemon DDL or PostgreSQL lifecycle management.

## Phase 0: running, artifacts, and validation

Phase 0 is the JSONL-only daemon: no PostgreSQL access, output is a
run-scoped directory of newline-delimited JSON files under `$HOME`.

Ad hoc / foreground runs (see `--help` on each subcommand for full options):

    node-monitor daemon dry-run --config config.example.yaml
    node-monitor daemon smoke --config config.example.yaml --duration-sec 30
    node-monitor validate-run <run_dir>

Each run directory (named `phase0-<run_id>` under the config's
`output_root`, mode 0700) contains one `.jsonl` file per production/
diagnostic record type, `manifest.json`, `summary.json`, and -- once the run
has cleanly finalized -- a `DONE` flag file. `validate-run` re-parses every
`.jsonl` artifact in a run directory, reports malformed/truncated lines, and
confirms `DONE` is present and consistent with a clean finish; it exits
nonzero if the run directory is missing, incomplete, or malformed.

### Detached deployment (`deploy/`)

`deploy/run_phase0.sh` and `deploy/check_phase0.sh` launch and check an
explicitly-durationed Phase 0 run detached under `/usr/bin/screen`, entirely
under `$HOME` (never `/tmp`), without ever starting, stopping, or otherwise
touching PostgreSQL/`pbs_monitor`:

    deploy/run_phase0.sh --config PATH --duration-sec SECONDS
    deploy/check_phase0.sh

`run_phase0.sh` refuses to launch a duplicate while a prior invocation's
invocation's daemon is still alive (tracked via a self-authored lock file
recording the exact PID + screen session name, atomically claimed so two
concurrent launches can never both succeed; a candidate lock is trusted only
when its PID is alive AND `ps -o command=` shows that exact recorded session
name -- never a `pgrep` text match, a generic node-monitor/screen substring
match, or a signal stronger than the harmless `kill -0` existence probe) and
reclaims the lock once that prior daemon has genuinely exited or the lock
is malformed. `check_phase0.sh` is read-only: it reports whether a daemon
is currently running and, once finished, runs `node-monitor validate-run`
against a run directory -- by default the most recent one under
`$HOME/phase0-runs`, or an explicit `--output-root DIR` (when the config
used a non-default `output_root`) or `--run-dir DIR` (to validate one exact
run directory directly). See `--help` on either script for the full flag
list.

## Web dashboard (TCP-only, `docs/web-dashboard.md`)

`node-monitor web --config FILE [--host HOST] [--port PORT] [--no-browser]`.
Default `127.0.0.1:8080`; TCP only (no Unix sockets; `socket_path` removed/rejected).
Use `screen`/`tmux` for persistence. See `docs/web-dashboard.md`.
