# node_monitor

Login-node observability for ALCF systems. A resident daemon that SSH fans out
to a configured set of login nodes, samples process and node-level state, and
writes aggregates to PostgreSQL.

**Status: Phase 0 implemented.** The JSONL-only Phase 0 daemon (config,
transport, metrics/usage transforms, scheduler, sink, CLI, and deployment
scripts) is built and unit/integration tested. It has not yet been proven by a
real 24-hour Polaris canary run; see `PHASE0_DAEMON_IMPLEMENTATION_PLAN.md`
(maintained outside this repository) for the remaining empirical-verification
task. Phase 0 writes JSONL artifacts under `$HOME` only -- no PostgreSQL
access; the PostgreSQL-backed collector described below is a later phase.

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
daemon is still alive (tracked via a self-authored lock file plus a
PID + process-identity check -- never a `pgrep` text match, and never a
signal stronger than the harmless `kill -0` existence probe) and reclaims the
lock once that prior daemon has genuinely exited. `check_phase0.sh` is
read-only: it reports whether a daemon is currently running and, once
finished, runs `node-monitor validate-run` against the most recent run
directory. See `--help` on either script for the full flag list.
