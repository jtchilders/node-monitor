# node_monitor

Login-node observability for ALCF systems. A resident daemon that SSH fans out
to a configured set of login nodes, samples process and node-level state, and
writes aggregates to PostgreSQL.

**Status: pre-implementation.** The design is complete and reviewed; the
collector is being implemented now.

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
  Login nodes run system python3 3.6.15. Everything else targets 3.9+.

## Development

    python3 -m venv venv && source venv/bin/activate
    pip install -r requirements.txt
    pip install -e .
    pytest
