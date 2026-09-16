#!/usr/bin/env bash
# deploy/check_phase0.sh -- read-only status check for the Phase 0
# node-monitor daemon launched by run_phase0.sh.
#
# Never starts, stops, or signals anything (besides the harmless `kill
# -0` existence probe) and never touches PostgreSQL/pbs_monitor. Reports
# whether a daemon is currently running (per the lock file + a genuine
# PID/process-identity check, never a `pgrep` text match), and once the
# daemon has finished, whether its DONE flag and artifacts validate --
# by shelling out to the installed `node-monitor validate-run` command,
# never re-implementing that check here.
set -euo pipefail

# Unconditional, matching run_phase0.sh -- this script also execs a
# subprocess (node-monitor validate-run) whose environment must never
# inherit LD_PRELOAD.
unset LD_PRELOAD

HOME_DIR="${HOME:-}"
VENV=""
LOCK_FILE=""

usage() {
   cat >&2 <<'EOF'
Usage: check_phase0.sh [--home DIR] [--venv DIR] [--lock-file PATH]

This script never starts, stops, or signals anything (besides the
harmless `kill -0` existence probe) and never touches the project's
database. Reports whether a daemon is currently running (per the lock
file + a genuine PID/process-identity check, never a free-text process
search), and once the daemon has finished, whether its DONE flag and
artifacts validate -- by shelling out to the installed `node-monitor
validate-run` command, never re-implementing that check here.
EOF
}

while [ $# -gt 0 ]; do
   case "$1" in
      --home) HOME_DIR="$2"; shift 2 ;;
      --venv) VENV="$2"; shift 2 ;;
      --lock-file) LOCK_FILE="$2"; shift 2 ;;
      -h|--help) usage; exit 0 ;;
      *) echo "check_phase0.sh: unknown argument: $1" >&2; usage; exit 1 ;;
   esac
done

if [ -z "$HOME_DIR" ]; then
   echo "check_phase0.sh: --home could not be determined (no \$HOME " \
        "set); pass --home explicitly" >&2
   exit 1
fi
HOME_DIR="$(cd "$HOME_DIR" 2>/dev/null && pwd -P || echo "$HOME_DIR")"
case "$HOME_DIR" in
   /tmp|/tmp/*|/private/tmp|/private/tmp/*)
      echo "check_phase0.sh: --home must not resolve under /tmp, got: $HOME_DIR" >&2
      exit 1
      ;;
esac

VENV="${VENV:-$HOME_DIR/node-monitor/venv}"
NODE_MONITOR_BIN="$VENV/bin/node-monitor"

LOCK_FILE="${LOCK_FILE:-$HOME_DIR/.node-monitor-phase0.lock}"

if [ ! -f "$LOCK_FILE" ]; then
   echo "no lock file found at $LOCK_FILE -- no Phase 0 run has been " \
        "launched from this host, or the lock was cleaned up"
   exit 1
fi

lock_pid="$(awk '{print $1}' "$LOCK_FILE" 2>/dev/null || true)"
lock_session="$(awk '{print $2}' "$LOCK_FILE" 2>/dev/null || true)"

# Same identity discipline as run_phase0.sh: liveness alone is never
# sufficient. `kill -0` (existence probe, never a real signal) plus a
# `ps -o command=` match against the recorded session name is required
# before this PID is trusted as "our" daemon -- never a PID matched
# only by a free-text process search.
_pid_is_our_daemon() {
   local pid="$1"
   if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then
      return 1
   fi
   local cmd
   cmd="$(ps -o command= -p "$pid" 2>/dev/null || true)"
   case "$cmd" in
      *"$lock_session"*|*node-monitor*|*screen*)
         return 0
         ;;
      *)
         return 1
         ;;
   esac
}

if [ -n "$lock_pid" ] && _pid_is_our_daemon "$lock_pid"; then
   echo "RUNNING: pid $lock_pid, screen session $lock_session, lock $LOCK_FILE"
   exit 0
fi

echo "not currently running (lock pid $lock_pid is no longer live/identified)"

# Locate the most recently created run directory under this host's
# output_root and validate it via the real, already-installed CLI --
# never a second, locally re-implemented validator.
output_root="$HOME_DIR/phase0-runs"
if [ ! -d "$output_root" ]; then
   echo "check_phase0.sh: no output_root found at $output_root" >&2
   exit 1
fi

latest_run_dir="$(ls -1dt "$output_root"/phase0-*/ 2>/dev/null | head -n 1 || true)"
if [ -z "$latest_run_dir" ]; then
   echo "check_phase0.sh: no phase0-* run directory found under $output_root" >&2
   exit 1
fi
latest_run_dir="${latest_run_dir%/}"

if [ -f "$latest_run_dir/DONE" ]; then
   echo "DONE present: true"
else
   echo "DONE present: false"
fi

if [ ! -x "$NODE_MONITOR_BIN" ]; then
   echo "check_phase0.sh: node-monitor executable not found or not " \
        "executable: $NODE_MONITOR_BIN" >&2
   exit 1
fi

"$NODE_MONITOR_BIN" validate-run "$latest_run_dir"
exit $?
