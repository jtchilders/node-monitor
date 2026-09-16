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
                        [--output-root DIR | --run-dir DIR]

This script never starts, stops, or signals anything (besides the
harmless `kill -0` existence probe) and never touches the project's
database. Reports whether a daemon is currently running (per the lock
file + a genuine PID/process-identity check against the EXACT recorded
screen session, never a free-text process search), and once the
daemon has finished, whether its DONE flag and artifacts validate --
by shelling out to the installed `node-monitor validate-run` command,
never re-implementing that check here.

  --output-root DIR   The Phase 0 config's own `output_root` (the
                       parent directory under which `phase0-<run_id>`
                       run directories are created). Only needed when
                       the config used something other than the
                       default `$HOME/phase0-runs` -- strict config
                       permits any `output_root` under `$HOME`
                       (node_monitor/config.py), so this script never
                       assumes the default path is the right one.
  --run-dir DIR        Validate this EXACT run directory directly,
                       bypassing "most recent under output_root"
                       discovery entirely. Mutually exclusive with
                       --output-root.
EOF
}

OUTPUT_ROOT=""
RUN_DIR_ARG=""

while [ $# -gt 0 ]; do
   case "$1" in
      --home) HOME_DIR="$2"; shift 2 ;;
      --venv) VENV="$2"; shift 2 ;;
      --lock-file) LOCK_FILE="$2"; shift 2 ;;
      --output-root) OUTPUT_ROOT="$2"; shift 2 ;;
      --run-dir) RUN_DIR_ARG="$2"; shift 2 ;;
      -h|--help) usage; exit 0 ;;
      *) echo "check_phase0.sh: unknown argument: $1" >&2; usage; exit 1 ;;
   esac
done

if [ -n "$OUTPUT_ROOT" ] && [ -n "$RUN_DIR_ARG" ]; then
   echo "check_phase0.sh: --output-root and --run-dir are mutually " \
        "exclusive" >&2
   exit 1
fi

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

# Same identity discipline as run_phase0.sh: liveness alone is never
# sufficient, and neither is a loose "looks like node-monitor/screen"
# substring match. The lock file must contain EXACTLY the two-field
# grammar "<pid> <session_name>" run_phase0.sh itself always writes
# (session_name shaped "node-monitor-phase0-<launcher-pid>"); anything
# else -- missing field, non-numeric pid, malformed session name -- is
# rejected outright rather than silently degrading into an
# empty-pattern match that would accept every live pid (a malformed
# one-field lock previously set lock_session="" here, and the shell
# pattern `*""*` matches ANY string). A well-formed lock's pid is only
# ever trusted once `kill -0` (existence probe, never a real signal)
# succeeds AND `ps -o command=` shows that EXACT recorded session name
# as the argument screen was detached-launched with -- never a PID
# matched only by a free-text process search or a generic
# node-monitor/screen substring.
_read_lock_fields() {
   LOCK_PID=""
   LOCK_SESSION=""
   local line field_count pid session suffix
   line="$(head -n 1 "$1" 2>/dev/null || true)"
   field_count="$(printf '%s\n' "$line" | awk '{print NF}')"
   if [ "$field_count" != "2" ]; then
      return 1
   fi
   pid="$(printf '%s\n' "$line" | awk '{print $1}')"
   session="$(printf '%s\n' "$line" | awk '{print $2}')"
   case "$pid" in
      ''|*[!0-9]*) return 1 ;;
   esac
   case "$session" in
      node-monitor-phase0-*)
         suffix="${session#node-monitor-phase0-}"
         case "$suffix" in
            ''|*[!0-9]*) return 1 ;;
         esac
         ;;
      *) return 1 ;;
   esac
   LOCK_PID="$pid"
   LOCK_SESSION="$session"
   return 0
}

_pid_matches_session() {
   local pid="$1" session="$2"
   if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then
      return 1
   fi
   local cmd
   cmd="$(ps -o command= -p "$pid" 2>/dev/null || true)"
   # Screen's own listed command line always carries the session name
   # as its own distinct, space-delimited argument (immediately after
   # the detached-launch flag) -- matching it as a WHOLE token here
   # (never a bare substring of a longer argument) is what makes this
   # an exact-identity check rather than the generic node-monitor/
   # screen substring match this replaces.
   case " $cmd " in
      *" $session "*)
         return 0
         ;;
      *)
         return 1
         ;;
   esac
}

if _read_lock_fields "$LOCK_FILE" && \
      _pid_matches_session "$LOCK_PID" "$LOCK_SESSION"; then
   echo "RUNNING: pid $LOCK_PID, screen session $LOCK_SESSION, lock $LOCK_FILE"
   exit 0
fi

echo "not currently running (no lock file, or its recorded pid/session " \
     "is malformed, stale, or no longer identifies a live Phase 0 daemon)"

# Locate the run directory to validate via the real, already-installed
# CLI -- never a second, locally re-implemented validator. Three ways
# to target it, most-specific first:
#   1. --run-dir: validate this exact directory, no discovery at all.
#   2. --output-root: search under an explicitly given output_root
#      (needed whenever the config's own output_root is not the
#      default $HOME/phase0-runs -- strict config permits ANY
#      output_root under $HOME, see node_monitor/config.py).
#   3. default: $HOME_DIR/phase0-runs, for deployments that kept the
#      default.
if [ -n "$RUN_DIR_ARG" ]; then
   latest_run_dir="${RUN_DIR_ARG%/}"
   if [ ! -d "$latest_run_dir" ]; then
      echo "check_phase0.sh: --run-dir does not exist: $latest_run_dir" >&2
      exit 1
   fi
else
   output_root="${OUTPUT_ROOT:-$HOME_DIR/phase0-runs}"
   if [ ! -d "$output_root" ]; then
      echo "check_phase0.sh: no output_root found at $output_root " \
           "(pass --output-root or --run-dir if the config used a " \
           "non-default output_root)" >&2
      exit 1
   fi

   latest_run_dir="$(ls -1dt "$output_root"/phase0-*/ 2>/dev/null | head -n 1 || true)"
   if [ -z "$latest_run_dir" ]; then
      echo "check_phase0.sh: no phase0-* run directory found under " \
           "$output_root" >&2
      exit 1
   fi
   latest_run_dir="${latest_run_dir%/}"
fi

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
