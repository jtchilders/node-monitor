#!/usr/bin/env bash
# deploy/run_phase0.sh -- launch the Phase 0 node-monitor daemon detached
# under /usr/bin/screen.
#
# Design: PHASE0_DAEMON_DESIGN.md "Run for 24 monotonic hours, detached
# on the host, with a durable completion flag." PLANNING.md 4.5 (XALT's
# LD_PRELOAD hazard). Implementation plan Task 8: "host storage (not
# /tmp), /usr/bin/screen detached launch, internal log redirection, DONE
# flag, unconditional unset LD_PRELOAD, no PostgreSQL command, explicit
# duration, and PID/process identity checks. ... refuse a duplicate
# matching daemon and never kill a PID found only by pgrep text."
#
# This script never starts, stops, or touches PostgreSQL/pbs_monitor in
# any way -- it only ever execs the already-installed node-monitor CLI
# (node_monitor.cli.main, "daemon smoke") inside a detached screen
# session. Idempotency uses a self-authored lock file recording this
# project's OWN daemon PID -- never a PID discovered by grepping `ps`/
# `pgrep` output for a text pattern -- and `kill -0` (an existence probe,
# never a real signal) to decide whether that PID is still alive AND
# still actually a node-monitor process before treating a lock as stale.
set -euo pipefail

# Unconditional: never guarded behind a conditional check. XALT's
# LD_PRELOAD is known to segfault an `exec postgres` (and, by the same
# hazard class, other execs) in this project's process tree.
unset LD_PRELOAD

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CONFIG=""
HOME_DIR="${HOME:-}"
VENV=""
DURATION_SEC=""
RUN_ID=""
LOCK_FILE=""
LOG_DIR=""

usage() {
   cat >&2 <<'EOF'
Usage: run_phase0.sh --config PATH --duration-sec SECONDS
                      [--home DIR] [--venv DIR] [--run-id ID]
                      [--lock-file PATH] [--log-dir DIR]

  --config PATH        Phase 0 strict YAML config (required).
  --duration-sec N      Explicit run duration in seconds (required).
                        Never falls back to the config file's own
                        duration_sec default -- every launch must state
                        how long THIS invocation runs.
  --home DIR            Deployment $HOME (default: $HOME). Must never
                        resolve under /tmp.
  --venv DIR            Path to the installed node-monitor venv
                        (default: $HOME/node-monitor/venv).
  --run-id ID           Explicit run id (default: a UTC timestamp).
  --lock-file PATH      Idempotency lock file (default:
                        $HOME/.node-monitor-phase0.lock).
  --log-dir DIR         Directory for the launched process's redirected
                        log (default: $HOME/node-monitor/logs).
EOF
}

while [ $# -gt 0 ]; do
   case "$1" in
      --config) CONFIG="$2"; shift 2 ;;
      --home) HOME_DIR="$2"; shift 2 ;;
      --venv) VENV="$2"; shift 2 ;;
      --duration-sec) DURATION_SEC="$2"; shift 2 ;;
      --run-id) RUN_ID="$2"; shift 2 ;;
      --lock-file) LOCK_FILE="$2"; shift 2 ;;
      --log-dir) LOG_DIR="$2"; shift 2 ;;
      -h|--help) usage; exit 0 ;;
      *) echo "run_phase0.sh: unknown argument: $1" >&2; usage; exit 1 ;;
   esac
done

if [ -z "$CONFIG" ]; then
   echo "run_phase0.sh: --config is required" >&2
   exit 1
fi
if [ ! -f "$CONFIG" ]; then
   echo "run_phase0.sh: config file not found: $CONFIG" >&2
   exit 1
fi
if [ -z "$DURATION_SEC" ]; then
   echo "run_phase0.sh: --duration-sec is required (explicit duration; " \
        "never falls back to a config default)" >&2
   exit 1
fi
case "$DURATION_SEC" in
   ''|*[!0-9]*)
      echo "run_phase0.sh: --duration-sec must be a positive integer, " \
           "got: $DURATION_SEC" >&2
      exit 1
      ;;
esac
if [ "$DURATION_SEC" -le 0 ]; then
   echo "run_phase0.sh: --duration-sec must be positive, got: $DURATION_SEC" >&2
   exit 1
fi

if [ -z "$HOME_DIR" ]; then
   echo "run_phase0.sh: --home could not be determined (no \$HOME set); " \
        "pass --home explicitly" >&2
   exit 1
fi

# Card: "deployment under \$HOME, never /tmp." Resolve to an absolute,
# symlink-free path so a sneaky relative component (e.g. "../../tmp")
# cannot slip past a naive prefix check.
HOME_DIR="$(cd "$HOME_DIR" 2>/dev/null && pwd -P || echo "$HOME_DIR")"
case "$HOME_DIR" in
   /tmp|/tmp/*|/private/tmp|/private/tmp/*)
      echo "run_phase0.sh: --home must not resolve under /tmp, got: $HOME_DIR" >&2
      exit 1
      ;;
esac

VENV="${VENV:-$HOME_DIR/node-monitor/venv}"
NODE_MONITOR_BIN="$VENV/bin/node-monitor"
if [ ! -x "$NODE_MONITOR_BIN" ]; then
   echo "run_phase0.sh: node-monitor executable not found or not " \
        "executable: $NODE_MONITOR_BIN" >&2
   exit 1
fi

LOCK_FILE="${LOCK_FILE:-$HOME_DIR/.node-monitor-phase0.lock}"
LOG_DIR="${LOG_DIR:-$HOME_DIR/node-monitor/logs}"
case "$LOG_DIR" in
   /tmp|/tmp/*|/private/tmp|/private/tmp/*)
      echo "run_phase0.sh: --log-dir must not resolve under /tmp, got: $LOG_DIR" >&2
      exit 1
      ;;
esac
mkdir -p "$LOG_DIR"

if [ -z "$RUN_ID" ]; then
   RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
fi

SCREEN_BIN="/usr/bin/screen"
if [ ! -x "$SCREEN_BIN" ]; then
   echo "run_phase0.sh: $SCREEN_BIN not found or not executable" >&2
   exit 1
fi

# Unique per invocation (this script's own PID, never reused while this
# process is alive) so the post-launch `screen -ls` PID-discovery lookup
# below can never match a DIFFERENT, unrelated invocation's session that
# happens to still be listed (e.g. a still-running earlier launch under
# a different lock file, or one whose screen entry has not yet been
# reaped) -- a name collision there would silently write the WRONG pid
# into THIS invocation's lock file.
SESSION_NAME="node-monitor-phase0-$$"
LOG_FILE="$LOG_DIR/phase0-$RUN_ID.log"

# --------------------------------------------------------------------
# Idempotent duplicate refusal.
#
# The lock file records EXACTLY one line, two whitespace-separated
# fields: "<pid> <session_name>", where <session_name> always has the
# shape "node-monitor-phase0-<launcher-pid>" this script itself always
# writes. `_read_lock_fields` refuses to trust anything that does not
# match that exact grammar (missing second field, non-numeric pid,
# wrong session-name shape) -- a malformed or foreign lock file is
# always treated as NOT a live daemon, never partially parsed.
#
# A well-formed lock is only ever trusted as "still running" when ALL
# of the following hold -- never PID liveness alone, which is exactly
# the pgrep-text-match failure mode this card explicitly forbids, and
# never a generic "looks like node-monitor/screen" match, which would
# accept an unrelated screen session or a recycled PID:
#
#   1. `kill -0 "$pid"` succeeds (the PID exists and is ours to signal
#      -- a harmless existence probe, never a real signal).
#   2. `ps -o command= -p "$pid"` shows THIS EXACT recorded session
#      name as the `-dmS` argument screen was launched with -- not a
#      substring match against a different invocation's own current
#      $SESSION_NAME (which can never identify a PRIOR session) and
#      not a loose `*node-monitor*`/`*screen*` match.
# --------------------------------------------------------------------
_read_lock_fields() {
   # On success, sets LOCK_PID/LOCK_SESSION and returns 0. On any
   # malformed content, clears both and returns 1 -- the caller must
   # never treat a partially-parsed field as trustworthy.
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
   if ! kill -0 "$pid" 2>/dev/null; then
      return 1
   fi
   local cmd
   cmd="$(ps -o command= -p "$pid" 2>/dev/null || true)"
   case "$cmd" in
      *"-dmS $session "*|*"-dmS $session")
         return 0
         ;;
      *)
         return 1
         ;;
   esac
}

# --------------------------------------------------------------------
# Atomic launch-lock acquisition (`mkdir` is POSIX-atomic: exactly one
# concurrent caller can ever create a given directory). This closes the
# TOCTOU window between "check no live lock" and "write our own claim"
# that previously let two invocations launched close together both
# observe no lock and both launch a duplicate daemon. No dependency on
# `flock`/`lockfile`, which are not guaranteed present on Polaris.
# --------------------------------------------------------------------
LOCK_CLAIM_DIR="$LOCK_FILE.claim"

_release_claim() {
   rm -rf "$LOCK_CLAIM_DIR" 2>/dev/null || true
}

_claim_lock() {
   local attempt claimer_pid
   for attempt in $(seq 1 100); do
      if mkdir "$LOCK_CLAIM_DIR" 2>/dev/null; then
         echo "$$" > "$LOCK_CLAIM_DIR/pid"
         return 0
      fi
      # The claim dir already exists: either a concurrent invocation is
      # actively racing us right now (expected, brief -- keep retrying),
      # or a PREVIOUS invocation crashed while holding it and left it
      # behind permanently. Only ever break the second case, and only
      # by checking the actual claimer's own recorded pid via `kill -0`
      # (an existence probe) -- never by directory age/mtime alone.
      claimer_pid="$(cat "$LOCK_CLAIM_DIR/pid" 2>/dev/null || true)"
      case "$claimer_pid" in
         ''|*[!0-9]*) claimer_pid="" ;;
      esac
      if [ -n "$claimer_pid" ] && ! kill -0 "$claimer_pid" 2>/dev/null; then
         rm -rf "$LOCK_CLAIM_DIR" 2>/dev/null || true
         continue
      fi
      sleep 0.1
   done
   return 1
}

if ! _claim_lock; then
   echo "run_phase0.sh: could not acquire the launch lock (contended by " \
        "another invocation); claim dir: $LOCK_CLAIM_DIR" >&2
   exit 1
fi
# Release the claim on every exit path (success, refusal, or error) --
# this invocation's own foreground process is short-lived (it returns
# once the launched screen session's pid is confirmed below), so the
# claim is held only for that brief window, never for the daemon's
# full run.
trap _release_claim EXIT

if [ -f "$LOCK_FILE" ]; then
   if _read_lock_fields "$LOCK_FILE" && \
         _pid_matches_session "$LOCK_PID" "$LOCK_SESSION"; then
      echo "run_phase0.sh: a Phase 0 daemon is already running " \
           "(pid $LOCK_PID, session $LOCK_SESSION, lock file $LOCK_FILE)" >&2
      exit 1
   fi
   echo "run_phase0.sh: stale or malformed lock file found; " \
        "reclaiming $LOCK_FILE" >&2
   rm -f "$LOCK_FILE"
fi

# Placeholder claim written BEFORE launch so a concurrent invocation
# racing this one sees a lock file immediately; overwritten with the
# real screen PID right after launch below.
echo "REPLACED_BELOW $SESSION_NAME" > "$LOCK_FILE"

# The launched command itself unconditionally strips LD_PRELOAD a
# second time, independently of the outer script's own environment --
# a module load inside the screen session's shell could reintroduce it
# on its own account. All output is redirected to $LOG_FILE by THIS
# script, not by screen's own -L/-Logfile flag.
INNER_CMD="unset LD_PRELOAD; exec \"$NODE_MONITOR_BIN\" daemon smoke \
--config \"$CONFIG\" --home \"$HOME_DIR\" --run-id \"$RUN_ID\" \
--duration-sec \"$DURATION_SEC\" >>\"$LOG_FILE\" 2>&1"

"$SCREEN_BIN" -dmS "$SESSION_NAME" bash -c "$INNER_CMD"

# Poll briefly for screen to register the session and report its PID --
# `screen -ls` includes "<pid>.<session_name>" once the session exists.
# This is the ONE place this script reads screen's own listing (never
# `pgrep`/`ps` free-text search) purely to learn the number screen
# itself assigned; the actual liveness/identity re-check on a FUTURE
# invocation is still done via `kill -0` + `ps -o command=` above, not
# by re-parsing `screen -ls` again.
screen_pid=""
for _ in $(seq 1 50); do
   screen_pid="$("$SCREEN_BIN" -ls 2>/dev/null \
      | awk -v s="$SESSION_NAME" '$1 ~ ("\\." s "$") { split($1, a, "."); print a[1]; exit }' \
      || true)"
   if [ -n "$screen_pid" ]; then
      break
   fi
   sleep 0.1
done

if [ -z "$screen_pid" ]; then
   echo "run_phase0.sh: launched screen session but could not confirm " \
        "its pid" >&2
   rm -f "$LOCK_FILE"
   exit 1
fi

echo "$screen_pid $SESSION_NAME" > "$LOCK_FILE"

echo "run_phase0.sh: launched Phase 0 daemon"
echo "  run_id:    $RUN_ID"
echo "  config:    $CONFIG"
echo "  home:      $HOME_DIR"
echo "  duration:  ${DURATION_SEC}s"
echo "  log_file:  $LOG_FILE"
echo "  lock_file: $LOCK_FILE"
echo "  screen:    $SESSION_NAME (pid $screen_pid)"
exit 0
