"""Tests for deploy/run_phase0.sh and deploy/check_phase0.sh.

Design: PHASE0_DAEMON_IMPLEMENTATION_PLAN.md Task 8 ("Write failing
static/subprocess tests asserting host storage (not /tmp), /usr/bin/screen
detached launch, internal log redirection, DONE flag, unconditional unset
LD_PRELOAD, no PostgreSQL command, explicit duration, and PID/process
identity checks"). PHASE0_DAEMON_DESIGN.md: "Run for 24 monotonic hours,
detached on the host, with a durable completion flag" and "Phase 0 never
connects to, starts, stops, or modifies PostgreSQL."

Two kinds of coverage:

* Static (grep/AST-free string) checks directly on the script SOURCE --
  these encode invariants that must hold no matter what a future edit
  does (no PostgreSQL command ever appears, no `pgrep`-derived kill,
  LD_PRELOAD is unconditionally stripped) and are cheap/fast.
* Real subprocess checks that actually launch `run_phase0.sh`/
  `check_phase0.sh` against a FAKE `node-monitor` executable (a small,
  fast, real bash script standing in for the installed CLI) using the
  REAL `/usr/bin/screen` on this machine -- proving the scripts' actual
  argv construction, log redirection, lock-file/idempotency, and
  discovery logic work end to end, not just that the right substrings
  appear in the source.

Every subprocess test uses a short (1-2 second) fake daemon and a
`--home`/`--lock-file` under `tmp_path` (a real macOS temp directory,
which is NOT literally under `/tmp` -- see tests/test_config.py's own
precedent for using `tmp_path` as an injectable "home" without tripping
the "never under /tmp" rule) so the whole module runs in real wall-clock
seconds.
"""

import os
import re
import stat
import subprocess
import sys
import time

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUN_SCRIPT = os.path.join(REPO_ROOT, "deploy", "run_phase0.sh")
CHECK_SCRIPT = os.path.join(REPO_ROOT, "deploy", "check_phase0.sh")

_SCREEN_BIN = "/usr/bin/screen"


def _read(path):
   with open(path, "r") as handle:
      return handle.read()


def _code_only(path):
   """Return `path`'s source with pure-comment lines removed.

   These scripts' own docstring-style header comments freely discuss
   the very things this module asserts are absent from the CODE (e.g.
   explaining why the script never uses `pgrep`, or never touches
   PostgreSQL) -- scanning raw source text would make the test trip
   over its own documentation. This keeps every line that contains
   actual shell code (including a trailing inline comment on a code
   line, which is rare in this project's style but harmless to keep)
   and drops only lines that are ENTIRELY a comment.
   """
   lines = []
   for line in _read(path).splitlines():
      if line.strip().startswith("#"):
         continue
      lines.append(line)
   return "\n".join(lines)


def _skip_unless_screen_available():
   if not os.path.exists(_SCREEN_BIN) or not os.access(_SCREEN_BIN, os.X_OK):
      pytest.skip("/usr/bin/screen is not available on this machine")


# --------------------------------------------------------------------------
# Static source checks -- both scripts
# --------------------------------------------------------------------------

class TestScriptsExistAndAreExecutable:
   def test_run_phase0_exists_and_is_executable(self):
      assert os.path.isfile(RUN_SCRIPT)
      mode = os.stat(RUN_SCRIPT).st_mode
      assert mode & stat.S_IXUSR

   def test_check_phase0_exists_and_is_executable(self):
      assert os.path.isfile(CHECK_SCRIPT)
      mode = os.stat(CHECK_SCRIPT).st_mode
      assert mode & stat.S_IXUSR

   def test_run_phase0_bash_syntax_is_valid(self):
      result = subprocess.run(
         ["bash", "-n", RUN_SCRIPT], capture_output=True, text=True)
      assert result.returncode == 0, result.stderr

   def test_check_phase0_bash_syntax_is_valid(self):
      result = subprocess.run(
         ["bash", "-n", CHECK_SCRIPT], capture_output=True, text=True)
      assert result.returncode == 0, result.stderr


class TestNoPostgresOrPbsMonitor:
   """Design: "Phase 0 never connects to, starts, stops, or modifies
   PostgreSQL." Card: "no PostgreSQL/pbs_monitor commands or interaction."
   """

   _FORBIDDEN_PATTERNS = (
      r"\bpg_ctl\b", r"\bpg_ctlcluster\b", r"\bpsql\b", r"\bpostgres\b",
      r"\bpostmaster\b", r"\binitdb\b", r"\bpg_dump\b", r"\bpg_resetwal\b",
      r"\bpbs_monitor\b", r"\bpbs-monitor\b",
   )

   @pytest.mark.parametrize("script_path", [RUN_SCRIPT, CHECK_SCRIPT])
   def test_no_forbidden_database_command_in_source(self, script_path):
      source = _code_only(script_path).lower()
      for pattern in self._FORBIDDEN_PATTERNS:
         assert not re.search(pattern, source, re.IGNORECASE), (
            "%s contains forbidden pattern %r" % (script_path, pattern))


class TestUnconditionalLdPreloadUnset:
   """Card: "unconditional unset LD_PRELOAD locally and in launched
   process." Design/PLANNING.md 4.5: XALT's LD_PRELOAD segfaults this
   project's process tree.
   """

   def test_run_phase0_unsets_ld_preload_unconditionally(self):
      source = _read(RUN_SCRIPT)
      # "unconditional" -- never guarded behind an `if [ -n "$LD_PRELOAD" ]`
      # style conditional; must appear as a bare statement.
      assert re.search(r"^\s*unset LD_PRELOAD\b", source, re.MULTILINE)

   def test_run_phase0_unsets_ld_preload_inside_launched_process_too(self):
      """The launched (screen-detached) process must ALSO strip
      LD_PRELOAD from its own environment, not just the outer script's
      -- a module load inside the screen session could reintroduce it
      independently of what the outer script's own env holds.
      """
      source = _read(RUN_SCRIPT)
      occurrences = re.findall(r"unset LD_PRELOAD\b", source)
      assert len(occurrences) >= 2, (
         "expected unset LD_PRELOAD both in the outer script and inside "
         "the launched/inner command, found %d occurrence(s)"
         % len(occurrences))


class TestNoPgrepBasedKill:
   """Card: "never killing a PID obtained only from pgrep text." The
   simplest structural guarantee: neither script ever calls `pgrep` at
   all -- all liveness/identity checks go through a self-authored lock
   file plus `kill -0`/`ps -o command=` against a PID this project's own
   prior invocation recorded, never a PID discovered by grepping `ps`/
   `pgrep` output for a text pattern.
   """

   @pytest.mark.parametrize("script_path", [RUN_SCRIPT, CHECK_SCRIPT])
   def test_no_pgrep_usage(self, script_path):
      source = _code_only(script_path)
      assert "pgrep" not in source

   @pytest.mark.parametrize("script_path", [RUN_SCRIPT, CHECK_SCRIPT])
   def test_no_kill_signal_stronger_than_probe(self, script_path):
      """Neither script ever sends a real signal to a process -- only
      the harmless existence probe `kill -0`. `run_phase0.sh` refuses to
      launch a duplicate rather than killing anything; `check_phase0.sh`
      is read-only.
      """
      source = _code_only(script_path)
      kill_calls = re.findall(r"\bkill\b[^\n;|&]*", source)
      assert kill_calls, "expected at least one kill -0 call in %s" % script_path
      for call in kill_calls:
         assert re.search(r"-0\b", call), (
            "found a kill call that is not a -0 existence probe: %r" % call)


class TestHostStorageNeverTmp:
   """Card: "deployment under $HOME, never /tmp."""

   @pytest.mark.parametrize("script_path", [RUN_SCRIPT, CHECK_SCRIPT])
   def test_source_guards_against_tmp(self, script_path):
      source = _read(script_path)
      assert "/tmp" in source, (
         "%s has no /tmp guard at all" % script_path)

   def test_run_phase0_rejects_home_under_tmp(self, tmp_path):
      _skip_unless_screen_available()
      config_path = str(tmp_path / "config.yaml")
      with open(config_path, "w") as handle:
         handle.write("system: polaris\n")
      result = subprocess.run(
         [RUN_SCRIPT, "--config", config_path, "--duration-sec", "1",
          "--home", "/tmp/should-be-rejected"],
         capture_output=True, text=True, timeout=30)
      assert result.returncode != 0
      assert "/tmp" in result.stderr


class TestScreenDetachedLaunch:
   """Card: "detached /usr/bin/screen launch."""

   @pytest.mark.parametrize("script_path", [RUN_SCRIPT])
   def test_source_launches_via_usr_bin_screen_detached(self, script_path):
      source = _read(script_path)
      assert "/usr/bin/screen" in source
      # The literal path is assigned to a variable and invoked with
      # -dmS (detached, named session) through that variable -- not
      # necessarily on the same source line as the literal path itself.
      assert re.search(r"SCREEN_BIN=[\"\']?/usr/bin/screen", source)
      assert re.search(r"\$\{?SCREEN_BIN\}?[\"\']?\s+-dmS\b", source)

   def test_check_phase0_never_launches_screen(self):
      source = _read(CHECK_SCRIPT)
      assert "-dmS" not in source


class TestLogRedirectionInsideHostScript:
   """Card: "log redirection inside the host script" -- i.e. the launch
   script itself redirects the launched process's stdout/stderr to a
   log file (`>>"$LOG_FILE" 2>&1`-style), rather than depending on an
   external terminal/screen `-L` logging flag.
   """

   def test_run_phase0_redirects_output_to_a_log_file_itself(self):
      source = _code_only(RUN_SCRIPT)
      assert re.search(r">>\\?[\"\']?\$\{?LOG_FILE\}?\\?[\"\']?\s+2>&1", source)
      # Not relying on screen's own `-L`/`-Logfile` flag for this.
      assert " -L " not in source
      assert "-Logfile" not in source


class TestExplicitDuration:
   """Card: "explicit duration" -- the deploy script never silently
   falls back to the daemon's own config-default duration_sec; the
   operator must always state how long this specific launch runs.
   """

   def test_missing_duration_sec_is_rejected(self, tmp_path):
      config_path = str(tmp_path / "config.yaml")
      with open(config_path, "w") as handle:
         handle.write("system: polaris\n")
      result = subprocess.run(
         [RUN_SCRIPT, "--config", config_path, "--home", str(tmp_path)],
         capture_output=True, text=True, timeout=30)
      assert result.returncode != 0
      assert "duration" in result.stderr.lower()

   def test_nonnumeric_duration_sec_is_rejected(self, tmp_path):
      config_path = str(tmp_path / "config.yaml")
      with open(config_path, "w") as handle:
         handle.write("system: polaris\n")
      result = subprocess.run(
         [RUN_SCRIPT, "--config", config_path, "--home", str(tmp_path),
          "--duration-sec", "not-a-number"],
         capture_output=True, text=True, timeout=30)
      assert result.returncode != 0


class TestMissingConfigRejected:
   def test_missing_config_flag_is_rejected(self, tmp_path):
      result = subprocess.run(
         [RUN_SCRIPT, "--home", str(tmp_path), "--duration-sec", "1"],
         capture_output=True, text=True, timeout=30)
      assert result.returncode != 0
      assert "config" in result.stderr.lower()


# --------------------------------------------------------------------------
# Real end-to-end subprocess tests against a fake node-monitor CLI, using
# the real /usr/bin/screen on this machine.
# --------------------------------------------------------------------------

_FAKE_NODE_MONITOR = '''#!/usr/bin/env bash
# Fake stand-in for the installed node-monitor console script, used only
# by tests/test_deploy_scripts.py. Mimics just enough of the real CLI's
# observable contract (status lines on stdout including "run_dir: ...",
# a run directory with DONE + summary.json on clean completion, and a
# validate-run subcommand) for the deploy scripts to be exercised for
# real without needing the full Python package or a real 24h run.
set -euo pipefail

if [ "$1" = "validate-run" ]; then
   run_dir="$2"
   if [ -f "$run_dir/DONE" ]; then
      echo "valid: run directory passed all structural checks"
      exit 0
   fi
   echo "INVALID: DONE flag is missing" >&2
   exit 1
fi

# "daemon" "smoke" --config C --home H --run-id R --duration-sec D
config=""
home=""
run_id=""
duration=""
while [ $# -gt 0 ]; do
   case "$1" in
      --config) config="$2"; shift 2 ;;
      --home) home="$2"; shift 2 ;;
      --run-id) run_id="$2"; shift 2 ;;
      --duration-sec) duration="$2"; shift 2 ;;
      *) shift ;;
   esac
done

output_root="$home/phase0-runs"
run_dir="$output_root/phase0-$run_id"
mkdir -p "$run_dir"

echo "config: $config"
echo "system: fakesystem"
echo "output_root: $output_root"
echo "run_dir: $run_dir"
echo "nodes: fake-local.example.org"
echo "duration_sec: $duration"

sleep "$duration"

echo '{"run_id": "'"$run_id"'"}' > "$run_dir/summary.json"
date -u +%Y-%m-%dT%H:%M:%SZ > "$run_dir/DONE"

echo "run complete: $run_dir"
exit 0
'''


@pytest.fixture
def fake_venv(tmp_path):
   venv_bin = tmp_path / "venv" / "bin"
   venv_bin.mkdir(parents=True)
   node_monitor = venv_bin / "node-monitor"
   node_monitor.write_text(_FAKE_NODE_MONITOR)
   os.chmod(str(node_monitor), 0o755)
   return str(tmp_path / "venv")


@pytest.fixture
def home_dir(tmp_path):
   home = tmp_path / "home"
   home.mkdir()
   return str(home)


def _wait_for(predicate, timeout=10.0, interval=0.1):
   deadline = time.monotonic() + timeout
   while time.monotonic() < deadline:
      if predicate():
         return True
      time.sleep(interval)
   return predicate()


def _run_launch(config_path, home, venv, duration_sec, run_id, lock_file,
                 log_dir):
   return subprocess.run(
      [RUN_SCRIPT,
       "--config", config_path,
       "--home", home,
       "--venv", venv,
       "--duration-sec", str(duration_sec),
       "--run-id", run_id,
       "--lock-file", lock_file,
       "--log-dir", log_dir],
      capture_output=True, text=True, timeout=30)


class TestRealLaunchAndIdempotency:
   def test_successful_launch_writes_lock_and_log(
         self, tmp_path, fake_venv, home_dir):
      _skip_unless_screen_available()
      config_path = str(tmp_path / "config.yaml")
      with open(config_path, "w") as handle:
         handle.write("system: polaris\n")
      lock_file = str(tmp_path / "phase0.lock")
      log_dir = str(tmp_path / "logs")

      result = _run_launch(
         config_path, home_dir, fake_venv, duration_sec=2,
         run_id="launch-1", lock_file=lock_file, log_dir=log_dir)

      assert result.returncode == 0, result.stderr
      assert os.path.exists(lock_file)

      log_path = os.path.join(log_dir, "phase0-launch-1.log")
      assert _wait_for(lambda: os.path.exists(log_path))

      run_dir = os.path.join(home_dir, "phase0-runs", "phase0-launch-1")
      assert _wait_for(
         lambda: os.path.exists(os.path.join(run_dir, "DONE")),
         timeout=15)

      log_contents = _read(log_path)
      assert ("run_dir: %s" % run_dir) in log_contents

   def test_duplicate_launch_is_refused_while_first_still_running(
         self, tmp_path, fake_venv, home_dir):
      _skip_unless_screen_available()
      config_path = str(tmp_path / "config.yaml")
      with open(config_path, "w") as handle:
         handle.write("system: polaris\n")
      lock_file = str(tmp_path / "phase0.lock")
      log_dir = str(tmp_path / "logs")

      first = _run_launch(
         config_path, home_dir, fake_venv, duration_sec=5,
         run_id="dup-1", lock_file=lock_file, log_dir=log_dir)
      assert first.returncode == 0, first.stderr

      # Give the inner screen session a moment to write its own PID into
      # the lock file (it overwrites the outer script's placeholder).
      assert _wait_for(lambda: "REPLACED_BELOW" not in _read(lock_file))

      second = _run_launch(
         config_path, home_dir, fake_venv, duration_sec=5,
         run_id="dup-2", lock_file=lock_file, log_dir=log_dir)

      assert second.returncode != 0
      assert "already running" in second.stderr.lower()
      # The duplicate must never have been allowed to create its own
      # run directory.
      assert not os.path.exists(
         os.path.join(home_dir, "phase0-runs", "phase0-dup-2"))

   def test_stale_lock_is_reclaimed_after_daemon_exits(
         self, tmp_path, fake_venv, home_dir):
      _skip_unless_screen_available()
      config_path = str(tmp_path / "config.yaml")
      with open(config_path, "w") as handle:
         handle.write("system: polaris\n")
      lock_file = str(tmp_path / "phase0.lock")
      log_dir = str(tmp_path / "logs")

      first = _run_launch(
         config_path, home_dir, fake_venv, duration_sec=1,
         run_id="stale-1", lock_file=lock_file, log_dir=log_dir)
      assert first.returncode == 0, first.stderr

      run_dir = os.path.join(home_dir, "phase0-runs", "phase0-stale-1")
      assert _wait_for(
         lambda: os.path.exists(os.path.join(run_dir, "DONE")), timeout=15)
      # Give the now-exited process a moment to be fully reaped.
      time.sleep(1.0)

      second = _run_launch(
         config_path, home_dir, fake_venv, duration_sec=1,
         run_id="stale-2", lock_file=lock_file, log_dir=log_dir)
      assert second.returncode == 0, second.stderr


class TestDoneFlagNeverFakedByDeployScripts:
   """Card: "DONE semantics consistent with the daemon (do not fake/
   overwrite daemon DONE)." Neither script writes a file literally named
   DONE anywhere on its own account -- DONE is exclusively the daemon's
   own ``Phase0Sink.write_done()`` output.
   """

   @pytest.mark.parametrize("script_path", [RUN_SCRIPT, CHECK_SCRIPT])
   def test_scripts_never_write_a_file_named_done(self, script_path):
      source = _read(script_path)
      assert not re.search(r">\s*[\"\']?\$?\{?[\w/\"\'.$]*\bDONE\b", source), (
         "%s appears to write a file named DONE itself" % script_path)

   def test_real_launch_never_creates_done_before_fake_daemon_does(
         self, tmp_path, fake_venv, home_dir):
      """Regression guard for the real subprocess path: DONE only ever
      appears because the fake node-monitor (standing in for the real
      daemon) wrote it, and only after its own sleep -- never written
      up front by run_phase0.sh itself.
      """
      _skip_unless_screen_available()
      config_path = str(tmp_path / "config.yaml")
      with open(config_path, "w") as handle:
         handle.write("system: polaris\n")
      lock_file = str(tmp_path / "phase0.lock")
      log_dir = str(tmp_path / "logs")

      result = _run_launch(
         config_path, home_dir, fake_venv, duration_sec=3,
         run_id="done-order-1", lock_file=lock_file, log_dir=log_dir)
      assert result.returncode == 0, result.stderr

      run_dir = os.path.join(home_dir, "phase0-runs", "phase0-done-order-1")
      # Immediately after the (fast, backgrounded) launch returns, the
      # fake daemon's sleep has not elapsed yet, so DONE must not exist.
      assert not os.path.exists(os.path.join(run_dir, "DONE"))

      assert _wait_for(
         lambda: os.path.exists(os.path.join(run_dir, "DONE")), timeout=15)


# --------------------------------------------------------------------------
# check_phase0.sh
# --------------------------------------------------------------------------

class TestCheckPhase0ReadOnly:
   def test_check_phase0_reports_no_lock_file(self, tmp_path, home_dir):
      lock_file = str(tmp_path / "does-not-exist.lock")
      result = subprocess.run(
         [CHECK_SCRIPT, "--home", home_dir, "--lock-file", lock_file],
         capture_output=True, text=True, timeout=30)
      assert result.returncode != 0
      assert "no lock file" in result.stdout.lower()

   def test_check_phase0_reports_running_then_done_and_valid(
         self, tmp_path, fake_venv, home_dir):
      _skip_unless_screen_available()
      config_path = str(tmp_path / "config.yaml")
      with open(config_path, "w") as handle:
         handle.write("system: polaris\n")
      lock_file = str(tmp_path / "phase0.lock")
      log_dir = str(tmp_path / "logs")

      launch = _run_launch(
         config_path, home_dir, fake_venv, duration_sec=2,
         run_id="check-1", lock_file=lock_file, log_dir=log_dir)
      assert launch.returncode == 0, launch.stderr

      assert _wait_for(lambda: "REPLACED_BELOW" not in _read(lock_file))

      running_check = subprocess.run(
         [CHECK_SCRIPT, "--home", home_dir, "--lock-file", lock_file,
          "--venv", fake_venv],
         capture_output=True, text=True, timeout=30)
      assert running_check.returncode == 0, running_check.stderr
      assert "RUNNING" in running_check.stdout

      run_dir = os.path.join(home_dir, "phase0-runs", "phase0-check-1")
      assert _wait_for(
         lambda: os.path.exists(os.path.join(run_dir, "DONE")), timeout=15)
      # Let the fake daemon process fully exit so the lock's recorded
      # PID is genuinely gone by the time we check again.
      time.sleep(1.0)

      done_check = subprocess.run(
         [CHECK_SCRIPT, "--home", home_dir, "--lock-file", lock_file,
          "--venv", fake_venv],
         capture_output=True, text=True, timeout=30)
      assert done_check.returncode == 0, done_check.stderr
      assert "DONE present: true" in done_check.stdout
      assert "valid: run directory passed all structural checks" \
         in done_check.stdout

   def test_check_phase0_never_touches_postgres_source(self):
      source = _code_only(CHECK_SCRIPT)
      for token in ("pg_ctl", "psql", "postgres", "pbs_monitor"):
         assert token not in source.lower()
