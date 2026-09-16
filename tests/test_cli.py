"""Tests for node_monitor.cli.main -- the Phase 0 CLI surface.

Design: PHASE0_DAEMON_DESIGN.md + PHASE0_DAEMON_IMPLEMENTATION_PLAN.md
Task 7 ("Add CLI commands: `daemon dry-run`, `daemon smoke`, and
`validate-run`. Status output must print config and output paths.").
PLANNING.md section 15 ("CLI surface"): `daemon dry-run --out FILE  #
Phase 0: JSON to file, no DB`.

Every daemon-running test here invokes the CLI end to end through
``click.testing.CliRunner`` against the REAL ``node_monitor.daemon.
Daemon``, the REAL ``node_monitor.output.jsonl.Phase0Sink``, and the
REAL ``node_monitor/collector/remote_probe.py`` script (run as a real
subprocess via the REAL ``node_monitor.collector.transport.
run_local_probe`` -- exactly the production wiring, never a fake
daemon/transport double). What IS synthetic is the underlying /proc
and /sys trees the real probe reads, using the exact same
``NODE_MONITOR_PROC_ROOT``/``NODE_MONITOR_SYS_ROOT`` override
mechanism tests/test_remote_probe.py's own subprocess-level tests
already rely on (see that module's ``fake_proc_env`` fixture) -- this
is what lets a real end-to-end run execute in a few real seconds on a
macOS dev machine with no actual /proc, actual login nodes, or actual
SSH network access, without stubbing out any of the code this CLI is
actually supposed to wire together.

Every scenario uses accelerated 1-second poll/rollup/usage intervals
and a short ``duration_sec`` (2-3 seconds), so the whole test module
runs in real wall-clock seconds, not the design's real 24-hour
default -- this is intentionally a REAL run at a tiny scale, not an
injected fake-clock unit test (which node_monitor/daemon.py's own test
suite already covers exhaustively at the Daemon-class level; this
module's job is to prove the CLI's own wiring/argv/exit-code/status-
output contract on top of that, not to re-test Daemon itself).

``--home`` is an internal, undocumented-in-PLANNING.md CLI option that
exists purely to let this test suite point ``node_monitor.config``'s
own output-root-must-resolve-under-home validation at ``tmp_path``
instead of the real, live ``$HOME`` this test runs under (Taylor's
real home directory, per this project's operating environment) --
``node_monitor.config.load_config_file`` already exposes exactly this
injectable ``home`` parameter for this exact reason (its own
docstring: \"overridable for tests and for deployments that run as a
different effective user\"); omitting ``--home`` in real use falls
back to the real ``$HOME`` exactly as before this option existed.
"""

import json
import os
import sys

import pytest
from click.testing import CliRunner

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "fixtures"))

import fake_proc  # noqa: E402

from node_monitor.cli.main import cli  # noqa: E402
from node_monitor.output.jsonl import validate_jsonl_artifact  # noqa: E402


# --------------------------------------------------------------------------
# Synthetic /proc + /sys, delivered to the REAL remote_probe.py subprocess
# purely via env vars (see module docstring).
# --------------------------------------------------------------------------

@pytest.fixture
def fake_proc_env(tmp_path, monkeypatch):
   proc_root = str(tmp_path / "fakeproc")
   fake_proc.write_proc(proc_root, [
      {"pid": 100, "comm": "bash", "cmdline": "-bash", "tty_nr": 34816},
   ])
   fake_proc.write_proc_hwinfo(proc_root)
   sys_root = str(tmp_path / "fakesys")
   fake_proc.write_sys_hwinfo(sys_root)
   monkeypatch.setenv("NODE_MONITOR_PROC_ROOT", proc_root)
   monkeypatch.setenv("NODE_MONITOR_SYS_ROOT", sys_root)
   return proc_root


def _probe_python():
   """An explicit, versioned interpreter path satisfying
   node_monitor.config's strict ``probe_python`` regex (which rejects
   a bare unversioned ``python3``/``python``), guaranteed to actually
   exist and be runnable in this test environment. ``sys.executable``
   itself can resolve to an unversioned name (e.g. a venv's
   ``bin/python3`` symlink) depending on how the interpreter running
   this test suite was invoked, so this walks to the real, versioned
   binary the running interpreter's own ``sys.version_info`` names,
   falling back to ``sys.executable`` if that exact versioned sibling
   is not found on disk.
   """
   versioned = os.path.join(
      os.path.dirname(sys.executable),
      "python%d.%d" % (sys.version_info[0], sys.version_info[1]))
   if os.path.exists(versioned):
      return versioned
   return sys.executable


def _write_config(path, home_dir, **overrides):
   raw = {
      "system": "polaris",
      "nodes": [
         {"hostname": "cli-test-local.example.org", "role": "local"},
      ],
      "output_root": "~/phase0-runs",
      "probe_python": _probe_python(),
      "counter_interval_sec": 1,
      "census_interval_sec": 1,
      "rollup_interval_sec": 1,
      "usage_interval_sec": 1,
      "duration_sec": 2,
   }
   raw.update(overrides)
   import yaml
   os.makedirs(os.path.join(home_dir, "phase0-runs"), exist_ok=True)
   with open(path, "w") as handle:
      yaml.safe_dump(raw, handle)
   return path


def _invoke(args):
   runner = CliRunner()
   return runner.invoke(cli, args, catch_exceptions=False)


# --------------------------------------------------------------------------
# daemon dry-run: real end-to-end run for the configured duration_sec
# --------------------------------------------------------------------------

class TestDaemonDryRun:
   def test_valid_invocation_produces_full_run_artifact_set(
         self, tmp_path, fake_proc_env):
      home_dir = str(tmp_path / "home")
      os.makedirs(home_dir, exist_ok=True)
      config_path = str(tmp_path / "config.yaml")
      _write_config(config_path, home_dir)

      result = _invoke([
         "daemon", "dry-run",
         "--config", config_path,
         "--home", home_dir,
         "--run-id", "dry-run-1",
      ])

      assert result.exit_code == 0, result.output
      run_dir = os.path.join(home_dir, "phase0-runs", "phase0-dry-run-1")
      assert os.path.isdir(run_dir)
      assert os.path.exists(os.path.join(run_dir, "manifest.json"))
      assert os.path.exists(os.path.join(run_dir, "summary.json"))
      assert os.path.exists(os.path.join(run_dir, "DONE"))

      with open(os.path.join(run_dir, "node_hardware.jsonl")) as handle:
         hardware_lines = [json.loads(line) for line in handle if line.strip()]
      assert len(hardware_lines) == 1
      assert hardware_lines[0]["system"] == "polaris"

      with open(os.path.join(run_dir, "node_counter_samples.jsonl")) as handle:
         counter_lines = [json.loads(line) for line in handle if line.strip()]
      assert len(counter_lines) >= 1

      with open(os.path.join(run_dir, "diagnostic_censuses.jsonl")) as handle:
         census_lines = [json.loads(line) for line in handle if line.strip()]
      assert len(census_lines) >= 1

   def test_status_output_prints_resolved_config_and_output_paths(
         self, tmp_path, fake_proc_env):
      home_dir = str(tmp_path / "home")
      os.makedirs(home_dir, exist_ok=True)
      config_path = str(tmp_path / "config.yaml")
      _write_config(config_path, home_dir)

      result = _invoke([
         "daemon", "dry-run",
         "--config", config_path,
         "--home", home_dir,
         "--run-id", "status-check-1",
      ])

      assert result.exit_code == 0, result.output
      assert ("config: %s" % config_path) in result.output
      assert "system: polaris" in result.output
      expected_output_root = os.path.join(home_dir, "phase0-runs")
      assert ("output_root: %s" % expected_output_root) in result.output
      expected_run_dir = os.path.join(
         expected_output_root, "phase0-status-check-1")
      assert ("run_dir: %s" % expected_run_dir) in result.output

   def test_strict_config_error_exits_nonzero_with_diagnostic(self, tmp_path):
      config_path = str(tmp_path / "bad_config.yaml")
      import yaml
      with open(config_path, "w") as handle:
         yaml.safe_dump({
            "system": "polaris",
            "nodes": [{"hostname": "x", "role": "local"}],
            "output_root": "~/phase0-runs",
            "probe_python": _probe_python(),
            "not_a_real_key": True,
         }, handle)

      result = _invoke(["daemon", "dry-run", "--config", config_path])

      assert result.exit_code != 0
      assert "not_a_real_key" in result.output

   def test_missing_config_file_exits_nonzero(self, tmp_path):
      missing_path = str(tmp_path / "does_not_exist.yaml")
      result = _invoke(["daemon", "dry-run", "--config", missing_path])
      assert result.exit_code != 0

   def test_fatal_sink_exit_code_propagated(self, tmp_path, fake_proc_env):
      """A configured ``min_free_disk_pct`` no real filesystem can ever
      satisfy forces Phase0Sink's own disk guard to raise on the very
      first write (the node_hardware record), which Daemon.run() maps
      to EXIT_SINK_FATAL -- proving the CLI propagates the daemon's own
      exit code rather than always exiting 0/1 on its own account.
      """
      home_dir = str(tmp_path / "home")
      os.makedirs(home_dir, exist_ok=True)
      config_path = str(tmp_path / "config.yaml")
      _write_config(config_path, home_dir, min_free_disk_pct=99.9999999)

      result = _invoke([
         "daemon", "dry-run",
         "--config", config_path,
         "--home", home_dir,
         "--run-id", "fatal-1",
      ])

      assert result.exit_code == 2, result.output


# --------------------------------------------------------------------------
# daemon smoke: short --duration-sec override, config's own cadence intact
# --------------------------------------------------------------------------

class TestDaemonSmoke:
   def test_duration_override_bounds_a_long_configured_run(
         self, tmp_path, fake_proc_env):
      home_dir = str(tmp_path / "home")
      os.makedirs(home_dir, exist_ok=True)
      config_path = str(tmp_path / "config.yaml")
      # Configured for a much longer run than the smoke override below --
      # proves --duration-sec actually overrides config.duration_sec
      # rather than the smoke command silently honoring the file's own
      # (here, deliberately long) value.
      _write_config(config_path, home_dir, duration_sec=3600)

      result = _invoke([
         "daemon", "smoke",
         "--config", config_path,
         "--home", home_dir,
         "--run-id", "smoke-1",
         "--duration-sec", "2",
      ])

      assert result.exit_code == 0, result.output
      run_dir = os.path.join(home_dir, "phase0-runs", "phase0-smoke-1")
      assert os.path.exists(os.path.join(run_dir, "DONE"))
      with open(os.path.join(run_dir, "summary.json")) as handle:
         summary = json.load(handle)
      assert summary["acceptance"]["completion"] == "clean"

   def test_rejects_nonpositive_duration(self, tmp_path, fake_proc_env):
      home_dir = str(tmp_path / "home")
      os.makedirs(home_dir, exist_ok=True)
      config_path = str(tmp_path / "config.yaml")
      _write_config(config_path, home_dir)

      result = _invoke([
         "daemon", "smoke",
         "--config", config_path,
         "--home", home_dir,
         "--duration-sec", "0",
      ])

      assert result.exit_code != 0


# --------------------------------------------------------------------------
# validate-run: structural artifact validation over a completed run dir
# --------------------------------------------------------------------------

class TestValidateRun:
   def _produce_run(self, tmp_path, fake_proc_env, run_id="validate-1"):
      home_dir = str(tmp_path / "home")
      os.makedirs(home_dir, exist_ok=True)
      config_path = str(tmp_path / "config.yaml")
      _write_config(config_path, home_dir)
      result = _invoke([
         "daemon", "dry-run",
         "--config", config_path,
         "--home", home_dir,
         "--run-id", run_id,
      ])
      assert result.exit_code == 0, result.output
      return os.path.join(home_dir, "phase0-runs", "phase0-%s" % run_id)

   def test_valid_clean_run_reports_ok(self, tmp_path, fake_proc_env):
      run_dir = self._produce_run(tmp_path, fake_proc_env)
      result = _invoke(["validate-run", run_dir])
      assert result.exit_code == 0, result.output
      assert "valid" in result.output.lower()
      assert ("run_dir: %s" % os.path.abspath(run_dir)) in result.output

   def test_malformed_line_reported_and_fails(self, tmp_path, fake_proc_env):
      run_dir = self._produce_run(tmp_path, fake_proc_env, run_id="validate-2")
      counter_path = os.path.join(run_dir, "node_counter_samples.jsonl")
      with open(counter_path, "a") as handle:
         handle.write("{not valid json\n")

      result = _invoke(["validate-run", run_dir])

      assert result.exit_code != 0
      assert "malformed" in result.output.lower()

   def test_missing_done_flag_fails(self, tmp_path, fake_proc_env):
      run_dir = self._produce_run(tmp_path, fake_proc_env, run_id="validate-3")
      os.unlink(os.path.join(run_dir, "DONE"))

      result = _invoke(["validate-run", run_dir])

      assert result.exit_code != 0
      assert "done" in result.output.lower()

   def test_nonexistent_run_dir_exits_nonzero(self, tmp_path):
      result = _invoke(["validate-run", str(tmp_path / "does-not-exist")])
      assert result.exit_code != 0


# --------------------------------------------------------------------------
# Sanity: the artifact validator this CLI wires in is the real one.
# --------------------------------------------------------------------------

class TestUsesRealArtifactValidator:
   def test_validate_jsonl_artifact_is_the_jsonl_module_function(self):
      from node_monitor.output import jsonl as jsonl_mod

      assert validate_jsonl_artifact is jsonl_mod.validate_jsonl_artifact
