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

from node_monitor.cli import main as cli_main  # noqa: E402
from node_monitor.cli.main import cli  # noqa: E402
from node_monitor.collector import transport  # noqa: E402
from node_monitor.config import load_config  # noqa: E402
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
# _config_to_raw: daemon smoke's config-reconstruction helper must
# preserve every already-loaded node's ssh_target exactly (bug found
# independently after t_88d97d8e merged at 41e7c64: _config_to_raw
# rebuilt each node from only hostname/role, silently discarding a
# configured remote ssh_target on every daemon smoke invocation).
# --------------------------------------------------------------------------

class TestConfigToRaw:
   def _mixed_config(self, tmp_path, home_dir, remote_ssh_target):
      raw = {
         "system": "polaris",
         "nodes": [
            {"hostname": "ctr-local.example.org", "role": "local"},
            {
               "hostname": "ctr-remote.example.org",
               "role": "remote",
               "ssh_target": remote_ssh_target,
            },
         ],
         "output_root": "~/phase0-runs",
         "probe_python": _probe_python(),
      }
      os.makedirs(os.path.join(home_dir, "phase0-runs"), exist_ok=True)
      return load_config(raw, home=home_dir)

   def test_preserves_explicit_remote_ssh_target(self, tmp_path):
      home_dir = str(tmp_path / "home")
      os.makedirs(home_dir, exist_ok=True)
      config = self._mixed_config(
         tmp_path, home_dir, remote_ssh_target="polaris-login-01.head")

      raw = cli_main._config_to_raw(config)

      remote_raw = next(
         n for n in raw["nodes"] if n["hostname"] == "ctr-remote.example.org")
      assert remote_raw.get("ssh_target") == "polaris-login-01.head"
      # Reloading the reconstructed raw mapping must reproduce the exact
      # same effective_ssh_target -- not silently fall back to hostname.
      reloaded = load_config(raw, home=home_dir)
      reloaded_remote = next(
         n for n in reloaded.nodes if n.hostname == "ctr-remote.example.org")
      assert reloaded_remote.effective_ssh_target == "polaris-login-01.head"

   def test_preserves_omission_when_ssh_target_is_none(self, tmp_path):
      home_dir = str(tmp_path / "home")
      os.makedirs(home_dir, exist_ok=True)
      raw_config = {
         "system": "polaris",
         "nodes": [
            {"hostname": "ctr-local.example.org", "role": "local"},
            {"hostname": "ctr-remote.example.org", "role": "remote"},
         ],
         "output_root": "~/phase0-runs",
         "probe_python": _probe_python(),
      }
      os.makedirs(os.path.join(home_dir, "phase0-runs"), exist_ok=True)
      config = load_config(raw_config, home=home_dir)

      raw = cli_main._config_to_raw(config)

      remote_raw = next(
         n for n in raw["nodes"] if n["hostname"] == "ctr-remote.example.org")
      assert "ssh_target" not in remote_raw
      # Reloading must not reject the reconstructed mapping (a stray
      # ssh_target=None key would fail _validate_node's non-empty-string
      # check) and must fall back to hostname exactly as before.
      reloaded = load_config(raw, home=home_dir)
      reloaded_remote = next(
         n for n in reloaded.nodes if n.hostname == "ctr-remote.example.org")
      assert reloaded_remote.effective_ssh_target == "ctr-remote.example.org"


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


# --------------------------------------------------------------------------
# validate-run: DONE + truncated final line must fail (review round 1
# finding #2, kanban task t_3d9d7387). A truncated final line without a
# DONE flag is unaffected -- DONE absence already fails on its own.
# --------------------------------------------------------------------------

class TestValidateRunDonePlusTruncated:
   def test_done_with_truncated_final_line_fails(self, tmp_path):
      run_dir = tmp_path / "phase0-truncated-1"
      run_dir.mkdir()
      (run_dir / "DONE").write_text("2026-01-01T00:00:00Z\n")
      # No trailing newline on the final line -- exactly what
      # validate_jsonl_artifact's own truncated_final_line=True case
      # detects (see node_monitor/output/jsonl.py).
      (run_dir / "x.jsonl").write_bytes(
         b'{"ok":1}\n{"broken":')

      result = _invoke(["validate-run", str(run_dir)])

      assert result.exit_code != 0, result.output
      assert "truncated" in result.output.lower()
      assert "done" in result.output.lower()

   def test_truncated_final_line_without_done_still_fails_on_done_alone(
         self, tmp_path):
      """Sanity: a truncated line with no DONE flag was already an
      INVALID run before this fix (missing DONE), and remains one --
      this fix must not weaken that pre-existing, unrelated check.
      """
      run_dir = tmp_path / "phase0-truncated-2"
      run_dir.mkdir()
      (run_dir / "x.jsonl").write_bytes(b'{"ok":1}\n{"broken":')

      result = _invoke(["validate-run", str(run_dir)])

      assert result.exit_code != 0, result.output
      assert "done" in result.output.lower()

   def test_truncated_without_malformed_and_without_done_reports_truncated_but_not_malformed(
         self, tmp_path):
      """Confirms validate-run still surfaces the underlying
      truncated_final_line=True detail even though this file has zero
      OTHER malformed lines -- proving the new DONE+truncated failure
      is a distinct check from the pre-existing malformed-line check.
      """
      run_dir = tmp_path / "phase0-truncated-3"
      run_dir.mkdir()
      (run_dir / "x.jsonl").write_bytes(b'{"ok":1}\n{"broken":')

      result = _invoke(["validate-run", str(run_dir)])

      assert "truncated_final_line=True" in result.output
      assert "malformed=0" in result.output


# --------------------------------------------------------------------------
# finalize-recovered-run: recovery finalizer CLI for an orphaned run
# directory whose daemon died before writing summary.json/DONE.
# --------------------------------------------------------------------------

class TestFinalizeRecoveredRun:
   def _orphaned_run_dir(self, tmp_path, fake_proc_env, run_id="orphan-1"):
      """Produce a real orphaned run directory the same way the daemon
      would leave one behind mid-crash: run a real dry-run to completion
      (so the JSONL artifacts are genuine, production-shaped content),
      then delete summary.json/DONE to simulate the daemon dying just
      before finalization -- exactly the phase0-canary-20260916T172446Z
      incident shape (complete JSONL data, no terminal summary/DONE).
      """
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
      run_dir = os.path.join(home_dir, "phase0-runs", "phase0-%s" % run_id)
      os.unlink(os.path.join(run_dir, "summary.json"))
      os.unlink(os.path.join(run_dir, "DONE"))
      return run_dir

   def test_finalizes_orphaned_run_successfully(self, tmp_path, fake_proc_env):
      run_dir = self._orphaned_run_dir(tmp_path, fake_proc_env)

      result = _invoke(["finalize-recovered-run", run_dir])

      assert result.exit_code == 0, result.output
      assert os.path.exists(os.path.join(run_dir, "summary.json"))
      assert os.path.exists(os.path.join(run_dir, "DONE"))
      with open(os.path.join(run_dir, "summary.json")) as handle:
         summary = json.load(handle)
      assert summary["recovery"]["recovered"] is True
      assert "acceptance" not in summary

   def test_refuses_run_with_existing_summary_and_done(
         self, tmp_path, fake_proc_env):
      home_dir = str(tmp_path / "home")
      os.makedirs(home_dir, exist_ok=True)
      config_path = str(tmp_path / "config.yaml")
      _write_config(config_path, home_dir)
      result = _invoke([
         "daemon", "dry-run",
         "--config", config_path,
         "--home", home_dir,
         "--run-id", "clean-1",
      ])
      assert result.exit_code == 0, result.output
      run_dir = os.path.join(home_dir, "phase0-runs", "phase0-clean-1")

      result = _invoke(["finalize-recovered-run", run_dir])

      assert result.exit_code != 0
      assert "already exists" in result.output.lower()

   def test_nonexistent_run_dir_exits_nonzero(self, tmp_path):
      result = _invoke(
         ["finalize-recovered-run", str(tmp_path / "does-not-exist")])
      assert result.exit_code != 0

   def test_symlinked_run_dir_refused(self, tmp_path):
      real_dir = tmp_path / "real"
      real_dir.mkdir(mode=0o700)
      link = tmp_path / "phase0-linked"
      os.symlink(str(real_dir), str(link))

      result = _invoke(["finalize-recovered-run", str(link)])

      assert result.exit_code != 0

   def test_explicit_run_id_override(self, tmp_path, fake_proc_env):
      run_dir = self._orphaned_run_dir(tmp_path, fake_proc_env, run_id="orphan-2")

      result = _invoke(
         ["finalize-recovered-run", run_dir, "--run-id", "override-name"])

      assert result.exit_code == 0, result.output
      with open(os.path.join(run_dir, "summary.json")) as handle:
         summary = json.load(handle)
      assert summary["run_id"] == "override-name"

   def test_refuses_directory_with_no_jsonl_artifacts(self, tmp_path):
      run_dir = tmp_path / "phase0-empty"
      run_dir.mkdir(mode=0o700)

      result = _invoke(["finalize-recovered-run", str(run_dir)])

      assert result.exit_code != 0
      assert not os.path.exists(str(run_dir / "summary.json"))
      assert not os.path.exists(str(run_dir / "DONE"))

   def test_refuses_malformed_artifact_before_publishing(
         self, tmp_path, fake_proc_env):
      run_dir = self._orphaned_run_dir(tmp_path, fake_proc_env, run_id="orphan-3")
      with open(os.path.join(run_dir, "node_hardware.jsonl"), "a") as handle:
         handle.write("{not json\n")

      result = _invoke(["finalize-recovered-run", run_dir])

      assert result.exit_code != 0
      assert not os.path.exists(os.path.join(run_dir, "summary.json"))
      assert not os.path.exists(os.path.join(run_dir, "DONE"))


# --------------------------------------------------------------------------
# Mixed local/remote transport dispatch (review round 1 finding #1,
# kanban task t_3d9d7387): the CLI must wire local nodes to
# collector.transport.run_local_probe and remote nodes to
# collector.transport.run_remote_probe -- never reject a config
# declaring a remote node. Exercised directly against
# ``cli_main._make_transport_fn`` (not a full Daemon.run()) with
# ``collector.transport``'s own run_local_probe/run_remote_probe
# monkeypatched -- exactly what the card allows ("tests may monkeypatch
# transport only where needed to avoid network") -- so these tests
# never touch a real network or a real login node.
# --------------------------------------------------------------------------

class TestMixedTransportDispatch:
   def _config(self, tmp_path, home_dir, remote_ssh_target=None):
      remote_node = {
         "hostname": "dispatch-remote.example.org", "role": "remote"}
      if remote_ssh_target is not None:
         remote_node["ssh_target"] = remote_ssh_target
      raw = {
         "system": "polaris",
         "nodes": [
            {"hostname": "dispatch-local.example.org", "role": "local"},
            remote_node,
         ],
         "output_root": "~/phase0-runs",
         "probe_python": _probe_python(),
         "ssh_connect_timeout_sec": 4,
      }
      os.makedirs(os.path.join(home_dir, "phase0-runs"), exist_ok=True)
      return load_config(raw, home=home_dir)

   def test_local_node_dispatches_to_run_local_probe(
         self, tmp_path, monkeypatch):
      home_dir = str(tmp_path / "home")
      os.makedirs(home_dir, exist_ok=True)
      config = self._config(tmp_path, home_dir)

      calls = {"local": None, "remote": None}

      async def fake_run_local_probe(**kwargs):
         calls["local"] = kwargs
         return transport.ProbeResult(
            payload={"loop": kwargs["loop"], "probe_version": 4,
                     "hostname_fqdn": "dispatch-local.example.org"},
            exit_code=0, stdout_bytes=10, stderr="", stderr_truncated=False,
            wall_seconds=0.01)

      async def fake_run_remote_probe(**kwargs):
         calls["remote"] = kwargs
         raise AssertionError("run_remote_probe must not be called for a local node")

      monkeypatch.setattr(cli_main.transport, "run_local_probe", fake_run_local_probe)
      monkeypatch.setattr(cli_main.transport, "run_remote_probe", fake_run_remote_probe)

      transport_fn = cli_main._make_transport_fn(config, probe_version=4)
      local_node = config.local_node

      import asyncio
      result = asyncio.run(transport_fn(local_node, "counter"))

      assert calls["local"] is not None
      assert calls["remote"] is None
      assert calls["local"]["probe_python"] == config.probe_python
      assert calls["local"]["expected_probe_version"] == 4
      assert result.payload["hostname_fqdn"] == "dispatch-local.example.org"

   def test_remote_node_dispatches_to_run_remote_probe(
         self, tmp_path, monkeypatch):
      home_dir = str(tmp_path / "home")
      os.makedirs(home_dir, exist_ok=True)
      config = self._config(tmp_path, home_dir)

      calls = {"local": None, "remote": None}

      async def fake_run_local_probe(**kwargs):
         calls["local"] = kwargs
         raise AssertionError("run_local_probe must not be called for a remote node")

      async def fake_run_remote_probe(**kwargs):
         calls["remote"] = kwargs
         return transport.ProbeResult(
            payload={"loop": kwargs["loop"], "probe_version": 4,
                     "hostname_fqdn": kwargs["hostname"]},
            exit_code=0, stdout_bytes=10, stderr="", stderr_truncated=False,
            wall_seconds=0.01)

      monkeypatch.setattr(cli_main.transport, "run_local_probe", fake_run_local_probe)
      monkeypatch.setattr(cli_main.transport, "run_remote_probe", fake_run_remote_probe)

      transport_fn = cli_main._make_transport_fn(config, probe_version=4)
      remote_node = config.remote_nodes[0]

      import asyncio
      result = asyncio.run(transport_fn(remote_node, "census"))

      assert calls["remote"] is not None
      assert calls["local"] is None
      assert calls["remote"]["hostname"] == "dispatch-remote.example.org"
      # kanban t_88d97d8e: the per-call transport.validate_probe_payload
      # alias-equals-FQDN check was itself the Problem 2 bug (a legitimate
      # ssh_target/alias never equals the probe's self-reported FQDN) --
      # _make_transport_fn now always passes expected_fqdn=None; the real
      # consistency/uniqueness checks live in Daemon itself (see
      # tests/test_daemon.py TestFqdnConsistencyAndUniqueness).
      assert calls["remote"]["expected_fqdn"] is None
      assert calls["remote"]["connect_timeout_sec"] == config.ssh_connect_timeout_sec
      # Project-owned SSH config, never the operator's ~/.ssh/config.
      assert calls["remote"]["ssh_config_path"].startswith(config.output_root)
      assert os.path.exists(calls["remote"]["ssh_config_path"])
      assert result.payload["hostname_fqdn"] == "dispatch-remote.example.org"

   def test_remote_node_uses_explicit_ssh_target_but_preserves_reported_fqdn(
         self, tmp_path, monkeypatch):
      home_dir = str(tmp_path / "home")
      os.makedirs(home_dir, exist_ok=True)
      config = self._config(
         tmp_path, home_dir, remote_ssh_target="polaris-login-01.head")
      calls = []

      async def fake_run_remote_probe(**kwargs):
         calls.append(kwargs)
         return transport.ProbeResult(
            payload={"loop": kwargs["loop"], "probe_version": 4,
                     "hostname_fqdn":
                        "polaris-login-01.hsn.cm.polaris.alcf.anl.gov"},
            exit_code=0, stdout_bytes=10, stderr="", stderr_truncated=False,
            wall_seconds=0.01)

      monkeypatch.setattr(cli_main.transport, "run_remote_probe", fake_run_remote_probe)
      transport_fn = cli_main._make_transport_fn(config, probe_version=4)

      import asyncio
      result = asyncio.run(transport_fn(config.remote_nodes[0], "counter"))

      assert calls[0]["hostname"] == "polaris-login-01.head"
      assert calls[0]["expected_fqdn"] is None
      assert result.payload["hostname_fqdn"] == \
         "polaris-login-01.hsn.cm.polaris.alcf.anl.gov"

   def test_remote_probe_failure_propagates(self, tmp_path, monkeypatch):
      """A remote transport failure (e.g. SSH auth/timeout) must
      propagate out of transport_fn unchanged -- Daemon._poll_fn's own
      existing failure handling (already tested at the Daemon level)
      is what turns this into a recorded node_poll_failures/
      node_collection_log entry; this CLI layer must not swallow or
      transform it.
      """
      home_dir = str(tmp_path / "home")
      os.makedirs(home_dir, exist_ok=True)
      config = self._config(tmp_path, home_dir)

      async def fake_run_remote_probe(**kwargs):
         raise transport.SSHAuthError("ssh failed (exit 255): permission denied")

      monkeypatch.setattr(cli_main.transport, "run_remote_probe", fake_run_remote_probe)

      transport_fn = cli_main._make_transport_fn(config, probe_version=4)
      remote_node = config.remote_nodes[0]

      import asyncio
      with pytest.raises(transport.SSHAuthError):
         asyncio.run(transport_fn(remote_node, "counter"))


# --------------------------------------------------------------------------
# Quarantine boundary (review round 1 finding #3, kanban task
# t_3d9d7387): the CLI must never import node_monitor.collector.
# remote_probe at runtime (README.md: "never imported by the daemon").
# --------------------------------------------------------------------------

class TestRemoteProbeQuarantine:
   def test_cli_main_module_does_not_import_remote_probe(self):
      assert not hasattr(cli_main, "PROBE_VERSION")
      assert "node_monitor.collector.remote_probe" not in {
         name for name in dir(cli_main) if not name.startswith("_")}

   def test_remote_probe_not_in_cli_main_globals(self):
      import node_monitor.collector.remote_probe as remote_probe_mod

      # cli_main must never hold a reference to the quarantined module
      # or any of its members under any name.
      for value in vars(cli_main).values():
         assert value is not remote_probe_mod

   def test_resolve_probe_version_uses_subprocess_not_import(self, tmp_path):
      """_resolve_probe_version must obtain PROBE_VERSION by executing
      the shipped probe's own --version contract as a subprocess, and
      must return the same integer the real probe module reports --
      without this test module itself needing to import remote_probe
      for anything other than reading its PROBE_VERSION to compare
      against (never invoked as part of the CLI's own runtime).
      """
      import node_monitor.collector.remote_probe as remote_probe_mod

      version = cli_main._resolve_probe_version(_probe_python())

      assert version == remote_probe_mod.PROBE_VERSION
