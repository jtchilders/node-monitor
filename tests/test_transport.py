"""Tests for node_monitor.collector.transport.

Design: PHASE0_DAEMON_DESIGN.md "Architecture" (local vs SSH invocation,
LD_PRELOAD hygiene, FQDN/version provenance) and PLANNING.md 4.2-4.3 (no
shell, project-owned SSH config, BatchMode=yes, hard timeouts, process
groups, bounded stderr). Every subprocess-level test runs a REAL
executable fake interpreter (tests/fixtures/fake_bin.py) rather than
mocking asyncio.create_subprocess_exec, so argv construction, stdin
delivery, environment, signal handling, and process-group cleanup are
all exercised against a real child process, on macOS, with no
/proc dependency.

All async behavior is exercised with plain ``asyncio.run()`` calls inside
ordinary (sync) test functions -- no pytest-asyncio plugin is installed
in this environment (see tests/test_jsonl_output.py, which established
this pattern first).
"""

import asyncio
import json
import os
import stat
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "fixtures"))

from node_monitor.collector import transport  # noqa: E402
from fake_bin import write_fake_probe  # noqa: E402


def _run(coro):
   return asyncio.run(coro)


@pytest.fixture
def fake_probe(tmp_path):
   return write_fake_probe(tmp_path / "fake_probe.py")


@pytest.fixture
def probe_script_path(tmp_path):
   """A stand-in for the real remote_probe.py file path -- transport.py
   never parses its contents for a local run, only passes the path.
   """
   path = tmp_path / "remote_probe.py"
   path.write_text("# not a real probe, just a path\n")
   return str(path)


# --------------------------------------------------------------------------
# argv construction -- pure functions, no subprocess involved
# --------------------------------------------------------------------------

class TestBuildLocalArgv:
   def test_exact_argv(self):
      argv = transport.build_local_argv(
         "/usr/bin/python3.11", "/path/to/remote_probe.py", "counter", 4)
      assert argv == [
         "/usr/bin/python3.11", "/path/to/remote_probe.py",
         "--loop", "counter", "--max-seconds", "4",
      ]

   def test_fractional_max_seconds_preserved(self):
      argv = transport.build_local_argv(
         "/usr/bin/python3.11", "/p/remote_probe.py", "census", 2.5)
      assert argv[-1] == "2.5"

   def test_missing_probe_python_rejected(self):
      with pytest.raises(ValueError):
         transport.build_local_argv("", "/p/remote_probe.py", "census", 4)

   def test_missing_script_path_rejected(self):
      with pytest.raises(ValueError):
         transport.build_local_argv("/usr/bin/python3.11", "", "census", 4)


class TestBuildRemoteArgv:
   def _build(self, **overrides):
      kwargs = dict(
         ssh_binary="ssh",
         ssh_config_path="/home/x/node-monitor/ssh_config",
         connect_timeout_sec=8,
         hostname="polaris-login-01.head",
         probe_python="/usr/bin/python3.11",
         loop="census",
         max_seconds=20,
      )
      kwargs.update(overrides)
      return transport.build_remote_argv(**kwargs)

   def test_exact_argv(self):
      argv = self._build()
      assert argv == [
         "ssh",
         "-F", "/home/x/node-monitor/ssh_config",
         "-o", "BatchMode=yes",
         "-o", "ConnectTimeout=8",
         "polaris-login-01.head",
         "env", "-u", "LD_PRELOAD",
         "/usr/bin/python3.11", "-",
         "--loop", "census",
         "--max-seconds", "20",
      ]

   def test_uses_head_alias_verbatim_never_substitutes(self):
      """PLANNING.md 4.2c: the daemon must use the discovered '.head'
      alias exactly as configured -- transport.py has no business
      rewriting or 'helpfully' stripping it.
      """
      argv = self._build(hostname="polaris-login-04.head")
      assert "polaris-login-04.head" in argv
      assert "polaris-login-04" not in [a.replace(".head", "") for a in argv
                                         if a == "polaris-login-04"]

   def test_batchmode_yes_always_present(self):
      argv = self._build()
      idx = argv.index("-o")
      assert "BatchMode=yes" in argv

   def test_stdin_delivery_marker_is_dash(self):
      argv = self._build()
      # probe_python is immediately followed by "-" -- stdin delivery,
      # never a staged path on the remote filesystem.
      py_index = argv.index("/usr/bin/python3.11")
      assert argv[py_index + 1] == "-"

   def test_env_strips_ld_preload_on_remote_command(self):
      argv = self._build()
      assert argv[argv.index("env"):argv.index("env") + 3] == [
         "env", "-u", "LD_PRELOAD"]

   def test_missing_ssh_config_path_rejected(self):
      with pytest.raises(ValueError):
         self._build(ssh_config_path="")

   def test_missing_probe_python_rejected(self):
      with pytest.raises(ValueError):
         self._build(probe_python="")

   def test_missing_hostname_rejected(self):
      with pytest.raises(ValueError):
         self._build(hostname="")


# --------------------------------------------------------------------------
# Project-owned SSH config / control path
# --------------------------------------------------------------------------

class TestWriteSSHConfig:
   def test_never_touches_home_ssh_config(self, tmp_path, monkeypatch):
      """write_ssh_config must write only to the caller-given path -- it
      must never read or modify anything under the operator's real
      ~/.ssh, even if that directory happens to exist.
      """
      fake_home = tmp_path / "fake_home"
      real_ssh_dir = fake_home / ".ssh"
      real_ssh_dir.mkdir(parents=True)
      real_config = real_ssh_dir / "config"
      real_config.write_text("# operator's own interactive config\n")
      monkeypatch.setattr(os.path, "expanduser",
                           lambda p: p.replace("~", str(fake_home)))

      control_dir = tmp_path / "ssh-control"
      config_path = tmp_path / "ssh_config"
      transport.write_ssh_config(str(config_path), str(control_dir), 8)

      # The operator's real config is untouched.
      assert real_config.read_text() == "# operator's own interactive config\n"
      # The file itself must live where the caller pointed it, not under
      # any conventional ~/.ssh location.
      assert str(config_path).startswith(str(tmp_path))
      assert config_path.exists()

   def test_batchmode_and_multiplexing_present(self, tmp_path):
      control_dir = tmp_path / "ssh-control"
      config_path = tmp_path / "ssh_config"
      transport.write_ssh_config(str(config_path), str(control_dir), 8)
      text = config_path.read_text()
      assert "BatchMode yes" in text
      assert "ControlMaster auto" in text
      assert "ControlPersist" in text
      assert str(control_dir) in text

   def test_connect_timeout_reflected(self, tmp_path):
      control_dir = tmp_path / "ssh-control"
      config_path = tmp_path / "ssh_config"
      transport.write_ssh_config(str(config_path), str(control_dir), 8)
      assert "ConnectTimeout 8" in config_path.read_text()

   def test_file_and_dir_modes_are_private(self, tmp_path):
      control_dir = tmp_path / "ssh-control"
      config_path = tmp_path / "ssh_config"
      transport.write_ssh_config(str(config_path), str(control_dir), 8)
      assert stat.S_IMODE(os.stat(config_path).st_mode) == 0o600
      assert stat.S_IMODE(os.stat(control_dir).st_mode) == 0o700

   def test_idempotent_rewrite(self, tmp_path):
      control_dir = tmp_path / "ssh-control"
      config_path = tmp_path / "ssh_config"
      transport.write_ssh_config(str(config_path), str(control_dir), 8)
      transport.write_ssh_config(str(config_path), str(control_dir), 8)
      assert config_path.exists()


# --------------------------------------------------------------------------
# Local invocation -- real subprocess, fake interpreter
# --------------------------------------------------------------------------

class TestRunLocalProbe:
   def test_success_returns_validated_payload(self, fake_probe, probe_script_path):
      env = dict(os.environ)
      env["FAKE_LOOP"] = "counter"
      result = _run(transport.run_local_probe(
         probe_python=fake_probe,
         probe_script_path=probe_script_path,
         loop="counter",
         probe_max_seconds=4,
         hard_timeout_sec=5,
         expected_probe_version=4,
         base_env=env,
      ))
      assert result.payload["loop"] == "counter"
      assert result.payload["probe_version"] == 4
      assert result.exit_code == 0

   def test_no_shell_invoked(self, tmp_path, probe_script_path):
      """A script that only works when exec'd directly (its argv[0] is
      not a shell builtin, and it has no shebang the OS could fall back
      to differently) proves create_subprocess_exec, not a shell, ran
      it: give it a filename shell metacharacters would mangle if any
      part of this path went through `sh -c`.
      """
      script = tmp_path / "fake;probe.py"
      write_fake_probe(script)
      env = dict(os.environ)
      result = _run(transport.run_local_probe(
         probe_python=str(script),
         probe_script_path=probe_script_path,
         loop="census",
         probe_max_seconds=4,
         hard_timeout_sec=5,
         expected_probe_version=4,
         base_env=env,
      ))
      assert result.payload["loop"] == "census"

   def test_argv_reaches_child_exactly(self, fake_probe, probe_script_path):
      env = dict(os.environ)
      env["FAKE_ECHO_ARGV"] = "1"
      result = _run(transport.run_local_probe(
         probe_python=fake_probe,
         probe_script_path=probe_script_path,
         loop="census",
         probe_max_seconds=7,
         hard_timeout_sec=8,
         expected_probe_version=4,
         base_env=env,
      ))
      assert result.payload["argv"] == [
         probe_script_path, "--loop", "census", "--max-seconds", "7"]

   def test_ld_preload_stripped_from_local_child(
         self, fake_probe, probe_script_path):
      env = dict(os.environ)
      env["LD_PRELOAD"] = "/soft/xalt/libxalt_init.so"
      env["FAKE_CHECK_NO_LD_PRELOAD"] = "1"
      result = _run(transport.run_local_probe(
         probe_python=fake_probe,
         probe_script_path=probe_script_path,
         loop="census",
         probe_max_seconds=4,
         hard_timeout_sec=5,
         expected_probe_version=4,
         base_env=env,
      ))
      assert result.exit_code == 0

   def test_ld_preload_present_would_fail_without_stripping(self):
      """Sanity check on the fake itself: prove FAKE_CHECK_NO_LD_PRELOAD
      actually detects a leaked LD_PRELOAD, so test_ld_preload_stripped
      above is not passing by accident.
      """
      import subprocess
      script = write_fake_probe(
         os.path.join(
            os.path.dirname(__file__), "fixtures", "_leak_check_tmp.py"))
      try:
         env = dict(os.environ)
         env["LD_PRELOAD"] = "/soft/xalt/libxalt_init.so"
         env["FAKE_CHECK_NO_LD_PRELOAD"] = "1"
         result = subprocess.run(
            [script], env=env, capture_output=True, text=True)
         assert result.returncode == 9
      finally:
         os.unlink(script)

   def test_payload_bytes_and_timing_captured(self, fake_probe, probe_script_path):
      env = dict(os.environ)
      env["FAKE_SLEEP_SECONDS"] = "0.05"
      result = _run(transport.run_local_probe(
         probe_python=fake_probe,
         probe_script_path=probe_script_path,
         loop="census",
         probe_max_seconds=4,
         hard_timeout_sec=5,
         expected_probe_version=4,
         base_env=env,
      ))
      assert result.stdout_bytes > 0
      assert result.wall_seconds >= 0.05

   def test_nonzero_exit_raises_probe_exit_error(
         self, fake_probe, probe_script_path):
      env = dict(os.environ)
      env["FAKE_EXIT_CODE"] = "3"
      env["FAKE_STDERR_TEXT"] = "/proc unreadable\n"
      with pytest.raises(transport.ProbeExitError) as excinfo:
         _run(transport.run_local_probe(
            probe_python=fake_probe,
            probe_script_path=probe_script_path,
            loop="census",
            probe_max_seconds=4,
            hard_timeout_sec=5,
            expected_probe_version=4,
            base_env=env,
         ))
      assert excinfo.value.exit_code == 3
      assert "proc unreadable" in excinfo.value.stderr
      assert excinfo.value.failure_type == "probe_exit"

   def test_malformed_json_raises_typed_error(
         self, fake_probe, probe_script_path):
      env = dict(os.environ)
      env["FAKE_STDOUT_TEXT"] = "not json at all\n"
      with pytest.raises(transport.MalformedJSONError) as excinfo:
         _run(transport.run_local_probe(
            probe_python=fake_probe,
            probe_script_path=probe_script_path,
            loop="census",
            probe_max_seconds=4,
            hard_timeout_sec=5,
            expected_probe_version=4,
            base_env=env,
         ))
      assert excinfo.value.failure_type == "malformed_json"

   def test_probe_version_mismatch_raises_typed_error(
         self, fake_probe, probe_script_path):
      env = dict(os.environ)
      env["FAKE_PROBE_VERSION"] = "3"
      with pytest.raises(transport.ProbeVersionMismatchError) as excinfo:
         _run(transport.run_local_probe(
            probe_python=fake_probe,
            probe_script_path=probe_script_path,
            loop="census",
            probe_max_seconds=4,
            hard_timeout_sec=5,
            expected_probe_version=4,
            base_env=env,
         ))
      assert excinfo.value.failure_type == "probe_version_mismatch"

   def test_loop_mismatch_raises_invariant_violation(
         self, fake_probe, probe_script_path):
      env = dict(os.environ)
      env["FAKE_LOOP"] = "counter"
      with pytest.raises(transport.InvariantViolationError):
         _run(transport.run_local_probe(
            probe_python=fake_probe,
            probe_script_path=probe_script_path,
            loop="census",
            probe_max_seconds=4,
            hard_timeout_sec=5,
            expected_probe_version=4,
            base_env=env,
         ))

   def test_fqdn_mismatch_raises_typed_error(
         self, fake_probe, probe_script_path):
      env = dict(os.environ)
      env["FAKE_HOSTNAME_FQDN"] = "wrong-node.example.org"
      with pytest.raises(transport.HostnameMismatchError) as excinfo:
         _run(transport.run_local_probe(
            probe_python=fake_probe,
            probe_script_path=probe_script_path,
            loop="census",
            probe_max_seconds=4,
            hard_timeout_sec=5,
            expected_probe_version=4,
            expected_fqdn="polaris-login-01.hsn.cm.polaris.alcf.anl.gov",
            base_env=env,
         ))
      assert excinfo.value.failure_type == "hostname_mismatch"

   def test_fqdn_match_succeeds(self, fake_probe, probe_script_path):
      env = dict(os.environ)
      env["FAKE_HOSTNAME_FQDN"] = "polaris-login-01.hsn.cm.polaris.alcf.anl.gov"
      result = _run(transport.run_local_probe(
         probe_python=fake_probe,
         probe_script_path=probe_script_path,
         loop="census",
         probe_max_seconds=4,
         hard_timeout_sec=5,
         expected_probe_version=4,
         expected_fqdn="polaris-login-01.hsn.cm.polaris.alcf.anl.gov",
         base_env=env,
      ))
      assert result.payload["hostname_fqdn"] == (
         "polaris-login-01.hsn.cm.polaris.alcf.anl.gov")

   def test_stderr_bounded(self, fake_probe, probe_script_path):
      env = dict(os.environ)
      env["FAKE_STDERR_TEXT"] = "x" * 200000
      env["FAKE_EXIT_CODE"] = "0"
      result = _run(transport.run_local_probe(
         probe_python=fake_probe,
         probe_script_path=probe_script_path,
         loop="census",
         probe_max_seconds=4,
         hard_timeout_sec=5,
         expected_probe_version=4,
         base_env=env,
         stderr_limit_bytes=1024,
      ))
      assert len(result.stderr) <= 1024
      assert result.stderr_truncated is True

   def test_stderr_not_truncated_when_under_limit(
         self, fake_probe, probe_script_path):
      env = dict(os.environ)
      env["FAKE_STDERR_TEXT"] = "short\n"
      result = _run(transport.run_local_probe(
         probe_python=fake_probe,
         probe_script_path=probe_script_path,
         loop="census",
         probe_max_seconds=4,
         hard_timeout_sec=5,
         expected_probe_version=4,
         base_env=env,
         stderr_limit_bytes=1024,
      ))
      assert result.stderr_truncated is False
      assert "short" in result.stderr


# --------------------------------------------------------------------------
# Remote invocation -- fake "ssh" binary, stdin delivery
# --------------------------------------------------------------------------

class TestRunRemoteProbe:
   def _config(self, tmp_path):
      control_dir = tmp_path / "ssh-control"
      config_path = tmp_path / "ssh_config"
      transport.write_ssh_config(str(config_path), str(control_dir), 8)
      return str(config_path)

   def test_success_returns_validated_payload(self, fake_probe, tmp_path):
      config_path = self._config(tmp_path)
      env = dict(os.environ)
      env["FAKE_LOOP"] = "census"
      result = _run(transport.run_remote_probe(
         ssh_binary=fake_probe,
         ssh_config_path=config_path,
         connect_timeout_sec=8,
         hostname="polaris-login-01.head",
         probe_python="/usr/bin/python3.11",
         probe_script_source=b"# probe source\n",
         loop="census",
         probe_max_seconds=20,
         hard_timeout_sec=30,
         expected_probe_version=4,
         base_env=env,
      ))
      assert result.payload["loop"] == "census"

   def test_probe_source_delivered_over_stdin(self, fake_probe, tmp_path):
      config_path = self._config(tmp_path)
      dump_path = tmp_path / "stdin_dump.bin"
      env = dict(os.environ)
      env["FAKE_STDIN_DUMP"] = str(dump_path)
      probe_source = b"#!/usr/bin/env python3\nprint('hello')\n"
      _run(transport.run_remote_probe(
         ssh_binary=fake_probe,
         ssh_config_path=config_path,
         connect_timeout_sec=8,
         hostname="polaris-login-01.head",
         probe_python="/usr/bin/python3.11",
         probe_script_source=probe_source,
         loop="census",
         probe_max_seconds=20,
         hard_timeout_sec=30,
         expected_probe_version=4,
         base_env=env,
      ))
      assert dump_path.read_bytes() == probe_source

   def test_ld_preload_stripped_from_local_ssh_process(
         self, fake_probe, tmp_path):
      config_path = self._config(tmp_path)
      env = dict(os.environ)
      env["LD_PRELOAD"] = "/soft/xalt/libxalt_init.so"
      env["FAKE_CHECK_NO_LD_PRELOAD"] = "1"
      result = _run(transport.run_remote_probe(
         ssh_binary=fake_probe,
         ssh_config_path=config_path,
         connect_timeout_sec=8,
         hostname="polaris-login-01.head",
         probe_python="/usr/bin/python3.11",
         probe_script_source=b"# src\n",
         loop="census",
         probe_max_seconds=20,
         hard_timeout_sec=30,
         expected_probe_version=4,
         base_env=env,
      ))
      assert result.exit_code == 0

   def test_ssh_auth_failure_classified(self, fake_probe, tmp_path):
      config_path = self._config(tmp_path)
      env = dict(os.environ)
      env["FAKE_EXIT_CODE"] = "255"
      env["FAKE_STDERR_TEXT"] = (
         "someuser@polaris-login-01.head: Permission denied "
         "(publickey,keyboard-interactive).\n")
      with pytest.raises(transport.SSHAuthError) as excinfo:
         _run(transport.run_remote_probe(
            ssh_binary=fake_probe,
            ssh_config_path=config_path,
            connect_timeout_sec=8,
            hostname="polaris-login-01.head",
            probe_python="/usr/bin/python3.11",
            probe_script_source=b"# src\n",
            loop="census",
            probe_max_seconds=20,
            hard_timeout_sec=30,
            expected_probe_version=4,
            base_env=env,
         ))
      assert excinfo.value.failure_type == "ssh_auth"

   def test_ssh_transport_failure_classified(self, fake_probe, tmp_path):
      config_path = self._config(tmp_path)
      env = dict(os.environ)
      env["FAKE_EXIT_CODE"] = "255"
      env["FAKE_STDERR_TEXT"] = (
         "ssh: connect to host polaris-login-03.head port 22: "
         "Connection closed by remote host\n")
      with pytest.raises(transport.SSHTransportError) as excinfo:
         _run(transport.run_remote_probe(
            ssh_binary=fake_probe,
            ssh_config_path=config_path,
            connect_timeout_sec=8,
            hostname="polaris-login-03.head",
            probe_python="/usr/bin/python3.11",
            probe_script_source=b"# src\n",
            loop="census",
            probe_max_seconds=20,
            hard_timeout_sec=30,
            expected_probe_version=4,
            base_env=env,
         ))
      assert excinfo.value.failure_type == "ssh_transport"

   def test_remote_probe_exit_nonzero_not_confused_with_ssh_failure(
         self, fake_probe, tmp_path):
      """A REMOTE probe exit (e.g. 3, /proc unreadable) must be typed as
      probe_exit, not misclassified as an ssh-layer failure -- only the
      reserved ssh exit code 255 goes through the ssh-failure path.
      """
      config_path = self._config(tmp_path)
      env = dict(os.environ)
      env["FAKE_EXIT_CODE"] = "3"
      env["FAKE_STDERR_TEXT"] = "/proc unreadable\n"
      with pytest.raises(transport.ProbeExitError) as excinfo:
         _run(transport.run_remote_probe(
            ssh_binary=fake_probe,
            ssh_config_path=config_path,
            connect_timeout_sec=8,
            hostname="polaris-login-01.head",
            probe_python="/usr/bin/python3.11",
            probe_script_source=b"# src\n",
            loop="census",
            probe_max_seconds=20,
            hard_timeout_sec=30,
            expected_probe_version=4,
            base_env=env,
         ))
      assert excinfo.value.exit_code == 3

   def test_connect_timeout_must_be_strictly_less_than_hard_timeout(
         self, fake_probe, tmp_path):
      """PLANNING.md 4.3: ConnectTimeout must be strictly smaller than
      the outer subprocess timeout, or ssh's own alarm can fire after
      we already SIGKILLed it, leaving a stale mux socket. Enforced as
      a startup assertion, not merely documented.
      """
      config_path = self._config(tmp_path)
      with pytest.raises(ValueError):
         _run(transport.run_remote_probe(
            ssh_binary=fake_probe,
            ssh_config_path=config_path,
            connect_timeout_sec=30,
            hostname="polaris-login-01.head",
            probe_python="/usr/bin/python3.11",
            probe_script_source=b"# src\n",
            loop="census",
            probe_max_seconds=20,
            hard_timeout_sec=30,
            expected_probe_version=4,
         ))

   def test_stderr_bounded_on_remote(self, fake_probe, tmp_path):
      config_path = self._config(tmp_path)
      env = dict(os.environ)
      env["FAKE_STDERR_TEXT"] = "y" * 100000
      result = _run(transport.run_remote_probe(
         ssh_binary=fake_probe,
         ssh_config_path=config_path,
         connect_timeout_sec=8,
         hostname="polaris-login-01.head",
         probe_python="/usr/bin/python3.11",
         probe_script_source=b"# src\n",
         loop="census",
         probe_max_seconds=20,
         hard_timeout_sec=30,
         expected_probe_version=4,
         base_env=env,
         stderr_limit_bytes=512,
      ))
      assert len(result.stderr) <= 512
      assert result.stderr_truncated is True

   def test_early_ssh_exit_with_oversized_stdin_raises_typed_ssh_error(
         self, fake_probe, tmp_path):
      """Review round 1 finding 1: when ssh exits early (e.g. exit 255,
      auth failure) before the daemon finishes writing probe source to
      its stdin pipe, writing must tolerate the child's early closure
      (BrokenPipeError/ConnectionResetError) instead of leaking an
      untyped exception, and the call must still classify the completed
      ssh exit through the normal SSHAuthError/SSHTransportError path.
      The fake ignores stdin entirely and exits immediately, so a probe
      source larger than the OS pipe buffer (well over 64KiB) is certain
      to still be in flight when the child closes its end.
      """
      config_path = self._config(tmp_path)
      env = dict(os.environ)
      env["FAKE_EXIT_CODE"] = "255"
      env["FAKE_STDERR_TEXT"] = "Permission denied (publickey)\n"
      oversized_source = b"x" * 2_000_000
      with pytest.raises(transport.SSHAuthError) as excinfo:
         _run(transport.run_remote_probe(
            ssh_binary=fake_probe,
            ssh_config_path=config_path,
            connect_timeout_sec=8,
            hostname="polaris-login-01.head",
            probe_python="/usr/bin/python3.11",
            probe_script_source=oversized_source,
            loop="census",
            probe_max_seconds=20,
            hard_timeout_sec=30,
            expected_probe_version=4,
            base_env=env,
         ))
      assert excinfo.value.failure_type == "ssh_auth"


# --------------------------------------------------------------------------
# Hard timeout: terminate/kill/reap of process groups, no orphan descendants
# --------------------------------------------------------------------------

class TestTimeoutAndProcessGroupCleanup:
   def test_timeout_raises_probe_timeout_error(self, fake_probe, probe_script_path):
      env = dict(os.environ)
      env["FAKE_SLEEP_SECONDS"] = "10"
      start = time.monotonic()
      with pytest.raises(transport.ProbeTimeoutError):
         _run(transport.run_local_probe(
            probe_python=fake_probe,
            probe_script_path=probe_script_path,
            loop="census",
            probe_max_seconds=30,
            hard_timeout_sec=0.3,
            expected_probe_version=4,
            base_env=env,
            grace_sec=0.2,
         ))
      elapsed = time.monotonic() - start
      # Must not have waited anywhere near the fake's 10s sleep.
      assert elapsed < 5

   def test_timeout_kills_descendant_no_orphan(self, fake_probe, tmp_path):
      """The fake probe forks its OWN child (FAKE_SPAWN_MARKER_FILE) that
      sleeps far longer than the timeout. A correct implementation kills
      the whole process GROUP, so that grandchild must be dead and
      reaped shortly after the timeout fires -- an orphan would still
      show up as a running PID.
      """
      marker = tmp_path / "child_pid.txt"
      env = dict(os.environ)
      env["FAKE_SPAWN_MARKER_FILE"] = str(marker)
      env["FAKE_SPAWN_SLEEP_SECONDS"] = "30"
      env["FAKE_SLEEP_SECONDS"] = "10"
      probe_script_path = str(tmp_path / "remote_probe.py")
      with open(probe_script_path, "w") as handle:
         handle.write("# stand-in\n")

      # hard_timeout_sec must give the fake enough real wall time to
      # actually fork its grandchild and write the marker file before
      # the timeout fires -- a too-tight timeout would make this test
      # flaky by racing process startup, not by testing cleanup.
      with pytest.raises(transport.ProbeTimeoutError):
         _run(transport.run_local_probe(
            probe_python=fake_probe,
            probe_script_path=probe_script_path,
            loop="census",
            probe_max_seconds=30,
            hard_timeout_sec=1.0,
            expected_probe_version=4,
            base_env=env,
            grace_sec=0.3,
         ))

      # Give the OS a brief moment to finish reaping/signal delivery.
      deadline = time.monotonic() + 3
      child_pid = None
      while time.monotonic() < deadline:
         if marker.exists():
            content = marker.read_text().strip()
            if content:
               child_pid = int(content)
               break
         time.sleep(0.05)
      assert child_pid is not None, "grandchild never started"

      time.sleep(0.3)
      with pytest.raises(ProcessLookupError):
         os.kill(child_pid, 0)

   def test_ignores_sigterm_gets_sigkilled(self, fake_probe, probe_script_path):
      env = dict(os.environ)
      env["FAKE_IGNORE_SIGTERM"] = "1"
      env["FAKE_SLEEP_SECONDS"] = "10"
      start = time.monotonic()
      with pytest.raises(transport.ProbeTimeoutError):
         _run(transport.run_local_probe(
            probe_python=fake_probe,
            probe_script_path=probe_script_path,
            loop="census",
            probe_max_seconds=30,
            hard_timeout_sec=0.3,
            expected_probe_version=4,
            base_env=env,
            grace_sec=0.3,
         ))
      elapsed = time.monotonic() - start
      # Bounded by hard_timeout_sec + grace_sec, not the fake's 10s sleep.
      assert elapsed < 5

   def test_cancellation_kills_process_group_no_orphan(
         self, fake_probe, tmp_path):
      """Review round 1 finding 2: transport.py owns the subprocess/
      process group and must clean it up when the AWAITING COROUTINE is
      cancelled (e.g. the scheduler cancels a slow poll's task at its
      next deadline), not only on its own internal asyncio.TimeoutError.
      The fake forks a long-lived grandchild; after cancelling the
      run_local_probe task, that grandchild must be dead -- an orphan
      would still answer os.kill(pid, 0).
      """
      marker = tmp_path / "cancel_child_pid.txt"
      env = dict(os.environ)
      env["FAKE_SPAWN_MARKER_FILE"] = str(marker)
      env["FAKE_SPAWN_SLEEP_SECONDS"] = "30"
      env["FAKE_SLEEP_SECONDS"] = "30"
      probe_script_path = str(tmp_path / "remote_probe.py")
      with open(probe_script_path, "w") as handle:
         handle.write("# stand-in\n")

      async def _drive():
         task = asyncio.ensure_future(transport.run_local_probe(
            probe_python=fake_probe,
            probe_script_path=probe_script_path,
            loop="census",
            probe_max_seconds=30,
            hard_timeout_sec=30,
            expected_probe_version=4,
            base_env=env,
            grace_sec=0.3,
         ))
         deadline = time.monotonic() + 5
         while time.monotonic() < deadline and not marker.exists():
            await asyncio.sleep(0.05)
         assert marker.exists(), "grandchild never started"
         task.cancel()
         with pytest.raises(asyncio.CancelledError):
            await task

      _run(_drive())

      content = marker.read_text().strip()
      assert content, "grandchild never wrote its pid"
      child_pid = int(content)
      time.sleep(0.5)
      with pytest.raises(ProcessLookupError):
         os.kill(child_pid, 0)

   def test_non_cancellation_collection_failure_still_reaps_process(
         self, fake_probe, tmp_path):
      """Review round 2: cleanup must not be limited to
      asyncio.TimeoutError/CancelledError. ANY exception raised while
      _collect() owns the live subprocess must still terminate/kill/
      reap the owned process group before propagating. A non-bytes
      probe_script_source makes the real asyncio StreamWriter.write()
      raise AssertionError synchronously inside _feed_stdin -- well
      before proc.wait() is ever reached -- which is exactly the
      "another collection-path exception aborts the await" case. The
      fake is made to sleep, so an unreaped child is trivially
      distinguishable from a reaped one via proc.returncode/os.kill.
      """
      control_dir = tmp_path / "ssh-control"
      config_path = tmp_path / "ssh_config"
      transport.write_ssh_config(str(config_path), str(control_dir), 8)
      env = dict(os.environ)
      env["FAKE_SLEEP_SECONDS"] = "30"
      captured = {}
      real_exec = asyncio.create_subprocess_exec

      async def _capturing_exec(*args, **kwargs):
         proc = await real_exec(*args, **kwargs)
         captured["proc"] = proc
         return proc

      with pytest.raises(AssertionError):
         _run(transport.run_remote_probe(
            ssh_binary=fake_probe,
            ssh_config_path=str(config_path),
            connect_timeout_sec=8,
            hostname="polaris-login-01.head",
            probe_python="/usr/bin/python3.11",
            probe_script_source="not-bytes",
            loop="census",
            probe_max_seconds=20,
            hard_timeout_sec=30,
            expected_probe_version=4,
            base_env=env,
            subprocess_exec=_capturing_exec,
         ))

      proc = captured["proc"]
      deadline = time.monotonic() + 3
      while time.monotonic() < deadline and proc.returncode is None:
         time.sleep(0.05)
      assert proc.returncode is not None, "child was never reaped"
      with pytest.raises(ProcessLookupError):
         os.kill(proc.pid, 0)


# --------------------------------------------------------------------------
# validate_probe_payload -- pure function, direct unit tests
# --------------------------------------------------------------------------

class TestValidateProbePayload:
   def test_valid_payload_passes_through(self):
      payload = {"probe_version": 4, "loop": "census",
                 "hostname_fqdn": "a.example.org"}
      assert transport.validate_probe_payload(payload, "census", 4) is payload

   def test_non_dict_payload_raises(self):
      with pytest.raises(transport.InvariantViolationError):
         transport.validate_probe_payload([1, 2, 3], "census", 4)

   def test_version_mismatch(self):
      payload = {"probe_version": 3, "loop": "census",
                 "hostname_fqdn": "a.example.org"}
      with pytest.raises(transport.ProbeVersionMismatchError):
         transport.validate_probe_payload(payload, "census", 4)

   def test_loop_mismatch(self):
      payload = {"probe_version": 4, "loop": "counter",
                 "hostname_fqdn": "a.example.org"}
      with pytest.raises(transport.InvariantViolationError):
         transport.validate_probe_payload(payload, "census", 4)

   def test_fqdn_mismatch_only_checked_when_expected_given(self):
      payload = {"probe_version": 4, "loop": "census",
                 "hostname_fqdn": "any-host.example.org"}
      # No expected_fqdn -- must not raise.
      transport.validate_probe_payload(payload, "census", 4)

   def test_fqdn_mismatch_raises_when_expected_given(self):
      payload = {"probe_version": 4, "loop": "census",
                 "hostname_fqdn": "wrong.example.org"}
      with pytest.raises(transport.HostnameMismatchError):
         transport.validate_probe_payload(
            payload, "census", 4, expected_fqdn="right.example.org")

   def test_missing_fqdn_rejected_even_without_expected_fqdn(self):
      """Design: 'Each successful payload must have ... a remote-reported
      FQDN.' This must hold independent of whether the caller happens to
      know the exact hostname it expects.
      """
      payload = {"probe_version": 4, "loop": "census"}
      with pytest.raises(transport.InvariantViolationError):
         transport.validate_probe_payload(payload, "census", 4)

   def test_empty_fqdn_rejected_even_without_expected_fqdn(self):
      payload = {"probe_version": 4, "loop": "census", "hostname_fqdn": ""}
      with pytest.raises(transport.InvariantViolationError):
         transport.validate_probe_payload(payload, "census", 4)

   def test_non_string_fqdn_rejected_even_without_expected_fqdn(self):
      payload = {"probe_version": 4, "loop": "census", "hostname_fqdn": 123}
      with pytest.raises(transport.InvariantViolationError):
         transport.validate_probe_payload(payload, "census", 4)
