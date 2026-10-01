"""Linux integration tests for the detached daemon lifecycle."""

import json
import os
import pathlib
import subprocess
import sys
import time

import pytest
import yaml


pytestmark = pytest.mark.skipif(
   not sys.platform.startswith("linux"),
   reason="detached daemon process identity requires Linux /proc",
)


def _wait_for(predicate, timeout=10.0):
   deadline = time.monotonic() + timeout
   while time.monotonic() < deadline:
      value = predicate()
      if value:
         return value
      time.sleep(0.05)
   raise AssertionError("condition not reached before timeout")


def _state(path):
   try:
      return json.loads(path.read_text())
   except (FileNotFoundError, json.JSONDecodeError):
      return None


def test_detached_start_ack_status_stop_and_exit(tmp_path):
   """Exercise the real fork, setsid, ACK, watcher, and child finalizer."""
   home = tmp_path / "home"
   home.mkdir()
   probe = tmp_path / "probe.py"
   probe.write_text(
      "import json, sys\n"
      "if '--version' in sys.argv:\n"
      "   print(4)\n"
      "else:\n"
      "   print(json.dumps({'probe_version': 4}))\n"
   )
   versioned_python = pathlib.Path(sys.executable).with_name(
      "python%d.%d" % (sys.version_info.major, sys.version_info.minor))
   assert versioned_python.exists()
   config = tmp_path / "config.yaml"
   config.write_text(yaml.safe_dump({
      "system": "polaris",
      "nodes": [{"hostname": "localhost", "role": "local"}],
      "probe_python": str(versioned_python),
      "output": {"root": str(home / "runs")},
      "collection": {"duration_sec": 60},
      "ssh": {},
      "safety": {},
      "database": {"url": "postgresql:///unreachable_by_test"},
      "retention": {},
   }))
   control_path = home / ".node_monitor_daemon.pid"
   env = dict(os.environ)
   env["HOME"] = str(home)
   env["NODE_MONITOR_REMOTE_PROBE_PATH"] = str(probe)

   # This integration seam replaces only preflight/runtime internals in the
   # forked child. The real CLI still performs fork/setsid/FD redirection,
   # state ACK, status, stop request, watcher, and final state handling.
   helper = tmp_path / "detached_driver.py"
   helper.write_text(
      "import asyncio, sys\n"
      "import node_monitor.cli.main as m\n"
      "class D:\n"
      "   def __init__(self): self.stopped = False\n"
      "   def request_stop(self): self.stopped = True\n"
      "def runtime(nested, config_path, run_id, probe_version, home, "
      "phase0_config=None, control_file=None, startup_ack=None):\n"
      "   run_dir = home + '/runs/' + run_id\n"
      "   startup_ack(run_dir)\n"
      "   d = D()\n"
      "   asyncio.run(m._watch_control_file(control_file, d, "
      "poll_interval=0.02, heartbeat_interval=0.05))\n"
      "   return 0\n"
      "m._run_daemon_postgres = runtime\n"
      "m._resolve_probe_version = lambda unused: 4\n"
      "m.cli.main(args=sys.argv[1:], standalone_mode=True)\n"
   )

   start = subprocess.run(
      [sys.executable, str(helper), "daemon", "start", "--config",
       str(config), "--home", str(home), "--run-id", "integration"],
      env=env, text=True, capture_output=True, timeout=10)
   assert start.returncode == 0, start.stderr
   assert "Daemon started in background" in start.stdout

   running = _wait_for(lambda: _state(control_path))
   assert running["exited"] is False
   assert running["stop_requested"] is False
   assert running["process_start_ticks"] > 0

   status = subprocess.run(
      [sys.executable, str(helper), "daemon", "status"], env=env,
      text=True, capture_output=True, timeout=10)
   assert status.returncode == 0
   assert "Status: Running" in status.stdout

   stop = subprocess.run(
      [sys.executable, str(helper), "daemon", "stop"], env=env,
      text=True, capture_output=True, timeout=10)
   assert stop.returncode == 0
   assert "Stop requested" in stop.stdout

   exited = _wait_for(
      lambda: (value if (value := _state(control_path)) and value["exited"]
               else None))
   assert exited["stop_requested"] is True
   assert exited["outcome"] == "partial"
   assert exited["exit_code"] == 0
