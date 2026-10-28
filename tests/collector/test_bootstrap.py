import json
import os
import shlex
import stat
import subprocess
import sys
from pathlib import Path

from node_monitor.collector.bootstrap import (
   BOOTSTRAP_CODE,
   build_payload,
   probe_config,
   probe_source,
   remote_command,
)
from node_monitor.collector.ssh import ssh_argv, write_ssh_config
from node_monitor.config import SshConfig
from tests.probe import fake_proc


def _env_without_preload():
   return {k: v for k, v in os.environ.items() if k != "LD_PRELOAD"}


def test_bootstrap_runs_probe_end_to_end(tmp_path):
   roots = fake_proc.write_tree(
      tmp_path / "t",
      pids=[{"pid": 4242, "comm": "python", "cmdline": ["python", "x.py"]}],
   )
   cfg = {
      "counter_interval_sec": 0.2,
      "census_interval_sec": 0.4,
      "active_cpu_seconds_per_min": 0.1,
      "gpu": False,
      "lock_path": str(tmp_path / "lock"),
      "proc_root": roots["proc"],
      "sys_root": roots["sys"],
   }
   p = subprocess.Popen(
      [sys.executable, "-u", "-c", BOOTSTRAP_CODE],
      stdin=subprocess.PIPE,
      stdout=subprocess.PIPE,
      env=_env_without_preload(),
      cwd=str(tmp_path),
   )
   p.stdin.write(build_payload(probe_source(), cfg))
   p.stdin.flush()
   first = json.loads(p.stdout.readline())
   assert first["type"] == "hello"
   p.stdin.close()
   assert p.wait(timeout=3) == 0
   p.stdout.close()


def test_payload_length_counts_bytes_not_chars():
   payload = build_payload("x = 'é'\n", {})
   header, body = payload.split(b"\n", 1)
   assert json.loads(header)["len"] == len(body) == len("x = 'é'\n".encode())


def test_remote_command_round_trips_through_a_shell():
   argv = shlex.split(remote_command("/usr/bin/python3.11"))
   assert argv == ["env", "-u", "LD_PRELOAD", "/usr/bin/python3.11", "-u", "-c", BOOTSTRAP_CODE]


def test_probe_config_takes_intervals_from_config():
   from node_monitor.config import ClassificationConfig, CollectionConfig, Config

   cfg = Config(
      system="s",
      nodes=(),
      probe_python="python3",
      working_dir=Path("."),
      ssh=SshConfig(),
      collection=CollectionConfig(counter_interval_sec=3, census_interval_sec=7),
      classification=ClassificationConfig(active_cpu_seconds_per_min=0.5),
      source_path=Path("c.yaml"),
   )
   assert probe_config(cfg) == {
      "counter_interval_sec": 3,
      "census_interval_sec": 7,
      "active_cpu_seconds_per_min": 0.5,
      "gpu": True,
   }


def test_ssh_config_contents_and_modes(tmp_path):
   path = write_ssh_config(
      tmp_path, SshConfig(identity_file="/home/u/.ssh/k", connect_timeout_sec=10)
   )
   text = path.read_text()
   for line in (
      "Host *",
      "BatchMode yes",
      "ControlMaster auto",
      "ControlPersist 60s",
      f"ControlPath {tmp_path}/ssh/cm-%C",
      "ServerAliveInterval 10",
      "ServerAliveCountMax 3",
      "ConnectTimeout 10",
      "IdentityFile /home/u/.ssh/k",
      "IdentitiesOnly yes",
   ):
      assert line in text
   assert stat.S_IMODE(path.stat().st_mode) == 0o600
   assert stat.S_IMODE((tmp_path / "ssh").stat().st_mode) == 0o700


def test_ssh_config_without_identity_omits_identities_only(tmp_path):
   text = write_ssh_config(tmp_path, SshConfig()).read_text()
   assert "IdentitiesOnly" not in text
   assert "IdentityFile" not in text


def test_ssh_argv_shape():
   argv = ssh_argv("ssh", Path("/s/ssh_config"), "host.x", "echo hi")
   assert argv == ["ssh", "-F", "/s/ssh_config", "-o", "BatchMode=yes", "host.x", "echo hi"]
