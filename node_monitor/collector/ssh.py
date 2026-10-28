"""Project-owned ssh configuration and argv construction."""

import os
from pathlib import Path

from node_monitor.config import SshConfig


def write_ssh_config(state_dir: Path, ssh: SshConfig) -> Path:
   state_dir = Path(state_dir)
   sock_dir = state_dir / "ssh"
   sock_dir.mkdir(parents=True, exist_ok=True)
   os.chmod(sock_dir, 0o700)
   lines = [
      "Host *",
      "   BatchMode yes",
      "   ControlMaster auto",
      "   ControlPersist 60s",
      f"   ControlPath {sock_dir}/cm-%C",
      f"   ConnectTimeout {ssh.connect_timeout_sec:g}",
      "   ServerAliveInterval 10",
      "   ServerAliveCountMax 3",
   ]
   if ssh.identity_file:
      lines += [f"   IdentityFile {ssh.identity_file}", "   IdentitiesOnly yes"]
   path = state_dir / "ssh_config"
   fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
   with os.fdopen(fd, "w") as f:
      f.write("\n".join(lines) + "\n")
   os.chmod(path, 0o600)
   return path


def ssh_argv(ssh_binary: str, ssh_config_path: Path, target: str, command: str) -> list[str]:
   return [ssh_binary, "-F", str(ssh_config_path), "-o", "BatchMode=yes", target, command]
