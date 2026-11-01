"""Project-owned ssh configuration and argv construction."""

import os
from pathlib import Path

from node_monitor.config import SshConfig


def write_ssh_config(state_dir: Path, ssh: SshConfig) -> Path:
   """Write `state_dir/ssh_config` owner-only and return its path.

   ssh runs whatever ProxyCommand this file names, so neither the file nor the directory
   holding it may be writable by anyone else: the directory is tightened even if it already
   exists, and the file is replaced, never opened in place, so a planted file or symlink is
   discarded rather than written through.
   """
   state_dir = Path(state_dir)
   state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
   # mkdir's mode is filtered by the umask and ignored for an existing directory.
   os.chmod(state_dir, 0o700)
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
   tmp = state_dir / f".ssh_config.{os.getpid()}.tmp"
   tmp.unlink(missing_ok=True)
   fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
   try:
      with os.fdopen(fd, "w") as f:
         os.fchmod(f.fileno(), 0o600)
         f.write("\n".join(lines) + "\n")
      os.replace(tmp, path)
   except BaseException:
      tmp.unlink(missing_ok=True)
      raise
   return path


def ssh_argv(ssh_binary: str, ssh_config_path: Path, target: str, command: str) -> list[str]:
   return [ssh_binary, "-F", str(ssh_config_path), "-o", "BatchMode=yes", target, command]
