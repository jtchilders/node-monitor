"""The running code's version string, with the git revision when run from a checkout."""

import subprocess
from pathlib import Path

from node_monitor import __version__

_PACKAGE_DIR = Path(__file__).resolve().parent


def _git(args: list[str], cwd: Path) -> str | None:
   try:
      out = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, timeout=5)
   except (OSError, subprocess.SubprocessError):
      return None
   return out.stdout.strip() if out.returncode == 0 and out.stdout.strip() else None


def _git_describe(cwd: Path = _PACKAGE_DIR) -> str | None:
   # "-dirty" marks uncommitted changes to tracked files: the code that ran is not HEAD.
   return _git(["describe", "--always", "--dirty"], cwd)


def describe(cwd: Path = _PACKAGE_DIR) -> str:
   revision = _git_describe(cwd)
   return f"{__version__}+{revision}" if revision else __version__
