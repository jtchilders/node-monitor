#!/usr/bin/env python3
"""Remote probe: one gather pass on a login node, one JSON line on stdout.

This file is the ONLY thing that runs on a monitored node. It is delivered
over stdin (`ssh <node> <probe_python> - < remote_probe.py`), never staged to
the filesystem, so it must be self-contained and standard-library only.

Design constraints (see PLANNING.md sections 4.2, 4.2a, 14.2):

* **It gathers; it does not think.** No aggregation, no rate computation, no
  state, no files, no sockets, no writes. Cumulative counters are emitted raw
  and the daemon deltas them. A probe that remembers anything between runs
  would have to be correct across node reboots, PID reuse and counter wraps
  on every node independently; the daemon does it once.
* **Standard library only, Python >= 3.9.** No pip install ever happens on a
  login node.
* **It never raises for an unreadable path.** Other users' processes deny
  most of /proc; that is the normal case, not an error. Missing data is
  emitted as null and counted, so coverage is visible in the payload rather
  than silently degrading.
* **Exit codes** (PLANNING.md 14.2): 0 ok, 2 bad args, 3 /proc unreadable,
  4 self-abort on timeout.

TEST-ONLY: the `NODE_MONITOR_PROC_ROOT` environment variable overrides the
proc filesystem root (see `PROC_ROOT` below). It exists so the subprocess
tests can point a real invocation of this file at a synthetic tree on a
macOS dev machine that has no `/proc`. Production never sets it; the daemon
never passes it. Do not use it as a CLI flag -- the daemon builds the ssh
command line, and a flag risks being set by accident in production, whereas
an env var reads as obviously test-only.

Privacy: the node is a shared ALCF login node with no expectation of user
privacy (PLANNING.md 5.6), and real usernames are retained deliberately. Raw
argv is nonetheless dropped by default (`--drop-raw-args`, PLANNING.md
decision 7) because full command lines carry incidental secrets -- tokens
passed as flags, private paths -- that the project has no use for. What
leaves the node is the classification, not the command line.
"""

import errno
import json
import os
import re
import signal
import sys
import time

PROBE_VERSION = 4

# Version 4 drops the Python 3.6 floor that versions <= 3 were written
# against. PLANNING.md decision 10: the 3.6 default on Polaris/Crux is only a
# $PATH default; 3.11/3.12/3.13 are present, and `probe_python` names an
# interpreter explicitly per system. `time.monotonic_ns` and friends are
# therefore safe now, but nothing here needs them.

MIN_PYTHON = (3, 9)

# Root of the proc filesystem. Overridable ONLY so the test suite can point
# the probe at a synthetic tree; nothing in production ever changes it. The
# alternative -- threading a root argument through every reader -- would add
# a parameter to functions whose signatures are otherwise self-documenting,
# to serve a need that exists only in tests.
#
# Read once at import time from NODE_MONITOR_PROC_ROOT so that the in-process
# tests (which monkeypatch this module attribute directly) and the subprocess
# tests (which cannot reach a module attribute, only the process env) share
# one override mechanism. Unset in production -- the daemon never sets this
# var -- so PROC_ROOT is "/proc" on every real login node.
PROC_ROOT = os.environ.get("NODE_MONITOR_PROC_ROOT", "/proc")


def _proc(*parts):
   return os.path.join(PROC_ROOT, *parts)


# Root of the sysfs tree. Same rationale and override mechanism as PROC_ROOT
# above -- the hwinfo loop needs a handful of /sys facts (cpufreq, NUMA node
# count, per-interface link speed) that have no /proc equivalent, and this
# lets the subprocess tests point both roots at one synthetic tree on macOS.
# Unset in production, so SYS_ROOT is "/sys" on every real login node.
SYS_ROOT = os.environ.get("NODE_MONITOR_SYS_ROOT", "/sys")


def _sys(*parts):
   return os.path.join(SYS_ROOT, *parts)


# /etc/os-release is neither under /proc nor /sys, so it has no ROOT-style
# test override -- it is a fixed OS file, not something the daemon's process
# view depends on. Kept as a module attribute (not inlined) purely so
# in-process tests can monkeypatch it the same way they monkeypatch
# PROC_ROOT.
OS_RELEASE_PATH = "/etc/os-release"

# --------------------------------------------------------------------------
# Classification
#
# Applied ON THE NODE so raw argv never leaves it. Resolution is explicit
# highest-priority-wins: each rule carries a numeric `priority` as its third
# tuple element, and `_resolve_rule` picks the highest-priority match, not
# the first one encountered in source order. This is deliberate -- a broad
# rule (e.g. "ide-remote"/"vscode-remote-server" matching any vscode-server
# path) and a narrow rule (e.g. "ai-coding-agent"/"claude-code" matching a
# Claude Code VS Code extension path under that same vscode-server tree)
# can both match the same cmdline, and the narrow rule must win regardless
# of which one happens to be written first or reordered later. Every regex
# is anchored against the process name and the full command line
# separately -- a bare `claude` and `/soft/.../bin/claude --resume` must
# both classify.
# --------------------------------------------------------------------------

_CATEGORY_RULES = [
   ("ide-remote", re.compile(
      r"vscode-server|vscode_server|\.vscode-server|cursor-server|"
      r"\.cursor-server|jetbrains|remote-dev-server|code-server"), 10),
   # Priority 20 (> ide-remote's 10): a Claude Code / Codex marker must beat
   # a broad IDE-path marker even when both appear in the same cmdline, e.g.
   # .vscode-server/extensions/anthropic.claude-code-*/bin/claude. Measured
   # live-Polaris impact of getting this wrong (2026-09-04): ~24% undercount
   # of AI-agent processes, 9 distinct users reported instead of 12.
   ("ai-coding-agent", re.compile(
      r"(^|/)(claude|codex|opencode|aider|gemini|cline|goose)( |$)|"
      r"-m\s+(claude|codex|opencode|aider|gemini|cline|goose)[_.\s]|"
      r"anthropic\.claude-code|openai\.chatgpt|claude-code|claude_code|"
      r"copilot-language"), 20),
   ("jupyter", re.compile(r"jupyter|ipykernel|jupyter-lab|jupyterhub"), 10),
   ("pbs-client", re.compile(
      r"(^|/)(qstat|qsub|qdel|qhold|qrls|qalter|pbsnodes|pbs_[a-z]+|"
      r"mpiexec|mpirun|aprun|srun)( |$)"), 10),
   ("data-xfer/vcs", re.compile(
      r"(^|/)(git|git-[a-z-]+|rsync|scp|sftp|globus|globus-url-copy|curl|"
      r"wget|tar|gzip|gunzip|bzip2|xz|zip|unzip|hsi|htar)( |$)"), 10),
   # --- The four rules below were unspecified in the plan (finding B-2).
   # --- They are first drafts and MUST be validated against the captured
   # --- cmdline corpus before any published number depends on them.
   ("fs-scan", re.compile(
      r"(^|/)(find|du|ncdu|updatedb|locate|stat|ls)( |$)|"
      r"(^|/)lfs( |$).*(find|df)|(^|/)mlocate"), 10),
   ("compute/build", re.compile(
      r"(^|/)(make|gmake|cmake|ninja|gcc|g\+\+|cc|c\+\+|clang|clang\+\+|"
      r"nvcc|ftn|CC|ld|ar|ranlib|cargo|rustc|go|javac|configure|conda|pip|"
      r"pip3|spack|python|python[0-9.]*)( |$)"), 10),
   ("shell/session", re.compile(
      r"(^|/)(sshd|bash|zsh|tcsh|csh|ksh|sh|screen|tmux|systemd|"
      r"\(sd-pam\)|dbus-daemon|dbus-launch|login|su|sudo)( |:|$)|^-"), 10),
]

_DEFAULT_CATEGORY = "other"

# Behavior: what shape of work this is, independent of which tool it is.
_BEHAVIOR_INTERACTIVE = "interactive"
_BEHAVIOR_BATCH = "batch"
_BEHAVIOR_DAEMON = "daemon"

_ACTIVITY_RULES = [
   ("vscode-remote-server", re.compile(r"vscode-server|code-server"), 10),
   ("cursor-remote-server", re.compile(r"cursor-server"), 10),
   # Priority 20 (> vscode-remote-server's 10): same overlapping-extension-
   # path hazard as _CATEGORY_RULES above -- claude-code/codex-cli must beat
   # vscode-remote-server for a Claude Code / Codex VS Code extension path.
   ("claude-code", re.compile(
      r"claude-code|anthropic\.claude-code|(^|/)claude( |$)"), 20),
   ("codex-cli", re.compile(r"(^|/)codex( |$)|openai\.chatgpt"), 20),
   ("jupyter-kernel", re.compile(r"ipykernel"), 10),
   ("jupyter-server", re.compile(r"jupyter"), 10),
   ("torch-distributed", re.compile(r"torch\.distributed|torchrun"), 10),
   ("mpi-launch", re.compile(r"(^|/)(mpiexec|mpirun|aprun|srun)( |$)"), 10),
   ("compiler-invocation", re.compile(
      r"(^|/)(gcc|g\+\+|clang|nvcc|ftn|cc|c\+\+)( |$)"), 10),
   ("build-driver", re.compile(r"(^|/)(make|gmake|cmake|ninja)( |$)"), 10),
   ("package-install", re.compile(r"(^|/)(pip|pip3|conda|spack)( |$)"), 10),
   ("git-operation", re.compile(r"(^|/)git( |$)|(^|/)git-[a-z-]+( |$)"), 10),
   ("data-transfer", re.compile(
      r"(^|/)(rsync|scp|sftp|globus|globus-url-copy|curl|wget)( |$)"), 10),
   ("filesystem-scan", re.compile(r"(^|/)(find|du|ncdu|updatedb)( |$)"), 10),
   ("pbs-query", re.compile(r"(^|/)(qstat|qsub|qdel|pbsnodes)( |$)"), 10),
   ("shell", re.compile(r"(^|/)(bash|zsh|tcsh|csh|ksh|sh)( |$)|^-"), 10),
   ("ssh-session", re.compile(r"(^|/)sshd( |:|$)"), 10),
   ("terminal-multiplexer", re.compile(r"(^|/)(screen|tmux)( |$)"), 10),
]

# Paths that qualify a process to a project with high confidence.
_PROJECT_PATH_RE = re.compile(r"/(?:lus|eagle|grand|flare)/[^/]*/?projects?/([^/]+)/")

_CONF_PATH = "path_qualified"
_CONF_ARGV = "argv_heuristic"
_CONF_UNKNOWN = "unknown"


class ProbeTimeout(Exception):
   """Raised by the SIGALRM handler when the probe exceeds its budget."""


def _on_alarm(signum, frame):
   raise ProbeTimeout()


# --------------------------------------------------------------------------
# Low-level readers. Every one of these returns a sentinel instead of raising
# for the permission and race errors that are routine on a busy node.
# --------------------------------------------------------------------------

def _read_text(path):
   """Read a /proc file. Returns None if unreadable or the process vanished.

   EACCES/EPERM: another user's process -- expected, not an error.
   ENOENT/ESRCH: the process exited between listdir and read -- expected on a
   node forking thousands of short-lived processes.
   EINVAL/EIO: some /proc files reject reads in certain kernel states.
   """
   try:
      with open(path, "rb") as handle:
         return handle.read().decode("utf-8", "replace")
   except (IOError, OSError) as exc:
      if exc.errno in (errno.EACCES, errno.EPERM, errno.ENOENT, errno.ESRCH,
                       errno.EINVAL, errno.EIO, errno.ENXIO):
         return None
      raise


def _read_first_line(path):
   text = _read_text(path)
   if text is None:
      return None
   return text.split("\n", 1)[0]


# --------------------------------------------------------------------------
# Node-level counters (both loops)
# --------------------------------------------------------------------------

def _collect_loadavg():
   line = _read_first_line(_proc("loadavg"))
   if not line:
      return {}, None
   parts = line.split()
   if len(parts) < 4:
      return {}, None
   running, _, total = parts[3].partition("/")
   out = {
      "load1": float(parts[0]),
      "load5": float(parts[1]),
      "load15": float(parts[2]),
      "procs_running": int(running),
   }
   return out, int(total) if total.isdigit() else None


def _collect_cpu_jiffies():
   line = _read_first_line(_proc("stat"))
   if not line or not line.startswith("cpu "):
      return None
   fields = line.split()[1:]
   names = ["user", "nice", "system", "idle", "iowait", "irq", "softirq",
            "steal", "guest", "guest_nice"]
   out = {}
   for name, value in zip(names, fields):
      try:
         out[name] = int(value)
      except ValueError:
         continue
   return out


def _collect_meminfo():
   text = _read_text(_proc("meminfo"))
   if text is None:
      return {}
   wanted = {
      "MemTotal": "total_kb",
      "MemAvailable": "available_kb",
      "Cached": "cached_kb",
      "Shmem": "shmem_kb",
      "MemFree": "free_kb",
      "SwapTotal": "swap_total_kb",
      "SwapFree": "swap_free_kb",
   }
   out = {}
   for line in text.split("\n"):
      key, _, rest = line.partition(":")
      if key in wanted:
         value = rest.split()
         if value and value[0].isdigit():
            out[wanted[key]] = int(value[0])
   return out


def _collect_net():
   """Cumulative rx/tx bytes for non-loopback interfaces.

   Emitted per interface rather than summed: the daemon decides which
   interface matters, and summing here would hide an interface flapping.
   """
   text = _read_text(_proc("net", "dev"))
   if text is None:
      return {}
   out = {}
   for line in text.split("\n")[2:]:
      name, _, rest = line.partition(":")
      name = name.strip()
      if not name or name == "lo":
         continue
      fields = rest.split()
      if len(fields) < 9:
         continue
      try:
         out[name] = {"rx_bytes": int(fields[0]), "tx_bytes": int(fields[8])}
      except ValueError:
         continue
   return out


def _collect_lustre_md():
   """Lustre client metadata-op counters, per MDC target.

   These are the counters behind the project's central hypothesis -- that the
   login-node bottleneck is Lustre metadata, not cores (PLANNING.md decision
   18). Absent on non-Lustre systems, which is not an error.
   """
   base = _proc("fs", "lustre", "mdc")
   out = {}
   try:
      targets = os.listdir(base)
   except (IOError, OSError):
      return out
   for target in targets:
      stats = _read_text(os.path.join(base, target, "md_stats"))
      if stats is None:
         continue
      counters = {}
      total = 0
      for line in stats.split("\n"):
         fields = line.split()
         if len(fields) >= 2 and fields[1].isdigit():
            count = int(fields[1])
            counters[fields[0]] = count
            total += count
      if counters:
         counters["total"] = total
         out[target] = counters
   return out


def _collect_socket_count():
   """Total sockets from /proc/net/sockstat.

   Per-uid socket attribution would require walking every /proc/<pid>/fd,
   which is denied for other users and would multiply the probe's own
   metadata-op cost. We emit the node total and say so, rather than emitting
   a per-uid map that is silently only our own processes.
   """
   text = _read_text(_proc("net", "sockstat"))
   if text is None:
      return None
   for line in text.split("\n"):
      if line.startswith("sockets: used"):
         fields = line.split()
         if fields and fields[-1].isdigit():
            return int(fields[-1])
   return None


def _collect_node_counters():
   counters = {}
   load, procs_total = _collect_loadavg()
   counters.update(load)
   if procs_total is not None:
      counters["procs_total"] = procs_total
   cpu = _collect_cpu_jiffies()
   if cpu is not None:
      counters["cpu_jiffies"] = cpu
   counters["clk_tck"] = os.sysconf("SC_CLK_TCK")
   counters["mem"] = _collect_meminfo()
   counters["net"] = _collect_net()
   counters["md_ops"] = _collect_lustre_md()
   counters["socket_count"] = _collect_socket_count()
   return counters


# --------------------------------------------------------------------------
# Hardware inventory (hwinfo loop only)
#
# Everything here is physical-and-static, not a time series: total RAM,
# socket/core counts, kernel release. Collecting it once and storing it,
# instead of re-reading it every 60 s like the counter loop does, is the
# whole point of this loop (PLANNING.md -- see the hwinfo task write-up).
#
# One field that LOOKS static is deliberately absent: instantaneous CPU MHz
# from /proc/cpuinfo. Measured on the real fleet it is a live DVFS reading
# that differs across cores and samples taken seconds apart, so it has no
# business in a table whose contract is "read once, trust until reboot".
# `cpu_max_freq_khz` below is the nominal ceiling from sysfs, not that.
# --------------------------------------------------------------------------

def _collect_cpuinfo():
   """cpu_model / cpu_logical / sockets / cores_per_socket from /proc/cpuinfo.

   sockets and cores_per_socket come back None (not 0) when the fields are
   absent -- e.g. inside some containers -- so the daemon can tell "no
   sockets" (impossible) apart from "this platform doesn't expose it".
   """
   text = _read_text(_proc("cpuinfo"))
   out = {"cpu_model": None, "cpu_logical": 0, "sockets": None,
          "cores_per_socket": None}
   if text is None:
      return out
   physical_ids = set()
   for line in text.split("\n"):
      key, sep, rest = line.partition(":")
      if not sep:
         continue
      key = key.strip()
      value = rest.strip()
      if key == "processor":
         out["cpu_logical"] += 1
      elif key == "model name" and out["cpu_model"] is None:
         out["cpu_model"] = value
      elif key == "physical id":
         physical_ids.add(value)
      elif key == "cpu cores" and out["cores_per_socket"] is None:
         try:
            out["cores_per_socket"] = int(value)
         except ValueError:
            pass
   if physical_ids:
      out["sockets"] = len(physical_ids)
   return out


def _collect_cpu_max_freq_khz():
   """Nominal max CPU frequency from sysfs -- NOT /proc/cpuinfo MHz.

   /proc/cpuinfo's per-core MHz is an instantaneous DVFS reading (measured on
   the real fleet: three samples 2s apart on one node produced 133/110/97
   distinct values across 256 cores). cpuinfo_max_freq is the fixed hardware
   ceiling instead, which is what makes it safe to store once. Absent sysfs
   file -- e.g. some virtualized platforms -- means NULL, never a fallback
   to the live reading.
   """
   text = _read_text(_sys(
      "devices", "system", "cpu", "cpu0", "cpufreq", "cpuinfo_max_freq"))
   if text is None:
      return None
   try:
      return int(text.strip())
   except ValueError:
      return None


def _collect_numa_nodes():
   base = _sys("devices", "system", "node")
   try:
      entries = os.listdir(base)
   except (IOError, OSError):
      return 0
   pattern = re.compile(r"^node\d+$")
   return sum(1 for entry in entries if pattern.match(entry))


def _collect_hw_meminfo():
   """mem_total_kb / swap_total_kb / hugepage_size_kb from /proc/meminfo.

   Separate from _collect_meminfo (node counters) because that function's
   output shape is tied to the counter/census payload contract; conflating
   the two would make an accidental hwinfo change ripple into the counters
   the daemon deltas every 60s.
   """
   wanted = {"MemTotal": "mem_total_kb", "SwapTotal": "swap_total_kb",
             "Hugepagesize": "hugepage_size_kb"}
   out = dict.fromkeys(wanted.values())
   text = _read_text(_proc("meminfo"))
   if text is None:
      return out
   for line in text.split("\n"):
      key, _, rest = line.partition(":")
      if key in wanted:
         value = rest.split()
         if value and value[0].isdigit():
            out[wanted[key]] = int(value[0])
   return out


def _collect_kernel_release():
   return _read_first_line(_proc("sys", "kernel", "osrelease"))


def _collect_os_pretty_name():
   text = _read_text(OS_RELEASE_PATH)
   if text is None:
      return None
   for line in text.split("\n"):
      key, sep, rest = line.partition("=")
      if sep and key.strip() == "PRETTY_NAME":
         return rest.strip().strip('"')
   return None


def _collect_net_fs_mounts():
   """Count of lustre/nfs/nfs4 rows in /proc/mounts.

   Just a count, not the mount table itself: this project's central
   hypothesis is that the login-node bottleneck is network-filesystem
   metadata ops, and the count is the denominator an analyst needs, not the
   mount points (which are already known from the fleet's static config).
   """
   text = _read_text(_proc("mounts"))
   if text is None:
      return 0
   fs_types = ("lustre", "nfs", "nfs4")
   count = 0
   for line in text.split("\n"):
      fields = line.split()
      if len(fields) >= 3 and fields[2] in fs_types:
         count += 1
   return count


def _collect_net_ifaces():
   """iface -> link speed (Mb/s), or None when unreadable.

   Some interfaces report speed as "?" at the kernel/ethtool layer -- bond
   members with no active slave, unplugged NICs. That is stored as null
   here, not the literal string, so the column stays numeric.

   Measured on polaris-login-02: not every /sys/class/net entry is an
   interface directory. `bonding_masters` is a plain FILE living alongside
   the real interfaces (a bonding-driver control node, not a NIC), so it is
   skipped rather than treated as an interface with an unreadable speed --
   including it under any key would misrepresent it as a network device.

   A `speed` file can also exist and still fail to read -- `lo` and other
   down/unsupported interfaces return EINVAL, and other errno values are
   possible beyond the allowlist `_read_text()` already treats as routine.
   The broad catch below turns any such failure into null rather than a
   probe-wide crash, same as a missing file. `ProbeTimeout` is re-raised
   ahead of it -- same pattern as `_collect_gpus_nvidia_smi` at
   remote_probe.py:556 -- so the probe's own SIGALRM self-abort still wins
   over "swallow the read failure".
   """
   base = _sys("class", "net")
   try:
      names = os.listdir(base)
   except (IOError, OSError):
      return {}
   out = {}
   for name in names:
      iface_dir = os.path.join(base, name)
      if not os.path.isdir(iface_dir):
         continue
      try:
         text = _read_text(os.path.join(iface_dir, "speed"))
      except ProbeTimeout:
         raise
      except Exception:
         text = None
      if text is None:
         out[name] = None
         continue
      try:
         out[name] = int(text.strip())
      except ValueError:
         out[name] = None
   return out


def _collect_boot_id():
   return _read_first_line(_proc("sys", "kernel", "random", "boot_id"))


def _collect_btime():
   text = _read_text(_proc("stat"))
   if text is None:
      return None
   for line in text.split("\n"):
      if line.startswith("btime "):
         parts = line.split()
         if len(parts) >= 2:
            try:
               return int(parts[1])
            except ValueError:
               return None
   return None


def _collect_gpus_nvidia_smi():
   """Query nvidia-smi for GPU names. [] on any failure -- see _collect_gpus."""
   import shutil
   import subprocess as _subprocess
   binary = shutil.which("nvidia-smi")
   if binary is None:
      return []
   try:
      result = _subprocess.run(
         [binary, "--query-gpu=name", "--format=csv,noheader"],
         capture_output=True, text=True, timeout=3)
   except ProbeTimeout:
      # The probe's own SIGALRM self-abort must win over "swallow nvidia-smi
      # failures" -- otherwise a hung nvidia-smi can eat the global timeout
      # and the probe reports success (exit 0) after the deadline instead of
      # honoring the required exit-4 self-abort contract.
      raise
   except Exception:
      # Anything else -- ordinary hang covered by our own timeout=3, missing
      # driver, permission -- must not surface as a probe failure. Login
      # nodes normally have no GPU and no nvidia-smi; [] is the expected
      # answer, not a degraded one.
      return []
   if result.returncode != 0:
      return []
   return [line.strip() for line in result.stdout.split("\n") if line.strip()]


def _collect_gpus():
   """GPU inventory. [] is the normal answer on login nodes, not an error.

   Tries the /proc/driver/nvidia path first (cheap, no subprocess) and only
   shells out to nvidia-smi if that is absent, so a GPU-less node -- the
   common case here -- never pays for a subprocess at all.
   """
   base = _proc("driver", "nvidia", "gpus")
   try:
      entries = sorted(os.listdir(base))
   except (IOError, OSError):
      entries = []
   if entries:
      return entries
   return _collect_gpus_nvidia_smi()


def _collect_hardware():
   cpuinfo = _collect_cpuinfo()
   meminfo = _collect_hw_meminfo()
   return {
      "cpu_model": cpuinfo["cpu_model"],
      "cpu_logical": cpuinfo["cpu_logical"],
      "sockets": cpuinfo["sockets"],
      "cores_per_socket": cpuinfo["cores_per_socket"],
      "cpu_max_freq_khz": _collect_cpu_max_freq_khz(),
      "numa_nodes": _collect_numa_nodes(),
      "mem_total_kb": meminfo["mem_total_kb"],
      "swap_total_kb": meminfo["swap_total_kb"],
      "hugepage_size_kb": meminfo["hugepage_size_kb"],
      "kernel_release": _collect_kernel_release(),
      "os_pretty_name": _collect_os_pretty_name(),
      "net_fs_mounts": _collect_net_fs_mounts(),
      "net_ifaces": _collect_net_ifaces(),
      "boot_id": _collect_boot_id(),
      "btime": _collect_btime(),
      "gpus": _collect_gpus(),
   }


# --------------------------------------------------------------------------
# Process census (census loop only)
# --------------------------------------------------------------------------

def _parse_stat(raw):
   """Parse /proc/<pid>/stat, tolerating spaces and parens in comm.

   The comm field is bounded by the FIRST '(' and the LAST ')'. Splitting on
   whitespace breaks on any process whose name contains a space -- which on
   this fleet includes real processes -- and splitting on the first ')'
   breaks on names containing parens, e.g. '(sd-pam)'. Both occur here.
   """
   open_paren = raw.find("(")
   close_paren = raw.rfind(")")
   if open_paren < 0 or close_paren < open_paren:
      return None
   comm = raw[open_paren + 1:close_paren]
   tail = raw[close_paren + 2:].split()
   if len(tail) < 22:
      return None
   return comm, tail


def _resolve_rule(rules, haystack):
   """Return the label of the highest-priority rule matching `haystack`.

   Highest-priority-wins, not first-match-in-list-wins: `rules` is a list of
   (label, pattern, priority) tuples, and every one is checked against
   `haystack` rather than stopping at the first hit, so a later list
   position or a future reordering can never accidentally change which
   rule wins for a given priority value -- only the priority numbers can.
   Returns None if nothing matches.
   """
   best_label = None
   best_priority = None
   for label, pattern, priority in rules:
      if pattern.search(haystack) and (
            best_priority is None or priority > best_priority):
         best_label = label
         best_priority = priority
   return best_label


def _classify(name, cmdline):
   """Return (category, activity, confidence).

   Matched against the command line (argv) when we have one, and the
   process name otherwise. `name` here is /proc/<pid>/stat's `comm` field,
   a renameable 15-character task name (`prctl(PR_SET_NAME)`; `MainThread`
   and bare `node` are both common) -- it is only a fallback for when
   cmdline is absent, and must never override a cmdline-based match: a
   process can rename its own comm to anything, but it cannot rewrite the
   argv the kernel already recorded at exec time. For other users'
   processes cmdline is readable but exe/cwd are not (PLANNING.md 5.6), so
   argv is usually all we get anyway.
   """
   haystack = cmdline if cmdline else name
   category = _resolve_rule(_CATEGORY_RULES, haystack) or _DEFAULT_CATEGORY
   activity = _resolve_rule(_ACTIVITY_RULES, haystack)
   if activity is None:
      confidence = _CONF_UNKNOWN
   elif cmdline and cmdline.startswith("/"):
      confidence = _CONF_PATH
   else:
      confidence = _CONF_ARGV
   return category, activity, confidence


def _behavior(tty_nr, ppid, name):
   """Classify the shape of the process, not the tool.

   A controlling terminal is the only reliable interactive signal available
   for another user's process. Reparented-to-init with no tty is a daemon;
   no tty but a live parent is batch work.
   """
   if tty_nr and tty_nr != 0:
      return _BEHAVIOR_INTERACTIVE
   if ppid == 1:
      return _BEHAVIOR_DAEMON
   return _BEHAVIOR_BATCH


def _project_from_path(cmdline):
   if not cmdline:
      return None
   match = _PROJECT_PATH_RE.search(cmdline)
   return match.group(1) if match else None


def _collect_processes(uid_names, drop_raw_args, deadline):
   """Walk /proc once. Returns (rows, coverage).

   Coverage counts every way a process can fail to be read, so the daemon can
   tell a quiet node from a probe that is being denied. A census that
   silently drops 40% of processes and reports a clean sample is the single
   most dangerous failure mode this project has.
   """
   rows = []
   coverage = {
      "pids_seen": 0,
      "stat_unreadable": 0,
      "stat_unparseable": 0,
      "kernel_thread": 0,
      "cmdline_empty": 0,
      "owner_unresolved": 0,
      "vanished": 0,
   }
   try:
      entries = os.listdir(PROC_ROOT)
   except (IOError, OSError) as exc:
      raise _ProcUnreadable("cannot list %s: %s" % (PROC_ROOT, exc))

   for entry in entries:
      if not entry.isdigit():
         continue
      coverage["pids_seen"] += 1
      if deadline is not None and time.monotonic() > deadline:
         raise ProbeTimeout()

      proc_dir = _proc(entry)
      raw_stat = _read_text(proc_dir + "/stat")
      if raw_stat is None:
         coverage["stat_unreadable"] += 1
         continue
      parsed = _parse_stat(raw_stat)
      if parsed is None:
         coverage["stat_unparseable"] += 1
         continue
      comm, tail = parsed

      try:
         uid = os.stat(proc_dir).st_uid
      except (IOError, OSError):
         coverage["vanished"] += 1
         continue

      raw_cmdline = _read_text(proc_dir + "/cmdline")
      if raw_cmdline is None:
         # Unreadable is indistinguishable here from "file absent": _read_text
         # collapses EACCES/EPERM (denied) and ENOENT/ESRCH (absent) into the
         # same None, and on Linux /proc/<pid>/cmdline is normally
         # world-readable, so a denial would be rare and its distinct cause
         # is not worth inventing a probe into file existence to detect. Both
         # outcomes get the same conservative treatment as an empty cmdline
         # below: exclude the row, count it as a kernel thread.
         coverage["kernel_thread"] += 1
         continue
      cmdline = raw_cmdline.replace("\x00", " ").strip()
      if not cmdline:
         # Kernel threads have a cmdline file that exists but reads empty.
         # Same meaning as the file being absent above: not user behavior,
         # excluded from the census, counted the same way so an analyst
         # cannot tell the two apart from coverage alone -- because on this
         # node, neither could we.
         coverage["cmdline_empty"] += 1
         continue

      try:
         state = tail[0]
         ppid = int(tail[1])
         tty_nr = int(tail[4])
         utime = int(tail[11])
         stime = int(tail[12])
         start_time = int(tail[19])
         rss_pages = int(tail[21])
      except (IndexError, ValueError):
         coverage["stat_unparseable"] += 1
         continue

      username = uid_names.get(uid)
      if username is None:
         coverage["owner_unresolved"] += 1

      category, activity, confidence = _classify(comm, cmdline)
      row = {
         "pid": int(entry),
         "ppid": ppid,
         "uid": uid,
         "username": username,
         "comm": comm,
         "category": category,
         "behavior": _behavior(tty_nr, ppid, comm),
         "activity": activity,
         "activity_confidence": confidence,
         "project_path_hint": _project_from_path(cmdline),
         "utime_ticks": utime,
         "stime_ticks": stime,
         "rss_kb": rss_pages * (os.sysconf("SC_PAGESIZE") // 1024),
         "state": state,
         "start_time_ticks": start_time,
         "interactive": bool(tty_nr),
      }
      if not drop_raw_args:
         row["cmdline"] = cmdline
      rows.append(row)
   return rows, coverage


class _ProcUnreadable(Exception):
   """/proc itself could not be read -- exit code 3, not a partial result."""


def _resolve_uid_names(uids):
   """Map uid -> username via the passwd database.

   Looked up per distinct uid rather than via getpwall(): on an LDAP-backed
   site getpwall() enumerates every account at the facility, which is both
   far slower and far more data than we want. Unresolvable uids stay null --
   the daemon must not invent a name.
   """
   import pwd
   names = {}
   for uid in uids:
      try:
         names[uid] = pwd.getpwuid(uid).pw_name
      except (KeyError, OSError):
         names[uid] = None
   return names


def _distinct_uids():
   uids = set()
   try:
      entries = os.listdir(PROC_ROOT)
   except (IOError, OSError):
      return uids
   for entry in entries:
      if not entry.isdigit():
         continue
      try:
         uids.add(os.stat(_proc(entry)).st_uid)
      except (IOError, OSError):
         continue
   return uids


# --------------------------------------------------------------------------
# Session / user counts
# --------------------------------------------------------------------------

def _collect_session_counts(rows):
   """Count login sessions and distinct users from the census we already have.

   Derived from sshd processes with a controlling terminal rather than from
   utmp: utmp is not readable everywhere and would be a second source of
   truth for a number we can already compute. Only meaningful for the census
   loop.
   """
   users = set()
   sessions = 0
   for row in rows:
      if row["username"]:
         users.add(row["username"])
      if row["comm"].startswith("sshd") and row["uid"] != 0:
         sessions += 1
   return {"session_count": sessions, "user_count": len(users)}


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

def _parse_args(argv):
   """Minimal hand-rolled parsing.

   argparse is stdlib and would be fine, but this runs once per node per 60 s
   and importing argparse costs more than the whole parse.
   """
   opts = {
      "loop": "census",
      "max_seconds": 15.0,
      "drop_raw_args": True,
   }
   index = 1
   while index < len(argv):
      arg = argv[index]
      if arg == "--loop":
         index += 1
         if index >= len(argv) or argv[index] not in (
               "census", "counter", "hwinfo"):
            raise ValueError("--loop must be 'census', 'counter' or 'hwinfo'")
         opts["loop"] = argv[index]
      elif arg == "--max-seconds":
         index += 1
         if index >= len(argv):
            raise ValueError("--max-seconds requires a value")
         opts["max_seconds"] = float(argv[index])
      elif arg == "--keep-raw-args":
         opts["drop_raw_args"] = False
      elif arg == "--version":
         sys.stdout.write("%d\n" % PROBE_VERSION)
         raise SystemExit(0)
      else:
         raise ValueError("unknown argument: %s" % arg)
      index += 1
   return opts


def _hostname_fqdn():
   """Fully-qualified hostname as the NODE sees itself (PLANNING.md 8.3).

   The daemon must never substitute the alias it dialed: the whole point of
   this field is to catch a fan-out that silently probed the same node twice
   because two aliases resolved to one host.
   """
   import socket
   try:
      return socket.getfqdn()
   except Exception:
      return os.uname()[1]


def main(argv):
   started_monotonic = time.monotonic()
   try:
      opts = _parse_args(argv)
   except SystemExit:
      raise
   except ValueError as exc:
      sys.stderr.write("bad args: %s\n" % exc)
      return 2

   if sys.version_info[:2] < MIN_PYTHON:
      sys.stderr.write(
         "probe requires Python >= %d.%d, got %s\n"
         % (MIN_PYTHON[0], MIN_PYTHON[1],
            ".".join(str(part) for part in sys.version_info[:3])))
      return 2

   deadline = started_monotonic + opts["max_seconds"]
   signal.signal(signal.SIGALRM, _on_alarm)
   signal.alarm(int(opts["max_seconds"]) + 1)

   payload = {
      "probe_version": PROBE_VERSION,
      "loop": opts["loop"],
      "hostname_fqdn": _hostname_fqdn(),
      "python_version": ".".join(str(part) for part in sys.version_info[:3]),
      "wall_clock_utc": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + "Z",
      "monotonic_sec": time.monotonic(),
      "drop_raw_args": opts["drop_raw_args"],
   }

   try:
      uptime_line = _read_first_line(_proc("uptime"))
      if uptime_line is None:
         sys.stderr.write("%s unreadable\n" % _proc("uptime"))
         return 3

      if opts["loop"] == "hwinfo":
         # Deliberately skips uptime_sec/counters: those exist for the
         # census/counter cadence the daemon deltas every 60s, and the whole
         # point of hwinfo is to be far cheaper than that. The uptime read
         # above is kept anyway, purely as the same /proc-sanity probe the
         # other loops use, so all three loops share one exit-3 contract.
         payload["hardware"] = _collect_hardware()
      else:
         payload["uptime_sec"] = float(uptime_line.split()[0])
         payload["counters"] = _collect_node_counters()

         if opts["loop"] == "census":
            uid_names = _resolve_uid_names(_distinct_uids())
            rows, coverage = _collect_processes(
               uid_names, opts["drop_raw_args"], deadline)
            payload["processes"] = rows
            payload["census_coverage"] = coverage
            payload["counters"].update(_collect_session_counts(rows))
   except ProbeTimeout:
      signal.alarm(0)
      sys.stderr.write(
         "probe exceeded %.1fs budget\n" % opts["max_seconds"])
      return 4
   except _ProcUnreadable as exc:
      signal.alarm(0)
      sys.stderr.write("%s\n" % exc)
      return 3
   finally:
      signal.alarm(0)

   payload["probe_self_seconds"] = round(time.monotonic() - started_monotonic, 4)
   sys.stdout.write(json.dumps(payload, separators=(",", ":")) + "\n")
   return 0


if __name__ == "__main__":
   sys.exit(main(sys.argv))
