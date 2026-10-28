"""Remote probe: reads /proc and /sys on a login node and emits raw fields.

Single file, stdlib only, Python 3.6 compatible. It is shipped to nodes as
source text, so importing it must have no side effects.
"""

import argparse
import errno
import fcntl
import json
import os
import pwd
import re
import resource
import select
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time

PROBE_VERSION = 1
# Carried in the probe's cmdline (the bootstrap puts it there); lock takeover signals
# only a holder whose cmdline contains it.
MARKER = "node-monitor-probe"

# Hardware detection runs once at hello; a wedged driver must not stall it for long.
HARDWARE_GPU_TIMEOUT_SEC = 5.0

# errnos that are routine on a busy node: another user's process, a process that
# exited mid-walk, or /proc files that reject reads in some kernel states.
_BENIGN_ERRNOS = (
   errno.EACCES,
   errno.EPERM,
   errno.ENOENT,
   errno.ESRCH,
   errno.EINVAL,
   errno.EIO,
   errno.ENXIO,
)


def read_text(path):
   """Return file contents, or None for the routine /proc read failures."""
   try:
      with open(path, "rb") as handle:
         return handle.read().decode("utf-8", "replace")
   except (IOError, OSError) as exc:
      if exc.errno in _BENIGN_ERRNOS:
         return None
      raise


def read_first_line(path):
   text = read_text(path)
   if text is None:
      return None
   return text.split("\n", 1)[0]


def read_link(path):
   """Return a symlink target verbatim (including " (deleted)"), None on any OSError."""
   try:
      return os.readlink(path)
   except (IOError, OSError):
      return None


def _proc(roots, *parts):
   return os.path.join(roots["proc"], *parts)


def _sys(roots, *parts):
   return os.path.join(roots["sys"], *parts)


def _meminfo_fields(roots, wanted):
   out = dict.fromkeys(wanted.values())
   text = read_text(_proc(roots, "meminfo"))
   if text is None:
      return out
   for line in text.split("\n"):
      key, _, rest = line.partition(":")
      if key in wanted:
         value = rest.split()
         if value and value[0].isdigit():
            out[wanted[key]] = int(value[0])
   return out


# --------------------------------------------------------------------------
# Node counters
# --------------------------------------------------------------------------


def _collect_loadavg(roots):
   line = read_first_line(_proc(roots, "loadavg"))
   parts = line.split() if line else []
   if len(parts) < 4:
      return {}, None
   try:
      running, _, total = parts[3].partition("/")
      out = {
         "load1": float(parts[0]),
         "load5": float(parts[1]),
         "load15": float(parts[2]),
         "procs_running": int(running),
      }
   except ValueError:
      return {}, None
   return out, int(total) if total.isdigit() else None


def _collect_cpu_jiffies(roots):
   line = read_first_line(_proc(roots, "stat"))
   if not line or not line.startswith("cpu "):
      return None
   names = [
      "user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal", "guest",
      "guest_nice",
   ]  # fmt: skip
   out = {}
   values = line.split()[1:]
   for index, name in enumerate(names[: len(values)]):
      try:
         out[name] = int(values[index])
      except ValueError:
         continue
   return out


def _collect_meminfo(roots):
   wanted = {
      "MemTotal": "total_kb",
      "MemAvailable": "available_kb",
      "Cached": "cached_kb",
      "Shmem": "shmem_kb",
      "MemFree": "free_kb",
      "SwapTotal": "swap_total_kb",
      "SwapFree": "swap_free_kb",
   }
   fields = _meminfo_fields(roots, wanted)
   return {k: v for k, v in fields.items() if v is not None}


def _collect_net(roots):
   # Per interface, not summed: summing would hide an interface flapping.
   text = read_text(_proc(roots, "net", "dev"))
   if text is None:
      return {}
   out = {}
   for line in text.split("\n")[2:]:
      name, _, rest = line.partition(":")
      name = name.strip()
      fields = rest.split()
      if not name or name == "lo" or len(fields) < 9:
         continue
      try:
         out[name] = {"rx_bytes": int(fields[0]), "tx_bytes": int(fields[8])}
      except ValueError:
         continue
   return out


def _collect_lustre_md(roots):
   # Lustre client metadata-op counters per MDC target; absent off Lustre.
   base = _proc(roots, "fs", "lustre", "mdc")
   out = {}
   try:
      targets = os.listdir(base)
   except (IOError, OSError):
      return out
   for target in targets:
      stats = read_text(os.path.join(base, target, "md_stats"))
      if stats is None:
         continue
      counters = {}
      total = 0
      for line in stats.split("\n"):
         fields = line.split()
         if len(fields) >= 2 and fields[1].isdigit():
            counters[fields[0]] = int(fields[1])
            total += int(fields[1])
      if counters:
         counters["total"] = total
         out[target] = counters
   return out


def _collect_socket_count(roots):
   # Node total only: per-uid attribution would mean walking every /proc/<pid>/fd.
   text = read_text(_proc(roots, "net", "sockstat"))
   if text is None:
      return None
   for line in text.split("\n"):
      if line.startswith("sockets: used"):
         fields = line.split()
         if fields and fields[-1].isdigit():
            return int(fields[-1])
   return None


def _collect_uptime(roots):
   line = read_first_line(_proc(roots, "uptime"))
   if not line:
      return None
   try:
      return float(line.split()[0])
   except (IndexError, ValueError):
      return None


def collect_counters(roots):
   counters = {}
   uptime = _collect_uptime(roots)
   if uptime is not None:
      counters["uptime_sec"] = uptime
   load, procs_total = _collect_loadavg(roots)
   counters.update(load)
   if procs_total is not None:
      counters["procs_total"] = procs_total
   cpu = _collect_cpu_jiffies(roots)
   if cpu is not None:
      counters["cpu_jiffies"] = cpu
   counters["clk_tck"] = os.sysconf("SC_CLK_TCK")
   counters["mem"] = _collect_meminfo(roots)
   counters["net"] = _collect_net(roots)
   counters["md_ops"] = _collect_lustre_md(roots)
   counters["socket_count"] = _collect_socket_count(roots)
   return counters


# --------------------------------------------------------------------------
# Hardware inventory
#
# Static facts only. Instantaneous CPU MHz from /proc/cpuinfo is deliberately
# absent: it is a live DVFS reading that varies per core and per sample.
# --------------------------------------------------------------------------


def _collect_cpuinfo(roots):
   # sockets/cores_per_socket stay None (not 0) when the platform does not expose them.
   out = {"cpu_model": None, "cpu_logical": 0, "sockets": None, "cores_per_socket": None}
   text = read_text(_proc(roots, "cpuinfo"))
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


def _collect_cpu_max_freq_khz(roots):
   text = read_text(_sys(roots, "devices", "system", "cpu", "cpu0", "cpufreq", "cpuinfo_max_freq"))
   if text is None:
      return None
   try:
      return int(text.strip())
   except ValueError:
      return None


def _collect_numa_nodes(roots):
   try:
      entries = os.listdir(_sys(roots, "devices", "system", "node"))
   except (IOError, OSError):
      return 0
   return sum(1 for entry in entries if re.match(r"^node\d+$", entry))


def _collect_os_pretty_name(roots):
   text = read_text(roots.get("os_release", "/etc/os-release"))
   if text is None:
      return None
   for line in text.split("\n"):
      key, sep, rest = line.partition("=")
      if sep and key.strip() == "PRETTY_NAME":
         return rest.strip().strip('"')
   return None


def _collect_net_fs_mounts(roots):
   # A count, not the mount table: it is the denominator for metadata-op analysis.
   text = read_text(_proc(roots, "mounts"))
   if text is None:
      return 0
   count = 0
   for line in text.split("\n"):
      fields = line.split()
      if len(fields) >= 3 and fields[2] in ("lustre", "nfs", "nfs4"):
         count += 1
   return count


def _collect_net_ifaces(roots):
   # iface -> link speed in Mb/s, None when unreadable ("?" and EINVAL on down links).
   base = _sys(roots, "class", "net")
   try:
      names = os.listdir(base)
   except (IOError, OSError):
      return {}
   out = {}
   for name in names:
      iface_dir = os.path.join(base, name)
      # bonding_masters is a control file alongside the real interfaces, not a NIC.
      if not os.path.isdir(iface_dir):
         continue
      try:
         text = read_text(os.path.join(iface_dir, "speed"))
      except Exception:
         text = None
      try:
         out[name] = int(text.strip()) if text is not None else None
      except ValueError:
         out[name] = None
   return out


def _collect_gpus_nvidia_smi():
   binary = shutil.which("nvidia-smi")
   if binary is None:
      return []
   deadline = time.monotonic() + HARDWARE_GPU_TIMEOUT_SEC
   try:
      rows = _run_nvidia_smi(binary, "--query-gpu=name", deadline)
   except (subprocess.TimeoutExpired, RuntimeError, OSError):
      return []
   return [cells[0] for cells in rows if cells and cells[0]]


def _collect_gpus(roots, gpu=True):
   # Cheap /proc listing first so GPU-less nodes never spawn a subprocess.
   try:
      entries = sorted(os.listdir(_proc(roots, "driver", "nvidia", "gpus")))
   except (IOError, OSError):
      entries = []
   if entries or not gpu:
      return entries
   return _collect_gpus_nvidia_smi()


def collect_hardware(roots, gpu=True):
   """Static node facts; gpu=False never spawns nvidia-smi."""
   cpuinfo = _collect_cpuinfo(roots)
   mem = _meminfo_fields(
      roots,
      {
         "MemTotal": "mem_total_kb",
         "SwapTotal": "swap_total_kb",
         "Hugepagesize": "hugepage_size_kb",
      },
   )
   return {
      "cpu_model": cpuinfo["cpu_model"],
      "cpu_logical": cpuinfo["cpu_logical"],
      "sockets": cpuinfo["sockets"],
      "cores_per_socket": cpuinfo["cores_per_socket"],
      "cpu_max_freq_khz": _collect_cpu_max_freq_khz(roots),
      "numa_nodes": _collect_numa_nodes(roots),
      "mem_total_kb": mem["mem_total_kb"],
      "swap_total_kb": mem["swap_total_kb"],
      "hugepage_size_kb": mem["hugepage_size_kb"],
      "kernel_release": read_first_line(_proc(roots, "sys", "kernel", "osrelease")),
      "os_pretty_name": _collect_os_pretty_name(roots),
      "net_fs_mounts": _collect_net_fs_mounts(roots),
      "net_ifaces": _collect_net_ifaces(roots),
      "gpus": _collect_gpus(roots, gpu),
   }


def boot_identity(roots):
   """Return (boot_id, btime); together they identify one boot of the node."""
   boot_id = read_first_line(_proc(roots, "sys", "kernel", "random", "boot_id"))
   btime = None
   text = read_text(_proc(roots, "stat"))
   for line in (text or "").split("\n"):
      if line.startswith("btime "):
         try:
            btime = int(line.split()[1])
         except (IndexError, ValueError):
            pass
         break
   return boot_id, btime


# --------------------------------------------------------------------------
# Process census
# --------------------------------------------------------------------------

_SHELL_RE = re.compile(r"(^|/)(bash|zsh|tcsh|csh|ksh|sh)( |$)|^-")
# Real comms are "tmux: server" and "SCREEN", hence the colon and case-insensitivity.
_MULTIPLEXER_RE = re.compile(r"(^|/)(screen|tmux)(:| |$)", re.I)


def parse_stat(raw):
   """Split /proc/<pid>/stat into (comm, tail); tail[i] is proc(5) field i+3.

   comm is bounded by the first "(" and the last ")" because it may itself
   contain spaces and parens.
   """
   open_paren = raw.find("(")
   close_paren = raw.rfind(")")
   if open_paren < 0 or close_paren < open_paren:
      return None
   tail = raw[close_paren + 2 :].split()
   if len(tail) < 22:
      return None
   return raw[open_paren + 1 : close_paren], tail


def _parent_kind(ppid, parent_comm):
   # Order matters: reparenting to init is the orphan signal whatever pid 1 is called.
   if ppid == 1:
      return "init"
   if parent_comm is None:
      return "unknown"
   if _MULTIPLEXER_RE.search(parent_comm):
      return "multiplexer"
   if parent_comm.startswith("sshd"):
      return "sshd"
   if _SHELL_RE.search(parent_comm):
      return "shell"
   return "other"


def _resolve_uid(uid, uid_names):
   if uid not in uid_names:
      try:
         uid_names[uid] = pwd.getpwuid(uid).pw_name
      except (KeyError, OverflowError):
         uid_names[uid] = None
   return uid_names[uid]


def _status_fields(raw):
   uid = threads = None
   for line in raw.split("\n"):
      if line.startswith("Uid:"):
         fields = line.split()
         if len(fields) > 1 and fields[1].isdigit():
            uid = int(fields[1])
      elif line.startswith("Threads:"):
         fields = line.split()
         if len(fields) > 1 and fields[1].isdigit():
            threads = int(fields[1])
   return uid, threads


def _read_process(proc_dir, page_kb, uid_names, coverage):
   """Build one census row, or None if the process is excluded or unreadable."""
   raw_stat = read_text(proc_dir + "/stat")
   if raw_stat is None:
      coverage["stat_unreadable"] += 1
      return None
   parsed = parse_stat(raw_stat)
   if parsed is None:
      coverage["stat_unparseable"] += 1
      return None
   comm, tail = parsed
   raw_cmdline = read_text(proc_dir + "/cmdline")
   cmdline = (raw_cmdline or "").replace("\0", " ").strip()
   if not cmdline:
      coverage["kernel_thread"] += 1
      return None
   raw_status = read_text(proc_dir + "/status")
   uid, threads = _status_fields(raw_status) if raw_status is not None else (None, None)
   if uid is None:
      coverage["vanished"] += 1
      return None
   try:
      pid = int(os.path.basename(proc_dir))
      ppid = int(tail[1])
      sid = int(tail[3])
      tty_nr = int(tail[4])
      utime = int(tail[11])
      stime = int(tail[12])
      start_time = int(tail[19])
      rss_pages = int(tail[21])
   except ValueError:
      coverage["stat_unparseable"] += 1
      return None
   username = _resolve_uid(uid, uid_names)
   if username is None:
      coverage["owner_unresolved"] += 1
   exe = read_link(proc_dir + "/exe")
   if exe is None:
      coverage["exe_unreadable"] += 1
   return {
      "pid": pid,
      "ppid": ppid,
      "start_time": start_time,
      "uid": uid,
      "username": username,
      "comm": comm,
      "exe": exe,
      "argv0": raw_cmdline.split("\0", 1)[0],
      "cmdline": cmdline,
      "state": tail[0],
      "threads": threads,
      "rss_kb": rss_pages * page_kb,
      "utime": utime,
      "stime": stime,
      "tty_nr": tty_nr,
      "interactive": tty_nr != 0,
      "session_leader": pid == sid,
   }


def _resolve_parent_direct(roots, ppid):
   # A parent outside the census (root's sshd, a kernel thread): one bounded read, no recursion.
   proc_dir = _proc(roots, str(ppid))
   raw_stat = read_text(proc_dir + "/stat")
   parsed = parse_stat(raw_stat) if raw_stat is not None else None
   return (parsed[0] if parsed else None), read_link(proc_dir + "/exe")


def _enrich_parents(roots, rows, coverage):
   # After the walk: pids come in no particular order, so a parent row may not exist earlier.
   by_pid = {r["pid"]: r for r in rows}
   for row in rows:
      ppid = row["ppid"]
      parent = by_pid.get(ppid)
      if parent is not None:
         parent_comm, parent_exe = parent["comm"], parent["exe"]
      else:
         parent_comm, parent_exe = _resolve_parent_direct(roots, ppid)
      if parent_comm is None:
         coverage["parent_unresolved"] += 1
      row["parent_comm"] = parent_comm
      row["parent_exe"] = parent_exe
      row["under_screen_or_tmux"] = bool(parent_comm and _MULTIPLEXER_RE.search(parent_comm))
      row["parent_kind"] = _parent_kind(ppid, parent_comm)


def scan_processes(roots, uid_names):
   """Walk /proc once; return (rows, coverage). Kernel threads are excluded.

   Coverage counts every way a process can fail to be read so a probe being
   denied is distinguishable from a quiet node.
   """
   coverage = dict.fromkeys(
      (
         "pids_seen", "stat_unreadable", "stat_unparseable", "kernel_thread", "vanished",
         "owner_unresolved", "exe_unreadable", "parent_unresolved",
      ),
      0,
   )  # fmt: skip
   rows = []
   page_kb = os.sysconf("SC_PAGE_SIZE") // 1024
   try:
      entries = os.listdir(roots["proc"])
   except (IOError, OSError):
      return rows, coverage
   for entry in entries:
      if not entry.isdigit():
         continue
      coverage["pids_seen"] += 1
      row = _read_process(_proc(roots, entry), page_kb, uid_names, coverage)
      if row is not None:
         rows.append(row)
   _enrich_parents(roots, rows, coverage)
   return rows, coverage


class ProcessTracker(object):
   """Per-process CPU delta and dormancy, keyed by (pid, start_time).

   Keying on start_time makes a recycled pid a new process, so a delta is never
   computed across two different programs.
   """

   def __init__(self, active_cpu_seconds_per_min, clk_tck):
      self._active_per_min = active_cpu_seconds_per_min
      self._clk_tck = clk_tck
      self._state = {}
      self._updates = 0

   def update(self, rows, now, interval_sec):
      first_update = self._updates == 0
      self._updates += 1
      seen = {}
      for row in rows:
         key = (row["pid"], row["start_time"])
         ticks = row.pop("utime") + row.pop("stime")
         state = self._state.get(key)
         if state is None:
            state = {
               "ticks": ticks, "first_seen": now, "last_active": None,
               "existed_at_start": first_update,
            }  # fmt: skip
            row["cpu_seconds"] = None
            row["cpu_state"] = None
            row["idle_for_sec"] = None
            row["idle_for_is_lower_bound"] = False
         else:
            cpu_seconds = (ticks - state["ticks"]) / float(self._clk_tck)
            threshold = self._active_per_min * (interval_sec or 0) / 60.0
            row["cpu_seconds"] = cpu_seconds
            if cpu_seconds >= threshold:
               state["last_active"] = now
               row["cpu_state"] = "active"
               row["idle_for_sec"] = 0
            else:
               since = state["last_active"]
               row["cpu_state"] = "idle"
               row["idle_for_sec"] = now - (state["first_seen"] if since is None else since)
            row["idle_for_is_lower_bound"] = (
               state["last_active"] is None and state["existed_at_start"]
            )
            state["ticks"] = ticks
         seen[key] = state
      self._state = seen


# nvidia-smi children that survived SIGKILL of their group; polled each census so they
# do not linger as zombies once the driver lets them go.
_ABANDONED_GPU_QUERIES = []


def reap_abandoned_gpu_queries():
   """Poll abandoned nvidia-smi children without blocking; drop the reaped ones."""
   still_running = []
   for proc in _ABANDONED_GPU_QUERIES:
      try:
         if proc.poll() is None:
            still_running.append(proc)
      except Exception:
         pass
   _ABANDONED_GPU_QUERIES[:] = still_running


def _run_nvidia_smi(binary, query_flag, deadline):
   """Run one query against a shared monotonic deadline; never waits unboundedly.

   subprocess.run is avoided: on 3.6 its timeout path does an untimed second
   communicate() that blocks while a descendant holds the pipe, and a child in
   uninterruptible sleep ignores kill. So the whole process group is killed and,
   if it still will not go, the process and pipes are abandoned.
   """
   remaining = deadline - time.monotonic()
   if remaining <= 0:
      raise subprocess.TimeoutExpired(binary, 0)
   proc = subprocess.Popen(
      [binary, query_flag, "--format=csv,noheader,nounits"],
      stdout=subprocess.PIPE,
      stderr=subprocess.PIPE,
      universal_newlines=True,
      start_new_session=True,
   )
   try:
      out, err = proc.communicate(timeout=remaining)
   except subprocess.TimeoutExpired:
      try:
         os.killpg(proc.pid, signal.SIGKILL)
      except OSError:
         pass
      try:
         proc.communicate(timeout=1)
      except Exception:
         pass
      if proc.poll() is None:
         # Unkillable for now (uninterruptible sleep in the driver); reaped later.
         _ABANDONED_GPU_QUERIES.append(proc)
      for pipe in (proc.stdout, proc.stderr):
         try:
            pipe.close()
         except Exception:
            pass
      raise
   if proc.returncode != 0:
      message = "nvidia-smi exit %d" % proc.returncode
      # An exit code alone does not say why. nvidia-smi puts some failures on stderr and
      # others (NVML initialization) on stdout, so fall back to stdout.
      detail = _first_line(err) or _first_line(out)
      if detail:
         message += ": " + detail[:GPU_ERROR_DETAIL_MAX]
      raise RuntimeError(message)
   return [[cell.strip() for cell in line.split(",")] for line in out.split("\n") if line.strip()]


def _first_line(text):
   for line in (text or "").split("\n"):
      if line.strip():
         return line.strip()
   return None


GPU_TIMEOUT_ERROR = "nvidia-smi timeout"
GPU_ERROR_DETAIL_MAX = 200


def query_gpus(timeout=5.0):
   """Return (gpus, gpu_mem_by_pid, error); error is None when nvidia-smi is absent."""
   binary = shutil.which("nvidia-smi")
   if binary is None:
      return [], {}, None
   deadline = time.monotonic() + timeout
   try:
      gpus = []
      for cells in _run_nvidia_smi(
         binary, "--query-gpu=index,utilization.gpu,memory.used,memory.total", deadline
      ):
         index, util, used, total = cells
         gpus.append(
            {
               "index": int(index),
               "util_pct": int(util),
               "mem_used_mb": int(used),
               "mem_total_mb": int(total),
            }
         )
      by_pid = {}
      for cells in _run_nvidia_smi(binary, "--query-compute-apps=pid,used_memory", deadline):
         pid, used = cells
         by_pid[int(pid)] = by_pid.get(int(pid), 0) + int(used)
   except subprocess.TimeoutExpired:
      return [], {}, GPU_TIMEOUT_ERROR
   except RuntimeError as exc:
      return [], {}, str(exc)
   except (ValueError, OSError):
      return [], {}, "nvidia-smi output unparseable"
   return gpus, by_pid, None


# --------------------------------------------------------------------------
# Streaming
# --------------------------------------------------------------------------

# After an nvidia-smi timeout GPU queries pause this long, so a wedged driver does not
# collect a new hung nvidia-smi every census. A failure that keeps repeating word for word
# (NVML refusing a non-root user, say) pauses them too: retrying it every census would
# only spawn a doomed nvidia-smi and an identical error each minute.
GPU_SUSPEND_SEC = 600
GPU_IDENTICAL_FAILURES_TO_SUSPEND = 3
# Longest sleep between parent-liveness checks.
PARENT_POLL_SEC = 1.0
ERROR_MESSAGE_MAX = 500
LOCK_TAKEOVER_WAIT_SEC = 5.0
LOCK_POLL_SEC = 0.1
# A holder writes its pid just after taking the lock; allow it that long to appear.
LOCK_PID_WAIT_SEC = 1.0


def default_config():
   return {
      "counter_interval_sec": 10,
      "census_interval_sec": 60,
      "active_cpu_seconds_per_min": 0.1,
      "lock_path": "/dev/shm/node-monitor-%d.lock" % os.getuid(),
      "proc_root": "/proc",
      "sys_root": "/sys",
      "gpu": True,
      # Not part of the collector's config surface; tests shorten them.
      "gpu_timeout_sec": 5.0,
      "gpu_suspend_sec": GPU_SUSPEND_SEC,
   }


def _resolve_config(config):
   merged = default_config()
   for key, value in (config or {}).items():
      if value is not None:
         merged[key] = value
   return merged


def _roots(cfg):
   return {"proc": cfg["proc_root"], "sys": cfg["sys_root"]}


def _describe(exc):
   return "%s: %s" % (type(exc).__name__, exc)


class LockHeldByOther(Exception):
   def __init__(self, pid, reason="lock is held"):
      Exception.__init__(self, "%s (holder pid %s)" % (reason, pid))
      self.pid = pid


def _read_lock_pid(path):
   try:
      with open(path, "rb") as handle:
         text = handle.read(64).decode("ascii", "replace").strip()
   except (IOError, OSError):
      return None
   return int(text) if text.isdigit() else None


def _cmdline_of(pid):
   # Always the real /proc: the lock is a property of this host, not of a test tree.
   try:
      return read_text("/proc/%d/cmdline" % pid)
   except (IOError, OSError):
      return None


class ProbeLock(object):
   """One probe per user per node, via flock on a file holding the holder's pid.

   The flock, not the file's pid, decides who holds it: a lock file left by a dead
   probe names a pid that may now be someone else's process, but nobody holds its
   flock, so it is simply taken. Only a live flock holder whose cmdline carries the
   marker is ever signalled.
   """

   def __init__(self, path):
      self.path = path
      self._fd = None

   def acquire(self):
      # os.open fds are close-on-exec, so no nvidia-smi child inherits (and pins) the lock.
      fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
      try:
         self._acquire(fd)
         os.ftruncate(fd, 0)
         os.write(fd, ("%d\n" % os.getpid()).encode("ascii"))
      except BaseException:
         os.close(fd)
         raise
      self._fd = fd

   def _try_lock(self, fd):
      try:
         fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
         return True
      except (IOError, OSError) as exc:
         if exc.errno in (errno.EAGAIN, errno.EACCES, errno.EWOULDBLOCK):
            return False
         raise

   def _wait_for_lock(self, fd, timeout):
      deadline = time.monotonic() + timeout
      while True:
         if self._try_lock(fd):
            return True
         if time.monotonic() >= deadline:
            return False
         time.sleep(LOCK_POLL_SEC)

   def _acquire(self, fd):
      deadline = time.monotonic() + LOCK_PID_WAIT_SEC
      while True:
         if self._try_lock(fd):
            return
         pid = _read_lock_pid(self.path)
         if pid is not None:
            break
         if time.monotonic() >= deadline:
            raise LockHeldByOther(None, "lock file names no pid")
         time.sleep(LOCK_POLL_SEC)
      for sig in (signal.SIGTERM, signal.SIGKILL):
         self._signal_probe(pid, sig)
         if self._wait_for_lock(fd, LOCK_TAKEOVER_WAIT_SEC):
            return
      raise LockHeldByOther(pid, "previous probe did not release the lock")

   def _signal_probe(self, pid, sig):
      if pid <= 1 or pid == os.getpid():
         raise LockHeldByOther(pid, "refusing to signal this pid")
      # Re-read before every signal: between SIGTERM and SIGKILL the pid may be recycled.
      cmdline = _cmdline_of(pid)
      if cmdline is None:
         return  # exited, or hidden from us; the wait that follows decides
      if MARKER not in cmdline:
         raise LockHeldByOther(pid, "lock holder is not a node-monitor probe")
      try:
         os.kill(pid, sig)
      except OSError:
         pass


def _utc_timestamp(now):
   ms = int(now * 1000)
   return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ms // 1000)) + ".%03dZ" % (ms % 1000)


def _write_all(fd, data):
   # Raw fd writes: a census line can be ~600 KB, and a non-blocking stdout must
   # wait for the reader rather than fail mid-line.
   view = memoryview(data)
   while view:
      try:
         written = os.write(fd, view)
      except BlockingIOError:
         select.select([], [fd], [])
         continue
      view = view[written:]


_TERMINATING_SIGNALS = (
   (signal.SIGTERM, "SIGTERM"),
   (signal.SIGHUP, "SIGHUP"),
   (signal.SIGINT, "SIGINT"),
)
# How long a pending signal waits for a line in progress to finish before the probe
# exits unreported (its exit status still names the signal).
SIGNAL_REPORT_GRACE_SEC = 5.0


class _Emitter(object):
   """Writes one JSON object per line, flushed, with a per-process seq from 0.

   Also the meeting point for a terminating signal. The handler only records it; the
   next emit (main thread) or the watchdog reports it and exits. Lines are written
   under a lock, so the report always follows whole lines: a census half written when
   the signal lands is finished first, never split.
   """

   def __init__(self, out):
      self._out = out
      self._seq = 0
      self._lock = threading.Lock()
      self._signum = None
      self._signal_at = None
      try:
         out.flush()
         self._fd = out.fileno()
      except (AttributeError, ValueError, IOError, OSError):
         self._fd = None

   def emit(self, kind, fields):
      with self._lock:
         if self._signum is not None:
            self._report_signal_and_exit()
         if not self._write(kind, fields):
            # The reader is gone (BrokenPipeError: Python ignores SIGPIPE). Nobody is
            # listening, so exit at once without unwinding.
            os._exit(0)

   def error(self, where, message):
      self.emit("error", _error_fields(where, message))

   def install_signal_handlers(self):
      for signum, _ in _TERMINATING_SIGNALS:
         try:
            # A signal its launcher ignores (nohup, a background job) stays ignored.
            if signal.getsignal(signum) != signal.SIG_IGN:
               signal.signal(signum, self._on_signal)
         except (ValueError, OSError, RuntimeError):
            pass  # not the main thread: keep the default action

   def _on_signal(self, signum, frame):
      # Only a flag. Python runs this between bytecodes of the main thread, which may be
      # in the middle of writing a line (and holding the lock), so emitting here could
      # split that line or deadlock.
      if self._signum is None:
         self._signal_at = time.monotonic()
         self._signum = signum

   def check_signal(self, wait):
      """Report a pending signal and exit, waiting up to `wait` s for a line in progress.

      Returns if no signal is pending, or if the line in progress is still unfinished
      (the caller polls again). Once the grace has passed, exits unreported.
      """
      if self._signum is None:
         return
      if self._lock.acquire(True, wait):
         self._report_signal_and_exit()
      if time.monotonic() - self._signal_at >= SIGNAL_REPORT_GRACE_SEC:
         os._exit(128 + self._signum)

   def _report_signal_and_exit(self):
      # Called with the lock held, which is kept until exit so nothing follows the report.
      signum = self._signum
      name = dict(_TERMINATING_SIGNALS).get(signum, "signal")
      self._write("error", _error_fields("signal", "terminated by %s (%d)" % (name, signum)))
      os._exit(128 + signum)

   def _write(self, kind, fields):
      """Write one line; False if the reader is gone."""
      msg = {"type": kind, "seq": self._seq, "ts": _utc_timestamp(time.time())}
      msg.update(fields)
      # ensure_ascii keeps the line ASCII whatever the node's locale or the bytes in /proc.
      line = json.dumps(msg, separators=(",", ":")) + "\n"
      try:
         if self._fd is None:
            self._out.write(line)
            self._out.flush()
         else:
            _write_all(self._fd, line.encode("ascii"))
      except (IOError, OSError):
         return False
      self._seq += 1
      return True


def _error_fields(where, message):
   return {"where": where, "message": message[:ERROR_MESSAGE_MAX]}


def _emit_hello(emitter, cfg, roots):
   problems = []
   try:
      boot_id, btime = boot_identity(roots)
   except Exception as exc:
      boot_id = btime = None
      problems.append(_describe(exc))
   try:
      hardware = collect_hardware(roots, gpu=bool(cfg["gpu"]))
   except Exception as exc:
      hardware = None
      problems.append(_describe(exc))
   emitter.emit(
      "hello",
      {
         "probe_version": PROBE_VERSION,
         "python_version": "%d.%d.%d" % tuple(sys.version_info[:3]),
         "hostname_fqdn": socket.getfqdn(),
         "pid": os.getpid(),
         "boot_id": boot_id,
         "btime": btime,
         "hardware": hardware,
      },
   )
   for problem in problems:
      emitter.error("hello", problem)


def _emit_counters(emitter, roots):
   try:
      fields = collect_counters(roots)
      own = resource.getrusage(resource.RUSAGE_SELF)
      children = resource.getrusage(resource.RUSAGE_CHILDREN)
      fields["probe_self"] = {
         "cpu_seconds": round(own.ru_utime + own.ru_stime, 3),
         "children_cpu_seconds": round(children.ru_utime + children.ru_stime, 3),
         "maxrss_kb": own.ru_maxrss,
      }
      emitter.emit("counters", fields)
   except Exception as exc:
      emitter.error("counters", _describe(exc))


class _Census(object):
   def __init__(self, cfg, roots, emitter):
      self._roots = roots
      self._emitter = emitter
      self._uid_names = {}
      self._tracker = ProcessTracker(cfg["active_cpu_seconds_per_min"], os.sysconf("SC_CLK_TCK"))
      self._last_start = None
      self._gpu = bool(cfg["gpu"])
      self._gpu_timeout = float(cfg["gpu_timeout_sec"])
      self._gpu_suspend = float(cfg["gpu_suspend_sec"])
      self._gpu_resume_at = None
      # The last non-timeout failure and how many censuses in a row it has repeated. The
      # streak survives a suspension, so a failure that persists after one re-suspends at
      # once instead of earning a fresh run of errors.
      self._gpu_failure = None
      self._gpu_failure_streak = 0

   def emit(self):
      try:
         self._emitter.emit("census", self._take())
      except Exception as exc:
         self._emitter.error("census", _describe(exc))

   def _take(self):
      start = time.monotonic()
      reap_abandoned_gpu_queries()
      rows, coverage = scan_processes(self._roots, self._uid_names)
      gpus, gpu_mem_by_pid, gpu_status = self._query_gpus()
      interval = None if self._last_start is None else round(start - self._last_start, 3)
      self._tracker.update(rows, start, interval)
      # Only after a successful update, so the next delta spans exactly the ticks it covers.
      self._last_start = start
      for row in rows:
         row["gpu_mem_mb"] = gpu_mem_by_pid.get(row["pid"])
      return {
         "interval_sec": interval,
         "scan_seconds": round(time.monotonic() - start, 3),
         "coverage": coverage,
         "gpus": gpus,
         "gpu_status": gpu_status,
         "processes": rows,
      }

   def _query_gpus(self):
      """Return (gpus, gpu_mem_by_pid, gpu_status).

      gpu_status: "ok" (query succeeded), "absent" (no nvidia-smi, or GPU queries
      disabled), "error" (this census's query failed) or "suspended" (no query this
      census because of earlier failures).
      """
      if not self._gpu:
         return [], {}, "absent"
      if self._gpu_resume_at is not None and time.monotonic() < self._gpu_resume_at:
         return [], {}, "suspended"
      self._gpu_resume_at = None
      if shutil.which("nvidia-smi") is None:
         return [], {}, "absent"
      gpus, gpu_mem_by_pid, error = query_gpus(timeout=self._gpu_timeout)
      if error is None:
         self._gpu_failure = None
         self._gpu_failure_streak = 0
         return gpus, gpu_mem_by_pid, "ok"
      if error == GPU_TIMEOUT_ERROR:
         self._gpu_failure = None
         self._gpu_failure_streak = 0
         self._suspend_gpu(error)
         return [], {}, "error"
      if error == self._gpu_failure:
         self._gpu_failure_streak += 1
      else:
         self._gpu_failure = error
         self._gpu_failure_streak = 1
      if self._gpu_failure_streak >= GPU_IDENTICAL_FAILURES_TO_SUSPEND:
         self._suspend_gpu("%s; %d identical failures" % (error, self._gpu_failure_streak))
      else:
         self._emitter.error("gpu", error)
      return [], {}, "error"

   def _suspend_gpu(self, reason):
      self._gpu_resume_at = time.monotonic() + self._gpu_suspend
      self._emitter.error("gpu", "%s; GPU queries suspended for %g s" % (reason, self._gpu_suspend))


def _watchdog(stdin, fd, parent, emitter):
   """Exit the process on stdin EOF, a parent change or a terminating signal, whatever
   the main loop is doing.

   The main loop can sit in write() for as long as the reader stalls, or in a slow
   census, so its own checks cannot be the only ones. The raw fd is read, not the
   buffered object: a thread parked inside BufferedReader.read holds its lock, which
   interpreter shutdown may then wait on. Bytes the bootstrap's buffer already pulled
   in do not matter; only EOF does.
   """
   try:
      # Leave terminating signals to the main thread. Python runs handlers only there,
      # and a main thread blocked in write() notices the flag only if the signal
      # interrupts that write.
      signal.pthread_sigmask(signal.SIG_BLOCK, [s for s, _ in _TERMINATING_SIGNALS])
   except (AttributeError, ValueError, OSError):
      pass
   try:
      while os.getppid() == parent:
         emitter.check_signal(PARENT_POLL_SEC)
         if fd is None:
            if stdin is None:
               time.sleep(PARENT_POLL_SEC)
               continue
            data = stdin.buffer.read(1)  # no fd to poll; the main loop checks the parent
         else:
            readable, _, _ = select.select([fd], [], [], PARENT_POLL_SEC)
            if not readable:
               continue
            try:
               data = os.read(fd, 4096)
            except BlockingIOError:
               continue  # non-blocking stdin (shared with ssh, say): spurious wakeup
         if data is None:
            time.sleep(PARENT_POLL_SEC)
            continue
         if not data:
            break
   except Exception:
      pass
   os._exit(0)


def _start_watchdog(stdin, parent, emitter):
   fd = None
   if stdin is not None:
      try:
         fd = stdin.fileno()
      except (AttributeError, ValueError, IOError, OSError):
         fd = None
   thread = threading.Thread(target=_watchdog, args=(stdin, fd, parent, emitter), name="watchdog")
   thread.daemon = True
   thread.start()


def _next_due(due, interval, now):
   # Skip slots missed during a slow collection instead of bursting to catch up.
   due += interval
   if due <= now:
      due += interval * (int((now - due) / interval) + 1) if interval > 0 else now - due
   return due


def stream(config, out=None, stdin=None):
   """Emit hello, then counters and census (both first at once) on their intervals.

   Runs until stdin reaches EOF or stdout breaks (the process exits 0 on the spot), the
   parent changes (returns 0), or SIGTERM, SIGHUP or SIGINT arrives (an `error` with
   where "signal" is emitted and the process exits 128 + signum, so a kill on the node
   is told apart from a transport failure). Returns 3 if the lock cannot be taken.
   """
   out = sys.stdout if out is None else out
   stdin = sys.stdin if stdin is None else stdin
   cfg = _resolve_config(config)
   # Login nodes may write unlimited cores into the cwd (a user's home); a probe that
   # crashes must not leave hundreds of MB behind.
   try:
      resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
   except (ValueError, OSError):
      pass
   # Orphans are re-parented to `systemd --user` (a subreaper), not pid 1, so the
   # test is "parent changed", not "parent is 1".
   parent = os.getppid()
   emitter = _Emitter(out)
   emitter.install_signal_handlers()
   # Held (fd open) for the life of the process; the kernel drops it when we exit.
   lock = ProbeLock(cfg["lock_path"])
   try:
      lock.acquire()
   except LockHeldByOther as exc:
      emitter.error("lock", str(exc))
      return 3
   except (IOError, OSError) as exc:
      emitter.error("lock", "%s: %s" % (cfg["lock_path"], _describe(exc)))
      return 3
   _start_watchdog(stdin, parent, emitter)
   roots = _roots(cfg)
   _emit_hello(emitter, cfg, roots)
   census = _Census(cfg, roots, emitter)
   counter_interval = float(cfg["counter_interval_sec"])
   census_interval = float(cfg["census_interval_sec"])
   next_counters = next_census = time.monotonic()
   while True:
      emitter.check_signal(PARENT_POLL_SEC)
      if os.getppid() != parent:
         return 0
      if time.monotonic() >= next_counters:
         _emit_counters(emitter, roots)
         next_counters = _next_due(next_counters, counter_interval, time.monotonic())
      if time.monotonic() >= next_census:
         census.emit()
         next_census = _next_due(next_census, census_interval, time.monotonic())
      wait = min(next_counters, next_census) - time.monotonic()
      if wait > 0:
         time.sleep(min(wait, PARENT_POLL_SEC))


def once(config, out=None):
   """Emit hello, counters and census once; no lock, no stdin watcher."""
   out = sys.stdout if out is None else out
   cfg = _resolve_config(config)
   roots = _roots(cfg)
   emitter = _Emitter(out)
   _emit_hello(emitter, cfg, roots)
   _emit_counters(emitter, roots)
   _Census(cfg, roots, emitter).emit()
   return 0


def main(argv=None):
   parser = argparse.ArgumentParser(
      description="node-monitor probe. Streaming is started by the collector's bootstrap; "
      "from a shell only --once is supported."
   )
   parser.add_argument("--once", action="store_true", help="emit hello, counters, census")
   parser.add_argument("--proc-root", default="/proc")
   parser.add_argument("--sys-root", default="/sys")
   args = parser.parse_args(argv)
   if not args.once:
      parser.error("--once is required")
   return once({"proc_root": args.proc_root, "sys_root": args.sys_root})


if __name__ == "__node_monitor_probe__":
   # The bootstrap exec's this source with PROBE_CONFIG defined in its globals.
   # os._exit like every other exit path: interpreter teardown and LD_PRELOAD exit
   # hooks (site job trackers such as XALT, seen aborting in their fini on a login
   # node) buy a probe nothing and can crash after the work is done.
   _code = stream(PROBE_CONFIG)  # noqa: F821
   for _stream in (sys.stdout, sys.stderr):
      try:
         _stream.flush()
      except Exception:
         pass
   os._exit(_code)

if __name__ == "__main__":
   sys.exit(main())
