"""Remote probe: reads /proc and /sys on a login node and emits raw fields.

Single file, stdlib only, Python 3.6 compatible. It is shipped to nodes as
source text, so importing it must have no side effects.
"""

import errno
import os
import pwd
import re
import shutil
import subprocess

PROBE_VERSION = 1

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
   try:
      result = subprocess.run(
         [binary, "--query-gpu=name", "--format=csv,noheader"],
         stdout=subprocess.PIPE,
         stderr=subprocess.PIPE,
         universal_newlines=True,
         timeout=5,
      )
   except Exception:
      return []
   if result.returncode != 0:
      return []
   return [line.strip() for line in result.stdout.split("\n") if line.strip()]


def _collect_gpus(roots):
   # Cheap /proc listing first so GPU-less nodes never spawn a subprocess.
   try:
      entries = sorted(os.listdir(_proc(roots, "driver", "nvidia", "gpus")))
   except (IOError, OSError):
      entries = []
   if entries:
      return entries
   return _collect_gpus_nvidia_smi()


def collect_hardware(roots):
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
      "gpus": _collect_gpus(roots),
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


def _run_nvidia_smi(binary, query_flag, timeout):
   result = subprocess.run(
      [binary, query_flag, "--format=csv,noheader,nounits"],
      stdout=subprocess.PIPE,
      stderr=subprocess.PIPE,
      universal_newlines=True,
      timeout=timeout,
   )
   if result.returncode != 0:
      raise RuntimeError("nvidia-smi exit %d" % result.returncode)
   return [
      [cell.strip() for cell in line.split(",")]
      for line in result.stdout.split("\n")
      if line.strip()
   ]


def query_gpus(timeout=5.0):
   """Return (gpus, gpu_mem_by_pid, error); error is None when nvidia-smi is absent."""
   binary = shutil.which("nvidia-smi")
   if binary is None:
      return [], {}, None
   try:
      gpus = []
      for cells in _run_nvidia_smi(
         binary, "--query-gpu=index,utilization.gpu,memory.used,memory.total", timeout
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
      for cells in _run_nvidia_smi(binary, "--query-compute-apps=pid,used_memory", timeout):
         pid, used = cells
         by_pid[int(pid)] = by_pid.get(int(pid), 0) + int(used)
   except subprocess.TimeoutExpired:
      return [], {}, "nvidia-smi timeout"
   except RuntimeError as exc:
      return [], {}, str(exc)
   except (ValueError, OSError):
      return [], {}, "nvidia-smi output unparseable"
   return gpus, by_pid, None
