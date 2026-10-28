"""Remote probe: reads /proc and /sys on a login node and emits raw fields.

Single file, stdlib only, Python 3.6 compatible. It is shipped to nodes as
source text, so importing it must have no side effects.
"""

import errno
import os
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
