"""Synthetic /proc and /sys trees for probe tests."""

import os
from pathlib import Path

DEFAULT_MEMINFO = {
   "MemTotal": 527954108,
   "MemFree": 271989396,
   "MemAvailable": 403844552,
   "Cached": 45647368,
   "Shmem": 1996122,
   "SwapTotal": 0,
   "SwapFree": 0,
   "Hugepagesize": 2048,
}


def make_stat(
   pid,
   comm,
   state="S",
   ppid=1,
   tty_nr=0,
   utime=100,
   stime=50,
   starttime=98765,
   rss_pages=1024,
   sid=0,
):
   """Build a /proc/<pid>/stat line; tail[i] is proc(5) field i+3."""
   tail = ["0"] * 44
   tail[0] = state
   tail[1] = str(ppid)
   tail[3] = str(sid)
   tail[4] = str(tty_nr)
   tail[11] = str(utime)
   tail[12] = str(stime)
   tail[19] = str(starttime)
   tail[21] = str(rss_pages)
   return "%d (%s) %s\n" % (pid, comm, " ".join(tail))


def _write(path, text):
   path.parent.mkdir(parents=True, exist_ok=True)
   path.write_text(text)


def _write_pid(proc, spec):
   pid = spec["pid"]
   pid_dir = proc / str(pid)
   pid_dir.mkdir(parents=True, exist_ok=True)
   stat_keys = (
      "state", "ppid", "tty_nr", "utime", "stime", "starttime", "rss_pages", "sid",
   )  # fmt: skip
   overrides = {k: spec[k] for k in stat_keys if k in spec}
   _write(pid_dir / "stat", make_stat(pid, spec["comm"], **overrides))
   cmdline = spec.get("cmdline")
   if cmdline is not None:
      _write(pid_dir / "cmdline", "\0".join(cmdline.split(" ")) + "\0" if cmdline else "")
   uid = spec.get("uid", 1000)
   _write(
      pid_dir / "status",
      "Name:\t%s\nUid:\t%d\t%d\t%d\t%d\nThreads:\t%d\n"
      % (spec["comm"], uid, uid, uid, uid, spec.get("threads", 1)),
   )
   if spec.get("exe") is not None:
      os.symlink(spec["exe"], str(pid_dir / "exe"))


def write_tree(
   root,
   pids=(),
   *,
   loadavg=None,
   stat_cpu=None,
   btime=1760000000,
   uptime="340091.15 1234567.00\n",
   meminfo=None,
   net_dev=None,
   md_stats=None,
   sockstat_used=None,
   boot_id="boot-a",
   cpuinfo_processors=2,
   cpu_model="Test CPU",
   omit=(),
):
   """Materialize fake proc and sys trees under root; returns the roots dict.

   `omit` lists paths relative to the proc root that are left out.
   """
   root = Path(root)
   proc = root / "proc"
   sys_root = root / "sys"
   proc.mkdir(parents=True, exist_ok=True)
   sys_root.mkdir(parents=True, exist_ok=True)

   _write(proc / "uptime", uptime)
   _write(proc / "loadavg", loadavg or "53.92 54.96 55.88 42/6616 3371728\n")
   cpu_line = stat_cpu or "cpu  24771747 4426210 308836914 8264041757 97248194 0 5580295 0 0 0\n"
   _write(proc / "stat", cpu_line + "btime %d\n" % btime)

   mem = DEFAULT_MEMINFO if meminfo is None else meminfo
   _write(proc / "meminfo", "".join("%s: %d kB\n" % kv for kv in mem.items()))

   ifaces = {"lo": (1, 1), "hsn0": (100, 200)} if net_dev is None else net_dev
   rows = "".join(
      "%6s: %d 0 0 0 0 0 0 0 %d 0 0 0 0 0 0 0\n" % (name, rx, tx)
      for name, (rx, tx) in ifaces.items()
   )
   _write(proc / "net" / "dev", "Inter-|   Receive\n face |bytes\n" + rows)

   sockets = 100 if sockstat_used is None else sockstat_used
   _write(proc / "net" / "sockstat", "sockets: used %d\nTCP: inuse 5\n" % sockets)

   for target, ops in (md_stats or {}).items():
      body = "snapshot_time             1.0 secs.usecs\n"
      body += "".join("%s %d samples [reqs]\n" % (op, count) for op, count in ops.items())
      _write(proc / "fs" / "lustre" / "mdc" / target / "md_stats", body)
   (proc / "fs" / "lustre" / "mdc").mkdir(parents=True, exist_ok=True)

   _write(proc / "sys" / "kernel" / "random" / "boot_id", boot_id + "\n")
   _write(proc / "sys" / "kernel" / "osrelease", "5.14.21-test\n")
   _write(proc / "mounts", "x / ext4 rw 0 0\nmds:/e /e lustre rw 0 0\nnfs:/h /h nfs4 rw 0 0\n")
   cpuinfo = "".join(
      "processor\t: %d\nmodel name\t: %s\nphysical id\t: 0\ncpu cores\t: %d\n\n"
      % (i, cpu_model, cpuinfo_processors)
      for i in range(cpuinfo_processors)
   )
   _write(proc / "cpuinfo", cpuinfo)

   _write(
      sys_root / "devices" / "system" / "cpu" / "cpu0" / "cpufreq" / "cpuinfo_max_freq", "3700000\n"
   )
   (sys_root / "devices" / "system" / "node" / "node0").mkdir(parents=True, exist_ok=True)
   (sys_root / "class" / "net" / "hsn0").mkdir(parents=True, exist_ok=True)
   _write(sys_root / "class" / "net" / "hsn0" / "speed", "200000\n")
   _write(sys_root / "class" / "net" / "bonding_masters", "bond0\n")

   for spec in pids:
      _write_pid(proc, spec)

   for rel in omit:
      target = proc / rel
      if target.is_dir():
         import shutil

         shutil.rmtree(str(target))
      elif target.exists():
         target.unlink()

   return {"proc": str(proc), "sys": str(sys_root)}
