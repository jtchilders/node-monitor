"""Synthetic /proc fixtures.

Every fixture here encodes a real hazard observed on the Polaris fleet, not
an invented edge case. Where a fixture exists because a specific parse bug is
possible, the docstring says which one.
"""

import os


# /proc/<pid>/stat field layout after the comm field (0-indexed into the
# post-comm tail): 0=state 1=ppid 4=tty_nr 11=utime 12=stime 19=starttime
# 21=rss_pages. Padded to 44 fields, which is what a real kernel emits.
def make_stat(pid, comm, state="S", ppid=1, tty_nr=0, utime=100, stime=50,
              starttime=98765, rss_pages=1024):
   tail = ["0"] * 44
   tail[0] = state
   tail[1] = str(ppid)
   tail[4] = str(tty_nr)
   tail[11] = str(utime)
   tail[12] = str(stime)
   tail[19] = str(starttime)
   tail[21] = str(rss_pages)
   return "%d (%s) %s\n" % (pid, comm, " ".join(tail))


def write_proc(root, pids, loadavg=None, meminfo=None, stat_line=None,
               uptime="340091.15 1234567.00\n", net_dev=None,
               sockstat=None):
   """Materialize a fake /proc tree.

   pids: list of dicts with keys pid, comm, cmdline, uid, and any make_stat
   override. cmdline=None means the file is absent (kernel thread).
   """
   os.makedirs(root, exist_ok=True)
   with open(os.path.join(root, "uptime"), "w") as handle:
      handle.write(uptime)
   with open(os.path.join(root, "loadavg"), "w") as handle:
      handle.write(loadavg or "53.92 54.96 55.88 42/6616 3371728\n")
   with open(os.path.join(root, "stat"), "w") as handle:
      handle.write(stat_line or
                   "cpu  24771747 4426210 308836914 8264041757 97248194 0 "
                   "5580295 0 0 0\ncpu0 1 2 3 4 5 6 7 0 0 0\n")
   with open(os.path.join(root, "meminfo"), "w") as handle:
      handle.write(meminfo or
                   "MemTotal:       527954108 kB\n"
                   "MemFree:        271989396 kB\n"
                   "MemAvailable:   403844552 kB\n"
                   "Buffers:             3216 kB\n"
                   "Cached:          45647368 kB\n"
                   "Shmem:            1996122 kB\n"
                   "SwapTotal:              0 kB\n"
                   "SwapFree:               0 kB\n")
   with open(os.path.join(root, "net_dev_src"), "w") as handle:
      handle.write(net_dev or "")
   os.makedirs(os.path.join(root, "net"), exist_ok=True)
   with open(os.path.join(root, "net", "dev"), "w") as handle:
      handle.write(net_dev or
                   "Inter-|   Receive                    |  Transmit\n"
                   " face |bytes    packets errs drop fifo frame compressed "
                   "multicast|bytes    packets errs drop fifo colls carrier\n"
                   "    lo: 100 1 0 0 0 0 0 0 100 1 0 0 0 0 0 0\n"
                   "  pub0: 5000 10 0 0 0 0 0 0 7000 12 0 0 0 0 0 0\n")
   with open(os.path.join(root, "net", "sockstat"), "w") as handle:
      handle.write(sockstat or
                   "sockets: used 1234\n"
                   "TCP: inuse 100 orphan 0 tw 5 alloc 200 mem 3\n")

   for spec in pids:
      pid = spec["pid"]
      pid_dir = os.path.join(root, str(pid))
      os.makedirs(pid_dir, exist_ok=True)
      with open(os.path.join(pid_dir, "stat"), "w") as handle:
         handle.write(make_stat(
            pid, spec["comm"],
            state=spec.get("state", "S"),
            ppid=spec.get("ppid", 1),
            tty_nr=spec.get("tty_nr", 0),
            utime=spec.get("utime", 100),
            stime=spec.get("stime", 50),
            starttime=spec.get("starttime", 98765),
            rss_pages=spec.get("rss_pages", 1024)))
      if spec.get("cmdline") is not None:
         with open(os.path.join(pid_dir, "cmdline"), "wb") as handle:
            handle.write(spec["cmdline"].replace(" ", "\x00").encode() + b"\x00")
   return root


# --------------------------------------------------------------------------
# hwinfo fixtures -- proc side
# --------------------------------------------------------------------------

def write_proc_hwinfo(root, cpuinfo=None, meminfo=None, osrelease="6.4.0\n",
                       mounts=None, boot_id="abc-123\n", stat_line=None,
                       nvidia_gpus=None):
   """Extend a /proc tree (built by write_proc or standalone) with the
   files the hwinfo loop reads that the counter/census fixtures above don't
   already write (cpuinfo, osrelease, mounts, boot_id) -- and overwrite
   meminfo/stat with hwinfo-relevant fields when given.

   `nvidia_gpus`: if given, writes /proc/driver/nvidia/gpus/<name> dirs so
   _collect_gpus finds them without ever shelling out to nvidia-smi -- the
   cheap path this project prefers when it is available.
   """
   os.makedirs(root, exist_ok=True)
   with open(os.path.join(root, "cpuinfo"), "w") as handle:
      handle.write(cpuinfo if cpuinfo is not None else (
         "processor\t: 0\n"
         "model name\t: AMD EPYC 7713 64-Core Processor\n"
         "physical id\t: 0\n"
         "cpu cores\t: 64\n\n"
         "processor\t: 1\n"
         "model name\t: AMD EPYC 7713 64-Core Processor\n"
         "physical id\t: 1\n"
         "cpu cores\t: 64\n\n"))
   if meminfo is not None:
      with open(os.path.join(root, "meminfo"), "w") as handle:
         handle.write(meminfo)
   os.makedirs(os.path.join(root, "sys", "kernel"), exist_ok=True)
   with open(os.path.join(root, "sys", "kernel", "osrelease"), "w") as handle:
      handle.write(osrelease)
   os.makedirs(
      os.path.join(root, "sys", "kernel", "random"), exist_ok=True)
   with open(
         os.path.join(root, "sys", "kernel", "random", "boot_id"),
         "w") as handle:
      handle.write(boot_id)
   with open(os.path.join(root, "mounts"), "w") as handle:
      handle.write(mounts if mounts is not None else (
         "/dev/sda1 / ext4 rw 0 0\n"
         "10.0.0.1:/export/home /home nfs4 rw 0 0\n"
         "lustre-mds@o2ib:/fs1 /lus/eagle lustre rw 0 0\n"))
   if stat_line is not None:
      with open(os.path.join(root, "stat"), "w") as handle:
         handle.write(stat_line)
   else:
      stat_path = os.path.join(root, "stat")
      existing = ""
      if os.path.exists(stat_path):
         with open(stat_path) as handle:
            existing = handle.read()
      if "btime " not in existing:
         with open(stat_path, "a") as handle:
            handle.write("btime 1700000000\n")
   if nvidia_gpus is not None:
      gpu_dir = os.path.join(root, "driver", "nvidia", "gpus")
      os.makedirs(gpu_dir, exist_ok=True)
      for name in nvidia_gpus:
         os.makedirs(os.path.join(gpu_dir, name), exist_ok=True)
   return root


# Sentinels for write_sys_hwinfo's net_speeds values. Ordinary values are an
# int (readable speed) or None (directory exists, no speed file -- already
# handled by plain ENOENT). These two model the two fleet-observed hazards
# neither of those covers -- see polaris-login-02 enumeration in the task
# write-up for _collect_net_ifaces.
IFACE_AS_FILE = "fake-proc-iface-as-file"
IFACE_UNREADABLE_SPEED = "fake-proc-iface-unreadable-speed"


def write_sys_hwinfo(root, cpu_max_freq_khz=2000000, numa_nodes=2,
                      net_speeds=None):
   """Materialize the /sys subtree the hwinfo loop reads.

   `cpu_max_freq_khz=None` omits the cpufreq file entirely, so tests can
   exercise the "sysfs absent -> NULL, never fall back to /proc/cpuinfo MHz"
   contract.
   """
   os.makedirs(root, exist_ok=True)
   if cpu_max_freq_khz is not None:
      cpufreq_dir = os.path.join(
         root, "devices", "system", "cpu", "cpu0", "cpufreq")
      os.makedirs(cpufreq_dir, exist_ok=True)
      with open(
            os.path.join(cpufreq_dir, "cpuinfo_max_freq"), "w") as handle:
         handle.write("%d\n" % cpu_max_freq_khz)
   node_dir = os.path.join(root, "devices", "system", "node")
   os.makedirs(node_dir, exist_ok=True)
   for i in range(numa_nodes):
      os.makedirs(os.path.join(node_dir, "node%d" % i), exist_ok=True)
   net_dir = os.path.join(root, "class", "net")
   os.makedirs(net_dir, exist_ok=True)
   for name, speed in (net_speeds or {"bond0": 1000, "ens10f0": 1000}).items():
      iface_path = os.path.join(net_dir, name)
      if speed is IFACE_AS_FILE:
         # bonding_masters on the real fleet: a plain bonding-driver control
         # file that sits directly under /sys/class/net alongside the real
         # interface directories, not an interface itself. Written as a
         # regular file, not a directory, so a caller that assumes every
         # listdir() entry is an interface dir gets NotADirectoryError when
         # it tries to open <this>/speed.
         with open(iface_path, "w") as handle:
            handle.write("bond0\n")
         continue
      os.makedirs(iface_path, exist_ok=True)
      if speed is IFACE_UNREADABLE_SPEED:
         # lo and down/unsupported interfaces: the speed file EXISTS but the
         # kernel returns EINVAL on read. os.path.exists() would say yes and
         # be wrong -- mode 000 reproduces "present but unreadable" without
         # needing to fake a specific kernel errno.
         speed_path = os.path.join(iface_path, "speed")
         with open(speed_path, "w") as handle:
            handle.write("0\n")
         os.chmod(speed_path, 0o000)
         continue
      if speed is not None:
         with open(os.path.join(iface_path, "speed"), "w") as handle:
            handle.write("%d\n" % speed)
      # speed is left absent (not written as "?") when None -- the real
      # kernel returns EINVAL on read for a down/unbonded interface, which
      # _read_text already maps to None; there is nothing to write.
   return root


# --- The corpus. Each entry: (cmdline, expected_category, why it is here) ---

CMDLINE_CORPUS = [
   # ide-remote -- the largest category on the fleet (529 procs, 58 GB)
   ("/home/u/.vscode-server/bin/abc/node /home/u/.vscode-server/bin/abc/out/server-main.js",
    "ide-remote"),
   ("/home/u/.cursor-server/bin/xyz/node --max-old-space-size=4096 server.js",
    "ide-remote"),

   # ai-coding-agent -- the category the whole project exists to quantify
   ("claude", "ai-coding-agent"),
   ("/soft/nodejs/bin/node /home/u/.npm/bin/claude --resume", "ai-coding-agent"),
   ("codex exec --model gpt-5", "ai-coding-agent"),
   ("aider --model sonnet", "ai-coding-agent"),

   # jupyter
   ("/soft/python/bin/python -m ipykernel_launcher -f /tmp/kernel-1.json",
    "jupyter"),
   ("/soft/python/bin/jupyter-lab --no-browser", "jupyter"),

   # pbs-client
   ("qstat -u someone", "pbs-client"),
   ("mpiexec -n 4 ./a.out", "pbs-client"),

   # data-xfer/vcs
   ("git status", "data-xfer/vcs"),
   ("rsync -av /lus/eagle/projects/foo/ dest/", "data-xfer/vcs"),

   # compute/build
   ("make -j16", "compute/build"),
   ("/opt/cray/pe/gcc/bin/gcc -O3 -c kernel.c", "compute/build"),
   ("cmake --build build", "compute/build"),

   # fs-scan
   ("find /lus/eagle/projects/foo -name '*.h5'", "fs-scan"),
   ("du -sh /home/u", "fs-scan"),

   # shell/session
   ("-bash", "shell/session"),
   ("sshd: someuser@pts/12", "shell/session"),
   ("tmux new-session", "shell/session"),

   # other -- must NOT be forced into a category
   ("/usr/libexec/some-unknown-daemon --flag", "other"),
]
