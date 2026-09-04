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
