"""Manual check: the streaming probe's resident size over many censuses of the real /proc.

Not collected by pytest (no test_ prefix). Run it alone, with the site preload removed:

   env -u LD_PRELOAD python3 tests/probe/manual_rss_series.py [--python /usr/bin/python3.11]

The probe runs through its real bootstrap entry point (stream(), a pipe on stdout, GPU
queries off, its lock under /tmp). VmRSS is sampled from /proc/<pid>/status after the
first census and every --every censuses; the summary compares the rest of the run with the
sample at census 30. VmRSS comes from per-CPU counters that the kernel reads approximately
(it moves in steps of MiB on a many-core node), so the exact Rss from smaps_rollup, which
walks the page tables, is sampled alongside it.
"""

import argparse
import json
import os
import select
import signal
import subprocess
import sys
import tempfile
import time

PROBE = os.path.normpath(
   os.path.join(
      os.path.dirname(os.path.abspath(__file__)), "..", "..", "node_monitor", "probe", "probe.py"
   )
)
# The marker line puts it in the probe's cmdline, as the collector's bootstrap does.
STREAM_CODE = (
   "#node-monitor-probe\n"
   "import runpy,sys,json\n"
   "runpy.run_path(%r, run_name='__node_monitor_probe__',\n"
   "   init_globals={'PROBE_CONFIG': json.loads(sys.argv[1])})\n"
)
CENSUS_PREFIX = b'{"type":"census"'
BYE_PREFIX = b'{"type":"bye"'


def field_kb(path, field):
   with open(path) as handle:
      for line in handle:
         if line.startswith(field + ":"):
            return int(line.split()[1])
   return None


def sample(pid):
   status = "/proc/%d/status" % pid
   return {
      "vmrss": field_kb(status, "VmRSS"),
      "hwm": field_kb(status, "VmHWM"),
      "rss": field_kb("/proc/%d/smaps_rollup" % pid, "Rss"),
   }


def summarize(samples, key, label):
   reference = samples.get(30, {}).get(key)
   if reference is None:
      return
   worst = max((abs(s[key] - reference) for n, s in samples.items() if n > 30), default=0)
   print(
      "%s at census 30: %.1f MiB; largest later deviation %.1f MiB (bound 8 MiB): %s"
      % (label, reference / 1024.0, worst / 1024.0, "PASS" if worst <= 8 * 1024 else "FAIL")
   )


def main():
   parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
   parser.add_argument("--python", default=sys.executable, help="interpreter for the probe")
   parser.add_argument("--probe", default=PROBE, help="probe source (e.g. an older revision)")
   parser.add_argument("--censuses", type=int, default=300)
   parser.add_argument("--interval", type=float, default=1.0, help="census interval, s")
   parser.add_argument("--every", type=int, default=30, help="sample every N censuses")
   args = parser.parse_args()

   lock_dir = tempfile.mkdtemp(prefix="nm-rss-series-")
   config = {
      "proc_root": "/proc",
      "sys_root": "/sys",
      "gpu": False,
      "census_interval_sec": args.interval,
      "lock_path": os.path.join(lock_dir, "probe.lock"),
      # Out of the way: this run measures growth, it does not test the limit.
      "probe_rss_limit_mb": 4096,
   }
   env = {k: v for k, v in os.environ.items() if k != "LD_PRELOAD"}
   proc = subprocess.Popen(
      [args.python, "-u", "-c", STREAM_CODE % args.probe, json.dumps(config)],
      stdin=subprocess.PIPE,
      stdout=subprocess.PIPE,
      env=env,
      start_new_session=True,
   )
   print("probe %s, pid %d, python %s" % (args.probe, proc.pid, args.python))
   print("interval %g s" % args.interval)
   header = ("census", "elapsed_s", "VmRSS_KiB", "VmHWM_KiB", "Rss_KiB", "line_bytes")
   print("%8s %9s %10s %10s %10s %12s" % header)
   samples = {}
   censuses = 0
   buf = b""
   start = time.monotonic()
   try:
      fd = proc.stdout.fileno()
      while censuses < args.censuses:
         ready, _, _ = select.select([fd], [], [], 120)
         if not ready:
            raise SystemExit("no output for 120 s")
         chunk = os.read(fd, 1 << 20)
         if not chunk:
            raise SystemExit("probe exited (status %s)" % proc.wait())
         buf += chunk
         *lines, buf = buf.split(b"\n")
         for line in lines:
            if line.startswith(BYE_PREFIX):
               raise SystemExit("probe said bye: %s" % line.decode("ascii"))
            if not line.startswith(CENSUS_PREFIX):
               continue
            censuses += 1
            if censuses == 1 or censuses % args.every == 0:
               now = sample(proc.pid)
               samples[censuses] = now
               elapsed = time.monotonic() - start
               print(
                  "%8d %9.1f %10d %10d %10d %12d"
                  % (censuses, elapsed, now["vmrss"], now["hwm"], now["rss"], len(line))
               )
               sys.stdout.flush()
   finally:
      try:
         os.killpg(proc.pid, signal.SIGKILL)
      except OSError:
         pass
      proc.wait()
      try:
         os.unlink(config["lock_path"])
         os.rmdir(lock_dir)
      except OSError:
         pass
   summarize(samples, "vmrss", "VmRSS")
   summarize(samples, "rss", "Rss (smaps_rollup)")


if __name__ == "__main__":
   main()
