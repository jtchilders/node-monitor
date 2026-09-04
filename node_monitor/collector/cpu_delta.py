"""Cumulative-to-interval CPU deltas between two consecutive census samples.

`remote_probe.py` (by design -- see its module docstring) emits `utime_ticks`
and `stime_ticks` straight from `/proc/<pid>/stat`. Those counters are
CUMULATIVE since the process started, not per-interval. Summing them across a
15-minute census bin answers "how much CPU has this process used since it was
launched", which for a 20-hour Claude Code session is wildly wrong as an
interval measure -- one real Polaris row showed 19.7 cumulative core-hours
attributed to a single nominally-single-interval sample. This module is the
daemon-side fix: it takes two consecutive samples and returns the CPU
actually consumed *between* them.

Why key on (pid, start_time_ticks) instead of pid alone: pids are recycled by
the kernel, sometimes within a single census interval on a busy login node.
A recycled pid with unrelated utime/stime history produces a negative or
absurd delta if the two samples' rows are matched on pid alone.
`start_time_ticks` (the process's start time in ticks since boot, ints,
already present in the probe payload) disambiguates: two rows that share a
pid but differ in start_time_ticks are two different processes, one of which
exited and one of which is new, never a single continuously-running process.

Why an explicit `unmeasured` bucket instead of silently dropping rows we
can't delta: a process that exits between two census samples has no second
data point, so its final partial interval of CPU use is fundamentally
unmeasurable from these two samples alone. Dropping it silently would make
the daemon's CPU coverage look better than it is -- exactly the kind of
silent-zero footgun that motivated this module in the first place. Returning
it explicitly lets the daemon report what fraction of process-time it could
not account for, instead of asserting perfect coverage it does not have.

Known, permanent blind spot: any process that starts AND exits between two
census samples never appears in either sample and is invisible to this
function (and to the probe) entirely. No amount of cleverness at the delta
layer recovers CPU for a process the census never observed. This is an
acknowledged limitation of point-in-time /proc sampling, not a bug here.
"""


def _pid_key(row):
   """Compound key disambiguating recycled pids. See module docstring."""
   return (row["pid"], row["start_time_ticks"])


def _cpu_ticks(row):
   return row["utime_ticks"] + row["stime_ticks"]


def compute_cpu_delta(sample_a, sample_b, clk_tck):
   """Compute per-process CPU consumed between two consecutive census samples.

   sample_a, sample_b: each a dict with:
      "collected_at_ticks": int, the sample's collection time expressed in
         ticks since boot -- the same clock and unit as `start_time_ticks`,
         so a process's start can be compared directly against the window
         without a units conversion. (The daemon derives this from the
         probe's `uptime_sec` payload field: `uptime_sec * clk_tck`.)
      "processes": list of dicts, each with at least "pid", "start_time_ticks",
         "utime_ticks", "stime_ticks" -- the subset of a remote_probe.py
         process row this module needs.
   clk_tck: ticks-per-second for this node (from the probe's counters
      payload -- 100 on this fleet, but read from the payload and never
      hard-coded here; a login node's tick rate is a kernel config value,
      not a constant this module is entitled to assume).

   sample_a must have been collected before sample_b; this function does not
   itself verify sample ordering (it has no wall-clock timestamp to check
   against, only the caller's declared ticks-since-boot), so a caller that
   passes samples in the wrong order will see every delta reported as a
   negative-delta anomaly rather than a sensible result.

   Returns a dict:
      "deltas": list of {"pid", "start_time_ticks", "utime_delta_ticks",
         "stime_delta_ticks", "cpu_seconds"} for every process whose CPU
         consumption across the interval could be measured, whether it was
         present throughout or newly started inside the window.
      "unmeasured": list of {"pid", "start_time_ticks", "reason"} for
         processes whose interval CPU could NOT be attributed. "reason" is
         "exited" (present in sample_a, gone by sample_b -- its final
         partial interval has no second data point) or "started_before_window"
         (present only in sample_b, but its start_time_ticks predates
         sample_a's collection time, so some unknown fraction of its
         cumulative counters was already accrued before this window began
         and attributing the full cumulative value would overcount).
      "anomalies": list of {"pid", "start_time_ticks", "reason",
         "utime_delta_ticks", "stime_delta_ticks"} for any (pid,
         start_time_ticks) present in both samples where a delta computed
         negative. A negative delta on a correctly-keyed, continuously-running
         process is impossible under normal operation (cumulative counters do
         not go backwards) and signals a bug -- clock/counter wraparound, a
         probe defect, or bad input -- not a value to silently clamp to zero.
   """
   rows_a = {_pid_key(row): row for row in sample_a["processes"]}
   rows_b = {_pid_key(row): row for row in sample_b["processes"]}

   window_start = sample_a["collected_at_ticks"]
   window_end = sample_b["collected_at_ticks"]

   deltas = []
   unmeasured = []
   anomalies = []

   for key in rows_a.keys() & rows_b.keys():
      row_a = rows_a[key]
      row_b = rows_b[key]
      utime_delta = row_b["utime_ticks"] - row_a["utime_ticks"]
      stime_delta = row_b["stime_ticks"] - row_a["stime_ticks"]
      if utime_delta < 0 or stime_delta < 0:
         # Never clamp: a negative delta here means the ticks went backwards
         # for what this function believes is the SAME process (matched pid
         # AND start_time_ticks), which should be impossible. Surfacing it
         # lets the daemon flag the node/probe rather than silently reporting
         # a plausible-looking wrong number.
         anomalies.append({
            "pid": key[0],
            "start_time_ticks": key[1],
            "reason": "negative_delta",
            "utime_delta_ticks": utime_delta,
            "stime_delta_ticks": stime_delta,
         })
         continue
      deltas.append({
         "pid": key[0],
         "start_time_ticks": key[1],
         "utime_delta_ticks": utime_delta,
         "stime_delta_ticks": stime_delta,
         "cpu_seconds": (utime_delta + stime_delta) / clk_tck,
      })

   for key in rows_b.keys() - rows_a.keys():
      row_b = rows_b[key]
      start_time_ticks = key[1]
      if window_start <= start_time_ticks <= window_end:
         # Genuinely new: born inside this window, so its whole cumulative
         # counter value (there is no prior sample to subtract) IS its
         # interval consumption.
         deltas.append({
            "pid": key[0],
            "start_time_ticks": start_time_ticks,
            "utime_delta_ticks": row_b["utime_ticks"],
            "stime_delta_ticks": row_b["stime_ticks"],
            "cpu_seconds": _cpu_ticks(row_b) / clk_tck,
         })
      else:
         # Started before this window opened but absent from sample_a --
         # e.g. filtered out of the earlier census by a transient read
         # failure. We cannot tell how much of its cumulative CPU predates
         # the window, so attributing the full counter would overcount.
         unmeasured.append({
            "pid": key[0],
            "start_time_ticks": start_time_ticks,
            "reason": "started_before_window",
         })

   for key in rows_a.keys() - rows_b.keys():
      # Exited between the two samples. Its final partial interval has no
      # second data point and is a permanent, unrecoverable blind spot for
      # short-lived commands -- see module docstring. Reported explicitly,
      # never dropped, so the daemon can compute real coverage.
      unmeasured.append({
         "pid": key[0],
         "start_time_ticks": key[1],
         "reason": "exited",
      })

   return {
      "deltas": deltas,
      "unmeasured": unmeasured,
      "anomalies": anomalies,
   }
