"""node_monitor.collector.metrics -- counter deltas and 60-second rollups.

Design: PHASE0_DAEMON_DESIGN.md "Metric semantics" + "Output contract".
Two layers, matching the file map's task write-up (Task 4):

* ``compute_counter_delta`` is a pure function over one PAIR of
  consecutive raw counter-loop probe payloads (the exact JSON shape
  ``remote_probe.py``'s ``main()`` prints for ``loop="counter"``, i.e.
  ``uptime_sec`` + ``counters``). It never sees a whole window, has no
  state, and returns a rate/validity dict for that one pair only.
* ``CounterWindowAccumulator`` is bounded per-node state for one
  60-second window: it consumes raw samples one at a time (deltaing
  each new sample against the previous one via ``compute_counter_delta``
  internally), keeps only the small distributions/gauges the design's
  `node_counter_samples` contract needs, and ``finalize()`` produces a
  dict shaped exactly like that contract -- this module never invents
  its own intermediate shape that the sink would have to translate.

Everything here mirrors cpu_delta.py's own rules, restated for counters
rather than per-process CPU:

* No rate spans a reboot, boot-ID change, reset, negative counter, or an
  elapsed interval that is not strictly positive (design: "Metric
  semantics" first bullet). ``compute_counter_delta`` marks a pair
  ``invalid_reason="reboot_or_reset"`` (uptime went backwards or is
  unchanged) or ``invalid_reason="boot_id_change"`` (an explicit
  boot-identity field differs between the two samples, which can happen
  even when uptime_sec still looks monotonically increasing across a
  very fast reboot within one probe interval) and returns null rates
  rather than ever clamping a negative delta to zero.
* A counter series is independent of every other series: an interface
  reset excludes only that interface, a Lustre target/operation reset
  excludes only that (target, operation) pair, and a CPU-jiffy reset
  nulls only ``cpu_busy_pct`` -- one broken series never blinds the
  others, matching cpu_delta.py's "anomalies never take down the whole
  sample" philosophy.
* Nothing is ever fabricated. A newly-appeared interface/target with no
  prior sample to diff against is skipped for that pair, not seeded
  with a zero baseline that would produce a bogus first non-zero rate
  on the NEXT pair.
"""

import statistics


# --------------------------------------------------------------------------
# compute_counter_delta -- pure pair-delta function
# --------------------------------------------------------------------------

# Design: "CPU busy percentage ... derive from aggregate /proc/stat jiffy
# deltas; load average is never called CPU utilization." iowait is CPU
# sitting idle waiting on I/O -- explicitly NOT busy time -- so it joins
# idle in the denominator-only set, matching remote_probe.py's own
# _collect_cpu_jiffies field names.
_CPU_BUSY_FIELDS = ("user", "nice", "system", "irq", "softirq", "steal")
_CPU_IDLE_FIELDS = ("idle", "iowait")
_CPU_ALL_FIELDS = _CPU_BUSY_FIELDS + _CPU_IDLE_FIELDS

_LOOPBACK_IFACE = "lo"


def _get_counters(sample):
   return sample.get("counters") or {}


def _raw_cumulative_snapshot(sample):
   """Bounded raw-cumulative snapshot of one sample, for the audit object.

   Design: "Raw cumulative values and validity diagnostics remain in a
   bounded audit object" (PHASE0_DAEMON_DESIGN.md "Output contract").
   This captures exactly the cumulative fields ``compute_counter_delta``
   itself deltas -- ``cpu_jiffies``, per-interface ``net`` byte counters,
   and per-target/operation ``md_ops`` counts -- plus ``uptime_sec`` and
   ``boot_id`` as the boundary/identity context needed to interpret them,
   so a human auditing a finalized record can independently recompute
   (or sanity-check) any reported rate from the two boundary snapshots
   alone. It is always exactly one sample's worth of data (never a list
   growing with sample count), which is what keeps the accumulator's
   audit state bounded regardless of how many samples a window receives.
   """
   counters = _get_counters(sample)
   return {
      "uptime_sec": sample.get("uptime_sec"),
      "boot_id": sample.get("boot_id"),
      "cpu_jiffies": counters.get("cpu_jiffies"),
      "net": counters.get("net"),
      "md_ops": counters.get("md_ops"),
   }


def _compute_cpu_busy_pct(counters_a, counters_b):
   jiffies_a = counters_a.get("cpu_jiffies")
   jiffies_b = counters_b.get("cpu_jiffies")
   if jiffies_a is None or jiffies_b is None:
      return None

   busy_delta = 0
   total_delta = 0
   for field in _CPU_ALL_FIELDS:
      value_a = jiffies_a.get(field)
      value_b = jiffies_b.get(field)
      if value_a is None or value_b is None:
         # Some /proc/stat fields (irq, softirq, steal, ...) are absent on
         # older kernels or partial fixtures. A field missing from either
         # sample contributes nothing to either side of the ratio rather
         # than invalidating the whole aggregate -- it is genuinely absent
         # data, not a counter that went backwards.
         continue
      delta = value_b - value_a
      if delta < 0:
         # Never clamp: jiffy counters only increase between two normal
         # samples. A decrease here means a reset this function did not
         # already catch via the whole-pair reboot check (e.g. a per-CPU
         # counter reset independent of uptime), so the whole CPU rate
         # for this pair is unknowable, not "mostly right".
         return None
      total_delta += delta
      if field in _CPU_BUSY_FIELDS:
         busy_delta += delta

   if total_delta <= 0:
      return None
   return 100.0 * busy_delta / total_delta


def _compute_network_deltas(counters_a, counters_b, elapsed_sec):
   net_a = counters_a.get("net") or {}
   net_b = counters_b.get("net") or {}
   out = {}
   for iface, stats_b in net_b.items():
      if iface == _LOOPBACK_IFACE:
         # Design: "Loopback is excluded from external-traffic totals."
         continue
      stats_a = net_a.get(iface)
      if stats_a is None:
         # Interface newly appeared this pair -- no prior sample to diff
         # against. Skipped, not seeded at zero (see module docstring).
         continue
      rx_a, tx_a = stats_a.get("rx_bytes"), stats_a.get("tx_bytes")
      rx_b, tx_b = stats_b.get("rx_bytes"), stats_b.get("tx_bytes")
      if None in (rx_a, tx_a, rx_b, tx_b):
         continue
      rx_delta = rx_b - rx_a
      tx_delta = tx_b - tx_a
      if rx_delta < 0 or tx_delta < 0:
         # Counter reset on this interface only -- excluded, the rest of
         # the interfaces in this pair are unaffected.
         continue
      out[iface] = {
         "rx_bytes_per_sec": rx_delta / elapsed_sec,
         "tx_bytes_per_sec": tx_delta / elapsed_sec,
      }
   return out


def _compute_lustre_deltas(counters_a, counters_b, elapsed_sec):
   md_a = counters_a.get("md_ops") or {}
   md_b = counters_b.get("md_ops") or {}
   out = {}
   for target, ops_b in md_b.items():
      ops_a = md_a.get(target)
      if ops_a is None:
         # Target newly seen this pair -- nothing to diff against yet.
         continue
      target_rates = {}
      for op, count_b in ops_b.items():
         if op == "total":
            # "total" is a derived sum remote_probe.py adds for
            # convenience, not an independent operation counter -- rolling
            # it up here would double the target's own delta signal.
            continue
         count_a = ops_a.get(op)
         if count_a is None:
            continue
         delta = count_b - count_a
         if delta < 0:
            # Reset on this one operation only; every other operation for
            # this target (and every other target) is unaffected.
            continue
         target_rates[op] = delta / elapsed_sec
      if target_rates:
         out[target] = target_rates
   return out


def compute_counter_delta(sample_a, sample_b):
   """Compute per-series rates between two consecutive counter samples.

   ``sample_a``, ``sample_b``: raw counter-loop probe payloads (each the
   full JSON ``remote_probe.py``'s ``main()`` produces for
   ``loop="counter"``), with at least ``uptime_sec`` and ``counters``.
   Passed through unmodified from decoded probe JSON, same contract as
   ``cpu_delta.compute_cpu_delta``.

   Returns a dict:
      "elapsed_sec": remote uptime delta, or None if invalid.
      "invalid_reason": None, or "missing_uptime" / "reboot_or_reset" /
         "boot_id_change" -- set whenever no rate in this pair can be
         trusted at all.
      "cpu_busy_pct": aggregate CPU busy percentage for the interval, or
         None if unknowable (missing/reset jiffies, or the whole pair is
         invalid).
      "network": {iface: {"rx_bytes_per_sec", "tx_bytes_per_sec"}} for
         every interface present in both samples with a non-negative
         delta. Loopback is never included. Never contains an interface
         whose counters reset or that is missing from either sample.
      "lustre_md_ops": {target: {op: rate_per_sec}} with the same
         per-target/per-operation reset independence as network.

   Design: "No rate spans reboot, boot-ID change, reset, negative
   counter, or invalid elapsed interval." A negative or zero
   ``uptime_sec`` delta invalidates the WHOLE pair (every rate in it is
   untrustworthy, since the shared time denominator itself is broken);
   an explicit boot-identity change between the two samples invalidates
   the whole pair the same way, even if ``uptime_sec`` still looks like
   it increased (a fast reboot can land inside one probe interval and
   still leave uptime_sec monotonic-looking by coincidence); a reset on
   one field/interface/target invalidates only that one series, per
   cpu_delta.py's compute_cpu_delta precedent of scoping anomalies to
   what they actually affect.
   """
   uptime_a = sample_a.get("uptime_sec")
   uptime_b = sample_b.get("uptime_sec")
   if uptime_a is None or uptime_b is None:
      return {
         "elapsed_sec": None,
         "invalid_reason": "missing_uptime",
         "cpu_busy_pct": None,
         "network": {},
         "lustre_md_ops": {},
      }

   # Design: "No rate spans reboot, boot-ID change, reset...". Checked
   # ahead of the elapsed-interval check on purpose: an explicit
   # boot-identity mismatch is a stronger, independent signal than the
   # uptime_sec heuristic and must invalidate the pair even in the (rare)
   # case a fast reboot leaves uptime_sec looking monotonically
   # increasing. A sample missing the field entirely (most callers today,
   # since remote_probe.py's counter loop does not yet emit it) is simply
   # not compared -- this is an optional input, not a required one.
   boot_id_a = sample_a.get("boot_id")
   boot_id_b = sample_b.get("boot_id")
   if boot_id_a is not None and boot_id_b is not None and boot_id_a != boot_id_b:
      return {
         "elapsed_sec": None,
         "invalid_reason": "boot_id_change",
         "cpu_busy_pct": None,
         "network": {},
         "lustre_md_ops": {},
      }

   elapsed_sec = uptime_b - uptime_a
   if elapsed_sec <= 0:
      # Design: "No rate spans reboot ... or invalid elapsed interval."
      # uptime_sec decreasing is the reboot signature (a node's
      # since-boot clock cannot go backwards except across a reboot);
      # zero elapsed is a duplicate/out-of-order sample with no positive
      # denominator to divide by. Either way the WHOLE pair is untrusted.
      return {
         "elapsed_sec": None,
         "invalid_reason": "reboot_or_reset",
         "cpu_busy_pct": None,
         "network": {},
         "lustre_md_ops": {},
      }

   counters_a = _get_counters(sample_a)
   counters_b = _get_counters(sample_b)

   return {
      "elapsed_sec": elapsed_sec,
      "invalid_reason": None,
      "cpu_busy_pct": _compute_cpu_busy_pct(counters_a, counters_b),
      "network": _compute_network_deltas(counters_a, counters_b, elapsed_sec),
      "lustre_md_ops": _compute_lustre_deltas(counters_a, counters_b, elapsed_sec),
   }


# --------------------------------------------------------------------------
# CounterWindowAccumulator -- bounded per-node 60-second rollup
# --------------------------------------------------------------------------

# Design: "Each complete minute rollup has at least five valid counter
# samples" (canary acceptance criteria). Five, not "sample_count > 0" --
# a window with 1-4 samples is degraded and must say so explicitly rather
# than silently reporting a rollup with too few points to be meaningful.
_MINIMUM_VALID_SAMPLES = 5

# End-of-window gauges taken verbatim from the LAST sample of the window
# (never deltaed -- these are point-in-time readings, not counters).
# Keys here are the node_counter_samples "end_of_window" fields; values
# are (counters-dict-key, nested-key-or-None) telling _extract_gauges
# where to read each one from the raw counter payload.
_GAUGE_SPECS = (
   ("mem_available_kb", "mem", "available_kb"),
   ("cached_kb", "mem", "cached_kb"),
   ("shmem_kb", "mem", "shmem_kb"),
   ("load1", None, "load1"),
   ("load5", None, "load5"),
   ("load15", None, "load15"),
   ("procs_running", None, "procs_running"),
   ("procs_total", None, "procs_total"),
   ("socket_count", None, "socket_count"),
)


def _extract_gauges(counters):
   out = {}
   for gauge_key, nested_key, source_key in _GAUGE_SPECS:
      if nested_key is None:
         out[gauge_key] = counters.get(source_key)
      else:
         nested = counters.get(nested_key) or {}
         out[gauge_key] = nested.get(source_key)
   return out


def _percentile_stats(values):
   """{"p50", "p95", "max"} over ``values``, or None if empty.

   Design: "rollups include p50, p95, and max because bursts matter."
   ``statistics.quantiles`` needs at least 2 data points for n=100
   interpolation; with exactly one value every quantile equals that
   value, which is also the mathematically correct p50/p95/max for a
   single-point distribution, so that case is handled directly rather
   than as a special case with different semantics.
   """
   if not values:
      return None
   if len(values) == 1:
      only = values[0]
      return {"p50": only, "p95": only, "max": only}
   sorted_values = sorted(values)
   quantiles = statistics.quantiles(sorted_values, n=100, method="inclusive")
   return {
      "p50": quantiles[49],
      "p95": quantiles[94],
      "max": sorted_values[-1],
   }


class CounterWindowAccumulator:
   """Bounded per-(node) state for one 60-second counter-rollup window.

   Consumes raw counter-loop probe payloads one at a time via
   ``add_sample`` (in collection order) and produces exactly one
   ``node_counter_samples`` record via ``finalize``. State held between
   calls is bounded by ``expected_count`` regardless of how many times
   ``add_sample`` is actually called: at most ``expected_count`` samples
   ever count toward ``sample_count``/coverage, so at most
   ``expected_count - 1`` per-pair deltas ever grow the rate lists or the
   invalid-pairs audit list. A scheduler bug, duplicate delivery, or a
   replayed sample cannot make this accumulator's memory grow without
   bound, and cannot inflate ``finalize()``'s coverage past 1.0 (the
   ``node_counter_samples`` contract requires coverage in [0, 1]).
   Samples received once that cap is reached are "excess": they still
   update the end-of-window gauges (design: gauges come from the true
   LAST sample of the window, so an excess sample cannot be silently
   ignored for that purpose) and a bounded excess counter, but
   contribute no rate, no invalid-pair entry, and no sample_count.

   Design: "Raw cumulative values and validity diagnostics remain in a
   bounded audit object" -- the ``audit`` field on the finalized record
   carries summary counts/reasons plus ``raw_cumulative``: exactly TWO
   raw-counter snapshots (the window's first accepted sample and the
   last accepted sample that actually contributed to rate computation),
   each reduced to only the cumulative fields the rate math itself uses
   (``uptime_sec``, ``boot_id``, ``cpu_jiffies``, ``net``, ``md_ops``)
   so the record is independently auditable -- a reviewer can recompute
   any reported rate from these two snapshots alone -- without ever
   growing with sample count. An excess sample (beyond
   ``expected_count``) never moves ``window_last`` forward, since it
   contributes no rate; it only updates ``end_of_window``, whose
   point-in-time gauges are deliberately drawn from the true last
   sample of the window (tracked separately) and are likewise always
   exactly one sample's worth of data.
   """

   def __init__(self, system, source_hostname, collector_hostname,
                probe_version, daemon_version, window_start_utc,
                window_end_utc, expected_count):
      self._system = system
      self._source_hostname = source_hostname
      self._collector_hostname = collector_hostname
      self._probe_version = probe_version
      self._daemon_version = daemon_version
      self._window_start_utc = window_start_utc
      self._window_end_utc = window_end_utc
      self._expected_count = expected_count

      self._sample_count = 0
      self._excess_sample_count = 0
      self._previous_sample = None
      self._last_sample = None
      self._first_sample = None

      self._cpu_busy_values = []
      # {iface: {field: [values]}} -- per-interface field lists, built up
      # pair by pair; bounded because at most expected_count - 1 pairs
      # are ever accepted (see _at_capacity), regardless of how many
      # times add_sample is called.
      self._network_values = {}
      self._lustre_values = {}
      self._invalid_pairs = []

   def _at_capacity(self):
      # expected_count is contract-validated to be >= 0 (never negative);
      # zero means the window expected no samples at all, so the very
      # first call is already "excess" under this same check -- there is
      # no separate zero-expected special case to maintain.
      return self._sample_count >= self._expected_count

   def add_sample(self, sample):
      """Feed one raw counter-loop probe payload, in collection order."""
      if self._at_capacity():
         # Excess sample: bounded accumulator state must not grow past
         # what expected_count already sized it for. Still the true last
         # sample chronologically, so the end-of-window gauges (read from
         # self._last_sample in finalize()) must still reflect it.
         self._excess_sample_count += 1
         self._last_sample = sample
         return

      self._sample_count += 1
      self._last_sample = sample
      if self._first_sample is None:
         # Only ever set once, on the first accepted sample of the
         # window -- bounded regardless of sample count.
         self._first_sample = sample

      if self._previous_sample is not None:
         delta = compute_counter_delta(self._previous_sample, sample)
         if delta["invalid_reason"] is not None:
            self._invalid_pairs.append({"reason": delta["invalid_reason"]})
         else:
            if delta["cpu_busy_pct"] is not None:
               self._cpu_busy_values.append(delta["cpu_busy_pct"])
            for iface, rates in delta["network"].items():
               bucket = self._network_values.setdefault(
                  iface, {"rx_bytes_per_sec": [], "tx_bytes_per_sec": []})
               bucket["rx_bytes_per_sec"].append(rates["rx_bytes_per_sec"])
               bucket["tx_bytes_per_sec"].append(rates["tx_bytes_per_sec"])
            for target, ops in delta["lustre_md_ops"].items():
               target_bucket = self._lustre_values.setdefault(target, {})
               for op, rate in ops.items():
                  target_bucket.setdefault(op, []).append(rate)

      self._previous_sample = sample

   def finalize(self):
      """Produce the ``node_counter_samples`` record for this window.

      Always returns a record that
      ``node_monitor.output.contracts.validate_node_counter_samples``
      accepts, even for a window with zero samples -- design: "one row
      per node per complete 60-second window" means the row exists
      regardless of how degraded the window was; the record's own
      fields (``sample_count``, ``coverage``, null rates) carry that
      degradation, rather than the caller having to special-case an
      empty window into no row at all.
      """
      counters = _get_counters(self._last_sample) if self._last_sample else {}
      end_of_window = _extract_gauges(counters) if self._last_sample else \
         _extract_gauges({})

      coverage = (
         self._sample_count / self._expected_count
         if self._expected_count else 0.0)

      rates = {
         "cpu_busy_pct": _percentile_stats(self._cpu_busy_values),
         "network": {
            iface: {
               "rx_bytes_per_sec": _percentile_stats(fields["rx_bytes_per_sec"]),
               "tx_bytes_per_sec": _percentile_stats(fields["tx_bytes_per_sec"]),
            }
            for iface, fields in self._network_values.items()
         },
         "lustre_md_ops": {
            target: {
               op: _percentile_stats(values) for op, values in ops.items()
            }
            for target, ops in self._lustre_values.items()
         },
      }

      audit = {
         "meets_minimum_samples": self._sample_count >= _MINIMUM_VALID_SAMPLES,
         "invalid_pairs": list(self._invalid_pairs),
         "excess_sample_count": self._excess_sample_count,
         "raw_cumulative": {
            "window_first": (
               _raw_cumulative_snapshot(self._first_sample)
               if self._first_sample else None),
            "window_last": (
               _raw_cumulative_snapshot(self._previous_sample)
               if self._previous_sample else None),
         },
      }

      return {
         "system": self._system,
         "source_hostname": self._source_hostname,
         "collector_hostname": self._collector_hostname,
         "probe_version": self._probe_version,
         "daemon_version": self._daemon_version,
         "window_start_utc": self._window_start_utc,
         "window_end_utc": self._window_end_utc,
         "sample_count": self._sample_count,
         "expected_count": self._expected_count,
         "coverage": coverage,
         "end_of_window": end_of_window,
         "rates": rates,
         "audit": audit,
      }
