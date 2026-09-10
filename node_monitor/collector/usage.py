"""node_monitor.collector.usage -- census transformation and 15-minute
usage aggregation.

Design: PHASE0_DAEMON_DESIGN.md "Output contract" (``node_usage_intervals``,
``diagnostic_census``) + "Metric semantics" (process identity, CPU
attribution) + Task 5 write-up in PHASE0_DAEMON_IMPLEMENTATION_PLAN.md.
Two layers, matching the file map's task write-up:

* ``build_diagnostic_census`` is a pure function over ONE raw census-loop
  probe payload (the exact JSON shape ``remote_probe.py``'s ``main()``
  prints for ``loop="census"``) plus the daemon-known ``system``/
  ``source_hostname`` identity and an optional pre-computed
  ``cpu_delta.compute_cpu_delta`` result against the previous sample. It
  privacy-filters every process row -- raw argv/cmdline is stripped here
  regardless of what the probe payload happens to contain, because this
  function is the last checkpoint before a census-derived record reaches
  the sink (same rationale as ``output.contracts``'s own
  ``_reject_forbidden_argv_keys``: never rely solely on an upstream
  default). It never sees a window or state; it returns exactly one
  ``diagnostic_censuses`` record for the one payload it was given.
* ``UsageIntervalAccumulator`` is bounded per-node state for one
  15-minute window; it consumes CPU-delta results (each already computed
  by ``cpu_delta.compute_cpu_delta`` between two consecutive census
  payloads) one at a time via ``add_sample``, and ``finalize()`` produces
  one ``node_usage_intervals`` record per ``(category, activity,
  username)`` group observed during the window -- matching the planned
  database grain (design: "one row per (node, 15-minute interval,
  category, activity, username)").
"""

import statistics

from node_monitor.collector.cpu_delta import compute_cpu_delta


# --------------------------------------------------------------------------
# build_diagnostic_census -- pure per-sample privacy-filtered transform
# --------------------------------------------------------------------------

# Design: "Raw argv, environment, file descriptors, and process I/O never
# persist." remote_probe.py's own --drop-raw-args default already omits
# `cmdline` from process rows, but this module strips it again
# unconditionally rather than trusting that upstream default -- the same
# defense-in-depth rationale as output.contracts._reject_forbidden_argv_keys:
# this is the last checkpoint before a census-derived record reaches disk.
_FORBIDDEN_PROCESS_KEYS = frozenset((
   "cmdline", "argv", "cmdline_raw", "raw_argv", "raw_cmdline", "environ",
))


def _sanitize_process_row(row):
   """Copy of ``row`` with every forbidden raw-argv/environment key removed."""
   return {key: value for key, value in row.items()
           if key not in _FORBIDDEN_PROCESS_KEYS}


def build_diagnostic_census(system, source_hostname, payload, cpu_deltas=None):
   """Build one ``diagnostic_censuses`` record from a raw census payload.

   ``system``, ``source_hostname``: daemon-known identity, not read from
      the payload -- ``source_hostname`` is the daemon's SSH-target
      identity, distinct from the probe's own self-reported
      ``hostname_fqdn`` (which the daemon uses separately to detect a
      fan-out that silently probed the same node twice).
   ``payload``: the full JSON ``remote_probe.py``'s ``main()`` produces
      for ``loop="census"``, i.e. ``json.loads()`` of one line of probe
      stdout. Passed through unmodified except for the per-process
      privacy filter below.
   ``cpu_deltas``: the dict ``cpu_delta.compute_cpu_delta`` returns for
      (previous_payload, payload), or ``None`` when there is no previous
      sample yet for this node (e.g. the first census of a run) -- in
      which case an empty-but-valid ``cpu_deltas`` shape is used instead
      of fabricating deltas that were never computed.

   Returns a dict matching the ``diagnostic_census`` contract
   (``output.contracts.validate_diagnostic_census``). The design's
   "Diagnostic records" section: "preserves the privacy-filtered census
   payload and derived process CPU deltas needed to audit classifications
   and aggregation during Phase 0. It is not a future production table.
   Raw argv is absent."
   """
   if cpu_deltas is None:
      cpu_deltas = {"deltas": [], "unmeasured": [], "anomalies": []}

   cpu_deltas_out = dict(cpu_deltas)
   cpu_deltas_out["tools"] = list(payload.get("tools") or [])

   return {
      "system": system,
      "source_hostname": source_hostname,
      "timestamp_utc": payload.get("wall_clock_utc"),
      "probe_version": payload.get("probe_version"),
      "processes": [_sanitize_process_row(row)
                    for row in payload.get("processes", [])],
      "cpu_deltas": cpu_deltas_out,
   }


# --------------------------------------------------------------------------
# build_usage_observations -- joins cpu_delta results back to census rows
# --------------------------------------------------------------------------

def _row_by_pid_key(payload):
   return {(row["pid"], row["start_time_ticks"]): row
           for row in (payload or {}).get("processes", [])}


def _observation_from_row(row, cpu_seconds, unmeasured, currently_present):
   return {
      "pid": row["pid"],
      "category": row["category"],
      "activity": row["activity"],
      "username": row["username"],
      "cpu_seconds": cpu_seconds,
      "rss_kb": row["rss_kb"],
      "state": row["state"],
      "interactive": row["interactive"],
      "unmeasured": unmeasured,
      "currently_present": currently_present,
   }


def build_usage_observations(current_payload, previous_payload=None):
   """Join one census payload's process rows against
   ``cpu_delta.compute_cpu_delta``'s output into the per-process
   "observation" shape ``UsageIntervalAccumulator.add_sample`` expects.

   Reuses ``cpu_delta.compute_cpu_delta`` (design/Task 5 file map: "reuse
   collector/cpu_delta.py") rather than re-deriving process-identity CPU
   attribution here -- this function's only job is turning that already-
   correct per-pid delta/unmeasured/anomaly classification into
   observations grouped for the accumulator.

   ``current_payload``: the raw census-loop probe payload this sample's
      process rows (and their category/activity/username/rss_kb/state/
      interactive fields) come from.
   ``previous_payload``: the raw census-loop probe payload immediately
      preceding ``current_payload`` for this node, or ``None`` when there
      is no previous sample at all (the first census of a run) -- in
      that case ``compute_cpu_delta`` is never called (there is nothing
      to diff against) and every process in ``current_payload`` is
      present but has no CPU delta to report, matching cpu_delta.py's
      own "any amount of cleverness cannot recover CPU for a process the
      census never observed twice" limitation.

   Design: "Process identity is (pid, start_time_ticks). New processes
   observed inside a window may contribute cumulative CPU; disappeared or
   ambiguous processes are explicit unmeasured records, never
   zero-filled." Every measured or unmeasured pid becomes exactly one
   observation with ``cpu_seconds=0.0`` when unmeasured -- the
   accumulator counts it toward ``unmeasured_count``, never toward
   ``cpu_seconds``.
   """
   current_rows = _row_by_pid_key(current_payload)

   if previous_payload is None:
      # First sample of a run for this node: nothing to diff against, so
      # every process is present but entirely unmeasurable. Every row
      # comes from current_payload, so every one is currently resident.
      return [_observation_from_row(row, cpu_seconds=0.0, unmeasured=True,
                                     currently_present=True)
              for row in current_rows.values()]

   previous_rows = _row_by_pid_key(previous_payload)
   cpu_deltas = compute_cpu_delta(previous_payload, current_payload)

   observations = []
   consumed_keys = set()

   for delta in cpu_deltas["deltas"]:
      key = (delta["pid"], delta["start_time_ticks"])
      row = current_rows.get(key)
      if row is None:
         continue
      observations.append(_observation_from_row(
         row, delta["cpu_seconds"], unmeasured=False, currently_present=True))
      consumed_keys.add(key)

   for entry in cpu_deltas["unmeasured"]:
      key = (entry["pid"], entry["start_time_ticks"])
      # "exited" pids have no row in current_payload (they are gone by
      # this sample) -- their only classifiable row is in the PREVIOUS
      # payload, and they are NOT currently resident. "started_before_
      # window" pids are the opposite: present in current_payload,
      # absent from previous_payload, and ARE currently resident (their
      # gauge fields -- rss/state/interactive -- are real point-in-time
      # readings; only their CPU is unattributable).
      current_row = current_rows.get(key)
      if current_row is not None:
         row, currently_present = current_row, True
      else:
         row, currently_present = previous_rows.get(key), False
      if row is None:
         # Neither payload has a row for this pid -- nothing to classify
         # it by. Skipped rather than fabricated into an ungrounded grain.
         continue
      observations.append(_observation_from_row(
         row, cpu_seconds=0.0, unmeasured=True,
         currently_present=currently_present))
      consumed_keys.add(key)

   for entry in cpu_deltas["anomalies"]:
      key = (entry["pid"], entry["start_time_ticks"])
      if key in consumed_keys:
         continue
      # An anomaly is always a (pid, start_time_ticks) present in BOTH
      # samples (cpu_delta.py only raises "negative_delta" for a key it
      # matched in both), so it is always currently resident.
      row = current_rows.get(key)
      if row is None:
         continue
      observations.append(_observation_from_row(
         row, cpu_seconds=0.0, unmeasured=True, currently_present=True))
      consumed_keys.add(key)

   return observations


# --------------------------------------------------------------------------
# UsageIntervalAccumulator -- bounded per-node 15-minute rollup
# --------------------------------------------------------------------------

_UNKNOWN_ACTIVITY = "unknown"


def _percentile_stats(values):
   """{"p50", "p95", "max"} over ``values``.

   Mirrors ``metrics._percentile_stats`` exactly (design: rollups include
   p50/p95/max "because bursts matter", same rationale here for
   process-count and RSS distributions). Duplicated rather than imported
   across modules: each module owns a pure function over its own bounded
   window state, and the two percentile definitions are independent
   design decisions that happen to compute the same statistic today, not
   a shared dependency that would couple counter and usage rollups
   together for an unrelated future change.
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


class _GrainState:
   """Bounded accumulating state for one (category, activity, username)
   grain within a single 15-minute window.

   Review round 1 finding 1: state here must be bounded by
   ``expected_count`` (the window's known sample cadence), NOT by total
   process observation count -- a single busy sample with thousands of
   same-grain processes must not make this state grow past what
   ``expected_count`` already sized it for. Every list below holds
   exactly one entry per SAMPLE this grain was reported in (whether the
   grain had 0, 1, or 10,000 currently-resident processes in that
   sample), so state is O(expected census samples in the window), never
   O(process observations).

   Review round 1 finding 2 / round 2 finding 2: once a grain has
   appeared in the window, every subsequent sample ``add_sample`` is
   called with contributes an entry to ``process_counts``/
   ``rss_per_sample`` for this grain -- zero when the grain has no
   currently-resident process in that sample -- via ``note_sample``
   (called once per sample per KNOWN grain, whether or not that sample's
   process list touched this grain). A grain that first appears midway
   through the window is backfilled via ``backfill_zero_samples`` for
   every successful sample the interval already accepted BEFORE this
   grain existed -- round 1's fix only zero-filled going forward, which
   still under-counted ``sample_count``/skewed percentiles for grains
   appearing after sample 0; the interval's ``sample_count`` is the
   count of successful polls, not "polls since this grain was born".

   Review round 1 finding 3 / round 2 finding 1: ``add_observation`` is
   called for every CURRENTLY-RESIDENT (``currently_present=True``)
   observation -- including unmeasured ones (e.g. "started before
   window") -- so it still counts toward process-count/RSS/D-state/
   interactivity gauges, but it accumulates ``cpu_seconds`` ONLY when
   ``unmeasured`` is False; round 1 conflated "is this pid currently
   resident" with "is this pid's CPU measured", letting an unmeasured-
   but-resident observation's (irrelevant, per contract, but non-zero in
   a hostile/buggy caller) ``cpu_seconds`` leak into the interval total.
   A NOT-currently-resident ("exited") observation updates ONLY
   ``unmeasured_count`` via ``note_unmeasured``, never the gauges at
   all, because it describes a process no longer part of the CURRENT
   census, carried only so its final partial CPU interval is accounted
   for as unmeasured rather than silently dropped.
   """

   def __init__(self):
      self.process_counts = []
      self.rss_per_sample = []
      self.cpu_seconds = 0.0
      self.d_state_observations = 0
      self.interactive_observations = 0
      self.total_observations = 0
      self.unmeasured_count = 0
      # Internal accumulators for the sample CURRENTLY being built by
      # add_sample -- reset by note_sample once that sample is closed
      # out, so they never outlive one sample's worth of processing.
      self._pending_process_count = 0
      self._pending_rss_total = 0

   def backfill_zero_samples(self, count):
      """Called exactly once, at grain creation, with the number of
      successful samples the interval already accepted before this
      grain existed. Appends one zero-count/zero-RSS entry per such
      sample -- review round 2 finding 2: a grain born on sample 3 of an
      already-3-samples-old interval did not exist for samples 0-2, but
      those were still successful polls of the interval as a whole, so
      this grain's ``sample_count``/percentiles must include them as
      explicit zeros, exactly like a later disappearance (round 1
      finding 2) is an explicit zero rather than an omission. Bounded:
      ``count`` is capped by ``expected_count`` at the call site
      (``UsageIntervalAccumulator.add_sample`` never accepts more than
      ``expected_count`` samples), so this can add at most
      ``expected_count - 1`` entries.
      """
      self.process_counts.extend([0] * count)
      self.rss_per_sample.extend([0] * count)

   def add_observation(self, rss_kb, state, interactive, cpu_seconds,
                        unmeasured):
      """Record one CURRENTLY-RESIDENT process observation for the
      sample in progress. Never called for a not-currently-resident
      ("exited") observation -- see ``note_unmeasured``. ``cpu_seconds``
      is accumulated only when ``unmeasured`` is False: a currently-
      resident-but-CPU-unmeasured observation (e.g. "started before
      window") still counts toward process-count/RSS/D-state/
      interactivity, but never toward ``cpu_seconds`` (review round 2
      finding 1).
      """
      self.total_observations += 1
      self._pending_process_count += 1
      self._pending_rss_total += rss_kb
      if state == "D":
         self.d_state_observations += 1
      if interactive:
         self.interactive_observations += 1
      if not unmeasured:
         self.cpu_seconds += cpu_seconds

   def note_unmeasured(self):
      """Record one CPU-unmeasured observation (exited, started-before-
      window, or an ambiguous/anomalous delta) -- never affects the
      current-census gauges by itself, only the explicit unmeasured
      count. Called alongside ``add_observation`` when the same
      observation is both currently-resident AND unmeasured.
      """
      self.unmeasured_count += 1

   def note_sample(self):
      """Close out the sample currently being built: commit its pending
      process-count/RSS totals (zero if this grain had no currently-
      resident process in this sample) and reset the pending
      accumulators for the next sample. Bounded: exactly one entry is
      appended per call, regardless of how many observations this sample
      contributed, and the caller never calls this more times than
      ``expected_count`` allows (see
      ``UsageIntervalAccumulator._at_capacity``).
      """
      self.process_counts.append(self._pending_process_count)
      self.rss_per_sample.append(self._pending_rss_total)
      self._pending_process_count = 0
      self._pending_rss_total = 0


class UsageIntervalAccumulator:
   """Bounded per-node state for one 15-minute usage-interval window.

   Consumes one "sample" at a time via ``add_sample`` -- one sample is
   the list of per-process observations the daemon derived from a single
   census payload via ``build_usage_observations`` (each with ``pid``,
   ``category``, ``activity``, ``username``, ``cpu_seconds``, ``rss_kb``,
   ``state``, ``interactive``, ``unmeasured``, and ``currently_present``
   -- see that function and ``tests/test_usage.py``'s ``_process_row``
   helper for the exact shape).

   ``finalize()`` produces one ``node_usage_intervals`` record per
   ``(category, activity, username)`` grain observed anywhere in the
   window -- design: "one row per (node, 15-minute interval, category,
   activity, username) matching the planned database grain." A grain
   with zero observations never appears (there is nothing to report);
   the design's per-node degraded-row precedent (metrics.py's
   ``CounterWindowAccumulator``, "one row per node per complete 60-second
   window" even when empty) does not apply here because usage intervals
   are keyed by a grain that only exists once at least one process
   belongs to it -- an empty window simply has no grains.

   Once a grain exists, every subsequent sample contributes a
   process-count entry for it (zero when absent that sample), and a
   grain first appearing mid-window is backfilled with zero-entries for
   every successful sample the interval already accepted before it
   existed -- see ``_GrainState``'s docstring for the round-1/round-2
   rationale.

   The interval itself is bounded to at most ``expected_count`` accepted
   samples (review round 2 finding 3): once that many samples have been
   accepted, further ``add_sample`` calls are excess and are rejected
   outright (no grain state, new or existing, is touched) -- consistent
   with ``metrics.CounterWindowAccumulator``'s own excess-sample
   handling, so bounded state is bounded by construction, not merely by
   convention.
   """

   def __init__(self, system, source_hostname, interval_start_utc,
                interval_end_utc, expected_count):
      self._system = system
      self._source_hostname = source_hostname
      self._interval_start_utc = interval_start_utc
      self._interval_end_utc = interval_end_utc
      self._expected_count = expected_count

      self._sample_count = 0
      self._excess_sample_count = 0
      self._grains = {}

   def _at_capacity(self):
      # expected_count is contract-validated to be >= 0 (never
      # negative); zero means the interval expected no samples at all,
      # so the very first call is already "excess" under this same
      # check -- mirrors metrics.CounterWindowAccumulator._at_capacity.
      return self._sample_count >= self._expected_count

   def add_sample(self, processes):
      """Feed one sample's per-process observations, in collection order.

      ``processes``: iterable of per-process dicts (see class docstring
      for the expected shape). A CURRENTLY-RESIDENT process
      (``currently_present=True``) contributes to its grain's process
      count/RSS/D-state/interactivity for this sample, and to
      ``cpu_seconds`` too UNLESS it is also ``unmeasured`` (review round
      2 finding 1: currently-resident-but-CPU-unmeasured, e.g. "started
      before window", must still count as a resident process without
      contributing bogus CPU). A not-currently-resident ("exited")
      process (``currently_present=False``) contributes ONLY to
      ``unmeasured_count`` and never touches this sample's gauge state
      (review round 1 finding 3).

      Once the interval has already accepted ``expected_count`` samples,
      every further call is an excess sample and is rejected outright --
      no grain, new or existing, is created or mutated (review round 2
      finding 3: bounded state must actually be bounded, not just
      documented as such).

      Every grain touched in ANY way this sample (present or exited) --
      plus every grain already known from an earlier sample -- gets
      exactly one process-count/RSS entry closed out for this sample via
      ``_GrainState.note_sample``, so absence reads as an explicit zero
      rather than a silent omission (review round 1 finding 2). A grain
      that first appears in THIS sample is backfilled via
      ``_GrainState.backfill_zero_samples`` for every successful sample
      the interval already accepted before it existed (review round 2
      finding 2: the interval's ``sample_count`` is the count of
      successful polls, not "polls since this grain was born").
      """
      if self._at_capacity():
         self._excess_sample_count += 1
         return

      prior_accepted_samples = self._sample_count
      self._sample_count += 1

      touched_keys = set()
      for process in processes:
         activity = process["activity"]
         if activity is None:
            # Design: "Nullable activity is normalized to 'unknown',
            # avoiding nullable-PK semantics."
            activity = _UNKNOWN_ACTIVITY
         key = (process["category"], activity, process["username"])
         is_new_grain = key not in self._grains
         grain = self._grains.setdefault(key, _GrainState())
         if is_new_grain and prior_accepted_samples:
            grain.backfill_zero_samples(prior_accepted_samples)
         touched_keys.add(key)

         if process["unmeasured"]:
            grain.note_unmeasured()
         if process["currently_present"]:
            grain.add_observation(
               rss_kb=process["rss_kb"],
               state=process["state"],
               interactive=process["interactive"],
               cpu_seconds=process["cpu_seconds"],
               unmeasured=process["unmeasured"],
            )

      # Every grain already known (from an earlier sample) that this
      # sample did NOT touch at all still gets a zero-count entry for
      # this sample -- it is a known grain with no resident process
      # right now, not an unknown one. A brand-new grain first appearing
      # in THIS sample was already handled above via note_observation's
      # pending accumulators; it must not ALSO get a second, duplicate
      # note_sample call here.
      for key, grain in self._grains.items():
         if key not in touched_keys:
            grain.note_sample()
      for key in touched_keys:
         self._grains[key].note_sample()

   def finalize(self):
      """Produce one ``node_usage_intervals`` record per observed grain.

      Every record this method returns is accepted outright by
      ``output.contracts.validate_node_usage_intervals`` -- this method
      never invents its own intermediate shape that the sink would have
      to translate.
      """
      records = []
      for (category, activity, username), grain in self._grains.items():
         records.append({
            "system": self._system,
            "source_hostname": self._source_hostname,
            "interval_start_utc": self._interval_start_utc,
            "interval_end_utc": self._interval_end_utc,
            "category": category,
            "activity": activity,
            "username": username,
            "process_count": _percentile_stats(grain.process_counts),
            "cpu_seconds": grain.cpu_seconds,
            "rss_kb": _percentile_stats(grain.rss_per_sample),
            "d_state_fraction": (
               grain.d_state_observations / grain.total_observations
               if grain.total_observations else 0.0),
            "interactivity_fraction": (
               grain.interactive_observations / grain.total_observations
               if grain.total_observations else 0.0),
            # Every known grain gets exactly one process_counts/
            # rss_per_sample entry per accepted sample of the interval
            # (backfilled for samples before the grain existed, zero-
            # filled for samples where it had no resident process) --
            # so this list's length IS the interval's successful
            # sample_count for this grain, equal to self._sample_count.
            "sample_count": len(grain.process_counts),
            "expected_count": self._expected_count,
            "unmeasured_count": grain.unmeasured_count,
         })
      return records
