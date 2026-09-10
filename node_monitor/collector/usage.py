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


def _observation_from_row(row, cpu_seconds, unmeasured):
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
      # every process is present but entirely unmeasurable.
      return [_observation_from_row(row, cpu_seconds=0.0, unmeasured=True)
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
      observations.append(
         _observation_from_row(row, delta["cpu_seconds"], unmeasured=False))
      consumed_keys.add(key)

   for entry in cpu_deltas["unmeasured"]:
      key = (entry["pid"], entry["start_time_ticks"])
      # "exited" pids have no row in current_payload (they are gone by
      # this sample) -- their only classifiable row is in the PREVIOUS
      # payload. "started_before_window" pids are the opposite: present
      # in current_payload, absent from previous_payload. Try current
      # first (the common/typical case), fall back to previous.
      row = current_rows.get(key) or previous_rows.get(key)
      if row is None:
         # Neither payload has a row for this pid -- nothing to classify
         # it by. Skipped rather than fabricated into an ungrounded grain.
         continue
      observations.append(
         _observation_from_row(row, cpu_seconds=0.0, unmeasured=True))
      consumed_keys.add(key)

   for entry in cpu_deltas["anomalies"]:
      key = (entry["pid"], entry["start_time_ticks"])
      if key in consumed_keys:
         continue
      row = current_rows.get(key) or previous_rows.get(key)
      if row is None:
         continue
      observations.append(
         _observation_from_row(row, cpu_seconds=0.0, unmeasured=True))
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

   Every list here is bounded by ``expected_count`` (the window's known
   sample cadence), enforced by ``UsageIntervalAccumulator.add_sample``
   never calling this class more than once per grain per sample -- a
   grain observed in every sample of a 15-minute/60-second-census window
   accumulates at most ``expected_count`` entries per list, never growing
   with total process count across the run.
   """

   def __init__(self):
      self.process_counts = []
      self.cpu_seconds = 0.0
      self.rss_values = []
      self.d_state_observations = 0
      self.interactive_observations = 0
      self.total_observations = 0
      self.sample_indices = set()
      self.unmeasured_count = 0

   def add_observation(self, sample_index, rss_kb, state, interactive,
                        cpu_seconds, unmeasured):
      self.sample_indices.add(sample_index)
      self.total_observations += 1
      self.rss_values.append(rss_kb)
      if state == "D":
         self.d_state_observations += 1
      if interactive:
         self.interactive_observations += 1
      if unmeasured:
         self.unmeasured_count += 1
      else:
         self.cpu_seconds += cpu_seconds

   def note_process_count(self, count):
      self.process_counts.append(count)


class UsageIntervalAccumulator:
   """Bounded per-node state for one 15-minute usage-interval window.

   Consumes one "sample" at a time via ``add_sample`` -- one sample is
   the list of per-process observations the daemon derived from a single
   census payload (already joined against ``cpu_delta.compute_cpu_delta``
   results for that payload against the previous one; see
   ``tests/test_usage.py``'s ``_process_row`` helper for the exact
   per-process shape this expects: ``pid``, ``category``, ``activity``,
   ``username``, ``cpu_seconds``, ``rss_kb``, ``state``, ``interactive``,
   ``unmeasured``).

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
   """

   def __init__(self, system, source_hostname, interval_start_utc,
                interval_end_utc, expected_count):
      self._system = system
      self._source_hostname = source_hostname
      self._interval_start_utc = interval_start_utc
      self._interval_end_utc = interval_end_utc
      self._expected_count = expected_count

      self._sample_count = 0
      self._grains = {}

   def add_sample(self, processes):
      """Feed one sample's per-process observations, in collection order.

      ``processes``: iterable of per-process dicts (see class docstring
      for the expected shape). Every process in a single sample belongs
      to exactly one (category, activity, username) grain; a grain
      observed more than once within the SAME sample (e.g. two processes
      that happen to share category/activity/username) each still
      contribute their own process-count unit and their own CPU/RSS/
      state/interactivity observation -- only the grain's aggregate
      process COUNT for this sample is what gets recorded once, as the
      number of processes observed in this sample for that grain.
      """
      sample_index = self._sample_count
      self._sample_count += 1

      counts_this_sample = {}
      for process in processes:
         activity = process["activity"]
         if activity is None:
            # Design: "Nullable activity is normalized to 'unknown',
            # avoiding nullable-PK semantics."
            activity = _UNKNOWN_ACTIVITY
         key = (process["category"], activity, process["username"])
         grain = self._grains.setdefault(key, _GrainState())
         grain.add_observation(
            sample_index=sample_index,
            rss_kb=process["rss_kb"],
            state=process["state"],
            interactive=process["interactive"],
            cpu_seconds=process["cpu_seconds"],
            unmeasured=process["unmeasured"],
         )
         counts_this_sample[key] = counts_this_sample.get(key, 0) + 1

      for key, count in counts_this_sample.items():
         self._grains[key].note_process_count(count)

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
            "rss_kb": _percentile_stats(grain.rss_values),
            "d_state_fraction": (
               grain.d_state_observations / grain.total_observations),
            "interactivity_fraction": (
               grain.interactive_observations / grain.total_observations),
            "sample_count": len(grain.sample_indices),
            "expected_count": self._expected_count,
            "unmeasured_count": grain.unmeasured_count,
         })
      return records
