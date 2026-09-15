"""node_monitor.output.acceptance -- pure Phase 0 canary acceptance-summary
evaluator.

Design: PHASE0_DAEMON_DESIGN.md "Canary acceptance criteria" (kanban
task t_b0ac4c03, split B of t_90c962e7's Task 7 write-up: "add a pure
acceptance-summary evaluator over finalized collected records/artifact-
validation metadata and wire its result into summary.json").

``evaluate_acceptance`` is a PURE function: every argument is a plain,
already-computed value (counts, sample lists, the sink's own
per-file ``files_summary`` dict) -- this module never touches a sink,
a scheduler, the filesystem, or a clock. That is what "pure" means
here and is the whole reason this evaluator is unit-testable without
any daemon/asyncio machinery at all. The daemon (``node_monitor.
daemon``) is the only caller, and its own job is limited to gathering
these already-measured inputs and calling this function once per run,
right before ``finalize_summary()``.

Scope note (card): this increment covers exactly the design's own
"Canary acceptance criteria" bullets that are honestly computable from
what the daemon already measures -- per-node counter/census coverage,
minimum-five-valid-samples per complete counter rollup, and
scheduling-delay p95, plus malformed/truncated JSONL accounting and
clean-vs-partial completion classification. Two design bullets --
"Counter probe p95 <=0.5s; census probe p95 <=2s" -- are reported as
explicitly UNAVAILABLE rather than fabricated: today's
``transport_fn(node, loop)`` contract in ``node_monitor.daemon``
returns only the raw probe payload dict, never ``collector.transport.
ProbeResult.wall_seconds`` (see that module's own "probe wall time ...
observer-effect fields" docstring) -- threading that measurement
through is a transport/CLI-layer wiring change, and the card
explicitly scopes this increment to "Do not implement CLI, deploy
scripts, or docs." An unavailable threshold NEVER counts against the
overall status (see ``_STATUS_DEGRADED_MEANS_FAILED_THRESHOLD``
below) -- "we did not measure this yet" is categorically different
from "we measured it and it failed", and conflating the two would let
a future run's genuinely-missing instrumentation silently degrade a
run that is otherwise clean by every threshold this increment CAN
evaluate.

Every per-threshold result is a dict with at least ``value`` and
``met`` (``True``/``False``/``None`` for unavailable), so a human or a
future validator can see exactly which knob failed and by how much --
design: "Threshold failures remain in the artifact and mark the run
degraded/failed; they never trigger retrospective data deletion."
"""

import statistics


# Design: "Counter coverage >=99% per node; census coverage >=95% per
# node."
_COUNTER_COVERAGE_MINIMUM = 0.99
_CENSUS_COVERAGE_MINIMUM = 0.95

# Design: "Each complete minute rollup has at least five valid counter
# samples." -- matches metrics.py's own _MINIMUM_VALID_SAMPLES; kept
# local rather than imported because this module must stay a pure
# function over the CALLER's already-computed window-completeness
# tally (``counter_window_stats``), never re-deriving it from raw
# accumulator state itself.
_MINIMUM_VALID_SAMPLES_PER_WINDOW = 5

# Design: "scheduling-delay p95 <=1s."
_SCHEDULING_DELAY_P95_MAX_SEC = 1.0

_VALID_COMPLETIONS = frozenset(("clean", "partial"))

_PROBE_WALL_TIME_UNAVAILABLE_REASON = (
   "transport_fn(node, loop) returns only the raw probe payload dict; "
   "collector.transport.ProbeResult.wall_seconds is not threaded "
   "through node_monitor.daemon in this increment (CLI/transport "
   "wiring is out of this card's scope) -- reported unavailable "
   "rather than fabricated."
)


def _percentile95(values):
   """p95 over ``values`` via the same inclusive-method convention as
   ``collector.metrics._percentile_stats``/``collector.usage.
   _percentile_stats``, for consistency with every other p95 this
   project reports. Returns None for an empty sequence -- "no samples
   yet" is unavailable, not zero.
   """
   if not values:
      return None
   sorted_values = sorted(values)
   if len(sorted_values) == 1:
      return sorted_values[0]
   quantiles = statistics.quantiles(sorted_values, n=100, method="inclusive")
   return quantiles[94]


def _coverage_threshold(totals, duration_sec, interval_sec, minimum):
   """Per-node coverage threshold dict: {hostname: {"value", "required",
   "met", "expected_count", "actual_count"}}.

   ``expected_count`` is derived the same way ``node_monitor.daemon``
   sizes its own accumulator windows (``duration // interval``,
   floored) -- an honest ceiling on how many polls of this loop this
   run could ever have dispatched for a node, independent of whether
   any of them actually succeeded. A node with zero expected polls
   (duration shorter than one interval) is reported ``met: None``
   (unavailable) rather than a coverage ratio computed against a zero
   denominator -- there is nothing to honestly divide by.
   """
   expected_count = int(duration_sec // interval_sec) if interval_sec else 0
   result = {}
   for hostname, actual_count in totals.items():
      if expected_count <= 0:
         result[hostname] = {
            "value": None, "required": minimum, "met": None,
            "expected_count": expected_count, "actual_count": actual_count,
         }
         continue
      coverage = actual_count / expected_count
      result[hostname] = {
         "value": coverage, "required": minimum,
         "met": coverage >= minimum,
         "expected_count": expected_count, "actual_count": actual_count,
      }
   return result


def _minimum_samples_threshold(counter_window_stats):
   complete = counter_window_stats.get("complete_windows", 0)
   meeting_minimum = counter_window_stats.get(
      "complete_windows_meeting_minimum", 0)
   if complete <= 0:
      return {
         "value": None, "required": _MINIMUM_VALID_SAMPLES_PER_WINDOW,
         "met": None, "complete_windows": complete,
         "complete_windows_meeting_minimum": meeting_minimum,
      }
   return {
      "value": meeting_minimum / complete,
      "required": _MINIMUM_VALID_SAMPLES_PER_WINDOW,
      "met": meeting_minimum == complete,
      "complete_windows": complete,
      "complete_windows_meeting_minimum": meeting_minimum,
   }


def _scheduling_delay_threshold(scheduling_delay_samples):
   p95 = _percentile95(scheduling_delay_samples)
   if p95 is None:
      return {"value": None, "required": _SCHEDULING_DELAY_P95_MAX_SEC,
              "met": None}
   return {"value": p95, "required": _SCHEDULING_DELAY_P95_MAX_SEC,
           "met": p95 <= _SCHEDULING_DELAY_P95_MAX_SEC}


def _probe_wall_time_unavailable():
   return {"value": None, "required": None, "met": None,
           "unavailable_reason": _PROBE_WALL_TIME_UNAVAILABLE_REASON}


def _artifact_integrity_threshold(files_summary, completion):
   """Malformed/truncated JSONL accounting across every artifact file.

   Design: "malformed/truncated JSONL accounting" (card) + "Every
   complete JSONL line validates; clean completion has no malformed
   lines" (canary acceptance criteria). A malformed line ALWAYS fails
   this threshold, on either completion path -- a malformed line is
   never an artifact of an in-progress write the way a truncated FINAL
   line is (see ``output.jsonl.validate_jsonl_artifact``'s own
   truncated-vs-malformed distinction). A truncated final line is
   tolerated on a ``partial`` (signal-interrupted) completion -- design:
   "A crash may damage only the final line" -- but fails a ``clean``
   completion, since a clean finite-duration/orderly-flush run must
   never leave a truncated line behind at all (every write in that
   path is a complete, flushed record before the next one starts).
   """
   malformed_total = 0
   truncated_files = []
   for record_type, entry in sorted(files_summary.items()):
      malformed_total += entry.get("malformed_count", 0)
      if entry.get("truncated_final_line"):
         truncated_files.append(record_type)

   truncation_allowed = completion == "partial"
   met = malformed_total == 0 and (
      not truncated_files or truncation_allowed)
   return {
      "value": {
         "malformed_lines_total": malformed_total,
         "files_with_truncated_final_line": truncated_files,
      },
      "required": {
         "malformed_lines_total": 0,
         "truncated_final_line_tolerated": truncation_allowed,
      },
      "met": met,
   }


def evaluate_acceptance(*, completion, duration_sec, counter_interval_sec,
                         census_interval_sec, counter_totals, census_totals,
                         counter_window_stats, scheduling_delay_samples,
                         files_summary):
   """Evaluate every design-listed canary acceptance threshold this
   increment can honestly compute and classify the run's overall
   ``status``.

   ``completion``: ``"clean"`` (the run's own finite ``duration_sec``
      elapsed, or ``finalize_summary()``/``write_done()`` completed
      with no fatal error observed) or ``"partial"`` (an orderly
      SIGINT/SIGTERM stop -- design: "distinguish finite-duration
      clean completion from orderly signal stop as partial"). Any
      other value raises ``ValueError`` -- there is no honest third
      completion state this evaluator is ever called for (a FATAL sink
      condition never reaches this function at all; ``Daemon.run()``
      returns ``EXIT_SINK_FATAL`` before ``finalize_summary()``, and
      this evaluator's result is written only as part of that same
      finalized summary).
   ``duration_sec``, ``counter_interval_sec``, ``census_interval_sec``:
      the run's own configured values, used only to compute each
      node's expected-poll denominator for coverage -- never to derive
      a rate or duration figure this module reports directly.
   ``counter_totals``/``census_totals``: {hostname: total successful
      poll count for that loop across the whole run} -- the daemon's
      own bounded per-node counters, not re-derived from raw JSONL.
   ``counter_window_stats``: {"complete_windows": int,
      "complete_windows_meeting_minimum": int} -- a complete window is
      one that reached its own ``expected_count`` sample cadence
      (``CounterWindowAccumulator``'s ordinary at-capacity flush, never
      a trailing/degraded flush); "meeting minimum" additionally
      requires ``sample_count >= 5`` per the design's own threshold.
   ``scheduling_delay_samples``: a bounded list of per-poll scheduling
      delays in seconds (each dispatch's actual start time minus its
      own fixed grid deadline) collected across every ``(node, loop)``
      target for the whole run.
   ``files_summary``: exactly ``Phase0Sink.finalize_summary()``'s own
      ``files`` dict -- {record_type: {"record_count", "malformed_count",
      "truncated_final_line", ...}}.

   Returns a dict:
      "completion": echoed back verbatim (validated).
      "status": "clean" if ``completion == \"clean\"`` and every
         threshold this function evaluated (i.e. every ``met`` that is
         not ``None``) passed; "partial" if ``completion == \"partial\"``
         and every evaluated threshold passed; "degraded" if ANY
         evaluated threshold failed, regardless of completion path --
         design: "Threshold failures ... mark the run degraded/failed".
         An unavailable threshold (``met is None``) never contributes
         to this decision either way.
      "thresholds": {threshold_name: {"value", "required", "met", ...}}
         for every threshold this function evaluates, so a reviewer or
         a future validator can see exactly which one(s) failed.
   """
   if completion not in _VALID_COMPLETIONS:
      raise ValueError(
         "completion must be one of %s, got %r"
         % (sorted(_VALID_COMPLETIONS), completion))

   thresholds = {
      "counter_coverage_per_node": _coverage_threshold(
         counter_totals, duration_sec, counter_interval_sec,
         _COUNTER_COVERAGE_MINIMUM),
      "census_coverage_per_node": _coverage_threshold(
         census_totals, duration_sec, census_interval_sec,
         _CENSUS_COVERAGE_MINIMUM),
      "counter_window_minimum_samples": _minimum_samples_threshold(
         counter_window_stats),
      "scheduling_delay_p95_sec": _scheduling_delay_threshold(
         scheduling_delay_samples),
      "counter_probe_p95_sec": _probe_wall_time_unavailable(),
      "census_probe_p95_sec": _probe_wall_time_unavailable(),
      "artifact_integrity": _artifact_integrity_threshold(
         files_summary, completion),
   }

   # Per-node coverage thresholds nest one {"met": ...} dict per
   # hostname rather than a single flat entry, so they are walked
   # separately from every other (flat) threshold below.
   any_failed = False
   for per_node in (thresholds["counter_coverage_per_node"],
                     thresholds["census_coverage_per_node"]):
      for node_result in per_node.values():
         if node_result.get("met") is False:
            any_failed = True

   flat_thresholds = (
      thresholds["counter_window_minimum_samples"],
      thresholds["scheduling_delay_p95_sec"],
      thresholds["counter_probe_p95_sec"],
      thresholds["census_probe_p95_sec"],
      thresholds["artifact_integrity"],
   )
   for entry in flat_thresholds:
      if entry.get("met") is False:
         any_failed = True

   status = "degraded" if any_failed else completion

   return {
      "completion": completion,
      "status": status,
      "thresholds": thresholds,
   }
