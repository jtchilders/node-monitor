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
daemon``) is the only intended caller, and its own job would be
limited to gathering these already-measured inputs and calling this
function once per run, right before ``finalize_summary()`` -- that
daemon-side wiring (threading ``collector.transport.ProbeResult.
wall_seconds`` through to a bounded per-loop sample list, and calling
this function at all) is a separate, subsequent card; this card is
the evaluator itself.

Scope note (card): this increment covers every one of the design's own
"Canary acceptance criteria" bullets that are computable from
already-collected daemon state: per-node counter/census coverage
(explicitly over every CONFIGURED node, including one with zero
successful polls -- a node that never once succeeded must never be
silently absent from the summary), minimum-five-valid-samples per
complete counter rollup, scheduling-delay p95, counter/census probe
wall-time p95, and malformed/truncated JSONL accounting, plus
clean-vs-partial completion classification. Every numeric input is
validated (finite, non-negative; intervals/duration strictly
positive) so a caller bug -- a NaN slipping in from a bad rate
computation upstream, a negative count -- fails loudly here rather
than silently corrupting an acceptance verdict.

Every per-threshold result is a dict with at least ``value`` and
``met`` (``True``/``False``/``None`` for unavailable -- e.g. a probe
wall-time sample list that is empty because no poll of that loop ever
completed), so a human or a future validator can see exactly which
knob failed and by how much -- design: "Threshold failures remain in
the artifact and mark the run degraded/failed; they never trigger
retrospective data deletion."
"""

import math
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

# Design: "Counter probe p95 <=0.5s; census probe p95 <=2s;
# scheduling delay p95 <=1s."
_SCHEDULING_DELAY_P95_MAX_SEC = 1.0
_COUNTER_PROBE_P95_MAX_SEC = 0.5
_CENSUS_PROBE_P95_MAX_SEC = 2.0

_VALID_COMPLETIONS = frozenset(("clean", "partial"))


# --------------------------------------------------------------------------
# Input validation -- every numeric input is checked before any
# threshold math runs, so a caller bug fails loudly here rather than
# silently producing a bogus acceptance verdict.
# --------------------------------------------------------------------------

def _validate_finite_number(value, label):
   if isinstance(value, bool) or not isinstance(value, (int, float)):
      raise ValueError("%s must be a real number, got %r" % (label, value))
   value = float(value)
   if not math.isfinite(value):
      raise ValueError("%s must be finite, got %r" % (label, value))
   return value


def _validate_positive(value, label):
   value = _validate_finite_number(value, label)
   if value <= 0:
      raise ValueError("%s must be positive, got %r" % (label, value))
   return value


def _validate_nonnegative(value, label):
   value = _validate_finite_number(value, label)
   if value < 0:
      raise ValueError("%s must be nonnegative, got %r" % (label, value))
   return value


def _validate_nonnegative_int(value, label):
   if isinstance(value, bool) or not isinstance(value, int):
      raise ValueError("%s must be an integer, got %r" % (label, value))
   if value < 0:
      raise ValueError("%s must be nonnegative, got %r" % (label, value))
   return value


def _validate_sample_sequence(values, label):
   validated = []
   for index, value in enumerate(values):
      validated.append(
         _validate_nonnegative(value, "%s[%d]" % (label, index)))
   return validated


def _validate_nodes(nodes):
   nodes = list(nodes)
   if not nodes:
      raise ValueError("nodes must be non-empty")
   for node in nodes:
      if not isinstance(node, str) or not node:
         raise ValueError(
            "each node must be a non-empty string, got %r" % (node,))
   if len(set(nodes)) != len(nodes):
      raise ValueError("nodes must not contain duplicates")
   return nodes


def _validate_totals(totals, nodes, label):
   nodes_set = set(nodes)
   validated = {}
   for hostname, count in totals.items():
      if hostname not in nodes_set:
         raise ValueError(
            "%s has entry for %r not present in nodes" % (label, hostname))
      validated[hostname] = _validate_nonnegative_int(
         count, "%s[%r]" % (label, hostname))
   return validated


def _validate_counter_window_stats(stats):
   complete = _validate_nonnegative_int(
      stats.get("complete_windows"),
      "counter_window_stats['complete_windows']")
   meeting = _validate_nonnegative_int(
      stats.get("complete_windows_meeting_minimum"),
      "counter_window_stats['complete_windows_meeting_minimum']")
   if meeting > complete:
      raise ValueError(
         "counter_window_stats['complete_windows_meeting_minimum'] "
         "(%r) cannot exceed 'complete_windows' (%r)" % (meeting, complete))
   return {"complete_windows": complete,
           "complete_windows_meeting_minimum": meeting}


def _validate_files_summary(files_summary):
   validated = {}
   for record_type, entry in files_summary.items():
      record_count = _validate_nonnegative_int(
         entry.get("record_count", 0),
         "files_summary[%r]['record_count']" % (record_type,))
      malformed_count = _validate_nonnegative_int(
         entry.get("malformed_count", 0),
         "files_summary[%r]['malformed_count']" % (record_type,))
      truncated = entry.get("truncated_final_line", False)
      if not isinstance(truncated, bool):
         raise ValueError(
            "files_summary[%r]['truncated_final_line'] must be a bool, "
            "got %r" % (record_type, truncated))
      validated[record_type] = {
         "record_count": record_count,
         "malformed_count": malformed_count,
         "truncated_final_line": truncated,
      }
   return validated


# --------------------------------------------------------------------------
# Threshold math
# --------------------------------------------------------------------------

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


def _percentile_threshold(samples, maximum):
   p95 = _percentile95(samples)
   if p95 is None:
      return {"value": None, "required": maximum, "met": None}
   return {"value": p95, "required": maximum, "met": p95 <= maximum}


def _coverage_threshold(nodes, totals, duration_sec, interval_sec, minimum):
   """Per-node coverage threshold dict: {hostname: {"value", "required",
   "met", "expected_count", "actual_count"}} -- one entry for EVERY
   node in ``nodes``, including a node with zero recorded successes
   (``totals.get(hostname, 0)``), so a node that never once succeeded
   can never be silently absent from the summary.

   ``expected_count`` is ``ceil(duration_sec / interval_sec)``,
   matching ``collector.scheduler.Scheduler``'s own dispatch grid: a
   target's first dispatch happens at ``t=0`` (not after waiting one
   full interval), so a run whose duration is shorter than one
   interval still gets exactly one dispatch, not zero -- e.g.
   ``duration_sec=5.0, interval_sec=10.0`` dispatches once, at
   ``t=0``, then stops because the next deadline (``t=10``) is at or
   past the run's end time. Flooring that same ratio would report
   zero expected polls (and therefore an "unavailable" coverage
   verdict) for a node that in fact had one honest chance to be
   polled and was not.
   """
   expected_count = math.ceil(duration_sec / interval_sec)
   result = {}
   for hostname in nodes:
      actual_count = totals.get(hostname, 0)
      coverage = actual_count / expected_count
      result[hostname] = {
         "value": coverage, "required": minimum,
         "met": coverage >= minimum,
         "expected_count": expected_count, "actual_count": actual_count,
      }
   return result


def _minimum_samples_threshold(counter_window_stats):
   complete = counter_window_stats["complete_windows"]
   meeting_minimum = counter_window_stats["complete_windows_meeting_minimum"]
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
      malformed_total += entry["malformed_count"]
      if entry["truncated_final_line"]:
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
                         census_interval_sec, nodes, counter_totals,
                         census_totals, counter_window_stats,
                         scheduling_delay_samples,
                         counter_probe_wall_seconds,
                         census_probe_wall_seconds, files_summary):
   """Evaluate every design-listed canary acceptance threshold this
   function can honestly compute from already-collected inputs, and
   classify the run's overall ``status``.

   ``completion``: ``"clean"`` (the run's own finite ``duration_sec``
      elapsed, or ``finalize_summary()``/``write_done()`` completed
      with no fatal error observed) or ``"partial"`` (an orderly
      SIGINT/SIGTERM stop -- design: "distinguish finite-duration
      clean completion from orderly signal stop as partial"). Any
      other value raises ``ValueError``.
   ``duration_sec``, ``counter_interval_sec``, ``census_interval_sec``:
      the run's own configured values (each validated finite and
      strictly positive), used only to compute each node's
      expected-poll denominator for coverage.
   ``nodes``: every node CONFIGURED for this run (validated as a
      non-empty list of distinct non-empty strings) -- coverage is
      always reported for every one of these, even a node with zero
      entries in ``counter_totals``/``census_totals``, so a node that
      never once succeeded can never be silently omitted.
   ``counter_totals``/``census_totals``: {hostname: total successful
      poll count for that loop across the whole run} -- the daemon's
      own bounded per-node counters, not re-derived from raw JSONL.
      Every key must be one of ``nodes``; a node absent from this dict
      is treated as zero successful polls.
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
   ``counter_probe_wall_seconds``/``census_probe_wall_seconds``: bounded
      lists of ``collector.transport.ProbeResult.wall_seconds`` values
      for every completed poll of that loop, across every node, for
      the whole run. An empty list means no poll of that loop ever
      completed during the run -- reported unavailable (``met`` is
      ``None``), never fabricated as zero or as passing.
   ``files_summary``: exactly ``Phase0Sink.finalize_summary()``'s own
      ``files`` dict -- {record_type: {"record_count", "malformed_count",
      "truncated_final_line", ...}}.

   Returns a dict:
      "completion": echoed back verbatim (validated).
      "status": "clean" if ``completion == "clean"`` and every
         threshold this function evaluated (i.e. every ``met`` that is
         not ``None``) passed; "partial" if ``completion == "partial"``
         and every evaluated threshold passed; "degraded" if ANY
         evaluated threshold failed, regardless of completion path --
         design: "Threshold failures ... mark the run degraded/failed".
         An unavailable threshold (``met is None``) never contributes
         to this decision either way.
      "thresholds": {threshold_name: {"value", "required", "met", ...}}
         for every threshold this function evaluates, so a reviewer or
         a future validator can see exactly which one(s) failed.

   Raises ``ValueError`` for any invalid input: an unknown
   ``completion``, a non-finite or non-positive ``duration_sec``/
   interval, a non-finite or negative count/sample, a
   ``counter_totals``/``census_totals`` entry for a node not in
   ``nodes``, or ``complete_windows_meeting_minimum`` exceeding
   ``complete_windows``.
   """
   if completion not in _VALID_COMPLETIONS:
      raise ValueError(
         "completion must be one of %s, got %r"
         % (sorted(_VALID_COMPLETIONS), completion))

   duration_sec = _validate_positive(duration_sec, "duration_sec")
   counter_interval_sec = _validate_positive(
      counter_interval_sec, "counter_interval_sec")
   census_interval_sec = _validate_positive(
      census_interval_sec, "census_interval_sec")

   nodes = _validate_nodes(nodes)
   counter_totals = _validate_totals(counter_totals, nodes, "counter_totals")
   census_totals = _validate_totals(census_totals, nodes, "census_totals")
   counter_window_stats = _validate_counter_window_stats(counter_window_stats)
   scheduling_delay_samples = _validate_sample_sequence(
      scheduling_delay_samples, "scheduling_delay_samples")
   counter_probe_wall_seconds = _validate_sample_sequence(
      counter_probe_wall_seconds, "counter_probe_wall_seconds")
   census_probe_wall_seconds = _validate_sample_sequence(
      census_probe_wall_seconds, "census_probe_wall_seconds")
   files_summary = _validate_files_summary(files_summary)

   thresholds = {
      "counter_coverage_per_node": _coverage_threshold(
         nodes, counter_totals, duration_sec, counter_interval_sec,
         _COUNTER_COVERAGE_MINIMUM),
      "census_coverage_per_node": _coverage_threshold(
         nodes, census_totals, duration_sec, census_interval_sec,
         _CENSUS_COVERAGE_MINIMUM),
      "counter_window_minimum_samples": _minimum_samples_threshold(
         counter_window_stats),
      "scheduling_delay_p95_sec": _percentile_threshold(
         scheduling_delay_samples, _SCHEDULING_DELAY_P95_MAX_SEC),
      "counter_probe_p95_sec": _percentile_threshold(
         counter_probe_wall_seconds, _COUNTER_PROBE_P95_MAX_SEC),
      "census_probe_p95_sec": _percentile_threshold(
         census_probe_wall_seconds, _CENSUS_PROBE_P95_MAX_SEC),
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
         if node_result["met"] is False:
            any_failed = True

   flat_thresholds = (
      thresholds["counter_window_minimum_samples"],
      thresholds["scheduling_delay_p95_sec"],
      thresholds["counter_probe_p95_sec"],
      thresholds["census_probe_p95_sec"],
      thresholds["artifact_integrity"],
   )
   for entry in flat_thresholds:
      if entry["met"] is False:
         any_failed = True

   status = "degraded" if any_failed else completion

   return {
      "completion": completion,
      "status": status,
      "thresholds": thresholds,
   }
