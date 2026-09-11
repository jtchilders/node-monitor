"""node_monitor.daemon -- Phase 0 orchestration layer.

Design: PHASE0_DAEMON_DESIGN.md + PHASE0_DAEMON_IMPLEMENTATION_PLAN.md
Task 7. Wires config, the probe transport, the independent scheduler,
per-node counter-rollup/census/usage transforms, and the atomic JSONL
sink into one injectable orchestration coroutine that a test can drive
end to end against a fake clock instead of a real 24-hour duration.

INCREMENT SCOPE NOTE (2026-09-10, third increment on kanban task
t_c5cf59c4, building on the second increment's hardware/census/usage
orchestration): this increment adds structured ``node_poll_failures``
records for ORDINARY scheduled counter/census probe failures only
(design: "node_poll_failures: one record per failed or skipped poll
with bounded/scrubbed detail and breaker state"). At the time of that
increment, ``scheduler_miss`` (a same-node/same-loop overlap recorded
entirely inside ``collector.scheduler.Scheduler``'s own bookkeeping/
events -- this daemon-level path only ever saw an actual
``transport_fn`` invocation attempt, never a skipped/overlapped one)
was explicitly excluded.

FOLLOW-UP INCREMENT SCOPE NOTE (2026-09-10, kanban task t_d18ac4ae):
this increment closes that gap. ``_observe_scheduler_event`` -- already
installed as the Scheduler's own ``on_event`` to mirror its per-target
``consecutive_failures``/``breaker_state`` (see the comment on that
method below) -- is now itself ``async`` and, for every
``scheduler_miss`` event, writes exactly one schema-valid
``node_poll_failures`` record via ``_record_scheduler_miss`` (or the
same ``node_collection_log`` pre-FQDN fallback
``_record_ordinary_poll_failure`` already uses, before any FQDN has
ever been established for that node) with ``failure_type=
"scheduler_miss"``, ``loop``, and the Scheduler's own JUST-COMPUTED
``consecutive_failures``/``breaker_state`` for that exact miss --
unlike the ordinary-failure path, no anticipation is needed here: a
scheduler_miss event already carries the Scheduler's authoritative
post-increment state directly, because ``_observe_scheduler_event`` is
called synchronously (now awaited) from inside
``Scheduler._run_target`` right after it increments
``state.consecutive_failures`` for this exact miss, not before. Event
forwarding to this module's OWN ``on_event`` (this Daemon's own
caller-supplied hook) is unchanged.

Making ``Scheduler``'s own ``_emit`` await whatever its ``on_event``
returns (see ``collector/scheduler.py``'s own ``_emit`` docstring) is
what keeps this miss-triggered write deterministic rather than a
fire-and-forget race: ``Scheduler._run_target`` does not proceed to
consider dispatching this target's next attempt until the awaited
handler -- including this module's own sink write -- has returned (or
raised). A sink write/flush/OSError failure recording a miss is fatal
by the exact same contract as every other sink write in this module
(design: "Output write/flush failure or low disk is fatal"): it sets
``self._fatal_error`` and calls ``self._scheduler.request_stop()``,
and ``Scheduler._run_target`` checks ``self._stop_requested``
immediately after the awaited miss-emit call returns (in addition to
its existing top-of-loop check) so a fatal condition observed here
stops this target's OWN loop before it launches a further, doomed
dispatch in the same iteration -- request_stop()'s usual bounded-grace
drain then reaps whatever is already in flight for every target.

Every ordinary poll failure's ``source_hostname`` is the node's
remote-reported FQDN from the most recent PRIOR successful poll of any
loop (hwinfo, counter, or census) for that node -- never the
configured SSH-alias transport hostname (design: "SSH aliases are
transport identifiers only, never provenance"). Before any poll has
ever succeeded for a node, no FQDN has ever been established and a
``node_poll_failures`` record (whose ``source_hostname`` is REQUIRED,
non-nullable) cannot honestly be written; that case instead writes a
``node_collection_log`` record whose free-form ``detail`` may safely
carry the configured hostname under an explicitly-named
``configured_hostname`` key, never claiming it is a validated FQDN.

``failure_type`` is classified via ``getattr(exc, "failure_type",
None)`` checked against a fixed, closed set of the design's own
enumerated poll-failure vocabulary (mirroring
``collector.transport``'s own documented convention: "One exception
subclass per node_poll_failures.failure_type ... so a caller builds a
poll-failure record straight from exc.failure_type") -- any value
outside that closed set (including no attribute at all, e.g. a plain
``RuntimeError`` or a hostile custom exception class) falls back to
the fixed ``"invariant_violation"`` literal. This module still never
imports ``collector.transport`` itself (see class docstring below):
classification reads a bounded, membership-checked attribute value off
whatever exception ``transport_fn`` happens to raise, it does not
``isinstance``-check against that module's exception hierarchy. The
persisted ``detail`` string is always one fixed, non-parameterized
literal -- never the exception's own message or class name -- so a
hostile failure (arbitrary secret/argv text in the exception message,
or an attacker-controlled ``__name__``) can never reach the sink
through this path, mirroring ``_classify_hardware_failure``'s own
fixed-vocabulary-only discipline for the hardware-collection-failure
path above. ``consecutive_failures``/``breaker_state`` are ALWAYS the
authoritative ``collector.scheduler.Scheduler`` per-target state for
this exact ``(node, loop)`` pair -- including any ``scheduler_miss``
overlaps counted against that same target before this ordinary
failure -- never a second, independently-counted value. This module
tracks that authoritative state itself (see ``_observe_scheduler_
event``/``self._scheduler_state``) purely because the scheduler's
own per-target ``_TargetState`` is private and not incremented for
THIS failure until after ``poll_fn`` (this module's own ``_poll_fn``)
returns/raises -- i.e. after ``_record_ordinary_poll_failure`` has
already had to decide what to persist. Fidelity to the scheduler's own
count is what lets a same-node/same-loop miss immediately followed by
an ordinary transport failure open the breaker at the correct
cumulative count instead of under-reporting it (review round 2
finding: a prior version kept a second, ordinary-failure-only counter
here that silently diverged from the scheduler's own whenever a
scheduler_miss preceded an ordinary failure for the same target).

Still DEFERRED to a follow-up increment: signal handling, the
partial-vs-clean acceptance evaluation, and CLI/deploy/docs wiring.

KNOWN GAP carried over from the first increment: a trailing partial
counter window (fewer than ``rollup_interval_sec // counter_interval_
sec`` samples when the run's duration/stop ends mid-window) is never
flushed -- only a window that reaches its full expected sample count
calls ``CounterWindowAccumulator.finalize()``. The identical gap now
also applies to the analogous ``UsageIntervalAccumulator`` windowing
added in this increment, for the same reason: the design's own
accumulators already support finalizing a degraded/partial window,
but wiring that flush into ``Daemon.run()``'s post-scheduler shutdown
path is left to the deferred partial/clean-summary follow-up rather
than implemented speculatively here without its own test.

Nothing in this module imports a database driver, an ORM, or
``node_monitor.database``/``node_monitor.db`` -- design: "prove no
database imports/connections" (Task 7 write-up). Only
``node_monitor.collector.cpu_delta``, ``node_monitor.collector.
metrics``, ``node_monitor.collector.scheduler``, ``node_monitor.
collector.usage``, and ``node_monitor.output.jsonl`` are imported,
none of which touch PostgreSQL either (unlike ``node_monitor.
collector.hardware``, which is deliberately NOT imported here -- that
module's ``upsert_hardware``/SQLAlchemy-based upsert is a future
production-database concern, not this Phase 0 JSONL-only daemon's;
this module builds its own minimal, pure ``node_hardware`` contract
record straight from a raw hwinfo probe payload instead).
"""

import time

from node_monitor.collector.cpu_delta import compute_cpu_delta
from node_monitor.collector.metrics import CounterWindowAccumulator
from node_monitor.collector.scheduler import (
   BREAKER_CLOSED,
   BREAKER_OPEN,
   Scheduler,
   Target,
)
from node_monitor.collector.usage import (
   UsageIntervalAccumulator,
   build_diagnostic_census,
   build_usage_observations,
)
from node_monitor.output.jsonl import Phase0SinkDiskFullError, Phase0SinkError

# Exit codes for Daemon.run(). Design: "Output write/flush failure or
# low disk is fatal ... [must stop] the daemon nonzero" -- a caller
# (a future CLI entrypoint, out of scope for this increment) can map
# these straight to a process exit status.
EXIT_OK = 0
EXIT_SINK_FATAL = 2

# Every node_hardware contract field this daemon can fill in from a raw
# hwinfo probe payload's ``payload["hardware"]`` sub-object, i.e.
# node_monitor.output.contracts._HARDWARE_REQUIRED minus the four
# bookkeeping fields (system, source_hostname, first_seen_utc,
# probe_version) the daemon itself knows rather than reading from the
# probe. Kept local to this module rather than imported from
# collector.hardware (see module docstring: that module is the future
# production-database upsert path and pulls in sqlalchemy, which this
# Phase 0 JSONL-only daemon must never import even transitively).
_HARDWARE_PROBE_FIELDS = (
   "boot_id", "btime", "cpu_model", "cpu_logical", "sockets",
   "cores_per_socket", "cpu_max_freq_khz", "numa_nodes", "mem_total_kb",
   "swap_total_kb", "hugepage_size_kb", "kernel_release", "os_pretty_name",
   "net_fs_mounts", "net_ifaces", "gpus",
)


def _default_wall_clock():
   return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + "Z"


# Fixed, daemon-owned vocabulary for ``_classify_hardware_failure`` below.
# Every entry pairs a well-known builtin exception type with a FIXED
# literal label -- never a value read off the exception instance or its
# class (e.g. never ``type(exc).__name__``/``exc.__class__.__name__``,
# both of which are ordinary, unprivileged, mutable Python attributes
# that any custom exception subclass controls). Order matters: more
# specific types are checked first (``TimeoutError`` is itself an
# ``OSError`` subclass since Python 3.3, so it must be tested before the
# broader ``OSError`` bucket or it would never be reached). The trailing
# ``Exception`` entry is the closed fallback bucket for every other
# exception this daemon has no more specific label for, INCLUDING any
# attacker-controlled custom exception class/message content -- an
# instance never contributes anything to the persisted label beyond
# which one of these fixed literals it happens to match by inheritance.
_HARDWARE_FAILURE_TYPE_MAP = (
   (TimeoutError, "TimeoutError"),
   (OSError, "OSError"),
   (LookupError, "LookupError"),
   (ValueError, "ValueError"),
   (RuntimeError, "RuntimeError"),
   (Exception, "Exception"),
)
_HARDWARE_FAILURE_TYPE_FALLBACK = "Exception"


def _classify_hardware_failure(exc):
   """Map ``exc`` to a fixed, bounded ``error_type`` label purely by
   ``isinstance`` identity/inheritance against ``_HARDWARE_FAILURE_TYPE_
   MAP`` -- never by reading any attribute off ``exc`` or ``type(exc)``.
   A hostile custom exception class (including one with a renamed,
   attacker-controlled, unbounded ``__name__`` -- review round 2 finding)
   can only ever fall through to the fixed ``\"Exception\"`` bucket; it
   has no way to influence the returned string itself.
   """
   for exc_type, label in _HARDWARE_FAILURE_TYPE_MAP:
      if isinstance(exc, exc_type):
         return label
   return _HARDWARE_FAILURE_TYPE_FALLBACK


# Fixed, closed vocabulary this daemon will ever persist as an ordinary
# poll failure's ``node_poll_failures.failure_type`` -- exactly
# node_monitor.output.contracts._POLL_FAILURE_TYPES minus the two
# entries that are never reachable through this path: "scheduler_miss"
# (the scheduler's own bookkeeping, out of scope -- see module
# docstring) and "reboot"/"reset" (metrics-layer concepts compared
# across *successful* consecutive samples, never raised as an
# exception by transport_fn). Kept local to this module -- rather than
# importing node_monitor.collector.transport's exception hierarchy --
# because this module intentionally stays agnostic to how transport_fn
# is implemented (it is an injected callable, design: local/remote
# parity lives entirely in what the caller's own transport_fn does);
# classification below reads only a plain, membership-checked
# ``failure_type`` attribute value, mirroring collector.transport's own
# documented convention without requiring that module's types.
_ORDINARY_POLL_FAILURE_TYPES = frozenset((
   "timeout", "ssh_auth", "ssh_transport", "probe_exit", "malformed_json",
   "probe_version_mismatch", "hostname_mismatch", "invariant_violation",
))
_ORDINARY_POLL_FAILURE_FALLBACK = "invariant_violation"


def _classify_ordinary_poll_failure(exc):
   """Map ``exc`` to a fixed, bounded ``failure_type`` label.

   Reads ``getattr(exc, \"failure_type\", None)`` -- an ordinary,
   caller-controlled instance/class attribute -- but the returned value
   is used ONLY as a membership test against the fixed
   ``_ORDINARY_POLL_FAILURE_TYPES`` set, never persisted verbatim: a
   hostile exception setting ``failure_type = \"argv=...SECRET...\"``
   fails that membership test exactly like a plain exception with no
   attribute at all, and both fall through to the same fixed
   ``_ORDINARY_POLL_FAILURE_FALLBACK`` literal. This is the same
   discipline ``_classify_hardware_failure`` applies to
   ``type(exc).__name__`` above, applied here to a different
   caller-controlled attribute.
   """
   candidate = getattr(exc, "failure_type", None)
   if candidate in _ORDINARY_POLL_FAILURE_TYPES:
      return candidate
   return _ORDINARY_POLL_FAILURE_FALLBACK


# Design: "Each (node, loop) uses ... independent bounded exponential
# backoff after three consecutive failures". Must match
# node_monitor.collector.scheduler._DEFAULT_FAILURE_THRESHOLD exactly:
# Daemon.run() never overrides the Scheduler's failure_threshold, so
# this is the same three-consecutive-failure boundary the actually
# constructed Scheduler uses for ITS OWN breaker. Kept as a local
# constant (rather than reading Scheduler._DEFAULT_FAILURE_THRESHOLD)
# because a node_poll_failures record for a given ordinary failure must
# be computed and written BEFORE the Scheduler's own per-target state
# observes that same failure (see _record_ordinary_poll_failure) --
# there is no live Scheduler/target-state object to consult yet at
# that point, only this mirror of what the Scheduler will compute.
_POLL_FAILURE_BREAKER_THRESHOLD = 3


class Daemon:
   """Orchestrates one Phase 0 run: config + transport + scheduler +
   the counter-rollup transform + sink.

   Every collaborator that would otherwise block a fast unit test is
   injected:

   ``config``: a validated ``node_monitor.config.Phase0Config``.
   ``sink``: anything shaped like ``node_monitor.output.jsonl.
      Phase0Sink`` -- an async ``write_record(record_type, record)``,
      an async ``finalize_summary()``, and a sync ``write_done()``.
      A real ``Phase0Sink`` already writes its manifest at
      construction time, so this class never touches the manifest
      itself.
   ``transport_fn(node, loop)``: async callable returning the raw
      probe payload dict for one poll -- the same shape ``collector.
      transport.run_local_probe``/``run_remote_probe`` hand back as
      ``ProbeResult.payload``. Which node is local vs remote, and how
      its probe is actually invoked, is entirely the caller's
      business to wire up via this one callable; this module never
      imports ``collector.transport`` itself.
   ``clock``/``sleep``: passed straight through to the injected
      ``scheduler_cls`` (default ``Scheduler``) exactly like its own
      constructor -- a test drives a ``FakeClock`` the same way
      ``tests/test_scheduler.py`` does.
   """

   def __init__(self, config, sink, transport_fn, *,
                clock=time.monotonic, sleep=None,
                wall_clock_fn=_default_wall_clock,
                scheduler_cls=Scheduler, on_event=None):
      self._config = config
      self._sink = sink
      self._transport_fn = transport_fn
      self._clock = clock
      self._sleep = sleep
      self._wall_clock_fn = wall_clock_fn
      self._scheduler_cls = scheduler_cls
      self._on_event = on_event
      self._scheduler = None

      # One CounterWindowAccumulator per node hostname, replaced every
      # time its window rolls over -- bounded state, never one
      # accumulator per sample. Design: "counter rollup every 60
      # seconds" -- expressed here as a sample COUNT (rollup_interval
      # // counter_interval) rather than a wall-clock boundary check,
      # which is what lets an accelerated test with a 1-second counter
      # interval and a 3-second rollup interval observe a real rollup
      # after exactly 3 fake-clock samples instead of waiting out a
      # real 60 seconds.
      self._counter_expected_count = max(
         1, int(config.rollup_interval_sec // config.counter_interval_sec))
      self._counter_accumulators = {}
      self._counter_sample_counts = {}

      # Same sample-count-windowing pattern as the counter/rollup pair
      # above, applied to the census/usage-interval pair: a
      # UsageIntervalAccumulator per node, replaced every time its
      # window rolls over, sized in CENSUS SAMPLE COUNT (usage_interval
      # // census_interval) rather than a wall-clock boundary check.
      self._usage_expected_count = max(
         1, int(config.usage_interval_sec // config.census_interval_sec))
      self._usage_accumulators = {}
      self._usage_sample_counts = {}
      # The immediately-preceding raw census payload per node, needed
      # by collector.cpu_delta.compute_cpu_delta/build_usage_
      # observations to attribute CPU between two consecutive samples.
      # None for a node's first census of the run -- both helper
      # functions already handle that "no previous sample yet" case
      # explicitly rather than requiring a caller-side special case.
      self._previous_census_payload = {}

      # The most recent remote-reported FQDN observed from ANY
      # successful poll (hwinfo, counter, or census) for a node, keyed
      # by node.hostname (the only identity known before a payload
      # arrives). Design: "SSH aliases are transport identifiers only,
      # never provenance" -- an ordinary poll failure's
      # node_poll_failures.source_hostname must use this previously
      # established FQDN, never the configured node.hostname alias.
      # Absent until a node's first successful poll of any kind.
      self._established_fqdn = {}

      # Per-(node.hostname, loop) MIRROR of collector.scheduler.
      # Scheduler's own authoritative per-target consecutive_failures/
      # breaker_state, updated from the scheduler's own "scheduler_
      # miss"/"failure"/"success" events (see _observe_scheduler_
      # event). This is a mirror, not an independent count: review
      # round 2 finding -- a prior version kept its own separate
      # ordinary-failure-only counter here, which silently diverged
      # from the scheduler's real breaker state whenever a
      # same-node/same-loop scheduler_miss preceded an ordinary
      # transport failure for that same target (the scheduler's own
      # count include misses; the daemon-local one previously did
      # not). Reflects the scheduler's state as of the MOST RECENT
      # event observed for that (node, loop) pair -- i.e. as of just
      # BEFORE whatever ordinary failure is currently being recorded,
      # since the scheduler itself only increments/emits for that
      # failure AFTER this module's own poll_fn (``_poll_fn`` below)
      # returns or raises.
      self._scheduler_state = {}

      # Set the instant a fatal sink condition is observed; run()
      # checks this AFTER the scheduler stops to decide the exit code
      # and whether it is safe to finalize at all. Never cleared once
      # set -- a run that hit a fatal sink error must never be
      # reported clean.
      self._fatal_error = None

   def _emit(self, **fields):
      if self._on_event is not None:
         self._on_event(fields)

   async def _observe_scheduler_event(self, event):
      """Update ``self._scheduler_state`` -- this module's mirror of
      ``collector.scheduler.Scheduler``'s own per-(node, loop)
      ``consecutive_failures``/``breaker_state`` -- from every event
      the Scheduler emits, persist a structured ``node_poll_failures``
      (or pre-FQDN ``node_collection_log``) record for a
      ``scheduler_miss`` event specifically, then forward the event
      unchanged to whatever ``on_event`` callable this Daemon itself
      was constructed with (if any).

      Installed as the Scheduler's OWN ``on_event`` (see ``run()``
      below) rather than merely observing failures from inside
      ``_poll_fn``: a ``scheduler_miss`` event is emitted entirely
      inside ``Scheduler._run_target`` for a same-node/same-loop
      overlap that this module's ``_poll_fn`` never itself sees (no
      ``transport_fn`` invocation happens for a miss) -- the ONLY way
      this module can learn a miss occurred at all, let alone record
      it or fold it into an immediately-following ordinary failure's
      own node_poll_failures record, is by watching the Scheduler's
      own event stream directly (review round 2 finding on the prior
      increment: a same-node/same-loop miss immediately followed by an
      ordinary transport failure must open the breaker at the combined
      count, not just the ordinary-failure-only count this module
      could see on its own).

      This method is itself ``async`` (a prior increment's version was
      sync) and ``Scheduler._emit`` (see ``collector/scheduler.py``)
      awaits whatever it returns -- ``Scheduler._run_target`` does not
      proceed to its own next-dispatch decision for this target until
      this coroutine (including the miss's own sink write below) has
      returned or raised. This is what keeps the miss-triggered write
      serialized/deterministic rather than a detached, fire-and-forget
      background task racing the scheduler's own subsequent bookkeeping
      for the same target.

      A sink write/flush/OSError failure recording a miss is fatal by
      the exact same contract as every other sink write in this module
      (design: "Output write/flush failure or low disk is fatal") --
      this method still forwards the event to ``self._on_event`` (via
      the ``finally`` block) before re-raising, so an observer that
      only cares about event forwarding (this module's own caller, or
      a test asserting on the raw event stream) is not starved of the
      final event just because that same event's sink write happened
      to be the one that failed; the fatal condition itself is
      recorded via ``self._fatal_error``/``self._scheduler.
      request_stop()`` exactly like ``_poll_fn``'s own sink-failure
      handling, so the run still stops, nonzero, without a false DONE.
      """
      event_type = event.get("type")
      if event_type in ("scheduler_miss", "failure"):
         node = event["node"]
         key = (node.hostname, event["loop"])
         self._scheduler_state[key] = (
            event["consecutive_failures"], event["breaker_state"])
      elif event_type == "success":
         node = event["node"]
         key = (node.hostname, event["loop"])
         self._scheduler_state[key] = (0, BREAKER_CLOSED)

      try:
         if event_type == "scheduler_miss":
            try:
               await self._record_scheduler_miss(event)
            except (Phase0SinkDiskFullError, Phase0SinkError, OSError) as sink_exc:
               # Same fatal contract as every other sink write in this
               # module. Deliberately NOT re-raised past this point:
               # Scheduler itself is agnostic to sink exception types
               # and does not need to see this exception to react
               # correctly -- request_stop() (called here) already
               # flips self._stop_requested/self._stop_event, which
               # Scheduler._run_target checks immediately after this
               # awaited call returns (see that method's own comment)
               # to stop this target's own loop before it launches a
               # further, doomed dispatch in the same iteration.
               # Daemon.run() itself checks self._fatal_error (set
               # here) right after self._scheduler.run() returns to
               # produce EXIT_SINK_FATAL -- propagating this exception
               # up through Scheduler's own asyncio.gather() would only
               # risk an unhandled-exception crash out of run() instead
               # of that defined, clean nonzero result.
               self._fatal_error = sink_exc
               self._scheduler.request_stop()
      finally:
         if self._on_event is not None:
            self._on_event(event)

   async def _record_scheduler_miss(self, event):
      """Write one structured record for a ``scheduler_miss`` event --
      a same-node/same-loop overlap that ``Scheduler._run_target``
      cancelled and reaped before dispatching a new attempt for that
      same target. ``node_poll_failures`` (``failure_type=
      "scheduler_miss"``) once this node's FQDN has been established by
      a prior successful poll of any kind, or the same pre-FQDN
      ``node_collection_log`` fallback ``_record_ordinary_poll_failure``
      uses (never the configured SSH-alias hostname as
      ``source_hostname``) before that.

      Unlike ``_record_ordinary_poll_failure``, no "prior mirrored
      count plus one" anticipation is needed here: ``event[
      "consecutive_failures"]``/``event["breaker_state"]`` ARE already
      the Scheduler's own authoritative POST-increment state for this
      exact miss -- ``Scheduler._run_target`` increments
      ``state.consecutive_failures`` and reads ``state.breaker_state``
      BEFORE emitting this event (see that method's own non-overlap
      handling), unlike an ordinary ``poll_fn`` failure, which this
      module's ``_poll_fn``/``_record_ordinary_poll_failure`` must
      persist BEFORE the Scheduler's own ``_execute`` has had a chance
      to observe and count it.

      ``detail`` is a fixed, non-parameterized literal (never the
      Scheduler's own event fields verbatim beyond the bounded
      loop/count/state values already checked against contracts.py's
      own closed vocabularies) -- mirroring every other bounded-detail
      discipline in this module.
      """
      node = event["node"]
      loop = event["loop"]
      count = event["consecutive_failures"]
      breaker_state = event["breaker_state"]
      established_fqdn = self._established_fqdn.get(node.hostname)

      if established_fqdn is None:
         # Same honest-gap fallback as _record_ordinary_poll_failure:
         # no FQDN has ever been established for this node, so a
         # node_poll_failures record (whose source_hostname is
         # REQUIRED, non-nullable) cannot be written honestly yet.
         record = {
            "system": self._config.system,
            "timestamp_utc": self._wall_clock_fn(),
            "event": "poll_failed_before_fqdn_established",
            "detail": {
               "configured_hostname": node.hostname,
               "loop": loop,
               "failure_type": "scheduler_miss",
               "consecutive_failures": count,
               "breaker_state": breaker_state,
            },
         }
         await self._sink.write_record("node_collection_log", record)
         return

      record = {
         "system": self._config.system,
         "source_hostname": established_fqdn,
         "loop": loop,
         "timestamp_utc": self._wall_clock_fn(),
         "failure_type": "scheduler_miss",
         "detail": "scheduler_miss: same-node/same-loop overlap; see failure_type",
         "consecutive_failures": count,
         "breaker_state": breaker_state,
      }
      await self._sink.write_record("node_poll_failures", record)

   def _new_counter_accumulator(self, node, payload):
      now = self._wall_clock_fn()
      # Design (PHASE0_DAEMON_DESIGN.md line 25): "SSH aliases are
      # transport identifiers only, never provenance." node.hostname is
      # the configured SSH-alias/transport identity; the record's
      # source_hostname must instead be the probe's own remote-reported
      # hostname_fqdn (review round 1 finding #1). Bookkeeping is still
      # keyed by node.hostname throughout this module (it is the only
      # identity known BEFORE a payload arrives), but every persisted
      # record uses the reported FQDN.
      return CounterWindowAccumulator(
         system=self._config.system,
         source_hostname=payload.get("hostname_fqdn"),
         collector_hostname=self._config.local_node.hostname,
         probe_version=None,
         daemon_version=None,
         window_start_utc=now,
         window_end_utc=now,
         expected_count=self._counter_expected_count,
      )

   async def _flush_counter_window(self, node, payload):
      accumulator = self._counter_accumulators.pop(node.hostname)
      self._counter_sample_counts[node.hostname] = 0
      record = accumulator.finalize()
      # The accumulator was seeded with probe_version=daemon_version=
      # None before the first sample was known -- backfill both from
      # the payload/module here rather than threading them through
      # every add_sample() call, since neither value ever changes
      # mid-window for a given node.
      record["probe_version"] = payload.get("probe_version")
      record["daemon_version"] = record["daemon_version"] or "0.0.0"
      record["window_end_utc"] = self._wall_clock_fn()
      await self._sink.write_record("node_counter_samples", record)

   async def _handle_counter_poll(self, node, payload):
      """Feed one raw counter-loop payload into this node's bounded
      rollup accumulator, finalizing and writing a
      ``node_counter_samples`` record whenever the window fills.
      """
      accumulator = self._counter_accumulators.get(node.hostname)
      if accumulator is None:
         accumulator = self._new_counter_accumulator(node, payload)
         self._counter_accumulators[node.hostname] = accumulator
      accumulator.add_sample(payload)
      count = self._counter_sample_counts.get(node.hostname, 0) + 1
      self._counter_sample_counts[node.hostname] = count
      if count >= self._counter_expected_count:
         await self._flush_counter_window(node, payload)

   def _new_usage_accumulator(self, node, payload):
      now = self._wall_clock_fn()
      # Same FQDN-provenance rule as _new_counter_accumulator: never the
      # configured node.hostname alias.
      return UsageIntervalAccumulator(
         system=self._config.system,
         source_hostname=payload.get("hostname_fqdn"),
         interval_start_utc=now,
         interval_end_utc=now,
         expected_count=self._usage_expected_count,
      )

   async def _flush_usage_window(self, node):
      accumulator = self._usage_accumulators.pop(node.hostname)
      self._usage_sample_counts[node.hostname] = 0
      records = accumulator.finalize()
      # The accumulator was seeded with interval_end_utc == its own
      # interval_start_utc (the window's own birth instant, before any
      # sample was known) -- backfill the real end-of-window timestamp
      # here, exactly mirroring _flush_counter_window's own
      # window_end_utc backfill for the analogous counter/rollup pair.
      now = self._wall_clock_fn()
      for record in records:
         record["interval_end_utc"] = now
         await self._sink.write_record("node_usage_intervals", record)

   async def _handle_census_poll(self, node, payload):
      """Feed one raw census-loop payload into (1) a per-poll
      privacy-filtered ``diagnostic_census`` record and (2) this node's
      bounded 15-minute usage-interval accumulator, finalizing and
      writing bounded ``node_usage_intervals`` records whenever the
      window fills.

      ``collector.cpu_delta.compute_cpu_delta`` needs two consecutive
      raw payloads for the same node to attribute CPU -- ``None`` is
      passed for both ``build_diagnostic_census``'s ``cpu_deltas`` and
      ``build_usage_observations``'s ``previous_payload`` on a node's
      first census of the run, exactly as both functions already
      document handling explicitly.
      """
      previous_payload = self._previous_census_payload.get(node.hostname)

      cpu_deltas = None
      if previous_payload is not None:
         cpu_deltas = compute_cpu_delta(previous_payload, payload)
      census_record = build_diagnostic_census(
         self._config.system, payload.get("hostname_fqdn"), payload,
         cpu_deltas=cpu_deltas)
      await self._sink.write_record("diagnostic_census", census_record)

      observations = build_usage_observations(payload, previous_payload)
      self._previous_census_payload[node.hostname] = payload

      accumulator = self._usage_accumulators.get(node.hostname)
      if accumulator is None:
         accumulator = self._new_usage_accumulator(node, payload)
         self._usage_accumulators[node.hostname] = accumulator
      accumulator.add_sample(observations)
      count = self._usage_sample_counts.get(node.hostname, 0) + 1
      self._usage_sample_counts[node.hostname] = count
      if count >= self._usage_expected_count:
         await self._flush_usage_window(node)

   async def _poll_fn(self, node, loop):
      try:
         payload = await self._transport_fn(node, loop)
      except Exception as exc:
         try:
            await self._record_ordinary_poll_failure(node, loop, exc)
         except (Phase0SinkDiskFullError, Phase0SinkError, OSError) as sink_exc:
            # Same fatal contract as every other sink write in this
            # module: a write/flush failure recording the poll failure
            # ITSELF is still a sink failure, and design's "Output
            # write/flush failure or low disk is fatal" draws no
            # exception for the failure-record path.
            self._fatal_error = sink_exc
            self._scheduler.request_stop()
            raise
         raise
      self._established_fqdn[node.hostname] = payload.get("hostname_fqdn")
      try:
         if loop == "counter":
            await self._handle_counter_poll(node, payload)
         elif loop == "census":
            await self._handle_census_poll(node, payload)
      except (Phase0SinkDiskFullError, Phase0SinkError, OSError) as exc:
         # Design: "Output write/flush failure or low disk is fatal
         # because JSONL is the only Phase 0 result." A real
         # write/flush/fsync failure from the sink's own file handles
         # (node_monitor/output/jsonl.py's `handle.write`,
         # `handle.flush`, `os.fsync`) surfaces as a raw
         # OSError/IOError, not a Phase0SinkError subclass -- only
         # the *disk-full guard check* raises the dedicated
         # Phase0SinkDiskFullError. Catch OSError alongside the
         # sink's own exception types so an actual write failure is
         # just as fatal as a pre-flight low-disk rejection (review
         # round 1 finding: a raw OSError from write_record was
         # previously left uncaught here, so the scheduler absorbed
         # it as an ordinary poll failure and the run went on to
         # finalize/DONE as if nothing had happened). This applies
         # identically to every record type either loop handler can
         # write (node_counter_samples, diagnostic_census,
         # node_usage_intervals) -- record the fatal condition and
         # stop the scheduler from issuing any further polls, then
         # re-raise so this poll's own outcome is visible to the
         # scheduler's own failure/breaker bookkeeping like any other
         # poll_fn exception.
         self._fatal_error = exc
         self._scheduler.request_stop()
         raise
      return payload

   async def _record_ordinary_poll_failure(self, node, loop, exc):
      """Write one structured record for an ORDINARY (non-scheduler-miss)
      counter/census poll failure -- ``node_poll_failures`` once this
      node's FQDN has been established by a prior successful poll of
      any kind, or ``node_collection_log`` (never the configured
      SSH-alias hostname as ``source_hostname``) before that.

      ``failure_type``/category is always ``_classify_ordinary_poll_
      failure(exc)``'s fixed, closed-vocabulary label -- never
      ``str(exc)`` or ``type(exc).__name__`` verbatim -- mirroring
      ``_log_hardware_collection_failure``'s own scrubbing discipline
      for the analogous hardware-collection-failure path.

      ``consecutive_failures``/``breaker_state`` must be exactly what
      ``collector.scheduler.Scheduler``'s own ``_execute`` is about to
      compute for THIS SAME failure once this coroutine returns/raises
      back up to it -- including any scheduler_miss(es) already
      recorded against this same (node, loop) target (review round 2
      finding: excluding scheduler_miss from what this module counts
      as an "ordinary" failure type does not mean a prior miss's
      effect on the breaker/consecutive-failure state can be excluded
      from an ordinary failure's own record). ``self._scheduler_state``
      is this module's mirror of the Scheduler's own per-target state,
      updated by ``_observe_scheduler_event`` from every event the
      Scheduler emits (scheduler_miss, failure, success) -- but that
      mirror necessarily still reflects the state as of the LAST event
      observed (i.e. from BEFORE this exact failure, which the
      Scheduler itself only observes and emits an event for AFTER this
      coroutine's caller, ``_poll_fn``, propagates ``exc`` back up to
      ``Scheduler._execute``). So the count/state actually persisted
      here is deliberately computed one step ahead of the mirror:
      exactly the prior mirrored count plus one, and the breaker state
      that implies -- matching, field for field, what ``_execute``
      will independently compute for the very same failure via its own
      identical ``consecutive_failures += 1`` / threshold comparison.
      """
      key = (node.hostname, loop)
      prior_count, _prior_state = self._scheduler_state.get(
         key, (0, BREAKER_CLOSED))
      count = prior_count + 1
      failure_type = _classify_ordinary_poll_failure(exc)
      established_fqdn = self._established_fqdn.get(node.hostname)
      breaker_state = (
         BREAKER_OPEN if count >= _POLL_FAILURE_BREAKER_THRESHOLD
         else BREAKER_CLOSED)

      if established_fqdn is None:
         # No FQDN has ever been established for this node -- writing
         # a node_poll_failures record (whose source_hostname is
         # REQUIRED, non-nullable) would force either an outright
         # ContractError or a fallback to the configured alias, and
         # design forbids the latter as provenance. Surface the gap
         # via node_collection_log instead, whose free-form detail can
         # honestly carry the configured hostname under an
         # explicitly-named key. Still carries consecutive_failures/
         # breaker_state (review round 2 finding #2): this card scopes
         # ordinary counter/census failure records to include loop,
         # category/detail, consecutive failures, AND breaker state --
         # the pre-FQDN fallback is still an ordinary counter/census
         # failure record, just routed to a different sink table for
         # the one honest reason (no validated source_hostname yet).
         record = {
            "system": self._config.system,
            "timestamp_utc": self._wall_clock_fn(),
            "event": "poll_failed_before_fqdn_established",
            "detail": {
               "configured_hostname": node.hostname,
               "loop": loop,
               "failure_type": failure_type,
               "consecutive_failures": count,
               "breaker_state": breaker_state,
            },
         }
         await self._sink.write_record("node_collection_log", record)
         return

      record = {
         "system": self._config.system,
         "source_hostname": established_fqdn,
         "loop": loop,
         "timestamp_utc": self._wall_clock_fn(),
         "failure_type": failure_type,
         "detail": "ordinary poll failure; see failure_type",
         "consecutive_failures": count,
         "breaker_state": breaker_state,
      }
      await self._sink.write_record("node_poll_failures", record)

   async def _log_hardware_collection_failure(self, node, exc):
      """Write one ``node_collection_log`` record for a failed one-time
      hardware probe. Design: a hwinfo probe failure is not fatal to
      the run (only a sink write/flush failure is), but it still needs
      a structured place to surface -- the Scheduler's own
      breaker/backoff bookkeeping only covers polls it schedules
      itself, not this one-shot pre-scheduler step.

      Review round 1 finding #2: the probe never returned a payload
      here, so the exception's own message is the only detail
      available -- and a transport-layer exception's message can
      legitimately embed the failed command's argv (design: "Raw
      argv ... never persist"; PHASE0_DAEMON_DESIGN.md line 16/51/107).

      Review round 2 finding: ``type(exc).__name__`` is NOT a bounded,
      closed vocabulary -- it is the exception class's own ``__name__``
      attribute, which is ordinary, unprivileged, mutable Python state
      that any custom exception subclass (or even a plain ``Exception``
      instance whose class has been renamed) fully controls, including
      to arbitrary, unbounded, secret/argv-carrying text. Persist only
      ``_classify_hardware_failure(exc)``'s fixed, daemon-owned category
      label -- resolved purely by ``isinstance`` against a closed map of
      known exception types, never by reading any attribute off the
      exception instance or its class -- plus a fixed, non-parameterized
      message. Never ``str(exc)`` or ``type(exc).__name__`` verbatim.
      """
      record = {
         "system": self._config.system,
         "timestamp_utc": self._wall_clock_fn(),
         "event": "hardware_collection_failed",
         "detail": {
            "source_hostname": node.hostname,
            "error_type": _classify_hardware_failure(exc),
            "error": "hardware probe failed; see error_type",
         },
      }
      await self._sink.write_record("node_collection_log", record)

   async def _collect_hardware(self, node):
      """Collect and write the one-time ``node_hardware`` record for
      ``node``. Called sequentially, once per configured node, BEFORE
      the scheduler starts -- design: "node_hardware: one record per
      node when first seen in the run", which this daemon treats as a
      one-shot pre-scheduler step rather than a scheduled ``Target``/
      loop, since it never repeats within a run.

      A failed hwinfo probe itself is not fatal -- it is logged via
      ``_log_hardware_collection_failure`` and the run proceeds with no
      hardware baseline for this node. A failed *write* of either the
      ``node_hardware`` record or the failure's own
      ``node_collection_log`` record IS fatal (an ordinary
      Phase0SinkDiskFullError/Phase0SinkError/OSError propagates
      straight out of this method uncaught) -- identical contract to
      every other sink write in this module; the caller (``run()``)
      is the one place that maps that into ``EXIT_SINK_FATAL``.
      """
      try:
         payload = await self._transport_fn(node, "hwinfo")
      except Exception as exc:
         await self._log_hardware_collection_failure(node, exc)
         return

      self._established_fqdn[node.hostname] = payload.get("hostname_fqdn")
      hardware = payload.get("hardware") or {}
      record = {
         "system": self._config.system,
         "source_hostname": payload.get("hostname_fqdn"),
         "first_seen_utc": self._wall_clock_fn(),
         "probe_version": payload.get("probe_version"),
      }
      for field in _HARDWARE_PROBE_FIELDS:
         record[field] = hardware.get(field)
      await self._sink.write_record("node_hardware", record)

   def _build_targets(self):
      # One counter Target and one census Target per configured node,
      # regardless of local/remote role: that distinction lives
      # entirely inside transport_fn, never in scheduling policy
      # (mirrors collector.scheduler's own local/remote-agnostic
      # design). Hardware collection is deliberately NOT a Target here
      # -- see _collect_hardware's docstring for why it is a one-shot
      # pre-scheduler step instead.
      targets = []
      for node in self._config.nodes:
         targets.append(Target(node=node, loop="counter",
                                interval_sec=self._config.counter_interval_sec))
         targets.append(Target(node=node, loop="census",
                                interval_sec=self._config.census_interval_sec))
      return targets

   async def run(self):
      """Run every configured target to completion (or until a fatal
      sink error stops the scheduler early), then finalize the sink
      and return an exit code.

      Returns ``EXIT_OK`` after a clean ``finalize_summary()`` +
      ``write_done()``. Returns ``EXIT_SINK_FATAL`` -- WITHOUT ever
      calling ``finalize_summary()``/``write_done()`` -- the instant a
      fatal sink condition was observed during the run: design's
      "fatal" means exactly that, not "downgrade to a nonzero exit
      code but still produce a DONE-marked artifact that looks
      complete."
      """
      # Hardware-once collection runs sequentially, BEFORE the scheduler
      # starts -- design: "node_hardware: one record per node when
      # first seen in the run". A fatal sink error here (a real
      # write/flush failure or the disk guard, exactly like every
      # other sink write in this module) must abort the run before the
      # scheduler ever begins issuing counter/census polls -- there is
      # no point starting a run whose own hardware baseline already
      # failed to persist.
      try:
         for node in self._config.nodes:
            await self._collect_hardware(node)
      except (Phase0SinkDiskFullError, Phase0SinkError, OSError) as exc:
         self._fatal_error = exc
         return EXIT_SINK_FATAL

      targets = self._build_targets()
      scheduler_kwargs = dict(
         targets=targets, poll_fn=self._poll_fn, clock=self._clock,
         max_parallel_polls=self._config.max_parallel_polls,
         duration_sec=self._config.duration_sec,
         # _observe_scheduler_event, not self._on_event directly: this
         # module must observe every scheduler_miss/failure/success
         # event itself (to keep self._scheduler_state -- the mirror
         # of the Scheduler's own per-target consecutive_failures/
         # breaker_state -- accurate for node_poll_failures records),
         # while still forwarding every event on to this Daemon's own
         # caller-supplied on_event exactly as before.
         on_event=self._observe_scheduler_event)
      if self._sleep is not None:
         scheduler_kwargs["sleep"] = self._sleep
      self._scheduler = self._scheduler_cls(**scheduler_kwargs)

      await self._scheduler.run()

      if self._fatal_error is not None:
         return EXIT_SINK_FATAL

      try:
         await self._sink.finalize_summary()
         self._sink.write_done()
      except (Phase0SinkDiskFullError, Phase0SinkError, OSError) as exc:
         # Same fatal contract as the counter-poll boundary above:
         # finalize_summary() flushes/fsyncs every open JSONL handle
         # and write_done() fsyncs its own DONE file, both of which
         # can raise a raw OSError (disk-full, EIO, a yanked mount)
         # exactly like write_record() can. Review round 1 finding:
         # this path previously let such an OSError escape run()
         # entirely, uncaught, instead of producing the defined
         # nonzero orchestration result. Design's "fatal" means the
         # run must never look complete: never write DONE after a
         # failed finalize_summary(), and treat a write_done()
         # failure (finalize succeeded, DONE failed) as fatal too --
         # a summary.json without a DONE flag is exactly the
         # "partial, not complete" state a caller/monitor needs to
         # see, not a silently swallowed exception.
         self._fatal_error = exc
         return EXIT_SINK_FATAL
      return EXIT_OK
