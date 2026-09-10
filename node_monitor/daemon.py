"""node_monitor.daemon -- Phase 0 orchestration layer.

Design: PHASE0_DAEMON_DESIGN.md + PHASE0_DAEMON_IMPLEMENTATION_PLAN.md
Task 7. Wires config, the probe transport, the independent scheduler,
per-node counter-rollup/census/usage transforms, and the atomic JSONL
sink into one injectable orchestration coroutine that a test can drive
end to end against a fake clock instead of a real 24-hour duration.

INCREMENT SCOPE NOTE (2026-09-10, second increment on kanban task
t_14ece72e, building on the first increment's counter-loop-only
orchestration core): this increment adds

* one-time hardware collection per configured node (design:
  "node_hardware: one record per node when first seen in the run"),
  run sequentially BEFORE the scheduler starts -- it is not a
  scheduled ``Target``/loop like counter and census, since it never
  repeats within a run;
* a ``census`` loop ``Target`` per node (alongside the existing
  ``counter`` loop), wired through the existing
  ``collector.usage.build_diagnostic_census``/``build_usage_
  observations``/``UsageIntervalAccumulator`` and
  ``collector.cpu_delta.compute_cpu_delta`` to produce
  privacy-filtered ``diagnostic_census`` records every census poll
  and bounded ``node_usage_intervals`` records every
  ``usage_interval_sec // census_interval_sec`` census samples (the
  same sample-count-windowing pattern the first increment already
  used for the counter/rollup pair);
* the minimum ``node_collection_log`` records needed by hardware-once
  (a failed hwinfo poll has no other structured place to surface a
  failure -- the Scheduler's own breaker bookkeeping only covers
  polls it schedules itself, not this one-shot pre-scheduler step).

Still DEFERRED to a follow-up increment: ``node_poll_failures``
records for ordinary counter/census poll failures (those already
degrade gracefully through the Scheduler's own breaker/backoff
bookkeeping without a daemon-level record, exactly as the first
increment left them), signal handling, the partial-vs-clean
acceptance evaluation, and CLI/deploy/docs wiring -- none of those are
needed to prove hardware-once/census/usage are correctly wired to the
existing transport, contracts, scheduler, and sink.

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
from node_monitor.collector.scheduler import Scheduler, Target
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

      # Set the instant a fatal sink condition is observed; run()
      # checks this AFTER the scheduler stops to decide the exit code
      # and whether it is safe to finalize at all. Never cleared once
      # set -- a run that hit a fatal sink error must never be
      # reported clean.
      self._fatal_error = None

   def _emit(self, **fields):
      if self._on_event is not None:
         self._on_event(fields)

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
      payload = await self._transport_fn(node, loop)
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
         duration_sec=self._config.duration_sec, on_event=self._on_event)
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
