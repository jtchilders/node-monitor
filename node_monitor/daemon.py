"""node_monitor.daemon -- Phase 0 orchestration layer.

Design: PHASE0_DAEMON_DESIGN.md + PHASE0_DAEMON_IMPLEMENTATION_PLAN.md
Task 7. Wires config, the probe transport, the independent scheduler,
a per-node counter-rollup transform, and the atomic JSONL sink into one
injectable orchestration coroutine that a test can drive end to end
against a fake clock instead of a real 24-hour duration.

INCREMENT SCOPE NOTE (2026-09-10 recovery direction on kanban task
t_51db5d6c): this file implements only the first coherent slice of
Task 7 -- a ``Daemon`` class with injectable clock/transport/sink/
scheduler that runs the counter loop end to end (manifest -> at least
one ``node_counter_samples`` rollup -> finalized summary -> DONE) and
treats a fatal sink condition (disk-full / sink misuse) as a nonzero
exit without finalizing. Census/usage/hardware wiring, structured
``node_poll_failures``/``node_collection_log`` records, signal
handling, and the partial-vs-clean acceptance evaluation are
DEFERRED to a follow-up increment -- not implemented here. See the
kanban card's review-recovery comment for the explicit scope cut.

Nothing in this module imports a database driver, an ORM, or
``node_monitor.database``/``node_monitor.db`` -- design: "prove no
database imports/connections" (Task 7 write-up). Only
``node_monitor.collector.metrics`` and ``node_monitor.collector.
scheduler``/``node_monitor.output.jsonl`` are imported, none of which
touch PostgreSQL either (unlike ``node_monitor.collector.hardware``,
which is deliberately NOT imported here).
"""

import time

from node_monitor.collector.metrics import CounterWindowAccumulator
from node_monitor.collector.scheduler import Scheduler, Target
from node_monitor.output.jsonl import Phase0SinkDiskFullError, Phase0SinkError

# Exit codes for Daemon.run(). Design: "Output write/flush failure or
# low disk is fatal ... [must stop] the daemon nonzero" -- a caller
# (a future CLI entrypoint, out of scope for this increment) can map
# these straight to a process exit status.
EXIT_OK = 0
EXIT_SINK_FATAL = 2


class DaemonError(Exception):
   """Raised for daemon-orchestration-level construction/usage errors."""


def _default_wall_clock():
   return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + "Z"


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

      # Set the instant a fatal sink condition is observed; run()
      # checks this AFTER the scheduler stops to decide the exit code
      # and whether it is safe to finalize at all. Never cleared once
      # set -- a run that hit a fatal sink error must never be
      # reported clean.
      self._fatal_error = None

   def _emit(self, **fields):
      if self._on_event is not None:
         self._on_event(fields)

   def _new_counter_accumulator(self, node):
      now = self._wall_clock_fn()
      return CounterWindowAccumulator(
         system=self._config.system,
         source_hostname=node.hostname,
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
         accumulator = self._new_counter_accumulator(node)
         self._counter_accumulators[node.hostname] = accumulator
      accumulator.add_sample(payload)
      count = self._counter_sample_counts.get(node.hostname, 0) + 1
      self._counter_sample_counts[node.hostname] = count
      if count >= self._counter_expected_count:
         await self._flush_counter_window(node, payload)

   async def _poll_fn(self, node, loop):
      payload = await self._transport_fn(node, loop)
      if loop == "counter":
         try:
            await self._handle_counter_poll(node, payload)
         except (Phase0SinkDiskFullError, Phase0SinkError) as exc:
            # Design: "Output write/flush failure or low disk is
            # fatal because JSONL is the only Phase 0 result." Record
            # the fatal condition and stop the scheduler from issuing
            # any further polls -- there is no point burning the
            # run's remaining duration against a sink that can no
            # longer accept writes -- then re-raise so this poll's
            # own outcome is visible to the scheduler's own
            # failure/breaker bookkeeping like any other poll_fn
            # exception.
            self._fatal_error = exc
            self._scheduler.request_stop()
            raise
      return payload

   def _build_targets(self):
      # Only the counter loop is wired in this increment -- see the
      # module docstring's scope note. One target per configured node
      # regardless of local/remote role: that distinction lives
      # entirely inside transport_fn, never in scheduling policy
      # (mirrors collector.scheduler's own local/remote-agnostic
      # design).
      return [
         Target(node=node, loop="counter",
                interval_sec=self._config.counter_interval_sec)
         for node in self._config.nodes
      ]

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

      await self._sink.finalize_summary()
      self._sink.write_done()
      return EXIT_OK
