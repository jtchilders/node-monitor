"""node_monitor.collector.scheduler -- independent monotonic scheduler
and per-(node, loop) circuit breaker.

Design: PHASE0_DAEMON_DESIGN.md "Architecture" + "Failure handling" and
PHASE0_DAEMON_IMPLEMENTATION_PLAN.md Task 6. This module knows nothing
about subprocesses, SSH, or the JSONL sink -- ``collector/transport.py``
and ``output/jsonl.py`` own those concerns. ``Scheduler`` is handed an
opaque ``poll_fn(node, loop)`` coroutine callback and drives it on a
fixed monotonic grid per ``(node, loop)`` pair, entirely through the
injected ``clock``/``sleep`` callables so a caller can run it under a
real event loop against ``time.monotonic``/``asyncio.sleep`` in
production or against a deterministic fake clock in tests -- a 24-hour
canary duration can never be exercised for real inside a unit test.

Per-target state machine (design: "A node has separate counter and
census state: previous successful sample, next deadline, active flag,
consecutive failures, and backoff deadline"):

* ``next_deadline`` advances by a FIXED grid step (``deadline + gap``),
  never by ``clock() + gap`` after the poll completes -- this is what
  "fixed monotonic deadlines, not sleep-after-completion" (no drift)
  means in practice: a poll that happens to take 3 of its 10 allotted
  seconds does not push every later deadline 3 seconds later.
* Every ``(node, loop)`` pair is its own independent asyncio task, so a
  slow node can never delay another node's, or another loop's,
  dispatch (design: "A slow node cannot delay another" / "independent
  loops").
* Same-node/same-loop non-overlap: if the previous dispatch for this
  target is still running when its OWN next deadline arrives, it is
  cancelled, awaited (reaped), and a ``scheduler_miss`` event is
  emitted -- before the new attempt starts.
* Breaker: three consecutive failures opens the breaker; while open,
  the gap to the next attempt is bounded exponential backoff seeded
  from the target's own normal interval (``interval * 2**k``, capped
  at ``backoff_cap_sec``) instead of the normal interval. A single
  success immediately resets the breaker and the gap.
* A global ``max_parallel_polls`` semaphore caps concurrency across
  every target combined, regardless of node role -- this module never
  branches on "local" vs "remote"; that distinction lives entirely in
  what ``poll_fn`` does with the ``node`` value it is handed, which is
  exactly what keeps local/remote parity at the scheduling-policy
  level (design: "local and remote parity").
* ``request_stop()`` and a finite ``duration_sec`` are the two ways
  ``run()`` returns: an explicit stop request interrupts an in-progress
  wait immediately (it does not wait out the remainder of the current
  interval) and stops any further dispatch; a target whose next
  deadline would land at or after the run's end time simply stops
  scheduling itself. Either way, the last dispatched poll (if any) is
  given a bounded grace period to finish before being cancelled --
  design: "gives active polls a bounded grace period, reaps children".
"""

import asyncio
import dataclasses
import time


class SchedulerError(Exception):
   """Raised for invalid Scheduler construction arguments."""


_DEFAULT_FAILURE_THRESHOLD = 3
_DEFAULT_BACKOFF_CAP_SEC = 300.0
_DEFAULT_GRACE_SEC = 2.0

# Matches node_monitor.output.contracts._BREAKER_STATES minus
# "half_open": that third state is an output-layer concept for the
# single post-backoff trial record; this module's own decision surface
# is fully described by whether the breaker is presently open.
BREAKER_CLOSED = "closed"
BREAKER_OPEN = "open"


@dataclasses.dataclass(frozen=True)
class Target:
   """One independently scheduled ``(node, loop)`` pair.

   ``node`` is opaque to this module -- a hostname string, or any
   caller-defined object ``poll_fn`` knows how to interpret. Nothing
   here inspects it for a "local" vs "remote" role; that is the whole
   point of keeping scheduling policy local/remote-agnostic.
   """

   node: object
   loop: str
   interval_sec: float

   def __post_init__(self):
      if self.interval_sec <= 0:
         raise SchedulerError(
            "interval_sec must be positive, got %r" % (self.interval_sec,))


class _TargetState:
   """Mutable per-target bookkeeping. Not exposed publicly."""

   __slots__ = ("next_deadline", "active_task", "consecutive_failures",
                "failure_threshold")

   def __init__(self, next_deadline, failure_threshold):
      self.next_deadline = next_deadline
      self.active_task = None
      self.consecutive_failures = 0
      # Stored per-state (not read from the module-level default) so
      # that ``breaker_state`` reflects whatever ``failure_threshold``
      # this Scheduler was actually constructed with -- review round 1
      # found this hard-coded to ``_DEFAULT_FAILURE_THRESHOLD``, which
      # contradicted the real scheduling policy whenever a caller
      # configured a non-default threshold.
      self.failure_threshold = failure_threshold

   @property
   def breaker_state(self):
      if self.consecutive_failures >= self.failure_threshold:
         return BREAKER_OPEN
      return BREAKER_CLOSED


class Scheduler:
   """Drive ``poll_fn(node, loop)`` on an independent fixed grid per
   ``(node, loop)`` target with a shared concurrency cap and per-target
   breaker/backoff. See module docstring for the full policy.
   """

   def __init__(self, targets, poll_fn, *,
                clock=time.monotonic, sleep=asyncio.sleep,
                max_parallel_polls=8, duration_sec=86400.0,
                failure_threshold=_DEFAULT_FAILURE_THRESHOLD,
                backoff_cap_sec=_DEFAULT_BACKOFF_CAP_SEC,
                grace_sec=_DEFAULT_GRACE_SEC,
                on_event=None):
      targets = list(targets)
      if not targets:
         raise SchedulerError("targets must be non-empty")
      if max_parallel_polls <= 0:
         raise SchedulerError(
            "max_parallel_polls must be positive, got %r"
            % (max_parallel_polls,))
      if duration_sec <= 0:
         raise SchedulerError(
            "duration_sec must be positive, got %r" % (duration_sec,))
      if failure_threshold <= 0:
         raise SchedulerError(
            "failure_threshold must be positive, got %r"
            % (failure_threshold,))
      if backoff_cap_sec <= 0:
         raise SchedulerError(
            "backoff_cap_sec must be positive, got %r" % (backoff_cap_sec,))
      if grace_sec < 0:
         raise SchedulerError(
            "grace_sec must be non-negative, got %r" % (grace_sec,))

      self._targets = targets
      self._poll_fn = poll_fn
      self._clock = clock
      self._sleep = sleep
      self._max_parallel_polls = max_parallel_polls
      self._duration_sec = float(duration_sec)
      self._failure_threshold = failure_threshold
      self._backoff_cap_sec = float(backoff_cap_sec)
      self._grace_sec = float(grace_sec)
      self._on_event = on_event if on_event is not None else (lambda event: None)
      self._stop_requested = False
      # asyncio.Semaphore/Event bind to the running loop at construction
      # time on this project's Python (3.9): building them here, in
      # __init__, would bind them to whatever loop happens to be current
      # when a Scheduler is merely CONSTRUCTED (often none at all, since
      # every test builds a Scheduler outside its own asyncio.run() call)
      # rather than the loop `run()` actually executes under. Deferred to
      # first use inside `run()`/`request_stop()` instead.
      self._semaphore = None
      self._stop_event = None

   def request_stop(self):
      """Request a graceful shutdown: dispatch of further polls stops
      as soon as any in-progress wait can be interrupted (it does not
      wait out the remainder of a sleeping target's current interval);
      already-active polls still get their bounded grace period.

      Safe to call before ``run()`` starts (a caller racing a signal
      handler against startup should not crash): the stop is recorded
      via the plain ``_stop_requested`` flag either way, and the
      loop-bound ``asyncio.Event`` is only touched if ``run()`` has
      already created it.
      """
      self._stop_requested = True
      if self._stop_event is not None:
         self._stop_event.set()

   def _emit(self, **fields):
      self._on_event(fields)

   async def run(self):
      """Run every target concurrently until each one's own deadline
      grid reaches the run's end time, or ``request_stop()`` is called.
      Returns once every target task has returned.
      """
      # asyncio.Semaphore/Event bind to the running loop at construction
      # time on this project's Python (3.9), so they cannot be built in
      # __init__ (a Scheduler is normally constructed before its own
      # asyncio.run() call, i.e. with no running loop at all). Build them
      # now, the first and only time code actually runs inside the loop
      # this Scheduler will use.
      self._semaphore = asyncio.Semaphore(self._max_parallel_polls)
      self._stop_event = asyncio.Event()
      if self._stop_requested:
         self._stop_event.set()

      start_time = self._clock()
      end_time = start_time + self._duration_sec
      tasks = [
         asyncio.ensure_future(
            self._run_target(
               target,
               _TargetState(next_deadline=start_time,
                             failure_threshold=self._failure_threshold),
               end_time))
         for target in self._targets
      ]
      await asyncio.gather(*tasks)

   async def _interruptible_sleep(self, seconds):
      """Sleep ``seconds`` (via the injected ``sleep``) unless
      ``request_stop()`` fires first, in which case return immediately.
      Returns True if the stop event fired before the sleep elapsed.
      """
      if self._stop_requested:
         return True
      sleep_task = asyncio.ensure_future(self._sleep(seconds))
      stop_task = asyncio.ensure_future(self._stop_event.wait())
      try:
         done, _pending = await asyncio.wait(
            {sleep_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
         return stop_task in done
      finally:
         for pending_task in (sleep_task, stop_task):
            if not pending_task.done():
               pending_task.cancel()
               try:
                  await pending_task
               except asyncio.CancelledError:
                  pass

   def _next_gap(self, state, target):
      """The gap (in seconds) from a dispatch's own deadline to the
      target's next deadline, given ``state.consecutive_failures`` as
      of right now -- normal interval while the breaker is closed,
      bounded exponential backoff seeded from the target's own
      interval once it is open.
      """
      if state.consecutive_failures >= self._failure_threshold:
         exponent = state.consecutive_failures - self._failure_threshold + 1
         return min(target.interval_sec * (2 ** exponent),
                    self._backoff_cap_sec)
      return target.interval_sec

   async def _wait_for_task(self, task, timeout):
      """Wait up to ``timeout`` (virtual seconds, via the injected
      ``sleep``) for ``task`` to finish on its own, OR until
      ``request_stop()`` fires. Returns without cancelling anything
      either way -- a task that is still running when this returns is
      simply left in place as ``state.active_task``, to be
      cancelled/reaped as a ``scheduler_miss`` at its own next deadline
      (or drained by the normal end-of-run/stop-requested finally
      block), exactly as before this method existed. This only exists
      so a poll that fails or succeeds ASYNCHRONOUSLY (genuinely
      suspends before resolving) has its outcome folded into
      ``consecutive_failures`` before the caller computes the next
      deadline -- see the call site's comment.

      Must watch the stop event, not just the timeout: a
      request_stop() that lands while this wait is pending (e.g. a
      target already in backoff, waiting out a long provisional gap)
      would otherwise never observe it until the FULL provisional gap
      elapses -- for an open breaker that can be minutes, and for a
      poll_fn that never completes it hangs the whole run() forever,
      since a stop mid-wait never reaches the outer loop's own
      stop-requested check.
      """
      if task.done():
         return
      timeout_task = asyncio.ensure_future(self._sleep(timeout))
      stop_task = asyncio.ensure_future(self._stop_event.wait())
      try:
         await asyncio.wait({task, timeout_task, stop_task},
                             return_when=asyncio.FIRST_COMPLETED)
      finally:
         for pending_task in (timeout_task, stop_task):
            if not pending_task.done():
               pending_task.cancel()
               try:
                  await pending_task
               except asyncio.CancelledError:
                  pass

   async def _cancel_and_reap(self, task):
      task.cancel()
      try:
         await task
      except asyncio.CancelledError:
         pass
      except Exception:
         # The task's own failure path (_execute) already emits a
         # "failure" event for a non-cancellation exception; a
         # cancelled-but-already-failed race must not raise a second,
         # unhandled exception out of the scheduler's own bookkeeping.
         pass

   async def _wait_with_grace(self, task):
      """Give ``task`` (already dispatched, not yet known to be done)
      up to ``grace_sec`` of virtual time to finish; cancel/reap it if
      it has not. No-op if ``task`` is None or already done.
      """
      if task is None or task.done():
         return
      sleep_task = asyncio.ensure_future(self._sleep(self._grace_sec))
      try:
         done, _pending = await asyncio.wait(
            {task, sleep_task}, return_when=asyncio.FIRST_COMPLETED)
         if task not in done:
            await self._cancel_and_reap(task)
      finally:
         if not sleep_task.done():
            sleep_task.cancel()
            try:
               await sleep_task
            except asyncio.CancelledError:
               pass

   async def _run_target(self, target, state, end_time):
      try:
         while True:
            if self._stop_requested:
               break
            if state.next_deadline >= end_time:
               break

            now = self._clock()
            if state.next_deadline > now:
               stopped = await self._interruptible_sleep(
                  state.next_deadline - now)
               if stopped:
                  break

            deadline = state.next_deadline

            # Same-node/same-loop non-overlap (design: "a poll still
            # active at its next deadline is terminated, reaped, and
            # recorded as a scheduler failure"). The design's own
            # wording -- "recorded as a scheduler failure" -- means a
            # miss must feed the same breaker/backoff counter a
            # poll_fn exception does (review round 1 finding 2: this
            # previously emitted the event without ever touching
            # consecutive_failures, so repeated overruns could never
            # open or back off the breaker).
            if state.active_task is not None and not state.active_task.done():
               await self._cancel_and_reap(state.active_task)
               state.consecutive_failures += 1
               self._emit(type="scheduler_miss", node=target.node,
                          loop=target.loop, deadline=deadline,
                          consecutive_failures=state.consecutive_failures,
                          breaker_state=state.breaker_state)

            task = asyncio.ensure_future(self._execute(target, state, deadline))
            state.active_task = task

            # Wait up to the gap this dispatch would get if it turns
            # out to succeed-or-not-yet-fail (computed from
            # consecutive_failures as of the moment it was launched)
            # for the task to actually finish, so a poll that fails
            # ASYNCHRONOUSLY -- genuinely suspends (e.g. `await
            # sleep(...)`) before raising -- still has its outcome
            # folded into consecutive_failures before this loop
            # commits to the NEXT deadline. Review round 1 finding 1:
            # a plain ``await asyncio.sleep(0)`` here only caught a
            # poll_fn that failed WITHOUT ever suspending; a poll_fn
            # that awaited something first would not be done yet, so
            # the third failure's backoff would not apply until a
            # full extra normal-interval cycle later.
            #
            # The wait is bounded by the PROVISIONAL (pre-outcome) gap,
            # not an unbounded wait for completion: a poll that
            # legitimately overruns its own interval (the non-overlap
            # case) must never stall this loop past its own next
            # deadline. Fixed-grid, no-drift is preserved because the
            # actual deadline is always computed as ``deadline + gap``
            # below, never from whenever this wait happens to return --
            # a fast-but-slow-ish successful poll (finishes well inside
            # its own interval) simply has its outcome observed a
            # little earlier, it does not shift the grid.
            provisional_gap = self._next_gap(state, target)
            await self._wait_for_task(task, provisional_gap)

            state.next_deadline = deadline + self._next_gap(state, target)
      finally:
         # Two different endings need two different drain policies.
         # An explicit request_stop() is the urgent signal-driven case
         # the design describes: "gives active polls a bounded grace
         # period[, then] reaps children" -- an active poll gets
         # grace_sec to finish on its own before being cancelled.
         # Reaching this target's own end_time with NO stop requested
         # is not urgent: it can only mean either the poll already
         # finished, or it is still queued behind the shared
         # max_parallel_polls semaphore (e.g. a long-interval target
         # whose one-shot dispatch is still waiting for a slot when a
         # short overall duration_sec ends). Applying the same short
         # grace_sec here would cancel that queued work purely because
         # of concurrency backlog, not because it overran -- observed
         # as a false-positive cancellation that let a task's semaphore
         # permit release race a not-yet-started waiter into exceeding
         # max_parallel_polls. So the natural-end path simply awaits
         # whatever is already dispatched (running or still queued) to
         # completion instead of forcing a deadline on it.
         if self._stop_requested:
            await self._wait_with_grace(state.active_task)
         else:
            await self._await_to_completion(state.active_task)

   async def _await_to_completion(self, task):
      if task is None:
         return
      try:
         await task
      except asyncio.CancelledError:
         pass
      except Exception:
         # _execute already catches and reports every non-cancellation
         # failure itself; this is only a defensive backstop so a
         # genuinely unexpected bug in this bookkeeping cannot escape
         # as an unhandled exception out of the scheduler's own
         # shutdown path.
         pass

   async def _execute(self, target, state, deadline):
      async with self._semaphore:
         try:
            result = await self._poll_fn(target.node, target.loop)
         except asyncio.CancelledError:
            raise
         except Exception as exc:
            state.consecutive_failures += 1
            self._emit(
               type="failure", node=target.node, loop=target.loop,
               deadline=deadline, error=exc,
               consecutive_failures=state.consecutive_failures,
               breaker_state=state.breaker_state)
            return None
         else:
            recovered = state.consecutive_failures >= self._failure_threshold
            state.consecutive_failures = 0
            self._emit(
               type="success", node=target.node, loop=target.loop,
               deadline=deadline, result=result, recovered=recovered)
            return result
