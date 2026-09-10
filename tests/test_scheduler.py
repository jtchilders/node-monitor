"""Tests for node_monitor.collector.scheduler.

Design: PHASE0_DAEMON_DESIGN.md "Architecture" + "Failure handling" and
PHASE0_DAEMON_IMPLEMENTATION_PLAN.md Task 6 -- fixed monotonic
deadlines/no drift, all-node parallel dispatch, configurable cap,
local/remote parity at scheduler-policy level, independent loops,
same-node/same-loop non-overlap (active poll terminated/reaped +
scheduler-miss record), three-consecutive-failure breaker, bounded
exponential backoff capped at 300s, success recovery, finite monotonic
duration, and graceful signal-drain policy.

Every test drives a deterministic FakeClock (tests/fixtures/fake_clock.py)
rather than the real wall clock or asyncio.sleep -- a 24-hour canary
duration and exact drift assertions cannot be exercised for real inside
a unit test.

All async behavior is exercised with plain ``asyncio.run()`` calls
inside ordinary (sync) test functions -- no pytest-asyncio plugin is
installed in this environment (matching tests/test_jsonl_output.py and
tests/test_transport.py).
"""

import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "fixtures"))

from node_monitor.collector.scheduler import (  # noqa: E402
   Scheduler,
   SchedulerError,
   Target,
)
from fake_clock import FakeClock  # noqa: E402


def _run(coro, timeout=5.0):
   return asyncio.run(asyncio.wait_for(coro, timeout=timeout))


# --------------------------------------------------------------------------
# Construction validation
# --------------------------------------------------------------------------

class TestConstruction:
   def test_rejects_empty_targets(self):
      with pytest.raises(SchedulerError):
         Scheduler(targets=[], poll_fn=lambda node, loop: None)

   def test_rejects_nonpositive_max_parallel_polls(self):
      target = Target(node="a", loop="counter", interval_sec=10)
      with pytest.raises(SchedulerError):
         Scheduler(targets=[target], poll_fn=lambda node, loop: None,
                    max_parallel_polls=0)

   def test_rejects_nonpositive_duration(self):
      target = Target(node="a", loop="counter", interval_sec=10)
      with pytest.raises(SchedulerError):
         Scheduler(targets=[target], poll_fn=lambda node, loop: None,
                    duration_sec=0)

   def test_target_rejects_nonpositive_interval(self):
      with pytest.raises(SchedulerError):
         Target(node="a", loop="counter", interval_sec=0)


# --------------------------------------------------------------------------
# Fixed monotonic deadlines / no drift
# --------------------------------------------------------------------------

class TestFixedDeadlines:
   def test_dispatches_on_fixed_grid_regardless_of_poll_duration(self):
      """A poll that takes 3 of its 10 allotted seconds must not push
      later deadlines 3 seconds later -- deadlines are computed as
      deadline + interval, never clock() + interval after completion.
      """
      clock = FakeClock()
      dispatch_times = []

      async def poll_fn(node, loop):
         dispatch_times.append(clock.time())
         await clock.sleep(3)  # slow poll: consumes 3 of its 10s budget
         return "ok"

      target = Target(node="n1", loop="counter", interval_sec=10)
      scheduler = Scheduler(
         targets=[target], poll_fn=poll_fn, clock=clock.time,
         sleep=clock.sleep, duration_sec=35)

      async def scenario():
         run_task = asyncio.ensure_future(scheduler.run())
         await clock.advance(35)
         await run_task

      _run(scenario())

      # Deadlines at 0, 10, 20, 30 -- exactly on the fixed grid, with
      # zero cumulative drift from the 3-second poll duration.
      assert dispatch_times == [0.0, 10.0, 20.0, 30.0]

   def test_no_cumulative_drift_over_many_cycles(self):
      clock = FakeClock()
      dispatch_times = []

      async def poll_fn(node, loop):
         dispatch_times.append(clock.time())
         return "ok"

      target = Target(node="n1", loop="counter", interval_sec=5)
      scheduler = Scheduler(
         targets=[target], poll_fn=poll_fn, clock=clock.time,
         sleep=clock.sleep, duration_sec=50)

      async def scenario():
         run_task = asyncio.ensure_future(scheduler.run())
         await clock.advance(50)
         await run_task

      _run(scenario())

      assert dispatch_times == [float(i * 5) for i in range(10)]


# --------------------------------------------------------------------------
# All-node parallel dispatch / configurable cap
# --------------------------------------------------------------------------

class TestParallelDispatchAndCap:
   def test_all_nodes_dispatch_at_the_same_deadline(self):
      clock = FakeClock()
      dispatched = []

      async def poll_fn(node, loop):
         dispatched.append((clock.time(), node))
         return "ok"

      targets = [
         Target(node="a", loop="counter", interval_sec=10),
         Target(node="b", loop="counter", interval_sec=10),
         Target(node="c", loop="counter", interval_sec=10),
      ]
      scheduler = Scheduler(
         targets=targets, poll_fn=poll_fn, clock=clock.time,
         sleep=clock.sleep, duration_sec=15, max_parallel_polls=8)

      async def scenario():
         run_task = asyncio.ensure_future(scheduler.run())
         await clock.advance(15)
         await run_task

      _run(scenario())

      first_round = sorted(node for time_, node in dispatched if time_ == 0.0)
      assert first_round == ["a", "b", "c"]

   def test_configurable_cap_limits_concurrent_polls(self):
      clock = FakeClock()
      concurrent = 0
      max_observed = 0

      async def poll_fn(node, loop):
         nonlocal concurrent, max_observed
         concurrent += 1
         max_observed = max(max_observed, concurrent)
         await clock.sleep(1)
         concurrent -= 1
         return "ok"

      targets = [
         Target(node="n%d" % i, loop="counter", interval_sec=100)
         for i in range(5)
      ]
      scheduler = Scheduler(
         targets=targets, poll_fn=poll_fn, clock=clock.time,
         sleep=clock.sleep, duration_sec=5, max_parallel_polls=2)

      async def scenario():
         run_task = asyncio.ensure_future(scheduler.run())
         await clock.advance(5)
         await run_task

      _run(scenario())

      assert max_observed <= 2
      assert max_observed == 2


# --------------------------------------------------------------------------
# Local/remote parity, independent loops
# --------------------------------------------------------------------------

class TestParityAndIndependentLoops:
   def test_scheduler_does_not_branch_on_node_role(self):
      """The scheduler must treat every Target identically regardless
      of what the caller's node object represents -- this module has
      no notion of "local" vs "remote" at all.
      """
      clock = FakeClock()
      dispatched_nodes = []

      async def poll_fn(node, loop):
         dispatched_nodes.append(node)
         return "ok"

      targets = [
         Target(node={"hostname": "polaris-login-04", "role": "local"},
                loop="counter", interval_sec=10),
         Target(node={"hostname": "polaris-login-01.head", "role": "remote"},
                loop="counter", interval_sec=10),
      ]
      scheduler = Scheduler(
         targets=targets, poll_fn=poll_fn, clock=clock.time,
         sleep=clock.sleep, duration_sec=5)

      async def scenario():
         run_task = asyncio.ensure_future(scheduler.run())
         await clock.advance(5)
         await run_task

      _run(scenario())

      roles = sorted(node["role"] for node in dispatched_nodes)
      assert roles == ["local", "remote"]

   def test_counter_and_census_loops_are_independent(self):
      clock = FakeClock()
      dispatched = []

      async def poll_fn(node, loop):
         dispatched.append((clock.time(), loop))
         return "ok"

      targets = [
         Target(node="n1", loop="counter", interval_sec=10),
         Target(node="n1", loop="census", interval_sec=60),
      ]
      scheduler = Scheduler(
         targets=targets, poll_fn=poll_fn, clock=clock.time,
         sleep=clock.sleep, duration_sec=65)

      async def scenario():
         run_task = asyncio.ensure_future(scheduler.run())
         await clock.advance(65)
         await run_task

      _run(scenario())

      counter_times = [t for t, loop in dispatched if loop == "counter"]
      census_times = [t for t, loop in dispatched if loop == "census"]
      assert counter_times == [0.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0]
      assert census_times == [0.0, 60.0]


# --------------------------------------------------------------------------
# Same-node/same-loop non-overlap
# --------------------------------------------------------------------------

class TestNonOverlap:
   def test_overlapping_poll_is_terminated_reaped_and_recorded(self):
      """A poll_fn that never completes on its own -- e.g. hung SSH --
      must never delay or skip its target's OWN next deadline: the
      still-active previous attempt is cancelled/reaped and a
      scheduler_miss recorded before the new attempt starts.

      Ends the scenario with request_stop() (a real signal-driven
      shutdown), not by letting duration_sec elapse naturally: since
      this poll_fn never completes by construction, a duration-end
      exit would legitimately wait for it forever (in production a
      poll's own transport-level hard timeout would always eventually
      resolve it -- that guarantee belongs to collector/transport.py,
      not this module, so a scheduler unit test must not fabricate a
      poll that violates it and then expect a bounded natural exit).
      """
      clock = FakeClock()
      cancelled = []
      events = []

      async def poll_fn(node, loop):
         try:
            await clock.sleep(1000)
         except asyncio.CancelledError:
            cancelled.append(clock.time())
            raise

      target = Target(node="n1", loop="counter", interval_sec=10)
      scheduler = Scheduler(
         targets=[target], poll_fn=poll_fn, clock=clock.time,
         sleep=clock.sleep, duration_sec=100000, on_event=events.append)

      async def scenario():
         run_task = asyncio.ensure_future(scheduler.run())
         await clock.advance(25)  # dispatch at 0, miss+dispatch at 10, 20
         scheduler.request_stop()
         await clock.advance(10)  # let the bounded grace period elapse
         await run_task

      _run(scenario())

      # The still-running poll from deadline 0 is cancelled at (at
      # least) deadline 10, and again a still-running poll is
      # cancelled at deadline 20.
      assert len(cancelled) >= 2
      misses = [e for e in events if e["type"] == "scheduler_miss"]
      assert len(misses) >= 2
      assert misses[0]["node"] == "n1"
      assert misses[0]["loop"] == "counter"

   def test_non_overlapping_poll_never_produces_a_miss(self):
      clock = FakeClock()
      events = []

      async def poll_fn(node, loop):
         await clock.sleep(1)  # comfortably within the 10s interval
         return "ok"

      target = Target(node="n1", loop="counter", interval_sec=10)
      scheduler = Scheduler(
         targets=[target], poll_fn=poll_fn, clock=clock.time,
         sleep=clock.sleep, duration_sec=35, on_event=events.append)

      async def scenario():
         run_task = asyncio.ensure_future(scheduler.run())
         await clock.advance(35)
         await run_task

      _run(scenario())

      misses = [e for e in events if e["type"] == "scheduler_miss"]
      assert misses == []

   def test_three_consecutive_misses_open_the_breaker_and_apply_backoff(self):
      """Design: an active poll cancelled/reaped at its own next
      deadline is 'recorded as a scheduler failure' -- three
      consecutive scheduler_miss events must open the same breaker a
      poll_fn exception would, and the next gap must reflect backoff,
      not the normal interval. Regression for review round 1 finding
      2: a scheduler_miss did not increment consecutive_failures, so
      repeated overruns never opened/backed off the target breaker.
      """
      clock = FakeClock()
      events = []

      async def poll_fn(node, loop):
         # Never completes on its own -- every dispatch is reaped as
         # a miss by the following deadline.
         await clock.sleep(1000)

      target = Target(node="n1", loop="counter", interval_sec=10)
      scheduler = Scheduler(
         targets=[target], poll_fn=poll_fn, clock=clock.time,
         sleep=clock.sleep, duration_sec=200, on_event=events.append)

      async def scenario():
         run_task = asyncio.ensure_future(scheduler.run())
         # Dispatch at 0; misses (cancel+reap the prior attempt) at
         # 10, 20, 30 -- three consecutive misses by deadline 30 --
         # then continue far enough to observe the post-breaker gap.
         await clock.advance(120)
         scheduler.request_stop()
         await clock.advance(10)
         await run_task

      _run(scenario())

      misses = [e for e in events if e["type"] == "scheduler_miss"]
      assert len(misses) >= 3
      miss_deadlines = [m["deadline"] for m in misses]
      assert miss_deadlines[:3] == [10.0, 20.0, 30.0]
      # After the third consecutive miss (recorded at deadline 30),
      # the breaker must be open: the next dispatch is delayed by
      # backoff (interval * 2 = 20s), landing at 50, not the normal
      # 40 -- so the 4th miss (the dispatch that starts at the new
      # deadline gets reaped in turn) must be recorded at 50.
      assert len(misses) >= 4
      assert misses[3]["deadline"] == 50.0


# --------------------------------------------------------------------------
# Breaker: three-consecutive-failure threshold, backoff, recovery
# --------------------------------------------------------------------------

class TestBreaker:
   def test_breaker_opens_after_three_consecutive_failures(self):
      clock = FakeClock()
      events = []

      async def poll_fn(node, loop):
         raise RuntimeError("boom")

      target = Target(node="n1", loop="counter", interval_sec=10)
      scheduler = Scheduler(
         targets=[target], poll_fn=poll_fn, clock=clock.time,
         sleep=clock.sleep, duration_sec=35, on_event=events.append)

      async def scenario():
         run_task = asyncio.ensure_future(scheduler.run())
         await clock.advance(35)
         await run_task

      _run(scenario())

      failures = [e for e in events if e["type"] == "failure"]
      assert len(failures) >= 3
      assert failures[0]["breaker_state"] == "closed"
      assert failures[1]["breaker_state"] == "closed"
      assert failures[2]["breaker_state"] == "open"

   def test_backoff_is_exponential_and_capped_at_300_seconds(self):
      clock = FakeClock()
      dispatch_times = []

      async def poll_fn(node, loop):
         dispatch_times.append(clock.time())
         raise RuntimeError("boom")

      target = Target(node="n1", loop="counter", interval_sec=10)
      scheduler = Scheduler(
         targets=[target], poll_fn=poll_fn, clock=clock.time,
         sleep=clock.sleep, duration_sec=2000, backoff_cap_sec=300)

      async def scenario():
         run_task = asyncio.ensure_future(scheduler.run())
         await clock.advance(2000)
         await run_task

      _run(scenario())

      gaps = [b - a for a, b in zip(dispatch_times, dispatch_times[1:])]
      # First two gaps are the normal 10s interval (failures 1 and 2,
      # breaker still closed); from the third failure on, backoff
      # applies and grows: 20, 40, 80, 160, then capped at 300.
      assert gaps[0] == 10.0
      assert gaps[1] == 10.0
      assert gaps[2] == 20.0
      assert gaps[3] == 40.0
      assert gaps[4] == 80.0
      assert gaps[5] == 160.0
      assert gaps[6] == 300.0
      assert gaps[7] == 300.0
      assert max(gaps) <= 300.0

   def test_backoff_applies_to_the_dispatch_right_after_an_asynchronous_third_failure(self):
      """Regression for review round 1 finding 1: a poll_fn that
      genuinely suspends (awaits something) before failing must still
      have its failure incorporated before the scheduler computes the
      NEXT gap -- backoff must start immediately after the third
      completed failure, not one full normal-interval cycle later.

      Reproduction from the review: interval=0.05, poll_fn awaits
      0.01s then raises. Failures land at ~0, .05, .10 (all still
      inside their own interval, since 0.01 < 0.05); the dispatch
      immediately following the third failure must reflect backoff
      (interval * 2 = 0.10s), landing at ~0.20, not ~0.15.
      """
      clock = FakeClock()
      dispatch_times = []

      async def poll_fn(node, loop):
         dispatch_times.append(clock.time())
         await clock.sleep(0.01)
         raise RuntimeError("boom")

      target = Target(node="n1", loop="counter", interval_sec=0.05)
      scheduler = Scheduler(
         targets=[target], poll_fn=poll_fn, clock=clock.time,
         sleep=clock.sleep, duration_sec=1.0)

      async def scenario():
         run_task = asyncio.ensure_future(scheduler.run())
         await clock.advance(1.0)
         await run_task

      _run(scenario())

      # First three dispatches happen on the normal 0.05s grid
      # (failures 1, 2, 3 -- breaker opens on the third). The fourth
      # dispatch, immediately following the third failure, must
      # already reflect backoff: gap of 0.10s from the third
      # dispatch, not the normal 0.05s.
      assert dispatch_times[0] == pytest.approx(0.0)
      assert dispatch_times[1] == pytest.approx(0.05)
      assert dispatch_times[2] == pytest.approx(0.10)
      assert dispatch_times[3] == pytest.approx(0.20)

   def test_success_resets_breaker_and_backoff(self):
      clock = FakeClock()
      dispatch_times = []
      call_count = 0

      async def poll_fn(node, loop):
         nonlocal call_count
         call_count += 1
         dispatch_times.append(clock.time())
         if call_count <= 3:
            raise RuntimeError("boom")
         return "ok"

      target = Target(node="n1", loop="counter", interval_sec=10)
      scheduler = Scheduler(
         targets=[target], poll_fn=poll_fn, clock=clock.time,
         sleep=clock.sleep, duration_sec=100)

      async def scenario():
         run_task = asyncio.ensure_future(scheduler.run())
         await clock.advance(100)
         await run_task

      _run(scenario())

      # Failures at 0, 10, 20 (3rd failure opens the breaker and would
      # normally schedule a 20s backoff next); the 4th call at 40
      # succeeds, so the 5th call must return to the normal 10s
      # interval rather than continuing to grow the backoff.
      assert dispatch_times[3] == 40.0
      gap_after_recovery = dispatch_times[4] - dispatch_times[3]
      assert gap_after_recovery == 10.0

   def test_success_event_marks_recovered_after_open_breaker(self):
      clock = FakeClock()
      events = []
      call_count = 0

      async def poll_fn(node, loop):
         nonlocal call_count
         call_count += 1
         if call_count <= 3:
            raise RuntimeError("boom")
         return "ok"

      target = Target(node="n1", loop="counter", interval_sec=10)
      scheduler = Scheduler(
         targets=[target], poll_fn=poll_fn, clock=clock.time,
         sleep=clock.sleep, duration_sec=100, on_event=events.append)

      async def scenario():
         run_task = asyncio.ensure_future(scheduler.run())
         await clock.advance(100)
         await run_task

      _run(scenario())

      successes = [e for e in events if e["type"] == "success"]
      assert successes[0]["recovered"] is True


# --------------------------------------------------------------------------
# Finite monotonic duration
# --------------------------------------------------------------------------

class TestFiniteDuration:
   def test_run_returns_once_duration_elapses(self):
      clock = FakeClock()
      dispatch_times = []

      async def poll_fn(node, loop):
         dispatch_times.append(clock.time())
         return "ok"

      target = Target(node="n1", loop="counter", interval_sec=10)
      scheduler = Scheduler(
         targets=[target], poll_fn=poll_fn, clock=clock.time,
         sleep=clock.sleep, duration_sec=25)

      async def scenario():
         run_task = asyncio.ensure_future(scheduler.run())
         await clock.advance(1000)  # far past duration_sec
         await run_task

      _run(scenario())

      # duration_sec=25 with a 10s interval: deadlines at 0, 10, 20
      # all land strictly before 25; the would-be deadline at 30 does
      # not, so dispatch stops at exactly 3 calls.
      assert dispatch_times == [0.0, 10.0, 20.0]


# --------------------------------------------------------------------------
# Graceful signal-drain policy
# --------------------------------------------------------------------------

class TestGracefulDrain:
   def test_request_stop_prevents_further_dispatch(self):
      clock = FakeClock()
      dispatch_times = []

      async def poll_fn(node, loop):
         dispatch_times.append(clock.time())
         return "ok"

      target = Target(node="n1", loop="counter", interval_sec=10)
      scheduler = Scheduler(
         targets=[target], poll_fn=poll_fn, clock=clock.time,
         sleep=clock.sleep, duration_sec=1000)

      async def scenario():
         run_task = asyncio.ensure_future(scheduler.run())
         await clock.advance(25)  # dispatches at 0, 10, 20
         scheduler.request_stop()
         await clock.advance(100)
         await run_task

      _run(scenario())

      assert dispatch_times == [0.0, 10.0, 20.0]

   def test_active_poll_gets_bounded_grace_period_then_is_reaped(self):
      clock = FakeClock()
      cancelled_at = []
      finished_cleanly = []

      async def poll_fn(node, loop):
         try:
            await clock.sleep(1000)
            finished_cleanly.append(True)
         except asyncio.CancelledError:
            cancelled_at.append(clock.time())
            raise

      target = Target(node="n1", loop="counter", interval_sec=10)
      scheduler = Scheduler(
         targets=[target], poll_fn=poll_fn, clock=clock.time,
         sleep=clock.sleep, duration_sec=1000, grace_sec=2)

      async def scenario():
         run_task = asyncio.ensure_future(scheduler.run())
         await clock.advance(0)  # let the deadline-0 poll dispatch
         scheduler.request_stop()
         await clock.advance(10)
         await run_task

      _run(scenario())

      # The active poll was given its grace_sec=2 window (virtual
      # time actually advanced by it) and, having not finished on its
      # own by then, was cancelled -- not left running/orphaned.
      assert cancelled_at == [2.0]
      assert finished_cleanly == []

   def test_grace_period_lets_a_fast_finishing_poll_complete_cleanly(self):
      clock = FakeClock()
      finished_cleanly = []

      async def poll_fn(node, loop):
         await clock.sleep(1)  # finishes well inside grace_sec
         finished_cleanly.append(clock.time())
         return "ok"

      target = Target(node="n1", loop="counter", interval_sec=10)
      scheduler = Scheduler(
         targets=[target], poll_fn=poll_fn, clock=clock.time,
         sleep=clock.sleep, duration_sec=1000, grace_sec=5)

      async def scenario():
         run_task = asyncio.ensure_future(scheduler.run())
         await clock.advance(0)
         scheduler.request_stop()
         await clock.advance(5)
         await run_task

      _run(scenario())

      assert finished_cleanly == [1.0]
