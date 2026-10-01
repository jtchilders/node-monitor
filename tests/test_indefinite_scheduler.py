"""Regression tests for explicit indefinite scheduler semantics."""

import asyncio

import pytest

from node_monitor.collector.scheduler import Scheduler, SchedulerError, Target


def _run(coro):
   return asyncio.run(asyncio.wait_for(coro, timeout=2))


def test_none_duration_runs_until_explicit_stop():
   dispatched = []
   first_dispatch = asyncio.Event()

   async def poll_fn(node, loop):
      dispatched.append((node, loop))
      first_dispatch.set()

   scheduler = Scheduler(
      targets=[Target(node="login-04", loop="counter", interval_sec=60)],
      poll_fn=poll_fn,
      duration_sec=None,
   )

   async def scenario():
      task = asyncio.create_task(scheduler.run())
      await asyncio.wait_for(first_dispatch.wait(), timeout=1)
      assert not task.done()
      scheduler.request_stop()
      await task

   _run(scenario())

   assert dispatched == [("login-04", "counter")]
   assert scheduler.completion_reason == "partial"


@pytest.mark.parametrize("duration", [0, -1, True, False, "forever"])
def test_rejects_invalid_duration_sentinels(duration):
   with pytest.raises(SchedulerError):
      Scheduler(
         targets=[Target(node="login-04", loop="counter", interval_sec=60)],
         poll_fn=lambda node, loop: None,
         duration_sec=duration,
      )
