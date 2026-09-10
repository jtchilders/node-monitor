"""Deterministic virtual clock/sleep pair for scheduler tests.

Design: PHASE0_DAEMON_DESIGN.md "Architecture" -- "fixed monotonic
deadlines, not sleep-after-completion" -- and the Task 6 requirement to
test scheduler.py with "deterministic fake-clock tests" rather than
real wall-clock sleeps (a 24-hour duration cannot be exercised for real
in a unit test, and drift assertions need exact virtual timestamps, not
whatever jitter a real event loop happens to introduce).

FakeClock supplies both the ``clock`` and ``sleep`` callables that
``node_monitor.collector.scheduler.Scheduler`` takes as dependency
injection. A test drives virtual time forward explicitly with
``advance()``; nothing here ever touches the real wall clock.

``advance(seconds)`` must not simply set ``self._now`` to the target and
return: code under test has almost certainly not run yet by the time
``advance()`` is called (creating a task with ``asyncio.ensure_future``
only schedules its first step; it does not run it), so jumping straight
to the target time before anything has had a chance to observe
intermediate virtual instants -- or register a *new* sleep at an
earlier one -- silently corrupts every timestamp the test asserts on.
``advance()`` therefore repeatedly: (1) drains the event loop's
immediately-ready work (``_settle``) so anything that can make progress
without any virtual time passing does so first, (2) finds the single
nearest still-pending sleeper at or before the target, jumps ``_now``
to exactly that sleeper's deadline, and wakes only it, then (3) settles
again before repeating -- because waking one sleeper can itself
register a further, earlier-than-target sleeper that must be honored
before any later one. Only once no sleeper remains due at or before the
target does it jump the rest of the way and settle a final time.
"""

import asyncio


class FakeClock:
   def __init__(self, start=0.0):
      self._now = float(start)
      # Each entry is a mutable [deadline, asyncio.Event] pair so
      # advance() can flip the event in place and sleep() can remove
      # its own entry from the list once it wakes (or is cancelled).
      self._sleepers = []

   def time(self):
      return self._now

   async def sleep(self, seconds):
      if seconds <= 0:
         await asyncio.sleep(0)
         return
      deadline = self._now + seconds
      event = asyncio.Event()
      entry = [deadline, event]
      self._sleepers.append(entry)
      try:
         await event.wait()
      finally:
         if entry in self._sleepers:
            self._sleepers.remove(entry)

   async def _settle(self, rounds=50):
      """Yield control repeatedly so every coroutine that can make
      progress without any virtual time passing gets to do so before
      ``advance()`` inspects ``self._sleepers`` again.

      A fixed round count rather than a "run until quiescent" detector:
      this project's scheduler code has a small, bounded number of
      chained zero-delay awaits between any two points where it either
      blocks on a real ``sleep()`` registration or finishes, so a
      generous fixed budget is both sufficient and simple. Each round
      is a single cheap ``asyncio.sleep(0)``, so even a large budget
      costs microseconds in test wall-clock time.
      """
      for _ in range(rounds):
         await asyncio.sleep(0)

   async def advance(self, seconds):
      """Advance virtual time by exactly ``seconds``, waking sleepers
      strictly in deadline order with a full settle-pass between every
      wake (see module docstring for why).
      """
      target = self._now + seconds
      await self._settle()
      while True:
         due = [entry for entry in self._sleepers if entry[0] <= target]
         if not due:
            break
         due.sort(key=lambda entry: entry[0])
         nearest = due[0]
         self._now = nearest[0]
         nearest[1].set()
         await self._settle()
      self._now = target
      await self._settle()
