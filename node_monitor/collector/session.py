"""One long-lived ssh session per node, running the streaming probe, kept alive for days."""

import asyncio
import json
import os
import random
import signal
import time
from collections.abc import Callable

# A real census line is ~1.3 MB; asyncio's default 64 KiB limit would reject it.
STREAM_LIMIT = 32 * 1024 * 1024
# stderr is read in fixed-size chunks, never readline: a newline-free flood must not
# hit the StreamReader limit or stall the pipe (and with it the remote process).
STDERR_CHUNK = 64 * 1024
STDERR_LINE_MAX_CHARS = 500
STDERR_LINES_PER_SESSION = 20
STDERR_TAIL_BYTES = 2048
MALFORMED_SAMPLE_CHARS = 200

TERM_GRACE_SEC = 5.0
KILL_WAIT_SEC = 5.0
ROTATE_EOF_WAIT_SEC = 10.0
# After stdout EOF, how long the process gets to exit before the loss counts as "eof".
EXIT_GRACE_SEC = 1.0
# Bounds the wait for stderr EOF once the process group is gone; a stray holder of the
# pipe must not hold up the reconnect.
STDERR_SETTLE_SEC = 2.0
GROUP_POLL_SEC = 0.02
# 2 ** this already exceeds any sane backoff ceiling; capping keeps the float math
# from overflowing after days of consecutive failures.
MAX_BACKOFF_DOUBLINGS = 30


class NodeSession:
   """Streams probe messages from one node, reconnecting until stop().

   Each session spawns `argv` (an ssh command running the bootstrap), writes `payload`
   to its stdin and keeps stdin open as the probe's liveness channel. Every JSON line
   goes to `on_message(label, msg)`; lifecycle and diagnostics go to
   `on_event(label, event_type, detail)`.

   `session_lost.reason` says how a session ended:

   - ``spawn_failed``: the ssh binary could not be executed (OSError on spawn).
   - ``exit``: stdout reached EOF and the process exited within a short grace;
     `exit_code` is its own status (255 is ssh failing to connect or authenticate).
   - ``eof``: stdout reached EOF but the process was still running after the grace.
   - ``silence``: no line at all for `silence_timeout` seconds.

   For ``eof`` and ``silence``, `exit_code` is the status after we signalled the
   process group. Rotation (`max_lifetime`) and stop() are not losses: rotation emits
   `session_rotated` and reconnects at once, stop() emits nothing.

   stop() must be called from the event loop's thread. A stopped session stays stopped.
   """

   def __init__(
      self,
      label: str,
      *,
      argv: list[str],
      payload: bytes,
      on_message: Callable[[str, dict], None],
      on_event: Callable[[str, str, dict], None],
      silence_timeout: float,
      max_lifetime: float,
      backoff: tuple[float, float],
      healthy_after: float = 60.0,
   ) -> None:
      self._label = label
      self._argv = list(argv)
      self._payload = payload
      self._on_message = on_message
      self._on_event = on_event
      self._silence_timeout = float(silence_timeout)
      self._max_lifetime = float(max_lifetime)
      self._backoff = (float(backoff[0]), float(backoff[1]))
      self._healthy_after = float(healthy_after)
      self._stop = asyncio.Event()
      self._proc = None
      self._boot_id = None

   @property
   def current_pid(self) -> int | None:
      """Pid of the running ssh process (also its process group id), if any."""
      return None if self._proc is None else self._proc.pid

   def stop(self) -> None:
      self._stop.set()

   async def run(self) -> None:
      attempt = 0
      failures = 0
      while not self._stop.is_set():
         attempt += 1
         outcome, uptime = await self._session(attempt)
         if uptime >= self._healthy_after:
            failures = 0
         if self._stop.is_set():
            break
         if outcome == "rotate":
            continue
         failures += 1
         base = self._backoff[0] * 2 ** min(failures - 1, MAX_BACKOFF_DOUBLINGS)
         delay = min(self._backoff[1], base) * (0.5 + random.random() / 2)
         self._event("reconnect_wait", {"delay_sec": round(delay, 3), "failures": failures})
         try:
            await asyncio.wait_for(self._stop.wait(), delay)
         except asyncio.TimeoutError:
            pass

   def _event(self, kind: str, detail: dict) -> None:
      self._on_event(self._label, kind, detail)

   async def _session(self, attempt: int) -> tuple[str, float]:
      """Run one session to its end; returns (outcome, uptime in seconds)."""
      try:
         proc = await asyncio.create_subprocess_exec(
            *self._argv,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
            limit=STREAM_LIMIT,
         )
      except OSError as exc:
         self._event(
            "session_lost",
            {
               "reason": "spawn_failed",
               "exit_code": None,
               "uptime_sec": 0.0,
               "stderr_tail": str(exc)[-STDERR_TAIL_BYTES:],
            },
         )
         return "spawn_failed", 0.0

      started = time.monotonic()
      ended = started
      self._proc = proc
      self._event("session_started", {"attempt": attempt})
      stderr_log = _StderrLog(lambda detail: self._event("probe_stderr", detail))
      drainer = asyncio.create_task(_drain(proc.stderr, stderr_log))
      feeder = asyncio.create_task(self._feed(proc.stdin))
      try:
         outcome = await self._stream(proc, started)
         ended = time.monotonic()
         if outcome in ("rotate", "stop"):
            await _settle(feeder, 0)
            proc.stdin.close()  # the probe exits on stdin EOF
            await self._pump(proc.stdout, time.monotonic() + ROTATE_EOF_WAIT_SEC, None)
         elif outcome == "eof":
            try:
               await asyncio.wait_for(proc.wait(), EXIT_GRACE_SEC)
               outcome = "exit"
            except asyncio.TimeoutError:
               pass
      finally:
         await _settle(feeder, 0)
         proc.stdin.close()
         await _end_process_group(proc)
         await _settle(drainer, STDERR_SETTLE_SEC)
         self._proc = None

      stderr_log.finish()
      uptime = ended - started
      if outcome == "rotate":
         self._event("session_rotated", {"uptime_sec": round(uptime, 3)})
      elif outcome != "stop":
         self._event(
            "session_lost",
            {
               "reason": outcome,
               "exit_code": proc.returncode,
               "uptime_sec": round(uptime, 3),
               "stderr_tail": stderr_log.tail(),
            },
         )
      return outcome, uptime

   async def _feed(self, stdin: asyncio.StreamWriter) -> None:
      try:
         stdin.write(self._payload)
         await stdin.drain()
      except OSError:
         pass  # the process is already gone; stdout EOF reports the loss

   async def _stream(self, proc, started: float) -> str:
      """Deliver lines until the session ends: eof, silence, rotate or stop."""
      reader = asyncio.create_task(
         self._pump(proc.stdout, started + self._max_lifetime, self._silence_timeout)
      )
      stopper = asyncio.create_task(self._stop.wait())
      try:
         await asyncio.wait({reader, stopper}, return_when=asyncio.FIRST_COMPLETED)
      finally:
         stopper.cancel()
         if not reader.done():
            reader.cancel()
            await asyncio.wait({reader})
      if reader.cancelled():
         return "stop"
      outcome = reader.result()
      return "rotate" if outcome == "deadline" else outcome

   async def _pump(self, stdout: asyncio.StreamReader, deadline: float, silence) -> str:
      """Deliver lines until EOF ("eof"), `deadline` ("deadline"), or a gap of more than
      `silence` seconds between lines ("silence"; None disables it)."""
      while True:
         remaining = deadline - time.monotonic()
         if remaining <= 0:
            return "deadline"
         by_deadline = silence is None or remaining < silence
         try:
            line = await asyncio.wait_for(stdout.readline(), remaining if by_deadline else silence)
         except asyncio.TimeoutError:
            return "deadline" if by_deadline else "silence"
         except ValueError:
            # Longer than STREAM_LIMIT: readline has discarded what it buffered, and the
            # rest of the line will arrive as a fragment that is reported on its own.
            self._event("malformed_line", {"bytes": STREAM_LIMIT, "sample": ""})
            continue
         if not line:
            return "eof"
         self._handle_line(line)

   def _handle_line(self, raw: bytes) -> None:
      text = raw[:-1] if raw.endswith(b"\n") else raw
      try:
         msg = json.loads(text)
      except (ValueError, RecursionError):
         msg = None
      if not isinstance(msg, dict):
         sample = text[: MALFORMED_SAMPLE_CHARS * 4].decode("utf-8", "replace")
         self._event(
            "malformed_line",
            {"bytes": len(text), "sample": sample[:MALFORMED_SAMPLE_CHARS]},
         )
         return
      if msg.get("type") == "hello":
         self._note_boot_id(msg.get("boot_id"))
      self._on_message(self._label, msg)

   def _note_boot_id(self, boot_id) -> None:
      if boot_id is None:
         return
      if self._boot_id is not None and boot_id != self._boot_id:
         self._event("node_rebooted", {"old_boot_id": self._boot_id, "new_boot_id": boot_id})
      self._boot_id = boot_id


class _StderrLog:
   """Turns stderr chunks into capped `probe_stderr` events and keeps a short tail."""

   def __init__(self, emit: Callable[[dict], None]) -> None:
      self._emit = emit
      self._line = bytearray()
      self._tail = b""
      self._emitted = 0
      self._suppressed = 0

   def feed(self, chunk: bytes) -> None:
      self._tail = (self._tail + chunk[-STDERR_TAIL_BYTES:])[-STDERR_TAIL_BYTES:]
      # Only a line's head is kept (4 bytes per char covers any UTF-8), so memory stays
      # bounded however long a line runs.
      keep = STDERR_LINE_MAX_CHARS * 4
      start = 0
      while True:
         newline = chunk.find(b"\n", start)
         end = len(chunk) if newline < 0 else newline
         room = keep - len(self._line)
         if room > 0:
            self._line += chunk[start : min(end, start + room)]
         if newline < 0:
            return
         self._finish_line()
         start = newline + 1

   def finish(self) -> None:
      """Flush a trailing partial line and report how many lines were suppressed."""
      self._finish_line()
      if self._suppressed:
         self._emit({"suppressed": self._suppressed})

   def tail(self) -> str:
      return self._tail.decode("utf-8", "replace")

   def _finish_line(self) -> None:
      text = self._line.decode("utf-8", "replace").rstrip("\r")[:STDERR_LINE_MAX_CHARS]
      self._line.clear()
      if not text.strip():
         return
      if self._emitted < STDERR_LINES_PER_SESSION:
         self._emitted += 1
         self._emit({"line": text})
      else:
         self._suppressed += 1


async def _drain(stream: asyncio.StreamReader, log: _StderrLog) -> None:
   while True:
      chunk = await stream.read(STDERR_CHUNK)
      if not chunk:
         return
      log.feed(chunk)


async def _settle(task: asyncio.Task, timeout: float) -> None:
   """Give `task` up to `timeout` seconds to finish, then cancel it."""
   if not task.done() and timeout > 0:
      await asyncio.wait({task}, timeout=timeout)
   if not task.done():
      task.cancel()
      await asyncio.wait({task})
   # Helper tasks only feed stdin or collect stderr diagnostics; a failure there must not
   # take the session down, but it is retrieved so asyncio does not log it as lost.
   if not task.cancelled():
      task.exception()


def _signal_group(pgid: int, sig: int) -> None:
   try:
      os.killpg(pgid, sig)
   except (ProcessLookupError, PermissionError):
      pass


def _group_exists(pgid: int) -> bool:
   try:
      os.killpg(pgid, 0)
   except ProcessLookupError:
      return False
   except PermissionError:
      return True
   return True


async def _group_gone(proc, timeout: float) -> bool:
   """Wait up to `timeout` for the leader to be reaped and the whole group to vanish."""
   deadline = time.monotonic() + timeout
   try:
      await asyncio.wait_for(proc.wait(), timeout)
   except asyncio.TimeoutError:
      return False
   while _group_exists(proc.pid):
      if time.monotonic() >= deadline:
         return False
      await asyncio.sleep(GROUP_POLL_SEC)
   return True


async def _end_process_group(proc) -> None:
   """SIGTERM the process group, give it TERM_GRACE_SEC, then SIGKILL; reap the leader.

   start_new_session makes the ssh child a group leader, so the group holds it and
   anything it spawned (sh, the local probe in tests, a ProxyCommand), but not a
   ControlPersist master, which ssh detaches into a session of its own.
   """
   _signal_group(proc.pid, signal.SIGTERM)
   if await _group_gone(proc, TERM_GRACE_SEC):
      return
   _signal_group(proc.pid, signal.SIGKILL)
   # A process stuck in uninterruptible sleep cannot be reaped yet; asyncio's child
   # watcher reaps it whenever it does die, and the session moves on.
   await _group_gone(proc, KILL_WAIT_SEC)
