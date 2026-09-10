"""node_monitor.collector.transport -- probe invocation, local or over SSH.

Design: PHASE0_DAEMON_DESIGN.md "Architecture". The local node invokes the
configured Python interpreter directly, passing the probe script by path.
Every other configured node invokes the exact same probe delivered over
SSH stdin (`ssh <node> <probe_python> - < remote_probe.py`), using a
project-owned SSH config/control directory -- never the operator's
interactive control socket (PLANNING.md 4.3: "it must not touch
~/.ssh/config") -- with BatchMode=yes so any prompt fails immediately
instead of hanging a daemon with no terminal. Local and remote child
environments both strip LD_PRELOAD (PLANNING.md 4.5: XALT's LD_PRELOAD
segfaults an `exec postgres` in this project's process tree; the remote
side additionally uses `env -u LD_PRELOAD` because the login node's own
module environment would otherwise reintroduce it).

Every subprocess starts its own process session (`start_new_session`) so
a hard timeout can SIGTERM/SIGKILL the *whole* process group and reap it
-- design acceptance criterion "no orphan child after shutdown". Nothing
here invokes a shell: every command is an argv list handed straight to
``create_subprocess_exec``.

This module knows nothing about scheduling, breakers, or which node is
"next due" -- that is Task 6 (collector/scheduler.py). It is a pure
mechanism: given a fully-resolved command, run it, enforce its timeout,
and turn the result into either a validated ``ProbeResult`` or one of the
typed exceptions below.
"""

import asyncio
import dataclasses
import json
import os
import signal
import time


# --------------------------------------------------------------------------
# Failure taxonomy
#
# One exception subclass per node_poll_failures.failure_type (see
# node_monitor.output.contracts._POLL_FAILURE_TYPES) that this module can
# itself detect, so a caller builds a poll-failure record straight from
# ``exc.failure_type`` with no separate translation table. reboot/reset
# are scheduler/metrics-layer concepts (they compare *consecutive*
# samples) and scheduler_miss is the scheduler's own bookkeeping -- none
# of the three belong here.
# --------------------------------------------------------------------------

class TransportError(Exception):
   """Base class for every transport-layer failure."""

   failure_type = "invariant_violation"


class ProbeTimeoutError(TransportError):
   """The hard subprocess deadline elapsed before the probe finished."""

   failure_type = "timeout"


class SSHAuthError(TransportError):
   """ssh itself failed (exit 255) for an authentication reason."""

   failure_type = "ssh_auth"


class SSHTransportError(TransportError):
   """ssh itself failed (exit 255) for a non-authentication reason."""

   failure_type = "ssh_transport"


class ProbeExitError(TransportError):
   """The probe (local interpreter, or the remote command ssh passed
   through) exited nonzero on its own account -- not an ssh-level
   failure. Carries the exit code and captured stderr so a caller can
   build a structured node_poll_failures ``detail`` without re-parsing
   this exception's message string.
   """

   failure_type = "probe_exit"

   def __init__(self, message, exit_code, stderr):
      super().__init__(message)
      self.exit_code = exit_code
      self.stderr = stderr


class MalformedJSONError(TransportError):
   """The probe exited 0 but stdout was not one valid JSON object."""

   failure_type = "malformed_json"


class ProbeVersionMismatchError(TransportError):
   """The payload's probe_version did not match what the caller expected."""

   failure_type = "probe_version_mismatch"


class HostnameMismatchError(TransportError):
   """The payload's remote-reported FQDN did not match what the caller
   expected -- design: "SSH aliases are transport identifiers only,
   never provenance."
   """

   failure_type = "hostname_mismatch"


class InvariantViolationError(TransportError):
   """A self-consistency check failed that has no more specific failure
   type of its own (e.g. the probe reported a different ``loop`` than
   the one this call requested).
   """

   failure_type = "invariant_violation"


class ProcessGroupCleanupError(TransportError):
   """`_terminate_process_group` could not confirm its owned process
   group was gone even after bounded SIGKILL escalation.

   SIGKILL cannot be ignored by a process in a normal runnable state,
   so exhausting several confirmed escalation attempts means something
   is fundamentally wrong (e.g. a descendant stuck in uninterruptible
   I/O). There is no more specific failure type for this in the
   design's taxonomy, so it shares ``invariant_violation`` -- this is
   deliberately NOT swallowed into a quiet return, since doing so would
   let a caller believe cleanup succeeded (no orphan) when it did not
   (review round 5).
   """

   failure_type = "invariant_violation"


# --------------------------------------------------------------------------
# Result
# --------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class ProbeResult:
   """One successfully validated probe invocation.

   ``stdout_bytes`` and ``wall_seconds`` exist so the daemon can populate
   the design's "probe wall time ... and payload size" observer-effect
   fields without recomputing them from the payload itself.
   """

   payload: dict
   exit_code: int
   stdout_bytes: int
   stderr: str
   stderr_truncated: bool
   wall_seconds: float


@dataclasses.dataclass(frozen=True)
class _RawResult:
   """Internal: one completed subprocess, before JSON parsing/validation."""

   exit_code: int
   stdout: bytes
   stderr: bytes
   stderr_truncated: bool
   wall_seconds: float


# --------------------------------------------------------------------------
# argv / ssh config construction -- pure functions, independently testable
# --------------------------------------------------------------------------

def _format_seconds(value):
   """Render a positive number the way the probe's hand-rolled argv
   parser and a human reading a process list both expect: an integer
   value renders without a spurious ``.0``.
   """
   if float(value) == int(value):
      return str(int(value))
   return str(value)


def build_local_argv(probe_python, probe_script_path, loop, max_seconds):
   """Exact argv for a local invocation: the configured interpreter runs
   the probe script by path -- no shell, no stdin delivery, because
   there is no network hop for the local node to avoid.
   """
   if not probe_python:
      raise ValueError("probe_python is required")
   if not probe_script_path:
      raise ValueError("probe_script_path is required")
   return [
      probe_python, probe_script_path,
      "--loop", loop,
      "--max-seconds", _format_seconds(max_seconds),
   ]


def build_remote_argv(ssh_binary, ssh_config_path, connect_timeout_sec,
                       hostname, probe_python, loop, max_seconds):
   """Exact argv for a remote invocation.

   ``-F ssh_config_path`` pins the project-owned config (never the
   operator's ``~/.ssh/config``); ``-o BatchMode=yes`` is passed
   explicitly on the command line, not left to the config file alone,
   so a call site is safe even if the config file were ever wrong or
   missing. The remote command is ``env -u LD_PRELOAD <probe_python> -
   ...``: ``-`` tells the remote interpreter to read the script from
   stdin, which is how the probe source is delivered without ever being
   staged to the target's filesystem.
   """
   if not ssh_config_path:
      raise ValueError(
         "ssh_config_path is required -- never fall back to the "
         "operator's interactive ~/.ssh/config")
   if not probe_python:
      raise ValueError("probe_python is required")
   if not hostname:
      raise ValueError("hostname is required")
   return [
      ssh_binary,
      "-F", ssh_config_path,
      "-o", "BatchMode=yes",
      "-o", "ConnectTimeout=%s" % _format_seconds(connect_timeout_sec),
      hostname,
      "env", "-u", "LD_PRELOAD",
      probe_python, "-",
      "--loop", loop,
      "--max-seconds", _format_seconds(max_seconds),
   ]


_SSH_CONFIG_TEMPLATE = (
   "# Generated by node_monitor.collector.transport.write_ssh_config.\n"
   "# Project-owned: never the operator's interactive ~/.ssh/config or\n"
   "# control socket (PLANNING.md 4.3).\n"
   "Host *\n"
   "   BatchMode yes\n"
   "   ControlMaster auto\n"
   "   ControlPersist 60s\n"
   "   ControlPath %(control_path)s\n"
   "   ConnectTimeout %(connect_timeout)s\n"
)


def write_ssh_config(config_path, control_dir, connect_timeout_sec):
   """Write a project-owned ssh_config: BatchMode yes and a multiplexed
   ControlMaster/ControlPersist socket rooted at ``control_dir``.

   Design: "The SSH command uses a project-owned config/control
   directory and BatchMode=yes, never Taylor's interactive control
   socket." Both the directory and the file are created mode-0700/0600
   so the control socket path is not world-writable.
   """
   os.makedirs(control_dir, mode=0o700, exist_ok=True)
   os.chmod(control_dir, 0o700)
   control_path = os.path.join(control_dir, "%r@%h:%p")
   content = _SSH_CONFIG_TEMPLATE % {
      "control_path": control_path,
      "connect_timeout": _format_seconds(connect_timeout_sec),
   }
   fd = os.open(config_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
   try:
      with os.fdopen(fd, "w") as handle:
         handle.write(content)
   except BaseException:
      os.close(fd)
      raise
   os.chmod(config_path, 0o600)
   return config_path


# --------------------------------------------------------------------------
# Payload validation
# --------------------------------------------------------------------------

def validate_probe_payload(payload, expected_loop, expected_probe_version,
                            expected_fqdn=None):
   """Enforce design: "Each successful payload must have the expected
   loop and probe version and a remote-reported FQDN unique among
   configured targets." Uniqueness *among* targets is a scheduler-level
   concern (it needs every node's payload at once); this function
   enforces the per-call half -- the payload matches what THIS call
   asked for and, if the caller already knows which host it expects,
   that the remote-reported FQDN agrees.
   """
   if not isinstance(payload, dict):
      raise InvariantViolationError(
         "probe payload must be a JSON object, got %s"
         % type(payload).__name__)
   if payload.get("probe_version") != expected_probe_version:
      raise ProbeVersionMismatchError(
         "expected probe_version %r, got %r"
         % (expected_probe_version, payload.get("probe_version")))
   if payload.get("loop") != expected_loop:
      raise InvariantViolationError(
         "requested loop %r but probe reported loop %r"
         % (expected_loop, payload.get("loop")))
   fqdn = payload.get("hostname_fqdn")
   if not isinstance(fqdn, str) or not fqdn:
      raise InvariantViolationError(
         "probe payload must carry a non-empty string hostname_fqdn, "
         "got %r" % (fqdn,))
   if expected_fqdn is not None and fqdn != expected_fqdn:
      raise HostnameMismatchError(
         "expected FQDN %r, got %r"
         % (expected_fqdn, fqdn))
   return payload


# --------------------------------------------------------------------------
# Subprocess mechanics
# --------------------------------------------------------------------------

_DEFAULT_STDERR_LIMIT_BYTES = 8192
_DEFAULT_GRACE_SEC = 2.0
_READ_CHUNK_BYTES = 65536

# ssh(1) itself exits 255 when SSH FAILS -- auth, network, "no route to
# host", closed connection -- as opposed to passing through the REMOTE
# COMMAND's own exit status. A remote command that legitimately exits
# 255 is indistinguishable from an ssh-level failure by exit code alone,
# but that ambiguity is inherent to ssh's contract, not something this
# module can resolve; the design's own probe exit codes (0/2/3/4) never
# use 255, so in practice this collision does not occur.
_SSH_FAILURE_EXIT_CODE = 255

_SSH_AUTH_MARKERS = (
   "permission denied",
   "authentication failed",
   "no matching key exchange",
   "publickey",
)


def _classify_ssh_failure(stderr_text):
   lowered = stderr_text.lower()
   if any(marker in lowered for marker in _SSH_AUTH_MARKERS):
      return SSHAuthError
   return SSHTransportError


def _strip_ld_preload(base_env):
   """Local child environments remove LD_PRELOAD (design: "Local and
   remote child environments remove LD_PRELOAD"). Applies to the local
   probe interpreter AND to the local ssh client process -- the remote
   side additionally gets an explicit ``env -u LD_PRELOAD`` in its argv
   (build_remote_argv) because the login node's own module environment
   can reintroduce it independently of what the daemon's own env holds.
   """
   env = dict(base_env if base_env is not None else os.environ)
   env.pop("LD_PRELOAD", None)
   return env


async def _drain_bounded(stream, limit):
   """Read `stream` to EOF, keeping only the first `limit` bytes.

   Design: "bounded stderr". A misbehaving probe or remote shell can
   write unbounded stderr; this must not grow the daemon's memory
   without bound, and it must not deadlock a well-behaved probe by
   leaving stderr unread while a full pipe blocks the child -- so
   reading continues (and simply discards) past the limit rather than
   stopping.
   """
   data = bytearray()
   truncated = False
   if stream is None:
      return b"", False
   while True:
      chunk = await stream.read(_READ_CHUNK_BYTES)
      if not chunk:
         break
      if len(data) < limit:
         take = limit - len(data)
         data.extend(chunk[:take])
         if len(chunk) > take:
            truncated = True
      else:
         truncated = True
   return bytes(data), truncated


async def _read_all(stream):
   if stream is None:
      return b""
   chunks = []
   while True:
      chunk = await stream.read(_READ_CHUNK_BYTES)
      if not chunk:
         break
      chunks.append(chunk)
   return b"".join(chunks)


def _process_group_alive(pgid):
   """True if any process still belongs to `pgid` AND it is still ours
   to signal.

   A process group is not the same thing as its (former) leader: once
   at least one member remains, `killpg(pgid, 0)` keeps succeeding even
   after the leader that gave the group its pgid has exited and been
   reaped. That is exactly the state a SIGTERM-ignoring descendant
   leaves behind, so this is the only reliable way to tell "the group
   is gone" from "the leader is gone".

   `PermissionError` (EPERM, distinct from ESRCH/ProcessLookupError)
   means the OS has recycled `pgid` onto an unrelated process group
   this caller no longer has permission to signal -- observed in
   practice under rapid repeated process creation, where a pgid a
   moment ago was our own child can be reassigned to a different
   process owned by another user by the time this check runs. That
   process is not our orphan; further escalation against `pgid` would
   be signalling someone else's process group, so treat it the same as
   "gone" for our own cleanup's purposes and stop escalating.
   """
   try:
      os.killpg(pgid, 0)
   except (ProcessLookupError, PermissionError):
      return False
   return True


_GROUP_GONE_STABILITY_CHECKS = 3
_GROUP_GONE_POLL_SEC = 0.01


async def _wait_group_confirmed_gone(pgid, deadline):
   """Poll `_process_group_alive(pgid)` until it reports False on
   `_GROUP_GONE_STABILITY_CHECKS` consecutive checks, or `deadline`
   (a `time.monotonic()` value) passes.

   A single `killpg(pgid, 0)` success/failure is not by itself
   sufficient proof of the group's true state: `killpg` reports "no
   such process group" (`ESRCH`) as soon as the LAST member the kernel
   still associates with `pgid` has been signal-delivered and is
   exiting, which can be a brief window before `os.kill(that pid, 0)`
   -- checking the actual descendant PID directly -- also starts
   raising `ProcessLookupError`. Requiring several consecutive "gone"
   readings, each separated by a short real sleep, closes that window:
   a group that is genuinely gone stays reporting gone across every
   recheck, while a group observed "gone" only by a single racy read
   flips back to "alive" on the very next check because the kernel has
   not actually finished tearing it down yet. Returns True once the
   group is confirmed gone by `_GROUP_GONE_STABILITY_CHECKS` actual
   consecutive "gone" reads; returns False the instant `deadline`
   passes, with NO extra read of any kind -- review round 5 found this
   returning the result of one final, uncounted `_process_group_alive`
   read at the deadline, which lets a single racy "gone" observation
   (not yet corroborated by the required run of consecutive reads)
   pass as confirmation. The deadline check happens before the poll
   sleep specifically so a deadline that has already elapsed by the
   time this coroutine is first scheduled still reports False rather
   than silently performing one bonus read.
   """
   consecutive_gone = 0
   while True:
      if time.monotonic() >= deadline:
         return False
      if _process_group_alive(pgid):
         consecutive_gone = 0
      else:
         consecutive_gone += 1
         if consecutive_gone >= _GROUP_GONE_STABILITY_CHECKS:
            return True
      await asyncio.sleep(_GROUP_GONE_POLL_SEC)


async def _close_subprocess_transport(proc):
   """Drain `proc`'s stdout/stderr pipes to EOF and close its
   subprocess transport deterministically, instead of leaving that to
   `BaseSubprocessTransport.__del__` whenever the garbage collector
   happens to run it.

   `asyncio.create_subprocess_exec`'s pipe transports hold OS file
   descriptors and, once the child has exited, an internal reference
   to the event loop they were created on. If nothing reads a
   partially-buffered pipe to EOF and closes the transport before that
   loop is closed (every call in this module runs under its own
   `asyncio.run()`), `__del__` running later -- on a different loop,
   or after the process has already exited -- raises `RuntimeError:
   Event loop is closed` from inside `call_soon`, which asyncio can
   only report as an unraisable exception. Under repeated timeout/
   cleanup cycles in the same process (e.g. a long-running collector
   polling many nodes) this surfaces as intermittent "Event loop is
   closed" noise with no failed collection to explain it. The subgroup
   has already been SIGKILLed and reaped by the time this runs, so
   these reads return promptly.
   """
   for stream in (proc.stdout, proc.stderr):
      if stream is None:
         continue
      try:
         await stream.read()
      except (BrokenPipeError, ConnectionResetError, OSError):
         pass
   transport = getattr(proc, "_transport", None)
   if transport is not None:
      transport.close()


async def _terminate_process_group(proc, grace_sec):
   """SIGTERM the whole process group started with start_new_session,
   give it `grace_sec` to exit, SIGKILL if it has not, then reap.

   Design: "a poll still active at its next deadline is terminated,
   reaped, and recorded as a scheduler failure"; acceptance criterion:
   "no orphan child after shutdown." Signalling the GROUP (not just
   `proc.pid`) is what makes this safe against a probe or ssh session
   that has itself forked -- killing only the direct child would leave
   any grandchild (e.g. a hung nvidia-smi, or a remote shell's own
   children on the daemon-host end of the ssh pipe) running unreaped.

   Reaping the direct child (`proc.wait()`) is NOT sufficient proof the
   group is gone: a descendant that ignores SIGTERM keeps the process
   group alive even after the leader we spawned has exited. So after
   both the initial grace-period wait and any SIGKILL escalation, this
   explicitly re-checks the whole group via `_wait_group_confirmed_gone`
   and keeps escalating to SIGKILL until the group is CONFIRMED gone
   (several consecutive "gone" reads, not a single racy one) or the
   grace budget runs out -- never treating "our direct child exited",
   or a single `killpg` failure, as "the group is gone". If SIGKILL
   escalation still cannot confirm the group is gone before its own
   deadline, this raises `ProcessGroupCleanupError` rather than
   returning normally (review round 5: silently returning here let a
   caller believe the no-orphan postcondition held when it did not).
   Finally, drains and closes the owned subprocess transport so no
   pipe/fd cleanup is left to `__del__` after this coroutine's event
   loop shuts down.
   """
   pgid = None
   try:
      pgid = os.getpgid(proc.pid)
   except ProcessLookupError:
      pass
   if pgid is not None:
      try:
         os.killpg(pgid, signal.SIGTERM)
      except ProcessLookupError:
         pass
   try:
      await asyncio.wait_for(proc.wait(), timeout=grace_sec)
   except asyncio.TimeoutError:
      if pgid is not None:
         try:
            os.killpg(pgid, signal.SIGKILL)
         except ProcessLookupError:
            pass
      await proc.wait()

   if pgid is not None:
      deadline = time.monotonic() + max(grace_sec, 0.0)
      if not await _wait_group_confirmed_gone(pgid, deadline):
         # The leader is gone (proc.wait() above returned) but
         # something else in its process group is still alive -- e.g.
         # a descendant that ignored the group SIGTERM above. SIGKILL
         # cannot be ignored, so escalate and confirm again. `pgid`
         # was fetched while `proc` itself was still alive, but by
         # this point the leader has already exited -- so, same as
         # `_process_group_alive`, a PermissionError here means the OS
         # has recycled `pgid` onto some other process this caller has
         # no business signalling; treat that as "gone".
         try:
            os.killpg(pgid, signal.SIGKILL)
         except (ProcessLookupError, PermissionError):
            pass
         else:
            escalated_deadline = time.monotonic() + max(grace_sec, 0.0)
            if not await _wait_group_confirmed_gone(
                  pgid, escalated_deadline):
               # Do NOT proceed to drain/close the transport and return
               # normally: that would tell the caller cleanup succeeded
               # (no orphan) while the process group may still be
               # alive. A process cannot ignore SIGKILL in a normal
               # runnable state, so exhausting a full confirmed
               # escalation cycle without seeing the group go away is
               # an invariant violation worth surfacing explicitly
               # rather than swallowing.
               raise ProcessGroupCleanupError(
                  "process group %r not confirmed gone after SIGKILL "
                  "escalation (grace_sec=%r)" % (pgid, grace_sec))

   await _close_subprocess_transport(proc)


async def _run_subprocess(argv, env, stdin_data, timeout_sec,
                           stderr_limit_bytes=_DEFAULT_STDERR_LIMIT_BYTES,
                           grace_sec=_DEFAULT_GRACE_SEC,
                           clock=time.monotonic,
                           subprocess_exec=asyncio.create_subprocess_exec):
   """Run one argv list to completion or timeout. Never invokes a shell
   -- `subprocess_exec` defaults to `asyncio.create_subprocess_exec`,
   which execs `argv[0]` directly.

   `subprocess_exec` and `clock` are injected so a test can substitute a
   deterministic clock or (less commonly) intercept process creation;
   the default test strategy in this project is a REAL executable fake
   (a fake `python3.x`/`ssh` script), not a mocked subprocess layer, so
   most tests never override this parameter.
   """
   started = clock()
   proc = await subprocess_exec(
      *argv,
      stdin=(asyncio.subprocess.PIPE if stdin_data is not None
             else asyncio.subprocess.DEVNULL),
      stdout=asyncio.subprocess.PIPE,
      stderr=asyncio.subprocess.PIPE,
      env=env,
      start_new_session=True,
   )

   async def _feed_stdin():
      if stdin_data is None:
         return
      try:
         proc.stdin.write(stdin_data)
         await proc.stdin.drain()
      except (BrokenPipeError, ConnectionResetError):
         # The child (e.g. ssh failing fast on auth) closed its stdin
         # before the daemon finished writing the probe source. That is
         # not a transport bug -- the child's own exit code/stderr,
         # captured below regardless, is the authoritative failure
         # signal. Swallow the pipe error so it cannot surface as an
         # unhandled/"Future exception was never retrieved" exception
         # ahead of the typed classification in _finish.
         pass
      finally:
         try:
            proc.stdin.close()
         except (BrokenPipeError, ConnectionResetError):
            pass
         try:
            await proc.stdin.wait_closed()
         except (BrokenPipeError, ConnectionResetError):
            # CPython's asyncio subprocess transport tracks stdin
            # closure with its own internal future
            # (SubprocessStreamProtocol._stdin_closed) that is set
            # with an exception when the pipe closed abnormally (the
            # same early-exit case this whole except-block exists
            # for). If nothing ever awaits it, asyncio logs an
            # "exception was never retrieved" warning on GC. Retrieve
            # and discard it here for the same reason we discard the
            # write/drain error above.
            pass

   async def _collect():
      stderr_task = asyncio.ensure_future(
         _drain_bounded(proc.stderr, stderr_limit_bytes))
      stdout_task = asyncio.ensure_future(_read_all(proc.stdout))
      await _feed_stdin()
      stdout_bytes = await stdout_task
      stderr_bytes, stderr_truncated = await stderr_task
      await proc.wait()
      return stdout_bytes, stderr_bytes, stderr_truncated

   try:
      stdout_bytes, stderr_bytes, stderr_truncated = await asyncio.wait_for(
         _collect(), timeout=timeout_sec)
   except asyncio.TimeoutError:
      await _terminate_process_group(proc, grace_sec)
      raise ProbeTimeoutError("probe exceeded %.1fs timeout" % timeout_sec)
   except BaseException as exc:
      # ANY exception raised while _collect() owns the live subprocess
      # -- not just our own internal timeout, and not just external
      # cancellation -- must still terminate/kill/reap the owned
      # process group before propagating. This function is the sole
      # owner of `proc`; nothing else will ever reap it. Examples: the
      # scheduler cancels this task at its next deadline (CancelledError,
      # a BaseException), or a caller-supplied argument makes
      # `proc.stdin.write()` raise synchronously inside `_feed_stdin`
      # (e.g. AssertionError from asyncio's own StreamWriter on a
      # non-bytes payload) before `proc.wait()` is ever reached.
      # `asyncio.shield` only matters for the cancellation case (so a
      # second cancellation of *this* task cannot interrupt the
      # cleanup it caused), but does no harm for any other exception.
      cleanup = _terminate_process_group(proc, grace_sec)
      if isinstance(exc, asyncio.CancelledError):
         await asyncio.shield(cleanup)
      else:
         await cleanup
      raise

   wall_seconds = clock() - started
   return _RawResult(
      exit_code=proc.returncode,
      stdout=stdout_bytes,
      stderr=stderr_bytes,
      stderr_truncated=stderr_truncated,
      wall_seconds=wall_seconds,
   )


def _finish(raw, expected_loop, expected_probe_version, expected_fqdn,
            is_remote):
   stderr_text = raw.stderr.decode("utf-8", "replace")
   if raw.exit_code != 0:
      if is_remote and raw.exit_code == _SSH_FAILURE_EXIT_CODE:
         exc_cls = _classify_ssh_failure(stderr_text)
         raise exc_cls(
            "ssh failed (exit %d): %s"
            % (_SSH_FAILURE_EXIT_CODE, stderr_text.strip()))
      raise ProbeExitError(
         "probe exited %d: %s" % (raw.exit_code, stderr_text.strip()),
         exit_code=raw.exit_code, stderr=stderr_text)

   try:
      payload = json.loads(raw.stdout.decode("utf-8", "replace"))
   except ValueError as exc:
      raise MalformedJSONError("probe stdout was not valid JSON: %s" % exc)

   validate_probe_payload(
      payload, expected_loop, expected_probe_version, expected_fqdn)

   return ProbeResult(
      payload=payload,
      exit_code=raw.exit_code,
      stdout_bytes=len(raw.stdout),
      stderr=stderr_text,
      stderr_truncated=raw.stderr_truncated,
      wall_seconds=raw.wall_seconds,
   )


# --------------------------------------------------------------------------
# Public entry points
# --------------------------------------------------------------------------

async def run_local_probe(probe_python, probe_script_path, loop,
                           probe_max_seconds, hard_timeout_sec,
                           expected_probe_version, expected_fqdn=None,
                           base_env=None,
                           stderr_limit_bytes=_DEFAULT_STDERR_LIMIT_BYTES,
                           grace_sec=_DEFAULT_GRACE_SEC,
                           clock=time.monotonic,
                           subprocess_exec=asyncio.create_subprocess_exec):
   """Invoke the probe locally: the configured interpreter runs the
   script by path, no stdin delivery, LD_PRELOAD stripped from its env.

   `probe_max_seconds` is passed to the probe as `--max-seconds` (its
   own internal self-abort budget); `hard_timeout_sec` is this
   function's own external deadline on the whole subprocess and should
   be set with headroom above `probe_max_seconds` so the probe's own
   clean self-abort (exit 4) is what normally fires, not a hard kill.
   """
   argv = build_local_argv(probe_python, probe_script_path, loop,
                            probe_max_seconds)
   env = _strip_ld_preload(base_env)
   raw = await _run_subprocess(
      argv, env, stdin_data=None, timeout_sec=hard_timeout_sec,
      stderr_limit_bytes=stderr_limit_bytes, grace_sec=grace_sec,
      clock=clock, subprocess_exec=subprocess_exec)
   return _finish(raw, expected_loop=loop,
                  expected_probe_version=expected_probe_version,
                  expected_fqdn=expected_fqdn, is_remote=False)


async def run_remote_probe(ssh_binary, ssh_config_path, connect_timeout_sec,
                            hostname, probe_python, probe_script_source,
                            loop, probe_max_seconds, hard_timeout_sec,
                            expected_probe_version, expected_fqdn=None,
                            base_env=None,
                            stderr_limit_bytes=_DEFAULT_STDERR_LIMIT_BYTES,
                            grace_sec=_DEFAULT_GRACE_SEC,
                            clock=time.monotonic,
                            subprocess_exec=asyncio.create_subprocess_exec):
   """Invoke the probe on a remote node over SSH stdin.

   `probe_script_source` is the exact bytes of remote_probe.py, read
   once by the caller (the scheduler) and passed in so N concurrent
   remote polls never each re-read the file. `connect_timeout_sec` must
   be strictly less than `hard_timeout_sec` (PLANNING.md 4.3: otherwise
   ssh's own ConnectTimeout alarm can fire after this function has
   already SIGKILLed the process group, leaving a stale control-socket
   entry behind) -- enforced here, not just documented, because the
   design calls this a startup assertion that "cannot regress silently".
   """
   if connect_timeout_sec >= hard_timeout_sec:
      raise ValueError(
         "connect_timeout_sec (%r) must be strictly less than "
         "hard_timeout_sec (%r) -- PLANNING.md 4.3"
         % (connect_timeout_sec, hard_timeout_sec))
   argv = build_remote_argv(ssh_binary, ssh_config_path, connect_timeout_sec,
                             hostname, probe_python, loop, probe_max_seconds)
   env = _strip_ld_preload(base_env)
   raw = await _run_subprocess(
      argv, env, stdin_data=probe_script_source, timeout_sec=hard_timeout_sec,
      stderr_limit_bytes=stderr_limit_bytes, grace_sec=grace_sec,
      clock=clock, subprocess_exec=subprocess_exec)
   return _finish(raw, expected_loop=loop,
                  expected_probe_version=expected_probe_version,
                  expected_fqdn=expected_fqdn, is_remote=True)
