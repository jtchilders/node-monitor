"""Session setup and the run/stop lifecycle shared by the soak command and the daemon."""

import asyncio
import logging
import signal
from collections.abc import Callable

from node_monitor.collector.bootstrap import (
   build_payload,
   probe_config,
   probe_source,
   remote_command,
)
from node_monitor.collector.session import NodeSession
from node_monitor.collector.ssh import ssh_argv, write_ssh_config
from node_monitor.config import Config

log = logging.getLogger(__name__)


def build_sessions(
   config: Config,
   *,
   on_message: Callable[[str, dict], None],
   on_event: Callable[[str, str, dict], None],
   ssh_binary: str = "ssh",
   probe_overrides: dict | None = None,
) -> dict[str, NodeSession]:
   """One session per node, keyed by label.

   `probe_overrides` is merged over the probe config for every node. Probes on one host
   would take over each other's lock, so a `lock_path` override is made per node by
   appending `.<label>`.
   """
   ssh_config = write_ssh_config(config.working_dir / "state", config.ssh)
   source = probe_source()
   command = remote_command(config.probe_python)
   coll = config.collection
   sessions = {}
   for node in config.nodes:
      cfg = {**probe_config(config), **(probe_overrides or {})}
      if "lock_path" in cfg:
         cfg["lock_path"] = f"{cfg['lock_path']}.{node.label}"
      sessions[node.label] = NodeSession(
         node.label,
         argv=ssh_argv(ssh_binary, ssh_config, node.ssh_target, command),
         payload=build_payload(source, cfg),
         on_message=on_message,
         on_event=on_event,
         silence_timeout=coll.session_silence_timeout_sec,
         max_lifetime=coll.session_max_lifetime_sec,
         backoff=coll.reconnect_backoff_sec,
      )
   return sessions


async def run_sessions(
   sessions: dict[str, NodeSession],
   *,
   duration_sec: float | None = None,
   stop: asyncio.Event | None = None,
   on_crash: Callable[[str, BaseException], None],
   handle_signals: bool = True,
) -> None:
   """Run every session until `duration_sec` elapses, `stop` is set, SIGINT/SIGTERM
   arrives, or all sessions end; then stop them together and wait for them.

   With `handle_signals` false no signal handler is installed: the caller owns SIGINT and
   SIGTERM and sets `stop` itself, for work that must outlive the sessions.

   A session that raises is reported through `on_crash` and the others carry on. However
   this returns, cancellation included, every session has been stopped and awaited, so no
   ssh process outlives it.
   """

   async def supervise(label: str, session: NodeSession) -> None:
      try:
         await session.run()
      except Exception as exc:
         on_crash(label, exc)

   loop = asyncio.get_running_loop()
   stop_requested = stop if stop is not None else asyncio.Event()
   installed = install_signal_handlers(stop_requested.set) if handle_signals else []
   tasks = {label: asyncio.create_task(supervise(label, s)) for label, s in sessions.items()}
   try:
      waiter = asyncio.create_task(stop_requested.wait())
      all_done = asyncio.create_task(asyncio.wait(tasks.values())) if tasks else None
      pending = {waiter} if all_done is None else {waiter, all_done}
      try:
         await asyncio.wait(pending, timeout=duration_sec, return_when=asyncio.FIRST_COMPLETED)
      finally:
         waiter.cancel()
         if all_done is not None:
            all_done.cancel()
   finally:
      try:
         await _stop_all(sessions, tasks)
      finally:
         for sig in installed:
            loop.remove_signal_handler(sig)


def install_signal_handlers(callback: Callable[[], None]) -> list[int]:
   """Run `callback` on the running loop for SIGINT and SIGTERM; returns the signals handled.

   Installing over an existing handler replaces it with no gap in between. Where handlers
   cannot be installed (not the main thread, no Unix signals), the defaults stay.
   """
   loop = asyncio.get_running_loop()
   installed = []
   for sig in (signal.SIGINT, signal.SIGTERM):
      try:
         loop.add_signal_handler(sig, callback)
         installed.append(sig)
      except (NotImplementedError, RuntimeError, ValueError):
         pass
   return installed


async def _stop_all(sessions: dict[str, NodeSession], tasks: dict[str, asyncio.Task]) -> None:
   # Stop everything first, then wait together: shutdown costs the slowest session.
   for label, session in sessions.items():
      try:
         session.stop()
      except Exception:
         log.exception("stopping the session for %s failed; cancelling it", label)
         # Cancelling still runs the session's cleanup, which ends its ssh process group.
         tasks[label].cancel()
   if not tasks:
      return
   # asyncio.wait, not gather: a second cancellation of the caller must not cancel the
   # sessions while they are still shutting their processes down.
   await asyncio.wait(tasks.values())
   for label, task in tasks.items():
      if not task.cancelled() and task.exception() is not None:
         log.error("session task for %s failed: %r", label, task.exception())
