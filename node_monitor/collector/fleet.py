"""Session setup and the run/stop lifecycle shared by the soak command and the daemon."""

import asyncio
import signal
import sys
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
) -> None:
   """Run every session until `duration_sec` elapses, `stop` is set, SIGINT/SIGTERM
   arrives, or all sessions end; then stop them together and wait for them.

   A session that raises is reported through `on_crash` and the others carry on.
   """

   async def supervise(label: str, session: NodeSession) -> None:
      try:
         await session.run()
      except Exception as exc:
         print(f"soak: session for {label} ended with {exc!r}", file=sys.stderr)
         on_crash(label, exc)

   loop = asyncio.get_running_loop()
   stop_requested = stop if stop is not None else asyncio.Event()
   installed = []
   for sig in (signal.SIGINT, signal.SIGTERM):
      try:
         loop.add_signal_handler(sig, stop_requested.set)
         installed.append(sig)
      except (NotImplementedError, RuntimeError, ValueError):
         pass
   tasks = [asyncio.create_task(supervise(lbl, s)) for lbl, s in sessions.items()]
   try:
      waiter = asyncio.create_task(stop_requested.wait())
      all_done = asyncio.create_task(asyncio.wait(tasks)) if tasks else None
      pending = {waiter} if all_done is None else {waiter, all_done}
      try:
         await asyncio.wait(pending, timeout=duration_sec, return_when=asyncio.FIRST_COMPLETED)
      finally:
         waiter.cancel()
         if all_done is not None:
            all_done.cancel()
      # Stop everything first, then wait together: shutdown costs the slowest session.
      for session in sessions.values():
         session.stop()
      await asyncio.gather(*tasks)
   finally:
      for sig in installed:
         loop.remove_signal_handler(sig)
