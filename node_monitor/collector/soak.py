"""M0 soak: record every node's probe stream to JSONL and summarize the recording."""

import asyncio
import json
import re
import signal
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from node_monitor.collector.bootstrap import (
   build_payload,
   probe_config,
   probe_source,
   remote_command,
)
from node_monitor.collector.session import NodeSession
from node_monitor.collector.ssh import ssh_argv, write_ssh_config
from node_monitor.config import Config
from node_monitor.probe.probe import PROBE_VERSION

_FILE_RE = re.compile(r"^(?P<label>.+)-(?P<day>\d{8})\.jsonl$")


class SoakRecorder:
   """Appends one JSON line per message or event to `<out_dir>/<label>-<YYYYMMDD>.jsonl`.

   Never raises into a session: a failing disk must not end a node's collection, so write
   errors are counted, the first is reported to stderr, and the count is reported on close.
   """

   def __init__(self, out_dir: Path, full_census: bool = False) -> None:
      self._dir = Path(out_dir)
      self._dir.mkdir(parents=True, exist_ok=True)
      self._full_census = full_census
      self._files: dict[str, tuple[str, object]] = {}
      self.write_errors = 0

   def on_message(self, label: str, msg: dict) -> None:
      self._write(label, {"kind": "msg", "msg": self._shape(msg)})

   def on_event(self, label: str, kind: str, detail: dict) -> None:
      self._write(label, {"kind": "event", "event": kind, "detail": detail})

   def close(self) -> None:
      for _, handle in self._files.values():
         try:
            handle.close()
         except OSError:
            pass
      self._files.clear()
      if self.write_errors:
         print(f"soak: {self.write_errors} write errors in total", file=sys.stderr)

   def _shape(self, msg: dict) -> dict:
      if self._full_census or msg.get("type") != "census":
         return msg
      procs = msg.get("processes") or []
      return {
         "type": "census",
         "seq": msg.get("seq"),
         "ts": msg.get("ts"),
         "interval_sec": msg.get("interval_sec"),
         "scan_seconds": msg.get("scan_seconds"),
         "coverage": msg.get("coverage"),
         "gpus": msg.get("gpus"),
         "process_count": len(procs),
         "user_count": len({p.get("username") for p in procs}),
         "bytes": len(json.dumps(msg)),
      }

   def _write(self, label: str, body: dict) -> None:
      now = datetime.now(timezone.utc)
      try:
         day = now.strftime("%Y%m%d")
         entry = self._files.get(label)
         if entry is None or entry[0] != day:
            if entry is not None:
               entry[1].close()
            handle = open(self._dir / f"{label}-{day}.jsonl", "a", encoding="utf-8")
            entry = self._files[label] = (day, handle)
         line = {"recv": now.isoformat(), **body}
         entry[1].write(json.dumps(line) + "\n")
         entry[1].flush()
      except (OSError, ValueError, TypeError) as exc:
         self.write_errors += 1
         if self.write_errors == 1:
            print(f"soak: write error for {label}: {exc!r}", file=sys.stderr)


def _git_commit() -> str | None:
   try:
      out = subprocess.run(
         ["git", "rev-parse", "HEAD"],
         cwd=Path(__file__).resolve().parent,
         capture_output=True,
         text=True,
         timeout=5,
      )
   except (OSError, subprocess.SubprocessError):
      return None
   return out.stdout.strip() if out.returncode == 0 and out.stdout.strip() else None


def _write_manifest(config: Config, out_dir: Path) -> None:
   manifest = {
      "started": datetime.now(timezone.utc).isoformat(),
      "system": config.system,
      "nodes": [{"label": n.label, "ssh_target": n.ssh_target} for n in config.nodes],
      "counter_interval_sec": config.collection.counter_interval_sec,
      "census_interval_sec": config.collection.census_interval_sec,
      "probe_version": PROBE_VERSION,
      "git_commit": _git_commit(),
   }
   (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


async def run_soak(
   config: Config,
   out_dir: Path,
   duration_sec: float | None,
   full_census: bool = False,
   ssh_binary: str = "ssh",
   probe_overrides: dict | None = None,
) -> int:
   """Run one session per node until `duration_sec` elapses or SIGINT/SIGTERM.

   `probe_overrides` is merged over the probe config for every node. Probes on one host
   would take over each other's lock, so a `lock_path` override is made per node by
   appending `.<label>`.
   """
   out_dir = Path(out_dir)
   out_dir.mkdir(parents=True, exist_ok=True)
   ssh_config = write_ssh_config(config.working_dir / "state", config.ssh)
   _write_manifest(config, out_dir)
   recorder = SoakRecorder(out_dir, full_census)
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
         on_message=recorder.on_message,
         on_event=recorder.on_event,
         silence_timeout=coll.session_silence_timeout_sec,
         max_lifetime=coll.session_max_lifetime_sec,
         backoff=coll.reconnect_backoff_sec,
      )

   async def supervise(label: str, session: NodeSession) -> None:
      try:
         await session.run()
      except Exception as exc:
         print(f"soak: session for {label} ended with {exc!r}", file=sys.stderr)
         recorder.on_event(label, "session_crashed", {"error": repr(exc)})

   loop = asyncio.get_running_loop()
   stop_requested = asyncio.Event()
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
      all_done = asyncio.create_task(asyncio.wait(tasks))
      try:
         await asyncio.wait(
            {waiter, all_done}, timeout=duration_sec, return_when=asyncio.FIRST_COMPLETED
         )
      finally:
         waiter.cancel()
         all_done.cancel()
      # Stop everything first, then wait together: shutdown costs the slowest session.
      for session in sessions.values():
         session.stop()
      await asyncio.gather(*tasks)
   finally:
      for sig in installed:
         loop.remove_signal_handler(sig)
      recorder.close()
   return 0


def _percentile(values: list[float], pct: float) -> float | None:
   if not values:
      return None
   ordered = sorted(values)
   rank = max(1, -(-len(ordered) * pct // 100))
   return ordered[int(rank) - 1]


def _mean(values: list[float]) -> float | None:
   return sum(values) / len(values) if values else None


def _parse_ts(text: str) -> datetime:
   return datetime.fromisoformat(text)


def _summarize_node(lines: list[dict], counter_interval: float) -> dict:
   lines.sort(key=lambda ln: ln["recv"])
   first, last = _parse_ts(lines[0]["recv"]), _parse_ts(lines[-1]["recv"])
   hours = (last - first).total_seconds() / 3600
   counter_times = []
   losses: dict[str, int] = defaultdict(int)
   started = rotations = reboots = 0
   cpu_total = 0.0
   session_cpu: list[float] = []
   maxrss = 0
   scans, sizes, procs = [], [], []

   def close_session():
      nonlocal cpu_total
      if len(session_cpu) >= 2:
         cpu_total += session_cpu[-1] - session_cpu[0]
      session_cpu.clear()

   for ln in lines:
      if ln["kind"] == "event":
         ev = ln["event"]
         if ev == "session_started":
            started += 1
            close_session()
         elif ev == "session_lost":
            losses[ln["detail"].get("reason", "unknown")] += 1
         elif ev == "session_rotated":
            rotations += 1
         elif ev == "node_rebooted":
            reboots += 1
         continue
      msg = ln["msg"]
      if msg.get("type") == "counters":
         counter_times.append(_parse_ts(ln["recv"]))
         ps = msg.get("probe_self")
         if ps:
            session_cpu.append(ps.get("cpu_seconds", 0) + ps.get("children_cpu_seconds", 0))
            maxrss = max(maxrss, ps.get("maxrss_kb", 0))
      elif msg.get("type") == "census":
         if msg.get("scan_seconds") is not None:
            scans.append(msg["scan_seconds"])
         if "process_count" in msg:
            procs.append(msg["process_count"])
            sizes.append(msg.get("bytes", 0))
         else:
            procs.append(len(msg.get("processes") or []))
            sizes.append(len(json.dumps(msg)))
   close_session()

   gaps = [(b - a).total_seconds() for a, b in zip(counter_times, counter_times[1:], strict=False)]
   expected = hours * 3600 / counter_interval
   return {
      "first": lines[0]["recv"],
      "last": lines[-1]["recv"],
      "hours": hours,
      "counters_received": len(counter_times),
      "counters_expected": expected,
      "coverage_pct": 100 * len(counter_times) / expected if expected > 0 else None,
      "max_gap_sec": max(gaps) if gaps else None,
      "sessions_started": started,
      "losses_by_reason": dict(losses),
      "rotations": rotations,
      "reboots": reboots,
      "probe_cpu_sec_per_hour": cpu_total / hours if hours > 0 else None,
      "probe_maxrss_kb": maxrss,
      "census_scan_p50": _percentile(scans, 50),
      "census_scan_p95": _percentile(scans, 95),
      "census_scan_max": max(scans) if scans else None,
      "census_bytes_avg": _mean(sizes),
      "process_count_avg": _mean(procs),
   }


def summarize(out_dir: Path) -> dict:
   out_dir = Path(out_dir)
   manifest = json.loads((out_dir / "manifest.json").read_text())
   interval = manifest["counter_interval_sec"]
   by_label: dict[str, list[dict]] = defaultdict(list)
   for path in sorted(out_dir.glob("*.jsonl")):
      match = _FILE_RE.match(path.name)
      if not match:
         continue
      for raw in path.read_text(encoding="utf-8").splitlines():
         try:
            by_label[match["label"]].append(json.loads(raw))
         except ValueError:
            continue  # a torn last line from a killed run
   return {label: _summarize_node(lines, interval) for label, lines in sorted(by_label.items())}
