"""M0 soak: record every node's probe stream to JSONL and summarize the recording."""

import asyncio
import hashlib
import json
import os
import re
import signal
import subprocess
import sys
from collections import defaultdict
from collections.abc import Callable, Iterator
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
# Soak output can hold every user's command lines (--full-census) on a shared filesystem.
PRIVATE_DIR_MODE = 0o700
PRIVATE_FILE_MODE = 0o600
STDERR_TAIL_REPORT_CHARS = 200


def make_private_dir(path: Path) -> Path:
   """Create `path` (and any missing parents) owner-only; tighten `path` if it exists.

   Parents that already exist are left alone: they may be the user's own directories.
   """
   path = Path(path)
   missing = []
   ancestor = path
   while not ancestor.exists():
      missing.append(ancestor)
      ancestor = ancestor.parent
   for directory in reversed(missing):
      directory.mkdir(mode=PRIVATE_DIR_MODE, exist_ok=True)
   # mkdir's mode is filtered by the umask, and an existing out dir keeps its own mode.
   for directory in [*missing, path]:
      os.chmod(directory, PRIVATE_DIR_MODE)
   return path


def _open_private(path: Path, mode: str):
   flags = os.O_WRONLY | os.O_CREAT | (os.O_APPEND if mode == "a" else os.O_TRUNC)
   fd = os.open(path, flags, PRIVATE_FILE_MODE)
   try:
      # The open mode applies only on creation (and through the umask).
      os.fchmod(fd, PRIVATE_FILE_MODE)
      return os.fdopen(fd, mode, encoding="utf-8")
   except BaseException:
      os.close(fd)
      raise


class SoakRecorder:
   """Appends one JSON line per message or event to `<out_dir>/<label>-<YYYYMMDD>.jsonl`.

   Never raises into a session: a failing disk must not end a node's collection, so write
   errors are counted, the first is reported to stderr, and the count is reported on close.
   """

   def __init__(self, out_dir: Path, full_census: bool = False) -> None:
      self._dir = make_private_dir(out_dir)
      self._full_census = full_census
      self._files: dict[str, tuple[str, object]] = {}
      self.write_errors = 0

   def on_message(self, label: str, msg: dict) -> None:
      self._write(label, lambda: {"kind": "msg", "msg": self._shape(msg)})

   def on_event(self, label: str, kind: str, detail: dict) -> None:
      self._write(label, lambda: {"kind": "event", "event": kind, "detail": detail})

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
      states = {"active": 0, "idle": 0, "null": 0}
      cpu = [p["cpu_seconds"] for p in procs if p.get("cpu_seconds") is not None]
      idle = [p["idle_for_sec"] for p in procs if p.get("idle_for_sec") is not None]
      for p in procs:
         state = p.get("cpu_state")
         states[state if state in ("active", "idle") else "null"] += 1
      return {
         "type": "census",
         "seq": msg.get("seq"),
         "ts": msg.get("ts"),
         "interval_sec": msg.get("interval_sec"),
         "scan_seconds": msg.get("scan_seconds"),
         "coverage": msg.get("coverage"),
         "gpus": msg.get("gpus"),
         "gpu_status": msg.get("gpu_status"),
         "process_count": len(procs),
         "user_count": len({p.get("username") for p in procs}),
         # Dormancy and delta evidence the raw processes would have carried: a negative
         # or huge cpu_seconds would mean a delta across two different processes.
         "cpu_state_counts": states,
         "idle_lower_bound_count": sum(1 for p in procs if p.get("idle_for_is_lower_bound")),
         "idle_for_max_sec": max(idle) if idle else None,
         "cpu_seconds_min": min(cpu) if cpu else None,
         "cpu_seconds_max": max(cpu) if cpu else None,
         # The probe's own compact encoding, so this is the line size actually received.
         "bytes": len(json.dumps(msg, separators=(",", ":"))),
      }

   def _write(self, label: str, make_body: Callable[[], dict]) -> None:
      now = datetime.now(timezone.utc)
      try:
         # Shaping is inside the guard: a malformed census must not raise into the session.
         line = {"recv": now.isoformat(), **make_body()}
         day = now.strftime("%Y%m%d")
         entry = self._files.get(label)
         if entry is None or entry[0] != day:
            if entry is not None:
               entry[1].close()
            handle = _open_private(self._dir / f"{label}-{day}.jsonl", "a")
            entry = self._files[label] = (day, handle)
         entry[1].write(json.dumps(line) + "\n")
         entry[1].flush()
      except Exception as exc:
         self.write_errors += 1
         if self.write_errors == 1:
            print(f"soak: write error for {label}: {exc!r}", file=sys.stderr)


_PACKAGE_DIR = Path(__file__).resolve().parent


def _git(args: list[str], cwd: Path) -> str | None:
   try:
      out = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, timeout=5)
   except (OSError, subprocess.SubprocessError):
      return None
   return out.stdout.strip() if out.returncode == 0 and out.stdout.strip() else None


def _git_commit(cwd: Path = _PACKAGE_DIR) -> str | None:
   return _git(["rev-parse", "HEAD"], cwd)


def _git_describe(cwd: Path = _PACKAGE_DIR) -> str | None:
   # "-dirty" marks uncommitted changes to tracked files: the code that ran is not HEAD.
   return _git(["describe", "--always", "--dirty"], cwd)


def _write_manifest(config: Config, out_dir: Path, probe_bytes: bytes) -> None:
   manifest = {
      "started": datetime.now(timezone.utc).isoformat(),
      "system": config.system,
      "nodes": [{"label": n.label, "ssh_target": n.ssh_target} for n in config.nodes],
      "counter_interval_sec": config.collection.counter_interval_sec,
      "census_interval_sec": config.collection.census_interval_sec,
      "probe_version": PROBE_VERSION,
      "git_commit": _git_commit(),
      "git_describe": _git_describe(),
      # Of the exact source shipped to the nodes, whatever the checkout says.
      "probe_sha256": hashlib.sha256(probe_bytes).hexdigest(),
   }
   with _open_private(out_dir / "manifest.json", "w") as handle:
      handle.write(json.dumps(manifest, indent=2) + "\n")


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
   out_dir = make_private_dir(out_dir)
   ssh_config = write_ssh_config(config.working_dir / "state", config.ssh)
   source = probe_source()
   _write_manifest(config, out_dir, source.encode("utf-8"))
   recorder = SoakRecorder(out_dir, full_census)
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


def _parse_ts(text: str) -> datetime:
   return datetime.fromisoformat(text)


def _min(current, value):
   return value if current is None or (value is not None and value < current) else current


def _max(current, value):
   return value if current is None or (value is not None and value > current) else current


class _NodeSummary:
   """Folds one node's recording, line by line in receipt order, into scalars.

   Only what the metrics need is kept (a few counters, the census scan times for the
   percentiles), so memory does not grow with the recording: a multi-day full-census
   run is far larger than the collector's memory budget.
   """

   def __init__(self, counter_interval: float, census_interval: float | None) -> None:
      self._counter_interval = counter_interval
      self._census_interval = census_interval
      self.first = self.last = None
      self.counters = 0
      self._last_counter_at = None
      self.max_gap = None
      self.losses: dict[str, int] = defaultdict(int)
      self.loss_timeline: list[dict] = []
      self.started = self.rotations = self.reboots = 0
      self.crashed = self.malformed = self.stderr = 0
      self.errors_by_where: dict[str, int] = defaultdict(int)
      self.cpu_total = 0.0
      self._session_cpu_first = self._session_cpu_last = None
      self.maxrss = 0
      self.scans: list[float] = []
      self.census = 0
      self._size_total = self._proc_total = 0
      self.gpu_status: dict[str, int] = defaultdict(int)
      self.cpu_seconds_min = self.cpu_seconds_max = self.idle_for_max = None

   def add(self, ln: dict) -> None:
      if self.first is None:
         self.first = ln["recv"]
      self.last = ln["recv"]
      if ln.get("kind") == "event":
         self._add_event(ln)
      else:
         self._add_message(ln)

   def _close_session(self) -> None:
      if self._session_cpu_first is not None and self._session_cpu_last is not None:
         self.cpu_total += self._session_cpu_last - self._session_cpu_first
      self._session_cpu_first = self._session_cpu_last = None

   def _add_event(self, ln: dict) -> None:
      ev = ln.get("event")
      detail = ln.get("detail") or {}
      if ev == "session_started":
         self.started += 1
         self._close_session()
      elif ev == "session_lost":
         self.losses[detail.get("reason", "unknown")] += 1
         self.loss_timeline.append(
            {
               "recv": ln["recv"],
               "reason": detail.get("reason"),
               "exit_code": detail.get("exit_code"),
               "stderr_tail": (detail.get("stderr_tail") or "")[:STDERR_TAIL_REPORT_CHARS],
            }
         )
      elif ev == "session_rotated":
         self.rotations += 1
      elif ev == "node_rebooted":
         self.reboots += 1
      elif ev == "session_crashed":
         self.crashed += 1
      elif ev == "malformed_line":
         self.malformed += 1
      elif ev == "probe_stderr":
         self.stderr += 1

   def _add_message(self, ln: dict) -> None:
      msg = ln.get("msg") or {}
      kind = msg.get("type")
      if kind == "counters":
         self._add_counters(_parse_ts(ln["recv"]), msg)
      elif kind == "census":
         self._add_census(msg)
      elif kind == "error":
         self.errors_by_where[msg.get("where", "unknown")] += 1

   def _add_counters(self, at: datetime, msg: dict) -> None:
      self.counters += 1
      if self._last_counter_at is not None:
         self.max_gap = _max(self.max_gap, (at - self._last_counter_at).total_seconds())
      self._last_counter_at = at
      ps = msg.get("probe_self")
      if ps:
         cpu = ps.get("cpu_seconds", 0) + ps.get("children_cpu_seconds", 0)
         if self._session_cpu_first is None:
            self._session_cpu_first = cpu
         self._session_cpu_last = cpu
         self.maxrss = max(self.maxrss, ps.get("maxrss_kb", 0))

   def _add_census(self, msg: dict) -> None:
      self.census += 1
      if msg.get("scan_seconds") is not None:
         self.scans.append(msg["scan_seconds"])
      if msg.get("gpu_status") is not None:
         self.gpu_status[msg["gpu_status"]] += 1
      if "process_count" in msg:
         self._proc_total += msg["process_count"]
         self._size_total += msg.get("bytes", 0)
         cpu_min, cpu_max = msg.get("cpu_seconds_min"), msg.get("cpu_seconds_max")
         idle_max = msg.get("idle_for_max_sec")
      else:
         procs = msg.get("processes") or []
         self._proc_total += len(procs)
         self._size_total += len(json.dumps(msg, separators=(",", ":")))
         cpu = [p["cpu_seconds"] for p in procs if p.get("cpu_seconds") is not None]
         idle = [p["idle_for_sec"] for p in procs if p.get("idle_for_sec") is not None]
         cpu_min, cpu_max = min(cpu, default=None), max(cpu, default=None)
         idle_max = max(idle, default=None)
      self.cpu_seconds_min = _min(self.cpu_seconds_min, cpu_min)
      self.cpu_seconds_max = _max(self.cpu_seconds_max, cpu_max)
      self.idle_for_max = _max(self.idle_for_max, idle_max)

   def result(self) -> dict:
      self._close_session()
      hours = (_parse_ts(self.last) - _parse_ts(self.first)).total_seconds() / 3600
      expected = hours * 3600 / self._counter_interval
      census_expected = hours * 3600 / self._census_interval if self._census_interval else None
      return {
         "first": self.first,
         "last": self.last,
         "hours": hours,
         "counters_received": self.counters,
         "counters_expected": expected,
         "coverage_pct": 100 * self.counters / expected if expected > 0 else None,
         "census_received": self.census,
         "census_expected": census_expected,
         "census_coverage_pct": (100 * self.census / census_expected if census_expected else None),
         "max_gap_sec": self.max_gap,
         "sessions_started": self.started,
         "losses_by_reason": dict(self.losses),
         "loss_timeline": self.loss_timeline,
         "rotations": self.rotations,
         "reboots": self.reboots,
         "errors_by_where": dict(self.errors_by_where),
         "session_crashed_count": self.crashed,
         "malformed_line_count": self.malformed,
         "probe_stderr_count": self.stderr,
         "probe_cpu_sec_per_hour": self.cpu_total / hours if hours > 0 else None,
         "probe_maxrss_kb": self.maxrss,
         "census_scan_p50": _percentile(self.scans, 50),
         "census_scan_p95": _percentile(self.scans, 95),
         "census_scan_max": max(self.scans) if self.scans else None,
         "census_bytes_avg": self._size_total / self.census if self.census else None,
         "process_count_avg": self._proc_total / self.census if self.census else None,
         "gpu_status_counts": dict(self.gpu_status),
         "cpu_seconds_min": self.cpu_seconds_min,
         "cpu_seconds_max": self.cpu_seconds_max,
         "idle_for_max_sec": self.idle_for_max,
      }


def _recorded_lines(paths: list[Path]) -> Iterator[dict]:
   for path in paths:
      with open(path, encoding="utf-8") as handle:
         for raw in handle:
            try:
               line = json.loads(raw)
            except ValueError:
               continue  # a torn last line from a killed run
            if isinstance(line, dict) and isinstance(line.get("recv"), str):
               yield line


def summarize(out_dir: Path) -> dict:
   out_dir = Path(out_dir)
   manifest = json.loads((out_dir / "manifest.json").read_text())
   by_label: dict[str, list[tuple[str, Path]]] = defaultdict(list)
   for path in out_dir.glob("*.jsonl"):
      match = _FILE_RE.match(path.name)
      if match:
         by_label[match["label"]].append((match["day"], path))
   result = {}
   for label, dated in sorted(by_label.items()):
      summary = _NodeSummary(manifest["counter_interval_sec"], manifest.get("census_interval_sec"))
      # One file per UTC day, appended as lines arrive: day order is receipt order.
      for line in _recorded_lines([path for _, path in sorted(dated)]):
         summary.add(line)
      if summary.first is not None:
         result[label] = summary.result()
   return result
