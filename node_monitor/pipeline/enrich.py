"""Turn raw probe census rows into live process rows.

Every row gets a non-null `username`: when the probe could not resolve the owner it sends
None, and the row is keyed as `uid:<uid>` instead. Usage summaries are keyed by username, so
this keeps unresolved owners with different uids apart rather than merging them under one
null user, and owner presence uses the same key.

`gpu_mem_mb` is 0.0 for a process without GPU memory only when the census's GPU query
succeeded (`gpu_status == "ok"`). Otherwise it stays as the probe sent it, usually None, so
an unmeasured value is never stored as a measured zero.
"""

from __future__ import annotations

import re

from node_monitor.pipeline.classify import classify
from node_monitor.pipeline.redact import Redactor

LIVE_PROCESS_COLUMNS = (
   "pid",
   "start_time",
   "uid",
   "username",
   "comm",
   "exe",
   "argv0",
   "cmdline",
   "ppid",
   "parent_comm",
   "parent_exe",
   "parent_kind",
   "category",
   "activity",
   "state",
   "threads",
   "rss_kb",
   "cpu_seconds",
   "cpu_state",
   "gpu_mem_mb",
   "interactive",
   "under_screen_or_tmux",
   "idle_for_sec",
   "idle_for_is_lower_bound",
   "idle_bucket",
   "owner_presence",
   "is_user_process",
)

# Anchored so the root-owned "sshd: USER [priv]" form never counts as a session. OpenSSH 9.8+
# titles the same process "sshd-session: ...".
_SSHD_SESSION = re.compile(r"^sshd(?:-session)?: [^@\s\[]+@(?P<tty>pts/\d+|notty)$")

_COPIED = (
   "pid",
   "start_time",
   "uid",
   "username",
   "comm",
   "exe",
   "argv0",
   "ppid",
   "parent_comm",
   "parent_exe",
   "parent_kind",
   "state",
   "threads",
   "rss_kb",
   "cpu_seconds",
   "gpu_mem_mb",
   "interactive",
   "under_screen_or_tmux",
   "idle_for_sec",
   "idle_for_is_lower_bound",
)


def user_key(row: dict) -> str:
   username = row.get("username")
   return username if username is not None else f"uid:{row['uid']}"


def owner_presence_by_user(rows: list[dict]) -> dict[str, str]:
   """Presence per user key. The user-side sshd process runs as the user, so its owner (not
   the name in its title, which any process can set) says whose session it is."""
   presence: dict[str, str] = {}
   for row in rows:
      m = _SSHD_SESSION.match(row.get("cmdline") or "")
      if not m:
         continue
      user = user_key(row)
      if m.group("tty") == "notty":
         presence.setdefault(user, "connected")
      else:
         presence[user] = "interactive"
   return presence


def idle_bucket(cpu_state: str | None, idle_for_sec: float | None) -> str:
   if cpu_state is None or idle_for_sec is None:
      return "unknown"
   if idle_for_sec < 300:
      return "<5m"
   if idle_for_sec < 3600:
      return "5m-1h"
   if idle_for_sec < 86400:
      return "1h-1d"
   return ">1d"


def enrich_census(
   rows: list[dict], *, redactor: Redactor, min_user_uid: int, gpu_status: str | None = None
) -> list[dict]:
   """`gpu_status` is the census's own (None from probes that predate it)."""
   presence = owner_presence_by_user(rows)
   gpu_measured = gpu_status == "ok"
   out = []
   for row in rows:
      cmdline = row.get("cmdline") or ""
      # Classify before redacting so rules see the real arguments.
      cls = classify(row["comm"], cmdline, row["uid"], min_user_uid)
      new = {k: row.get(k) for k in _COPIED}
      new["username"] = user_key(row)
      # A setproctitle title lands in argv0 whole, arguments included.
      if new["argv0"]:
         new["argv0"] = redactor.redact(new["argv0"])
      new["cmdline"] = redactor.redact(cmdline)
      if gpu_measured and new["gpu_mem_mb"] is None:
         new["gpu_mem_mb"] = 0.0
      new["category"] = cls.category
      new["activity"] = cls.activity
      new["cpu_state"] = row.get("cpu_state") or "unknown"
      new["idle_bucket"] = idle_bucket(row.get("cpu_state"), row.get("idle_for_sec"))
      new["owner_presence"] = presence.get(new["username"], "absent")
      new["is_user_process"] = row["uid"] >= min_user_uid and cls.category != "system"
      out.append({k: new[k] for k in LIVE_PROCESS_COLUMNS})
   return out
