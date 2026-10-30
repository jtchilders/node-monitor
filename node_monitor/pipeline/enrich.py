"""Turn raw probe census rows into live process rows."""

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

# Anchored so the root-owned "sshd: USER [priv]" form never counts as a session.
_SSHD_SESSION = re.compile(r"^sshd: (?P<user>[^@\s\[]+)@(?P<tty>pts/\d+|notty)$")

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


def owner_presence_by_user(rows: list[dict]) -> dict[str, str]:
   presence: dict[str, str] = {}
   for row in rows:
      m = _SSHD_SESSION.match(row.get("cmdline") or "")
      if not m:
         continue
      user = m.group("user")
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


def enrich_census(rows: list[dict], *, redactor: Redactor, min_user_uid: int) -> list[dict]:
   presence = owner_presence_by_user(rows)
   out = []
   for row in rows:
      cmdline = row.get("cmdline") or ""
      # Classify before redacting so rules see the real arguments.
      cls = classify(row["comm"], cmdline, row["uid"], min_user_uid)
      new = {k: row.get(k) for k in _COPIED}
      new["cmdline"] = redactor.redact(cmdline)
      new["category"] = cls.category
      new["activity"] = cls.activity
      new["cpu_state"] = row.get("cpu_state") or "unknown"
      new["idle_bucket"] = idle_bucket(row.get("cpu_state"), row.get("idle_for_sec"))
      new["owner_presence"] = presence.get(row.get("username"), "absent")
      new["is_user_process"] = row["uid"] >= min_user_uid and cls.category != "system"
      out.append({k: new[k] for k in LIVE_PROCESS_COLUMNS})
   return out
