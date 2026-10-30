import pytest

from node_monitor.pipeline.enrich import (
   LIVE_PROCESS_COLUMNS,
   enrich_census,
   idle_bucket,
   owner_presence_by_user,
)
from node_monitor.pipeline.redact import Redactor


def r(**overrides):
   row = {
      "pid": 1,
      "ppid": 0,
      "start_time": 100,
      "uid": 1001,
      "username": "alice",
      "comm": "bash",
      "exe": "/bin/bash",
      "argv0": "bash",
      "cmdline": "bash",
      "state": "S",
      "threads": 1,
      "rss_kb": 10,
      "tty_nr": 0,
      "interactive": False,
      "session_leader": False,
      "parent_comm": "sshd",
      "parent_exe": "/usr/sbin/sshd",
      "parent_kind": "sshd",
      "under_screen_or_tmux": False,
      "gpu_mem_mb": 0,
      "cpu_seconds": 1.0,
      "cpu_state": "idle",
      "idle_for_sec": 10,
      "idle_for_is_lower_bound": False,
   }
   row.update(overrides)
   return row


def test_owner_presence_forms():
   rows = [
      r(uid=0, cmdline="sshd: alice [priv]"),
      r(uid=1001, username="alice", cmdline="sshd: alice@notty"),
      r(uid=1002, username="bob.x-y", cmdline="sshd: bob.x-y@pts/3"),
      r(uid=1003, username="carol", cmdline="sshd: carol@notty"),
      r(uid=1003, username="carol", cmdline="sshd: carol@pts/9"),
   ]
   assert owner_presence_by_user(rows) == {
      "alice": "connected",
      "bob.x-y": "interactive",
      "carol": "interactive",
   }


@pytest.mark.parametrize(
   "state,idle,bucket",
   [
      (None, None, "unknown"),
      ("active", 0, "<5m"),
      ("idle", 299, "<5m"),
      ("idle", 300, "5m-1h"),
      ("idle", 3599, "5m-1h"),
      ("idle", 3600, "1h-1d"),
      ("idle", 86400, ">1d"),
   ],
)
def test_idle_bucket(state, idle, bucket):
   assert idle_bucket(state, idle) == bucket


def test_enrich_census_rows():
   rows = [
      r(
         pid=7,
         uid=31112,
         username="parton",
         cmdline="claude --api-key sk-abcdefghijklmnopqrst --resume",
         cpu_state=None,
         idle_for_sec=None,
      ),
      r(pid=8, uid=31112, username="parton", cmdline="sshd: parton@pts/2"),
      r(pid=9, uid=0, username="root", cmdline="/usr/sbin/sshd -D"),
   ]
   out = {x["pid"]: x for x in enrich_census(rows, redactor=Redactor(), min_user_uid=1000)}
   assert tuple(out[7]) == LIVE_PROCESS_COLUMNS
   assert out[7]["cmdline"] == "claude --api-key *** --resume"
   assert (out[7]["category"], out[7]["activity"]) == ("ai_agent", "claude-code")
   assert (out[7]["cpu_state"], out[7]["idle_bucket"], out[7]["owner_presence"]) == (
      "unknown",
      "unknown",
      "interactive",
   )
   assert out[7]["is_user_process"] and not out[9]["is_user_process"]
   assert rows[0]["cmdline"].startswith("claude --api-key sk-")


def test_enrich_unresolvable_username_is_absent():
   out = enrich_census([r(username=None)], redactor=Redactor(), min_user_uid=1000)
   assert out[0]["owner_presence"] == "absent"
