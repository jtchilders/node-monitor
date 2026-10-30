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


def test_enrich_unresolved_username_keys_on_uid():
   rows = [r(pid=1, uid=5001, username=None), r(pid=2, uid=5002, username=None), r(pid=3)]
   out = {x["pid"]: x for x in enrich_census(rows, redactor=Redactor(), min_user_uid=1000)}
   assert out[1]["username"] == "uid:5001" and out[2]["username"] == "uid:5002"
   assert out[3]["username"] == "alice"
   assert rows[0]["username"] is None


def test_enrich_owner_presence_uses_the_uid_key():
   # The user-side sshd process runs as the user, so its owner is the presence key.
   rows = [
      r(pid=1, uid=5001, username=None),
      r(pid=2, uid=5001, username=None, cmdline="sshd: dave@pts/1"),
      r(pid=3, uid=5002, username=None),
   ]
   out = {x["pid"]: x for x in enrich_census(rows, redactor=Redactor(), min_user_uid=1000)}
   assert out[1]["owner_presence"] == "interactive"
   assert out[3]["owner_presence"] == "absent"


def test_owner_presence_accepts_sshd_session_titles():
   rows = [
      r(uid=0, cmdline="sshd-session: alice [priv]"),
      r(uid=1001, username="alice", cmdline="sshd-session: alice@notty"),
      r(uid=1002, username="bob", cmdline="sshd-session: bob@pts/4"),
   ]
   assert owner_presence_by_user(rows) == {"alice": "connected", "bob": "interactive"}


@pytest.mark.parametrize(
   "status,sent,stored",
   [
      ("ok", None, 0.0),
      ("ok", 512, 512),
      ("error", None, None),
      ("suspended", None, None),
      ("absent", None, None),
      (None, None, None),
      (None, 64, 64),
   ],
)
def test_enrich_gpu_mem_is_zero_only_when_measured(status, sent, stored):
   (out,) = enrich_census(
      [r(gpu_mem_mb=sent)], redactor=Redactor(), min_user_uid=1000, gpu_status=status
   )
   assert out["gpu_mem_mb"] == stored


def test_enrich_redacts_argv0_titles():
   rows = [r(argv0="worker --token abc123", cmdline="worker --token abc123")]
   (out,) = enrich_census(rows, redactor=Redactor(), min_user_uid=1000)
   assert out["argv0"] == "worker --token ***" and out["cmdline"] == "worker --token ***"
   (out,) = enrich_census([r(argv0=None)], redactor=Redactor(), min_user_uid=1000)
   assert out["argv0"] is None
