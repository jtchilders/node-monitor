import pytest

from node_monitor.database import queries
from node_monitor.database.connect import connect
from tests.database.live_fixture import SYSTEM, populate, put_heartbeat


@pytest.fixture
def conn(migrated_db):
   now = populate(migrated_db)
   put_heartbeat(migrated_db, now)
   with connect(migrated_db, autocommit=True) as c:
      yield c


def pids(rows):
   return [r["pid"] for r in rows]


def test_heartbeat_missing_for_other_system(conn):
   assert queries.heartbeat(conn, SYSTEM)["version"] == "9.9"
   assert queries.heartbeat(conn, "other") is None


def test_node_states_counts_users_and_user_processes(conn):
   n1, n2 = queries.node_states(conn, SYSTEM)
   assert (n1["node"], n1["label"], n1["user_processes"], n1["users"]) == ("n1.example", "N1", 3, 2)
   # root's process is not a user process.
   assert (n2["label"], n2["user_processes"], n2["users"]) == ("N2", 2, 2)
   assert n1["session_state"] == "up" and n1["load1"] == 1.5 and n1["mem_total_kb"] == 4194304
   assert set(n1) >= {"updated_at", "last_census_at", "procs_total", "probe_version", "boot_id"}


def test_node_states_label_falls_back_to_nodes_then_host(conn):
   conn.execute("UPDATE live_nodes SET label = NULL WHERE node = 'n1.example'")
   conn.execute("UPDATE nodes SET label = NULL WHERE node = 'n2.example'")
   conn.execute("UPDATE live_nodes SET label = NULL WHERE node = 'n2.example'")
   n1, n2 = queries.node_states(conn, SYSTEM)
   assert n1["label"] == "N1"  # from nodes, if the writer recorded it
   assert n2["label"] == "n2.example"


def test_processes_filters_by_label_or_host_user_category(conn):
   assert pids(queries.processes(conn, SYSTEM, node="N1", sort="cpu")) == [1, 3, 2]
   assert pids(queries.processes(conn, SYSTEM, node="n2.example")) == [4, 5]
   assert pids(queries.processes(conn, SYSTEM, user="bob")) == [3, 4]
   assert pids(queries.processes(conn, SYSTEM, category="shell")) == [2, 5]
   assert pids(queries.processes(conn, SYSTEM, user="bob", category="ml")) == [3]
   assert pids(queries.processes(conn, SYSTEM, user_only=False, node="N2")) == [6, 4, 5]
   row = queries.processes(conn, SYSTEM, user="carol")[0]
   assert row["label"] == "N2" and row["username"] == "carol" and row["cmdline"] == "cmd5"


def test_processes_sort_reverse_limit_nulls_last(conn):
   assert pids(queries.processes(conn, SYSTEM, sort="rss")) == [1, 3, 4, 2, 5]
   assert pids(queries.processes(conn, SYSTEM, sort="rss", reverse=True, limit=2)) == [5, 2]
   # Only 1 and 3 use the GPU; NULLs stay last whichever way the sort runs.
   assert pids(queries.processes(conn, SYSTEM, sort="gpu"))[:2] == [1, 3]
   assert pids(queries.processes(conn, SYSTEM, sort="gpu", reverse=True))[:2] == [3, 1]
   assert pids(queries.processes(conn, SYSTEM, sort="idle"))[:3] == [2, 4, 3]
   assert pids(queries.processes(conn, SYSTEM, sort="threads"))[0] == 1


def test_processes_idle_filter_requires_idle_state(conn):
   conn.execute("UPDATE live_processes SET idle_for_sec = 9000 WHERE pid = 5")  # busy
   rows = queries.processes(conn, SYSTEM, idle_min_sec=3600, sort="idle")
   assert pids(rows) == [2, 4]
   absent = queries.processes(conn, SYSTEM, presence="absent", sort="rss")
   assert pids(absent) == [1, 2]
   assert pids(queries.processes(conn, SYSTEM, presence="absent", idle_min_sec=3600)) == [2]


def test_users_aggregates_and_top_category(conn):
   rows = queries.users(conn, SYSTEM)
   assert [r["username"] for r in rows] == ["alice", "bob", "carol"]
   alice, bob, _ = rows
   assert alice["processes"] == 2 and alice["nodes"] == 1
   assert alice["rss_kb"] == 4194304 + 10240 and alice["cpu_seconds"] == 505
   assert alice["gpu_mem_mb"] == 1000 and alice["top_category"] == "ml"
   assert bob["nodes"] == 2 and bob["top_category"] == "ml" and bob["idle_processes"] == 2
   assert [r["username"] for r in queries.users(conn, SYSTEM, node="N2", sort="procs")][-1] in (
      "bob",
      "carol",
   )
   assert [r["username"] for r in queries.users(conn, SYSTEM, category="shell", sort="cpu")] == [
      "alice",
      "carol",
   ]
   assert [r["username"] for r in queries.users(conn, SYSTEM, reverse=True, limit=1)] == ["carol"]


def test_unknown_sort_raises(conn):
   with pytest.raises(ValueError):
      queries.processes(conn, SYSTEM, sort="rss; DROP TABLE nodes")
   with pytest.raises(ValueError):
      queries.users(conn, SYSTEM, sort="threads")


def _age_snapshot(conn, node, minutes):
   conn.execute(
      "UPDATE live_snapshots SET ts = now() - make_interval(mins => %s) WHERE node = %s",
      (minutes, node),
   )


def test_max_age_leaves_out_nodes_whose_snapshot_is_old(conn):
   _age_snapshot(conn, "n1.example", 10)
   assert pids(queries.processes(conn, SYSTEM, max_age_sec=120)) == [4, 5]
   assert pids(queries.processes(conn, SYSTEM)) == [1, 3, 4, 2, 5]
   assert pids(queries.processes(conn, SYSTEM, max_age_sec=900)) == [1, 3, 4, 2, 5]
   assert [r["username"] for r in queries.users(conn, SYSTEM, max_age_sec=120)] == ["bob", "carol"]
   assert [r["username"] for r in queries.users(conn, SYSTEM)] == ["alice", "bob", "carol"]
   assert queries.stale_process_nodes(conn, SYSTEM, 120) == ["N1"]
   assert queries.stale_process_nodes(conn, SYSTEM, 120, node="N2") == []
   assert queries.stale_process_nodes(conn, SYSTEM, 900) == []


def test_processes_rows_carry_the_snapshot_time(conn):
   (snapshot_ts,) = conn.execute(
      "SELECT ts FROM live_snapshots WHERE node = 'n1.example'"
   ).fetchone()
   rows = queries.processes(conn, SYSTEM, node="N1")
   assert {r["ts"] for r in rows} == {snapshot_ts}
