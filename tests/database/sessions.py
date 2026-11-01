"""Real pipeline records for database tests, from the scripted session the pipeline tests use."""

from tests.pipeline import at
from tests.pipeline.test_node_pipeline import census, counters, hello, make_pipeline, row

# What the probe's collect_hardware sends, plus a key the schema has no column for.
HARDWARE = {
   "cpu_model": "AMD EPYC 7543P",
   "cpu_logical": 64,
   "sockets": 1,
   "cores_per_socket": 32,
   "cpu_max_freq_khz": 2800000,
   "numa_nodes": 4,
   "mem_total_kb": 527000000,
   "swap_total_kb": 0,
   "hugepage_size_kb": 2048,
   "kernel_release": "5.14.21",
   "os_pretty_name": "SUSE Linux Enterprise Server 15 SP5",
   "net_fs_mounts": 3,
   "net_ifaces": {"hsn0": 200000, "eth0": None},
   "gpus": ["0000:07:00.0", "0000:46:00.0"],
   "cpu_count": 64,
}


def full_hello(boot_id="b1"):
   msg = hello(boot_id=boot_id)
   msg["probe_version"] = 1
   msg["hardware"] = dict(HARDWARE)
   return msg


def scripted_records(pipeline=None, *, start=0):
   """Every record kind: hello, 5 minutes of counters with a census each minute, a lost
   session and a flush. Times are `start` seconds after tests.pipeline.at(0)."""
   p = pipeline or make_pipeline()
   recs = p.on_event("session_started", {"attempt": 1, "pid": 9}, at(start))
   recs += p.on_message(full_hello(), at(start))
   for i in range(31):
      t = start + i * 10
      recs += p.on_message(counters(t, user=i * 50), at(t))
      if i % 6 == 0:
         recs += p.on_message(
            census([row(cmdline="claude --token t1"), row(pid=2, start_time=200)]), at(t)
         )
   recs += p.on_event("session_lost", {"reason": "eof"}, at(start + 305))
   recs += p.flush()
   return recs
