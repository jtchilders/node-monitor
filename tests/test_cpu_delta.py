"""Tests for cpu_delta: cumulative-to-interval CPU conversion.

Each test targets one failure mode called out in cpu_delta.py's docstring:
a plain running process, pid recycling, a newly-started process, an exited
process landing in the unmeasured bucket, and a would-be-negative delta
surfacing as an anomaly instead of being clamped.
"""

from node_monitor.collector.cpu_delta import compute_cpu_delta


CLK_TCK = 100


def _row(pid, start_time_ticks, utime_ticks, stime_ticks):
   return {
      "pid": pid,
      "start_time_ticks": start_time_ticks,
      "utime_ticks": utime_ticks,
      "stime_ticks": stime_ticks,
   }


def _sample(collected_at_ticks, processes):
   return {"collected_at_ticks": collected_at_ticks, "processes": processes}


class TestNormalDelta:
   def test_continuously_running_process(self):
      sample_a = _sample(1000, [_row(42, 500, utime_ticks=100, stime_ticks=50)])
      sample_b = _sample(1900, [_row(42, 500, utime_ticks=250, stime_ticks=90)])

      result = compute_cpu_delta(sample_a, sample_b, CLK_TCK)

      assert result["unmeasured"] == []
      assert result["anomalies"] == []
      assert len(result["deltas"]) == 1
      delta = result["deltas"][0]
      assert delta["pid"] == 42
      assert delta["start_time_ticks"] == 500
      assert delta["utime_delta_ticks"] == 150
      assert delta["stime_delta_ticks"] == 40
      assert delta["cpu_seconds"] == 1.9  # (150 + 40) / 100


class TestPidRecycling:
   def test_same_pid_different_start_time_is_two_processes(self):
      # pid 42 in sample_a exited; the kernel handed pid 42 to an unrelated
      # new process by sample_b. Keying on pid alone would diff sample_b's
      # utime against sample_a's utime for what is actually two different
      # processes and could easily go negative or be absurdly large.
      old_proc = _row(42, start_time_ticks=100, utime_ticks=5000, stime_ticks=2000)
      new_proc = _row(42, start_time_ticks=1200, utime_ticks=10, stime_ticks=5)

      sample_a = _sample(1000, [old_proc])
      sample_b = _sample(1900, [new_proc])

      result = compute_cpu_delta(sample_a, sample_b, CLK_TCK)

      # Old pid-42 process is gone by sample_b: exited, unmeasured.
      assert {"pid": 42, "start_time_ticks": 100, "reason": "exited"} \
         in result["unmeasured"]
      # New pid-42 process started inside the window: attributed in full,
      # not diffed against the old process's counters.
      assert len(result["deltas"]) == 1
      delta = result["deltas"][0]
      assert delta["start_time_ticks"] == 1200
      assert delta["utime_delta_ticks"] == 10
      assert delta["stime_delta_ticks"] == 5
      assert result["anomalies"] == []


class TestNewProcess:
   def test_new_process_started_inside_window_gets_full_cumulative(self):
      sample_a = _sample(1000, [])
      sample_b = _sample(1900, [_row(77, start_time_ticks=1500,
                                      utime_ticks=30, stime_ticks=10)])

      result = compute_cpu_delta(sample_a, sample_b, CLK_TCK)

      assert result["unmeasured"] == []
      assert result["anomalies"] == []
      assert len(result["deltas"]) == 1
      delta = result["deltas"][0]
      assert delta["pid"] == 77
      assert delta["utime_delta_ticks"] == 30
      assert delta["stime_delta_ticks"] == 10
      assert delta["cpu_seconds"] == 0.4

   def test_process_started_before_window_is_unmeasured_not_attributed(self):
      # Present only in sample_b, but its start_time_ticks predates
      # sample_a's collection -- e.g. it was missed by a transient read
      # failure in the earlier census. We cannot know how much of its
      # cumulative CPU predates the window, so it must NOT be attributed in
      # full (that would overcount), and it is not a "new" process.
      sample_a = _sample(1000, [])
      sample_b = _sample(1900, [_row(88, start_time_ticks=500,
                                      utime_ticks=9000, stime_ticks=4000)])

      result = compute_cpu_delta(sample_a, sample_b, CLK_TCK)

      assert result["deltas"] == []
      assert result["anomalies"] == []
      assert {"pid": 88, "start_time_ticks": 500,
              "reason": "started_before_window"} in result["unmeasured"]


class TestExitedProcess:
   def test_exited_process_lands_in_unmeasured_not_dropped(self):
      sample_a = _sample(1000, [_row(55, start_time_ticks=200,
                                      utime_ticks=400, stime_ticks=100)])
      sample_b = _sample(1900, [])

      result = compute_cpu_delta(sample_a, sample_b, CLK_TCK)

      assert result["deltas"] == []
      assert result["anomalies"] == []
      assert len(result["unmeasured"]) == 1
      assert result["unmeasured"][0] == {
         "pid": 55, "start_time_ticks": 200, "reason": "exited",
      }


class TestNegativeDeltaAnomaly:
   def test_would_be_negative_delta_surfaces_as_anomaly_not_clamped(self):
      # Same (pid, start_time_ticks) -- genuinely the same process -- but
      # utime went backwards, which should be impossible for a cumulative
      # counter. This must be surfaced, not silently clamped to zero and
      # not included in deltas.
      sample_a = _sample(1000, [_row(9, start_time_ticks=50,
                                      utime_ticks=800, stime_ticks=200)])
      sample_b = _sample(1900, [_row(9, start_time_ticks=50,
                                      utime_ticks=700, stime_ticks=250)])

      result = compute_cpu_delta(sample_a, sample_b, CLK_TCK)

      assert result["deltas"] == []
      assert result["unmeasured"] == []
      assert len(result["anomalies"]) == 1
      anomaly = result["anomalies"][0]
      assert anomaly["pid"] == 9
      assert anomaly["start_time_ticks"] == 50
      assert anomaly["reason"] == "negative_delta"
      assert anomaly["utime_delta_ticks"] == -100
      assert anomaly["stime_delta_ticks"] == 50

   def test_negative_stime_alone_also_surfaces(self):
      sample_a = _sample(1000, [_row(9, start_time_ticks=50,
                                      utime_ticks=800, stime_ticks=250)])
      sample_b = _sample(1900, [_row(9, start_time_ticks=50,
                                      utime_ticks=900, stime_ticks=200)])

      result = compute_cpu_delta(sample_a, sample_b, CLK_TCK)

      assert result["deltas"] == []
      assert len(result["anomalies"]) == 1
      assert result["anomalies"][0]["reason"] == "negative_delta"


class TestMixedSample:
   def test_multiple_processes_sorted_into_correct_buckets(self):
      # One continuously running, one exited, one new-in-window, one
      # recycled pid -- exercised together to check nothing cross-talks.
      sample_a = _sample(1000, [
         _row(1, start_time_ticks=10, utime_ticks=100, stime_ticks=50),   # continues
         _row(2, start_time_ticks=20, utime_ticks=300, stime_ticks=100),  # exits
         _row(3, start_time_ticks=30, utime_ticks=50, stime_ticks=20),    # pid recycled
      ])
      sample_b = _sample(1900, [
         _row(1, start_time_ticks=10, utime_ticks=150, stime_ticks=70),
         _row(3, start_time_ticks=1600, utime_ticks=5, stime_ticks=2),    # new owner of pid 3
         _row(4, start_time_ticks=1700, utime_ticks=20, stime_ticks=10),  # brand new
      ])

      result = compute_cpu_delta(sample_a, sample_b, CLK_TCK)

      delta_pids = {d["pid"]: d for d in result["deltas"]}
      assert set(delta_pids) == {1, 3, 4}
      assert delta_pids[1]["utime_delta_ticks"] == 50
      assert delta_pids[3]["start_time_ticks"] == 1600
      assert delta_pids[4]["start_time_ticks"] == 1700

      unmeasured_pids = {(u["pid"], u["start_time_ticks"]): u["reason"]
                         for u in result["unmeasured"]}
      assert unmeasured_pids == {(2, 20): "exited", (3, 30): "exited"}
      assert result["anomalies"] == []
