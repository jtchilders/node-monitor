"""Probe emitter: bounded memory per message, and byte-exact compact JSON output."""

import io
import json
import tracemalloc

import pytest


def fake_row(pid, cmdline="python x.py"):
   """A census row with the fields scan_processes, the tracker and the GPU join produce."""
   return {
      "pid": pid,
      "ppid": 1,
      "start_time": 1000 + pid,
      "uid": 1000,
      "username": "alice",
      "comm": "python",
      "exe": "/usr/bin/python3",
      "argv0": cmdline.split(" ", 1)[0],
      "cmdline": cmdline,
      "state": "S",
      "threads": 1,
      "rss_kb": 4096,
      "tty_nr": 0,
      "interactive": False,
      "session_leader": False,
      "parent_comm": "bash",
      "parent_exe": "/bin/bash",
      "under_screen_or_tmux": False,
      "parent_kind": "shell",
      "cpu_seconds": 0.02,
      "cpu_state": "idle",
      "idle_for_sec": 120.0,
      "idle_for_is_lower_bound": False,
      "gpu_mem_mb": None,
   }


class _Recorder(io.RawIOBase):
   """A binary sink without a fileno; keeps every write's length and the bytes."""

   def __init__(self):
      io.RawIOBase.__init__(self)
      self.sizes = []
      self.data = bytearray()

   def writable(self):
      return True

   def write(self, b):
      self.sizes.append(len(b))
      self.data += b
      return len(b)


def compact(msg):
   return json.dumps(msg, separators=(",", ":")) + "\n"


def test_census_emission_peak_allocation_is_bounded(probe):
   # 2,000 processes with 1 KiB command lines -> a ~2.8 MB line
   rows = [dict(fake_row(pid=i), cmdline="x" * 1024) for i in range(2000)]
   out = io.BytesIO()  # fd-less path
   em = probe._Emitter(out)
   tracemalloc.start()
   tracemalloc.reset_peak()
   base = tracemalloc.get_traced_memory()[0]
   em.emit("census", {"processes": rows})
   current, peak = tracemalloc.get_traced_memory()
   tracemalloc.stop()
   # The BytesIO buffer is itself traced and must hold the whole line; what is bounded is
   # the emitter's transient allocation above what the sink keeps.
   retained = current - base
   line = out.getvalue()
   assert len(line) > 2 * 1024 * 1024
   assert json.loads(line)["processes"][1999]["pid"] == 1999
   assert peak - base - retained < 1024 * 1024  # was > 3 x line size before the fix


def test_streamed_bytes_match_compact_json(probe):
   msg_fields = {"processes": [fake_row(pid=1), fake_row(pid=2, cmdline="é ünïcode")]}
   out = io.BytesIO()
   probe._Emitter(out).emit("census", msg_fields)
   msg = json.loads(out.getvalue())
   expected = compact(msg)
   assert out.getvalue().decode("ascii") == expected


def test_large_message_is_written_in_bounded_chunks(probe):
   rows = [dict(fake_row(pid=i), cmdline="x" * 1024 + "é ") for i in range(500)]
   out = _Recorder()
   probe._Emitter(out).emit("census", {"processes": rows})
   assert len(out.sizes) > 5
   assert max(out.sizes) <= probe.EMIT_CHUNK_BYTES
   assert bytes(out.data).decode("ascii") == compact(json.loads(bytes(out.data)))


def test_piece_larger_than_a_chunk_is_written_whole(probe):
   big = "y" * (probe.EMIT_CHUNK_BYTES * 2)
   out = _Recorder()
   probe._Emitter(out).emit("census", {"processes": [fake_row(1), fake_row(2, cmdline=big)]})
   msg = json.loads(bytes(out.data))
   assert msg["processes"][1]["cmdline"] == big
   assert bytes(out.data).decode("ascii") == compact(msg)


def test_fd_path_streams_identical_bytes_and_counts_seq(probe, tmp_path):
   rows = [dict(fake_row(pid=i), cmdline="z" * 700) for i in range(400)]
   path = tmp_path / "out"
   with open(str(path), "wb") as handle:
      em = probe._Emitter(handle)
      em.emit("census", {"processes": rows})
      em.emit("counters", {"load1": 1.5})
   lines = path.read_bytes().split(b"\n")
   assert lines[-1] == b""
   first, second = json.loads(lines[0]), json.loads(lines[1])
   assert (first["seq"], second["seq"]) == (0, 1)
   assert lines[0] + b"\n" == compact(first).encode("ascii")
   assert len(first["processes"]) == 400


def test_encoding_failure_mid_line_ends_the_fragment_and_keeps_seq(probe):
   # Enough rows that the first chunks are already written when the bad value is reached.
   rows = [dict(fake_row(pid=i), cmdline="x" * 1024) for i in range(200)]
   out = io.BytesIO()
   em = probe._Emitter(out)
   with pytest.raises(TypeError):
      em.emit("census", {"processes": rows, "bad": object()})
   em.emit("error", {"where": "census", "message": "boom"})
   fragment, line, tail = out.getvalue().split(b"\n")
   assert len(fragment) > probe.EMIT_CHUNK_BYTES and tail == b""
   assert json.loads(line)["seq"] == 0 and json.loads(line)["type"] == "error"


def test_rss_limit_default(probe):
   assert probe.default_config()["probe_rss_limit_mb"] == 128
