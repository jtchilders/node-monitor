"""Collector-side pieces that ship the probe to a node over ssh stdin."""

import json
import shlex
from importlib import resources

from node_monitor.config import Config

# Python 3.6-compatible; the first line is the marker the probe's lock takeover looks for.
# stdin is left open after the source is read: it is the probe's liveness channel.
BOOTSTRAP_CODE = """\
#node-monitor-probe
import sys, json
h = json.loads(sys.stdin.buffer.readline().decode("utf-8"))
src = sys.stdin.buffer.read(h["len"]).decode("utf-8")
g = {"__name__": "__node_monitor_probe__", "PROBE_CONFIG": h["config"]}
exec(compile(src, "node_monitor_probe", "exec"), g)
"""


def probe_source() -> str:
   return resources.files("node_monitor.probe").joinpath("probe.py").read_text(encoding="utf-8")


def probe_config(config: Config) -> dict:
   return {
      "counter_interval_sec": config.collection.counter_interval_sec,
      "census_interval_sec": config.collection.census_interval_sec,
      "active_cpu_seconds_per_min": config.classification.active_cpu_seconds_per_min,
      "gpu": True,
   }


def build_payload(source: str, probe_cfg: dict) -> bytes:
   # The header length is in bytes: the bootstrap reads that many bytes from a binary stream.
   body = source.encode("utf-8")
   header = json.dumps({"len": len(body), "config": probe_cfg}) + "\n"
   return header.encode("utf-8") + body


def remote_command(probe_python: str) -> str:
   # The site's XALT LD_PRELOAD can abort the interpreter at exit, so strip it.
   return shlex.join(["env", "-u", "LD_PRELOAD", probe_python, "-u", "-c", BOOTSTRAP_CODE])
