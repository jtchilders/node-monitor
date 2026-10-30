"""Feed recorded soak captures through the per-node pipeline (I/O adapter, not pure)."""

from __future__ import annotations

import dataclasses
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from node_monitor.config import Config
from node_monitor.pipeline.node_pipeline import NodePipeline


def _parse_recv(text):
   dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
   return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _lines(path: Path):
   with open(path, encoding="utf-8", errors="replace") as handle:
      for raw in handle:
         try:
            item = json.loads(raw)
            recv = _parse_recv(item["recv"])
            kind = item["kind"]
         except (ValueError, KeyError, TypeError):
            continue  # a torn final line from a killed recorder
         if isinstance(item, dict) and kind in ("msg", "event"):
            yield recv, item


def replay(out_dir: Path, config: Config, on_record=None, on_skip=None) -> dict[str, Counter]:
   out_dir = Path(out_dir)
   manifest = json.loads((out_dir / "manifest.json").read_text())
   collection = config.collection
   # The capture's own intervals decide how its lines must be bucketed.
   overrides = {
      key: manifest[key]
      for key in ("counter_interval_sec", "census_interval_sec")
      if isinstance(manifest.get(key), (int, float))
   }
   collection = dataclasses.replace(collection, **overrides)
   nodes = {n.label: n for n in config.nodes}
   result: dict[str, Counter] = {}
   for entry in manifest.get("nodes", []):
      label = entry["label"]
      node = nodes.get(label)
      if node is None:
         if on_skip:
            on_skip(label)
         continue
      pipeline = NodePipeline(
         system=config.system,
         node=node,
         collection=collection,
         classification=config.classification,
         redaction=config.redaction,
      )
      counts = result[label] = Counter()

      def take(records, counts=counts):
         for record in records:
            counts[record.kind] += 1
            if on_record:
               on_record(record)

      # Per-UTC-day filenames sort in receipt order.
      for path in sorted(out_dir.glob(f"{label}-*.jsonl")):
         for recv, item in _lines(path):
            if item["kind"] == "msg":
               take(pipeline.on_message(item.get("msg") or {}, recv))
            else:
               take(pipeline.on_event(item.get("event"), item.get("detail") or {}, recv))
      take(pipeline.flush())
   return result
