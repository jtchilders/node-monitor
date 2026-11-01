"""Value encoding for SQL parameters, and record (de)serialization for the disk spool."""

from __future__ import annotations

import math
from datetime import datetime

from psycopg.types.json import Jsonb

from node_monitor.pipeline.records import Record

_DT = "$dt"


def _finite(value):
   """`value` with every non-finite float, at any depth, replaced by None.

   PostgreSQL rejects NaN and Infinity in jsonb, and stores NaN in a double column as a
   value that poisons averages; a gauge the node could not compute is unknown, i.e. NULL.
   """
   if isinstance(value, float):
      return value if math.isfinite(value) else None
   if isinstance(value, dict):
      return {k: _finite(v) for k, v in value.items()}
   if isinstance(value, (list, tuple)):
      return [_finite(v) for v in value]
   return value


def to_db_value(value):
   """A row value as a SQL parameter: containers become jsonb, non-finite floats NULL."""
   if isinstance(value, (dict, list)):
      return Jsonb(_finite(value))
   if isinstance(value, float) and not math.isfinite(value):
      return None
   return value


def _encode(value):
   if isinstance(value, datetime):
      return {_DT: value.isoformat()}
   if isinstance(value, dict):
      return {k: _encode(v) for k, v in value.items()}
   if isinstance(value, (list, tuple)):
      return [_encode(v) for v in value]
   if isinstance(value, float) and not math.isfinite(value):
      # Strict JSON has no NaN or Infinity, and the database stores them as NULL anyway.
      return None
   return value


def _decode(value):
   if isinstance(value, dict):
      if len(value) == 1 and isinstance(value.get(_DT), str):
         return datetime.fromisoformat(value[_DT])
      return {k: _decode(v) for k, v in value.items()}
   if isinstance(value, list):
      return [_decode(v) for v in value]
   return value


def encode_record(record: Record) -> dict:
   """`record` as a JSON-safe dict; datetimes become {"$dt": iso} and keep their offset."""
   return {"kind": record.kind, "row": _encode(record.row)}


def decode_record(d: dict) -> Record:
   return Record(d["kind"], _decode(d["row"]))
