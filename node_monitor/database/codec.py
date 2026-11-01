"""Value encoding for SQL parameters, and record (de)serialization for the disk spool."""

from __future__ import annotations

import math
from datetime import datetime

from psycopg.types.json import Jsonb

from node_monitor.pipeline.records import Record

_DT = "$dt"


def _clean_str(s: str) -> str:
   """`s` as PostgreSQL will store it: without NUL characters and without lone surrogates.

   A surrogate comes from a non-UTF-8 byte read with surrogateescape (os.readlink, os.listdir
   and argv do this); it is mapped back to its byte and replaced as invalid UTF-8.
   """
   if "\x00" in s:
      s = s.replace("\x00", "")
   try:
      s.encode("utf-8")
   except UnicodeEncodeError:
      try:
         s = s.encode("utf-8", "surrogateescape").decode("utf-8", "replace")
      except UnicodeEncodeError:
         # A surrogate that stands for no byte, e.g. "\ud800".
         s = s.encode("utf-8", "surrogatepass").decode("utf-8", "replace")
   return s


def _clean(value):
   """`value` with every non-finite float, at any depth, replaced by None and every string,
   dict keys included, made storable; tuples become lists.

   PostgreSQL rejects NaN and Infinity in jsonb, and stores NaN in a double column as a
   value that poisons averages; a gauge the node could not compute is unknown, i.e. NULL.
   It rejects NUL and unpaired surrogates in text and jsonb alike, and one such value from
   a long-lived process would otherwise fail its node's batch every minute.
   """
   if isinstance(value, str):
      return _clean_str(value)
   if isinstance(value, float):
      return value if math.isfinite(value) else None
   if isinstance(value, dict):
      return {(_clean_str(k) if isinstance(k, str) else k): _clean(v) for k, v in value.items()}
   if isinstance(value, (list, tuple)):
      return [_clean(v) for v in value]
   return value


def to_db_value(value):
   """A row value as a SQL parameter: containers become jsonb, non-finite floats NULL, and
   strings lose what PostgreSQL cannot store."""
   if isinstance(value, (dict, list, tuple)):
      return Jsonb(_clean(value))
   return _clean(value)


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
