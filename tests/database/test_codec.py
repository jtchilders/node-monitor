import json
import math
from datetime import datetime, timedelta, timezone

from psycopg.types.json import Jsonb

from node_monitor.database.codec import decode_record, encode_record, to_db_value
from node_monitor.pipeline.records import (
   COLLECTION_EVENT,
   LIVE_NODE,
   LIVE_PROCESSES,
   NODE_HARDWARE,
   NODE_METRICS_1M,
   NODE_USAGE_5M,
   USER_USAGE_5M,
   Record,
)
from tests.database.sessions import scripted_records


def test_codec_round_trip():
   recs = scripted_records()
   kinds = {r.kind for r in recs}
   assert kinds == {
      COLLECTION_EVENT,
      LIVE_NODE,
      LIVE_PROCESSES,
      NODE_HARDWARE,
      NODE_METRICS_1M,
      NODE_USAGE_5M,
      USER_USAGE_5M,
   }
   for rec in recs:
      # Strict JSON: the spool must be readable by any JSON parser.
      text = json.dumps(encode_record(rec), allow_nan=False)
      back = decode_record(json.loads(text))
      assert isinstance(back, Record)
      assert back == rec


def test_codec_keeps_timezone_of_datetimes():
   ts = datetime(2026, 11, 1, 12, 0, 0, 123456, tzinfo=timezone.utc)
   other = datetime(2026, 11, 1, 7, 0, tzinfo=timezone(timedelta(hours=-5)))
   rec = Record(COLLECTION_EVENT, {"ts": ts, "detail": {"nested": [other]}})
   back = decode_record(json.loads(json.dumps(encode_record(rec))))
   assert back.row["ts"] == ts and back.row["ts"].tzinfo is not None
   assert back.row["ts"].microsecond == 123456
   assert back.row["detail"]["nested"][0].utcoffset() == timedelta(hours=-5)


def test_codec_writes_non_finite_floats_as_null():
   # The database stores them as NULL anyway, and strict JSON has no NaN or Infinity.
   rec = Record(NODE_METRICS_1M, {"load1": math.inf, "cpu_pct": {"user": math.nan}})
   text = json.dumps(encode_record(rec), allow_nan=False)
   assert decode_record(json.loads(text)).row == {"load1": None, "cpu_pct": {"user": None}}


def test_to_db_value_wraps_containers_in_jsonb_without_non_finite_floats():
   value = to_db_value({"a": math.nan, "b": [1.5, -math.inf, {"c": math.inf}], "d": "x"})
   assert isinstance(value, Jsonb)
   assert value.obj == {"a": None, "b": [1.5, None, {"c": None}], "d": "x"}
   lst = to_db_value([math.nan, 2])
   assert isinstance(lst, Jsonb) and lst.obj == [None, 2]


def test_to_db_value_scalars():
   assert to_db_value(math.nan) is None
   assert to_db_value(math.inf) is None
   assert to_db_value(-math.inf) is None
   assert to_db_value(1.25) == 1.25
   ts = datetime(2026, 11, 1, tzinfo=timezone.utc)
   for v in (None, 3, "text", True, ts):
      assert to_db_value(v) is v


def test_to_db_value_does_not_mutate_its_input():
   original = {"a": [math.nan]}
   to_db_value(original)
   assert math.isnan(original["a"][0])
