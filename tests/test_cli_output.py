import csv
import io
import json
from datetime import datetime, timedelta, timezone

from node_monitor.cli.output import Column, fmt_age, fmt_duration, fmt_kb, fmt_pct, render

T = datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc)
ROWS = [
   {"name": "a", "kb": 1536, "ts": T, "extra": "x"},
   {"name": "b", "kb": None, "ts": None, "extra": "y"},
]
COLS = [Column("name", "Name"), Column("kb", "Size", fmt_kb, "right"), Column("ts", "When")]


def out(fmt, **kw):
   buf = io.StringIO()
   render(ROWS, COLS, fmt, file=buf, **kw)
   return buf.getvalue()


def test_render_json_restricts_to_column_keys_and_isoformats():
   assert json.loads(out("json")) == [
      {"name": "a", "kb": 1536, "ts": T.isoformat()},
      {"name": "b", "kb": None, "ts": None},
   ]


def test_render_csv_header_and_raw_values():
   rows = list(csv.reader(io.StringIO(out("csv"))))
   assert rows[0] == ["name", "kb", "ts"]
   assert rows[1] == ["a", "1536", T.isoformat()]
   assert rows[2] == ["b", "", ""]


def test_render_table_contains_headers_and_formatted_values():
   text = out("table", title="Things")
   for expected in ("Things", "Name", "Size", "When", "1.5 MiB", "a", "b"):
      assert expected in text
   assert "1536" not in text
   assert max(len(line) for line in text.splitlines()) <= 200


def test_render_table_empty_still_shows_headers():
   buf = io.StringIO()
   render([], COLS, "table", file=buf)
   assert "Name" in buf.getvalue()


def test_fmt_helpers():
   assert fmt_kb(512) == "512 KiB"
   assert fmt_kb(1536) == "1.5 MiB"
   assert fmt_kb(1536 * 1024) == "1.5 GiB"
   assert fmt_kb(None) == "-"
   assert fmt_duration(3600, True) == "≥1h00m"
   assert fmt_duration(7500) == "2h05m"
   assert fmt_duration(42) == "42s"
   assert fmt_duration(180) == "3m"
   assert fmt_duration(4 * 86400) == "4d"
   assert fmt_duration(None) == "-"
   assert fmt_pct(12.34) == "12.3%"
   assert fmt_pct(None) == "-"


def test_fmt_age_boundaries():
   def age(sec):
      return fmt_age(T - timedelta(seconds=sec), T)

   assert [age(s) for s in (0, 42, 59)] == ["0s", "42s", "59s"]
   assert [age(s) for s in (60, 180, 3599)] == ["1m", "3m", "59m"]
   assert [age(s) for s in (3600, 7500)] == ["1h00m", "2h05m"]
   assert age(4 * 86400) == "4d"
   assert age(-5) == "0s"  # clock skew between collector and reader
   assert fmt_age(None, T) == "-"


def test_render_table_shows_bracketed_values_literally():
   tricky = "[kworker/0:1] [bold]x[/bold] python [a.py]"
   buf = io.StringIO()
   render([{"cmd": tricky}], [Column("cmd", "[Cmd]")], "table", title="[T]", file=buf)
   text = buf.getvalue()
   assert tricky in text
   assert "[Cmd]" in text and "[T]" in text
