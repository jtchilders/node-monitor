"""Rendering rows as a table, JSON or CSV, and the value formatters the tables use."""

import csv
import json
import os
import sys
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from rich.console import Console
from rich.table import Table
from rich.text import Text

# Wide enough that a piped table does not wrap; a terminal uses its own width.
_PIPED_WIDTH = 200


@dataclass(frozen=True)
class Column:
   key: str
   header: str
   fmt: Callable[[Any], str] | None = None
   justify: str = "left"


def json_default(value):
   if isinstance(value, datetime):
      return value.isoformat()
   raise TypeError(f"cannot serialize {type(value).__name__}")


def _raw(value):
   return value.isoformat() if isinstance(value, datetime) else value


def render(
   rows: list[dict], columns: list[Column], fmt: str, *, title: str | None = None, file=None
) -> None:
   """Write `rows` restricted to `columns`. JSON and CSV carry raw values; the table applies
   each column's formatter. `file` defaults to the current stdout."""
   out = file if file is not None else sys.stdout
   if fmt == "json":
      data = [{c.key: r.get(c.key) for c in columns} for r in rows]
      out.write(json.dumps(data, default=json_default, indent=2) + "\n")
   elif fmt == "csv":
      writer = csv.writer(out, lineterminator="\n")
      writer.writerow([c.key for c in columns])
      for r in rows:
         writer.writerow(["" if r.get(c.key) is None else _raw(r.get(c.key)) for c in columns])
   elif fmt == "table":
      table = Table(title=Text(title) if title else None, title_justify="left")
      for c in columns:
         table.add_column(Text(c.header), justify=c.justify)
      for r in rows:
         table.add_row(*[_cell(c, r.get(c.key)) for c in columns])
      console = Console(file=out)
      if not console.is_terminal and "COLUMNS" not in os.environ:
         console.width = _PIPED_WIDTH
      console.print(table)
   else:
      raise ValueError(f"unknown format {fmt!r}")


def _cell(column: Column, value) -> Text:
   # Text, not str: rich would parse brackets in process names and command lines as markup.
   if column.fmt is not None:
      return Text(column.fmt(value))
   return Text("-" if value is None else str(value))


def fmt_kb(kb) -> str:
   """Binary units from a KiB count: 512 KiB, 1.5 MiB, 1.5 GiB."""
   if kb is None:
      return "-"
   if kb < 1024:
      return f"{kb:.0f} KiB"
   value = kb / 1024
   for unit in ("MiB", "GiB"):
      if value < 1024 or unit == "GiB":
         return f"{value:.1f} {unit}"
      value /= 1024
   raise AssertionError("unreachable")


def fmt_duration(sec, lower_bound: bool = False) -> str:
   """42s, 3m, 2h05m, 4d; `lower_bound` prefixes the value with ≥."""
   if sec is None:
      return "-"
   sec = max(0, int(sec))
   if sec < 60:
      text = f"{sec}s"
   elif sec < 3600:
      text = f"{sec // 60}m"
   elif sec < 86400:
      text = f"{sec // 3600}h{sec % 3600 // 60:02d}m"
   else:
      text = f"{sec // 86400}d"
   return ("≥" if lower_bound else "") + text


def fmt_age(ts: datetime | None, now: datetime) -> str:
   """How long before `now` `ts` was; a timestamp ahead of `now` (clock skew) reads 0s."""
   if ts is None:
      return "-"
   return fmt_duration((now - ts).total_seconds())


def fmt_pct(x) -> str:
   return "-" if x is None else f"{x:.1f}%"
