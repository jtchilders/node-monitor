"""node-monitor command line."""

import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import click

from node_monitor.collector.soak import run_soak, summarize
from node_monitor.config import ConfigError, load_config

COLUMNS = (
   "node",
   "hours",
   "coverage%",
   "max_gap_s",
   "sessions",
   "losses",
   "rotations",
   "reboots",
   "cpu_s/h",
   "maxrss_MB",
   "scan_p95_s",
   "procs",
)


@click.group()
def cli() -> None:
   """Login-node monitor."""


@cli.group()
def soak() -> None:
   """M0 soak: record probe streams and report on them."""


@soak.command("run")
@click.option("--config", "config_path", type=click.Path(), default=None)
@click.option("--duration-hours", type=float, default=None, help="Run forever if omitted.")
@click.option("--full-census", is_flag=True, help="Record every process in each census.")
@click.option("--out-dir", type=click.Path(path_type=Path), default=None)
def soak_run(config_path, duration_hours, full_census, out_dir) -> None:
   try:
      config = load_config(config_path)
   except ConfigError as exc:
      raise click.ClickException(str(exc)) from exc
   if out_dir is None:
      stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
      out_dir = config.working_dir / "logs" / "soak" / stamp
   out_dir.mkdir(parents=True, exist_ok=True)
   click.echo(f"recording to {out_dir}")
   duration = None if duration_hours is None else duration_hours * 3600
   sys.exit(asyncio.run(run_soak(config, out_dir, duration, full_census=full_census)))


def _fmt(value, spec: str) -> str:
   return "-" if value is None else format(value, spec)


def _row(label: str, s: dict) -> list[str]:
   losses = sum(s["losses_by_reason"].values())
   return [
      label,
      _fmt(s["hours"], ".2f"),
      _fmt(s["coverage_pct"], ".1f"),
      _fmt(s["max_gap_sec"], ".0f"),
      str(s["sessions_started"]),
      str(losses),
      str(s["rotations"]),
      str(s["reboots"]),
      _fmt(s["probe_cpu_sec_per_hour"], ".1f"),
      _fmt(s["probe_maxrss_kb"] / 1024, ".1f"),
      _fmt(s["census_scan_p95"], ".2f"),
      _fmt(s["process_count_avg"], ".0f"),
   ]


@soak.command("report")
@click.argument("directory", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option("--format", "fmt", type=click.Choice(["table", "json"]), default="table")
def soak_report(directory, fmt) -> None:
   try:
      result = summarize(directory)
   except (OSError, ValueError, KeyError) as exc:
      raise click.ClickException(f"cannot summarize {directory}: {exc}") from exc
   if fmt == "json":
      click.echo(json.dumps(result, indent=2))
      return
   rows = [list(COLUMNS)] + [_row(label, s) for label, s in result.items()]
   widths = [max(len(r[i]) for r in rows) for i in range(len(COLUMNS))]
   for r in rows:
      click.echo(
         "  ".join(
            c.rjust(w) if i else c.ljust(w) for i, (c, w) in enumerate(zip(r, widths, strict=True))
         )
      )
