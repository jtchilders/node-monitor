"""`node-monitor reclassify` — re-run classify+tags_for over history tables."""

from __future__ import annotations

from collections import Counter

import click
import psycopg
from psycopg.rows import dict_row

from node_monitor.config import ConfigError, load_config
from node_monitor.database.connect import DatabaseUnavailable, connect
from node_monitor.pipeline.classify import classify
from node_monitor.pipeline.tags import tags_for

_BATCH = 1000


def _load(config_path):
   try:
      return load_config(config_path)
   except ConfigError as exc:
      raise click.ClickException(str(exc)) from exc


@click.command("reclassify")
@click.option(
   "--dry-run", is_flag=True, default=False, help="Report changes without writing (default)."
)
@click.option("--apply", "apply_", is_flag=True, help="Write changes to the database.")
@click.option("--config", "config_path", type=click.Path(), default=None)
def reclassify(dry_run, apply_, config_path) -> None:
   """Re-classify process_samples and command_lines with current rules."""
   if apply_ and dry_run:
      raise click.ClickException("use only one of --dry-run or --apply")
   do_write = bool(apply_)
   # default is dry-run

   config = _load(config_path)
   url = config.database.url
   if not url:
      raise click.ClickException(f"database.url is not set in {config.source_path}")

   min_uid = config.classification.min_user_uid

   def _classify_row(row: dict) -> tuple[str | None, str | None, list[str]]:
      comm = row.get("comm") or ""
      cmdline = row.get("cmdline") or ""
      uid = row.get("uid")
      # command_lines may not have uid; treat as user
      if uid is None:
         uid = min_uid
      cls = classify(comm, cmdline, int(uid), min_uid)
      argv = cmdline.split() if cmdline else []
      tags = tags_for(cls.activity, argv)
      return cls.category, cls.activity, tags

   changed = 0
   by_category: Counter = Counter()

   # Collect updates to apply
   sample_updates: list[tuple] = []  # (category, activity, tags, system, node, ts, pid, start_time)
   cmdline_updates: list[tuple] = []  # (category, activity, tags, system, username, cmdline_hash)

   def run():
      nonlocal changed
      with connect(
         url, connect_timeout_sec=config.database.connect_timeout_sec, autocommit=True
      ) as conn:
         # Use server-side cursor for batched iteration
         with conn.cursor(row_factory=dict_row) as cur:
            # Reclassify process_samples
            cur.execute(
               "SELECT system, node, ts, pid, start_time, comm, cmdline, uid,"
               " category, activity, tags FROM process_samples"
            )
            while True:
               batch = cur.fetchmany(_BATCH)
               if not batch:
                  break
               for row in batch:
                  new_cat, new_act, new_tags = _classify_row(row)
                  old_cat, old_act, old_tags = (
                     row.get("category"),
                     row.get("activity"),
                     row.get("tags") or [],
                  )
                  if (
                     new_cat != old_cat
                     or new_act != old_act
                     or sorted(new_tags) != sorted(old_tags or [])
                  ):
                     changed += 1
                     # delta by new category
                     by_category[new_cat or "other"] += 1
                     if new_cat != old_cat:
                        by_category[f"{old_cat or 'other'}->{new_cat or 'other'}"] += 1
                     sample_updates.append(
                        (
                           new_cat,
                           new_act,
                           new_tags,
                           row["system"],
                           row["node"],
                           row["ts"],
                           row["pid"],
                           row["start_time"],
                        )
                     )
            # Reclassify command_lines
            cur.execute(
               "SELECT system, username, cmdline_hash, cmdline, exe, category, activity, tags"
               " FROM command_lines"
            )
            while True:
               batch = cur.fetchmany(_BATCH)
               if not batch:
                  break
               for row in batch:
                  # For command_lines, use stored exe/comm? Use cmdline only
                  comm = row.get("exe") or ""
                  cmdline = row.get("cmdline") or ""
                  # command_lines rows have no uid; use min_uid so uid check doesn't force system
                  cls = classify(comm, cmdline, min_uid, min_uid)
                  argv = cmdline.split() if cmdline else []
                  new_tags = tags_for(cls.activity, argv)
                  new_cat, new_act = cls.category, cls.activity
                  old_cat, old_act, old_tags = (
                     row.get("category"),
                     row.get("activity"),
                     row.get("tags") or [],
                  )
                  if (
                     new_cat != old_cat
                     or new_act != old_act
                     or sorted(new_tags) != sorted(old_tags or [])
                  ):
                     changed += 1
                     by_category[new_cat or "other"] += 1
                     if new_cat != old_cat:
                        by_category[f"{old_cat or 'other'}->{new_cat or 'other'}"] += 1
                     cmdline_updates.append(
                        (
                           new_cat,
                           new_act,
                           new_tags,
                           row["system"],
                           row["username"],
                           row["cmdline_hash"],
                        )
                     )

         if do_write and (sample_updates or cmdline_updates):
            with conn.transaction():
               with conn.cursor() as wcur:
                  for cat, act, tags, system, node, ts, pid, start_time in sample_updates:
                     wcur.execute(
                        "UPDATE process_samples SET category=%s, activity=%s, tags=%s"
                        " WHERE system=%s AND node=%s AND ts=%s AND pid=%s AND start_time=%s",
                        (cat, act, tags, system, node, ts, pid, start_time),
                     )
                  for cat, act, tags, system, username, h in cmdline_updates:
                     wcur.execute(
                        "UPDATE command_lines SET category=%s, activity=%s, tags=%s"
                        " WHERE system=%s AND username=%s AND cmdline_hash=%s",
                        (cat, act, tags, system, username, h),
                     )

   try:
      run()
   except (psycopg.Error, DatabaseUnavailable) as exc:
      reason = (str(exc).strip().splitlines() or [type(exc).__name__])[0]
      raise click.ClickException(f"database error: {reason}") from exc

   # Report
   by_cat_str = (
      ", ".join(f"{k}={v}" for k, v in sorted(by_category.items())) if by_category else "none"
   )
   mode = "would change" if not do_write else "changed"
   click.echo(f"changed: {changed} ({mode})")
   click.echo(f"by_category: {by_cat_str}")
   if not do_write:
      click.echo("dry-run: no writes performed (use --apply to write)")
   else:
      click.echo("applied in one transaction")
