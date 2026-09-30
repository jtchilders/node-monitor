"""Real-PostgreSQL acceptance tests for the compact database writer."""

import os
from datetime import datetime, timezone

import pytest

from node_monitor.database.migration import MigrationRunner
from node_monitor.database.writer import DatabaseWriter
from tests.test_database_migration_postgres import postgres_engine
from tests.test_database_writer import (
   NOW,
   _collection_log,
   _counter,
   _hardware,
   _poll_failure,
   _usage,
)


pytestmark = pytest.mark.skipif(
   not os.environ.get("NODE_MONITOR_TEST_DATABASE_URL"),
   reason="NODE_MONITOR_TEST_DATABASE_URL is required",
)


class _InjectedDB:
   def __init__(self, engine):
      self._engine = engine

   def begin(self):
      return self._engine.begin()


def _writer(engine):
   MigrationRunner(engine, "test").migrate()
   return DatabaseWriter(_InjectedDB(engine), clock=lambda: NOW)


def test_hardware_retry_preserves_first_seen_and_known_boot_values(postgres_engine):
   writer = _writer(postgres_engine)
   writer.write_record("node_hardware", _hardware())
   writer.write_record(
      "node_hardware",
      _hardware(first_seen_utc="2026-09-30T13:00:00Z",
                boot_id=None, btime=None, cpu_model="Zen 2"),
   )
   with postgres_engine.connect() as connection:
      row = connection.exec_driver_sql(
         "SELECT first_seen, last_verified, boot_id, btime, cpu_model, "
         "net_ifaces, gpus FROM node_monitor.node_hardware"
      ).mappings().one()
   assert row["first_seen"].astimezone(timezone.utc) == datetime(
      2026, 9, 29, 12, 0, tzinfo=timezone.utc)
   assert row["last_verified"].astimezone(timezone.utc) == NOW
   assert row["boot_id"] == "boot-a"
   assert row["btime"] == 100
   assert row["cpu_model"] == "Zen 2"
   assert row["net_ifaces"] == _hardware()["net_ifaces"]
   assert row["gpus"] == _hardware()["gpus"]


def test_counter_retry_is_one_compact_row_without_raw_diagnostics(postgres_engine):
   writer = _writer(postgres_engine)
   writer.write_record("node_counter_samples", _counter())
   writer.write_record("node_counter_samples", _counter(coverage=0.5))
   with postgres_engine.connect() as connection:
      row = connection.exec_driver_sql(
         "SELECT coverage, invalid_pair_count, lustre_md_summary, "
         "network_rates FROM node_monitor.node_counter_minute"
      ).mappings().one()
      columns = set(connection.exec_driver_sql("""
         SELECT column_name FROM information_schema.columns
         WHERE table_schema = 'node_monitor'
           AND table_name = 'node_counter_minute'
      """).scalars())
   assert row["coverage"] == 0.5
   assert row["invalid_pair_count"] == 2
   assert row["lustre_md_summary"]["getattr"]["target_count"] == 2
   assert "raw_cumulative" not in columns
   assert "invalid_pairs" not in columns


@pytest.mark.parametrize("username", ["alice", None])
def test_usage_retry_is_idempotent_for_named_and_null_usernames(postgres_engine, username):
   writer = _writer(postgres_engine)
   writer.write_record("node_usage_intervals", _usage(username))
   writer.write_record("node_usage_intervals", _usage(username, cpu_seconds=2.5))
   with postgres_engine.connect() as connection:
      rows = connection.exec_driver_sql(
         "SELECT username, username_key, cpu_seconds, process_count, rss_kb "
         "FROM node_monitor.node_usage_intervals"
      ).mappings().all()
   assert len(rows) == 1
   assert rows[0]["username"] == username
   assert rows[0]["username_key"] == (username or "")
   assert rows[0]["cpu_seconds"] == 2.5
   assert rows[0]["process_count"] == _usage(username)["process_count"]


def test_event_records_append_and_round_trip_json(postgres_engine):
   writer = _writer(postgres_engine)
   writer.write_records([
      ("node_poll_failures", _poll_failure()),
      ("node_poll_failures", _poll_failure()),
      ("node_collection_log", _collection_log()),
      ("node_collection_log", _collection_log()),
   ])
   with postgres_engine.connect() as connection:
      poll_rows = connection.exec_driver_sql(
         "SELECT detail FROM node_monitor.node_poll_failures ORDER BY id"
      ).scalars().all()
      log_rows = connection.exec_driver_sql(
         "SELECT detail FROM node_monitor.node_collection_log ORDER BY id"
      ).scalars().all()
   assert poll_rows == ["probe timed out", "probe timed out"]
   assert log_rows == [{"version": "0.2.0"}, {"version": "0.2.0"}]
