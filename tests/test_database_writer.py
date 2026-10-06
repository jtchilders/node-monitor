"""Unit contracts for the compact transactional PostgreSQL writer."""

from contextlib import contextmanager
from datetime import datetime, timezone

import pytest

from node_monitor.output.contracts import ContractError
from node_monitor.database.writer import (
   DatabaseWriteError, DatabaseWriter, _ALLOWLISTED_SQLSTATE,
)


NOW = datetime(2026, 9, 30, 14, 30, tzinfo=timezone.utc)


class _Result:
   rowcount = 1


class _Connection:
   def __init__(self):
      self.calls = []

   def execute(self, statement, parameters):
      self.calls.append((str(statement), parameters))
      return _Result()


# Driver-level exceptions that carry recognized SQLSTATE interfaces.
class _SqlStateException(Exception):
   def __init__(self, sqlstate, message=""):
      super().__init__(message)
      self.sqlstate = sqlstate


class _PgCodeException(Exception):
   def __init__(self, pgcode, message=""):
      super().__init__(message)
      self.pgcode = pgcode


class _DB:
   def __init__(self, failure=None, failures_sequence=None):
      self.connection = _Connection()
      self.begin_count = 0
      self.failure = failure
      self.failures_sequence = failures_sequence or []
      self._fail_index = 0

   @contextmanager
   def begin(self):
      self.begin_count += 1
      if self.failures_sequence:
         if self._fail_index < len(self.failures_sequence):
            exc = self.failures_sequence[self._fail_index]
            self._fail_index += 1
            raise exc
         # No more failures -> succeed.
      elif self.failure is not None:
         raise self.failure
      yield self.connection


def _hardware(**overrides):
   record = {
      "system": "polaris", "source_hostname": "login-04.example.org",
      "first_seen_utc": "2026-09-29T12:00:00Z", "probe_version": 4,
      "boot_id": "boot-a", "btime": 100, "cpu_model": "Zen",
      "cpu_logical": 64, "sockets": 2, "cores_per_socket": 16,
      "cpu_max_freq_khz": 3500000, "numa_nodes": 4,
      "mem_total_kb": 1000000, "swap_total_kb": 0,
      "hugepage_size_kb": 2048, "kernel_release": "6.1",
      "os_pretty_name": "Linux", "net_fs_mounts": 2,
      "net_ifaces": {"hsn0": {"speed_mbps": 100000}},
      "gpus": [{"model": "GPU"}],
   }
   record.update(overrides)
   return record


def _counter(**overrides):
   record = {
      "system": "polaris", "source_hostname": "login-04.example.org",
      "collector_hostname": "login-04.example.org", "probe_version": 4,
      "daemon_version": "0.2.0",
      "window_start_utc": "2026-09-30T14:00:00Z",
      "window_end_utc": "2026-09-30T14:01:00Z",
      "sample_count": 6, "expected_count": 6, "coverage": 1.0,
      "end_of_window": {
         "mem_available_kb": 900, "cached_kb": 100, "shmem_kb": 10,
         "load1": 1.0, "load5": 2.0, "load15": 3.0,
         "procs_running": 2, "procs_total": 100, "socket_count": 8,
      },
      "rates": {
         "cpu_busy_pct": {"p50": 10.0, "p95": 20.0, "max": 25.0},
         "network": {
            "hsn0": {"rx_bytes_per_sec": {"p50": 1, "p95": 2, "max": 3},
                     "tx_bytes_per_sec": {"p50": 4, "p95": 5, "max": 6}},
         },
         "lustre_md_ops": {
            "fs-MDT0000": {
               "getattr": {"p50": 1.0, "p95": 2.0, "max": 3.0},
               "open": {"p50": 4.0, "p95": 5.0, "max": 6.0},
            },
            "fs-MDT0001": {
               "getattr": {"p50": 10.0, "p95": 20.0, "max": 30.0},
            },
         },
      },
      "audit": {
         "meets_minimum_samples": True,
         "invalid_pairs": [{"reason": "reset"}, {"reason": "reboot"}],
         "excess_sample_count": 1,
         "raw_cumulative": {"window_first": {"secret": 1},
                            "window_last": {"secret": 2}},
      },
   }
   record.update(overrides)
   return record


def _usage(username="alice", **overrides):
   record = {
      "system": "polaris", "source_hostname": "login-04.example.org",
      "interval_start_utc": "2026-09-30T14:00:00Z",
      "interval_end_utc": "2026-09-30T14:01:00Z",
      "category": "ai_agent", "activity": "claude", "username": username,
      "process_count": {"p50": 1, "p95": 2, "max": 2},
      "cpu_seconds": 1.5, "rss_kb": {"p50": 10, "p95": 20, "max": 20},
      "d_state_fraction": 0.0, "interactivity_fraction": 1.0,
      "sample_count": 6, "expected_count": 6, "unmeasured_count": 0,
   }
   record.update(overrides)
   return record


def _poll_failure(**overrides):
   record = {
      "system": "polaris", "source_hostname": "login-04.example.org",
      "loop": "counter", "timestamp_utc": "2026-09-30T14:00:01Z",
      "failure_type": "timeout", "detail": "probe timed out",
      "consecutive_failures": 1, "breaker_state": "closed",
   }
   record.update(overrides)
   return record


def _collection_log(**overrides):
   record = {
      "system": "polaris", "timestamp_utc": "2026-09-30T14:00:02Z",
      "event": "daemon_started", "detail": {"version": "0.2.0"},
   }
   record.update(overrides)
   return record


def _writer(db=None, retry_policy=None, sleeper=None):
   db = db or _DB()
   return DatabaseWriter(
      db, clock=lambda: NOW, retry_policy=retry_policy or {},
      sleeper=sleeper,
   ), db


def _single_call(writer, db, record_type, record):
   writer.write_record(record_type, record)
   assert db.begin_count == 1
   assert len(db.connection.calls) == 1
   return db.connection.calls[0]


# ------------------------------------------------------------------
# Existing adapter/contract tests (unchanged behavior preserved)
# ------------------------------------------------------------------

def test_hardware_adapter_preserves_first_seen_and_known_boot_identity():
   writer, db = _writer()
   sql, params = _single_call(writer, db, "node_hardware", _hardware())
   assert "INSERT INTO node_monitor.node_hardware" in sql
   assert "ON CONFLICT (system, source_hostname) DO UPDATE" in sql
   assert "first_seen =" not in sql.split("DO UPDATE", 1)[1]
   assert "boot_id = COALESCE(EXCLUDED.boot_id, current.boot_id)" in sql
   assert "btime = COALESCE(EXCLUDED.btime, current.btime)" in sql
   # Requirement 6: numa_nodes = EXCLUDED.numa_nodes preserved exactly.
   assert "numa_nodes = EXCLUDED.numa_nodes" in sql
   assert params["first_seen"] == datetime(2026, 9, 29, 12, 0, tzinfo=timezone.utc)
   assert params["last_verified"] == NOW
   assert params["net_ifaces"] == _hardware()["net_ifaces"]
   assert params["gpus"] == _hardware()["gpus"]


def test_counter_adapter_compacts_audit_and_lustre_independent_of_target_names():
   writer, db = _writer()
   sql, params = _single_call(writer, db, "node_counter_samples", _counter())
   assert "INSERT INTO node_monitor.node_counter_minute" in sql
   assert "ON CONFLICT (system, source_hostname, window_start) DO UPDATE" in sql
   assert params["invalid_pair_count"] == 2
   assert params["excess_sample_count"] == 1
   assert "raw_cumulative" not in params
   assert "invalid_pairs" not in params
   assert params["network_rates"] == _counter()["rates"]["network"]
   assert params["lustre_md_summary"] == {
      "getattr": {"p50_sum": 11.0, "p95_sum": 22.0,
                  "max_sum": 33.0, "target_count": 2},
      "open": {"p50_sum": 4.0, "p95_sum": 5.0,
               "max_sum": 6.0, "target_count": 1},
   }
   assert "fs-MDT0000" not in repr(params["lustre_md_summary"])
   assert params["window_start"] == datetime(
      2026, 9, 30, 14, 0, tzinfo=timezone.utc)


def test_usage_adapter_never_supplies_identity_or_generated_username_key():
   writer, db = _writer()
   sql, params = _single_call(writer, db, "node_usage_intervals", _usage(None))
   assert "INSERT INTO node_monitor.node_usage_intervals" in sql
   assert "ON CONFLICT (system, source_hostname, interval_start, category, activity, username_key)" in sql
   assert "username_key" not in params
   assert "id" not in params
   assert params["username"] is None


@pytest.mark.parametrize(
   "record_type,record,table",
   [("node_poll_failures", _poll_failure(), "node_poll_failures"),
    ("node_collection_log", _collection_log(), "node_collection_log")],
)
def test_event_records_append_without_conflict_clause(record_type, record, table):
   writer, db = _writer()
   sql, params = _single_call(writer, db, record_type, record)
   assert "INSERT INTO node_monitor.%s" % table in sql
   assert "ON CONFLICT" not in sql
   assert params["recorded_at"].tzinfo == timezone.utc
   if record_type == "node_poll_failures":
      assert params["detail"] == "probe timed out"
   else:
      assert params["detail"] == {"version": "0.2.0"}
      assert "source_hostname" not in params


def test_contract_validation_occurs_before_transaction_or_sql():
   writer, db = _writer()
   with pytest.raises(ContractError):
      writer.write_record("node_hardware", _hardware(unexpected=True))
   assert db.begin_count == 0
   assert db.connection.calls == []


@pytest.mark.parametrize("record_type", ["diagnostic_census", "unknown"])
def test_unsupported_and_diagnostic_records_fail_before_transaction(record_type):
   writer, db = _writer()
   with pytest.raises(DatabaseWriteError, match="unsupported record type"):
      writer.write_record(record_type, {})
   assert db.begin_count == 0


@pytest.mark.parametrize(
   "timestamp",
   ["2026-09-30T14:00:00", "2026-09-30T10:00:00-04:00", "not-a-time"],
)
def test_timestamps_must_be_explicit_utc(timestamp):
   writer, db = _writer()
   with pytest.raises(DatabaseWriteError, match="invalid UTC timestamp"):
      writer.write_record(
         "node_collection_log", _collection_log(timestamp_utc=timestamp))
   assert db.begin_count == 0


def test_batch_validates_all_records_then_uses_one_transaction():
   writer, db = _writer()
   writer.write_records([
      ("node_poll_failures", _poll_failure()),
      ("node_collection_log", _collection_log()),
   ])
   assert db.begin_count == 1
   assert len(db.connection.calls) == 2


def test_batch_validation_failure_executes_nothing():
   writer, db = _writer()
   with pytest.raises(ContractError):
      writer.write_records([
         ("node_collection_log", _collection_log()),
         ("node_hardware", _hardware(unexpected=True)),
      ])
   assert db.begin_count == 0
   assert db.connection.calls == []


def test_database_failure_is_sanitized_and_chainless():
   secret = "DATABASE_PASSWORD_MUST_NOT_LEAK"
   writer, db = _writer(_DB(RuntimeError("driver failure: " + secret)))
   with pytest.raises(DatabaseWriteError, match="database write failed") as caught:
      writer.write_record("node_collection_log", _collection_log())
   assert secret not in str(caught.value)
   assert caught.value.__cause__ is None


def test_malformed_nested_record_conversion_is_sanitized_before_transaction():
   writer, db = _writer()
   malformed = _counter()
   malformed["audit"] = {}
   with pytest.raises(DatabaseWriteError, match="record conversion failed") as caught:
      writer.write_record("node_counter_samples", malformed)
   assert caught.value.__cause__ is None
   assert db.begin_count == 0
   assert db.connection.calls == []


# ------------------------------------------------------------------
# Retry behavior: SQLSTATE extraction, retries, exhaustion, replay
# ------------------------------------------------------------------

@pytest.mark.parametrize("sqlstate", list(_ALLOWLISTED_SQLSTATE))
def test_all_four_allowlisted_sqlstates_retry_then_recover(sqlstate):
   """Each allowlisted SQLSTATE retries once then commits; full batch replayed."""
   sleeps = []
   def fake_sleeper(delay):
      sleeps.append(delay)
   # First attempt fails with allowlisted SQLSTATE; second succeeds.
   writer, db = _writer(
      retry_policy={"max_attempts": 5, "initial_delay": 1.0, "max_delay": 8.0},
      sleeper=fake_sleeper,
   )
   db.failures_sequence = [_SqlStateException(sqlstate), None]
   # Actually _DB.begin() raises exception objects; None is not an exception.
   # Replace with a sequence of exceptions. Success is represented by empty sequence tail.
   # Instead: first call raises, second yields.
   db.failures_sequence = [_SqlStateException(sqlstate)]
   # After consuming the sequence, begin succeeds (no exception raised).
   # But our _DB logic: if sequence non-empty, pop and raise. After that sequence empty -> yield.
   writer.write_record("node_collection_log", _collection_log())
   # 2 attempts: first failed, second succeeded.
   assert db.begin_count == 2
   assert sleeps == [1.0]
   assert len(db.connection.calls) == 1  # only on success


def test_retry_log_contains_only_allowed_fields(caplog):
   import logging
   sleeps = []
   def fake_sleeper(delay):
      sleeps.append(delay)
   writer, db = _writer(
      retry_policy={"max_attempts": 5, "initial_delay": 1.0, "max_delay": 8.0},
      sleeper=fake_sleeper,
   )
   db.failures_sequence = [_SqlStateException("57014")]
   with caplog.at_level(logging.WARNING, logger="node_monitor.database.writer"):
      writer.write_record("node_collection_log", _collection_log())
   # There should be a retry warning with fixed-template fields only.
   retry_msgs = [r for r in caplog.records if r.levelname == "WARNING"]
   assert len(retry_msgs) >= 1
   msg = retry_msgs[0].message
   # Assert allowed fields present; no raw exception text/SQL/params/secrets.
   assert "event=write_retry" in msg
   assert "sqlstate=57014" in msg
   assert "failed_attempt=1" in msg
   assert "batch_size=1" in msg
   assert "record_types=node_collection_log" in msg
   # Assert forbidden content absent from the log message.
   assert "secret" not in msg.lower()
   assert "SELECT" not in msg
   assert "postgresql://" not in msg


def test_recovery_log_on_subsequent_success(caplog):
   import logging
   sleeps = []
   def fake_sleeper(delay):
      sleeps.append(delay)
   writer, db = _writer(
      retry_policy={"max_attempts": 5, "initial_delay": 1.0, "max_delay": 8.0},
      sleeper=fake_sleeper,
   )
   db.failures_sequence = [_SqlStateException("40001")]
   with caplog.at_level(logging.WARNING, logger="node_monitor.database.writer"):
      writer.write_record("node_collection_log", _collection_log())
   messages = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
   # One retry warning, then one recovery warning.
   assert any("event=write_retry" in m for m in messages)
   assert any("event=write_recovered" in m for m in messages)


def test_exact_delays_1_2_4_8_for_five_total_attempts():
   sleeps = []
   def fake_sleeper(delay):
      sleeps.append(delay)
   writer, db = _writer(
      retry_policy={"max_attempts": 5, "initial_delay": 1.0, "max_delay": 8.0},
      sleeper=fake_sleeper,
   )
   # 4 failures then success on 5th attempt -> 4 delays.
   db.failures_sequence = [
      _SqlStateException("57014"),
      _SqlStateException("55P03"),
      _SqlStateException("40001"),
      _SqlStateException("40P01"),
   ]
   writer.write_record("node_collection_log", _collection_log())
   assert sleeps == [1.0, 2.0, 4.0, 8.0]


def test_exhaustion_five_attempts_ends_with_database_write_error():
   sleeps = []
   def fake_sleeper(delay):
      sleeps.append(delay)
   writer, db = _writer(
      retry_policy={"max_attempts": 5, "initial_delay": 1.0, "max_delay": 8.0},
      sleeper=fake_sleeper,
   )
   # Always fail with allowlisted SQLSTATE; 5 attempts -> 4 sleeps then failure.
   db.failures_sequence = [
      _SqlStateException("57014"),
      _SqlStateException("57014"),
      _SqlStateException("57014"),
      _SqlStateException("57014"),
      _SqlStateException("57014"),
   ]
   with pytest.raises(DatabaseWriteError, match="database write failed"):
      writer.write_record("node_collection_log", _collection_log())
   assert db.begin_count == 5
   assert sleeps == [1.0, 2.0, 4.0, 8.0]


# ------------------------------------------------------------------
# Permanent / missing / non-allowlisted SQLSTATE failures (no retry)
# ------------------------------------------------------------------

def test_permanent_non_allowlisted_sqlstate_fails_immediately():
   writer, db = _writer()
   db.failure = _SqlStateException("08006")  # connection failure
   with pytest.raises(DatabaseWriteError, match="database write failed"):
      writer.write_record("node_collection_log", _collection_log())
   assert db.begin_count == 1  # only first attempt


def test_missing_sqlstate_fails_immediately():
   writer, db = _writer()
   db.failure = RuntimeError("some random failure without sqlstate")
   with pytest.raises(DatabaseWriteError, match="database write failed"):
      writer.write_record("node_collection_log", _collection_log())
   assert db.begin_count == 1


def test_08xxx_never_retried():
   writer, db = _writer()
   db.failure = _SqlStateException("08S01")  # connection-class failure
   with pytest.raises(DatabaseWriteError, match="database write failed"):
      writer.write_record("node_collection_log", _collection_log())
   assert db.begin_count == 1


# ------------------------------------------------------------------
# Whole-batch replay / rollback behavior in doubles
# ------------------------------------------------------------------

def test_whole_batch_replay_rollback_no_partial_commit():
   sleeps = []
   def fake_sleeper(delay):
      sleeps.append(delay)
   writer, db = _writer(
      retry_policy={"max_attempts": 3, "initial_delay": 1.0, "max_delay": 4.0},
      sleeper=fake_sleeper,
   )
   # First attempt fails; second succeeds. All records replayed.
   db.failures_sequence = [_SqlStateException("57014")]
   writer.write_records([
      ("node_collection_log", _collection_log()),
      ("node_poll_failures", _poll_failure()),
   ])
   assert db.begin_count == 2
   # On success, both statements executed in one transaction (second attempt).
   assert len(db.connection.calls) == 2


def test_retry_never_includes_raw_exception_text(caplog):
   secret = "SECRET_VALUE_MUST_NOT_LOG"
   sleeps = []
   def fake_sleeper(delay):
      sleeps.append(delay)
   writer, db = _writer(
      retry_policy={"max_attempts": 2, "initial_delay": 1.0, "max_delay": 8.0},
      sleeper=fake_sleeper,
   )
   exc = _SqlStateException("40001", "internal detail with %s" % secret)
   db.failures_sequence = [exc]
   import logging
   with caplog.at_level(logging.WARNING, logger="node_monitor.database.writer"):
      writer.write_record("node_collection_log", _collection_log())
   for record in caplog.records:
      msg = record.getMessage()
      assert secret not in msg


# ------------------------------------------------------------------
# SQLSTATE extraction from recognized interfaces only
# ------------------------------------------------------------------

def test_sqlstate_extracted_from_orig_sqlstate():
   writer, db = _writer(
      retry_policy={"max_attempts": 2, "initial_delay": 1.0, "max_delay": 8.0},
      sleeper=lambda d: None,
   )
   db.failures_sequence = [_SqlStateException("40P01")]
   writer.write_record("node_collection_log", _collection_log())
   assert db.begin_count == 2


def test_sqlstate_extracted_from_chained_exception():
   writer, db = _writer(
      retry_policy={"max_attempts": 2, "initial_delay": 1.0, "max_delay": 8.0},
      sleeper=lambda d: None,
   )
   inner = _SqlStateException("55P03")
   class Chained(Exception):
      pass
   # Manually build exception with cause.
   chained = Chained("wrapper")
   chained.__cause__ = inner
   db.failures_sequence = [chained]
   writer.write_record("node_collection_log", _collection_log())
   assert db.begin_count == 2


def test_sqlstate_never_inferred_from_string_args():
   writer, db = _writer(
      retry_policy={"max_attempts": 2, "initial_delay": 1.0, "max_delay": 8.0},
      sleeper=lambda d: None,
   )
   # Exception with a 5-char code embedded in message but no sqlstate attr.
   db.failure = RuntimeError("error 57014 happened")
   with pytest.raises(DatabaseWriteError, match="database write failed"):
      writer.write_record("node_collection_log", _collection_log())
   assert db.begin_count == 1


def test_public_database_write_error_remains_fixed_bounded_chainless():
   writer, db = _writer(_DB(RuntimeError("anything")))
   with pytest.raises(DatabaseWriteError, match="database write failed") as caught:
      writer.write_record("node_collection_log", _collection_log())
   assert str(caught.value) == "database write failed"
   assert caught.value.__cause__ is None


def test_log_never_contains_sql_parameters_or_record_values(caplog):
   import logging
   sleeps = []
   def fake_sleeper(delay):
      sleeps.append(delay)
   writer, db = _writer(
      retry_policy={"max_attempts": 2, "initial_delay": 1.0, "max_delay": 8.0},
      sleeper=fake_sleeper,
   )
   db.failures_sequence = [_SqlStateException("57014")]
   with caplog.at_level(logging.WARNING, logger="node_monitor.database.writer"):
      writer.write_record("node_collection_log", _collection_log(detail={"secret": 42}))
   for record in caplog.records:
      msg = record.getMessage()
      assert "secret" not in msg
      assert "42" not in msg
