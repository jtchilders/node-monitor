"""Tests for the hwinfo refresh policy and upsert helper.

The daemon side is not built yet (see the hwinfo task write-up), so these
exercise `needs_refresh` and `upsert_hardware` against a fake connection --
no real Postgres required.
"""

import datetime

import pytest

from node_monitor.collector import hardware


UTC = datetime.timezone.utc


def _dt(days_ago=0):
   return datetime.datetime.now(UTC) - datetime.timedelta(days=days_ago)


class _FakeConn:
   """Records executed statements instead of touching a real database."""

   def __init__(self):
      self.calls = []

   def execute(self, stmt, params):
      self.calls.append((stmt, params))


# --------------------------------------------------------------------------
# needs_refresh -- the four branches from the task write-up
# --------------------------------------------------------------------------

class TestNeedsRefresh:
   def test_missing_row_always_refreshes(self):
      assert hardware.needs_refresh(
         stored_row=None, current_boot_id="boot-a") is True

   def test_changed_boot_id_refreshes(self):
      stored = {"boot_id": "boot-old", "last_verified": _dt(days_ago=0)}
      assert hardware.needs_refresh(
         stored_row=stored, current_boot_id="boot-new") is True

   def test_stale_last_verified_refreshes(self):
      stored = {"boot_id": "boot-a", "last_verified": _dt(days_ago=31)}
      assert hardware.needs_refresh(
         stored_row=stored, current_boot_id="boot-a",
         hardware_refresh_days=30) is True

   def test_force_flag_refreshes_even_when_fresh(self):
      stored = {"boot_id": "boot-a", "last_verified": _dt(days_ago=0)}
      assert hardware.needs_refresh(
         stored_row=stored, current_boot_id="boot-a", force=True) is True

   def test_fresh_row_same_boot_no_refresh(self):
      stored = {"boot_id": "boot-a", "last_verified": _dt(days_ago=1)}
      assert hardware.needs_refresh(
         stored_row=stored, current_boot_id="boot-a",
         hardware_refresh_days=30) is False

   def test_boundary_exactly_at_refresh_days_not_yet_stale(self):
      now = _dt(days_ago=0)
      stored = {
         "boot_id": "boot-a",
         "last_verified": now - datetime.timedelta(days=30),
      }
      assert hardware.needs_refresh(
         stored_row=stored, current_boot_id="boot-a",
         hardware_refresh_days=30, now=now) is False

   def test_malformed_row_missing_last_verified_refreshes(self):
      stored = {"boot_id": "boot-a", "last_verified": None}
      assert hardware.needs_refresh(
         stored_row=stored, current_boot_id="boot-a") is True


# --------------------------------------------------------------------------
# detect_hardware_changes
# --------------------------------------------------------------------------

class TestDetectHardwareChanges:
   def test_no_stored_row_is_not_a_change(self):
      hw = {"mem_total_kb": 527954112}
      assert hardware.detect_hardware_changes(None, hw) == {}

   def test_changed_fact_column_detected(self):
      stored = {"mem_total_kb": 527954112, "cpu_model": "AMD EPYC 7713"}
      hw = {"mem_total_kb": 263977056, "cpu_model": "AMD EPYC 7713"}
      changes = hardware.detect_hardware_changes(stored, hw)
      assert changes == {"mem_total_kb": (527954112, 263977056)}

   def test_boot_id_change_alone_is_not_a_hardware_change(self):
      """boot_id/btime are expected to move on every reboot; they are not
      in _HARDWARE_FACT_COLUMNS and must not show up as 'changes'."""
      stored = {"boot_id": "old", "btime": 100, "mem_total_kb": 1}
      hw = {"boot_id": "new", "btime": 200, "mem_total_kb": 1}
      assert hardware.detect_hardware_changes(stored, hw) == {}


# --------------------------------------------------------------------------
# upsert_hardware
# --------------------------------------------------------------------------

class TestUpsertHardware:
   def _hw(self, **overrides):
      base = {
         "cpu_model": "AMD EPYC 7713 64-Core Processor",
         "cpu_logical": 256,
         "sockets": 2,
         "cores_per_socket": 64,
         "cpu_max_freq_khz": 2000000,
         "numa_nodes": 2,
         "mem_total_kb": 527954112,
         "swap_total_kb": 0,
         "hugepage_size_kb": 2048,
         "kernel_release": "6.4.0-150700.53.73-default",
         "os_pretty_name": "SUSE Linux Enterprise Server 15 SP7",
         "net_fs_mounts": 7,
         "net_ifaces": {"bond0": 1000},
         "boot_id": "boot-a",
         "btime": 1700000000,
         "gpus": [],
      }
      base.update(overrides)
      return base

   def test_first_insert_sets_first_seen_to_now(self):
      conn = _FakeConn()
      now = _dt(days_ago=0)
      hardware.upsert_hardware(
         conn, "polaris", "polaris-login-01", self._hw(),
         probe_version=4, stored_row=None, now=now)
      _, params = conn.calls[0]
      assert params["first_seen"] == now
      assert params["last_verified"] == now

   def test_refresh_preserves_first_seen(self):
      conn = _FakeConn()
      first_seen = _dt(days_ago=90)
      stored = {"first_seen": first_seen, "boot_id": "boot-a",
                "last_verified": _dt(days_ago=31)}
      now = _dt(days_ago=0)
      hardware.upsert_hardware(
         conn, "polaris", "polaris-login-01", self._hw(),
         probe_version=4, stored_row=stored, now=now)
      _, params = conn.calls[0]
      assert params["first_seen"] == first_seen
      assert params["last_verified"] == now

   def test_upsert_uses_on_conflict_on_the_pk_columns(self):
      conn = _FakeConn()
      hardware.upsert_hardware(
         conn, "polaris", "polaris-login-01", self._hw(), probe_version=4)
      stmt, _ = conn.calls[0]
      sql_text = str(stmt)
      assert "ON CONFLICT (system, source_hostname) DO UPDATE" in sql_text
      update_clause = sql_text.split("DO UPDATE SET", 1)[1]
      set_clause = update_clause.split("-- first_seen", 1)[0]
      assert "first_seen" not in set_clause

   def test_changed_value_is_logged_at_warning(self, caplog):
      conn = _FakeConn()
      hw = self._hw(mem_total_kb=263977056)
      stored = dict(self._hw(mem_total_kb=527954112))
      stored["first_seen"] = _dt(days_ago=10)
      stored["last_verified"] = _dt(days_ago=1)
      with caplog.at_level("WARNING"):
         changes = hardware.upsert_hardware(
            conn, "polaris", "polaris-login-01", hw,
            probe_version=4, stored_row=stored)
      assert changes == {"mem_total_kb": (527954112, 263977056)}
      assert any(
         "mem_total_kb" in record.message and "polaris-login-01" in record.message
         for record in caplog.records)

   def test_net_ifaces_and_gpus_serialized_as_json_strings(self):
      conn = _FakeConn()
      hw = self._hw(net_ifaces={"bond0": 1000, "ens10f0": None}, gpus=[])
      hardware.upsert_hardware(
         conn, "polaris", "polaris-login-01", hw, probe_version=4)
      _, params = conn.calls[0]
      assert params["net_ifaces"] == '{"bond0": 1000, "ens10f0": null}'
      assert params["gpus"] == "[]"
