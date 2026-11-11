import importlib.resources
import importlib.util
import re
import threading
from datetime import datetime, timezone

import psycopg
import pytest

from node_monitor.database import migrate as migrate_mod
from node_monitor.database.connect import connect
from node_monitor.database.migrate import (
   MIGRATIONS,
   REQUIRED_VERSION,
   SchemaOutOfDate,
   check_schema,
   current_version,
   migrate,
)
from node_monitor.pipeline.enrich import LIVE_PROCESS_COLUMNS
from node_monitor.pipeline.node_metrics import NODE_METRICS_COLUMNS
from node_monitor.pipeline.usage import NODE_USAGE_COLUMNS, USER_USAGE_COLUMNS
from tests.probe import fake_proc
from tests.probe.conftest import PROBE_PATH

PRIMARY_KEYS = {
   "live_processes": ["system", "node", "pid", "start_time"],
   "live_snapshots": ["system", "node"],
   "live_nodes": ["system", "node"],
   "nodes": ["system", "node"],
   "node_hardware": ["system", "node", "boot_id"],
   "node_metrics_1m": ["system", "node", "minute"],
   "user_usage_5m": ["system", "node", "username", "interval_start"],
   "node_usage_5m": ["system", "node", "interval_start"],
   "collection_events": ["id", "ts"],
   "collector_heartbeat": ["system"],
   "schema_migrations": ["version"],
   "process_samples": ["system", "node", "ts", "pid", "start_time"],
   "command_lines": ["system", "username", "cmdline_hash"],
}

PARTITION_KEYS = {
   "node_metrics_1m": "minute",
   "user_usage_5m": "interval_start",
   "node_usage_5m": "interval_start",
   "collection_events": "ts",
   "process_samples": "ts",
}


def _columns(conn, table):
   rows = conn.execute(
      "SELECT column_name FROM information_schema.columns"
      " WHERE table_schema = 'node_monitor' AND table_name = %s",
      (table,),
   ).fetchall()
   return {r[0] for r in rows}


def _column_types(conn):
   rows = conn.execute(
      "SELECT table_name, column_name, data_type FROM information_schema.columns"
      " WHERE table_schema = 'node_monitor'"
   ).fetchall()
   return {(t, c): d for t, c, d in rows}


# --- without a database ---------------------------------------------------------------


def test_migrations_are_discovered_from_package_data():
   assert MIGRATIONS == [
      (1, "0001_initial.sql"),
      (2, "0002_user_maps_and_event_key.sql"),
      (3, "0003_history.sql"),
   ]
   assert REQUIRED_VERSION == 3
   pkg = importlib.resources.files("node_monitor.database.migrations")
   assert pkg.joinpath("0001_initial.sql").read_text().strip()


def test_migrations_reference_no_roles():
   # Role grants and default privileges belong to bootstrap: a migration naming roles
   # fails on any server where those roles do not exist.
   pkg = importlib.resources.files("node_monitor.database.migrations")
   for _, filename in MIGRATIONS:
      text = pkg.joinpath(filename).read_text()
      code = "\n".join(line.split("--", 1)[0] for line in text.splitlines())
      assert not re.search(r"\b(GRANT|REVOKE|OWNER\s+TO|DEFAULT\s+PRIVILEGES)\b", code, re.I)
      assert "node_monitor_owner" not in code and "node_monitor_reader" not in code


# --- the runner -----------------------------------------------------------------------


def test_current_version_is_zero_before_migrating(pg_db):
   with connect(pg_db) as conn:
      assert current_version(conn) == 0


def test_migrate_applies_all_and_is_idempotent(pg_db):
   with connect(pg_db) as conn:
      assert migrate(conn) == [1, 2, 3]
      assert migrate(conn) == []
      assert current_version(conn) == REQUIRED_VERSION
      rows = conn.execute(
         "SELECT version, name, applied_at FROM node_monitor.schema_migrations"
      ).fetchall()
   assert [(v, n) for v, n, _ in rows] == MIGRATIONS
   assert rows[0][2].tzinfo is not None


def test_migrate_works_on_an_autocommit_connection(pg_db):
   with connect(pg_db, autocommit=True) as conn:
      assert migrate(conn) == [1, 2, 3]
      assert conn.autocommit is True


def test_check_schema_raises_until_migrated(pg_db):
   with connect(pg_db) as conn:
      with pytest.raises(SchemaOutOfDate) as info:
         check_schema(conn)
      assert str(info.value) == "schema version 0, need 3: run `node-monitor database migrate`"
      migrate(conn)
      check_schema(conn)


_HEARTBEAT_INSERT = (
   "INSERT INTO collector_heartbeat (system, collector_host, version, started_at, last_seen,"
   " status) VALUES (%s, 'h', 'v', now(), now(), '{}')"
)


def _heartbeat_count(url):
   with connect(url) as other:
      return other.execute("SELECT count(*) FROM collector_heartbeat").fetchone()[0]


def test_check_schema_leaves_a_default_connection_idle(migrated_db):
   # Left inside the check's implicit transaction, the next `conn.transaction()` would be a
   # savepoint and its INSERT would never be committed.
   with connect(migrated_db) as conn:
      check_schema(conn)
      assert conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
      with conn.transaction():
         conn.execute(_HEARTBEAT_INSERT, ("polaris",))
      assert _heartbeat_count(migrated_db) == 1


def test_check_schema_that_raises_leaves_the_connection_idle(pg_db):
   with connect(pg_db) as conn:
      with pytest.raises(SchemaOutOfDate):
         check_schema(conn)
      assert conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
      assert migrate(conn) == [1, 2, 3]


def test_current_version_keeps_the_callers_open_transaction(migrated_db):
   with connect(migrated_db) as conn:
      conn.execute(_HEARTBEAT_INSERT, ("polaris",))
      assert current_version(conn) == REQUIRED_VERSION
      assert conn.info.transaction_status == psycopg.pq.TransactionStatus.INTRANS
      conn.commit()
   assert _heartbeat_count(migrated_db) == 1


def test_concurrent_migrate_is_serialized(pg_db):
   barrier = threading.Barrier(2)
   results, errors = [], []

   def run():
      try:
         with connect(pg_db) as conn:
            barrier.wait(timeout=30)
            results.append(migrate(conn))
      except Exception as exc:  # surfaced by the assertions below
         errors.append(exc)

   threads = [threading.Thread(target=run) for _ in range(2)]
   for t in threads:
      t.start()
   for t in threads:
      t.join(timeout=60)
   assert errors == []
   assert sorted(results) == [[], [1, 2, 3]]
   with connect(pg_db) as conn:
      assert current_version(conn) == REQUIRED_VERSION


def test_failed_migration_rolls_back_and_releases_the_lock(pg_db, monkeypatch):
   real_read = migrate_mod._read_sql

   def read(filename):
      if filename == "0004_broken.sql":
         return "CREATE TABLE node_monitor.half_done (x int);\nSELEC broken;"
      return real_read(filename)

   monkeypatch.setattr(migrate_mod, "MIGRATIONS", MIGRATIONS + [(4, "0004_broken.sql")])
   monkeypatch.setattr(migrate_mod, "_read_sql", read)
   with connect(pg_db) as conn:
      with pytest.raises(psycopg.errors.SyntaxError):
         migrate(conn)
      assert current_version(conn) == REQUIRED_VERSION
      assert conn.execute("SELECT to_regclass('node_monitor.half_done')").fetchone()[0] is None
   with connect(pg_db) as other:
      locked = other.execute(
         "SELECT pg_try_advisory_lock(hashtext('node_monitor.migrate'))"
      ).fetchone()[0]
   assert locked is True


_EVENTS_PARTITION = (
   "CREATE TABLE collection_events_2026_11 PARTITION OF collection_events"
   " FOR VALUES FROM ('2026-11-01 00:00+00') TO ('2026-12-01 00:00+00')"
)
_EVENT_INSERT = (
   "INSERT INTO collection_events (ts, system, node, event_type, detail)"
   " VALUES ('2026-11-01 12:00+00', 'polaris', %s, %s, '{}')"
)


def _version_1(conn, monkeypatch):
   monkeypatch.setattr(migrate_mod, "MIGRATIONS", [m for m in MIGRATIONS if m[0] <= 1])
   assert migrate(conn) == [1]
   monkeypatch.setattr(migrate_mod, "MIGRATIONS", MIGRATIONS)


def test_migration_0002_drops_user_maps_and_dedupes_events(pg_db, monkeypatch):
   with connect(pg_db, autocommit=True) as conn:
      _version_1(conn, monkeypatch)
      conn.execute(_EVENTS_PARTITION)
      for node, event_type in ((None, "e"), (None, "e"), (None, "other"), ("h1", "e"), ("h1", "e")):
         conn.execute(_EVENT_INSERT, (node, event_type))
      before = conn.execute("SELECT id, node, event_type FROM collection_events ORDER BY id")
      ids = before.fetchall()
      assert len(ids) == 5
      monkeypatch.setattr(migrate_mod, "MIGRATIONS", [m for m in MIGRATIONS if m[0] <= 2])
      assert migrate(conn) == [2]
      monkeypatch.setattr(migrate_mod, "MIGRATIONS", MIGRATIONS)
      assert "by_category" not in _columns(conn, "user_usage_5m")
      assert "by_dormancy" not in _columns(conn, "user_usage_5m")
      assert {"by_category", "by_dormancy"} <= _columns(conn, "node_usage_5m")
      left = conn.execute("SELECT id, node, event_type FROM collection_events ORDER BY id")
      # The lower id of each duplicated pair survives; NULL nodes compare equal.
      assert left.fetchall() == [ids[0], ids[2], ids[3]]


def test_event_natural_key_rejects_duplicates_including_null_node(migrated_db):
   with connect(migrated_db) as conn:
      conn.execute(_EVENTS_PARTITION)
      for node in (None, "h1"):
         conn.execute(_EVENT_INSERT, (node, "e"))
         with pytest.raises(psycopg.errors.UniqueViolation):
            with conn.transaction():
               conn.execute(_EVENT_INSERT, (node, "e"))


# --- the schema -----------------------------------------------------------------------


def test_tables_and_primary_keys_exist(migrated_db):
   with connect(migrated_db) as conn:
      tables = {
         r[0]
         for r in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'node_monitor'"
         )
      }
      rows = conn.execute(
         "SELECT tc.table_name, kcu.column_name"
         " FROM information_schema.table_constraints tc"
         " JOIN information_schema.key_column_usage kcu"
         "   USING (constraint_schema, constraint_name, table_name)"
         " WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = 'node_monitor'"
         " ORDER BY tc.table_name, kcu.ordinal_position"
      ).fetchall()
   assert tables == set(PRIMARY_KEYS)
   keys = {}
   for table, column in rows:
      keys.setdefault(table, []).append(column)
   assert keys == PRIMARY_KEYS


def test_partitioned_tables_are_partitioned(migrated_db):
   with connect(migrated_db) as conn:
      rows = conn.execute(
         "SELECT c.relname, p.partstrat, a.attname"
         " FROM pg_partitioned_table p"
         " JOIN pg_class c ON c.oid = p.partrelid"
         " JOIN pg_namespace n ON n.oid = c.relnamespace"
         " JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = p.partattrs[0]"
         " WHERE n.nspname = 'node_monitor'"
      ).fetchall()
      partitions = conn.execute(
         "SELECT count(*) FROM pg_inherits i"
         " JOIN pg_class c ON c.oid = i.inhparent"
         " JOIN pg_namespace n ON n.oid = c.relnamespace"
         " WHERE n.nspname = 'node_monitor'"
      ).fetchone()[0]
   assert {r[0]: r[2] for r in rows} == PARTITION_KEYS
   assert {r[1] for r in rows} == {"r"}
   assert partitions == 0


def test_insert_into_missing_month_fails_loudly(migrated_db):
   # No DEFAULT partition: a row for a month without a partition must fail, not land in a
   # catch-all that later blocks creating that month's partition.
   with connect(migrated_db) as conn:
      with pytest.raises(psycopg.errors.CheckViolation):
         conn.execute(
            "INSERT INTO node_usage_5m (system, node, interval_start) VALUES (%s, %s, %s)",
            ("polaris", "n1", datetime(2026, 11, 1, tzinfo=timezone.utc)),
         )


def test_columns_match_the_pipeline_record_contract(migrated_db):
   ident = {"system", "node"}
   with connect(migrated_db) as conn:
      assert _columns(conn, "live_processes") == ident | {"ts"} | set(LIVE_PROCESS_COLUMNS)
      assert _columns(conn, "node_metrics_1m") == ident | set(NODE_METRICS_COLUMNS)
      assert _columns(conn, "user_usage_5m") == ident | set(USER_USAGE_COLUMNS)
      assert _columns(conn, "node_usage_5m") == ident | set(NODE_USAGE_COLUMNS)


def test_node_hardware_columns_match_the_probe_hello(migrated_db, tmp_path):
   spec = importlib.util.spec_from_file_location("probe", str(PROBE_PATH))
   probe = importlib.util.module_from_spec(spec)
   spec.loader.exec_module(probe)
   hardware = probe.collect_hardware(fake_proc.write_tree(tmp_path), gpu=False)
   hello = {"boot_id", "btime", "collected_at", "probe_version", "python_version"}
   expected = {"system", "node", "hostname_fqdn"} | hello | set(hardware)
   with connect(migrated_db) as conn:
      assert _columns(conn, "node_hardware") == expected


def test_column_types(migrated_db):
   with connect(migrated_db) as conn:
      types = _column_types(conn)
   # Every instant is timestamptz; ints that can grow are bigint; no lossy or untyped
   # column types slip in.
   allowed = {"text", "bigint", "integer", "double precision", "boolean", "jsonb", "ARRAY"}
   allowed.add("timestamp with time zone")
   assert {d for d in types.values()} <= allowed
   integer = {k for k, d in types.items() if d == "integer"}
   assert ("live_processes", "pid") in integer
   # uid_t is unsigned 32-bit (nfsnobody is 4294967294); ppid is bigint as well.
   assert types[("live_processes", "uid")] == "bigint"
   assert types[("live_processes", "ppid")] == "bigint"
   assert all(
      t in ("live_processes", "node_hardware", "live_nodes", "schema_migrations", "process_samples")
      for t, _ in integer
   )
   assert types[("live_processes", "start_time")] == "bigint"
   assert types[("live_processes", "rss_kb")] == "bigint"
   assert types[("live_processes", "cpu_seconds")] == "double precision"
   assert types[("live_processes", "interactive")] == "boolean"
   assert types[("collection_events", "id")] == "bigint"
   assert types[("node_hardware", "btime")] == "bigint"
   jsonb = {k for k, d in types.items() if d == "jsonb"}
   assert jsonb == {
      ("node_metrics_1m", "cpu_pct"),
      ("node_metrics_1m", "network_rates"),
      ("node_metrics_1m", "lustre_md_rates"),
      ("node_metrics_1m", "gpus"),
      ("user_usage_5m", "by_activity"),
      ("user_usage_5m", "by_category_dormancy"),
      ("node_usage_5m", "by_category"),
      ("node_usage_5m", "by_activity"),
      ("node_usage_5m", "by_dormancy"),
      ("node_usage_5m", "by_category_dormancy"),
      ("node_hardware", "net_ifaces"),
      ("node_hardware", "gpus"),
      ("collection_events", "detail"),
      ("collector_heartbeat", "status"),
   }
   timestamps = {k for k, d in types.items() if d == "timestamp with time zone"}
   assert {
      ("live_processes", "ts"),
      ("live_snapshots", "ts"),
      ("live_nodes", "updated_at"),
      ("live_nodes", "last_census_at"),
      ("node_metrics_1m", "minute"),
      ("user_usage_5m", "interval_start"),
      ("node_usage_5m", "interval_start"),
      ("collection_events", "ts"),
   } <= timestamps


def test_indexes_exist(migrated_db):
   with connect(migrated_db) as conn:
      defs = [
         r[0]
         for r in conn.execute("SELECT indexdef FROM pg_indexes WHERE schemaname = 'node_monitor'")
      ]
   found = set()
   for d in defs:
      m = re.search(r" ON (?:ONLY )?node_monitor\.(\w+) USING btree \((.*)\)$", d)
      if m:
         found.add((m.group(1), m.group(2)))
   assert {
      ("node_metrics_1m", "system, minute DESC"),
      ("user_usage_5m", "system, interval_start DESC"),
      ("user_usage_5m", "system, username, interval_start DESC"),
      ("node_usage_5m", "system, interval_start DESC"),
      ("collection_events", "system, ts DESC"),
      ("live_processes", "system, username"),
   } <= found


def test_collection_events_generate_ids_and_allow_collector_level_rows(migrated_db):
   with connect(migrated_db) as conn:
      conn.execute(
         "CREATE TABLE collection_events_y2026m11 PARTITION OF collection_events"
         " FOR VALUES FROM ('2026-11-01 00:00+00') TO ('2026-12-01 00:00+00')"
      )
      ids = [
         conn.execute(
            "INSERT INTO collection_events (ts, system, node, event_type, detail)"
            " VALUES (%s, 'polaris', NULL, %s, '{}') RETURNING id",
            (datetime(2026, 11, 1, 12, tzinfo=timezone.utc), event_type),
         ).fetchone()[0]
         for event_type in ("collector_started", "collector_stopped")
      ]
   assert len(set(ids)) == 2 and all(isinstance(i, int) for i in ids)


def test_live_processes_stores_an_unsigned_32_bit_uid(migrated_db):
   with connect(migrated_db) as conn:
      conn.execute(
         "INSERT INTO live_processes (system, node, ts, pid, start_time, uid, ppid)"
         " VALUES ('s', 'n', now(), 1, 1, 4294967294, 1)"
      )
      assert conn.execute("SELECT uid FROM live_processes").fetchone() == (4294967294,)


# --- history tables (M3) ----------------------------------------------------------------


def test_migrate_creates_history_tables(migrated_db):
   with connect(migrated_db) as conn:
      tables = {
         r[0]
         for r in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'node_monitor'"
         )
      }
      assert "process_samples" in tables
      assert "command_lines" in tables
      # pg_trgm extension and GIN index for substring search (or fallback if unavailable).
      has_trgm = (
         conn.execute(
            "SELECT count(*) FROM pg_available_extensions WHERE name='pg_trgm'"
         ).fetchone()[0]
         == 1
      )
      if has_trgm:
         ext = conn.execute(
            "SELECT count(*) FROM pg_extension WHERE extname = 'pg_trgm'"
         ).fetchone()[0]
         assert ext == 1
      idx = conn.execute(
         "SELECT indexdef FROM pg_indexes"
         " WHERE schemaname = 'node_monitor' AND tablename = 'command_lines'"
      ).fetchall()
      assert any("cmdline" in d[0] for d in idx)
      if has_trgm:
         assert any("gin_trgm_ops" in d[0] for d in idx)
      # column sanity: tags is text[], hourly is bool, retention columns present.
      cols = _columns(conn, "process_samples")
      assert {
         "system",
         "node",
         "ts",
         "pid",
         "start_time",
         "username",
         "uid",
         "comm",
         "exe",
         "argv0",
         "cmdline",
         "cmdline_hash",
         "parent_comm",
         "parent_exe",
         "parent_kind",
         "category",
         "activity",
         "tags",
         "state",
         "threads",
         "cpu_seconds",
         "rss_kb",
         "gpu_mem_mb",
         "interactive",
         "under_screen_or_tmux",
         "cpu_state",
         "idle_for_sec",
         "idle_for_is_lower_bound",
         "idle_bucket",
         "owner_presence",
         "hourly",
      } <= cols
      cmd_cols = _columns(conn, "command_lines")
      assert {
         "system",
         "username",
         "cmdline_hash",
         "cmdline",
         "exe",
         "activity",
         "category",
         "tags",
         "first_seen",
         "last_seen",
         "sample_count",
         "nodes",
      } <= cmd_cols
      # types: tags and nodes are text[], hourly is boolean.
      types = _column_types(conn)
      assert types[("process_samples", "tags")] == "ARRAY"
      assert types[("command_lines", "tags")] == "ARRAY"
      assert types[("command_lines", "nodes")] == "ARRAY"
      assert types[("process_samples", "hourly")] == "boolean"
      # no roles in migration code.
      pkg = importlib.resources.files("node_monitor.database.migrations")
      text = pkg.joinpath("0003_history.sql").read_text()
      code = "\n".join(line.split("--", 1)[0] for line in text.splitlines())
      assert not re.search(r"\b(GRANT|REVOKE|OWNER\s+TO|DEFAULT\s+PRIVILEGES)\b", code, re.I)
      assert "node_monitor_owner" not in code and "node_monitor_reader" not in code


def test_process_samples_partition_naming_daily(migrated_db):
   with connect(migrated_db, autocommit=True) as conn:
      # partitioned by ts, daily naming yYYYYMMDD, no DEFAULT partition.
      assert (
         conn.execute(
            "SELECT partstrat, partattrs::text FROM pg_partitioned_table"
            " WHERE partrelid = 'node_monitor.process_samples'::regclass"
         ).fetchone()[0]
         == "r"
      )
      # create two daily partitions and verify routing by day.
      for day in (1, 2):
         conn.execute(
            "CREATE TABLE node_monitor.process_samples_y2026110%d"
            " PARTITION OF node_monitor.process_samples"
            " FOR VALUES FROM ('2026-11-0%d 00:00+00') TO ('2026-11-0%d 00:00+00')"
            % (day, day, day + 1)
         )
      for ts_str, expected_part in [
         ("2026-11-01T12:00:00+00:00", "process_samples_y20261101"),
         ("2026-11-02T00:00:00+00:00", "process_samples_y20261102"),
      ]:
         conn.execute(
            "INSERT INTO node_monitor.process_samples"
            " (system, node, ts, pid, start_time, hourly) VALUES ('s', 'n', %s, 1, 1, false)",
            (datetime.fromisoformat(ts_str),),
         )
         assert (
            conn.execute("SELECT count(*) FROM node_monitor.%s" % expected_part).fetchone()[0] == 1
         )
      # missing day must fail loudly (no DEFAULT).
      with pytest.raises(psycopg.errors.CheckViolation):
         conn.execute(
            "INSERT INTO node_monitor.process_samples"
            " (system, node, ts, pid, start_time, hourly) VALUES ('s', 'n', %s, 1, 2, false)",
            (datetime(2026, 11, 3, tzinfo=timezone.utc),),
         )
      # retention helper would drop by daily name: upper bound <= cutoff.
      assert conn.execute(
         "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid"
         " WHERE i.inhparent = 'node_monitor.process_samples'::regclass ORDER BY c.relname"
      ).fetchall() == [("process_samples_y20261101",), ("process_samples_y20261102",)]


def test_command_lines_upsert_keeps_first_seen(migrated_db):
   with connect(migrated_db) as conn:
      conn.execute(
         "INSERT INTO node_monitor.command_lines"
         " (system, username, cmdline_hash, cmdline, first_seen, last_seen, sample_count, nodes)"
         " VALUES ('polaris', 'alice', 'h1', 'cmd one', %s, %s, 1, ARRAY['n1'])",
         (
            datetime(2026, 11, 1, 10, tzinfo=timezone.utc),
            datetime(2026, 11, 1, 10, tzinfo=timezone.utc),
         ),
      )
      # upsert same PK with later first_seen must keep earliest, greatest last_seen,
      # and bump sample_count and merge nodes.
      conn.execute(
         "INSERT INTO node_monitor.command_lines"
         " (system, username, cmdline_hash, cmdline, first_seen, last_seen, sample_count, nodes)"
         " VALUES ('polaris', 'alice', 'h1', 'cmd one', %s, %s, 1, ARRAY['n2'])"
         " ON CONFLICT (system, username, cmdline_hash) DO UPDATE SET"
         "  cmdline = EXCLUDED.cmdline,"
         "  first_seen = LEAST(command_lines.first_seen, EXCLUDED.first_seen),"
         "  last_seen = GREATEST(command_lines.last_seen, EXCLUDED.last_seen),"
         "  sample_count = command_lines.sample_count + EXCLUDED.sample_count,"
         "  nodes = (SELECT array_agg(DISTINCT x ORDER BY x)"  # noqa: E501
         " FROM unnest(command_lines.nodes || EXCLUDED.nodes) AS x)",
         (
            datetime(2026, 11, 2, 10, tzinfo=timezone.utc),
            datetime(2026, 11, 2, 10, tzinfo=timezone.utc),
         ),
      )
      row = conn.execute(
         "SELECT first_seen, last_seen, sample_count, nodes FROM node_monitor.command_lines"
         " WHERE system='polaris' AND username='alice' AND cmdline_hash='h1'"
      ).fetchone()
      assert row[0] == datetime(2026, 11, 1, 10, tzinfo=timezone.utc)
      assert row[1] == datetime(2026, 11, 2, 10, tzinfo=timezone.utc)
      assert row[2] == 2
      assert sorted(row[3]) == ["n1", "n2"]
      # third upsert with earlier first_seen moves it back, nodes deduped.
      conn.execute(
         "INSERT INTO node_monitor.command_lines"
         " (system, username, cmdline_hash, cmdline, first_seen, last_seen, sample_count, nodes)"
         " VALUES ('polaris', 'alice', 'h1', 'cmd one', %s, %s, 1, ARRAY['n1'])"
         " ON CONFLICT (system, username, cmdline_hash) DO UPDATE SET"
         "  first_seen = LEAST(command_lines.first_seen, EXCLUDED.first_seen),"
         "  last_seen = GREATEST(command_lines.last_seen, EXCLUDED.last_seen),"
         "  sample_count = command_lines.sample_count + EXCLUDED.sample_count,"
         "  nodes = (SELECT array_agg(DISTINCT x ORDER BY x)"  # noqa: E501
         " FROM unnest(command_lines.nodes || EXCLUDED.nodes) AS x)",
         (
            datetime(2026, 10, 31, 10, tzinfo=timezone.utc),
            datetime(2026, 10, 31, 11, tzinfo=timezone.utc),
         ),
      )
      row2 = conn.execute(
         "SELECT first_seen, last_seen, sample_count, nodes FROM node_monitor.command_lines"
         " WHERE system='polaris' AND username='alice' AND cmdline_hash='h1'"
      ).fetchone()
      assert row2[0] == datetime(2026, 10, 31, 10, tzinfo=timezone.utc)
      # last_seen stays at the greatest seen (Nov 2).
      assert row2[1] == datetime(2026, 11, 2, 10, tzinfo=timezone.utc)
      assert row2[2] == 3
      assert sorted(row2[3]) == ["n1", "n2"]
