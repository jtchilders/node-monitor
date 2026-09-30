"""Tests for node_monitor.database.migration -- the immutable Migration
value type and fail-closed discover_migrations() resource discovery.

Design: node_monitor_planning increment2-schema-migrations-writer.md
Task 1 ("Migration resource model and fail-closed discovery"). This task
implements discovery only -- no migration runner, no SQL execution, no
CLI, no final tables. Migration 0001's SQL body is a syntactically
harmless placeholder that Task 3 will replace.
"""

import dataclasses
import hashlib

import pytest

from node_monitor.database.migration import (
   Migration,
   discover_migrations,
)


# --------------------------------------------------------------------------
# Requirement 1: Migration is a frozen, immutable value with the required
# fields.
# --------------------------------------------------------------------------

class TestMigrationValueType:
   def test_migration_is_frozen_dataclass(self):
      assert dataclasses.is_dataclass(Migration)
      fields = {f.name for f in dataclasses.fields(Migration)}
      assert {"version", "name", "sql", "checksum", "mode"} <= fields

   def test_migration_instances_are_immutable(self):
      migration = Migration(
         version=1,
         name="initial_source_schema",
         sql=b"-- placeholder\n",
         checksum=hashlib.sha256(b"-- placeholder\n").hexdigest(),
         mode="transactional",
      )
      with pytest.raises(dataclasses.FrozenInstanceError):
         migration.version = 2

   def test_version_must_be_a_positive_integer(self):
      with pytest.raises(ValueError):
         Migration(
            version=0,
            name="x",
            sql=b"-- x\n",
            checksum=hashlib.sha256(b"-- x\n").hexdigest(),
            mode="transactional",
         )
      with pytest.raises(ValueError):
         Migration(
            version=-1,
            name="x",
            sql=b"-- x\n",
            checksum=hashlib.sha256(b"-- x\n").hexdigest(),
            mode="transactional",
         )

   def test_sql_must_be_bytes(self):
      with pytest.raises(TypeError):
         Migration(
            version=1,
            name="x",
            sql="-- x\n",
            checksum=hashlib.sha256(b"-- x\n").hexdigest(),
            mode="transactional",
         )

   def test_checksum_must_match_exact_sql_bytes(self):
      sql = b"-- placeholder\n"
      correct = hashlib.sha256(sql).hexdigest()
      with pytest.raises(ValueError):
         Migration(
            version=1,
            name="x",
            sql=sql,
            checksum="0" * 64,
            mode="transactional",
         )
      # correct checksum is accepted
      Migration(version=1, name="x", sql=sql, checksum=correct,
                 mode="transactional")

   def test_checksum_is_lowercase_hex(self):
      sql = b"-- placeholder\n"
      correct = hashlib.sha256(sql).hexdigest()
      migration = Migration(version=1, name="x", sql=sql, checksum=correct,
                             mode="transactional")
      assert migration.checksum == migration.checksum.lower()
      assert migration.checksum == correct

   def test_only_transactional_mode_is_supported(self):
      sql = b"-- placeholder\n"
      checksum = hashlib.sha256(sql).hexdigest()
      with pytest.raises(ValueError):
         Migration(version=1, name="x", sql=sql, checksum=checksum,
                    mode="nontransactional")
      with pytest.raises(ValueError):
         Migration(version=1, name="x", sql=sql, checksum=checksum,
                    mode="concurrent")


# --------------------------------------------------------------------------
# Requirement 2: discover_migrations() finds the packaged placeholder
# migration via importlib.resources.
# --------------------------------------------------------------------------

class TestDiscoverMigrationsHappyPath:
   def test_discovers_migration_1(self):
      migrations = discover_migrations()
      assert len(migrations) >= 1
      assert migrations[0].version == 1
      assert migrations[0].name == "initial_source_schema"

   def test_returned_migrations_are_sorted_numerically_by_version(self):
      migrations = discover_migrations()
      versions = [m.version for m in migrations]
      assert versions == sorted(versions)

   def test_checksum_matches_exact_packaged_bytes(self):
      migrations = discover_migrations()
      first = migrations[0]
      assert first.checksum == hashlib.sha256(first.sql).hexdigest()

   def test_sql_bytes_are_not_decoded_or_reencoded(self):
      # The packaged bytes must be readable as UTF-8 (it's a SQL comment)
      # but discovery must not normalize e.g. line endings before
      # hashing -- prove by comparing against a direct resource read.
      import importlib.resources as resources
      migrations = discover_migrations()
      first = migrations[0]
      raw = resources.files(
         "node_monitor.database.migrations.versions"
      ).joinpath("0001_initial_source_schema.sql").read_bytes()
      assert first.sql == raw
      assert first.checksum == hashlib.sha256(raw).hexdigest()

   def test_all_discovered_migrations_are_transactional_mode(self):
      migrations = discover_migrations()
      for migration in migrations:
         assert migration.mode == "transactional"


# --------------------------------------------------------------------------
# Requirement 3: filename parsing accepts only NNNN_name.sql and rejects
# malformed names, ignoring non-matching files.
# --------------------------------------------------------------------------

class TestFilenameParsing:
   def test_non_sql_files_in_versions_package_are_ignored(self):
      # __init__.py lives alongside the .sql files; discovery must not
      # choke on it or treat it as a migration.
      migrations = discover_migrations()
      names = [m.name for m in migrations]
      assert "__init__" not in names

   @pytest.mark.parametrize("filename", [
      "1_initial_source_schema.sql",     # not zero-padded to 4 digits
      "0001-initial_source_schema.sql",  # wrong separator
      "0001_initial source schema.sql",  # spaces
      "abcd_initial_source_schema.sql",  # non-numeric version
      "0001.sql",                        # missing name segment
      "initial_source_schema.sql",       # missing version segment
   ])
   def test_malformed_filenames_are_rejected(self, filename, tmp_path,
                                              monkeypatch):
      import node_monitor.database.migration as migration_module
      good_sql = b"-- placeholder\n"
      bad_path = tmp_path / filename
      bad_path.write_bytes(good_sql)
      good_path = tmp_path / "0001_initial_source_schema.sql"
      good_path.write_bytes(good_sql)

      with pytest.raises(ValueError):
         migration_module._migrations_from_directory(tmp_path)


# --------------------------------------------------------------------------
# Requirement 4: versions must be positive, unique, and contiguous from 1;
# duplicates/gaps are rejected.
# --------------------------------------------------------------------------

class TestVersionContiguity:
   def _write(self, directory, filename, sql=b"-- placeholder\n"):
      (directory / filename).write_bytes(sql)

   def test_duplicate_versions_are_rejected(self, tmp_path):
      import node_monitor.database.migration as migration_module
      self._write(tmp_path, "0001_first.sql")
      self._write(tmp_path, "0001_second.sql")
      with pytest.raises(ValueError):
         migration_module._migrations_from_directory(tmp_path)

   def test_gap_in_versions_is_rejected(self, tmp_path):
      import node_monitor.database.migration as migration_module
      self._write(tmp_path, "0001_first.sql")
      self._write(tmp_path, "0003_third.sql")
      with pytest.raises(ValueError):
         migration_module._migrations_from_directory(tmp_path)

   def test_non_positive_version_is_rejected(self, tmp_path):
      import node_monitor.database.migration as migration_module
      self._write(tmp_path, "0000_zero.sql")
      with pytest.raises(ValueError):
         migration_module._migrations_from_directory(tmp_path)

   def test_contiguous_from_one_is_accepted(self, tmp_path):
      import node_monitor.database.migration as migration_module
      self._write(tmp_path, "0001_first.sql")
      self._write(tmp_path, "0002_second.sql")
      migrations = migration_module._migrations_from_directory(tmp_path)
      assert [m.version for m in migrations] == [1, 2]

   def test_must_start_at_one_not_two(self, tmp_path):
      import node_monitor.database.migration as migration_module
      self._write(tmp_path, "0002_second.sql")
      with pytest.raises(ValueError):
         migration_module._migrations_from_directory(tmp_path)

   def test_empty_migration_set_is_rejected(self, tmp_path):
      # An empty migrations source is a packaging/authoring failure --
      # every installation must ship at least migration 1. Fail closed
      # instead of silently returning an empty tuple.
      import node_monitor.database.migration as migration_module
      with pytest.raises(ValueError):
         migration_module._migrations_from_directory(tmp_path)


# --------------------------------------------------------------------------
# Requirement 5: unsupported migration mode metadata is rejected, never
# silently guessed.
# --------------------------------------------------------------------------

class TestModeRejection:
   def test_directory_with_unsupported_mode_marker_is_rejected(
         self, tmp_path):
      import node_monitor.database.migration as migration_module
      (tmp_path / "0001_first.sql").write_bytes(b"-- placeholder\n")
      (tmp_path / "0001_first.sql.mode").write_text("concurrent")
      with pytest.raises(ValueError):
         migration_module._migrations_from_directory(tmp_path)
