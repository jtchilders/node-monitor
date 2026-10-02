"""Tests proving that schema_contract.py is the single canonical source of
migration type/discovery/contiguity/mode/checksum logic, and that
migration.py imports/re-exports from it rather than re-implementing.

These tests enforce the shared-contract requirement from the Task 2 review:
migration.py must NOT define its own duplicates of the canonical objects that
live in schema_contract.py.
"""

import dataclasses
import hashlib
import importlib

import pytest


# ---------------------------------------------------------------------------
# Requirement: schema_contract.py is the canonical source of shared types
# ---------------------------------------------------------------------------

class TestSharedContractCanonicalLocation:
    """The canonical immutable migration type, discovery logic, contiguity
    validation, and catalog signatures must live in schema_contract.py.
    migration.py must re-export them from there, not re-implement them.
    """

    def test_migration_type_defined_in_schema_contract(self):
        """Migration dataclass must be importable from schema_contract."""
        from node_monitor.database import schema_contract
        assert hasattr(schema_contract, "Migration"), (
            "Migration must be defined in schema_contract.py"
        )
        assert dataclasses.is_dataclass(schema_contract.Migration)

    def test_migration_from_migration_py_is_same_class_as_schema_contract(self):
        """migration.py.Migration must be the exact same class as
        schema_contract.Migration (imported/re-exported, not a separate copy).
        """
        from node_monitor.database import migration as migration_module
        from node_monitor.database import schema_contract
        assert migration_module.Migration is schema_contract.Migration, (
            "migration.Migration must be schema_contract.Migration "
            "(re-exported, not a distinct copy)"
        )

    def test_supported_modes_defined_in_schema_contract(self):
        """_SUPPORTED_MODES must live in schema_contract, not be a separate
        copy in migration.py.
        """
        from node_monitor.database import schema_contract
        assert hasattr(schema_contract, "_SUPPORTED_MODES")
        assert "transactional" in schema_contract._SUPPORTED_MODES

    def test_migration_py_supported_modes_is_schema_contract_object(self):
        """migration._SUPPORTED_MODES must be the same object as
        schema_contract._SUPPORTED_MODES (imported, not copied).
        """
        from node_monitor.database import migration as migration_module
        from node_monitor.database import schema_contract
        assert migration_module._SUPPORTED_MODES is schema_contract._SUPPORTED_MODES, (
            "migration._SUPPORTED_MODES must be schema_contract._SUPPORTED_MODES"
        )

    def test_validate_contiguous_defined_in_schema_contract(self):
        """_validate_contiguous must live in schema_contract."""
        from node_monitor.database import schema_contract
        assert hasattr(schema_contract, "_validate_contiguous"), (
            "_validate_contiguous must be in schema_contract"
        )

    def test_migration_py_validate_contiguous_is_schema_contract_function(self):
        """migration._validate_contiguous must be the same function object
        as schema_contract._validate_contiguous.
        """
        from node_monitor.database import migration as migration_module
        from node_monitor.database import schema_contract
        assert migration_module._validate_contiguous is schema_contract._validate_contiguous, (
            "migration._validate_contiguous must be re-exported from schema_contract"
        )

    def test_source_schema_columns_defined_in_schema_contract(self):
        """_SOURCE_SCHEMA_COLUMNS catalog must live in schema_contract."""
        from node_monitor.database import schema_contract
        assert hasattr(schema_contract, "_SOURCE_SCHEMA_COLUMNS"), (
            "_SOURCE_SCHEMA_COLUMNS must be defined in schema_contract.py"
        )
        # Must have the five telemetry tables
        cols = schema_contract._SOURCE_SCHEMA_COLUMNS
        for table in ("node_hardware", "node_counter_minute", "node_usage_intervals",
                      "node_poll_failures", "node_collection_log"):
            assert table in cols, "%s must be in _SOURCE_SCHEMA_COLUMNS" % table

    def test_migration_py_source_schema_columns_is_schema_contract_object(self):
        """migration._SOURCE_SCHEMA_COLUMNS must be the same object as
        schema_contract._SOURCE_SCHEMA_COLUMNS.
        """
        from node_monitor.database import migration as migration_module
        from node_monitor.database import schema_contract
        assert migration_module._SOURCE_SCHEMA_COLUMNS is schema_contract._SOURCE_SCHEMA_COLUMNS

    def test_required_source_constraints_defined_in_schema_contract(self):
        """_REQUIRED_SOURCE_CONSTRAINTS must live in schema_contract."""
        from node_monitor.database import schema_contract
        assert hasattr(schema_contract, "_REQUIRED_SOURCE_CONSTRAINTS")
        assert isinstance(schema_contract._REQUIRED_SOURCE_CONSTRAINTS, frozenset)

    def test_migration_py_required_source_constraints_is_schema_contract_object(self):
        from node_monitor.database import migration as migration_module
        from node_monitor.database import schema_contract
        assert migration_module._REQUIRED_SOURCE_CONSTRAINTS is schema_contract._REQUIRED_SOURCE_CONSTRAINTS

    def test_required_source_indexes_defined_in_schema_contract(self):
        """_REQUIRED_SOURCE_INDEXES must live in schema_contract."""
        from node_monitor.database import schema_contract
        assert hasattr(schema_contract, "_REQUIRED_SOURCE_INDEXES")
        assert isinstance(schema_contract._REQUIRED_SOURCE_INDEXES, frozenset)

    def test_migration_py_required_source_indexes_is_schema_contract_object(self):
        from node_monitor.database import migration as migration_module
        from node_monitor.database import schema_contract
        assert migration_module._REQUIRED_SOURCE_INDEXES is schema_contract._REQUIRED_SOURCE_INDEXES

    def test_discover_migrations_defined_in_schema_contract(self):
        """discover_migrations must be callable from schema_contract."""
        from node_monitor.database import schema_contract
        assert hasattr(schema_contract, "discover_migrations"), (
            "discover_migrations must be importable from schema_contract"
        )
        assert callable(schema_contract.discover_migrations)

    def test_migration_py_discover_migrations_is_schema_contract_function(self):
        """migration.discover_migrations must be the same callable as
        schema_contract.discover_migrations.
        """
        from node_monitor.database import migration as migration_module
        from node_monitor.database import schema_contract
        assert migration_module.discover_migrations is schema_contract.discover_migrations

    def test_schema_contract_does_not_import_migration_module(self):
        """schema_contract.py must not import migration.py (dependency direction)."""
        import sys
        # Evict both modules
        for key in list(sys.modules):
            if "node_monitor.database.migration" in key:
                del sys.modules[key]
            if "node_monitor.database.schema_contract" in key:
                del sys.modules[key]

        importlib.import_module("node_monitor.database.schema_contract")
        assert "node_monitor.database.migration" not in sys.modules, (
            "schema_contract.py must not import migration.py"
        )

    def test_migration_type_has_required_fields(self):
        """The canonical Migration type in schema_contract must have the same
        fields as the one previously defined in migration.py.
        """
        from node_monitor.database.schema_contract import Migration
        fields = {f.name for f in dataclasses.fields(Migration)}
        assert {"version", "name", "sql", "checksum", "mode"} <= fields

    def test_migration_type_validates_checksum(self):
        """Migration defined in schema_contract enforces checksum correctness."""
        from node_monitor.database.schema_contract import Migration
        sql = b"-- test\\n"
        correct = hashlib.sha256(sql).hexdigest()
        m = Migration(version=1, name="x", sql=sql, checksum=correct,
                      mode="transactional")
        assert m.checksum == correct
        with pytest.raises(ValueError):
            Migration(version=1, name="x", sql=sql, checksum="0" * 64,
                      mode="transactional")

    def test_validate_contiguous_in_schema_contract_rejects_gaps(self):
        """_validate_contiguous from schema_contract rejects gaps and duplicates."""
        from node_monitor.database.schema_contract import Migration, _validate_contiguous
        sql = b"-- x\\n"
        checksum = hashlib.sha256(sql).hexdigest()
        m1 = Migration(version=1, name="a", sql=sql, checksum=checksum,
                       mode="transactional")
        m3 = Migration(version=3, name="c", sql=sql, checksum=checksum,
                       mode="transactional")
        with pytest.raises(ValueError):
            _validate_contiguous([m1, m3])

    def test_discover_migrations_in_schema_contract_returns_migration_instances(self):
        """discover_migrations from schema_contract returns Migration instances
        (the same canonical type).
        """
        from node_monitor.database.schema_contract import Migration, discover_migrations
        migrations = discover_migrations()
        assert len(migrations) >= 1
        for m in migrations:
            assert isinstance(m, Migration)


class TestMigrationPyDocstringHonesty:
    """migration.py docstring must accurately describe that it imports/re-exports
    from schema_contract.py rather than defining these things itself.
    """

    def test_migration_module_docstring_mentions_schema_contract(self):
        """migration.py's module docstring must mention schema_contract to
        inform future authors of the dependency direction.
        """
        from node_monitor.database import migration as migration_module
        docstring = migration_module.__doc__ or ""
        assert "schema_contract" in docstring.lower(), (
            "migration.py docstring must mention schema_contract to document "
            "the import relationship"
        )
