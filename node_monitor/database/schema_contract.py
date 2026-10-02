"""node_monitor.database.schema_contract -- pure, immutable migration
discovery/catalog contract shared by the operator migration runner and
the read-only web preflight.

This module is intentionally self-contained: it performs its own
resource discovery via ``importlib.resources`` and does NOT import
``node_monitor.database.migration``.  The web module imports this
module; the DDL runner (migration.py) also imports this module for
shared constants.  The dependency direction is:

    migration.py ──imports──▶ schema_contract.py
    web.py       ──imports──▶ schema_contract.py

``schema_contract.py`` imports neither ``migration.py`` nor ``web.py``.
"""

import dataclasses
import hashlib
import importlib.resources as resources
import re

# ---------------------------------------------------------------------------
# Table catalog -- the six tables the web reader must be able to SELECT.
# ---------------------------------------------------------------------------

REQUIRED_TABLES = (
   "schema_migrations",
   "node_hardware",
   "node_counter_minute",
   "node_usage_intervals",
   "node_poll_failures",
   "node_collection_log",
)

# ---------------------------------------------------------------------------
# Internal migration-resource constants (mirrored from migration.py so this
# module stays independent).
# ---------------------------------------------------------------------------

_MIGRATIONS_PACKAGE = "node_monitor.database.migrations.versions"
_FILENAME_PATTERN = re.compile(r"^(\d{4})_([A-Za-z0-9_]+)\.sql$")
_SUPPORTED_MODES = frozenset({"transactional"})


@dataclasses.dataclass(frozen=True)
class _MigrationSummary:
   """Minimal migration value type for catalog/checksum purposes only."""
   version: int
   name: str
   checksum: str
   mode: str


def _parse_filename(filename):
   match = _FILENAME_PATTERN.match(filename)
   if match is None:
      return None
   return int(match.group(1)), match.group(2)


def _looks_like_intended_migration(filename):
   if filename.startswith("__"):
      return False
   return filename.endswith(".sql")


def _read_mode_marker(package_root, marker_name):
   try:
      marker = package_root / marker_name
      if hasattr(marker, "is_file"):
         if not marker.is_file():
            return None
      else:
         from pathlib import Path
         if not (Path(str(package_root)) / marker_name).is_file():
            return None
   except (FileNotFoundError, NotADirectoryError):
      return None
   if hasattr(marker, "read_text"):
      return marker.read_text().strip()
   return (package_root / marker_name).read_text().strip()


def _discover_migration_summaries():
   """Return sorted ``_MigrationSummary`` objects for all packaged migrations.

   Intentionally lighter than the full ``discover_migrations()`` in
   ``migration.py`` -- it omits the raw SQL bytes and validation logic that
   only the DDL runner needs, keeping this module dependency-free.
   """
   package_root = resources.files(_MIGRATIONS_PACKAGE)
   entries = sorted(
      entry.name for entry in package_root.iterdir() if entry.is_file()
   )
   summaries = []
   for filename in entries:
      if not _looks_like_intended_migration(filename):
         continue
      parsed = _parse_filename(filename)
      if parsed is None:
         raise ValueError(
            "malformed migration filename %r; expected NNNN_name.sql"
            % (filename,))
      version, name = parsed
      sql_bytes = package_root.joinpath(filename).read_bytes()
      checksum = hashlib.sha256(sql_bytes).hexdigest()
      mode = "transactional"
      marker_mode = _read_mode_marker(package_root, filename + ".mode")
      if marker_mode is not None:
         if marker_mode not in _SUPPORTED_MODES:
            raise ValueError(
               "unsupported migration mode %r in %s" % (marker_mode, filename))
         mode = marker_mode
      summaries.append(_MigrationSummary(
         version=version, name=name, checksum=checksum, mode=mode))
   if not summaries:
      raise ValueError(
         "no migrations found; every installation must ship at least "
         "migration 1")
   summaries.sort(key=lambda s: s.version)
   return tuple(summaries)


def expected_migration_rows():
   """Return the exact ``(version, name, checksum, transactional)`` tuples
   that the schema_migrations ledger must contain after all packaged
   migrations have been applied.

   Used by the web preflight to verify that the live database matches the
   exact migration history this binary was built against.
   """
   return tuple(
      (s.version, s.name, s.checksum, s.mode == "transactional")
      for s in _discover_migration_summaries()
   )
