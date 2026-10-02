"""node_monitor.database.schema_contract -- canonical, immutable migration
type, resource discovery, contiguity validation, and schema catalog used by
both the operator migration runner (migration.py) and the read-only web
preflight (web.py).

This module is the single authoritative source for:

  * ``Migration`` -- frozen, validated migration value type.
  * ``_SUPPORTED_MODES`` -- frozenset of valid execution strategies.
  * ``_validate_contiguous`` -- contiguity check for a sequence of Migrations.
  * ``discover_migrations`` -- fail-closed importlib.resources discovery.
  * ``_SOURCE_SCHEMA_COLUMNS`` -- exact column/type/nullability catalog for all
    five telemetry tables created by migration 1.
  * ``_REQUIRED_SOURCE_CONSTRAINTS`` -- frozenset of required constraint names.
  * ``_REQUIRED_SOURCE_INDEXES`` -- frozenset of required index names.
  * ``compare_source_schema`` -- compare an actual catalog dict against the
    expected one; returns None on match, error string on drift.
  * ``expected_migration_rows`` -- ledger row tuples for web preflight.

Dependency direction:

    migration.py ──imports──▶ schema_contract.py
    web.py       ──imports──▶ schema_contract.py

``schema_contract.py`` imports neither ``migration.py`` nor ``web.py``.
"""

import dataclasses
import hashlib
import importlib.resources as resources
import re

# ---------------------------------------------------------------------------
# Supported migration execution modes
# ---------------------------------------------------------------------------

_SUPPORTED_MODES = frozenset({"transactional"})

# ---------------------------------------------------------------------------
# Internal migration-resource constants
# ---------------------------------------------------------------------------

_MIGRATIONS_PACKAGE = "node_monitor.database.migrations.versions"

# NNNN_name.sql -- exactly 4 digits, underscore, a name using only
# letters/digits/underscores, then ".sql". Deliberately strict: no
# hyphens, no spaces, no missing segments.
_FILENAME_PATTERN = re.compile(r"^(\d{4})_([A-Za-z0-9_]+)\.sql$")

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
# Exact column/type/nullability catalog for the five telemetry tables.
# Each value is a tuple of (column_name, udt_name, is_nullable) tuples,
# in ordinal order.  This is the ground truth used by both the migration
# runner postcondition and the web preflight schema drift check.
# ---------------------------------------------------------------------------

_SOURCE_SCHEMA_COLUMNS = {
   "node_hardware": (
      ("system", "text", "NO"), ("source_hostname", "text", "NO"),
      ("first_seen", "timestamptz", "NO"),
      ("last_verified", "timestamptz", "NO"), ("boot_id", "text", "YES"),
      ("btime", "int8", "YES"), ("cpu_model", "text", "YES"),
      ("cpu_logical", "int4", "YES"), ("sockets", "int4", "YES"),
      ("cores_per_socket", "int4", "YES"),
      ("cpu_max_freq_khz", "int8", "YES"),
      ("numa_nodes", "int4", "YES"), ("mem_total_kb", "int8", "YES"),
      ("swap_total_kb", "int8", "YES"),
      ("hugepage_size_kb", "int4", "YES"),
      ("kernel_release", "text", "YES"),
      ("os_pretty_name", "text", "YES"),
      ("net_fs_mounts", "int4", "YES"), ("net_ifaces", "jsonb", "YES"),
      ("gpus", "jsonb", "YES"), ("probe_version", "int4", "NO"),
   ),
   "node_counter_minute": (
      ("system", "text", "NO"), ("source_hostname", "text", "NO"),
      ("window_start", "timestamptz", "NO"),
      ("window_end", "timestamptz", "NO"),
      ("collector_hostname", "text", "NO"),
      ("probe_version", "int4", "NO"), ("daemon_version", "text", "NO"),
      ("sample_count", "int4", "NO"), ("expected_count", "int4", "NO"),
      ("coverage", "float8", "NO"), ("mem_available_kb", "int8", "YES"),
      ("cached_kb", "int8", "YES"), ("shmem_kb", "int8", "YES"),
      ("load1", "float8", "YES"), ("load5", "float8", "YES"),
      ("load15", "float8", "YES"), ("procs_running", "int4", "YES"),
      ("procs_total", "int4", "YES"), ("socket_count", "int4", "YES"),
      ("cpu_busy_pct", "jsonb", "YES"),
      ("network_rates", "jsonb", "YES"),
      ("lustre_md_summary", "jsonb", "YES"),
      ("meets_minimum_samples", "bool", "NO"),
      ("invalid_pair_count", "int4", "NO"),
      ("excess_sample_count", "int4", "NO"),
   ),
   "node_usage_intervals": (
      ("id", "int8", "NO"), ("system", "text", "NO"),
      ("source_hostname", "text", "NO"),
      ("interval_start", "timestamptz", "NO"),
      ("interval_end", "timestamptz", "NO"),
      ("category", "text", "NO"), ("activity", "text", "NO"),
      ("username", "text", "YES"), ("username_key", "text", "YES"),
      ("process_count", "jsonb", "NO"),
      ("cpu_seconds", "float8", "NO"), ("rss_kb", "jsonb", "NO"),
      ("d_state_fraction", "float8", "NO"),
      ("interactivity_fraction", "float8", "NO"),
      ("sample_count", "int4", "NO"), ("expected_count", "int4", "NO"),
      ("unmeasured_count", "int4", "NO"),
   ),
   "node_poll_failures": (
      ("id", "int8", "NO"), ("system", "text", "NO"),
      ("source_hostname", "text", "NO"), ("loop", "text", "NO"),
      ("recorded_at", "timestamptz", "NO"),
      ("failure_type", "text", "NO"), ("detail", "text", "NO"),
      ("consecutive_failures", "int4", "NO"),
      ("breaker_state", "text", "NO"),
   ),
   "node_collection_log": (
      ("id", "int8", "NO"), ("system", "text", "NO"),
      ("recorded_at", "timestamptz", "NO"), ("event", "text", "NO"),
      ("detail", "jsonb", "NO"),
   ),
}

_REQUIRED_SOURCE_CONSTRAINTS = frozenset({
   "node_hardware_pkey", "node_hardware_time_check",
   "node_counter_minute_pkey", "node_counter_minute_window_check",
   "node_usage_intervals_pkey", "node_usage_intervals_grain_key",
   "node_usage_intervals_window_check", "node_usage_intervals_username_check",
   "node_poll_failures_pkey", "node_collection_log_pkey",
})

_REQUIRED_SOURCE_INDEXES = frozenset({
   "node_counter_minute_system_time_idx", "node_counter_minute_retention_idx",
   "node_usage_intervals_system_time_idx", "node_usage_intervals_retention_idx",
   "node_poll_failures_system_node_time_idx",
   "node_poll_failures_retention_idx",
   "node_collection_log_system_time_idx", "node_collection_log_retention_idx",
})


def compare_source_schema(actual):
   """Compare an actual schema catalog dict against ``_SOURCE_SCHEMA_COLUMNS``.

   ``actual`` must be a dict of the form::

       {table_name: [(column_name, udt_name, is_nullable), ...], ...}

   Returns ``None`` if ``actual`` exactly matches the expected catalog, or a
   non-empty string describing the first discovered discrepancy otherwise.
   The comparison is order-sensitive (column order matters).
   """
   expected = {table: list(cols) for table, cols in _SOURCE_SCHEMA_COLUMNS.items()}
   if actual == expected:
      return None
   # Report first discrepancy for diagnostics.
   for table in expected:
      if table not in actual:
         return "missing table %r" % table
      if list(actual[table]) != expected[table]:
         return "column mismatch in table %r" % table
   for table in actual:
      if table not in expected:
         return "unexpected table %r in actual schema" % table
   return "schema mismatch"

# ---------------------------------------------------------------------------
# Migration value type
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class Migration:
   """One immutable, validated migration resource.

   ``sql`` holds the exact packaged bytes -- never decoded, re-encoded,
   or newline-normalized. ``checksum`` is the lowercase hex SHA-256 of
   those exact bytes, and construction fails closed if the checksum
   passed in does not match. ``mode`` names the execution strategy a
   future runner will use; only ``"transactional"`` is supported in
   this increment.
   """

   version: int
   name: str
   sql: bytes
   checksum: str
   mode: str

   def __post_init__(self):
      if not isinstance(self.version, int) or isinstance(self.version, bool):
         raise TypeError("version must be an int, got %r" % (self.version,))
      if self.version <= 0:
         raise ValueError(
            "version must be a positive integer, got %r" % (self.version,))
      if not isinstance(self.name, str) or not self.name:
         raise ValueError("name must be a non-empty string")
      if not isinstance(self.sql, (bytes, bytearray)):
         raise TypeError("sql must be bytes, got %r" % (type(self.sql),))
      if isinstance(self.sql, bytearray):
         object.__setattr__(self, "sql", bytes(self.sql))
      if self.mode not in _SUPPORTED_MODES:
         raise ValueError(
            "unsupported migration mode %r; only %r is supported this "
            "increment" % (self.mode, sorted(_SUPPORTED_MODES)))
      expected_checksum = hashlib.sha256(self.sql).hexdigest()
      if not isinstance(self.checksum, str) or self.checksum != self.checksum.lower():
         raise ValueError(
            "checksum must be a lowercase hex string, got %r"
            % (self.checksum,))
      if self.checksum != expected_checksum:
         raise ValueError(
            "checksum %r does not match SHA-256 of exact SQL bytes "
            "(expected %r)" % (self.checksum, expected_checksum))

# ---------------------------------------------------------------------------
# Filename parsing helpers
# ---------------------------------------------------------------------------


def _parse_filename(filename):
   """Return (version, name) for a well-formed ``NNNN_name.sql``
   filename, or None if it does not match the required shape at all
   (used to silently skip unrelated files such as ``__init__.py``).
   """
   match = _FILENAME_PATTERN.match(filename)
   if match is None:
      return None
   version_str, name = match.group(1), match.group(2)
   return int(version_str), name


def _looks_like_intended_migration(filename):
   """True if a filename is clearly *meant* to be a migration (ends in
   .sql and isn't a dunder file) even if it is malformed -- so a typo'd
   filename fails closed with a clear error instead of being silently
   skipped.
   """
   if filename.startswith("__"):
      return False
   return filename.endswith(".sql")


def _read_mode_marker(directory_or_traversable, marker_name):
   """Return the stripped text content of a ``<file>.sql.mode`` sidecar
   if present, else None. This function only reads and strips whatever
   text is present -- it does not itself validate or reject the mode
   value; that check happens in ``Migration.__post_init__`` when the
   returned string is used to construct the ``Migration``, so an
   unsupported mode fails closed there rather than being guessed here.
   """
   try:
      marker = directory_or_traversable / marker_name
      if hasattr(marker, "is_file"):
         if not marker.is_file():
            return None
      else:
         from pathlib import Path
         if not (Path(directory_or_traversable) / marker_name).is_file():
            return None
   except (FileNotFoundError, NotADirectoryError):
      return None
   if hasattr(marker, "read_text"):
      return marker.read_text().strip()
   return (directory_or_traversable / marker_name).read_text().strip()

# ---------------------------------------------------------------------------
# Contiguity validation
# ---------------------------------------------------------------------------


def _validate_contiguous(migrations):
   """Raise ValueError if migration versions are not positive, unique,
   and contiguous starting at 1.
   """
   versions = [m.version for m in migrations]
   seen = set()
   for version in versions:
      if version in seen:
         raise ValueError("duplicate migration version: %d" % version)
      seen.add(version)
   ordered = sorted(seen)
   expected = list(range(1, len(ordered) + 1))
   if ordered != expected:
      raise ValueError(
         "migration versions must be contiguous starting at 1; found %r"
         % (ordered,))

# ---------------------------------------------------------------------------
# Migration assembly helpers
# ---------------------------------------------------------------------------


def _build_migration(directory_or_traversable, filename, version, name,
                     read_bytes):
   sql = read_bytes(filename)
   mode = "transactional"
   mode_marker_name = filename + ".mode"
   marker_mode = _read_mode_marker(directory_or_traversable, mode_marker_name)
   if marker_mode is not None:
      mode = marker_mode
   checksum = hashlib.sha256(sql).hexdigest()
   return Migration(version=version, name=name, sql=sql, checksum=checksum,
                    mode=mode)


def _migrations_from_entries(entries, read_bytes, container):
   """Shared assembly logic given an iterable of filenames present in
   a migration source (a directory or a packaged resource
   traversable), a ``read_bytes(filename)`` callable, and the
   container itself (used to look up ``.mode`` sidecar files).
   """
   migrations = []
   for filename in entries:
      if not _looks_like_intended_migration(filename):
         continue
      parsed = _parse_filename(filename)
      if parsed is None:
         raise ValueError(
            "malformed migration filename %r; expected NNNN_name.sql"
            % (filename,))
      version, name = parsed
      migrations.append(
         _build_migration(container, filename, version, name, read_bytes))
   if not migrations:
      raise ValueError(
         "no migrations found; every installation must ship at least "
         "migration 1 (expected files matching NNNN_name.sql)")
   migrations.sort(key=lambda m: m.version)
   _validate_contiguous(migrations)
   return migrations


def _migrations_from_directory(directory):
   """Discover migrations from a plain filesystem directory. Used
   directly by tests to exercise filename-parsing and
   version-contiguity edge cases without needing to rebuild the
   packaged resource tree; production discovery goes through
   ``discover_migrations()`` / ``importlib.resources`` instead.
   """
   from pathlib import Path
   directory = Path(directory)
   entries = sorted(
      entry.name for entry in directory.iterdir() if entry.is_file()
   )

   def read_bytes(filename):
      return (directory / filename).read_bytes()

   return _migrations_from_entries(entries, read_bytes, directory)

# ---------------------------------------------------------------------------
# Public discovery API
# ---------------------------------------------------------------------------


def discover_migrations():
   """Return the sorted, validated tuple of packaged ``Migration``
   resources from ``node_monitor.database.migrations.versions``, read
   via ``importlib.resources`` so behavior is identical under a wheel
   install, an sdist install, or editable/development mode.

   Raises ``ValueError`` if any filename is malformed, or if versions
   are non-positive, duplicated, or not contiguous from 1.
   """
   package_root = resources.files(_MIGRATIONS_PACKAGE)
   entries = sorted(
      entry.name for entry in package_root.iterdir() if entry.is_file()
   )

   def read_bytes(filename):
      return package_root.joinpath(filename).read_bytes()

   return tuple(
      _migrations_from_entries(entries, read_bytes, package_root))


def expected_migration_rows():
   """Return the exact ``(version, name, checksum, transactional)`` tuples
   that the schema_migrations ledger must contain after all packaged
   migrations have been applied.

   Used by the web preflight to verify that the live database matches the
   exact migration history this binary was built against.
   """
   migrations = discover_migrations()
   return tuple(
      (m.version, m.name, m.checksum, m.mode == "transactional")
      for m in migrations
   )
