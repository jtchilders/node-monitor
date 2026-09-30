"""node_monitor.database.migration -- immutable Migration value type and
fail-closed, deterministic discovery of packaged SQL migration
resources.

Design: node_monitor_planning increment2-schema-migrations-writer.md
Task 1 ("Migration resource model and fail-closed discovery"). This
module implements *discovery only*: no migration runner, no SQL
execution, no CLI, no final tables. Task 2 builds the transactional
runner on top of this; Task 3 replaces the placeholder SQL body of
migration 0001 with reviewed source-table DDL.

``discover_migrations()`` reads the packaged
``node_monitor.database.migrations.versions`` resource package via
``importlib.resources`` -- never the filesystem path of the developer's
working tree -- so behavior is identical whether node_monitor is
installed as a wheel, an sdist, or in editable/development mode.
"""

import dataclasses
import hashlib
import importlib.resources as resources
import re

_SUPPORTED_MODES = frozenset({"transactional"})

# NNNN_name.sql -- exactly 4 digits, underscore, a name using only
# letters/digits/underscores, then ".sql". Deliberately strict: no
# hyphens, no spaces, no missing segments.
_FILENAME_PATTERN = re.compile(r"^(\d{4})_([A-Za-z0-9_]+)\.sql$")

_MIGRATIONS_PACKAGE = "node_monitor.database.migrations.versions"


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
   if filename.startswith("__") :
      return False
   return filename.endswith(".sql")


def _build_migration(directory_or_traversable, filename, version, name,
                      read_bytes):
   sql = read_bytes(filename)
   mode = "transactional"
   mode_marker_name = filename + ".mode"
   marker_mode = _read_mode_marker(directory_or_traversable,
                                    mode_marker_name)
   if marker_mode is not None:
      mode = marker_mode
   checksum = hashlib.sha256(sql).hexdigest()
   return Migration(version=version, name=name, sql=sql, checksum=checksum,
                     mode=mode)


def _read_mode_marker(directory_or_traversable, marker_name):
   """Return the stripped text content of a ``<file>.sql.mode`` sidecar
   if present, else None. Presence of this file means the migration
   author explicitly declared a mode; only "transactional" is accepted
   here, anything else is rejected rather than guessed.
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


def _validate_contiguous(migrations):
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
