"""node_monitor.database public connection and migration API.

Migration-runner symbols (MigrationRunner, MigrationError, etc.) are
exported lazily via __getattr__ so that sub-modules that only need
node_monitor.database.web (or node_monitor.database.schema_contract)
do NOT pull in node_monitor.database.migration at import time.

Public API is fully preserved: ``from node_monitor.database import
MigrationRunner`` (and friends) still works identically from any caller.

Python >= 3.7 supports module-level __getattr__ (PEP 562).
"""

from __future__ import annotations

from node_monitor.database.connection import NodeMonitorDB
from node_monitor.database.writer import DatabaseWriteError, DatabaseWriter

# ---------------------------------------------------------------------------
# Symbols that are only available after importing migration -- listed here
# so that __all__ is correct for star-imports and IDE inspection.
# ---------------------------------------------------------------------------
_MIGRATION_EXPORTS = frozenset({
   "MigrationApplyError",
   "MigrationDriftError",
   "MigrationError",
   "MigrationLockError",
   "MigrationResult",
   "MigrationRunner",
   "MigrationStatus",
})

__all__ = [
   "DatabaseWriteError",
   "DatabaseWriter",
   "MigrationApplyError",
   "MigrationDriftError",
   "MigrationError",
   "MigrationLockError",
   "MigrationResult",
   "MigrationRunner",
   "MigrationStatus",
   "NodeMonitorDB",
]


def __getattr__(name: str):
   """Lazily import migration-runner symbols on first access.

   This satisfies PEP 562 (Python 3.7+): the interpreter calls this
   function when an attribute lookup on the *module* itself fails,
   i.e. exactly when a caller writes::

       from node_monitor.database import MigrationRunner

   The actual import of node_monitor.database.migration is deferred
   until that moment, so importing node_monitor.database.web (which
   never references MigrationRunner) does not trigger it.
   """
   if name in _MIGRATION_EXPORTS:
      import node_monitor.database.migration as _migration  # noqa: PLC0415
      value = getattr(_migration, name)
      # Cache on the module so subsequent lookups skip __getattr__.
      import sys as _sys
      setattr(_sys.modules[__name__], name, value)
      return value
   raise AttributeError(
      "module %r has no attribute %r" % (__name__, name))
