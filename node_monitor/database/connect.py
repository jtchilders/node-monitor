"""Opening connections, and telling a database outage apart from a bad statement or row."""

import math

import psycopg
from psycopg.conninfo import conninfo_to_dict

# Set as startup options so they hold for the whole session, survive ROLLBACK, and cost no
# extra round trip or transaction.
_SESSION_OPTIONS = "-c search_path=node_monitor,public -c TimeZone=UTC"

_PROGRAM_LIMIT_CLASS = "54"
_QUERY_CANCELED = "57014"
_READ_ONLY_TRANSACTION = "25006"


class DatabaseUnavailable(Exception):
   """The database cannot be reached or is not accepting work; the operation may succeed
   if retried later. Callers raise it to report an outage without exposing psycopg."""


def connect(
   url: str,
   *,
   connect_timeout_sec: float = 10,
   autocommit: bool = False,
   lock_timeout: str | None = None,
) -> psycopg.Connection:
   """Open a session with search_path and TimeZone set, and `lock_timeout` (e.g. "30s") if
   given; a lock wait beyond it fails with LockNotAvailable, which is_unavailable accepts."""
   params = conninfo_to_dict(url)
   options = _SESSION_OPTIONS
   if lock_timeout is not None:
      options += f" -c lock_timeout={lock_timeout}"
   # Appended, not replaced, so options already in the URL still apply.
   params["options"] = " ".join(filter(None, [params.get("options"), options]))
   # libpq accepts only whole seconds.
   params["connect_timeout"] = max(1, math.ceil(connect_timeout_sec))
   return psycopg.connect(autocommit=autocommit, **params)


def is_unavailable(exc: BaseException) -> bool:
   """True when `exc` means the server is down, unreachable, shutting down or timed out.

   psycopg raises OperationalError (and subclasses such as AdminShutdown and
   ConnectionTimeout) for failures of the connection or the server rather than of the
   statement, so retrying later is the right response. Programming and data errors would
   fail again on retry and are False.

   Two OperationalError families are False because they depend on the data, not the
   server: a program limit (SQLSTATE class 54, e.g. an index row or jsonb value too large)
   and a statement timeout (QueryCanceled). A batch that hits one fails identically on
   every replay, so treating it as an outage would spool it and block every batch behind it.

   A read-only transaction (25006, an InternalError) is True: it means the server is a
   standby, e.g. during a failover, and writes will succeed once a primary is back.
   """
   if isinstance(exc, DatabaseUnavailable):
      return True
   if isinstance(exc, psycopg.Error) and exc.sqlstate == _READ_ONLY_TRANSACTION:
      return True
   if not isinstance(exc, psycopg.OperationalError):
      return False
   # Matched by SQLSTATE: psycopg makes StatementTooComplex, TooManyColumns and the other
   # class 54 errors siblings of ProgramLimitExceeded, not subclasses.
   sqlstate = exc.sqlstate or ""
   return not (sqlstate.startswith(_PROGRAM_LIMIT_CLASS) or sqlstate == _QUERY_CANCELED)
