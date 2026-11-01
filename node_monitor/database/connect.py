"""Opening connections, and telling a database outage apart from a bad statement or row."""

import math

import psycopg
from psycopg.conninfo import conninfo_to_dict

# Set as startup options so they hold for the whole session, survive ROLLBACK, and cost no
# extra round trip or transaction.
_SESSION_OPTIONS = "-c search_path=node_monitor,public -c TimeZone=UTC"


class DatabaseUnavailable(Exception):
   """The database cannot be reached or is not accepting work; the operation may succeed
   if retried later. Callers raise it to report an outage without exposing psycopg."""


def connect(
   url: str, *, connect_timeout_sec: float = 10, autocommit: bool = False
) -> psycopg.Connection:
   params = conninfo_to_dict(url)
   # Appended, not replaced, so options already in the URL still apply.
   params["options"] = " ".join(filter(None, [params.get("options"), _SESSION_OPTIONS]))
   # libpq accepts only whole seconds.
   params["connect_timeout"] = max(1, math.ceil(connect_timeout_sec))
   return psycopg.connect(autocommit=autocommit, **params)


def is_unavailable(exc: BaseException) -> bool:
   """True when `exc` means the server is down, unreachable, shutting down or timed out.

   psycopg raises OperationalError (and subclasses such as AdminShutdown and
   ConnectionTimeout) for failures of the connection or the server rather than of the
   statement, so retrying later is the right response. Programming and data errors would
   fail again on retry and are False.
   """
   return isinstance(exc, (DatabaseUnavailable, psycopg.OperationalError))
