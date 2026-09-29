"""node_monitor.database.connection -- injected, PostgreSQL-only
connection factory.

Design: node_monitor_planning tiered-storage-retention-design.md
"Database section" ("explicitly injected, not a mutable global
singleton"; "must not execute CREATE SCHEMA, create_all, drop_all,
VACUUM, or any server lifecycle operation"). Plan:
increment1-nested-config-pg-connection.md Task 4.

``NodeMonitorDB`` wraps a single SQLAlchemy ``Engine`` built from an
explicitly injected ``node_monitor.config.DatabaseConfig``. It never
reads ambient environment/config itself, never creates a global
singleton, and performs no connection/DDL/lifecycle operation at
construction time -- ``create_engine`` is lazy on its own; the only
extra behavior this class adds at construction is registering a
``connect`` event listener that sets the already-provisioned
``node_monitor`` search path on every new DBAPI connection.
"""

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError


class NodeMonitorDB:
   """Injected PostgreSQL connection wrapper.

   No schema/table creation, migration, or any other PostgreSQL
   lifecycle operation belongs here or is ever issued by this class --
   that is explicitly deferred to a later, operator-run increment.
   """

   def __init__(self, config):
      self._config = config
      self._engine = create_engine(
         config.url,
         pool_size=config.pool_size,
         max_overflow=config.max_overflow,
         echo=config.echo_sql,
         pool_pre_ping=config.pool_pre_ping,
         pool_timeout=config.pool_timeout_sec,
         pool_recycle=config.pool_recycle_sec,
         connect_args=dict(config.connect_args),
      )
      event.listen(self._engine, "connect", self._set_search_path)

   @staticmethod
   def _set_search_path(dbapi_connection, connection_record):
      cursor = dbapi_connection.cursor()
      try:
         cursor.execute('SET search_path TO "node_monitor"')
      finally:
         cursor.close()

   def ping(self):
      """Return True if a trivial ``SELECT 1`` succeeds, False on any
      SQLAlchemy/DBAPI failure -- never raises, never exposes raw
      connection/exception details to the caller.
      """
      try:
         with self._engine.connect() as connection:
            connection.execute(text("SELECT 1"))
         return True
      except SQLAlchemyError:
         return False

   def close(self):
      self._engine.dispose()

   @staticmethod
   def mask_url(url):
      """Render ``url`` with its password hidden, using SQLAlchemy's
      own ``make_url(...).render_as_string(hide_password=True)`` --
      never a hand-rolled regex -- so percent-encoded credentials and
      every supported driver form are handled correctly.
      """
      return make_url(url).render_as_string(hide_password=True)
