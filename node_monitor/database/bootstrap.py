"""Creating the roles, the database and the schema, and owning every grant.

Migrations name no roles, so everything the reader role needs is granted here, by the
admin. `database migrate` must run as the owner so that the default privileges set below
apply to the tables and partitions it creates.
"""

import re
from urllib.parse import urlsplit, urlunsplit

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

SCHEMA = "node_monitor"
_NAME = re.compile(r"^[a-z_][a-z0-9_]*$")


def _with_dbname(url: str, dbname: str) -> str:
   parts = urlsplit(url)
   if parts.scheme in ("postgresql", "postgres"):
      return urlunsplit(parts._replace(path="/" + dbname))
   params = conninfo_to_dict(url)
   params["dbname"] = dbname
   return make_conninfo(**params)


def _check_name(kind: str, name: str) -> None:
   if not _NAME.match(name):
      raise ValueError(f"invalid {kind} name {name!r}: must match {_NAME.pattern}")


def _create_role(conn: psycopg.Connection, name: str) -> bool:
   if conn.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (name,)).fetchone():
      return False
   conn.execute(
      sql.SQL("CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE").format(
         sql.Identifier(name)
      )
   )
   return True


def bootstrap(
   admin_url: str,
   *,
   database: str = "node_monitor",
   owner: str = "node_monitor_owner",
   reader: str = "node_monitor_reader",
) -> list[str]:
   """Create what is missing and return a description of each change made.

   Existing roles and databases are never altered. Grants are idempotent and are
   re-applied every run, but reported only when something was created.
   """
   for kind, name in (("database", database), ("owner", owner), ("reader", reader)):
      _check_name(kind, name)
   db, own, rd = sql.Identifier(database), sql.Identifier(owner), sql.Identifier(reader)
   actions: list[str] = []

   with psycopg.connect(admin_url, autocommit=True) as conn:
      for name in (owner, reader):
         if _create_role(conn, name):
            actions.append(f"created role {name}")
      if not conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database,)).fetchone():
         conn.execute(sql.SQL("CREATE DATABASE {} OWNER {}").format(db, own))
         actions.append(f"created database {database}")

   with psycopg.connect(_with_dbname(admin_url, database), autocommit=True) as conn:
      conn.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(db, rd))
      if not conn.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s", (SCHEMA,)).fetchone():
         conn.execute(
            sql.SQL("CREATE SCHEMA {} AUTHORIZATION {}").format(sql.Identifier(SCHEMA), own)
         )
         actions.append(f"created schema {SCHEMA}")
      schema = sql.Identifier(SCHEMA)
      conn.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(schema, rd))
      # Default privileges cover tables and partitions the owner creates later; the explicit
      # grant covers those that exist already when bootstrap is re-run.
      conn.execute(
         sql.SQL(
            "ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA {} GRANT SELECT ON TABLES TO {}"
         ).format(own, schema, rd)
      )
      conn.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(schema, rd))
   return actions
