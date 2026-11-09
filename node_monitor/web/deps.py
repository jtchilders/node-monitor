"""Shared dependencies for the web app."""

from typing import Generator

import psycopg

from node_monitor.config import Config
from node_monitor.database.connect import DatabaseUnavailable, connect


def web_reader(config: Config) -> Generator[psycopg.Connection, None, None]:
   """Yield a read connection from web.database_url.

   Raises psycopg.Error / DatabaseUnavailable so callers can translate to the
   health payload's db_ok flag.
   """
   url = config.web.database_url
   if not url:
      raise DatabaseUnavailable(f"web.database_url is not set in {config.source_path}")
   conn = connect(url, connect_timeout_sec=config.database.connect_timeout_sec, autocommit=True)
   try:
      yield conn
   finally:
      conn.close()
