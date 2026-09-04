"""node_monitor.collector.hardware -- hwinfo refresh policy + upsert.

Daemon-side only: unlike remote_probe.py (stdlib-only, runs on the monitored
node), this runs on the daemon host and may use project dependencies
(sqlalchemy is already a requirement -- see requirements.txt).

Not wired into a daemon here -- the daemon does not exist yet (see the
hwinfo task write-up). This module is the pure decision function plus an
upsert helper, both unit-tested in isolation.
"""

import datetime
import json
import logging

from sqlalchemy import text

logger = logging.getLogger(__name__)

# Columns that reflect the physical machine, as opposed to bookkeeping
# columns (system, source_hostname, first_seen, last_verified) or columns
# that are EXPECTED to change every time a refresh is triggered by a reboot
# (boot_id, btime). Logging a WARNING every time boot_id changes would just
# be reboot noise; logging one when mem_total_kb changes underneath a boot
# that "shouldn't" have touched it is the actual signal this project wants.
_HARDWARE_FACT_COLUMNS = (
   "cpu_model", "cpu_logical", "sockets", "cores_per_socket",
   "cpu_max_freq_khz", "numa_nodes", "mem_total_kb", "swap_total_kb",
   "hugepage_size_kb", "kernel_release", "os_pretty_name", "net_fs_mounts",
   "net_ifaces", "gpus",
)

_ALL_HARDWARE_COLUMNS = _HARDWARE_FACT_COLUMNS + ("boot_id", "btime")


def needs_refresh(stored_row, current_boot_id, hardware_refresh_days=30,
                   force=False, now=None):
   """Decide whether the hwinfo loop should run for this node.

   `stored_row` is None (no row for this system/source_hostname yet) or a
   mapping with at least 'boot_id' and 'last_verified' (a timezone-aware
   datetime). Refresh when ANY of the four conditions in the task write-up
   holds -- this function is deliberately an OR of independent checks, not
   a priority chain, because each is sufficient on its own to justify the
   (cheap) cost of re-running hwinfo.
   """
   if force:
      return True
   if stored_row is None:
      return True
   if stored_row.get("boot_id") != current_boot_id:
      return True
   last_verified = stored_row.get("last_verified")
   if last_verified is None:
      # Malformed row -- treat like missing data, not like "recently
      # verified". Refusing to refresh here would wedge a node forever.
      return True
   if now is None:
      now = datetime.datetime.now(datetime.timezone.utc)
   age = now - last_verified
   return age > datetime.timedelta(days=hardware_refresh_days)


def detect_hardware_changes(stored_row, hardware):
   """Return {column: (old, new)} for every hardware fact that changed.

   Only compares _HARDWARE_FACT_COLUMNS (not boot_id/btime -- see the
   comment above them) and only when a stored row exists: a first-ever
   insert has nothing to compare against and is not a "change".
   """
   if stored_row is None:
      return {}
   changes = {}
   for col in _HARDWARE_FACT_COLUMNS:
      old = stored_row.get(col)
      new = hardware.get(col)
      if old != new:
         changes[col] = (old, new)
   return changes


_UPSERT_SQL = text("""
   INSERT INTO node_monitor.node_hardware (
      system, source_hostname, first_seen, last_verified,
      boot_id, btime, cpu_model, cpu_logical, sockets, cores_per_socket,
      cpu_max_freq_khz, numa_nodes, mem_total_kb, swap_total_kb,
      hugepage_size_kb, kernel_release, os_pretty_name, net_fs_mounts,
      net_ifaces, gpus, probe_version
   ) VALUES (
      :system, :source_hostname, :first_seen, :last_verified,
      :boot_id, :btime, :cpu_model, :cpu_logical, :sockets,
      :cores_per_socket, :cpu_max_freq_khz, :numa_nodes, :mem_total_kb,
      :swap_total_kb, :hugepage_size_kb, :kernel_release, :os_pretty_name,
      :net_fs_mounts, CAST(:net_ifaces AS jsonb), CAST(:gpus AS jsonb),
      :probe_version
   )
   ON CONFLICT (system, source_hostname) DO UPDATE SET
      last_verified = EXCLUDED.last_verified,
      boot_id = EXCLUDED.boot_id,
      btime = EXCLUDED.btime,
      cpu_model = EXCLUDED.cpu_model,
      cpu_logical = EXCLUDED.cpu_logical,
      sockets = EXCLUDED.sockets,
      cores_per_socket = EXCLUDED.cores_per_socket,
      cpu_max_freq_khz = EXCLUDED.cpu_max_freq_khz,
      numa_nodes = EXCLUDED.numa_nodes,
      mem_total_kb = EXCLUDED.mem_total_kb,
      swap_total_kb = EXCLUDED.swap_total_kb,
      hugepage_size_kb = EXCLUDED.hugepage_size_kb,
      kernel_release = EXCLUDED.kernel_release,
      os_pretty_name = EXCLUDED.os_pretty_name,
      net_fs_mounts = EXCLUDED.net_fs_mounts,
      net_ifaces = EXCLUDED.net_ifaces,
      gpus = EXCLUDED.gpus,
      probe_version = EXCLUDED.probe_version
   -- first_seen is intentionally absent from the UPDATE SET: it must
   -- survive every re-verification untouched (task write-up, TASK 3).
""")


def upsert_hardware(conn, system, source_hostname, hardware, probe_version,
                     stored_row=None, now=None):
   """Insert or refresh the node_hardware row for one node.

   `conn` is anything with a SQLAlchemy-connection-shaped `.execute(stmt,
   params)` (a real Engine/Connection in production, a stub in tests).
   `hardware` is the `hardware` object produced by remote_probe.py's hwinfo
   loop. `stored_row`, if given, is the previously-stored row (see
   needs_refresh) and is used only to decide what changed and what
   first_seen should be -- the actual conflict resolution happens in
   Postgres via ON CONFLICT, not in Python, so this stays correct even if
   two writers race.

   Returns the {column: (old, new)} dict of changes that were logged.
   """
   if now is None:
      now = datetime.datetime.now(datetime.timezone.utc)
   first_seen = stored_row["first_seen"] if stored_row is not None else now

   changes = detect_hardware_changes(stored_row, hardware)
   for col, (old, new) in changes.items():
      # WARNING, not INFO: a login node's RAM (or any other "fact")
      # changing underneath a capacity study invalidates every ratio
      # computed against the old denominator -- an operator needs to see
      # this, not just have it in a debug log nobody reads.
      logger.warning(
         "node_hardware.%s changed for %s/%s: %r -> %r",
         col, system, source_hostname, old, new)

   params = {
      "system": system,
      "source_hostname": source_hostname,
      "first_seen": first_seen,
      "last_verified": now,
      "probe_version": probe_version,
   }
   for col in _ALL_HARDWARE_COLUMNS:
      params[col] = hardware.get(col)
   # net_ifaces/gpus are Python dict/list; the driver has no jsonb adapter
   # for those without an explicit registration, so serialize here and let
   # the CAST(:x AS jsonb) in the SQL do the rest -- same approach either
   # way, without depending on psycopg2-specific extras.
   params["net_ifaces"] = json.dumps(hardware.get("net_ifaces"))
   params["gpus"] = json.dumps(hardware.get("gpus"))

   conn.execute(_UPSERT_SQL, params)
   return changes
