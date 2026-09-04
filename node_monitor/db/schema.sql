-- node_monitor.node_hardware -- one row per node, read-once inventory.
--
-- This is deliberately NOT a time series like the counter/census tables:
-- the fields here are physical and static (total RAM, socket/core counts,
-- kernel release), so storing one row per boot-worth-of-truth per node is
-- what makes it usable as a denominator ("the node was 78% full") instead
-- of 1440 redundant reads a day.
--
-- Two Postgres pitfalls already bit this project (see the hwinfo task
-- write-up) and are why the schema looks the way it does:
--
-- 1. A nullable column in a PRIMARY KEY is implicitly NOT NULL, and NULLs
--    do not compare equal to each other, so an upsert keyed on a
--    NULL-bearing column silently duplicates instead of conflicting.
--    Both PK columns below (system, source_hostname) are NOT NULL for
--    exactly this reason.
-- 2. round(double precision, int) does not exist in Postgres -- cast to
--    numeric first. Not exercised by this DDL, but relevant to any example
--    query written against it.

CREATE SCHEMA IF NOT EXISTS node_monitor;

CREATE TABLE IF NOT EXISTS node_monitor.node_hardware (
   system            text        NOT NULL,
   source_hostname   text        NOT NULL,
   first_seen        timestamptz NOT NULL,
   last_verified     timestamptz NOT NULL,
   boot_id           text,
   btime             bigint,
   cpu_model         text,
   cpu_logical       int,
   sockets           int,
   cores_per_socket  int,
   cpu_max_freq_khz  bigint,
   numa_nodes        int,
   mem_total_kb      bigint,
   swap_total_kb     bigint,
   hugepage_size_kb  int,
   kernel_release    text,
   os_pretty_name    text,
   net_fs_mounts     int,
   net_ifaces        jsonb,
   gpus              jsonb,
   probe_version     int         NOT NULL,
   PRIMARY KEY (system, source_hostname)
);
