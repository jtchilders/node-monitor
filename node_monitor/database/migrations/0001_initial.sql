-- Initial node_monitor schema.
--
-- The schema itself, roles, grants and default privileges are created by
-- `node-monitor database bootstrap`, so this file creates tables only and names no role:
-- it must apply on any server, as whichever role owns the schema.
--
-- Column sets follow the pipeline's record tuples (LIVE_PROCESS_COLUMNS,
-- NODE_METRICS_COLUMNS, USER_USAGE_COLUMNS, NODE_USAGE_COLUMNS); a test keeps them equal.
-- Counters that can grow are bigint; pid, ppid and uid fit integer.
--
-- Time-series parents are range-partitioned by month on their time column. They get no
-- partitions here and deliberately no DEFAULT partition: a row for a month with no
-- partition must fail loudly (the writer then creates it), never collect in a catch-all
-- that would block creating that month's partition later.

-- Live tier: current state only, replaced in place.

-- Every process of the node's latest census, replaced per node in one transaction.
CREATE TABLE node_monitor.live_processes (
   system text NOT NULL,
   node text NOT NULL,
   ts timestamptz NOT NULL,
   pid integer NOT NULL,
   start_time bigint NOT NULL,
   uid integer,
   username text,
   comm text,
   exe text,
   argv0 text,
   cmdline text,
   ppid integer,
   parent_comm text,
   parent_exe text,
   parent_kind text,
   category text,
   activity text,
   state text,
   threads bigint,
   rss_kb bigint,
   cpu_seconds double precision,
   cpu_state text,
   gpu_mem_mb double precision,
   interactive boolean,
   under_screen_or_tmux boolean,
   idle_for_sec double precision,
   idle_for_is_lower_bound boolean,
   idle_bucket text,
   owner_presence text,
   is_user_process boolean,
   -- start_time (clock ticks since boot) makes a recycled pid a different process.
   PRIMARY KEY (system, node, pid, start_time)
);

CREATE INDEX live_processes_system_username_idx
   ON node_monitor.live_processes (system, username);

-- Time of each node's current live_processes snapshot. Kept apart from the process rows so
-- an empty snapshot still has a timestamp that an older, replayed snapshot is compared
-- against.
CREATE TABLE node_monitor.live_snapshots (
   system text NOT NULL,
   node text NOT NULL,
   ts timestamptz NOT NULL,
   PRIMARY KEY (system, node)
);

-- Latest gauges and session state per node. Rows are updated partially: a column absent
-- from a record keeps its value, so a lost session does not null out the last gauges.
CREATE TABLE node_monitor.live_nodes (
   system text NOT NULL,
   node text NOT NULL,
   label text,
   -- Every live_node record carries these two; updated_at is what a replayed, older
   -- record is compared against.
   updated_at timestamptz NOT NULL,
   session_state text NOT NULL,
   boot_id text,
   probe_version integer,
   load1 double precision,
   mem_used_kb bigint,
   mem_total_kb bigint,
   procs_total bigint,
   last_census_at timestamptz,
   PRIMARY KEY (system, node)
);

-- Inventory: kept forever.

CREATE TABLE node_monitor.nodes (
   system text NOT NULL,
   node text NOT NULL,
   label text,
   first_seen timestamptz NOT NULL,
   last_seen timestamptz NOT NULL,
   boot_id text,
   PRIMARY KEY (system, node)
);

-- One row per boot of each node, from the probe's hello message.
CREATE TABLE node_monitor.node_hardware (
   system text NOT NULL,
   node text NOT NULL,
   boot_id text NOT NULL,
   btime bigint,
   collected_at timestamptz NOT NULL,
   probe_version integer,
   python_version text,
   hostname_fqdn text,
   cpu_model text,
   cpu_logical integer,
   sockets integer,
   cores_per_socket integer,
   cpu_max_freq_khz bigint,
   numa_nodes integer,
   mem_total_kb bigint,
   swap_total_kb bigint,
   hugepage_size_kb bigint,
   kernel_release text,
   os_pretty_name text,
   net_fs_mounts integer,
   net_ifaces jsonb,
   gpus jsonb,
   PRIMARY KEY (system, node, boot_id)
);

-- Long-term tier: monthly partitions, two years by default.

CREATE TABLE node_monitor.node_metrics_1m (
   system text NOT NULL,
   node text NOT NULL,
   minute timestamptz NOT NULL,
   cpu_pct jsonb,
   load1 double precision,
   load5 double precision,
   load15 double precision,
   procs_running bigint,
   procs_total bigint,
   socket_count bigint,
   mem_total_kb bigint,
   mem_available_kb bigint,
   mem_cached_kb bigint,
   mem_shmem_kb bigint,
   mem_used_kb bigint,
   swap_used_kb bigint,
   network_rates jsonb,
   lustre_md_rates jsonb,
   gpus jsonb,
   gpu_status text,
   sample_latency_sec double precision,
   clock_offset_sec double precision,
   probe_cpu_seconds double precision,
   probe_maxrss_kb bigint,
   probe_rss_kb bigint,
   sample_count bigint,
   expected_count bigint,
   PRIMARY KEY (system, node, minute)
) PARTITION BY RANGE (minute);

-- System-wide time-range scans; the primary key already serves per-node ones.
CREATE INDEX node_metrics_1m_system_minute_idx
   ON node_monitor.node_metrics_1m (system, minute DESC);

CREATE TABLE node_monitor.user_usage_5m (
   system text NOT NULL,
   node text NOT NULL,
   username text NOT NULL,
   interval_start timestamptz NOT NULL,
   process_count_avg double precision,
   process_count_max bigint,
   cpu_seconds double precision,
   rss_kb_avg double precision,
   rss_kb_max bigint,
   gpu_mem_mb_avg double precision,
   gpu_mem_mb_max double precision,
   threads_avg double precision,
   d_state_fraction double precision,
   interactive_fraction double precision,
   by_category jsonb,
   by_activity jsonb,
   by_dormancy jsonb,
   by_category_dormancy jsonb,
   sample_count bigint,
   expected_count bigint,
   unmeasured_count bigint,
   PRIMARY KEY (system, node, username, interval_start)
) PARTITION BY RANGE (interval_start);

CREATE INDEX user_usage_5m_system_interval_idx
   ON node_monitor.user_usage_5m (system, interval_start DESC);
-- One user's history across all nodes.
CREATE INDEX user_usage_5m_system_username_interval_idx
   ON node_monitor.user_usage_5m (system, username, interval_start DESC);

CREATE TABLE node_monitor.node_usage_5m (
   system text NOT NULL,
   node text NOT NULL,
   interval_start timestamptz NOT NULL,
   process_count_avg double precision,
   process_count_max bigint,
   cpu_seconds double precision,
   rss_kb_avg double precision,
   rss_kb_max bigint,
   gpu_mem_mb_avg double precision,
   gpu_mem_mb_max double precision,
   threads_avg double precision,
   d_state_fraction double precision,
   interactive_fraction double precision,
   by_category jsonb,
   by_activity jsonb,
   by_dormancy jsonb,
   by_category_dormancy jsonb,
   sample_count bigint,
   expected_count bigint,
   unmeasured_count bigint,
   user_count_avg double precision,
   user_count_max bigint,
   active_user_count_avg double precision,
   PRIMARY KEY (system, node, interval_start)
) PARTITION BY RANGE (interval_start);

CREATE INDEX node_usage_5m_system_interval_idx
   ON node_monitor.node_usage_5m (system, interval_start DESC);

-- Ops.

-- A primary key on a partitioned table must include the partition key, hence (id, ts).
-- node is NULL for collector-level events.
CREATE TABLE node_monitor.collection_events (
   id bigint GENERATED ALWAYS AS IDENTITY,
   ts timestamptz NOT NULL,
   system text NOT NULL,
   node text NULL,
   event_type text NOT NULL,
   detail jsonb NOT NULL,
   PRIMARY KEY (id, ts)
) PARTITION BY RANGE (ts);

CREATE INDEX collection_events_system_ts_idx
   ON node_monitor.collection_events (system, ts DESC);

CREATE TABLE node_monitor.collector_heartbeat (
   system text PRIMARY KEY,
   collector_host text NOT NULL,
   version text NOT NULL,
   started_at timestamptz NOT NULL,
   last_seen timestamptz NOT NULL,
   status jsonb NOT NULL
);
