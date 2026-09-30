-- Compatibility bootstrap for node-monitor source tables.
--
-- Runtime database ownership (connections, migrations, and writers) belongs to
-- node_monitor.database. This file is retained for legacy provisioning only;
-- it intentionally defines no migration ledger. Keep it semantically aligned
-- with migration 0001's source-table DDL.

CREATE SCHEMA IF NOT EXISTS node_monitor;

CREATE TABLE IF NOT EXISTS node_monitor.node_hardware (
   system text NOT NULL,
   source_hostname text NOT NULL,
   first_seen timestamptz NOT NULL,
   last_verified timestamptz NOT NULL,
   boot_id text,
   btime bigint,
   cpu_model text,
   cpu_logical integer,
   sockets integer,
   cores_per_socket integer,
   cpu_max_freq_khz bigint,
   numa_nodes integer,
   mem_total_kb bigint,
   swap_total_kb bigint,
   hugepage_size_kb integer,
   kernel_release text,
   os_pretty_name text,
   net_fs_mounts integer,
   net_ifaces jsonb,
   gpus jsonb,
   probe_version integer NOT NULL,
   CONSTRAINT node_hardware_pkey PRIMARY KEY (system, source_hostname)
);

DO $$ BEGIN
   ALTER TABLE node_monitor.node_hardware ADD CONSTRAINT node_hardware_time_check
      CHECK (last_verified >= first_seen);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
DO $$ BEGIN
   ALTER TABLE node_monitor.node_hardware ADD CONSTRAINT node_hardware_cpu_logical_check CHECK (cpu_logical IS NULL OR cpu_logical >= 0);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
DO $$ BEGIN
   ALTER TABLE node_monitor.node_hardware ADD CONSTRAINT node_hardware_sockets_check CHECK (sockets IS NULL OR sockets >= 0);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
DO $$ BEGIN
   ALTER TABLE node_monitor.node_hardware ADD CONSTRAINT node_hardware_cores_check CHECK (cores_per_socket IS NULL OR cores_per_socket >= 0);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
DO $$ BEGIN
   ALTER TABLE node_monitor.node_hardware ADD CONSTRAINT node_hardware_freq_check CHECK (cpu_max_freq_khz IS NULL OR cpu_max_freq_khz >= 0);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
DO $$ BEGIN
   ALTER TABLE node_monitor.node_hardware ADD CONSTRAINT node_hardware_numa_check CHECK (numa_nodes IS NULL OR numa_nodes >= 0);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
DO $$ BEGIN
   ALTER TABLE node_monitor.node_hardware ADD CONSTRAINT node_hardware_mem_check CHECK (mem_total_kb IS NULL OR mem_total_kb >= 0);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
DO $$ BEGIN
   ALTER TABLE node_monitor.node_hardware ADD CONSTRAINT node_hardware_swap_check CHECK (swap_total_kb IS NULL OR swap_total_kb >= 0);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
DO $$ BEGIN
   ALTER TABLE node_monitor.node_hardware ADD CONSTRAINT node_hardware_hugepage_check CHECK (hugepage_size_kb IS NULL OR hugepage_size_kb >= 0);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
DO $$ BEGIN
   ALTER TABLE node_monitor.node_hardware ADD CONSTRAINT node_hardware_net_fs_check CHECK (net_fs_mounts IS NULL OR net_fs_mounts >= 0);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS node_monitor.node_counter_minute (
   system text NOT NULL,
   source_hostname text NOT NULL,
   window_start timestamptz NOT NULL,
   window_end timestamptz NOT NULL,
   collector_hostname text NOT NULL,
   probe_version integer NOT NULL,
   daemon_version text NOT NULL,
   sample_count integer NOT NULL CHECK (sample_count >= 0),
   expected_count integer NOT NULL CHECK (expected_count >= 0),
   coverage double precision NOT NULL CHECK (coverage >= 0 AND coverage <= 1),
   mem_available_kb bigint,
   cached_kb bigint,
   shmem_kb bigint,
   load1 double precision,
   load5 double precision,
   load15 double precision,
   procs_running integer,
   procs_total integer,
   socket_count integer,
   cpu_busy_pct jsonb,
   network_rates jsonb,
   lustre_md_summary jsonb,
   meets_minimum_samples boolean NOT NULL,
   invalid_pair_count integer NOT NULL CHECK (invalid_pair_count >= 0),
   excess_sample_count integer NOT NULL CHECK (excess_sample_count >= 0),
   CONSTRAINT node_counter_minute_pkey PRIMARY KEY (system, source_hostname, window_start),
   CONSTRAINT node_counter_minute_window_check CHECK (window_end > window_start),
   CONSTRAINT node_counter_minute_mem_check CHECK (mem_available_kb IS NULL OR mem_available_kb >= 0),
   CONSTRAINT node_counter_minute_cached_check CHECK (cached_kb IS NULL OR cached_kb >= 0),
   CONSTRAINT node_counter_minute_shmem_check CHECK (shmem_kb IS NULL OR shmem_kb >= 0),
   CONSTRAINT node_counter_minute_procs_running_check CHECK (procs_running IS NULL OR procs_running >= 0),
   CONSTRAINT node_counter_minute_procs_total_check CHECK (procs_total IS NULL OR procs_total >= 0),
   CONSTRAINT node_counter_minute_socket_check CHECK (socket_count IS NULL OR socket_count >= 0)
);
CREATE INDEX IF NOT EXISTS node_counter_minute_system_time_idx ON node_monitor.node_counter_minute (system, window_start DESC);
CREATE INDEX IF NOT EXISTS node_counter_minute_retention_idx ON node_monitor.node_counter_minute (window_start);

CREATE TABLE IF NOT EXISTS node_monitor.node_usage_intervals (
   id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
   system text NOT NULL,
   source_hostname text NOT NULL,
   interval_start timestamptz NOT NULL,
   interval_end timestamptz NOT NULL,
   category text NOT NULL,
   activity text NOT NULL,
   username text,
   username_key text GENERATED ALWAYS AS (COALESCE(username, '')) STORED,
   process_count jsonb NOT NULL,
   cpu_seconds double precision NOT NULL CHECK (cpu_seconds >= 0),
   rss_kb jsonb NOT NULL,
   d_state_fraction double precision NOT NULL CHECK (d_state_fraction >= 0 AND d_state_fraction <= 1),
   interactivity_fraction double precision NOT NULL CHECK (interactivity_fraction >= 0 AND interactivity_fraction <= 1),
   sample_count integer NOT NULL CHECK (sample_count >= 0),
   expected_count integer NOT NULL CHECK (expected_count >= 0),
   unmeasured_count integer NOT NULL CHECK (unmeasured_count >= 0),
   CONSTRAINT node_usage_intervals_window_check CHECK (interval_end > interval_start),
   CONSTRAINT node_usage_intervals_username_check CHECK (username IS NULL OR username <> ''),
   CONSTRAINT node_usage_intervals_grain_key UNIQUE (system, source_hostname, interval_start, category, activity, username_key)
);
CREATE INDEX IF NOT EXISTS node_usage_intervals_system_time_idx ON node_monitor.node_usage_intervals (system, interval_start DESC);
CREATE INDEX IF NOT EXISTS node_usage_intervals_retention_idx ON node_monitor.node_usage_intervals (interval_start);

CREATE TABLE IF NOT EXISTS node_monitor.node_poll_failures (
   id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
   system text NOT NULL,
   source_hostname text NOT NULL,
   loop text NOT NULL CHECK (loop IN ('counter', 'census')),
   recorded_at timestamptz NOT NULL,
   failure_type text NOT NULL CHECK (failure_type IN ('scheduler_miss', 'timeout', 'ssh_auth', 'ssh_transport', 'probe_exit', 'malformed_json', 'probe_version_mismatch', 'hostname_mismatch', 'reboot', 'reset', 'invariant_violation')),
   detail text NOT NULL,
   consecutive_failures integer NOT NULL CHECK (consecutive_failures >= 0),
   breaker_state text NOT NULL CHECK (breaker_state IN ('closed', 'open', 'half_open'))
);
CREATE INDEX IF NOT EXISTS node_poll_failures_system_node_time_idx ON node_monitor.node_poll_failures (system, source_hostname, recorded_at DESC);
CREATE INDEX IF NOT EXISTS node_poll_failures_retention_idx ON node_monitor.node_poll_failures (recorded_at);

CREATE TABLE IF NOT EXISTS node_monitor.node_collection_log (
   id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
   system text NOT NULL,
   recorded_at timestamptz NOT NULL,
   event text NOT NULL,
   detail jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS node_collection_log_system_time_idx ON node_monitor.node_collection_log (system, recorded_at DESC);
CREATE INDEX IF NOT EXISTS node_collection_log_retention_idx ON node_monitor.node_collection_log (recorded_at);
