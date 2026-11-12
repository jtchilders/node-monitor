-- Process samples (daily, 30d) and distinct command lines (hash-keyed, 730d).

-- No roles, grants or default privileges: must apply as any owner.

DO $$
BEGIN
   CREATE EXTENSION IF NOT EXISTS pg_trgm;
EXCEPTION WHEN OTHERS THEN
   RAISE NOTICE 'pg_trgm not available, skipping extension';
END
$$;

CREATE TABLE node_monitor.process_samples (
   system text NOT NULL,
   node text NOT NULL,
   ts timestamptz NOT NULL,
   pid integer NOT NULL,
   start_time bigint NOT NULL,
   username text,
   uid bigint,
   comm text,
   exe text,
   argv0 text,
   cmdline text,
   cmdline_hash text,
   ppid bigint,
   parent_comm text,
   parent_exe text,
   parent_kind text,
   category text,
   activity text,
   tags text[],
   state text,
   threads bigint,
   cpu_seconds double precision,
   rss_kb bigint,
   gpu_mem_mb double precision,
   interactive boolean,
   under_screen_or_tmux boolean,
   cpu_state text,
   idle_for_sec double precision,
   idle_for_is_lower_bound boolean,
   idle_bucket text,
   owner_presence text,
   hourly boolean NOT NULL,
   PRIMARY KEY (system, node, ts, pid, start_time)
) PARTITION BY RANGE (ts);

CREATE TABLE node_monitor.command_lines (
   system text NOT NULL,
   username text NOT NULL,
   cmdline_hash text NOT NULL,
   cmdline text NOT NULL,
   exe text,
   activity text,
   category text,
   tags text[],
   first_seen timestamptz NOT NULL,
   last_seen timestamptz NOT NULL,
   sample_count bigint NOT NULL,
   nodes text[],
   PRIMARY KEY (system, username, cmdline_hash)
);

DO $$
BEGIN
   IF EXISTS (SELECT 1 FROM pg_opclass WHERE opcname = 'gin_trgm_ops') THEN
      EXECUTE 'CREATE INDEX IF NOT EXISTS command_lines_cmdline_trgm_idx'
         ' ON node_monitor.command_lines USING gin (cmdline gin_trgm_ops)';
   ELSE
      EXECUTE 'CREATE INDEX IF NOT EXISTS command_lines_cmdline_trgm_idx'
         ' ON node_monitor.command_lines (cmdline)';
   END IF;
EXCEPTION WHEN OTHERS THEN
   EXECUTE 'CREATE INDEX IF NOT EXISTS command_lines_cmdline_trgm_idx'
      ' ON node_monitor.command_lines (cmdline)';
END
$$;
