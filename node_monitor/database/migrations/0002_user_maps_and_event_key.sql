-- by_category and by_dormancy of a user are per-key sums of by_category_dormancy, so the
-- per-user table no longer stores them. The node table keeps both.
ALTER TABLE node_monitor.user_usage_5m
   DROP COLUMN by_category,
   DROP COLUMN by_dormancy;

-- Replayed batches used to append duplicate events. Keep the earliest of each group
-- before adding the key; node is NULL for collector-level events, which compare equal.
DELETE FROM node_monitor.collection_events e
USING node_monitor.collection_events keep
WHERE keep.system = e.system
  AND keep.node IS NOT DISTINCT FROM e.node
  AND keep.ts = e.ts
  AND keep.event_type = e.event_type
  AND keep.id < e.id;

-- NULLS NOT DISTINCT so collector-level events (node NULL) dedupe too. The key includes
-- ts, the partition key, as a unique constraint on a partitioned table must.
ALTER TABLE node_monitor.collection_events
   ADD CONSTRAINT collection_events_natural_key
   UNIQUE NULLS NOT DISTINCT (system, node, ts, event_type);
