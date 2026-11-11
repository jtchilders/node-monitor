"""Storage record type and kinds produced by the per-node pipeline.

`LIVE_NODE` rows are partial updates keyed by (system, node): a key missing from a row
means "unchanged", so a writer must update only the columns present. When a session is lost
the row carries just the identity, `label`, `updated_at` and `session_state`, and the last
known gauges are kept rather than overwritten with nulls.
"""

from collections import namedtuple

Record = namedtuple("Record", "kind row")

LIVE_PROCESSES = "live_processes"
LIVE_NODE = "live_node"
NODE_METRICS_1M = "node_metrics_1m"
USER_USAGE_5M = "user_usage_5m"
NODE_USAGE_5M = "node_usage_5m"
NODE_HARDWARE = "node_hardware"
COLLECTION_EVENT = "collection_event"
PROCESS_SAMPLES = "process_samples"
COMMAND_LINES = "command_lines"
