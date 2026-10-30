"""Storage record type and kinds produced by the per-node pipeline."""

from collections import namedtuple

Record = namedtuple("Record", "kind row")

LIVE_PROCESSES = "live_processes"
LIVE_NODE = "live_node"
NODE_METRICS_1M = "node_metrics_1m"
USER_USAGE_5M = "user_usage_5m"
NODE_USAGE_5M = "node_usage_5m"
NODE_HARDWARE = "node_hardware"
COLLECTION_EVENT = "collection_event"
