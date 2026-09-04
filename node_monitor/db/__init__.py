"""node_monitor.db -- schema DDL and the daemon-side hardware writer.

Separate from node_monitor/database/ (which predates this task and is left
untouched) because the card asked for schema.sql to live under a `db`
package specifically; consolidating the two is a larger refactor than this
task's scope.
"""
