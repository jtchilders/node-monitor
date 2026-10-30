# node-monitor

Monitor for ALCF login nodes: a stdlib-only remote probe streamed over SSH, with a
collector that records process and node activity.

## Development setup

The repo is a subfolder of the `~/node-monitor` working directory, so the venv lives one
level up.

```bash
python3 -m venv ../venv && ../venv/bin/pip install -e '.[dev]' && ../venv/bin/pytest
```

## M0 soak

Record every configured node's probe stream, then summarize it:

```bash
node-monitor soak run --duration-hours 24    # omit the duration to run until Ctrl-C
node-monitor soak report ~/node-monitor/logs/soak/<timestamp> [--format json]
```

`soak run` writes `manifest.json` and one `<label>-<YYYYMMDD>.jsonl` per node per UTC day
into the out dir (default `<working_dir>/logs/soak/<UTC timestamp>`, or `--out-dir`).
Censuses are stored as summaries unless `--full-census` is given. The config comes from
`--config` or the usual search path (see `config.example.yaml`).

`soak report` prints one row per node: coverage of expected counter messages, largest gap,
sessions, losses, rotations, reboots, probe CPU per hour, peak probe RSS, census scan p95
and average process count. `rss_MB` is the exact peak (smaps_rollup); `maxrss_MB` comes
from `ru_maxrss`, which drifts tens of MiB on many-core kernels, so it is only a secondary
check. `--format json` adds the per-session RSS series and rotations by reason.

## Replaying a capture through the pipeline

```bash
node-monitor soak replay ~/node-monitor/logs/soak/<timestamp> [--config PATH] \
   [--write-records /tmp/records.jsonl]
```

`soak replay` feeds every recorded line through the per-node pipeline (classification,
redaction, minute and 5-minute aggregation) and prints the storage record counts per node.
`--write-records` writes each record as a `{kind, row}` JSON line; the rows hold redacted
but still sensitive command lines, so the file is owner-only. A capture recorded without
`--full-census` has no process rows, so it yields node metrics but no process or usage
records.

## Configuration keys added for M1

- `collection.summary_interval_sec` (default 300): length of the per-user and per-node
  usage intervals; a multiple of `census_interval_sec` that divides 3600.
- `collection.probe_rss_limit_mb` (default 128): above this resident size the probe says
  `bye` and exits, and the collector starts a fresh session.
- `classification.min_user_uid` (default 1000): processes owned by lower uids are
  classified as `system` and left out of usage summaries.
- `redaction.extra_patterns` (default `[]`): extra regexes applied after the built-in
  command-line redaction rules; the first capture group (or the whole match) is masked.
