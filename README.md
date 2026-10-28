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
sessions, losses, rotations, reboots, probe CPU per hour, peak RSS, census scan p95 and
average process count.
