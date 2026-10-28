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

Usage is documented here once the soak CLI lands.
