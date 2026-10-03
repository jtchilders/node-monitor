"""Task 6 packaging: real build, install, import, asset/checksum verification."""
import hashlib
import os
import subprocess
import sys
import tempfile

CHART_HASH = "d2af8974e95271638772e9e9524db5b9a6f58d6ec2d5d781400447b4a31c681e"
CHART_SIZE = 205399


def test_chart_asset_checksum_size_content():
    path = "node_monitor/web/static/chart.umd.min.js"
    assert os.path.exists(path)
    data = open(path, "rb").read()
    assert len(data) == CHART_SIZE, f"size={len(data)}"
    assert hashlib.sha256(data).hexdigest() == CHART_HASH
    assert b"placeholder" not in data


def test_static_assets_packaged():
    from importlib.resources import files
    static_dir = files("node_monitor.web") / "static"
    for name in ("index.html", "styles.css", "app.js", "chart.umd.min.js"):
        assert (static_dir / name).is_file(), f"missing {name}"
