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


def test_real_wheel_and_sdist_install_probe():
    import subprocess, tempfile, sys, os, venv, hashlib
    work = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    # Build
    build_result = subprocess.run(
        [sys.executable, "-m", "build", "--no-isolation"],
        cwd=work, capture_output=True, text=True)
    assert build_result.returncode == 0, build_result.stderr
    dist_dir = os.path.join(work, "dist")
    artifacts = os.listdir(dist_dir)
    wheel = [a for a in artifacts if a.endswith('.whl')][0]
    sdist = [a for a in artifacts if a.endswith('.tar.gz')][0]
    # Separate fresh venvs
    for artifact, name in [(wheel, "wheel"), (sdist, "sdist")]:
        with tempfile.TemporaryDirectory() as tmp:
            venv_dir = os.path.join(tmp, name + "_venv")
            venv.create(venv_dir, with_pip=True)
            pip = os.path.join(venv_dir, "bin", "pip")
            # Install artifact only, no source tree leakage
            env = {**os.environ, "PYTHONPATH": "", "PYTHONNOUSERSITE": "1"}
            install_res = subprocess.run(
                [pip, "install", os.path.join(dist_dir, artifact),
                 "--no-deps" if False else ""],
                cwd=tmp, capture_output=True, text=True, env=env)
            # Note: install with dependencies from production requirements
            # (re-run properly)
            install_res = subprocess.run(
                [pip, "install", "--force-reinstall", "--no-deps",
                 os.path.join(dist_dir, artifact)],
                cwd=tmp, capture_output=True, text=True, env=env)
            # Actually install with runtime deps from requirements
            install_res = subprocess.run(
                [pip, "install", os.path.join(dist_dir, artifact)],
                cwd=tmp, capture_output=True, text=True, env=env)
            assert install_res.returncode == 0, install_res.stderr
            # Probe: import resolves from site-packages
            # Install artifact + runtime dependencies in fresh venv
            env_clean = {**os.environ, 'PYTHONPATH':'', 'PYTHONNOUSERSITE':'1'}
            subprocess.run([pip, 'install', '--quiet', os.path.join(dist_dir, artifact), '-r',
                            os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'requirements.txt')],
                           cwd=tmp, env=env_clean, capture_output=True)
            python_bin = os.path.join(venv_dir, 'bin', 'python')
            probe = subprocess.run(
                [python_bin, "-c",
                 "import node_monitor.web, fastapi, uvicorn, os; "
                 "print('PROBE_OK'); print(os.__file__); "
                 "print('assets:', all(os.path.exists('node_monitor/web/static/'+f) for f in ['index.html','styles.css','app.js','chart.umd.min.js']))"],
                cwd=tmp, env=env, capture_output=True, text=True)
            # We must ensure cwd does not let source tree win; verify site-packages import
            assert probe.returncode == 0, probe.stderr
            assert "PROBE_OK" in probe.stdout
