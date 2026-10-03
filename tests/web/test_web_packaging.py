"""Task 6 packaging: deterministic clean-install probe with 3-space indent."""
import hashlib
import os
import subprocess
import sys
import tempfile
import venv

CHART_HASH = ("d2af8974e95271638772e9e9524db5b9a6f58d6ec2d5d781"
              "400447b4a31c681e")
CHART_SIZE = 205399
ALLOWLIST = {"index.html", "styles.css", "app.js", "chart.umd.min.js"}


def test_chart_checksum_and_size():
   import importlib.resources as r
   data = (r.files("node_monitor.web") / "static" / "chart.umd.min.js").read_bytes()
   assert len(data) == CHART_SIZE
   assert hashlib.sha256(data).hexdigest() == CHART_HASH


def test_packaging_probe_clean_install():
   repo = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
   with tempfile.TemporaryDirectory() as tmp:
      outdir = os.path.join(tmp, "dist")
      os.makedirs(outdir, exist_ok=True)
      build = subprocess.run(
         [sys.executable, "-m", "build", "--no-isolation",
          "--outdir", outdir], cwd=repo, capture_output=True, text=True)
      assert build.returncode == 0, build.stderr
      artifacts = os.listdir(outdir)
      wheels = [a for a in artifacts if a.endswith(".whl")]
      sdists = [a for a in artifacts if a.endswith(".tar.gz")]
      assert wheels
      assert sdists
      # Probe from installed site-packages with empty PYTHONPATH
      for art in (wheels[0], sdists[0]):
         with tempfile.TemporaryDirectory() as vtmp:
            vdir = os.path.join(vtmp, "venv")
            venv.create(vdir, with_pip=True)
            python_bin = os.path.join(vdir, "bin", "python")
            pip_bin = os.path.join(vdir, "bin", "pip")
            env = {**os.environ, "PYTHONPATH": "", "PYTHONNOUSERSITE": "1"}
            install_res = subprocess.run(
               [pip_bin, "install", "--quiet", os.path.join(outdir, art)],
               cwd=vtmp, env=env, capture_output=True, text=True)
            assert install_res.returncode == 0, install_res.stderr
            probe = subprocess.run(
               [python_bin, "-c",
                "import node_monitor.web, importlib.resources, os, sys; "
                "assert node_monitor.web.__file__ is not None; "
                "f=node_monitor.web.__file__; "
                "assert 'site-packages' in f or 'dist-packages' in f; "
                "print('INSTALLED_OK'); "
                "print('FILE:', f)",
                ], cwd=vtmp, env=env, capture_output=True, text=True)
            assert probe.returncode == 0, probe.stderr
            assert "INSTALLED_OK" in probe.stdout
            assert "FILE:" in probe.stdout
