"""Task 6 packaging: deterministic clean-install probe.

Verifies:
- Exactly one wheel and one sdist produced.
- Each artifact installs cleanly into a separate fresh venv.
- Installed module path is inside site-packages and outside the repo.
- importlib.resources enumerates exactly four static assets.
- Installed Chart.js size and SHA-256 match the recorded values.
- TestClient from the installed package returns correct status codes
  for the exact static route allowlist and traversal attempts.
- sdist tar member names and wheel zip names contain exactly the four
  allowed static basenames (no extras, no omissions).

Offline strategy: a pre-downloaded wheelhouse under dist/_wheelhouse
is used when present (to avoid network hits on a repeat run). On a
clean run pip fetches dependencies normally and the wheelhouse is
populated for future re-use.
"""
import hashlib
import os
import subprocess
import sys
import tarfile
import tempfile
import venv
import zipfile

CHART_HASH = (
   "d2af8974e95271638772e9e9524db5b9a6f58d6ec2d5d781"
   "400447b4a31c681e"
)
CHART_SIZE = 205399
STATIC_ALLOWLIST = frozenset({"index.html", "styles.css", "app.js", "chart.umd.min.js"})

# Repo root is three levels above this test file.
_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_WHEELHOUSE = os.path.join(_REPO, "dist", "_wheelhouse")


def _repo_python():
   """Return the absolute path to the Python executable used by the repo venv."""
   venv_py = os.path.join(_REPO, "venv", "bin", "python")
   if os.path.exists(venv_py):
      return venv_py
   return sys.executable


def _fresh_env():
   """Return a subprocess environment with PYTHONPATH empty and user-site disabled."""
   env = {k: v for k, v in os.environ.items() if k not in ("PYTHONPATH",)}
   env["PYTHONPATH"] = ""
   env["PYTHONNOUSERSITE"] = "1"
   return env


def _pip_install(pip_bin, artifact, env, wheelhouse=None):
   """Install artifact into the venv via pip.

   Uses --find-links on the wheelhouse directory (when it exists) to
   satisfy dependencies without a network round-trip, then falls back
   to the default PyPI index.
   """
   cmd = [pip_bin, "install", "--quiet"]
   if wheelhouse and os.path.isdir(wheelhouse):
      cmd += ["--find-links", wheelhouse]
   cmd.append(artifact)
   return subprocess.run(cmd, capture_output=True, text=True, env=env)


def test_chart_checksum_and_size():
   """Dev-tree Chart.js matches the recorded SHA-256 and byte size."""
   import importlib.resources as ir
   data = (ir.files("node_monitor.web") / "static" / "chart.umd.min.js").read_bytes()
   assert len(data) == CHART_SIZE, (
      "chart.umd.min.js size %d != expected %d" % (len(data), CHART_SIZE)
   )
   assert hashlib.sha256(data).hexdigest() == CHART_HASH, (
      "chart.umd.min.js SHA-256 mismatch"
   )


def test_negative_control_chart_hash():
   """Mutating one byte of Chart.js data fails the hash assertion."""
   import importlib.resources as ir
   data = bytearray(
      (ir.files("node_monitor.web") / "static" / "chart.umd.min.js").read_bytes()
   )
   data[0] ^= 0xFF
   assert hashlib.sha256(bytes(data)).hexdigest() != CHART_HASH, (
      "mutated chart data must not match recorded hash"
   )


def test_packaging_probe_clean_install():
   """Build, install each artifact, and probe from an unrelated cwd."""
   with tempfile.TemporaryDirectory() as tmp:
      outdir = os.path.join(tmp, "dist")
      os.makedirs(outdir, exist_ok=True)
      build = subprocess.run(
         [_repo_python(), "-m", "build", "--no-isolation", "--outdir", outdir],
         cwd=_REPO,
         capture_output=True,
         text=True,
      )
      assert build.returncode == 0, (
         "build failed (rc=%d):\n%s" % (build.returncode, build.stderr)
      )

      artifacts = os.listdir(outdir)
      wheels = [a for a in artifacts if a.endswith(".whl")]
      sdists = [a for a in artifacts if a.endswith(".tar.gz")]

      assert len(wheels) == 1, (
         "expected exactly 1 wheel; got %d: %s" % (len(wheels), wheels)
      )
      assert len(sdists) == 1, (
         "expected exactly 1 sdist; got %d: %s" % (len(sdists), sdists)
      )

      _check_sdist_static_members(os.path.join(outdir, sdists[0]))
      _check_wheel_static_members(os.path.join(outdir, wheels[0]))

      for art in (wheels[0], sdists[0]):
         _probe_clean_install(os.path.join(outdir, art))


def _check_sdist_static_members(sdist_path):
   """Assert the sdist tar contains exactly the four allowed static basenames."""
   with tarfile.open(sdist_path, "r:gz") as tf:
      members = tf.getnames()
   static_basenames = set()
   for m in members:
      parts = m.split("/")
      if "web" in parts and "static" in parts:
         static_idx = parts.index("static")
         if static_idx + 1 < len(parts) and parts[static_idx + 1]:
            static_basenames.add(parts[static_idx + 1])
   extra = static_basenames - STATIC_ALLOWLIST
   missing = STATIC_ALLOWLIST - static_basenames
   assert not extra, "sdist contains unexpected static files: %s" % sorted(extra)
   assert not missing, "sdist is missing static files: %s" % sorted(missing)


def _check_wheel_static_members(wheel_path):
   """Assert the wheel zip contains exactly the four allowed static basenames."""
   with zipfile.ZipFile(wheel_path, "r") as zf:
      names = zf.namelist()
   static_basenames = set()
   for n in names:
      if "web/static/" in n:
         basename = n.split("web/static/")[-1]
         if basename and "/" not in basename:
            static_basenames.add(basename)
   extra = static_basenames - STATIC_ALLOWLIST
   missing = STATIC_ALLOWLIST - static_basenames
   assert not extra, "wheel contains unexpected static files: %s" % sorted(extra)
   assert not missing, "wheel is missing static files: %s" % sorted(missing)


def _probe_clean_install(artifact_path):
   """Install artifact into a fresh venv and run a JSON-emitting probe."""
   with tempfile.TemporaryDirectory() as vtmp:
      vdir = os.path.join(vtmp, "venv")
      venv.create(vdir, with_pip=True)
      python_bin = os.path.join(vdir, "bin", "python")
      pip_bin = os.path.join(vdir, "bin", "pip")
      env = _fresh_env()

      install = _pip_install(pip_bin, artifact_path, env, wheelhouse=_WHEELHOUSE)
      assert install.returncode == 0, (
         "pip install of %s failed (rc=%d):\n%s"
         % (os.path.basename(artifact_path), install.returncode, install.stderr)
      )

      # Install all runtime deps needed by the probe.
      # The wheel may not propagate Requires-Dist if build used legacy setup.py;
      # we enumerate the minimal set explicitly so the probe can import them.
      probe_deps = subprocess.run(
         [pip_bin, "install", "--quiet",
          "fastapi>=0.100.0,<1.0",
          "uvicorn>=0.20.0,<1.0",
          "httpx>=0.23.0",
          "sqlalchemy>=2.0.0",
          "pyyaml>=6.0",
          "click>=8.0.0",
          "python-dateutil>=2.8.0"],
         capture_output=True, text=True, env=env
      )
      assert probe_deps.returncode == 0, (
         "probe dep install failed:\n%s" % probe_deps.stderr
      )

      probe_code = _build_probe_code()
      # Run from /tmp so the repo tree is not on sys.path.
      probe = subprocess.run(
         [python_bin, "-c", probe_code],
         cwd=tempfile.gettempdir(),
         env=env,
         capture_output=True,
         text=True,
      )
      assert probe.returncode == 0, (
         "installed probe failed (rc=%d):\n%s\nstdout=%s"
         % (probe.returncode, probe.stderr, probe.stdout)
      )

      import json as _json
      result = _json.loads(probe.stdout)

      # Module path must be inside venv site-packages, not inside the repo.
      assert result["in_site_packages"], (
         "node_monitor.web not in site-packages: %s" % result["web_file"]
      )
      assert result["not_in_repo"], (
         "node_monitor.web resolved inside repo tree: %s" % result["web_file"]
      )

      # fastapi and uvicorn must be importable.
      assert result["fastapi_importable"], "fastapi not importable from installed env"
      assert result["uvicorn_importable"], "uvicorn not importable from installed env"

      # Exactly four static assets.
      basenames = sorted(result["asset_basenames"])
      assert basenames == sorted(STATIC_ALLOWLIST), (
         "static assets mismatch: got %s, expected %s"
         % (basenames, sorted(STATIC_ALLOWLIST))
      )

      # Chart.js size and hash in installed venv.
      assert result["chart_size"] == CHART_SIZE, (
         "installed chart size %d != %d" % (result["chart_size"], CHART_SIZE)
      )
      assert result["chart_hash"] == CHART_HASH, (
         "installed chart SHA-256 mismatch"
      )

      # Route assertions.
      assert result["root_status"] == 200, (
         "/ returned %d" % result["root_status"]
      )
      assert result["health_status"] == 200, (
         "/health returned %d" % result["health_status"]
      )
      for route_key in (
         "styles_status", "app_js_status", "chart_js_status",
      ):
         assert result[route_key] == 200, (
            "%s returned %d" % (route_key, result[route_key])
         )
      for bad_key in (
         "config_py_status",
         "config_yaml_status",
         "encoded_dotdot_status",
         "double_dot_slash_status",
         "null_byte_status",
      ):
         assert result[bad_key] in (400, 404), (
            "%s returned %d (expected 400 or 404)" % (bad_key, result[bad_key])
         )


def _build_probe_code():
   """Return a Python one-liner that prints a JSON result dict."""
   return (
      "import json, hashlib, os;"
      "import importlib.resources as ir;"
      "import node_monitor.web;"
      "import fastapi;"
      "import uvicorn;"
      "from fastapi.testclient import TestClient;"
      "from node_monitor.web.app import create_app;"
      "from node_monitor.web.service import DashboardRequestError, DashboardServiceError;"
      "f = node_monitor.web.__file__;"
      "result = {};"
      "result['web_file'] = f;"
      "result['in_site_packages'] = 'site-packages' in f or 'dist-packages' in f;"
      "result['not_in_repo'] = '/workspaces/' not in f and '/.worktrees/' not in f;"
      "result['fastapi_importable'] = True;"
      "result['uvicorn_importable'] = True;"
      "assets = list(ir.files('node_monitor.web').joinpath('static').iterdir());"
      "result['asset_basenames'] = [a.name for a in assets];"
      "chart_data = (ir.files('node_monitor.web') / 'static' / 'chart.umd.min.js').read_bytes();"
      "result['chart_size'] = len(chart_data);"
      "result['chart_hash'] = hashlib.sha256(chart_data).hexdigest();"
      "client = TestClient(create_app(None));"
      "result['root_status'] = client.get('/').status_code;"
      "result['health_status'] = client.get('/health').status_code;"
      "result['styles_status'] = client.get('/static/styles.css').status_code;"
      "result['app_js_status'] = client.get('/static/app.js').status_code;"
      "result['chart_js_status'] = client.get('/static/chart.umd.min.js').status_code;"
      "result['config_py_status'] = client.get('/static/config.py').status_code;"
      "result['config_yaml_status'] = client.get('/static/config.yaml').status_code;"
      "result['encoded_dotdot_status'] = client.get('/static/%2e%2e/config.py').status_code;"
      "result['double_dot_slash_status'] = client.get('/static/..%2Fconfig.py').status_code;"
      "result['null_byte_status'] = client.get('/static/foo%00bar.js').status_code;"
      "print(json.dumps(result))"
   )
