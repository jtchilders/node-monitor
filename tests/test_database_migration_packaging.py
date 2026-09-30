"""Packaging tests for node_monitor.database.migrations.versions SQL
resources -- proves migration 0001 survives both wheel and sdist
installation into a clean, non-editable virtual environment.

Design: node_monitor_planning increment2-schema-migrations-writer.md
Task 1. These tests build real distributions with ``python -m build``,
install each into a fresh throwaway venv (no editable install, no
reliance on the developer's working tree), and use that venv's own
interpreter to prove ``importlib.resources`` discovers migration 1 with
byte-identical content and checksum.

These tests are slow (they build distributions and create venvs) and
network-independent: ``python -m build --no-isolation`` reuses the
outer interpreter's already-installed setuptools/wheel rather than
fetching a build backend from PyPI.
"""

import hashlib
import json
import shutil
import subprocess
import sys
import sysconfig
import textwrap
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
MIGRATIONS_DIR = (
   REPO_ROOT / "node_monitor" / "database" / "migrations" / "versions"
)
EXPECTED_SQL_PATH = MIGRATIONS_DIR / "0001_initial_source_schema.sql"


def _run(cmd, cwd=None):
   result = subprocess.run(
      cmd, cwd=cwd, capture_output=True, text=True,
   )
   assert result.returncode == 0, (
      "command failed: %r\nstdout:\n%s\nstderr:\n%s"
      % (cmd, result.stdout, result.stderr)
   )
   return result


def _expected_bytes():
   assert EXPECTED_SQL_PATH.exists(), (
      "fixture missing: %s" % EXPECTED_SQL_PATH)
   return EXPECTED_SQL_PATH.read_bytes()


def _build_distributions(build_dir):
   """Build both a wheel and an sdist into build_dir, returning their
   paths. Uses --no-isolation so it reuses the calling interpreter's
   already-installed setuptools/wheel/build rather than requiring
   network access for an isolated build environment.
   """
   _run(
      [sys.executable, "-m", "build", "--no-isolation",
       "--outdir", str(build_dir), str(REPO_ROOT)],
   )
   wheels = list(build_dir.glob("*.whl"))
   sdists = list(build_dir.glob("*.tar.gz"))
   assert len(wheels) == 1, "expected exactly one wheel, found %r" % wheels
   assert len(sdists) == 1, "expected exactly one sdist, found %r" % sdists
   return wheels[0], sdists[0]


def _fresh_venv(venv_dir):
   _run([sys.executable, "-m", "venv", str(venv_dir)])
   if sys.platform == "win32":
      return venv_dir / "Scripts" / "python.exe"
   return venv_dir / "bin" / "python"


def _install(venv_python, distribution_path):
   _run([str(venv_python), "-m", "pip", "install", "--quiet",
         str(distribution_path)])


def _discover_via_subprocess(venv_python):
   """Run discovery inside the fresh venv's own interpreter -- never
   import the installed package from the outer (developer/editable)
   interpreter, so this genuinely proves what got packaged.
   """
   probe = textwrap.dedent(
      """
      import hashlib
      import importlib.resources as resources
      import json

      raw = resources.files(
         "node_monitor.database.migrations.versions"
      ).joinpath("0001_initial_source_schema.sql").read_bytes()

      from node_monitor.database.migration import discover_migrations

      migrations = discover_migrations()
      first = migrations[0]
      print(json.dumps({
         "version": first.version,
         "name": first.name,
         "mode": first.mode,
         "checksum": first.checksum,
         "raw_checksum": hashlib.sha256(raw).hexdigest(),
         "sql_equals_raw": first.sql == raw,
      }))
      """
   )
   result = _run([str(venv_python), "-c", probe])
   return json.loads(result.stdout.strip().splitlines()[-1])


@pytest.mark.slow
class TestPackagedMigrationSurvivesInstall:
   def test_wheel_install_exposes_migration_1_with_exact_bytes(
         self, tmp_path):
      build_dir = tmp_path / "dist"
      build_dir.mkdir()
      wheel_path, _sdist_path = _build_distributions(build_dir)

      venv_dir = tmp_path / "venv-wheel"
      venv_python = _fresh_venv(venv_dir)
      _install(venv_python, wheel_path)

      expected = _expected_bytes()
      expected_checksum = hashlib.sha256(expected).hexdigest()

      report = _discover_via_subprocess(venv_python)
      assert report["version"] == 1
      assert report["name"] == "initial_source_schema"
      assert report["mode"] == "transactional"
      assert report["checksum"] == expected_checksum
      assert report["raw_checksum"] == expected_checksum
      assert report["sql_equals_raw"] is True

   def test_sdist_install_exposes_migration_1_with_exact_bytes(
         self, tmp_path):
      build_dir = tmp_path / "dist"
      build_dir.mkdir()
      _wheel_path, sdist_path = _build_distributions(build_dir)

      venv_dir = tmp_path / "venv-sdist"
      venv_python = _fresh_venv(venv_dir)
      _install(venv_python, sdist_path)

      expected = _expected_bytes()
      expected_checksum = hashlib.sha256(expected).hexdigest()

      report = _discover_via_subprocess(venv_python)
      assert report["version"] == 1
      assert report["name"] == "initial_source_schema"
      assert report["mode"] == "transactional"
      assert report["checksum"] == expected_checksum
      assert report["raw_checksum"] == expected_checksum
      assert report["sql_equals_raw"] is True

   def test_installed_package_is_not_the_editable_developer_tree(
         self, tmp_path):
      # Guard against a false-positive: prove the fresh venv's installed
      # node_monitor package lives under its own site-packages, not the
      # developer worktree, so this genuinely exercises packaging.
      build_dir = tmp_path / "dist"
      build_dir.mkdir()
      wheel_path, _sdist_path = _build_distributions(build_dir)

      venv_dir = tmp_path / "venv-location"
      venv_python = _fresh_venv(venv_dir)
      _install(venv_python, wheel_path)

      result = _run([
         str(venv_python), "-c",
         "import node_monitor; print(node_monitor.__file__)",
      ])
      installed_location = Path(result.stdout.strip()).resolve()
      assert str(REPO_ROOT) not in str(installed_location)
      assert str(venv_dir.resolve()) in str(installed_location)
