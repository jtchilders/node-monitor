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
import subprocess
import sys
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


def _install_runtime_dependencies(venv_python, workdir):
   # setup.py's install_requires is populated from requirements.txt at
   # metadata-generation time, which is a preexisting packaging gap
   # unrelated to this task's scope (Task 1 is discovery/resource
   # packaging only). Install the runtime dependency the discovery
   # import chain actually needs -- node_monitor/database/__init__.py
   # imports NodeMonitorDB, which imports sqlalchemy -- explicitly so
   # this packaging test genuinely isolates the migration-resource
   # question instead of failing on an unrelated dependency gap.
   requirements_path = REPO_ROOT / "requirements.txt"
   assert requirements_path.exists()
   _run([str(venv_python), "-m", "pip", "install", "--quiet",
         "-r", str(requirements_path)], cwd=workdir)


def _install(venv_python, distribution_path, workdir):
   _run([str(venv_python), "-m", "pip", "install", "--quiet",
         str(distribution_path)], cwd=workdir)


def _discover_via_subprocess(venv_python, workdir):
   """Run discovery inside the fresh venv's own interpreter -- never
   import the installed package from the outer (developer/editable)
   interpreter, so this genuinely proves what got packaged. Runs with
   cwd=workdir (never REPO_ROOT) so ``python -c``'s implicit sys.path[0]
   cannot shadow the installed site-packages copy with the developer's
   working tree.
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
   result = _run([str(venv_python), "-c", probe], cwd=workdir)
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
      _install_runtime_dependencies(venv_python, tmp_path)
      _install(venv_python, wheel_path, tmp_path)

      expected = _expected_bytes()
      expected_checksum = hashlib.sha256(expected).hexdigest()

      report = _discover_via_subprocess(venv_python, tmp_path)
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
      _install_runtime_dependencies(venv_python, tmp_path)
      _install(venv_python, sdist_path, tmp_path)

      expected = _expected_bytes()
      expected_checksum = hashlib.sha256(expected).hexdigest()

      report = _discover_via_subprocess(venv_python, tmp_path)
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
      _install_runtime_dependencies(venv_python, tmp_path)
      _install(venv_python, wheel_path, tmp_path)

      result = _run([
         str(venv_python), "-c",
         "import node_monitor; print(node_monitor.__file__)",
      ], cwd=tmp_path)
      installed_location = Path(result.stdout.strip()).resolve()
      assert str(REPO_ROOT) not in str(installed_location)
      assert str(venv_dir.resolve()) in str(installed_location)


@pytest.mark.slow
class TestPackagedModeSidecarSurvivesInstall:
   """discover_migrations() reads ``<file>.sql.mode`` sidecars via the
   same importlib.resources traversable as the .sql files themselves
   (see ``_read_mode_marker``). setup.py's package_data must therefore
   include ``*.sql.mode`` alongside ``*.sql``, or a mode override
   authored by a migration writer would silently vanish from a real
   wheel/sdist install. These tests add a temporary 0002 migration with
   an explicit sidecar directly into the packaged source tree, build
   real distributions, install into a fresh venv, and prove the
   sidecar's effect is visible post-install -- for both an accepted
   ("transactional") and a rejected (unsupported) mode value.
   """

   def _with_temporary_migration(self, sql_name, mode_text):
      """Context manager-free helper: writes a temporary 0002 migration
      (and its .sql.mode sidecar) into the real packaged source tree,
      returning a cleanup callable. Task 1 ships no runner/CLI to do
      this through public API, so the packaging proof must place the
      fixture where setup.py's package_data glob will actually find it.
      """
      sql_path = MIGRATIONS_DIR / sql_name
      mode_path = MIGRATIONS_DIR / (sql_name + ".mode")
      assert not sql_path.exists(), "fixture collision: %s" % sql_path
      assert not mode_path.exists(), "fixture collision: %s" % mode_path
      sql_path.write_bytes(b"-- packaging-test placeholder migration\n")
      mode_path.write_text(mode_text)

      def cleanup():
         sql_path.unlink(missing_ok=True)
         mode_path.unlink(missing_ok=True)

      return cleanup

   def test_transactional_sidecar_survives_wheel_install(self, tmp_path):
      # "transactional" happens to equal discover_migrations()'s default
      # when no sidecar is present at all, so asserting on the resulting
      # Migration.mode alone would pass even if the sidecar file were
      # silently dropped by packaging -- a false-negative risk. Assert
      # directly, via the installed package's own importlib.resources
      # traversable, that the ``.sql.mode`` sidecar file itself exists
      # and its packaged content matches what we wrote, so this proves
      # packaging rather than coincidence.
      cleanup = self._with_temporary_migration(
         "0002_packaging_sidecar.sql", "transactional")
      try:
         build_dir = tmp_path / "dist"
         build_dir.mkdir()
         wheel_path, _sdist_path = _build_distributions(build_dir)

         venv_dir = tmp_path / "venv-wheel-sidecar"
         venv_python = _fresh_venv(venv_dir)
         _install_runtime_dependencies(venv_python, tmp_path)
         _install(venv_python, wheel_path, tmp_path)

         probe = textwrap.dedent(
            """
            import json
            import importlib.resources as resources
            from node_monitor.database.migration import discover_migrations

            versions_pkg = resources.files(
               "node_monitor.database.migrations.versions"
            )
            sidecar = versions_pkg.joinpath(
               "0002_packaging_sidecar.sql.mode")

            migrations = discover_migrations()
            second = migrations[1]
            print(json.dumps({
               "version": second.version,
               "mode": second.mode,
               "sidecar_is_file": sidecar.is_file(),
               "sidecar_text": sidecar.read_text().strip(),
            }))
            """
         )
         result = _run([str(venv_python), "-c", probe], cwd=tmp_path)
         report = json.loads(result.stdout.strip().splitlines()[-1])
         assert report["version"] == 2
         assert report["mode"] == "transactional"
         assert report["sidecar_is_file"] is True
         assert report["sidecar_text"] == "transactional"
      finally:
         cleanup()

   def test_unsupported_sidecar_fails_closed_after_sdist_install(
         self, tmp_path):
      cleanup = self._with_temporary_migration(
         "0002_packaging_sidecar.sql", "concurrent")
      try:
         build_dir = tmp_path / "dist"
         build_dir.mkdir()
         _wheel_path, sdist_path = _build_distributions(build_dir)

         venv_dir = tmp_path / "venv-sdist-sidecar"
         venv_python = _fresh_venv(venv_dir)
         _install_runtime_dependencies(venv_python, tmp_path)
         _install(venv_python, sdist_path, tmp_path)

         probe = (
            "from node_monitor.database.migration import "
            "discover_migrations\n"
            "discover_migrations()\n"
         )
         result = subprocess.run(
            [str(venv_python), "-c", probe],
            cwd=tmp_path, capture_output=True, text=True,
         )
         assert result.returncode != 0, (
            "expected discover_migrations() to fail closed on an "
            "unsupported packaged mode sidecar, but it exited 0")
         assert "ValueError" in result.stderr
      finally:
         cleanup()
