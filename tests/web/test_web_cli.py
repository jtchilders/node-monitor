"""Task 6 CLI import-graph regression: fresh subprocess proof."""
import subprocess, sys, os

def test_import_cli_main_does_not_load_migration():
   # Fresh interpreter, import cli.main, fail if migration loaded
   code = (
      "import sys; import node_monitor.cli.main; "
      "sys.exit(1 if 'node_monitor.database.migration' in sys.modules else 0)"
   )
   result = subprocess.run(
      [sys.executable, "-c", code],
      capture_output=True, text=True,
      cwd=os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
      env={**os.environ, "PYTHONPATH": os.path.dirname(os.path.dirname(os.path.dirname(__file__)))},
   )
   # Must exit 0 and not import migration
   assert result.returncode == 0, (
      "import cli.main loaded migration: stdout=%s stderr=%s" % (result.stdout, result.stderr)
   )
   assert "node_monitor.database.migration" not in result.stdout + result.stderr
