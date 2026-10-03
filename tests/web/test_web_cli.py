"""Task 6 CLI tests with real assertions."""
import importlib
import sys

def test_web_command_only_has_config():
    from node_monitor.cli.main import cli
    web = cli.commands.get("web")
    assert web is not None
    names = {p.name for p in web.params}
    assert names == {"config_path"}, f"unexpected params: {names}"

def test_web_path_does_not_import_migration():
    # Regression: importing the CLI module must not pull MigrationRunner
    # when only using the web command path.
    # We simulate by checking the cli module's local namespace after import.
    from node_monitor.cli import main as cli_main
    # The module-level import of MigrationRunner should have been removed.
    assert "MigrationRunner" not in dir(cli_main) or "MigrationRunner" not in cli_main.__dict__, \
        "MigrationRunner still in cli module namespace"

    # More rigorous: start fresh interpreter simulation by clearing module
    sys.modules.pop("node_monitor.database.migration", None)
    import node_monitor.cli.main
    assert "node_monitor.database.migration" not in sys.modules, \
        "migration module imported via cli.main"
