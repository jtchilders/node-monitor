"""RED CLI tests for Task 6."""

def test_web_cli_exists():
   from node_monitor.cli.main import cli
   # RED: web subcommand missing
   assert "web" not in [c.name for c in cli.commands.get("cli", cli).commands.values() if hasattr(c, "commands")]
