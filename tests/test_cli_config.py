from click.testing import CliRunner

from node_monitor.cli.main import cli
from node_monitor.config import load_config


def test_config_create_writes_0600_and_refuses_overwrite(tmp_path):
   path = tmp_path / "cfg.yaml"
   result = CliRunner().invoke(cli, ["config", "--create", str(path)])
   assert result.exit_code == 0, result.output
   assert path.stat().st_mode & 0o777 == 0o600
   assert load_config(str(path)).system == "polaris"

   before = path.read_text()
   path.write_text(before + "# edited\n")
   again = CliRunner().invoke(cli, ["config", "--create", str(path)])
   assert again.exit_code == 1
   assert "exists" in again.output
   assert path.read_text().endswith("# edited\n")


def test_config_without_create_explains_itself(tmp_path):
   result = CliRunner().invoke(cli, ["config"])
   assert result.exit_code != 0
   assert "--create" in result.output
