"""Contracts for unified web configuration and operator documentation."""

from pathlib import Path

import yaml

from node_monitor.config import load_nested_config, load_web_config


ROOT = Path(__file__).resolve().parents[1]


def test_readme_preserves_project_guidance_and_documents_web_quick_start():
   text = (ROOT / "README.md").read_text()
   for required in (
      "docs/database.md",
      "Phase 0",
      "database status",
      "database migrate",
      "~/.node_monitor.yml",
      "node-monitor web --port 9998",
      "ssh -L 9998:127.0.0.1:9998",
   ):
      assert required in text
   assert "--no-browser" not in text


def test_runbook_documents_unified_config_inventory_and_preserves_boundaries():
   text = (ROOT / "docs" / "web-dashboard.md").read_text()
   for required in (
      "~/.node_monitor.yml",
      "node-monitor web --port 9998",
      "database-backed",
      "exact stored",
      "No monitored nodes available",
      "Node unavailable",
      "PostgreSQL reader role",
      "Chart.js asset documentation",
   ):
      assert required in text
   assert "web-only YAML" not in text
   assert "dedicated YAML file" not in text
   assert "--no-browser" not in text


def test_example_is_valid_unified_config_with_distinct_web_reader(tmp_path):
   path = ROOT / "config.example.phase1.yaml"
   raw = yaml.safe_load(path.read_text())
   assert [node["display_name"] for node in raw["nodes"]] == [
      "login-04", "login-01"]
   assert raw["database"]["url"]
   assert raw["web"]["database"] == {
      "schema": "node_monitor",
      "pool_size": 1,
      "max_overflow": 0,
   }

   cfg = load_nested_config(
      raw, home="/home/operator",
      database_url_env="postgresql://writer@localhost/node_monitor_dev")
   private_config = tmp_path / ".node_monitor.yml"
   private_config.write_text(path.read_text())
   private_config.chmod(0o600)
   web_config = load_web_config(
      private_config, home="/home/operator",
      database_url_env="postgresql://reader@localhost/node_monitor_dev")
   assert cfg.database is not None
   assert web_config.database.url == "postgresql://reader@localhost/node_monitor_dev"
   assert web_config.database.url != cfg.database.url
   assert web_config.nodes[0].display_name == "login-04"
