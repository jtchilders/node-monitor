"""Documentation contract tests for PostgreSQL operator guidance."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_database_runbook_documents_fail_closed_operator_workflow():
   text = (ROOT / "docs" / "database.md").read_text().lower()
   for required in (
      "database status", "database migrate", "backup", "advisory lock",
      "forward fix", "restore", "never", "daemon", "ddl",
   ):
      assert required in text


def test_phase1_example_targets_node_monitor_database_without_credentials():
   text = (ROOT / "config.example.phase1.yaml").read_text()
   assert "postgresql://localhost/node_monitor_dev" in text
   assert "pbs_monitor_dev" not in text
   assert "NODE_MONITOR_DB_URL" in text


def test_readme_links_database_runbook_and_keeps_phase0_jsonl_boundary():
   text = (ROOT / "README.md").read_text()
   assert "docs/database.md" in text
   assert "database status" in text
   assert "database migrate" in text
   assert "Phase 0" in text
   assert "JSONL" in text
