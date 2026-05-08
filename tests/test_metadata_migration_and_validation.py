from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class MetadataMigrationValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp(prefix="meta-migrate-test-"))
        self.migrate_script = Path(__file__).resolve().parents[1] / "scripts" / "metadata" / "migrate_store_to_version_files.py"
        self.validate_script = Path(__file__).resolve().parents[1] / "scripts" / "metadata" / "validate_metadata.py"
        legacy_dir = self.root / "airflow" / "web_app_data"
        legacy_dir.mkdir(parents=True, exist_ok=True)
        self.legacy_store = legacy_dir / "pipeline_versions_store.json"

    def test_migration_creates_version_files(self):
        payload = {
            "pipelines": {
                "pipe1": {
                    "versions": [
                        {
                            "version_id": "v1",
                            "airflow_dag_id": "pipe1__v1",
                            "repo_file_path": "dags/pipe1_git__v1.py",
                            "promotion_status": "submitted",
                            "base_pipeline_id": "pipe1",
                        }
                    ]
                }
            }
        }
        self.legacy_store.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        subprocess.run([sys.executable, str(self.migrate_script), "--root", str(self.root)], check=True)
        version_path = self.root / "airflow" / "web_app_data" / "metadata" / "pipe1" / "v1.json"
        self.assertTrue(version_path.exists())
        data = json.loads(version_path.read_text(encoding="utf-8"))
        self.assertEqual(data["local_dag_id"], "pipe1__v1")
        self.assertEqual(data["git_dag_id"], "pipe1_git__v1")
        self.assertEqual(data["git_dag_file"], "dags/pipe1_git__v1.py")

    def test_validate_metadata_rejects_diff_markers(self):
        bad_path = self.root / "airflow" / "web_app_data" / "metadata" / "pipe1" / "v1.json"
        bad_path.parent.mkdir(parents=True, exist_ok=True)
        bad_path.write_text(
            """{
  "pipeline_id": "pipe1",
  "version_id": "v1",
  "base_pipeline_id": "pipe1",
  "local_dag_id": "pipe1__v1",
  "git_dag_id": "pipe1_git__v1",
  "local_dag_file": "airflow/dags/pipe1__v1.py",
  "git_dag_file": "dags/pipe1_git__v1.py",
  "promotion_status": "submitted",
  "payload": "@@ diff --git"
}""",
            encoding="utf-8",
        )
        proc = subprocess.run(
            [sys.executable, str(self.validate_script), "--root", str(self.root)],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("INVALID:", proc.stdout)


if __name__ == "__main__":
    unittest.main()
