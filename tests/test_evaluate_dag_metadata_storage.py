from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class EvaluateDagMetadataStorageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp(prefix="eval-meta-test-"))
        self.script = Path(__file__).resolve().parents[1] / "scripts" / "evaluation" / "evaluate_dag.py"
        (self.root / "scripts" / "metadata").mkdir(parents=True, exist_ok=True)
        # make helper module importable from test root layout
        src_meta = Path(__file__).resolve().parents[1] / "scripts" / "metadata" / "version_metadata.py"
        (self.root / "scripts" / "metadata" / "version_metadata.py").write_text(src_meta.read_text(encoding="utf-8"), encoding="utf-8")
        src_client = Path(__file__).resolve().parents[1] / "scripts" / "evaluation" / "airflow_client.py"
        (self.root / "scripts" / "evaluation").mkdir(parents=True, exist_ok=True)
        (self.root / "scripts" / "evaluation" / "airflow_client.py").write_text(src_client.read_text(encoding="utf-8"), encoding="utf-8")
        (self.root / "scripts" / "evaluation" / "evaluate_dag.py").write_text(self.script.read_text(encoding="utf-8"), encoding="utf-8")

    def test_script_does_not_import_services_module(self):
        source = self.script.read_text(encoding="utf-8")
        self.assertNotIn("services.pipeline_versions", source)

    def test_legacy_store_fallback_only_when_version_file_missing(self):
        (self.root / "dags").mkdir(parents=True, exist_ok=True)
        (self.root / "dags" / "pipe1_git__v1.py").write_text("from airflow import DAG\n", encoding="utf-8")
        legacy_store = self.root / "airflow" / "web_app_data" / "pipeline_versions_store.json"
        legacy_store.parent.mkdir(parents=True, exist_ok=True)
        legacy_store.write_text(
            json.dumps(
                {
                    "pipelines": {
                        "pipe1": {
                            "versions": [
                                {
                                    "version_id": "v1",
                                    "local_dag_id": "pipe1__v1",
                                    "git_dag_id": "pipe1_git__v1",
                                    "git_dag_file": "dags/pipe1_git__v1.py",
                                    "local_dag_file": "airflow/dags/pipe1__v1.py",
                                    "promotion_status": "eval",
                                }
                            ]
                        }
                    }
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        proc = subprocess.run(
            [
                sys.executable,
                str(self.root / "scripts" / "evaluation" / "evaluate_dag.py"),
                "--root",
                str(self.root),
                "--pipeline-id",
                "pipe1",
                "--dag-id",
                "pipe1__v1",
                "--evaluation-mode",
                "static_only",
                "--run-count",
                "1",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertIn("Evaluation complete", proc.stdout)
        updated = json.loads(legacy_store.read_text(encoding="utf-8"))
        version = updated["pipelines"]["pipe1"]["versions"][0]
        self.assertEqual(version["promotion_status"], "challenger")
        self.assertIsNotNone(version.get("score"))


if __name__ == "__main__":
    unittest.main()
