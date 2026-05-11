from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class PromoteChangedMetadataToEvalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_dir = Path(tempfile.mkdtemp(prefix="promote-eval-test-"))
        self.root = self.tmp_dir
        self.script = Path(__file__).resolve().parents[1] / "scripts" / "promotion" / "promote_changed_metadata_to_eval.py"
        (self.root / "airflow" / "web_app_data" / "metadata" / "testops").mkdir(parents=True, exist_ok=True)
        (self.root / "dags").mkdir(parents=True, exist_ok=True)
        (self.root / "dags" / "testops_git__v2.py").write_text("from airflow import DAG\n", encoding="utf-8")

    def _write_metadata(self, promotion_status: str, publish_status: str) -> Path:
        path = self.root / "airflow" / "web_app_data" / "metadata" / "testops" / "v2.json"
        payload = {
            "pipeline_id": "testops",
            "version_id": "v2",
            "base_pipeline_id": "testops",
            "local_dag_id": "testops__v2",
            "git_dag_id": "testops_git__v2",
            "local_dag_file": "airflow/dags/testops__v2.py",
            "git_dag_file": "dags/testops_git__v2.py",
            "promotion_status": promotion_status,
            "publish_status": publish_status,
            "updated_at": "2026-01-01T00:00:00+00:00",
        }
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return path

    def _run(self, metadata_file: Path) -> subprocess.CompletedProcess[str]:
        cmd = [
            sys.executable,
            str(self.script),
            "--root",
            str(self.root),
            "--metadata-file",
            str(metadata_file.relative_to(self.root)).replace("\\", "/"),
        ]
        return subprocess.run(cmd, check=False, capture_output=True, text=True)

    def test_marks_eval_for_submitted_and_merged_to_git(self):
        path = self._write_metadata("submitted", "merged_to_git")
        result = self._run(path)
        self.assertEqual(result.returncode, 0, msg=result.stdout + result.stderr)
        updated = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(updated["pipeline_id"], "testops")
        self.assertEqual(updated["git_dag_id"], "testops_git__v2")
        self.assertEqual(updated["promotion_status"], "eval")
        self.assertEqual(updated["evaluated_branch"], "eval")

    def test_skips_when_already_non_submitted_state(self):
        for status in ("eval", "challenger", "champion"):
            path = self._write_metadata(status, "merged_to_git")
            result = self._run(path)
            self.assertEqual(result.returncode, 0, msg=f"{status}: {result.stdout}{result.stderr}")
            updated = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(updated["promotion_status"], status)


if __name__ == "__main__":
    unittest.main()

