from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class MetadataUpdateScriptTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_dir = Path(tempfile.mkdtemp(prefix="meta-script-test-"))
        self.store = self.tmp_dir / "pipeline_versions_store.json"
        self.script = Path(__file__).resolve().parents[1] / "scripts" / "metadata" / "update_pipeline_metadata.py"
        payload = {
            "pipelines": {
                "pipe1": {
                    "versions": [
                        {
                            "version_id": "v1",
                            "airflow_dag_id": "pipe1__v1",
                            "promotion_status": "draft",
                            "publish_status": "pr_open",
                            "pull_request_number": 11,
                            "git_branch_name": "feature/workflow/pipe1",
                            "score": None,
                        },
                        {
                            "version_id": "v0",
                            "airflow_dag_id": "pipe1__v0",
                            "promotion_status": "champion",
                            "publish_status": "merged_to_git",
                            "score": 90.0,
                        },
                    ]
                }
            }
        }
        self.store.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _run(self, *args: str) -> None:
        cmd = [sys.executable, str(self.script), *args]
        subprocess.run(cmd, check=True)

    def _load(self) -> dict:
        return json.loads(self.store.read_text(encoding="utf-8"))

    def test_lifecycle_updates(self):
        self._run("pr_open", "--store", str(self.store), "--pipeline-id", "pipe1", "--version-id", "v1", "--pr-number", "11", "--pr-url", "https://example/pr/11", "--branch", "feature/workflow/pipe1", "--commit-sha", "abc123")
        v1 = self._load()["pipelines"]["pipe1"]["versions"][0]
        self.assertEqual(v1["github_pr_number"], 11)
        self.assertEqual(v1["publish_status"], "pr_open")

        self._run("merged", "--store", str(self.store), "--pr-number", "11", "--branch", "feature/workflow/pipe1", "--commit-sha", "merge123")
        v1 = self._load()["pipelines"]["pipe1"]["versions"][0]
        self.assertEqual(v1["publish_status"], "merged_to_git")
        self.assertEqual(v1["promotion_status"], "submitted")

        self._run("mark_eval", "--store", str(self.store), "--pipeline-id", "pipe1", "--version-id", "v1", "--branch", "eval")
        self._run("scored", "--store", str(self.store), "--pipeline-id", "pipe1", "--version-id", "v1", "--score-json", json.dumps({"score": 88.5, "score_type": "static_only", "breakdown": {"reliability": 20}, "warnings": [], "critical_failures": []}))
        v1 = self._load()["pipelines"]["pipe1"]["versions"][0]
        self.assertEqual(v1["promotion_status"], "challenger")
        self.assertEqual(v1["score"], 88.5)

        self._run("promote", "--store", str(self.store), "--pipeline-id", "pipe1", "--version-id", "v1")
        versions = self._load()["pipelines"]["pipe1"]["versions"]
        v1 = next(v for v in versions if v["version_id"] == "v1")
        v0 = next(v for v in versions if v["version_id"] == "v0")
        self.assertEqual(v1["promotion_status"], "champion")
        self.assertEqual(v0["promotion_status"], "archived")


if __name__ == "__main__":
    unittest.main()
