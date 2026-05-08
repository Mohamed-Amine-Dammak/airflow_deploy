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
        self.assertEqual(v1["pipeline_id"], "pipe1")
        self.assertEqual(v1["version"], "v1")

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

    def test_pr_open_upserts_missing_version_before_pr_creation(self):
        empty_store = self.tmp_dir / "empty_store.json"
        empty_store.write_text(json.dumps({"pipelines": {}}, indent=2), encoding="utf-8")
        self._run(
            "pr_open",
            "--store",
            str(empty_store),
            "--pipeline-id",
            "pipe_new",
            "--version-id",
            "v7",
            "--dag-id",
            "pipe_new__v7",
            "--dag-file",
            "dags/pipe_new__v7.py",
            "--branch",
            "feature/workflow/pipe_new-v7",
            "--publish-status",
            "pr_open",
            "--promotion-status",
            "submitted",
        )
        payload = json.loads(empty_store.read_text(encoding="utf-8"))
        versions = payload["pipelines"]["pipe_new"]["versions"]
        self.assertEqual(len(versions), 1)
        v = versions[0]
        self.assertEqual(v["version_id"], "v7")
        self.assertEqual(v["dag_file"], "dags/pipe_new__v7.py")
        self.assertEqual(v["publish_status"], "pr_open")
        self.assertEqual(v["promotion_status"], "submitted")

    def test_pr_open_naming_and_json_integrity(self):
        store = self.tmp_dir / "naming_store.json"
        store.write_text(json.dumps({"pipelines": {}}, indent=2), encoding="utf-8")

        self._run(
            "pr_open",
            "--store",
            str(store),
            "--pipeline-id",
            "testdataops",
            "--version-id",
            "v4",
            "--dag-id",
            "testdataops__v4",
            "--dag-file",
            "dags/testdataops__v4.py",
            "--publish-status",
            "pr_open",
            "--promotion-status",
            "submitted",
        )

        # Must be valid JSON and parseable by both json.load and python -m json.tool
        parsed = json.loads(store.read_text(encoding="utf-8"))
        subprocess.run(
            [sys.executable, "-m", "json.tool", str(store)],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )

        # No duplicate top-level pipelines key: one dict key named "pipelines"
        self.assertIsInstance(parsed, dict)
        self.assertIn("pipelines", parsed)
        self.assertEqual(sum(1 for k in parsed.keys() if k == "pipelines"), 1)

        versions = parsed["pipelines"]["testdataops"]["versions"]
        self.assertEqual(len(versions), 1)
        v = versions[0]
        self.assertEqual(v["repo_file_path"], "dags/testdataops__v4.py")
        self.assertEqual(v["output_filename"], "testdataops__v4.py")
        self.assertEqual(v["airflow_dag_id"], "testdataops__v4")
        self.assertEqual(v["dag_id"], "testdataops__v4")

    def test_pr_open_separate_local_and_git_identity(self):
        store = self.tmp_dir / "identity_store.json"
        store.write_text(json.dumps({"pipelines": {}}, indent=2), encoding="utf-8")
        self._run(
            "pr_open",
            "--store",
            str(store),
            "--pipeline-id",
            "testdataops",
            "--version-id",
            "v5",
            "--dag-id",
            "testdataops__v5",
            "--dag-file",
            "dags/testdataops_git__v5.py",
            "--publish-status",
            "pr_open",
            "--promotion-status",
            "submitted",
        )
        v = json.loads(store.read_text(encoding="utf-8"))["pipelines"]["testdataops"]["versions"][0]
        self.assertEqual(v["local_dag_id"], "testdataops__v5")
        self.assertEqual(v["git_dag_id"], "testdataops_git__v5")
        self.assertEqual(v["local_dag_file"], "airflow/dags/testdataops__v5.py")
        self.assertEqual(v["git_dag_file"], "dags/testdataops_git__v5.py")
        self.assertEqual(v["repo_file_path"], "dags/testdataops_git__v5.py")
        self.assertEqual(v["airflow_dag_id"], "testdataops__v5")
        self.assertEqual(v["dag_id"], "testdataops__v5")
        self.assertEqual(v["deployment_target"], "git")

    def test_pr_open_persists_split_fields_for_v6(self):
        store = self.tmp_dir / "split_v6_store.json"
        store.write_text(json.dumps({"pipelines": {}}, indent=2), encoding="utf-8")
        self._run(
            "pr_open",
            "--store",
            str(store),
            "--pipeline-id",
            "testdataops",
            "--version-id",
            "v6",
            "--dag-id",
            "testdataops__v6",
            "--dag-file",
            "dags/testdataops_git__v6.py",
            "--publish-status",
            "pr_open",
            "--promotion-status",
            "submitted",
        )
        v = json.loads(store.read_text(encoding="utf-8"))["pipelines"]["testdataops"]["versions"][0]
        self.assertEqual(v["local_dag_id"], "testdataops__v6")
        self.assertEqual(v["local_dag_file"], "airflow/dags/testdataops__v6.py")
        self.assertEqual(v["git_dag_id"], "testdataops_git__v6")
        self.assertEqual(v["git_dag_file"], "dags/testdataops_git__v6.py")
        self.assertEqual(v["deployment_target"], "git")
        self.assertEqual(v["airflow_dag_id"], "testdataops__v6")
        self.assertEqual(v["dag_id"], "testdataops__v6")
        self.assertEqual(v["repo_file_path"], "dags/testdataops_git__v6.py")


if __name__ == "__main__":
    unittest.main()
