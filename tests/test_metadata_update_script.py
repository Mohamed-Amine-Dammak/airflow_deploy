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
        self.root = self.tmp_dir
        self.script = Path(__file__).resolve().parents[1] / "scripts" / "metadata" / "update_pipeline_metadata.py"
        (self.root / "airflow" / "web_app_data" / "metadata").mkdir(parents=True, exist_ok=True)

    def _run(self, *args: str) -> None:
        cmd = [sys.executable, str(self.script), *args, "--root", str(self.root)]
        subprocess.run(cmd, check=True)

    def _run_capture(self, *args: str) -> subprocess.CompletedProcess[str]:
        cmd = [sys.executable, str(self.script), *args, "--root", str(self.root)]
        return subprocess.run(cmd, check=False, capture_output=True, text=True)

    def _version_path(self, pipeline_id: str, version_id: str) -> Path:
        return self.root / "airflow" / "web_app_data" / "metadata" / pipeline_id / f"{version_id}.json"

    def _load_version(self, pipeline_id: str, version_id: str) -> dict:
        return json.loads(self._version_path(pipeline_id, version_id).read_text(encoding="utf-8"))

    def test_pr_open_creates_one_version_file_with_split_identity(self):
        self._run(
            "pr_open",
            "--pipeline-id",
            "testdataops",
            "--version-id",
            "v4",
            "--dag-id",
            "testdataops__v4",
            "--dag-file",
            "dags/testdataops_git__v4.py",
            "--branch",
            "feature/workflow/testdataops-v4",
            "--publish-status",
            "pr_open",
            "--promotion-status",
            "submitted",
        )
        path = self._version_path("testdataops", "v4")
        self.assertTrue(path.exists())
        subprocess.run([sys.executable, "-m", "json.tool", str(path)], check=True, stdout=subprocess.DEVNULL)
        data = self._load_version("testdataops", "v4")
        self.assertEqual(data["pipeline_id"], "testdataops")
        self.assertEqual(data["version_id"], "v4")
        self.assertEqual(data["local_dag_id"], "testdataops__v4")
        self.assertEqual(data["airflow_dag_id"], "testdataops__v4")
        self.assertEqual(data["dag_id"], "testdataops__v4")
        self.assertEqual(data["local_dag_file"], "airflow/dags/testdataops__v4.py")
        self.assertEqual(data["git_dag_id"], "testdataops_git__v4")
        self.assertEqual(data["git_dag_file"], "dags/testdataops_git__v4.py")
        self.assertEqual(data["repo_file_path"], "dags/testdataops_git__v4.py")
        self.assertEqual(data["deployment_target"], "git")

    def test_merged_updates_single_version_file(self):
        self._run(
            "pr_open",
            "--pipeline-id",
            "pipe1",
            "--version-id",
            "v1",
            "--dag-id",
            "pipe1__v1",
            "--dag-file",
            "dags/pipe1_git__v1.py",
            "--pr-number",
            "11",
            "--branch",
            "feature/workflow/pipe1-v1",
        )
        self._run(
            "merged",
            "--pr-number",
            "11",
            "--branch",
            "feature/workflow/pipe1-v1",
            "--commit-sha",
            "merge123",
        )
        data = self._load_version("pipe1", "v1")
        self.assertEqual(data["publish_status"], "merged_to_git")
        self.assertEqual(data["promotion_status"], "submitted")
        self.assertEqual(data["merge_commit_sha"], "merge123")

    def test_promote_updates_candidate_and_previous_champion_only(self):
        self._run("pr_open", "--pipeline-id", "pipe1", "--version-id", "v0", "--dag-id", "pipe1__v0", "--dag-file", "dags/pipe1_git__v0.py")
        self._run("pr_open", "--pipeline-id", "pipe1", "--version-id", "v1", "--dag-id", "pipe1__v1", "--dag-file", "dags/pipe1_git__v1.py")
        self._run("promote", "--pipeline-id", "pipe1", "--version-id", "v0", "--promotion-status", "champion")
        self._run("promote", "--pipeline-id", "pipe1", "--version-id", "v1", "--promotion-status", "champion")
        v0 = self._load_version("pipe1", "v0")
        v1 = self._load_version("pipe1", "v1")
        self.assertEqual(v0["promotion_status"], "archived")
        self.assertEqual(v1["promotion_status"], "champion")
        self.assertTrue(v1.get("promotion_history"))

    def test_mark_eval_uses_per_version_metadata_and_ignores_invalid_legacy_store(self):
        legacy_store = self.root / "airflow" / "web_app_data" / "pipeline_versions_store.json"
        legacy_store.parent.mkdir(parents=True, exist_ok=True)
        legacy_store.write_text("{not-json", encoding="utf-8")
        self._run("pr_open", "--pipeline-id", "pipe1", "--version-id", "v1", "--dag-id", "pipe1__v1", "--dag-file", "dags/pipe1_git__v1.py")
        self._run("mark_eval", "--pipeline-id", "pipe1", "--dag-id", "pipe1__v1")
        data = self._load_version("pipe1", "v1")
        self.assertEqual(data["promotion_status"], "eval")
        self.assertEqual(data["evaluated_branch"], "eval")

    def test_mark_eval_transitions_submitted_to_eval_and_persists(self):
        self._run(
            "pr_open",
            "--pipeline-id",
            "pipe1",
            "--version-id",
            "v2",
            "--dag-id",
            "pipe1__v2",
            "--dag-file",
            "dags/pipe1_git__v2.py",
            "--publish-status",
            "merged_to_git",
            "--promotion-status",
            "submitted",
        )
        before = self._load_version("pipe1", "v2")
        self.assertEqual(before["publish_status"], "merged_to_git")
        self.assertEqual(before["promotion_status"], "submitted")
        self._run("mark_eval", "--pipeline-id", "pipe1", "--dag-id", "pipe1__v2")
        after = self._load_version("pipe1", "v2")
        self.assertEqual(after["publish_status"], "merged_to_git")
        self.assertEqual(after["promotion_status"], "eval")
        self.assertEqual(after["evaluated_branch"], "eval")

    def test_mark_eval_fails_clearly_when_metadata_file_missing(self):
        result = self._run_capture("mark_eval", "--pipeline-id", "pipe1", "--dag-id", "pipe1__v1")
        self.assertEqual(result.returncode, 1)
        self.assertIn("metadata file not found for pipeline_id=pipe1, dag_id=pipe1__v1", result.stdout)

    def test_mark_eval_file_updates_exact_metadata_file(self):
        self._run(
            "pr_open",
            "--pipeline-id",
            "pipe1",
            "--version-id",
            "v3",
            "--dag-id",
            "pipe1__v3",
            "--dag-file",
            "dags/pipe1_git__v3.py",
            "--publish-status",
            "pr_open",
            "--promotion-status",
            "draft",
        )
        rel = "airflow/web_app_data/metadata/pipe1/v3.json"
        self._run("mark_eval_file", "--metadata-file", rel)
        data = self._load_version("pipe1", "v3")
        self.assertEqual(data["promotion_status"], "eval")
        self.assertEqual(data["evaluated_branch"], "eval")


if __name__ == "__main__":
    unittest.main()
