from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

TEST_DIR = Path(__file__).resolve().parent
APP_DIR = TEST_DIR.parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

import app as app_module
from services.dag_publish_store import (
    STATUS_LOCAL_DELETED,
    STATUS_PR_CLOSED_UNMERGED,
    STATUS_PR_OPEN,
    get_record_by_pipeline_version,
)
from services.pipeline_versions import get_version, register_version
from services.pr_publish_service import (
    RequesterIdentity,
    build_branch_name,
    create_pull_request_for_version,
    handle_pull_request_closed_event,
    sanitize_repo_relative_path,
    sanitize_workflow_slug,
)


class FakeGitHubClient:
    def __init__(self):
        self.settings = SimpleNamespace(
            base_branch="main",
            dags_repo_dir="airflow/dags",
            branch_collision_strategy="suffix",
        )
        self._branches = {"main": "sha-main"}
        self._prs_by_branch: dict[str, list[dict]] = {}
        self._pr_counter = 100
        self._files: dict[tuple[str, str], dict] = {}

    def create_installation_token(self) -> str:
        return "token"

    def get_branch_sha(self, *, branch: str, token: str) -> str | None:
        return self._branches.get(branch)

    def create_branch(self, *, branch: str, source_sha: str, token: str) -> str:
        self._branches[branch] = source_sha
        return source_sha

    def upsert_text_file(self, *, repo_path: str, branch: str, content_text: str, commit_message: str, token: str):
        commit_sha = f"commit-{len(content_text)}"
        self._files[(repo_path, branch)] = {"sha": "file-sha", "content": content_text}
        return "file-sha", commit_sha

    def list_open_pull_requests_for_branch(self, *, branch: str, token: str) -> list[dict]:
        return list(self._prs_by_branch.get(branch, []))

    def create_pull_request(self, *, title: str, body: str, head: str, token: str) -> dict:
        self._pr_counter += 1
        payload = {
            "number": self._pr_counter,
            "html_url": f"https://example.test/pr/{self._pr_counter}",
            "title": title,
            "body": body,
            "head": {"ref": head},
        }
        self._prs_by_branch.setdefault(head, []).append(payload)
        return payload

    def get_file(self, *, repo_path: str, ref: str, token: str) -> dict | None:
        return self._files.get((repo_path, ref))


class PrPublishServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmp.name)
        self.pipeline_versions_path = self.tmp_path / "pipeline_versions_store.json"
        self.publish_store_path = self.tmp_path / "dag_publish_store.json"
        self.generated_file = self.tmp_path / "generated" / "test_v1.py"
        self.airflow_file = self.tmp_path / "airflow" / "test_v1.py"
        self.generated_file.parent.mkdir(parents=True, exist_ok=True)
        self.airflow_file.parent.mkdir(parents=True, exist_ok=True)
        self.generated_file.write_text("print('hello')\n", encoding="utf-8")
        self.airflow_file.write_text("print('hello')\n", encoding="utf-8")

        register_version(
            self.pipeline_versions_path,
            pipeline_id="test",
            dag_version=1,
            version_id="v1",
            airflow_dag_id="test__v1",
            output_filename="test_v1.py",
            source_definition={"pipeline": {"dag_id": "test"}},
            generated_local_path=str(self.generated_file),
            generated_airflow_path=str(self.airflow_file),
            node_task_map={},
            node_primary_task_map={},
            created_by="dev_user",
        )
        self.requester = RequesterIdentity(
            user_id="usr_1",
            email="dev@example.com",
            display_name="Dev User",
            username="dev_user",
        )

    def tearDown(self):
        self.tmp.cleanup()

    def test_workflow_slug_sanitization(self):
        self.assertEqual(sanitize_workflow_slug("My Workflow 42"), "my-workflow-42")

    def test_branch_naming(self):
        self.assertEqual(build_branch_name("test-flow"), "feature/workflow/test-flow")
        self.assertEqual(build_branch_name("test-flow", 2), "feature/workflow/test-flow-2")

    def test_repo_path_generation_is_safe(self):
        self.assertEqual(sanitize_repo_relative_path("airflow/dags", "test_v1.py"), "airflow/dags/test_v1.py")
        with self.assertRaises(Exception):
            sanitize_repo_relative_path("airflow/dags", "../evil.py")

    def test_create_pr_from_explicit_selected_version(self):
        client = FakeGitHubClient()
        result = create_pull_request_for_version(
            pipeline_versions_store_path=self.pipeline_versions_path,
            publish_store_path=self.publish_store_path,
            github_client=client,
            pipeline_id="test",
            workflow_name="Test Workflow",
            version_identifier="v1",
            pr_description="publish now",
            requester=self.requester,
        )
        self.assertEqual(result["status"], "pr_open")
        self.assertTrue(result["pull_request_url"])
        self.assertEqual(result["repo_file_path"], "airflow/dags/test_v1.py")

        record = get_record_by_pipeline_version(self.publish_store_path, pipeline_id="test", version_id="v1")
        self.assertIsNotNone(record)
        self.assertEqual(record["status"], STATUS_PR_OPEN)
        version = get_version(self.pipeline_versions_path, "test", "v1")
        self.assertEqual(version.get("publish_status"), STATUS_PR_OPEN)

    def test_webhook_merge_deletes_local_only_after_merge(self):
        client = FakeGitHubClient()
        create_pull_request_for_version(
            pipeline_versions_store_path=self.pipeline_versions_path,
            publish_store_path=self.publish_store_path,
            github_client=client,
            pipeline_id="test",
            workflow_name="Test Workflow",
            version_identifier="v1",
            pr_description="",
            requester=self.requester,
        )
        self.assertTrue(self.airflow_file.exists())

        repo_path = "airflow/dags/test_v1.py"
        client._files[(repo_path, "main")] = {"sha": "repo-sha"}
        payload = {
            "action": "closed",
            "pull_request": {
                "number": 101,
                "merged": True,
                "merged_at": "2026-04-23T10:00:00Z",
                "merge_commit_sha": "abc123",
            },
        }
        result = handle_pull_request_closed_event(
            payload=payload,
            delivery_id="delivery-1",
            publish_store_path=self.publish_store_path,
            pipeline_versions_store_path=self.pipeline_versions_path,
            github_client=client,
        )
        self.assertTrue(result["merged"])
        self.assertFalse(self.airflow_file.exists())
        version = get_version(self.pipeline_versions_path, "test", "v1")
        self.assertEqual(version.get("publish_status"), STATUS_LOCAL_DELETED)

    def test_webhook_is_idempotent(self):
        client = FakeGitHubClient()
        create_pull_request_for_version(
            pipeline_versions_store_path=self.pipeline_versions_path,
            publish_store_path=self.publish_store_path,
            github_client=client,
            pipeline_id="test",
            workflow_name="Test Workflow",
            version_identifier="v1",
            pr_description="",
            requester=self.requester,
        )
        client._files[("airflow/dags/test_v1.py", "main")] = {"sha": "repo-sha"}
        payload = {
            "action": "closed",
            "pull_request": {"number": 101, "merged": True, "merge_commit_sha": "abc123"},
        }
        first = handle_pull_request_closed_event(
            payload=payload,
            delivery_id="delivery-2",
            publish_store_path=self.publish_store_path,
            pipeline_versions_store_path=self.pipeline_versions_path,
            github_client=client,
        )
        second = handle_pull_request_closed_event(
            payload=payload,
            delivery_id="delivery-2",
            publish_store_path=self.publish_store_path,
            pipeline_versions_store_path=self.pipeline_versions_path,
            github_client=client,
        )
        self.assertTrue(first["handled"])
        self.assertTrue(second["duplicate"])

    def test_pr_closed_without_merge_keeps_local_file(self):
        client = FakeGitHubClient()
        create_pull_request_for_version(
            pipeline_versions_store_path=self.pipeline_versions_path,
            publish_store_path=self.publish_store_path,
            github_client=client,
            pipeline_id="test",
            workflow_name="Test Workflow",
            version_identifier="v1",
            pr_description="",
            requester=self.requester,
        )
        payload = {
            "action": "closed",
            "pull_request": {"number": 101, "merged": False},
        }
        result = handle_pull_request_closed_event(
            payload=payload,
            delivery_id="delivery-3",
            publish_store_path=self.publish_store_path,
            pipeline_versions_store_path=self.pipeline_versions_path,
            github_client=client,
        )
        self.assertFalse(result["merged"])
        self.assertTrue(self.airflow_file.exists())
        record = get_record_by_pipeline_version(self.publish_store_path, pipeline_id="test", version_id="v1")
        self.assertEqual(record.get("status"), STATUS_PR_CLOSED_UNMERGED)


class PrPublishApiAuthorizationTests(unittest.TestCase):
    def setUp(self):
        self.app = app_module.app
        self.app.config["TESTING"] = True
        self.client = self.app.test_client()

    @staticmethod
    def _set_session(client, username: str, roles: list[str]):
        with client.session_transaction() as sess:
            sess["is_authenticated"] = True
            sess["username"] = username
            sess["roles"] = roles

    def test_viewer_cannot_create_pull_request(self):
        self._set_session(self.client, "viewer_user", ["viewer"])
        response = self.client.post(
            "/api/workflows/test/pull-requests",
            json={"workflow_name": "Test", "dag_version_id": "v1"},
        )
        self.assertEqual(response.status_code, 403)

    def test_publisher_can_create_pull_request(self):
        self._set_session(self.client, "publisher_user", ["publisher"])
        with patch.object(
            app_module,
            "create_pull_request_for_version",
            return_value={
                "status": "pr_open",
                "pipeline_id": "test",
                "version_id": "v1",
                "pull_request_number": 10,
                "pull_request_url": "https://example/pr/10",
                "branch_name": "feature/workflow/test",
                "commit_sha": "abc",
                "repo_file_path": "airflow/dags/test_v1.py",
                "branch_reused": False,
            },
        ), patch.object(app_module, "_github_app_client", return_value=object()):
            response = self.client.post(
                "/api/workflows/test/pull-requests",
                json={"workflow_name": "Test", "dag_version_id": "v1"},
            )
        self.assertEqual(response.status_code, 200)
        payload = response.get_json() or {}
        self.assertTrue(payload.get("success"))
        self.assertEqual(payload.get("status"), "pr_open")


if __name__ == "__main__":
    unittest.main()
