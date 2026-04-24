from __future__ import annotations

import hashlib
import hmac
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

from services.dag_publish_store import (
    STATUS_LOCAL_DELETED,
    STATUS_MERGED_TO_GIT,
    STATUS_PR_CLOSED_UNMERGED,
    STATUS_PR_OPEN,
    get_record_by_pipeline_version,
    get_record_by_pull_request_number,
    mark_webhook_delivery_processed,
    upsert_record,
    utc_now_iso,
)
from services.github_app_client import GitHubAppClient
from services.pipeline_versions import get_version, patch_version_fields


class PublishError(Exception):
    def __init__(self, message: str, *, code: str, status_code: int):
        super().__init__(message)
        self.code = code
        self.status_code = status_code


class PublishValidationError(PublishError):
    def __init__(self, message: str):
        super().__init__(message, code="validation_error", status_code=400)


class PublishNotFoundError(PublishError):
    def __init__(self, message: str):
        super().__init__(message, code="not_found", status_code=404)


class PublishConflictError(PublishError):
    def __init__(self, message: str):
        super().__init__(message, code="conflict", status_code=409)


class PublishExternalError(PublishError):
    def __init__(self, message: str):
        super().__init__(message, code="github_api_error", status_code=502)


@dataclass(frozen=True)
class RequesterIdentity:
    user_id: str
    email: str
    display_name: str
    username: str


def sanitize_workflow_slug(name: str) -> str:
    raw = str(name or "").strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "-", raw).strip("-")
    normalized = re.sub(r"-{2,}", "-", normalized)
    if not normalized:
        raise PublishValidationError("workflow_name is required and must contain at least one letter or number.")
    return normalized[:80]


def sanitize_repo_relative_path(repo_dir: str, filename: str) -> str:
    safe_filename = str(filename or "").strip()
    if not safe_filename or not safe_filename.endswith(".py"):
        raise PublishValidationError("selected version output filename must end with .py.")
    if "/" in safe_filename or "\\" in safe_filename:
        raise PublishValidationError("selected version output filename cannot contain path separators.")

    base_dir = PurePosixPath(str(repo_dir or "").strip().strip("/"))
    if not str(base_dir):
        raise PublishValidationError("GITHUB_DAGS_REPO_DIR must be configured.")
    full = base_dir / safe_filename
    normalized = PurePosixPath(str(full))
    if ".." in normalized.parts:
        raise PublishValidationError("repo path traversal detected.")
    return str(normalized).replace("\\", "/")


def select_version_identifier(payload: dict[str, Any]) -> str:
    version_id = str(payload.get("dag_version_id", "")).strip()
    if not version_id:
        version_id = str(payload.get("version_id", "")).strip()
    if not version_id:
        version_id = str(payload.get("version_name", "")).strip()
    if not version_id:
        raise PublishValidationError("dag_version_id or version_id/version_name is required.")
    return version_id


def build_branch_name(workflow_slug: str, suffix: int | None = None) -> str:
    base = f"feature/workflow/{workflow_slug}"
    if suffix and suffix > 1:
        return f"{base}-{suffix}"
    return base


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _pr_default_body(
    *,
    workflow_name: str,
    version_id: str,
    source_label: str,
    requested_at: str,
    requester: RequesterIdentity,
    extra_body: str,
) -> str:
    chunks = [
        "## DAG Publish Request",
        f"- Workflow: {workflow_name}",
        f"- Selected DAG version: {version_id}",
        f"- Source: {source_label}",
        f"- Requested at: {requested_at}",
        f"- Requested by: {requester.display_name or requester.username} ({requester.email or requester.username})",
    ]
    if extra_body:
        chunks.append("")
        chunks.append("## Additional Notes")
        chunks.append(extra_body)
    return "\n".join(chunks).strip()


def _load_version_content(version: dict[str, Any]) -> tuple[str, Path]:
    local_path = str(version.get("generated_local_path", "")).strip()
    if not local_path:
        raise PublishValidationError("Selected DAG version does not include generated_local_path.")
    path = Path(local_path)
    if not path.exists() or not path.is_file():
        raise PublishNotFoundError("Selected DAG version file is missing from local storage.")
    content = path.read_text(encoding="utf-8")
    if not content.strip():
        raise PublishValidationError("Selected DAG version content is empty.")
    return content, path


def create_pull_request_for_version(
    *,
    pipeline_versions_store_path: Path,
    publish_store_path: Path,
    github_client: GitHubAppClient,
    pipeline_id: str,
    workflow_name: str,
    version_identifier: str,
    pr_description: str,
    requester: RequesterIdentity,
) -> dict[str, Any]:
    safe_pipeline_id = str(pipeline_id or "").strip()
    if not safe_pipeline_id:
        raise PublishValidationError("workflowId path parameter is required.")

    workflow_slug = sanitize_workflow_slug(workflow_name)
    safe_version_id = str(version_identifier or "").strip()
    version = get_version(pipeline_versions_store_path, safe_pipeline_id, safe_version_id)
    if not version:
        raise PublishNotFoundError("Selected DAG version does not exist.")

    output_filename = str(version.get("output_filename", "")).strip()
    repo_file_path = sanitize_repo_relative_path(github_client.settings.dags_repo_dir, output_filename)
    content, source_path = _load_version_content(version)
    source_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
    requested_at = _utc_now()

    existing_record = get_record_by_pipeline_version(
        publish_store_path,
        pipeline_id=safe_pipeline_id,
        version_id=safe_version_id,
    )
    if existing_record and str(existing_record.get("status", "")).strip() == STATUS_PR_OPEN:
        pr_url = str(existing_record.get("pull_request_url", "")).strip()
        if pr_url:
            return {
                "status": "already_open",
                "pipeline_id": safe_pipeline_id,
                "version_id": safe_version_id,
                "pull_request_number": int(existing_record.get("pull_request_number") or 0),
                "pull_request_url": pr_url,
                "branch_name": str(existing_record.get("git_branch_name", "")).strip(),
                "commit_sha": str(existing_record.get("commit_sha", "")).strip(),
            }

    token = github_client.create_installation_token()
    base_branch = github_client.settings.base_branch
    branch_name = build_branch_name(workflow_slug)

    branch_sha = github_client.get_branch_sha(branch=branch_name, token=token)
    branch_reused = False
    if not branch_sha:
        base_sha = github_client.get_branch_sha(branch=base_branch, token=token)
        if not base_sha:
            raise PublishExternalError(f"Base branch '{base_branch}' was not found.")
        github_client.create_branch(branch=branch_name, source_sha=base_sha, token=token)
    else:
        should_suffix = github_client.settings.branch_collision_strategy == "suffix"
        if not should_suffix and github_client.settings.branch_collision_strategy == "reuse_or_suffix":
            open_prs = github_client.list_open_pull_requests_for_branch(branch=branch_name, token=token)
            same_version_reusable = bool(
                existing_record
                and str(existing_record.get("status", "")).strip() == STATUS_PR_OPEN
                and str(existing_record.get("git_branch_name", "")).strip() == branch_name
                and int(existing_record.get("pull_request_number") or 0) > 0
                and open_prs
            )
            should_suffix = not same_version_reusable
            branch_reused = same_version_reusable

        if should_suffix:
            idx = 2
            while True:
                candidate = build_branch_name(workflow_slug, idx)
                candidate_sha = github_client.get_branch_sha(branch=candidate, token=token)
                if not candidate_sha:
                    base_sha = github_client.get_branch_sha(branch=base_branch, token=token)
                    if not base_sha:
                        raise PublishExternalError(f"Base branch '{base_branch}' was not found.")
                    github_client.create_branch(branch=candidate, source_sha=base_sha, token=token)
                    branch_name = candidate
                    break
                idx += 1
                if idx > 99:
                    raise PublishConflictError("Could not allocate a unique branch name.")
        else:
            branch_reused = True

    commit_msg = f"Publish DAG {output_filename} for workflow {workflow_slug} ({safe_version_id})"
    _, commit_sha = github_client.upsert_text_file(
        repo_path=repo_file_path,
        branch=branch_name,
        content_text=content,
        commit_message=commit_msg,
        token=token,
    )

    title = str(workflow_name or "").strip() or workflow_slug
    body = _pr_default_body(
        workflow_name=workflow_name,
        version_id=safe_version_id,
        source_label="airflow-orchestration-platform",
        requested_at=requested_at,
        requester=requester,
        extra_body=str(pr_description or "").strip(),
    )

    existing_prs = github_client.list_open_pull_requests_for_branch(branch=branch_name, token=token)
    if existing_prs:
        pr_payload = existing_prs[0]
    else:
        pr_payload = github_client.create_pull_request(title=title, body=body, head=branch_name, token=token)

    pr_number = int(pr_payload.get("number") or 0)
    pr_url = str(pr_payload.get("html_url") or "").strip()
    if not pr_number or not pr_url:
        raise PublishExternalError("GitHub pull request creation returned incomplete metadata.")

    local_airflow_path = str(version.get("generated_airflow_path", "")).strip()
    record = upsert_record(
        publish_store_path,
        {
            "id": str(existing_record.get("id") if isinstance(existing_record, dict) else "") or f"{safe_pipeline_id}:{safe_version_id}",
            "pipeline_id": safe_pipeline_id,
            "version_id": safe_version_id,
            "workflow_name": workflow_name,
            "workflow_slug": workflow_slug,
            "version_name": safe_version_id,
            "local_file_name": output_filename,
            "local_file_path": local_airflow_path or str(source_path),
            "repo_file_path": repo_file_path,
            "git_branch_name": branch_name,
            "pull_request_number": pr_number,
            "pull_request_url": pr_url,
            "commit_sha": commit_sha,
            "status": STATUS_PR_OPEN,
            "requested_by_user_id": requester.user_id,
            "requested_by_email": requester.email,
            "requested_by_display_name": requester.display_name,
            "requested_by_username": requester.username,
            "source_hash": source_hash,
            "created_at": str((existing_record or {}).get("created_at") or requested_at),
            "updated_at": requested_at,
        },
    )

    patch_version_fields(
        pipeline_versions_store_path,
        pipeline_id=safe_pipeline_id,
        version_id=safe_version_id,
        patch={
            "publish_status": STATUS_PR_OPEN,
            "workflow_name": workflow_name,
            "workflow_slug": workflow_slug,
            "repo_file_path": repo_file_path,
            "git_branch_name": branch_name,
            "pull_request_number": pr_number,
            "pull_request_url": pr_url,
            "commit_sha": commit_sha,
            "requested_by_user_id": requester.user_id,
            "requested_by_email": requester.email,
            "requested_by_display_name": requester.display_name,
        },
    )

    return {
        "status": "pr_open",
        "pipeline_id": safe_pipeline_id,
        "version_id": safe_version_id,
        "pull_request_number": pr_number,
        "pull_request_url": pr_url,
        "branch_name": branch_name,
        "commit_sha": commit_sha,
        "repo_file_path": repo_file_path,
        "branch_reused": branch_reused,
        "record": record,
    }


def validate_webhook_signature(secret: str, body: bytes, header_signature: str) -> bool:
    safe_secret = str(secret or "").strip()
    safe_sig = str(header_signature or "").strip()
    if not safe_secret:
        return False
    if not safe_sig.startswith("sha256="):
        return False
    expected = "sha256=" + hmac.new(safe_secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, safe_sig)


def handle_pull_request_closed_event(
    *,
    payload: dict[str, Any],
    delivery_id: str,
    publish_store_path: Path,
    pipeline_versions_store_path: Path,
    github_client: GitHubAppClient,
) -> dict[str, Any]:
    if not mark_webhook_delivery_processed(publish_store_path, delivery_id):
        return {"handled": True, "duplicate": True, "reason": "delivery_already_processed"}

    pr = payload.get("pull_request") if isinstance(payload.get("pull_request"), dict) else {}
    pr_number = int(pr.get("number") or payload.get("number") or 0)
    if not pr_number:
        return {"handled": True, "ignored": True, "reason": "missing_pr_number"}

    record = get_record_by_pull_request_number(publish_store_path, pull_request_number=pr_number)
    if not record:
        return {"handled": True, "ignored": True, "reason": "unknown_pr_number", "pull_request_number": pr_number}

    merged = bool(pr.get("merged"))
    pipeline_id = str(record.get("pipeline_id", "")).strip()
    version_id = str(record.get("version_id", "")).strip()

    if not merged:
        upsert_record(
            publish_store_path,
            {**record, "status": STATUS_PR_CLOSED_UNMERGED, "updated_at": utc_now_iso()},
        )
        patch_version_fields(
            pipeline_versions_store_path,
            pipeline_id=pipeline_id,
            version_id=version_id,
            patch={"publish_status": STATUS_PR_CLOSED_UNMERGED},
        )
        return {"handled": True, "merged": False, "status": STATUS_PR_CLOSED_UNMERGED, "pull_request_number": pr_number}

    merged_at = str(pr.get("merged_at") or _utc_now()).strip()
    merge_commit_sha = str(pr.get("merge_commit_sha") or "").strip()
    token = github_client.create_installation_token()
    repo_file_path = str(record.get("repo_file_path", "")).strip()
    if repo_file_path:
        github_file = github_client.get_file(
            repo_path=repo_file_path,
            ref=github_client.settings.base_branch,
            token=token,
        )
        if not github_file and not merge_commit_sha:
            raise PublishExternalError("Merged PR verification failed: expected repo file not found.")

    local_path_raw = str(record.get("local_file_path", "")).strip()
    deleted_local = False
    if local_path_raw:
        local_path = Path(local_path_raw)
        if local_path.exists() and local_path.is_file():
            local_path.unlink()
            deleted_local = True

    next_status = STATUS_LOCAL_DELETED if deleted_local else STATUS_MERGED_TO_GIT
    now_iso = utc_now_iso()
    updated = upsert_record(
        publish_store_path,
        {
            **record,
            "status": next_status,
            "merged_at": merged_at,
            "merge_commit_sha": merge_commit_sha,
            "deleted_from_local_at": now_iso if deleted_local else str(record.get("deleted_from_local_at", "")),
            "updated_at": now_iso,
        },
    )
    patch_version_fields(
        pipeline_versions_store_path,
        pipeline_id=pipeline_id,
        version_id=version_id,
        patch={
            "publish_status": next_status,
            "merged_at": merged_at,
            "merge_commit_sha": merge_commit_sha,
            "deleted_from_local_at": now_iso if deleted_local else None,
        },
    )

    return {
        "handled": True,
        "merged": True,
        "status": next_status,
        "pull_request_number": pr_number,
        "deleted_local_file": deleted_local,
        "record": updated,
    }
