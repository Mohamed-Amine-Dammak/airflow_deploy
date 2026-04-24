from __future__ import annotations

import json
import re
import hmac
import os
import socket
import time
import copy
import base64
import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from uuid import uuid4

from flask import Flask, jsonify, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash
import requests
from requests import RequestException

from config import AppConfig, ensure_app_directories
from services.airflow_api import (
    AirflowApiConfig,
    AirflowApiError,
    check_dag_readiness,
    create_connection,
    delete_connection,
    get_airflow_platform_snapshot,
    get_dag_run_status,
    get_task_instance_log,
    list_dag_runs_for_dag,
    list_dag_run_task_instances,
    list_connections,
    retry_task_instance,
    stop_dag_run,
    trigger_dag_run,
    update_connection,
)
from services.container_file_sync import (
    ContainerFileSyncError,
    sync_host_file_to_containers_at_path,
    sync_host_file_to_containers,
    sync_gcs_local_files_to_containers,
)
from services.dag_generator import DagGenerator
from services.file_writer import write_and_copy_dag
from services.pipeline_versions import (
    clear_versions as clear_pipeline_versions,
    find_matching_version,
    get_version as get_pipeline_version,
    list_versions as list_pipeline_versions,
    plan_next_version,
    register_version,
    set_current_version,
    update_version_definition,
)
from services.pipeline_validator import validate_pipeline
from services.schema_registry import get_module_schema, list_modules
from services.github_app_client import GitHubAppClient, GitHubAppSettings, GitHubClientError, load_private_key_from_config
from services.pr_publish_service import (
    PublishError,
    RequesterIdentity,
    create_pull_request_for_version,
    handle_pull_request_closed_event,
    select_version_identifier,
    validate_webhook_signature,
)


app = Flask(__name__)
app.config.from_object(AppConfig)
ensure_app_directories()

dag_generator = DagGenerator(AppConfig.DAG_TEMPLATES_DIR)
SUPPORTED_CONNECTION_TYPES = {"http", "wasb", "gcp_service_account"}
PERMISSION_DEFINITIONS: dict[str, dict[str, str]] = {
    "builder_view": {"label": "View builder workspace", "description": "Open and inspect the pipeline builder."},
    "pipeline_load": {"label": "Load saved pipelines", "description": "Load saved pipeline JSON files."},
    "pipeline_edit": {"label": "Edit pipeline and nodes", "description": "Modify pipeline settings, node configs, and connections."},
    "pipeline_new": {"label": "Create new pipeline", "description": "Reset workspace to start a new pipeline."},
    "pipeline_save": {"label": "Save pipeline JSON", "description": "Save the current pipeline to saved files."},
    "pipeline_validate": {"label": "Validate pipeline", "description": "Run validation checks from the builder."},
    "pipeline_layout": {"label": "Auto layout", "description": "Rearrange nodes automatically on canvas."},
    "dag_generate": {"label": "Generate DAG", "description": "Generate and export Airflow DAG files."},
    "pr_publish": {
        "label": "Publish DAG version to GitHub PR",
        "description": "Create GitHub branches/commits/pull requests for selected DAG versions.",
    },
    "dag_run": {"label": "Run DAG", "description": "Trigger DAG runs from builder."},
    "dag_retry": {"label": "Retry DAG run", "description": "Trigger DAG retry from header action."},
    "task_retry": {"label": "Retry failed module", "description": "Retry failed/up-for-retry task instances."},
    "logs_view": {"label": "View task logs", "description": "Read task instance logs and run status."},
    "connections_view": {"label": "View connections", "description": "Open Airflow connections page and list items."},
    "connections_manage": {
        "label": "Manage connections",
        "description": "Create, edit, and delete Airflow connections.",
    },
    "admin_dashboard": {"label": "Access admin dashboard", "description": "Open admin pages and metrics."},
    "users_manage": {"label": "Manage users", "description": "Create, edit, and delete users."},
    "roles_manage": {"label": "Manage roles", "description": "Create, edit, and delete custom roles."},
}
PREDEFINED_ROLE_DEFINITIONS: dict[str, dict[str, Any]] = {
    "admin": {
        "description": "Full platform administration access.",
        "privileges": [
            "Access all pages and features",
            "Manage users and roles",
            "Manage Airflow connections",
            "Generate, run, and retry DAGs",
        ],
        "permissions": list(PERMISSION_DEFINITIONS.keys()),
    },
    "developer": {
        "description": "Build and configure orchestration pipelines.",
        "privileges": [
            "Use visual pipeline builder",
            "Configure module settings",
            "Validate and generate DAG files",
            "Preview generated DAG code",
        ],
        "permissions": [
            "builder_view",
            "pipeline_load",
            "pipeline_edit",
            "pipeline_new",
            "pipeline_save",
            "pipeline_validate",
            "pipeline_layout",
            "dag_generate",
            "logs_view",
            "connections_view",
        ],
    },
    "publisher": {
        "description": "Publish tested DAG versions to GitHub through pull requests.",
        "privileges": [
            "View pipeline versions",
            "Create GitHub pull requests for selected DAG versions",
        ],
        "permissions": [
            "builder_view",
            "pipeline_load",
            "pr_publish",
        ],
    },
    "operator": {
        "description": "Operate production runs and monitoring.",
        "privileges": [
            "Generate DAGs when updates are required",
            "Run DAGs from builder",
            "Retry failed DAG/task executions",
            "View run status and task logs",
        ],
        "permissions": [
            "builder_view",
            "pipeline_load",
            "dag_generate",
            "dag_run",
            "dag_retry",
            "task_retry",
            "logs_view",
            "connections_view",
        ],
    },
    "viewer": {
        "description": "Read-only platform visibility.",
        "privileges": [
            "View pipelines and configurations",
            "View DAG previews and run status",
            "View task logs",
        ],
        "permissions": [
            "builder_view",
            "pipeline_load",
            "logs_view",
            "connections_view",
        ],
    },
    "connection_manager": {
        "description": "Manage Airflow connection records.",
        "privileges": [
            "List Airflow connections",
            "Create and edit connections",
            "Delete existing connections",
        ],
        "permissions": [
            "builder_view",
            "pipeline_load",
            "connections_view",
            "connections_manage",
        ],
    },
}
PREDEFINED_ROLES = list(PREDEFINED_ROLE_DEFINITIONS.keys())
REQUEST_TYPE_ADD_MODULE = "add_module"
REQUEST_TYPE_SIGNAL_BUG = "signal_bug"
REQUEST_TYPE_REQUEST_ROLE = "request_role"
REQUEST_TYPE_OTHER = "other_request"
REQUEST_TYPES = {
    REQUEST_TYPE_ADD_MODULE,
    REQUEST_TYPE_SIGNAL_BUG,
    REQUEST_TYPE_REQUEST_ROLE,
    REQUEST_TYPE_OTHER,
}
REQUEST_STATUS_PENDING = "pending"
REQUEST_STATUS_FINISHED = "finished"
REQUEST_STATUSES = {REQUEST_STATUS_PENDING, REQUEST_STATUS_FINISHED}


def _airflow_api_config(timeout_seconds: int | None = None) -> AirflowApiConfig:
    return AirflowApiConfig(
        base_url=AppConfig.AIRFLOW_API_BASE_URL,
        username=AppConfig.AIRFLOW_API_USERNAME,
        password=AppConfig.AIRFLOW_API_PASSWORD,
        verify_tls=AppConfig.AIRFLOW_API_VERIFY_TLS,
        timeout_seconds=timeout_seconds or AppConfig.AIRFLOW_API_TIMEOUT_SECONDS,
    )


def _is_airflow_not_registered_error(exc: Exception) -> bool:
    if not isinstance(exc, AirflowApiError):
        return False
    if getattr(exc, "status_code", None) != 404:
        return False
    return "not registered in airflow yet" in str(exc).lower()


@app.after_request
def _add_cors_headers(response):
    origin = request.headers.get("Origin", "*")
    response.headers["Access-Control-Allow-Origin"] = origin or "*"
    response.headers["Vary"] = "Origin"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    return response


@app.route("/api/<path:_path>", methods=["OPTIONS"])
def api_preflight(_path: str):
    return ("", 204)


def _safe_filename(name: str, default_name: str) -> str:
    clean = re.sub(r"[^A-Za-z0-9._-]+", "_", (name or "").strip())
    clean = clean.strip("._")
    if not clean:
        clean = default_name
    return clean


def _json_error(message: str, status_code: int = 400, **extra: Any):
    payload: dict[str, Any] = {"success": False, "error": message}
    payload.update(extra)
    return jsonify(payload), status_code


TASK_STATE_PRIORITY: dict[str, int] = {
    "failed": 6,
    "up_for_retry": 5,
    "retrying": 5,
    "retry": 5,
    "running": 4,
    "queued": 3,
    "success": 2,
    "skipped": 1,
}


def _current_actor_username() -> str:
    return str(session.get("username", "")).strip()


def _current_requester_identity() -> RequesterIdentity:
    username = _current_actor_username()
    if not username:
        return RequesterIdentity(user_id="", email="", display_name="", username="")

    store = _load_users_store()
    users = [item for item in (store.get("users") or []) if isinstance(item, dict)]
    matched = next(
        (
            item
            for item in users
            if str(item.get("username", "")).strip().lower() == username.lower()
        ),
        None,
    )
    if not matched:
        return RequesterIdentity(user_id=username, email="", display_name=username, username=username)

    user_id = str(matched.get("id", "")).strip() or username
    email = str(matched.get("email", "")).strip()
    display_name = str(matched.get("display_name", "")).strip() or username
    return RequesterIdentity(user_id=user_id, email=email, display_name=display_name, username=username)


def _github_app_client() -> GitHubAppClient:
    private_key = load_private_key_from_config(
        AppConfig.GITHUB_APP_PRIVATE_KEY,
        AppConfig.GITHUB_APP_PRIVATE_KEY_PATH,
        AppConfig.GENERATED_DAGS_DIR.parent,
    )
    if not AppConfig.GITHUB_APP_ID:
        raise PublishError("GitHub App is not configured: missing GITHUB_APP_ID.", code="config_error", status_code=500)
    if not private_key:
        raise PublishError(
            "GitHub App is not configured: missing private key value/path.",
            code="config_error",
            status_code=500,
        )
    if not AppConfig.GITHUB_APP_INSTALLATION_ID:
        raise PublishError(
            "GitHub App is not configured: missing GITHUB_APP_INSTALLATION_ID.",
            code="config_error",
            status_code=500,
        )
    if not AppConfig.GITHUB_OWNER or not AppConfig.GITHUB_REPO:
        raise PublishError(
            "GitHub App is not configured: missing GITHUB_OWNER or GITHUB_REPO.",
            code="config_error",
            status_code=500,
        )

    settings = GitHubAppSettings(
        app_id=AppConfig.GITHUB_APP_ID,
        private_key=private_key,
        installation_id=AppConfig.GITHUB_APP_INSTALLATION_ID,
        owner=AppConfig.GITHUB_OWNER,
        repo=AppConfig.GITHUB_REPO,
        base_branch=AppConfig.GITHUB_BASE_BRANCH,
        dags_repo_dir=AppConfig.GITHUB_DAGS_REPO_DIR,
        api_base_url=AppConfig.GITHUB_API_BASE_URL,
        branch_collision_strategy=AppConfig.GITHUB_BRANCH_COLLISION_STRATEGY,
    )
    return GitHubAppClient(settings=settings)


def _pipeline_topology_signature(payload: dict[str, Any] | None) -> dict[str, Any]:
    safe_payload = payload if isinstance(payload, dict) else {}
    nodes = safe_payload.get("nodes") if isinstance(safe_payload.get("nodes"), list) else []
    edges = safe_payload.get("edges") if isinstance(safe_payload.get("edges"), list) else []

    normalized_nodes = sorted(
        (
            str(item.get("id", "")).strip(),
            str(item.get("type", "")).strip().lower(),
        )
        for item in nodes
        if isinstance(item, dict) and str(item.get("id", "")).strip()
    )
    normalized_edges = sorted(
        (
            str(item.get("source", "")).strip(),
            str(item.get("target", "")).strip(),
        )
        for item in edges
        if isinstance(item, dict) and str(item.get("source", "")).strip() and str(item.get("target", "")).strip()
    )

    return {
        "nodes": normalized_nodes,
        "edges": normalized_edges,
    }


def _build_task_status_by_id(task_instances: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    status_by_id: dict[str, dict[str, Any]] = {}
    for task in task_instances:
        task_id = str(task.get("task_id", "")).strip()
        if not task_id:
            continue
        status_by_id[task_id] = {
            "task_id": task_id,
            "state": task.get("state") or "",
            "try_number": task.get("try_number") or 1,
            "start_date": task.get("start_date") or "",
            "end_date": task.get("end_date") or "",
        }
    return status_by_id


def _build_node_boxes_from_version(
    version: dict[str, Any],
    task_status_by_id: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    source = version.get("source_definition") if isinstance(version.get("source_definition"), dict) else {}
    nodes = source.get("nodes") if isinstance(source.get("nodes"), list) else []
    node_task_map = version.get("node_task_map") if isinstance(version.get("node_task_map"), dict) else {}
    normalized_node_task_map: dict[str, list[str]] = {}
    for node_id, value in node_task_map.items():
        if isinstance(value, list):
            normalized_node_task_map[str(node_id)] = [str(item).strip() for item in value if str(item).strip()]
        elif isinstance(value, str) and value.strip():
            normalized_node_task_map[str(node_id)] = [value.strip()]

    result: list[dict[str, Any]] = []
    for node in nodes:
        node_id = str(node.get("id", "")).strip()
        if not node_id:
            continue
        label = str(node.get("label", "")).strip() or node_id
        node_type = str(node.get("type", "")).strip()
        task_ids = normalized_node_task_map.get(node_id, [])
        matched = [task_status_by_id[task_id] for task_id in task_ids if task_id in task_status_by_id]

        if not matched:
            result.append(
                {
                    "node_id": node_id,
                    "label": label,
                    "type": node_type,
                    "state": "",
                    "has_task_instance": False,
                    "task_id": "",
                    "try_number": 1,
                    "start_date": "",
                    "end_date": "",
                    "task_ids": task_ids,
                }
            )
            continue

        matched.sort(
            key=lambda item: (
                -(TASK_STATE_PRIORITY.get(str(item.get("state") or "").strip().lower(), 0)),
                -(int(item.get("try_number") or 1)),
            )
        )
        chosen = matched[0]
        result.append(
            {
                "node_id": node_id,
                "label": label,
                "type": node_type,
                "state": chosen.get("state") or "",
                "has_task_instance": True,
                "task_id": chosen.get("task_id") or "",
                "try_number": chosen.get("try_number") or 1,
                "start_date": chosen.get("start_date") or "",
                "end_date": chosen.get("end_date") or "",
                "task_ids": task_ids,
            }
        )
    return result


def _version_last_execution_summary(version: dict[str, Any]) -> dict[str, Any]:
    airflow_dag_id = str(version.get("airflow_dag_id", "")).strip()
    summary: dict[str, Any] = {
        "pipeline_id": version.get("pipeline_id"),
        "version_id": version.get("version_id"),
        "airflow_dag_id": airflow_dag_id,
        "has_ever_run": False,
        "last_run": None,
        "tasks": [],
        "task_status_by_id": {},
        "node_boxes": [],
    }
    if not airflow_dag_id:
        return summary

    runs = list_dag_runs_for_dag(_airflow_api_config(), airflow_dag_id, limit=1)
    if not runs:
        return summary

    last_run = runs[0]
    dag_run_id = str(last_run.get("dag_run_id", "")).strip()
    summary["has_ever_run"] = True
    summary["last_run"] = {
        "dag_run_id": dag_run_id,
        "state": last_run.get("state"),
        "start_date": last_run.get("start_date"),
        "end_date": last_run.get("end_date"),
        "logical_date": last_run.get("logical_date"),
    }

    if not dag_run_id:
        return summary

    tasks = list_dag_run_task_instances(_airflow_api_config(), airflow_dag_id, dag_run_id)
    task_status_by_id = _build_task_status_by_id(tasks)
    summary["tasks"] = tasks
    summary["task_status_by_id"] = task_status_by_id
    summary["node_boxes"] = _build_node_boxes_from_version(version, task_status_by_id)
    return summary


def _is_authenticated() -> bool:
    return bool(session.get("is_authenticated"))


def _session_roles() -> set[str]:
    return {
        str(role).strip().lower()
        for role in (session.get("roles") or [])
        if str(role).strip()
    }


def _normalize_role_name(raw: str) -> str:
    role = str(raw or "").strip().lower()
    role = re.sub(r"[^a-z0-9_-]+", "_", role)
    role = role.strip("_")
    if role == "operation":
        return "operator"
    return role


def _normalize_role_privilege(raw: Any) -> str:
    text = re.sub(r"\s+", " ", str(raw or "")).strip()
    return text


def _permission_catalog() -> list[dict[str, str]]:
    return [
        {
            "key": key,
            "label": value["label"],
            "description": value["description"],
        }
        for key, value in PERMISSION_DEFINITIONS.items()
    ]


def _normalize_permission_name(raw: Any) -> str:
    value = str(raw or "").strip().lower()
    if not value:
        return ""
    if value in PERMISSION_DEFINITIONS:
        return value
    for key, data in PERMISSION_DEFINITIONS.items():
        if value == str(data.get("label", "")).strip().lower():
            return key
    value = re.sub(r"[^a-z0-9_]+", "_", value).strip("_")
    return value if value in PERMISSION_DEFINITIONS else ""


def _normalize_permissions(raw: Any) -> list[str]:
    values: list[str] = []
    if isinstance(raw, list):
        values = [_normalize_permission_name(item) for item in raw]
    elif isinstance(raw, str):
        for line in raw.splitlines():
            for piece in line.split(","):
                values.append(_normalize_permission_name(piece))
    seen: set[str] = set()
    normalized: list[str] = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def _infer_permissions_from_privileges(privileges: list[str]) -> list[str]:
    text = " ".join(privileges).lower()
    inferred: set[str] = set()

    if any(keyword in text for keyword in ["view", "read", "inspect", "pipeline"]):
        inferred.update({"builder_view", "pipeline_load"})
    if any(keyword in text for keyword in ["edit", "configure", "drag", "drop", "connect"]):
        inferred.update({"pipeline_edit", "pipeline_new", "pipeline_save", "pipeline_layout"})
    if "validate" in text:
        inferred.add("pipeline_validate")
    if any(keyword in text for keyword in ["generate dag", "export dag", "render dag"]):
        inferred.add("dag_generate")
    if any(keyword in text for keyword in ["run dag", "trigger dag", "start dag"]):
        inferred.add("dag_run")
    if "retry" in text:
        inferred.update({"dag_retry", "task_retry"})
    if "log" in text or "monitor" in text or "status" in text:
        inferred.add("logs_view")
    if "connection" in text:
        inferred.add("connections_view")
    if any(keyword in text for keyword in ["create connection", "edit connection", "delete connection", "manage connection"]):
        inferred.add("connections_manage")
    if "admin" in text:
        inferred.update({"admin_dashboard", "users_manage", "roles_manage"})

    return sorted(inferred)


def _normalize_role_privileges(raw: Any) -> list[str]:
    values: list[str] = []
    if isinstance(raw, list):
        values = [_normalize_role_privilege(item) for item in raw]
    elif isinstance(raw, str):
        for line in raw.splitlines():
            for piece in line.split(","):
                values.append(_normalize_role_privilege(piece))
    seen: set[str] = set()
    normalized: list[str] = []
    for item in values:
        if not item:
            continue
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(item)
    return normalized


def _load_users_store() -> dict[str, Any]:
    path = AppConfig.USERS_STORE_FILE
    if not path.exists():
        initial = {"roles": [], "users": []}
        path.write_text(json.dumps(initial, indent=2), encoding="utf-8")
        return initial
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        payload = {"roles": [], "users": []}
    if not isinstance(payload, dict):
        payload = {"roles": [], "users": []}
    payload["roles"] = payload.get("roles") if isinstance(payload.get("roles"), list) else []
    payload["users"] = payload.get("users") if isinstance(payload.get("users"), list) else []
    return payload


def _save_users_store(store: dict[str, Any]) -> None:
    AppConfig.USERS_STORE_FILE.write_text(json.dumps(store, indent=2), encoding="utf-8")


def _load_requests_store() -> dict[str, Any]:
    path = AppConfig.REQUESTS_STORE_FILE
    if not path.exists():
        initial = {"requests": []}
        path.write_text(json.dumps(initial, indent=2), encoding="utf-8")
        return initial
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        payload = {"requests": []}
    if not isinstance(payload, dict):
        payload = {"requests": []}
    payload["requests"] = payload.get("requests") if isinstance(payload.get("requests"), list) else []
    return payload


def _save_requests_store(store: dict[str, Any]) -> None:
    AppConfig.REQUESTS_STORE_FILE.write_text(json.dumps(store, indent=2), encoding="utf-8")


def _load_gcp_connections_store() -> dict[str, Any]:
    path = AppConfig.GCP_CONNECTIONS_STORE_FILE
    if not path.exists():
        initial = {"connections": []}
        path.write_text(json.dumps(initial, indent=2), encoding="utf-8")
        return initial
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        payload = {"connections": []}
    if not isinstance(payload, dict):
        payload = {"connections": []}
    payload["connections"] = payload.get("connections") if isinstance(payload.get("connections"), list) else []
    return payload


def _save_gcp_connections_store(store: dict[str, Any]) -> None:
    AppConfig.GCP_CONNECTIONS_STORE_FILE.write_text(json.dumps(store, indent=2), encoding="utf-8")


def _list_gcp_service_account_connections() -> list[dict[str, Any]]:
    store = _load_gcp_connections_store()
    rows: list[dict[str, Any]] = []
    for item in store.get("connections", []):
        if not isinstance(item, dict):
            continue
        connection_id = str(item.get("connection_id", "")).strip()
        if not connection_id:
            continue
        rows.append(
            {
                "connection_id": connection_id,
                "conn_type": "gcp_service_account",
                "normalized_type": "gcp_service_account",
                "description": str(item.get("description", "")).strip(),
                "host": "",
                "login": "",
                "port": None,
                "schema": "",
                "extra": None,
                "service_account_file": str(item.get("service_account_file", "")).strip(),
                "updated_at": str(item.get("updated_at", "")).strip(),
                "raw": item,
            }
        )
    rows.sort(key=lambda row: str(row.get("connection_id", "")).lower())
    return rows


def _find_gcp_connection(connection_id: str) -> dict[str, Any] | None:
    safe_id = str(connection_id or "").strip()
    if not safe_id:
        return None
    for item in _list_gcp_service_account_connections():
        if str(item.get("connection_id", "")).strip() == safe_id:
            return item
    return None


def _copy_gcp_service_account_to_airflow_containers(host_file: Path) -> str:
    return sync_host_file_to_containers_at_path(
        host_file=host_file,
        enabled=AppConfig.AIRFLOW_DOCKER_SYNC_ENABLED,
        containers=AppConfig.GCP_SERVICE_ACCOUNT_DOCKER_CONTAINERS,
        container_file_path=AppConfig.GCP_SERVICE_ACCOUNT_CONTAINER_FILE,
    )


def _resolve_gcp_connections_in_payload(raw_payload: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    payload = copy.deepcopy(raw_payload or {})
    errors: list[str] = []
    nodes = payload.get("nodes")
    if not isinstance(nodes, list):
        return payload, errors

    store = _load_gcp_connections_store()
    connection_map: dict[str, str] = {}
    for item in store.get("connections", []):
        if not isinstance(item, dict):
            continue
        connection_id = str(item.get("connection_id", "")).strip()
        service_account_file = str(item.get("service_account_file", "")).strip()
        if connection_id and service_account_file:
            connection_map[connection_id] = service_account_file

    for idx, node in enumerate(nodes):
        if not isinstance(node, dict):
            continue
        node_type = str(node.get("type", "")).strip().lower()
        if node_type not in {"gcs", "cloudrun", "dataform"}:
            continue
        config = node.get("config")
        if not isinstance(config, dict):
            continue

        connection_id = str(config.get("gcp_connection_id", "")).strip()
        if not connection_id:
            continue
        resolved = connection_map.get(connection_id)
        if not resolved:
            node_id = str(node.get("id", "")).strip() or f"index {idx}"
            errors.append(
                f"Node '{node_id}' ({node_type}) references unknown GCP connection '{connection_id}'."
            )
            continue
        config["service_account_file"] = resolved

    return payload, errors


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _azure_sso_configured() -> bool:
    return bool(
        AppConfig.AZURE_CLIENT_ID
        and AppConfig.AZURE_CLIENT_SECRET
        and AppConfig.AZURE_TENANT_ID
        and AppConfig.AZURE_REDIRECT_URI
    )


def _azure_authority_url() -> str:
    tenant = str(AppConfig.AZURE_TENANT_ID or "").strip()
    return f"https://login.microsoftonline.com/{tenant}"


def _azure_authorize_url(*, state: str, next_path: str) -> str:
    authorize_endpoint = f"{_azure_authority_url()}/oauth2/v2.0/authorize"
    params = {
        "client_id": AppConfig.AZURE_CLIENT_ID,
        "response_type": "code",
        "redirect_uri": AppConfig.AZURE_REDIRECT_URI,
        "response_mode": "query",
        "scope": "openid profile email User.Read",
        "state": state,
    }
    return f"{authorize_endpoint}?{urlencode(params)}"


def _decode_jwt_payload(token: str) -> dict[str, Any]:
    text = str(token or "").strip()
    if not text:
        return {}
    parts = text.split(".")
    if len(parts) < 2:
        return {}
    payload_b64 = parts[1]
    padding = "=" * ((4 - len(payload_b64) % 4) % 4)
    try:
        decoded = base64.urlsafe_b64decode(payload_b64 + padding).decode("utf-8")
        data = json.loads(decoded)
    except Exception:  # noqa: BLE001
        return {}
    return data if isinstance(data, dict) else {}


def _azure_fetch_profile(access_token: str) -> dict[str, Any]:
    token = str(access_token or "").strip()
    if not token:
        return {}
    try:
        resp = requests.get(
            "https://graph.microsoft.com/v1.0/me",
            headers={"Authorization": f"Bearer {token}"},
            timeout=15,
        )
    except RequestException:
        return {}
    if resp.status_code != 200:
        return {}
    try:
        data = resp.json()
    except ValueError:
        return {}
    return data if isinstance(data, dict) else {}


def _current_user_identity() -> dict[str, str]:
    username = str(session.get("username", "")).strip()
    if not username:
        return {"user_id": "__unknown__", "username": ""}

    if hmac.compare_digest(username.lower(), AppConfig.ADMIN_USERNAME.lower()):
        return {"user_id": "__env_admin__", "username": username}

    store = _load_users_store()
    users = [item for item in (store.get("users") or []) if isinstance(item, dict)]
    for item in users:
        item_username = str(item.get("username", "")).strip()
        if item_username and hmac.compare_digest(item_username.lower(), username.lower()):
            return {"user_id": str(item.get("id", "")).strip() or "__unknown__", "username": item_username}

    return {"user_id": "__unknown__", "username": username}


def _public_request_record(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(item.get("id", "")).strip(),
        "requester_user_id": str(item.get("requester_user_id", "")).strip(),
        "requester_username": str(item.get("requester_username", "")).strip(),
        "request_type": str(item.get("request_type", "")).strip(),
        "title": str(item.get("title", "")).strip(),
        "description": str(item.get("description", "")).strip(),
        "extra_data": item.get("extra_data") if isinstance(item.get("extra_data"), dict) else {},
        "status": str(item.get("status", REQUEST_STATUS_PENDING)).strip().lower(),
        "created_at": str(item.get("created_at", "")).strip(),
        "updated_at": str(item.get("updated_at", "")).strip(),
    }


def _normalize_role_names(raw_value: Any) -> list[str]:
    values: list[str] = []
    if isinstance(raw_value, list):
        values = [str(item or "").strip() for item in raw_value]
    elif isinstance(raw_value, str):
        values = [part.strip() for part in raw_value.split(",")]
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(value)
    return deduped


def _build_request_payload(raw: dict[str, Any], requester: dict[str, str]) -> tuple[dict[str, Any] | None, list[str]]:
    request_type = str(raw.get("request_type", "")).strip().lower()
    if request_type not in REQUEST_TYPES:
        return None, [f"request_type must be one of {sorted(REQUEST_TYPES)}."]

    errors: list[str] = []
    title = ""
    description = str(raw.get("description", "")).strip()
    extra_data: dict[str, Any] = {}

    if request_type == REQUEST_TYPE_ADD_MODULE:
        module_name = str(raw.get("module_name", "")).strip()
        if not module_name:
            errors.append("module_name is required.")
        if not description:
            errors.append("description is required for Add Module requests.")
        title = module_name
        extra_data["module_name"] = module_name
    elif request_type == REQUEST_TYPE_SIGNAL_BUG:
        bug_title = str(raw.get("bug_title", "")).strip()
        if not bug_title:
            errors.append("bug_title is required.")
        if not description:
            errors.append("description is required for bug reports.")
        title = bug_title
        extra_data["bug_title"] = bug_title
    elif request_type == REQUEST_TYPE_REQUEST_ROLE:
        role_names = _normalize_role_names(raw.get("role_names"))
        if not role_names:
            errors.append("role_names is required and must contain at least one role.")
        title = "Role request: " + ", ".join(role_names) if role_names else "Role request"
        extra_data["role_names"] = role_names
    elif request_type == REQUEST_TYPE_OTHER:
        request_title = str(raw.get("request_title", "")).strip()
        if not request_title:
            errors.append("request_title is required.")
        if not description:
            errors.append("description is required for Other Request.")
        title = request_title
        extra_data["request_title"] = request_title

    if description:
        description = re.sub(r"\s+", " ", description).strip()
    if title:
        title = re.sub(r"\s+", " ", title).strip()

    if not title:
        errors.append("title could not be derived from request payload.")
    if len(title) > 220:
        errors.append("title must be 220 characters or less.")
    if len(description) > 5000:
        errors.append("description must be 5000 characters or less.")

    if errors:
        return None, errors

    now = _utc_now_iso()
    record = {
        "id": "req_" + uuid4().hex[:12],
        "requester_user_id": str(requester.get("user_id", "")).strip() or "__unknown__",
        "requester_username": str(requester.get("username", "")).strip(),
        "request_type": request_type,
        "title": title,
        "description": description,
        "extra_data": extra_data,
        "status": REQUEST_STATUS_PENDING,
        "created_at": now,
        "updated_at": now,
    }
    return record, []


def _custom_role_items(store: dict[str, Any]) -> list[dict[str, Any]]:
    raw_roles = store.get("roles") if isinstance(store.get("roles"), list) else []
    indexed: dict[str, dict[str, Any]] = {}
    for item in raw_roles:
        if isinstance(item, dict):
            role_name = _normalize_role_name(item.get("name") or item.get("role_name") or item.get("id"))
            role_description = str(item.get("description", "")).strip()
            role_privileges = _normalize_role_privileges(item.get("privileges"))
            role_permissions = _normalize_permissions(item.get("permissions"))
        else:
            role_name = _normalize_role_name(item)
            role_description = ""
            role_privileges = []
            role_permissions = []
        if not role_name or role_name in PREDEFINED_ROLES:
            continue
        if role_name in indexed:
            continue
        if not role_permissions:
            role_permissions = _infer_permissions_from_privileges(role_privileges)
        if not role_permissions:
            role_permissions = ["builder_view", "pipeline_load"]
        indexed[role_name] = {
            "name": role_name,
            "description": role_description,
            "privileges": role_privileges,
            "permissions": role_permissions,
        }
    return [indexed[name] for name in sorted(indexed.keys())]


def _set_custom_role_items(store: dict[str, Any], custom_roles: list[dict[str, Any]]) -> None:
    normalized_roles: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in custom_roles:
        role_name = _normalize_role_name(item.get("name"))
        if not role_name or role_name in PREDEFINED_ROLES or role_name in seen:
            continue
        seen.add(role_name)
        normalized_roles.append(
            {
                "name": role_name,
                "description": str(item.get("description", "")).strip(),
                "privileges": _normalize_role_privileges(item.get("privileges")),
                "permissions": _normalize_permissions(item.get("permissions")) or _infer_permissions_from_privileges(
                    _normalize_role_privileges(item.get("privileges"))
                ),
            }
        )
    normalized_roles.sort(key=lambda row: row["name"])
    store["roles"] = normalized_roles


def _role_catalog(store: dict[str, Any]) -> list[dict[str, Any]]:
    catalog: list[dict[str, Any]] = []
    for role_name, details in PREDEFINED_ROLE_DEFINITIONS.items():
        permissions = _normalize_permissions(details.get("permissions"))
        catalog.append(
            {
                "name": role_name,
                "display_name": role_name.replace("_", " ").title(),
                "description": str(details.get("description", "")).strip(),
                "privileges": _normalize_role_privileges(details.get("privileges")),
                "permissions": permissions,
                "permission_labels": [
                    PERMISSION_DEFINITIONS[key]["label"]
                    for key in permissions
                    if key in PERMISSION_DEFINITIONS
                ],
                "is_predefined": True,
                "editable": False,
                "deletable": False,
            }
        )
    for item in _custom_role_items(store):
        permissions = _normalize_permissions(item.get("permissions"))
        catalog.append(
            {
                "name": item["name"],
                "display_name": item["name"].replace("_", " ").title(),
                "description": item["description"] or "Custom role created by an admin.",
                "privileges": item["privileges"],
                "permissions": permissions,
                "permission_labels": [
                    PERMISSION_DEFINITIONS[key]["label"]
                    for key in permissions
                    if key in PERMISSION_DEFINITIONS
                ],
                "is_predefined": False,
                "editable": True,
                "deletable": True,
            }
        )
    return catalog


def _all_role_names(store: dict[str, Any]) -> list[str]:
    return [item["name"] for item in _role_catalog(store)]


def _is_admin_session() -> bool:
    roles = list(_session_roles())
    if "admin" in roles:
        return True
    return hmac.compare_digest(str(session.get("username", "")), AppConfig.ADMIN_USERNAME)


def _permissions_for_role(role_name: str, store: dict[str, Any] | None = None) -> list[str]:
    normalized = _normalize_role_name(role_name)
    if not normalized:
        return []
    if normalized in PREDEFINED_ROLE_DEFINITIONS:
        return _normalize_permissions(PREDEFINED_ROLE_DEFINITIONS[normalized].get("permissions"))
    use_store = store or _load_users_store()
    for item in _custom_role_items(use_store):
        if item["name"] == normalized:
            return _normalize_permissions(item.get("permissions"))
    return []


def _session_permissions() -> set[str]:
    if _is_admin_session():
        return set(PERMISSION_DEFINITIONS.keys())
    store = _load_users_store()
    permissions: set[str] = set()
    for role in _session_roles():
        permissions.update(_permissions_for_role(role, store))
    return permissions


def _has_permission(permission_key: str) -> bool:
    if _is_admin_session():
        return True
    return _normalize_permission_name(permission_key) in _session_permissions()


def _require_permission(permission_key: str, message: str | None = None):
    normalized = _normalize_permission_name(permission_key)
    if not normalized:
        return _json_error("Invalid permission check.", 500)
    if _has_permission(normalized):
        return None
    error_msg = message or f"Permission '{normalized}' is required."
    if request.path.startswith("/api/"):
        return _json_error(error_msg, 403)
    return redirect(url_for("index"))


def _require_admin_access():
    if _has_permission("admin_dashboard"):
        return None
    if request.path.startswith("/api/"):
        return _json_error("Admin privileges required.", 403)
    return redirect(url_for("index"))


def _public_user_record(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(item.get("id", "")),
        "username": str(item.get("username", "")),
        "roles": [str(role) for role in (item.get("roles") or []) if str(role).strip()],
        "is_system": bool(item.get("is_system", False)),
    }


def _is_safe_next_path(target: str) -> bool:
    value = str(target or "").strip()
    return bool(value and value.startswith("/") and not value.startswith("//"))


@app.before_request
def _require_authentication():
    # Keep login + static files public; everything else requires authentication.
    endpoint = request.endpoint or ""
    if request.method == "OPTIONS":
        return None
    if not endpoint:
        return None
    if endpoint in {"login", "logout", "login_azure", "auth_callback", "static"} or endpoint.startswith("static"):
        return None
    if _is_authenticated():
        return None

    if request.path.startswith("/api/"):
        return _json_error("Authentication required.", 401)

    next_path = request.full_path if request.query_string else request.path
    return redirect(url_for("login", next=next_path))


def _normalize_connection_type(raw: str) -> str:
    value = str(raw or "").strip().lower()
    if value in {"http"}:
        return "http"
    if value in {"wasb", "azure_blob_storage", "azure_blob"}:
        return "wasb"
    if value in {"gcp_service_account", "google_service_account", "gcp", "google"}:
        return "gcp_service_account"
    return value


def _collect_admin_dashboard_metrics() -> dict[str, Any]:
    users_store = _load_users_store()
    env_admin_username = AppConfig.ADMIN_USERNAME
    stored_users = users_store.get("users") or []
    total_users = len(stored_users)
    if not any(str(user.get("username", "")) == env_admin_username for user in stored_users):
        total_users += 1

    saved_pipeline_files = list(AppConfig.SAVED_PIPELINES_DIR.glob("*.json"))
    generated_dag_files = list(AppConfig.GENERATED_DAGS_DIR.glob("*.py"))
    template_files = list(AppConfig.DAG_TEMPLATES_DIR.glob("*.j2"))

    all_saved_pipelines = sorted(saved_pipeline_files, key=lambda p: p.stat().st_mtime, reverse=True)
    all_generated_dags = sorted(generated_dag_files, key=lambda p: p.stat().st_mtime, reverse=True)

    airflow_platform: dict[str, Any] = {}
    airflow_platform_error = ""
    try:
        airflow_platform = get_airflow_platform_snapshot(_airflow_api_config())
    except Exception as exc:  # noqa: BLE001
        airflow_platform_error = str(exc)

    return {
        "total_users": total_users,
        "total_saved_pipelines": len(saved_pipeline_files),
        "total_generated_dags": len(generated_dag_files),
        "total_module_templates": len(template_files),
        "total_supported_modules": len(list_modules()),
        "saved_pipelines": [
            {
                "name": path.name,
                "updated_at": datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
            }
            for path in all_saved_pipelines
        ],
        "generated_dags": [
            {
                "name": path.name,
                "updated_at": datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
            }
            for path in all_generated_dags
        ],
        "airflow_platform": airflow_platform,
        "airflow_platform_error": airflow_platform_error,
    }


def _resolve_admin_file_target(base_dir: Path, filename: str, *, expected_extension: str) -> Path | None:
    safe_name = _safe_filename(filename, "")
    if not safe_name or safe_name != filename:
        return None
    if expected_extension and not safe_name.endswith(expected_extension):
        return None
    base_resolved = base_dir.resolve()
    target = (base_dir / safe_name).resolve()
    if base_resolved != target.parent:
        return None
    return target


def _parse_extra_payload(raw_extra: Any) -> tuple[dict[str, Any] | None, str | None]:
    if raw_extra in (None, "", {}):
        return None, None
    if isinstance(raw_extra, dict):
        return raw_extra, None
    if isinstance(raw_extra, str):
        text = raw_extra.strip()
        if not text:
            return None, None
        try:
            loaded = json.loads(text)
        except json.JSONDecodeError:
            return None, "Extra JSON must be valid JSON."
        if not isinstance(loaded, dict):
            return None, "Extra JSON must be an object."
        return loaded, None
    return None, "Extra JSON must be an object or valid JSON text."


def _build_connection_payload(body: dict[str, Any], *, for_update: bool = False) -> tuple[dict[str, Any] | None, list[str]]:
    errors: list[str] = []

    connection_id = str(body.get("connection_id", "")).strip()
    if not for_update and not connection_id:
        errors.append("Connection ID is required.")

    conn_type = _normalize_connection_type(body.get("conn_type") or body.get("connection_type"))
    if conn_type not in {"http", "wasb"}:
        errors.append("Connection Type must be HTTP or Azure Blob Storage.")

    description = str(body.get("description", "")).strip()
    host = str(body.get("host", "")).strip()
    login = str(body.get("login", "")).strip()
    password = str(body.get("password", "")).strip()
    schema = str(body.get("schema", "")).strip()
    account_url = str(body.get("account_url", "")).strip()

    port_value: int | None = None
    raw_port = body.get("port")
    if raw_port not in (None, ""):
        try:
            port_value = int(raw_port)
        except (TypeError, ValueError):
            errors.append("Port must be a valid integer.")
        else:
            if port_value <= 0:
                errors.append("Port must be greater than 0.")

    extra_obj, extra_error = _parse_extra_payload(body.get("extra"))
    if extra_error:
        errors.append(extra_error)

    if conn_type == "http":
        if not host:
            errors.append("Host is required for HTTP connections.")
    elif conn_type == "wasb":
        if not account_url and not host:
            errors.append("Account URL is required for Azure Blob Storage connections.")
        if account_url:
            host = account_url
        if extra_obj is None:
            extra_obj = {}
        if account_url:
            extra_obj["account_url"] = account_url

    if errors:
        return None, errors

    airflow_payload: dict[str, Any] = {
        "conn_type": conn_type,
        "description": description,
        "host": host,
        "login": login,
        "password": password,
        "schema": schema,
    }
    if not for_update:
        airflow_payload["connection_id"] = connection_id

    if port_value is not None:
        airflow_payload["port"] = port_value
    if extra_obj:
        airflow_payload["extra"] = json.dumps(extra_obj)

    return airflow_payload, []


def _upsert_gcp_service_account_connection(
    *,
    connection_id: str,
    description: str,
    uploaded_file: Any | None,
    for_update: bool,
) -> tuple[dict[str, Any] | None, list[str]]:
    errors: list[str] = []
    safe_connection_id = str(connection_id or "").strip()
    if not safe_connection_id:
        errors.append("Connection ID is required.")
    elif not re.match(r"^[A-Za-z0-9_.-]+$", safe_connection_id):
        errors.append("Connection ID may only contain letters, numbers, dot, underscore, or dash.")

    store = _load_gcp_connections_store()
    rows = [item for item in store.get("connections", []) if isinstance(item, dict)]
    existing_idx = -1
    existing_item: dict[str, Any] | None = None
    for idx, item in enumerate(rows):
        if str((item or {}).get("connection_id", "")).strip() == safe_connection_id:
            existing_idx = idx
            existing_item = item if isinstance(item, dict) else {}
            break

    if for_update and existing_idx < 0:
        errors.append("GCP connection not found.")
    if not for_update and existing_idx >= 0:
        errors.append("A GCP connection with this ID already exists.")

    has_file_upload = bool(uploaded_file and str(getattr(uploaded_file, "filename", "")).strip())
    if not has_file_upload and (not for_update or not existing_item):
        errors.append("Service account file is required for this connection.")

    if errors:
        return None, errors

    container_target_path = str((existing_item or {}).get("service_account_file", "")).strip()
    if has_file_upload:
        host_target = AppConfig.GCP_SERVICE_ACCOUNT_HOST_FILE
        host_target.parent.mkdir(parents=True, exist_ok=True)
        try:
            uploaded_file.save(str(host_target))
        except Exception as exc:  # noqa: BLE001
            return None, [f"Unable to save service account file: {exc}"]

        try:
            container_target_path = _copy_gcp_service_account_to_airflow_containers(host_target)
        except ContainerFileSyncError as exc:
            return None, [f"Unable to sync service account file to Airflow containers: {exc}"]

    now_iso = _utc_now_iso()
    connection_record = {
        "connection_id": safe_connection_id,
        "conn_type": "gcp_service_account",
        "description": str(description or "").strip(),
        "service_account_file": container_target_path or AppConfig.GCP_SERVICE_ACCOUNT_CONTAINER_FILE,
        "host_file_path": str(AppConfig.GCP_SERVICE_ACCOUNT_HOST_FILE),
        "updated_at": now_iso,
    }
    if existing_item and isinstance(existing_item, dict):
        connection_record["created_at"] = str(existing_item.get("created_at", "")).strip() or now_iso
    else:
        connection_record["created_at"] = now_iso

    if existing_idx >= 0:
        rows[existing_idx] = connection_record
    else:
        rows.append(connection_record)
    store["connections"] = rows
    _save_gcp_connections_store(store)

    return {
        "connection_id": connection_record["connection_id"],
        "conn_type": "gcp_service_account",
        "normalized_type": "gcp_service_account",
        "description": connection_record["description"],
        "service_account_file": connection_record["service_account_file"],
        "host": "",
        "login": "",
        "port": None,
        "schema": "",
        "extra": None,
        "raw": connection_record,
    }, []


@app.route("/login", methods=["GET", "POST"])
def login():
    if _is_authenticated():
        return redirect(url_for("index"))

    error = ""
    next_path = str(request.args.get("next", "")).strip()

    if request.method == "POST":
        username = str(request.form.get("username", "")).strip()
        password = str(request.form.get("password", ""))
        next_from_form = str(request.form.get("next", "")).strip()
        if next_from_form:
            next_path = next_from_form

        authenticated_roles: list[str] = []
        valid_user = hmac.compare_digest(username, AppConfig.ADMIN_USERNAME)
        valid_pass = hmac.compare_digest(password, AppConfig.ADMIN_PASSWORD)
        if valid_user and valid_pass:
            authenticated_roles = ["admin"]
        else:
            store = _load_users_store()
            for item in store.get("users") or []:
                item_username = str(item.get("username", "")).strip()
                pwd_hash = str(item.get("password_hash", "")).strip()
                if not item_username or not pwd_hash:
                    continue
                if not hmac.compare_digest(item_username, username):
                    continue
                # Inactive accounts must not be able to authenticate.
                if item.get("is_active", True) is False:
                    break
                if check_password_hash(pwd_hash, password):
                    authenticated_roles = [
                        str(role).strip().lower()
                        for role in (item.get("roles") or [])
                        if str(role).strip()
                    ] or ["viewer"]
                    break

        if authenticated_roles:
            session.clear()
            session["is_authenticated"] = True
            session["username"] = username
            session["roles"] = authenticated_roles
            if _is_safe_next_path(next_path):
                return redirect(next_path)
            return redirect(url_for("index"))
        error = "Invalid username or password."

    return render_template(
        "login.html",
        error=error,
        next_path=next_path,
        azure_sso_enabled=_azure_sso_configured(),
    )


@app.get("/login/azure")
def login_azure():
    if _is_authenticated():
        return redirect(url_for("index"))
    if not _azure_sso_configured():
        return _json_error(
            "Azure SSO is not configured. Set AZURE_CLIENT_ID, AZURE_TENANT_ID, AZURE_CLIENT_SECRET, AZURE_REDIRECT_URI.",
            500,
        )

    next_path = str(request.args.get("next", "")).strip()
    if not _is_safe_next_path(next_path):
        next_path = ""

    state = secrets.token_urlsafe(32)
    session["azure_oauth_state"] = state
    session["azure_oauth_next"] = next_path

    return redirect(_azure_authorize_url(state=state, next_path=next_path))


@app.get("/auth/callback")
def auth_callback():
    if not _azure_sso_configured():
        return _json_error(
            "Azure SSO is not configured. Set AZURE_CLIENT_ID, AZURE_TENANT_ID, AZURE_CLIENT_SECRET, AZURE_REDIRECT_URI.",
            500,
        )

    oauth_error = str(request.args.get("error", "")).strip()
    if oauth_error:
        oauth_error_desc = str(request.args.get("error_description", "")).strip()
        return render_template(
            "login.html",
            error=f"Azure sign-in failed: {oauth_error_desc or oauth_error}",
            next_path="",
            azure_sso_enabled=_azure_sso_configured(),
        )

    state = str(request.args.get("state", "")).strip()
    expected_state = str(session.pop("azure_oauth_state", "")).strip()
    if not state or not expected_state or not hmac.compare_digest(state, expected_state):
        return render_template(
            "login.html",
            error="Azure sign-in failed: invalid authentication state.",
            next_path="",
            azure_sso_enabled=_azure_sso_configured(),
        )

    code = str(request.args.get("code", "")).strip()
    if not code:
        return render_template(
            "login.html",
            error="Azure sign-in failed: missing authorization code.",
            next_path="",
            azure_sso_enabled=_azure_sso_configured(),
        )

    token_endpoint = f"{_azure_authority_url()}/oauth2/v2.0/token"
    token_payload = {
        "client_id": AppConfig.AZURE_CLIENT_ID,
        "client_secret": AppConfig.AZURE_CLIENT_SECRET,
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": AppConfig.AZURE_REDIRECT_URI,
        "scope": "openid profile email User.Read",
    }

    try:
        token_resp = requests.post(token_endpoint, data=token_payload, timeout=20)
    except RequestException as exc:
        return render_template(
            "login.html",
            error=f"Azure sign-in failed: token request error ({exc}).",
            next_path="",
            azure_sso_enabled=_azure_sso_configured(),
        )

    if token_resp.status_code != 200:
        details = ""
        try:
            payload = token_resp.json()
            details = str(payload.get("error_description") or payload.get("error") or "").strip()
        except Exception:  # noqa: BLE001
            details = token_resp.text[:300]
        return render_template(
            "login.html",
            error=f"Azure sign-in failed: {details or 'unable to exchange authorization code.'}",
            next_path="",
            azure_sso_enabled=_azure_sso_configured(),
        )

    token_json = token_resp.json() if token_resp.text else {}
    id_token = str((token_json or {}).get("id_token") or "").strip()
    access_token = str((token_json or {}).get("access_token") or "").strip()

    claims = _decode_jwt_payload(id_token)
    graph_profile = _azure_fetch_profile(access_token)
    merged = {}
    merged.update(claims if isinstance(claims, dict) else {})
    merged.update(graph_profile if isinstance(graph_profile, dict) else {})

    username = str(
        merged.get("preferred_username")
        or merged.get("userPrincipalName")
        or merged.get("mail")
        or merged.get("email")
        or ""
    ).strip()
    if not username:
        return render_template(
            "login.html",
            error="Azure sign-in failed: unable to resolve user identity.",
            next_path="",
            azure_sso_enabled=_azure_sso_configured(),
        )

    display_name = str(merged.get("displayName") or merged.get("name") or username).strip()
    email = str(merged.get("mail") or merged.get("email") or username).strip()

    store = _load_users_store()
    users = [item for item in (store.get("users") or []) if isinstance(item, dict)]
    matched_user: dict[str, Any] | None = None
    for item in users:
        item_username = str(item.get("username", "")).strip()
        if item_username and hmac.compare_digest(item_username.lower(), username.lower()):
            matched_user = item
            break

    if matched_user is None:
        matched_user = {
            "id": f"usr_{uuid4().hex[:12]}",
            "username": username,
            "roles": ["viewer"],
            "is_system": False,
            "is_active": True,
            "auth_provider": "azure_sso",
            "display_name": display_name,
            "email": email,
            "created_at": _utc_now_iso(),
            "updated_at": _utc_now_iso(),
        }
        users.append(matched_user)
        store["users"] = users
        _save_users_store(store)
    else:
        if matched_user.get("is_active", True) is False:
            return render_template(
                "login.html",
                error="Your account is inactive. Please contact an administrator.",
                next_path="",
                azure_sso_enabled=_azure_sso_configured(),
            )
        # Keep identity metadata fresh for SSO accounts.
        matched_user["auth_provider"] = "azure_sso"
        if display_name:
            matched_user["display_name"] = display_name
        if email:
            matched_user["email"] = email
        matched_user["updated_at"] = _utc_now_iso()
        store["users"] = users
        _save_users_store(store)

    authenticated_roles = [
        str(role).strip().lower()
        for role in (matched_user.get("roles") or [])
        if str(role).strip()
    ] or ["viewer"]

    next_path = str(session.pop("azure_oauth_next", "")).strip()
    session.clear()
    session["is_authenticated"] = True
    session["username"] = str(matched_user.get("username", "")).strip() or username
    session["roles"] = authenticated_roles
    session["auth_provider"] = "azure_sso"

    if _is_safe_next_path(next_path):
        return redirect(next_path)
    return redirect(url_for("index"))


@app.route("/logout", methods=["GET", "POST"])
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.context_processor
def inject_auth_context():
    roles = [str(role).strip().lower() for role in (session.get("roles") or []) if str(role).strip()]
    permissions = sorted(_session_permissions())
    return {
        "current_username": session.get("username", ""),
        "current_roles": roles,
        "current_permissions": permissions,
        "is_admin": _is_admin_session(),
        "is_viewer_only": (not _is_admin_session()) and _session_roles() == {"viewer"},
        "can_write_workspace": any(
            perm in permissions
            for perm in (
                "pipeline_edit",
                "pipeline_new",
                "pipeline_save",
                "dag_generate",
                "dag_run",
                "connections_manage",
            )
        ),
    }


@app.get("/")
def index():
    return render_template("index.html")


@app.get("/builder")
def builder():
    forbidden = _require_permission("builder_view", "You do not have access to the builder.")
    if forbidden is not None:
        return forbidden
    return render_template("pipeline_builder.html")


@app.get("/requests")
def requests_page():
    if not _is_authenticated():
        return redirect(url_for("login"))
    return render_template("requests.html")


@app.get("/airflow-connections")
def airflow_connections_page():
    forbidden = _require_permission("connections_view", "You do not have access to Airflow connections.")
    if forbidden is not None:
        return forbidden
    return render_template("airflow_connections.html")


@app.get("/admin-dashboard")
def admin_dashboard():
    forbidden = _require_admin_access()
    if forbidden is not None:
        return forbidden
    metrics = _collect_admin_dashboard_metrics()
    return render_template(
        "admin_dashboard.html",
        metrics=metrics,
        admin_username=session.get("username", AppConfig.ADMIN_USERNAME),
    )


@app.get("/admin/users/new")
def admin_new_user_page():
    forbidden = _require_permission("users_manage", "You do not have permission to manage users.")
    if forbidden is not None:
        return forbidden
    store = _load_users_store()
    return render_template(
        "admin_user_form.html",
        mode="create",
        user={},
        all_roles=_all_role_names(store),
        role_catalog=_role_catalog(store),
        admin_username=session.get("username", AppConfig.ADMIN_USERNAME),
    )


@app.get("/admin/users/<user_id>/edit")
def admin_edit_user_page(user_id: str):
    forbidden = _require_permission("users_manage", "You do not have permission to manage users.")
    if forbidden is not None:
        return forbidden
    safe_user_id = str(user_id or "").strip()
    if not safe_user_id:
        return redirect(url_for("admin_dashboard", msg="User not found"))

    store = _load_users_store()
    users = [item for item in (store.get("users") or []) if isinstance(item, dict)]
    user = next((item for item in users if str(item.get("id", "")) == safe_user_id), None)
    if not user:
        return redirect(url_for("admin_dashboard", msg="User not found"))

    return render_template(
        "admin_user_form.html",
        mode="edit",
        user=_public_user_record(user),
        all_roles=_all_role_names(store),
        role_catalog=_role_catalog(store),
        admin_username=session.get("username", AppConfig.ADMIN_USERNAME),
    )


@app.get("/admin/roles")
def admin_roles_page():
    forbidden = _require_permission("roles_manage", "You do not have permission to manage roles.")
    if forbidden is not None:
        return forbidden
    return render_template(
        "admin_roles.html",
        admin_username=session.get("username", AppConfig.ADMIN_USERNAME),
    )


@app.get("/admin/roles/new")
def admin_new_role_page():
    forbidden = _require_permission("roles_manage", "You do not have permission to manage roles.")
    if forbidden is not None:
        return forbidden
    return render_template(
        "admin_role_form.html",
        mode="create",
        role={},
        role_catalog=_role_catalog(_load_users_store()),
        permission_catalog=_permission_catalog(),
        role_name_readonly=False,
        admin_username=session.get("username", AppConfig.ADMIN_USERNAME),
    )


@app.get("/admin/roles/<role_name>/edit")
def admin_edit_role_page(role_name: str):
    forbidden = _require_permission("roles_manage", "You do not have permission to manage roles.")
    if forbidden is not None:
        return forbidden
    safe_role_name = _normalize_role_name(role_name)
    if not safe_role_name:
        return redirect(url_for("admin_roles_page", msg="Role not found"))

    store = _load_users_store()
    custom_roles = _custom_role_items(store)
    role = next((item for item in custom_roles if item["name"] == safe_role_name), None)
    if not role:
        return redirect(url_for("admin_roles_page", msg="Role not found or not editable"))

    return render_template(
        "admin_role_form.html",
        mode="edit",
        role=role,
        role_catalog=_role_catalog(store),
        permission_catalog=_permission_catalog(),
        role_name_readonly=False,
        admin_username=session.get("username", AppConfig.ADMIN_USERNAME),
    )


def _validate_roles(selected_roles: Any, all_roles: list[str]) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    if not isinstance(selected_roles, list) or not selected_roles:
        return [], ["At least one role must be assigned."]
    normalized = []
    for role in selected_roles:
        role_name = _normalize_role_name(str(role))
        if not role_name:
            continue
        if role_name not in all_roles:
            errors.append(f"Unknown role: {role_name}")
            continue
        if role_name not in normalized:
            normalized.append(role_name)
    if not normalized:
        errors.append("At least one valid role must be assigned.")
    return normalized, errors


def _public_users_list(store: dict[str, Any]) -> list[dict[str, Any]]:
    users = [_public_user_record(item) for item in (store.get("users") or [])]
    admin_exists = any(str(item.get("username", "")) == AppConfig.ADMIN_USERNAME for item in users)
    if not admin_exists:
        users.insert(
            0,
            {
                "id": "__env_admin__",
                "username": AppConfig.ADMIN_USERNAME,
                "roles": ["admin"],
                "is_system": True,
            },
        )
    users.sort(key=lambda item: str(item.get("username", "")).lower())
    return users


@app.get("/api/admin/users")
def api_admin_list_users():
    forbidden = _require_permission("users_manage", "You do not have permission to manage users.")
    if forbidden is not None:
        return forbidden

    store = _load_users_store()
    return jsonify(
        {
            "success": True,
            "users": _public_users_list(store),
            "predefined_roles": PREDEFINED_ROLES,
            "custom_roles": [item["name"] for item in _custom_role_items(store)],
            "all_roles": _all_role_names(store),
            "role_catalog": _role_catalog(store),
            "permission_catalog": _permission_catalog(),
        }
    )


@app.get("/api/admin/roles")
def api_admin_list_roles():
    forbidden = _require_permission("roles_manage", "You do not have permission to manage roles.")
    if forbidden is not None:
        return forbidden

    store = _load_users_store()
    return jsonify(
        {
            "success": True,
            "predefined_roles": PREDEFINED_ROLES,
            "custom_roles": _custom_role_items(store),
            "roles": _role_catalog(store),
            "permission_catalog": _permission_catalog(),
        }
    )


@app.post("/api/admin/roles")
def api_admin_create_role():
    forbidden = _require_permission("roles_manage", "You do not have permission to manage roles.")
    if forbidden is not None:
        return forbidden

    payload = request.get_json(silent=True) or {}
    role_name = _normalize_role_name(payload.get("role_name", ""))
    if not role_name:
        return _json_error("role_name is required.", 400)
    if role_name in PREDEFINED_ROLES:
        return _json_error("This is already a predefined role.", 400)

    store = _load_users_store()
    existing = {item["name"] for item in _custom_role_items(store)}
    if role_name in existing:
        return _json_error("Role already exists.", 409)

    description = str(payload.get("description", "")).strip()
    privileges = _normalize_role_privileges(payload.get("privileges") or payload.get("privileges_text"))
    permissions = _normalize_permissions(payload.get("permissions"))
    if not permissions:
        permissions = _infer_permissions_from_privileges(privileges)
    if not permissions:
        permissions = ["builder_view", "pipeline_load"]
    if not privileges and permissions:
        privileges = [PERMISSION_DEFINITIONS[key]["label"] for key in permissions if key in PERMISSION_DEFINITIONS]

    custom_roles = _custom_role_items(store)
    custom_roles.append(
        {
            "name": role_name,
            "description": description,
            "privileges": privileges,
            "permissions": permissions,
        }
    )
    _set_custom_role_items(store, custom_roles)
    _save_users_store(store)

    return jsonify(
        {
            "success": True,
            "role": {
                "name": role_name,
                "description": description or "Custom role created by an admin.",
                "privileges": privileges,
                "permissions": permissions,
                "permission_labels": [
                    PERMISSION_DEFINITIONS[key]["label"]
                    for key in permissions
                    if key in PERMISSION_DEFINITIONS
                ],
                "is_predefined": False,
                "editable": True,
                "deletable": True,
            },
            "all_roles": _all_role_names(store),
            "roles": _role_catalog(store),
        }
    )


@app.put("/api/admin/roles/<role_name>")
def api_admin_update_role(role_name: str):
    forbidden = _require_permission("roles_manage", "You do not have permission to manage roles.")
    if forbidden is not None:
        return forbidden

    safe_role_name = _normalize_role_name(role_name)
    if not safe_role_name:
        return _json_error("role_name is required.", 400)
    if safe_role_name in PREDEFINED_ROLES:
        return _json_error("Predefined roles cannot be edited.", 400)

    payload = request.get_json(silent=True) or {}
    new_role_name = _normalize_role_name(payload.get("role_name", safe_role_name))
    if not new_role_name:
        return _json_error("role_name is required.", 400)
    if new_role_name in PREDEFINED_ROLES:
        return _json_error("Role name conflicts with a predefined role.", 400)

    store = _load_users_store()
    custom_roles = _custom_role_items(store)
    target = next((item for item in custom_roles if item["name"] == safe_role_name), None)
    if not target:
        return _json_error("Role not found.", 404)

    if new_role_name != safe_role_name and any(item["name"] == new_role_name for item in custom_roles):
        return _json_error("Role already exists.", 409)

    updated_role = {
        "name": new_role_name,
        "description": str(payload.get("description", target.get("description", ""))).strip(),
        "privileges": _normalize_role_privileges(payload.get("privileges") or payload.get("privileges_text")),
        "permissions": _normalize_permissions(payload.get("permissions")),
    }
    if not updated_role["privileges"]:
        updated_role["privileges"] = _normalize_role_privileges(target.get("privileges"))
    if not updated_role["permissions"]:
        updated_role["permissions"] = _normalize_permissions(target.get("permissions"))
    if not updated_role["permissions"]:
        updated_role["permissions"] = _infer_permissions_from_privileges(updated_role["privileges"])
    if not updated_role["permissions"]:
        updated_role["permissions"] = ["builder_view", "pipeline_load"]
    if not updated_role["privileges"] and updated_role["permissions"]:
        updated_role["privileges"] = [
            PERMISSION_DEFINITIONS[key]["label"]
            for key in updated_role["permissions"]
            if key in PERMISSION_DEFINITIONS
        ]

    for idx, item in enumerate(custom_roles):
        if item["name"] == safe_role_name:
            custom_roles[idx] = updated_role
            break
    _set_custom_role_items(store, custom_roles)

    renamed_users_count = 0
    if new_role_name != safe_role_name:
        users = [item for item in (store.get("users") or []) if isinstance(item, dict)]
        for user in users:
            roles = [str(role).strip().lower() for role in (user.get("roles") or []) if str(role).strip()]
            if safe_role_name not in roles:
                continue
            renamed_users_count += 1
            remapped = [new_role_name if role == safe_role_name else role for role in roles]
            deduped: list[str] = []
            for role in remapped:
                if role not in deduped:
                    deduped.append(role)
            user["roles"] = deduped
        store["users"] = users

    _save_users_store(store)
    return jsonify(
        {
            "success": True,
            "role": {
                "name": updated_role["name"],
                "description": updated_role["description"] or "Custom role created by an admin.",
                "privileges": updated_role["privileges"],
                "permissions": updated_role["permissions"],
                "permission_labels": [
                    PERMISSION_DEFINITIONS[key]["label"]
                    for key in updated_role["permissions"]
                    if key in PERMISSION_DEFINITIONS
                ],
                "is_predefined": False,
                "editable": True,
                "deletable": True,
            },
            "roles": _role_catalog(store),
            "renamed_users_count": renamed_users_count,
        }
    )


@app.delete("/api/admin/roles/<role_name>")
def api_admin_delete_role(role_name: str):
    forbidden = _require_permission("roles_manage", "You do not have permission to manage roles.")
    if forbidden is not None:
        return forbidden

    safe_role_name = _normalize_role_name(role_name)
    if not safe_role_name:
        return _json_error("role_name is required.", 400)
    if safe_role_name in PREDEFINED_ROLES:
        return _json_error("Predefined roles cannot be deleted.", 400)

    store = _load_users_store()
    custom_roles = _custom_role_items(store)
    kept_roles = [item for item in custom_roles if item["name"] != safe_role_name]
    if len(kept_roles) == len(custom_roles):
        return _json_error("Role not found.", 404)

    _set_custom_role_items(store, kept_roles)

    affected_users = 0
    users = [item for item in (store.get("users") or []) if isinstance(item, dict)]
    for user in users:
        roles = [str(role).strip().lower() for role in (user.get("roles") or []) if str(role).strip()]
        if safe_role_name not in roles:
            continue
        affected_users += 1
        remaining = [role for role in roles if role != safe_role_name]
        if not remaining:
            remaining = ["viewer"]
        user["roles"] = remaining
    store["users"] = users

    _save_users_store(store)
    return jsonify(
        {
            "success": True,
            "deleted_role": safe_role_name,
            "affected_users": affected_users,
            "roles": _role_catalog(store),
            "users": _public_users_list(store),
        }
    )


@app.post("/api/admin/users")
def api_admin_create_user():
    forbidden = _require_permission("users_manage", "You do not have permission to manage users.")
    if forbidden is not None:
        return forbidden

    payload = request.get_json(silent=True) or {}
    username = str(payload.get("username", "")).strip()
    password = str(payload.get("password", ""))
    roles_raw = payload.get("roles")

    if not username:
        return _json_error("username is required.", 400)
    if not re.match(r"^[A-Za-z0-9_.-]{3,64}$", username):
        return _json_error("username must be 3-64 chars and contain only letters, digits, . _ -", 400)
    if len(password) < 6:
        return _json_error("password must be at least 6 characters.", 400)

    store = _load_users_store()
    all_roles = _all_role_names(store)
    roles, role_errors = _validate_roles(roles_raw, all_roles)
    if role_errors:
        return _json_error("Role validation failed.", 400, errors=role_errors)

    usernames = {
        str(item.get("username", "")).strip().lower()
        for item in (store.get("users") or [])
        if str(item.get("username", "")).strip()
    }
    usernames.add(AppConfig.ADMIN_USERNAME.lower())
    if username.lower() in usernames:
        return _json_error("username already exists.", 409)

    user_record = {
        "id": "usr_" + uuid4().hex[:12],
        "username": username,
        "password_hash": generate_password_hash(password),
        "roles": roles,
        "is_system": False,
    }
    users = [item for item in (store.get("users") or []) if isinstance(item, dict)]
    users.append(user_record)
    store["users"] = users
    _save_users_store(store)

    metrics = _collect_admin_dashboard_metrics()
    return jsonify(
        {
            "success": True,
            "user": _public_user_record(user_record),
            "users": _public_users_list(store),
            "all_roles": _all_role_names(store),
            "role_catalog": _role_catalog(store),
            "metrics": {"total_users": metrics["total_users"]},
        }
    )


@app.put("/api/admin/users/<user_id>")
def api_admin_update_user(user_id: str):
    forbidden = _require_permission("users_manage", "You do not have permission to manage users.")
    if forbidden is not None:
        return forbidden

    safe_user_id = str(user_id or "").strip()
    if not safe_user_id:
        return _json_error("user_id is required.", 400)
    if safe_user_id == "__env_admin__":
        return _json_error("System admin user cannot be edited here.", 400)

    payload = request.get_json(silent=True) or {}
    username = str(payload.get("username", "")).strip()
    password = str(payload.get("password", ""))
    roles_raw = payload.get("roles")

    if not username:
        return _json_error("username is required.", 400)
    if not re.match(r"^[A-Za-z0-9_.-]{3,64}$", username):
        return _json_error("username must be 3-64 chars and contain only letters, digits, . _ -", 400)

    store = _load_users_store()
    users = [item for item in (store.get("users") or []) if isinstance(item, dict)]
    target = next((item for item in users if str(item.get("id", "")) == safe_user_id), None)
    if not target:
        return _json_error("User not found.", 404)

    all_roles = _all_role_names(store)
    roles, role_errors = _validate_roles(roles_raw, all_roles)
    if role_errors:
        return _json_error("Role validation failed.", 400, errors=role_errors)

    lower_username = username.lower()
    for item in users:
        if str(item.get("id", "")) == safe_user_id:
            continue
        if str(item.get("username", "")).strip().lower() == lower_username:
            return _json_error("username already exists.", 409)
    if lower_username == AppConfig.ADMIN_USERNAME.lower():
        return _json_error("username is reserved.", 409)

    target["username"] = username
    target["roles"] = roles
    if password:
        if len(password) < 6:
            return _json_error("password must be at least 6 characters.", 400)
        target["password_hash"] = generate_password_hash(password)

    store["users"] = users
    _save_users_store(store)

    metrics = _collect_admin_dashboard_metrics()
    return jsonify(
        {
            "success": True,
            "user": _public_user_record(target),
            "users": _public_users_list(store),
            "all_roles": _all_role_names(store),
            "role_catalog": _role_catalog(store),
            "metrics": {"total_users": metrics["total_users"]},
        }
    )


@app.delete("/api/admin/users/<user_id>")
def api_admin_delete_user(user_id: str):
    forbidden = _require_permission("users_manage", "You do not have permission to manage users.")
    if forbidden is not None:
        return forbidden

    safe_user_id = str(user_id or "").strip()
    if not safe_user_id:
        return _json_error("user_id is required.", 400)
    if safe_user_id == "__env_admin__":
        return _json_error("System admin user cannot be deleted.", 400)

    store = _load_users_store()
    users = [item for item in (store.get("users") or []) if isinstance(item, dict)]
    kept = [item for item in users if str(item.get("id", "")) != safe_user_id]
    if len(kept) == len(users):
        return _json_error("User not found.", 404)
    store["users"] = kept
    _save_users_store(store)

    metrics = _collect_admin_dashboard_metrics()
    return jsonify(
        {
            "success": True,
            "deleted_user_id": safe_user_id,
            "users": _public_users_list(store),
            "all_roles": _all_role_names(store),
            "role_catalog": _role_catalog(store),
            "metrics": {"total_users": metrics["total_users"]},
        }
    )


@app.delete("/api/admin/saved-pipelines/<filename>")
def api_admin_delete_saved_pipeline_file(filename: str):
    forbidden = _require_admin_access()
    if forbidden is not None:
        return forbidden

    target = _resolve_admin_file_target(
        AppConfig.SAVED_PIPELINES_DIR,
        str(filename or "").strip(),
        expected_extension=".json",
    )
    if target is None:
        return _json_error("Invalid filename.", 400)
    if not target.exists():
        return _json_error("Saved pipeline file not found.", 404)
    try:
        target.unlink()
    except Exception as exc:  # noqa: BLE001
        return _json_error(f"Unable to delete saved pipeline file: {exc}", 500)

    return jsonify(
        {
            "success": True,
            "deleted_file": target.name,
            "metrics": _collect_admin_dashboard_metrics(),
        }
    )


@app.delete("/api/admin/generated-dags/<filename>")
def api_admin_delete_generated_dag_file(filename: str):
    forbidden = _require_admin_access()
    if forbidden is not None:
        return forbidden

    safe_name = _safe_filename(str(filename or "").strip(), "")
    if not safe_name or safe_name != str(filename or "").strip() or not safe_name.endswith(".py"):
        return _json_error("Invalid filename.", 400)

    target = _resolve_admin_file_target(
        AppConfig.GENERATED_DAGS_DIR,
        safe_name,
        expected_extension=".py",
    )
    if target is None:
        return _json_error("Invalid filename.", 400)

    airflow_candidates: list[Path] = []
    airflow_dir = AppConfig.AIRFLOW_DAGS_DIR
    try:
        if airflow_dir.exists():
            direct = airflow_dir / safe_name
            if direct.exists():
                airflow_candidates.append(direct)
            for candidate in airflow_dir.rglob("*.py"):
                try:
                    if candidate.name.lower() == safe_name.lower():
                        airflow_candidates.append(candidate)
                except Exception:  # noqa: BLE001
                    continue
    except Exception:  # noqa: BLE001
        airflow_candidates = []

    deduped_airflow_candidates: list[Path] = []
    seen_candidates: set[str] = set()
    for path in airflow_candidates:
        key = str(path.resolve()).lower()
        if key in seen_candidates:
            continue
        seen_candidates.add(key)
        deduped_airflow_candidates.append(path)
    airflow_candidates = deduped_airflow_candidates

    local_exists = bool(target.exists())
    airflow_exists = bool(airflow_candidates)
    if not local_exists and not airflow_exists:
        return _json_error("Generated DAG file not found in local or Airflow DAG folders.", 404)

    local_deleted = False
    airflow_deleted_paths: list[str] = []
    try:
        if local_exists:
            target.unlink()
            local_deleted = True
        for airflow_target in airflow_candidates:
            if airflow_target.exists():
                airflow_target.unlink()
                airflow_deleted_paths.append(str(airflow_target))
    except Exception as exc:  # noqa: BLE001
        return _json_error(
            f"Unable to delete generated DAG file: {exc}",
            500,
            local_deleted=local_deleted,
            airflow_deleted=bool(airflow_deleted_paths),
            airflow_deleted_paths=airflow_deleted_paths,
        )

    return jsonify(
        {
            "success": True,
            "deleted_file": target.name,
            "local_deleted": local_deleted,
            "airflow_deleted": bool(airflow_deleted_paths),
            "airflow_deleted_paths": airflow_deleted_paths,
            "metrics": _collect_admin_dashboard_metrics(),
        }
    )


@app.get("/release-notes/v0")
def release_notes_v0():
    return render_template("release_notes_v0.html")


@app.get("/release-notes/v0.1")
@app.get("/release-notes/0v.1")
@app.get("/release-notes/v0_1")
@app.get("/release-notes/v0-1")
def release_notes_v0_1():
    return render_template("release_notes_v0_1.html")


@app.get("/release-notes/v0.2")
@app.get("/release-notes/v0.2/")
@app.get("/release-notes/v0_2")
@app.get("/release-notes/v0_2/")
@app.get("/release-notes/v0-2")
@app.get("/release-notes/v0-2/")
@app.get("/release-notes/v0.2.html")
@app.get("/release-notes/0v.2")
@app.get("/release-notes/0v.2/")
def release_notes_v0_2():
    return render_template("release_notes_v0_2.html")


@app.get("/api/modules")
def api_modules():
    forbidden = _require_permission("builder_view")
    if forbidden is not None:
        return forbidden
    return jsonify({"success": True, "modules": list_modules()})


@app.get("/api/airflow/connections")
def api_list_airflow_connections():
    forbidden = _require_permission("connections_view", "You do not have permission to view connections.")
    if forbidden is not None:
        return forbidden

    items: list[dict[str, Any]] = []
    airflow_list_warning = ""
    try:
        items = list_connections(_airflow_api_config())
    except AirflowApiError as exc:
        airflow_list_warning = str(exc)
    except Exception as exc:  # noqa: BLE001
        airflow_list_warning = f"Unable to list Airflow connections: {exc}"

    filtered = []
    for item in items:
        conn_type = _normalize_connection_type(item.get("conn_type", ""))
        if conn_type not in {"http", "wasb"}:
            continue
        filtered.append(item)
    filtered.extend(_list_gcp_service_account_connections())
    filtered.sort(key=lambda row: str(row.get("connection_id", "")).lower())
    return jsonify({"success": True, "connections": filtered, "warning": airflow_list_warning})


@app.post("/api/airflow/connections")
def api_create_airflow_connection():
    forbidden = _require_permission("connections_manage", "You do not have permission to manage connections.")
    if forbidden is not None:
        return forbidden

    if request.is_json:
        body = request.get_json(silent=True) or {}
    else:
        body = {key: value for key, value in request.form.items()}
    conn_type = _normalize_connection_type(body.get("conn_type") or body.get("connection_type"))

    if conn_type == "gcp_service_account":
        created, errors = _upsert_gcp_service_account_connection(
            connection_id=str(body.get("connection_id", "")).strip(),
            description=str(body.get("description", "")).strip(),
            uploaded_file=request.files.get("service_account_file_upload"),
            for_update=False,
        )
        if errors:
            return _json_error("Connection validation failed.", 400, errors=errors)
    else:
        payload, errors = _build_connection_payload(body, for_update=False)
        if errors:
            return _json_error("Connection validation failed.", 400, errors=errors)

        try:
            created = create_connection(_airflow_api_config(), payload or {})
        except AirflowApiError as exc:
            status_code = 409 if exc.status_code == 409 else 502
            return _json_error(str(exc), status_code)
        except Exception as exc:  # noqa: BLE001
            return _json_error(f"Unable to create Airflow connection: {exc}", 500)

    return jsonify({"success": True, "connection": created})


@app.put("/api/airflow/connections/<connection_id>")
def api_update_airflow_connection(connection_id: str):
    forbidden = _require_permission("connections_manage", "You do not have permission to manage connections.")
    if forbidden is not None:
        return forbidden

    safe_connection_id = str(connection_id or "").strip()
    if not safe_connection_id:
        return _json_error("Connection ID is required in URL path.", 400)

    if request.is_json:
        body = request.get_json(silent=True) or {}
    else:
        body = {key: value for key, value in request.form.items()}

    requested_type = _normalize_connection_type(body.get("conn_type") or body.get("connection_type"))
    existing_gcp = _find_gcp_connection(safe_connection_id)
    if requested_type == "gcp_service_account" or existing_gcp is not None:
        updated, errors = _upsert_gcp_service_account_connection(
            connection_id=safe_connection_id,
            description=str(body.get("description", "")).strip(),
            uploaded_file=request.files.get("service_account_file_upload"),
            for_update=True,
        )
        if errors:
            return _json_error("Connection validation failed.", 400, errors=errors)
    else:
        payload, errors = _build_connection_payload(body, for_update=True)
        if errors:
            return _json_error("Connection validation failed.", 400, errors=errors)
        payload = payload or {}
        payload["connection_id"] = safe_connection_id

        try:
            updated = update_connection(_airflow_api_config(), safe_connection_id, payload)
        except AirflowApiError as exc:
            status_code = exc.status_code if exc.status_code in {400, 401, 403, 404, 409, 422} else 502
            return _json_error(str(exc), status_code)
        except Exception as exc:  # noqa: BLE001
            return _json_error(f"Unable to update Airflow connection: {exc}", 500)

    return jsonify({"success": True, "connection": updated})


@app.delete("/api/airflow/connections/<connection_id>")
def api_delete_airflow_connection(connection_id: str):
    forbidden = _require_permission("connections_manage", "You do not have permission to manage connections.")
    if forbidden is not None:
        return forbidden

    safe_connection_id = str(connection_id or "").strip()
    if not safe_connection_id:
        return _json_error("Connection ID is required in URL path.", 400)

    existing_gcp = _find_gcp_connection(safe_connection_id)
    if existing_gcp is not None:
        store = _load_gcp_connections_store()
        rows = [item for item in store.get("connections", []) if isinstance(item, dict)]
        kept = [
            item
            for item in rows
            if str(item.get("connection_id", "")).strip() != safe_connection_id
        ]
        if len(kept) == len(rows):
            return _json_error("Connection not found.", 404)
        store["connections"] = kept
        _save_gcp_connections_store(store)
    else:
        try:
            delete_connection(_airflow_api_config(), safe_connection_id)
        except AirflowApiError as exc:
            status_code = 404 if exc.status_code == 404 else 502
            return _json_error(str(exc), status_code)
        except Exception as exc:  # noqa: BLE001
            return _json_error(f"Unable to delete Airflow connection: {exc}", 500)

    return jsonify({"success": True, "deleted_connection_id": safe_connection_id})


@app.get("/api/health")
def api_health():
    return jsonify({"success": True, "status": "ok"})


@app.get("/api/module-schema/<module_type>")
def api_module_schema(module_type: str):
    forbidden = _require_permission("builder_view")
    if forbidden is not None:
        return forbidden
    schema = get_module_schema(module_type)
    if not schema:
        return _json_error(f"Unknown module type: {module_type}", 404)
    return jsonify({"success": True, "schema": schema})


@app.post("/api/upload-local-files")
def api_upload_local_files():
    forbidden = _require_permission("pipeline_edit", "You do not have permission to upload local files.")
    if forbidden is not None:
        return forbidden

    pipeline_id = _safe_filename(str(request.form.get("pipeline_id", "")).strip(), "pipeline")
    node_id = _safe_filename(str(request.form.get("node_id", "")).strip(), "node")
    files = request.files.getlist("files")
    if not files:
        return _json_error("No files provided.", 400)

    target_dir = (AppConfig.LOCAL_UPLOAD_STAGING_DIR / pipeline_id / node_id).resolve()
    base_upload_root = AppConfig.LOCAL_UPLOAD_STAGING_DIR.resolve()
    if not (target_dir == base_upload_root or target_dir.is_relative_to(base_upload_root)):
        return _json_error("Invalid upload target directory.", 400)
    target_dir.mkdir(parents=True, exist_ok=True)

    uploaded: list[dict[str, Any]] = []
    for item in files:
        filename = _safe_filename(str(getattr(item, "filename", "")).strip(), "upload.bin")
        host_path = target_dir / f"{uuid4().hex}_{filename}"
        try:
            item.save(str(host_path))
        except Exception as exc:  # noqa: BLE001
            return _json_error(f"Unable to save uploaded file '{filename}': {exc}", 500)

        try:
            container_path = sync_host_file_to_containers(
                host_file=host_path,
                pipeline_id=pipeline_id,
                node_id=node_id,
                target_name=filename,
                enabled=AppConfig.AIRFLOW_DOCKER_SYNC_ENABLED,
                containers=AppConfig.AIRFLOW_DOCKER_SYNC_CONTAINERS,
                container_base_dir=AppConfig.AIRFLOW_DOCKER_SYNC_BASE_DIR,
            )
        except ContainerFileSyncError as exc:
            return _json_error(f"Unable to sync file '{filename}' to Airflow containers: {exc}", 500)

        uploaded.append(
            {
                "filename": filename,
                "staged_path": str(host_path),
                "container_path": container_path,
            }
        )

    return jsonify(
        {
            "success": True,
            "pipeline_id": pipeline_id,
            "node_id": node_id,
            "files": uploaded,
        }
    )


@app.get("/api/requests")
def api_list_my_requests():
    if not _is_authenticated():
        return _json_error("Authentication required.", 401)

    identity = _current_user_identity()
    store = _load_requests_store()
    rows = [item for item in (store.get("requests") or []) if isinstance(item, dict)]
    mine = [
        _public_request_record(item)
        for item in rows
        if str(item.get("requester_user_id", "")).strip() == identity["user_id"]
    ]
    mine.sort(key=lambda row: row.get("created_at", ""), reverse=True)
    return jsonify({"success": True, "requests": mine})


@app.post("/api/requests")
def api_create_request():
    if not _is_authenticated():
        return _json_error("Authentication required.", 401)

    payload = request.get_json(silent=True) or {}
    requester = _current_user_identity()
    record, errors = _build_request_payload(payload, requester)
    if errors:
        return _json_error("Request validation failed.", 400, errors=errors)
    if not record:
        return _json_error("Request payload is invalid.", 400)

    store = _load_requests_store()
    requests_rows = [item for item in (store.get("requests") or []) if isinstance(item, dict)]
    requests_rows.append(record)
    store["requests"] = requests_rows
    _save_requests_store(store)

    return jsonify(
        {
            "success": True,
            "request": _public_request_record(record),
            "message": "Request submitted successfully.",
        }
    )


@app.get("/api/admin/requests")
def api_admin_list_requests():
    forbidden = _require_admin_access()
    if forbidden is not None:
        return forbidden

    store = _load_requests_store()
    rows = [_public_request_record(item) for item in (store.get("requests") or []) if isinstance(item, dict)]
    rows.sort(key=lambda row: row.get("created_at", ""), reverse=True)
    return jsonify({"success": True, "requests": rows})


@app.put("/api/admin/requests/<request_id>/finish")
def api_admin_finish_request(request_id: str):
    forbidden = _require_admin_access()
    if forbidden is not None:
        return forbidden

    safe_request_id = str(request_id or "").strip()
    if not safe_request_id:
        return _json_error("request_id is required.", 400)

    store = _load_requests_store()
    rows = [item for item in (store.get("requests") or []) if isinstance(item, dict)]
    target = next((item for item in rows if str(item.get("id", "")).strip() == safe_request_id), None)
    if not target:
        return _json_error("Request not found.", 404)

    target["status"] = REQUEST_STATUS_FINISHED
    target["updated_at"] = _utc_now_iso()
    store["requests"] = rows
    _save_requests_store(store)

    normalized = [_public_request_record(item) for item in rows]
    normalized.sort(key=lambda row: row.get("created_at", ""), reverse=True)
    return jsonify(
        {
            "success": True,
            "request": _public_request_record(target),
            "requests": normalized,
        }
    )


@app.delete("/api/admin/requests/<request_id>")
def api_admin_delete_request(request_id: str):
    forbidden = _require_admin_access()
    if forbidden is not None:
        return forbidden

    safe_request_id = str(request_id or "").strip()
    if not safe_request_id:
        return _json_error("request_id is required.", 400)

    store = _load_requests_store()
    rows = [item for item in (store.get("requests") or []) if isinstance(item, dict)]
    kept = [item for item in rows if str(item.get("id", "")).strip() != safe_request_id]
    if len(kept) == len(rows):
        return _json_error("Request not found.", 404)

    store["requests"] = kept
    _save_requests_store(store)

    normalized = [_public_request_record(item) for item in kept]
    normalized.sort(key=lambda row: row.get("created_at", ""), reverse=True)
    return jsonify({"success": True, "deleted_request_id": safe_request_id, "requests": normalized})


@app.post("/api/validate-pipeline")
def api_validate_pipeline():
    forbidden = _require_permission("pipeline_validate", "You do not have permission to validate pipelines.")
    if forbidden is not None:
        return forbidden

    payload = request.get_json(silent=True) or {}
    resolved_payload, resolve_errors = _resolve_gcp_connections_in_payload(payload)
    result = validate_pipeline(resolved_payload)
    if resolve_errors:
        result["valid"] = False
        result["errors"] = list(result.get("errors", [])) + resolve_errors
    return jsonify(
        {
            "success": True,
            "valid": result["valid"],
            "errors": result["errors"],
            "execution_order": result["execution_order"],
            "normalized_payload": result["normalized_payload"],
        }
    )


@app.post("/api/generate-dag")
def api_generate_dag():
    forbidden = _require_permission("dag_generate", "You do not have permission to generate DAG files.")
    if forbidden is not None:
        return forbidden

    payload = request.get_json(silent=True) or {}
    resolved_payload, resolve_errors = _resolve_gcp_connections_in_payload(payload)
    validation = validate_pipeline(resolved_payload)
    if resolve_errors:
        validation["valid"] = False
        validation["errors"] = list(validation.get("errors", [])) + resolve_errors
    if not validation["valid"]:
        return _json_error(
            "Pipeline validation failed.",
            400,
            errors=validation["errors"],
            execution_order=validation["execution_order"],
        )

    normalized_payload = validation["normalized_payload"]
    logical_pipeline = normalized_payload.get("pipeline", {})
    logical_pipeline_id = str(logical_pipeline.get("dag_id", "")).strip()
    if not logical_pipeline_id:
        return _json_error("pipeline.dag_id is required for versioning.", 400)

    listing_before = list_pipeline_versions(AppConfig.PIPELINE_VERSIONS_STORE_FILE, logical_pipeline_id)
    current_before = None
    current_before_id = str(listing_before.get("current_version_id") or "").strip()
    if current_before_id:
        current_before = get_pipeline_version(
            AppConfig.PIPELINE_VERSIONS_STORE_FILE,
            logical_pipeline_id,
            current_before_id,
        )

    matched_version = find_matching_version(
        AppConfig.PIPELINE_VERSIONS_STORE_FILE,
        logical_pipeline_id,
        normalized_payload,
    )

    current_source_definition = (
        (current_before or {}).get("source_definition")
        if isinstance((current_before or {}).get("source_definition"), dict)
        else {}
    )
    topology_changed = bool(
        current_before
        and _pipeline_topology_signature(current_source_definition) != _pipeline_topology_signature(normalized_payload)
    )

    created_new_version = matched_version is None
    updated_current_in_place = False
    if matched_version is not None:
        matched_version_id = str(matched_version.get("version_id", "")).strip()
        matched_dag_version = int(matched_version.get("dag_version", 0) or 0)
        fallback_airflow_dag_id = f"{logical_pipeline_id}__{matched_version_id or ('v' + str(matched_dag_version))}"
        fallback_output_filename = f"{fallback_airflow_dag_id}.py"
        version_plan = {
            "pipeline_id": logical_pipeline_id,
            "dag_version": matched_dag_version,
            "version_id": matched_version_id,
            "airflow_dag_id": str(matched_version.get("airflow_dag_id", "")).strip() or fallback_airflow_dag_id,
            "output_filename": str(matched_version.get("output_filename", "")).strip() or fallback_output_filename,
        }
        if not version_plan["airflow_dag_id"] or not version_plan["output_filename"] or not version_plan["version_id"]:
            version_plan = plan_next_version(AppConfig.PIPELINE_VERSIONS_STORE_FILE, logical_pipeline_id)
            created_new_version = True
    elif current_before and not topology_changed:
        current_version_id = str(current_before.get("version_id", "")).strip()
        current_dag_version = int(current_before.get("dag_version", 0) or 0)
        fallback_airflow_dag_id = f"{logical_pipeline_id}__{current_version_id or ('v' + str(current_dag_version))}"
        fallback_output_filename = f"{fallback_airflow_dag_id}.py"
        version_plan = {
            "pipeline_id": logical_pipeline_id,
            "dag_version": current_dag_version,
            "version_id": current_version_id,
            "airflow_dag_id": str(current_before.get("airflow_dag_id", "")).strip() or fallback_airflow_dag_id,
            "output_filename": str(current_before.get("output_filename", "")).strip() or fallback_output_filename,
        }
        if not version_plan["airflow_dag_id"] or not version_plan["output_filename"] or not version_plan["version_id"]:
            version_plan = plan_next_version(AppConfig.PIPELINE_VERSIONS_STORE_FILE, logical_pipeline_id)
            created_new_version = True
        else:
            created_new_version = False
            updated_current_in_place = True
    else:
        version_plan = plan_next_version(AppConfig.PIPELINE_VERSIONS_STORE_FILE, logical_pipeline_id)

    payload_for_generation = json.loads(json.dumps(normalized_payload))
    payload_for_generation["pipeline"]["dag_id"] = version_plan["airflow_dag_id"]
    payload_for_generation["pipeline"]["output_filename"] = version_plan["output_filename"]
    sync_summary: dict[str, Any] | None = None

    try:
        sync_summary = sync_gcs_local_files_to_containers(
            payload_for_generation,
            enabled=AppConfig.AIRFLOW_DOCKER_SYNC_ENABLED,
            containers=AppConfig.AIRFLOW_DOCKER_SYNC_CONTAINERS,
            container_base_dir=AppConfig.AIRFLOW_DOCKER_SYNC_BASE_DIR,
        )
    except ContainerFileSyncError as exc:
        return _json_error(f"Local file container sync failed: {exc}", 500)

    try:
        generated = dag_generator.generate(payload_for_generation, validation["execution_order"])
    except Exception as exc:  # noqa: BLE001
        return _json_error(f"Template rendering failed: {exc}", 500)

    try:
        write_result = write_and_copy_dag(
            filename=generated["filename"],
            content=generated["content"],
            generated_dags_dir=AppConfig.GENERATED_DAGS_DIR,
            airflow_dags_dir=AppConfig.AIRFLOW_DAGS_DIR,
        )
    except Exception as exc:  # noqa: BLE001
        return _json_error(f"File writing failed: {exc}", 500)

    dag_id = payload_for_generation["pipeline"]["dag_id"]

    if created_new_version:
        version_record = register_version(
            AppConfig.PIPELINE_VERSIONS_STORE_FILE,
            pipeline_id=logical_pipeline_id,
            dag_version=version_plan["dag_version"],
            version_id=version_plan["version_id"],
            airflow_dag_id=version_plan["airflow_dag_id"],
            output_filename=version_plan["output_filename"],
            source_definition=normalized_payload,
            generated_local_path=write_result["local_path"],
            generated_airflow_path=write_result["airflow_path"],
            node_task_map=generated.get("node_task_map", {}),
            node_primary_task_map=generated.get("node_primary_task_map", {}),
            created_by=_current_actor_username(),
        )
    elif updated_current_in_place:
        version_record = update_version_definition(
            AppConfig.PIPELINE_VERSIONS_STORE_FILE,
            pipeline_id=logical_pipeline_id,
            version_id=version_plan["version_id"],
            source_definition=normalized_payload,
            generated_local_path=write_result["local_path"],
            generated_airflow_path=write_result["airflow_path"],
            node_task_map=generated.get("node_task_map", {}),
            node_primary_task_map=generated.get("node_primary_task_map", {}),
            actor=_current_actor_username(),
        )
    else:
        switch_result = set_current_version(
            AppConfig.PIPELINE_VERSIONS_STORE_FILE,
            pipeline_id=logical_pipeline_id,
            version_id=version_plan["version_id"],
            actor=_current_actor_username(),
        )
        version_record = switch_result.get("version") or matched_version or {}

    previous_output_filename = str((current_before or {}).get("output_filename", "")).strip()
    planned_output_filename = str(version_plan.get("output_filename", "")).strip()
    registration_wait_required = True
    if previous_output_filename and planned_output_filename and previous_output_filename == planned_output_filename:
        registration_wait_required = False

    readiness: dict[str, Any] | None = None
    if write_result["airflow_copy_success"] and registration_wait_required:
        try:
            readiness = check_dag_readiness(
                config=_airflow_api_config(),
                dag_id=dag_id,
                airflow_dag_path=write_result["airflow_path"],
                auto_unpause=True,
            )
        except Exception:  # noqa: BLE001
            # Keep generation successful even if readiness probe fails.
            readiness = None

    return jsonify(
        {
            "success": True,
            "pipeline_id": logical_pipeline_id,
            "version_id": version_record.get("version_id"),
            "dag_version": version_record.get("dag_version"),
            "version_reused": bool((not created_new_version) and (not updated_current_in_place)),
            "version_updated_in_place": bool(updated_current_in_place),
            "dag_id": dag_id,
            "generated_filename": write_result["filename"],
            "local_path": write_result["local_path"],
            "airflow_path": write_result["airflow_path"],
            "airflow_copy_success": write_result["airflow_copy_success"],
            "registration_wait_required": registration_wait_required,
            "warnings": write_result["warnings"],
            "container_sync": sync_summary or {"enabled": False, "copied_files": [], "updated_nodes": []},
            "readiness": readiness,
            "readiness_poll_timeout_seconds": AppConfig.DAG_READY_POLL_TIMEOUT_SECONDS,
            "readiness_poll_interval_seconds": AppConfig.DAG_READY_POLL_INTERVAL_SECONDS,
            "execution_order": generated["execution_order"],
            "node_task_map": generated.get("node_task_map", {}),
            "node_primary_task_map": generated.get("node_primary_task_map", {}),
            "dag_preview": generated["content"],
        }
    )


@app.post("/api/dag-readiness")
def api_dag_readiness():
    forbidden = _require_permission("builder_view")
    if forbidden is not None:
        return forbidden

    payload = request.get_json(silent=True) or {}
    dag_id = str(payload.get("dag_id", "")).strip()
    if not dag_id:
        return _json_error("dag_id is required.")

    airflow_path = str(payload.get("airflow_path", "")).strip() or None
    auto_unpause = bool(payload.get("auto_unpause", True))

    try:
        readiness = check_dag_readiness(
            config=_airflow_api_config(),
            dag_id=dag_id,
            airflow_dag_path=airflow_path,
            auto_unpause=auto_unpause,
        )
    except AirflowApiError as exc:
        return _json_error(str(exc), 502, ready=False, dag_id=dag_id)
    except Exception as exc:  # noqa: BLE001
        return _json_error(f"Readiness check failed: {exc}", 500, ready=False, dag_id=dag_id)

    return jsonify({"success": True, **readiness})


@app.post("/api/run-dag")
def api_run_dag():
    forbidden = _require_permission("dag_run", "You do not have permission to run DAGs.")
    if forbidden is not None:
        return forbidden

    payload = request.get_json(silent=True) or {}
    dag_id = str(payload.get("dag_id", "")).strip()
    if not dag_id:
        return _json_error("dag_id is required.")

    conf = payload.get("conf")
    if conf is None:
        conf = {}
    if not isinstance(conf, dict):
        return _json_error("conf must be an object.")

    logical_date = payload.get("logical_date")
    if logical_date is not None:
        logical_date = str(logical_date)

    raw_trigger_timeout = payload.get("registration_retry_timeout_seconds")
    if raw_trigger_timeout is None:
        trigger_timeout_seconds = 0
    else:
        trigger_timeout_seconds = max(0, min(3600, int(raw_trigger_timeout or 0)))
    trigger_retry_interval_seconds = max(1, min(10, int(payload.get("registration_retry_interval_seconds", 2) or 2)))
    effective_logical_date = logical_date or datetime.now(timezone.utc).isoformat()
    started_at = time.monotonic()
    attempts = 0

    while True:
        attempts += 1
        try:
            run_result = trigger_dag_run(
                config=_airflow_api_config(),
                dag_id=dag_id,
                conf=conf,
                logical_date=effective_logical_date,
                auto_unpause=True,
            )
            break
        except AirflowApiError as exc:
            if _is_airflow_not_registered_error(exc):
                elapsed = time.monotonic() - started_at
                if trigger_timeout_seconds <= 0 or elapsed < trigger_timeout_seconds:
                    time.sleep(trigger_retry_interval_seconds)
                    continue
                return _json_error(
                    "DAG run trigger timed out while waiting for Airflow to register DAG "
                    f"'{dag_id}'. Attempts: {attempts}.",
                    502,
                    dag_id=dag_id,
                )
            return _json_error(str(exc), 502, dag_id=dag_id)
        except Exception as exc:  # noqa: BLE001
            return _json_error(f"DAG run trigger failed: {exc}", 500, dag_id=dag_id)

    return jsonify(
        {
            "success": True,
            "run_poll_timeout_seconds": AppConfig.DAG_RUN_POLL_TIMEOUT_SECONDS,
            "run_poll_interval_seconds": AppConfig.DAG_RUN_POLL_INTERVAL_SECONDS,
            **run_result,
        }
    )


@app.get("/api/runs/<dag_id>/<dag_run_id>/status")
def api_run_status(dag_id: str, dag_run_id: str):
    forbidden = _require_permission("logs_view", "You do not have permission to view run status.")
    if forbidden is not None:
        return forbidden

    try:
        status = get_dag_run_status(
            config=_airflow_api_config(),
            dag_id=dag_id,
            dag_run_id=dag_run_id,
        )
    except AirflowApiError as exc:
        return _json_error(str(exc), 502, dag_id=dag_id, dag_run_id=dag_run_id)
    except Exception as exc:  # noqa: BLE001
        return _json_error(f"DAG run status lookup failed: {exc}", 500, dag_id=dag_id, dag_run_id=dag_run_id)

    return jsonify({"success": True, **status})


@app.get("/api/runs/<dag_id>/<dag_run_id>/tasks")
def api_run_tasks(dag_id: str, dag_run_id: str):
    forbidden = _require_permission("logs_view", "You do not have permission to view task instances.")
    if forbidden is not None:
        return forbidden

    try:
        task_instances = list_dag_run_task_instances(
            config=_airflow_api_config(),
            dag_id=dag_id,
            dag_run_id=dag_run_id,
        )
    except AirflowApiError as exc:
        return _json_error(str(exc), 502, dag_id=dag_id, dag_run_id=dag_run_id)
    except Exception as exc:  # noqa: BLE001
        return _json_error(f"Task instance lookup failed: {exc}", 500, dag_id=dag_id, dag_run_id=dag_run_id)

    return jsonify({"success": True, "dag_id": dag_id, "dag_run_id": dag_run_id, "task_instances": task_instances})


@app.get("/api/runs/<dag_id>/<dag_run_id>/tasks/<task_id>/logs")
def api_run_task_logs(dag_id: str, dag_run_id: str, task_id: str):
    forbidden = _require_permission("logs_view", "You do not have permission to view task logs.")
    if forbidden is not None:
        return forbidden

    raw_try_number = str(request.args.get("try_number", "1")).strip()
    try:
        try_number = max(1, int(raw_try_number))
    except ValueError:
        return _json_error("try_number must be an integer >= 1.")

    try:
        log_payload = get_task_instance_log(
            config=_airflow_api_config(),
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            try_number=try_number,
        )
    except AirflowApiError as exc:
        return _json_error(str(exc), 502, dag_id=dag_id, dag_run_id=dag_run_id, task_id=task_id)
    except Exception as exc:  # noqa: BLE001
        return _json_error(
            f"Task log fetch failed: {exc}",
            500,
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
        )

    return jsonify({"success": True, **log_payload})


@app.post("/api/runs/<dag_id>/<dag_run_id>/tasks/<task_id>/retry")
def api_retry_task_instance(dag_id: str, dag_run_id: str, task_id: str):
    forbidden = _require_permission("task_retry", "You do not have permission to retry tasks.")
    if forbidden is not None:
        return forbidden

    try:
        retry_payload = retry_task_instance(
            config=_airflow_api_config(),
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
        )
    except AirflowApiError as exc:
        return _json_error(str(exc), 502, dag_id=dag_id, dag_run_id=dag_run_id, task_id=task_id)
    except Exception as exc:  # noqa: BLE001
        return _json_error(
            f"Task retry failed: {exc}",
            500,
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
        )

    return jsonify({"success": True, **retry_payload})


@app.post("/api/runs/<dag_id>/<dag_run_id>/stop")
def api_stop_dag_run(dag_id: str, dag_run_id: str):
    forbidden = _require_permission("dag_retry", "You do not have permission to stop DAG runs.")
    if forbidden is not None:
        return forbidden

    dag_id = str(dag_id or "").strip()
    dag_run_id = str(dag_run_id or "").strip()
    if not dag_id or not dag_run_id:
        return _json_error("dag_id and dag_run_id are required.", 400)

    try:
        payload = stop_dag_run(_airflow_api_config(), dag_id, dag_run_id)
    except AirflowApiError as exc:
        return _json_error(
            f"Unable to stop DAG run: {exc}",
            exc.status_code or 502,
            dag_id=dag_id,
            dag_run_id=dag_run_id,
        )
    except Exception as exc:  # noqa: BLE001
        return _json_error(
            f"Unable to stop DAG run: {exc}",
            500,
            dag_id=dag_id,
            dag_run_id=dag_run_id,
        )

    return jsonify({"success": True, **payload})


@app.post("/api/save-pipeline")
def api_save_pipeline():
    forbidden = _require_permission("pipeline_save", "You do not have permission to save pipelines.")
    if forbidden is not None:
        return forbidden

    payload = request.get_json(silent=True) or {}
    pipeline_data = payload.get("pipeline_data")
    if not isinstance(pipeline_data, dict):
        return _json_error("Body must contain 'pipeline_data' object.")

    provided_name = str(payload.get("filename", "")).strip()
    dag_id = str((pipeline_data.get("pipeline", {}) or {}).get("dag_id", "")).strip()
    filename = _safe_filename(provided_name, f"{dag_id or 'pipeline'}.json")
    if not filename.endswith(".json"):
        filename += ".json"

    target_path = AppConfig.SAVED_PIPELINES_DIR / filename
    target_path.write_text(json.dumps(pipeline_data, indent=2), encoding="utf-8")
    return jsonify({"success": True, "filename": filename, "path": str(target_path)})


@app.get("/api/load-pipeline")
def api_list_pipeline_files():
    forbidden = _require_permission("pipeline_load", "You do not have permission to load pipelines.")
    if forbidden is not None:
        return forbidden
    files = sorted([path.name for path in AppConfig.SAVED_PIPELINES_DIR.glob("*.json")])
    return jsonify({"success": True, "files": files})


@app.post("/api/load-pipeline")
def api_load_pipeline():
    forbidden = _require_permission("pipeline_load", "You do not have permission to load pipelines.")
    if forbidden is not None:
        return forbidden

    payload = request.get_json(silent=True) or {}
    filename = str(payload.get("filename", "")).strip()
    if not filename:
        return _json_error("filename is required.")
    safe_name = _safe_filename(filename, "")
    if not safe_name or safe_name != filename:
        return _json_error("Invalid filename.")

    path = AppConfig.SAVED_PIPELINES_DIR / safe_name
    if not path.exists():
        return _json_error(f"Pipeline file not found: {filename}", 404)

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return _json_error(f"Saved file is not valid JSON: {exc}", 500)

    return jsonify({"success": True, "filename": safe_name, "pipeline_data": data})


@app.get("/api/pipeline-versions/<pipeline_id>")
def api_list_pipeline_versions(pipeline_id: str):
    forbidden = _require_permission("pipeline_load", "You do not have permission to view pipeline versions.")
    if forbidden is not None:
        return forbidden

    normalized_pipeline_id = str(pipeline_id or "").strip()
    if not normalized_pipeline_id:
        return _json_error("pipeline_id is required.", 400)

    listing = list_pipeline_versions(AppConfig.PIPELINE_VERSIONS_STORE_FILE, normalized_pipeline_id)
    return jsonify({"success": True, **listing})


@app.delete("/api/pipeline-versions/<pipeline_id>")
def api_clear_pipeline_versions(pipeline_id: str):
    forbidden = _require_permission("dag_generate", "You do not have permission to clear DAG versions.")
    if forbidden is not None:
        return forbidden

    normalized_pipeline_id = str(pipeline_id or "").strip()
    if not normalized_pipeline_id:
        return _json_error("pipeline_id is required.", 400)

    listing_before = list_pipeline_versions(AppConfig.PIPELINE_VERSIONS_STORE_FILE, normalized_pipeline_id)
    versions_before = [item for item in listing_before.get("versions", []) if isinstance(item, dict)]

    def _safe_unlink(path: Path, allowed_roots: list[Path]) -> bool:
        try:
            resolved = path.resolve()
        except Exception:  # noqa: BLE001
            return False
        inside_allowed = any(
            resolved == root or resolved.is_relative_to(root)
            for root in allowed_roots
        )
        if not inside_allowed:
            return False
        if not resolved.exists() or not resolved.is_file():
            return False
        resolved.unlink()
        return True

    airflow_root = AppConfig.AIRFLOW_DAGS_DIR.resolve()
    generated_root = AppConfig.GENERATED_DAGS_DIR.resolve()
    allowed_roots = [airflow_root, generated_root]

    output_filenames: set[str] = set()
    explicit_paths: list[Path] = []
    for version in versions_before:
        output_name = str(version.get("output_filename", "")).strip()
        if output_name and output_name.endswith(".py"):
            output_filenames.add(output_name)
        for key in ("generated_local_path", "generated_airflow_path"):
            raw_path = str(version.get(key, "")).strip()
            if raw_path:
                explicit_paths.append(Path(raw_path))

    deleted_paths: list[str] = []
    delete_errors: list[str] = []

    # Delete exact recorded paths first.
    for path in explicit_paths:
        try:
            if _safe_unlink(path, allowed_roots):
                deleted_paths.append(str(path.resolve()))
        except Exception as exc:  # noqa: BLE001
            delete_errors.append(f"{path}: {exc}")

    # Delete by versioned filename in local generated folder.
    for filename in output_filenames:
        local_path = AppConfig.GENERATED_DAGS_DIR / filename
        try:
            if _safe_unlink(local_path, allowed_roots):
                deleted_paths.append(str(local_path.resolve()))
        except Exception as exc:  # noqa: BLE001
            delete_errors.append(f"{local_path}: {exc}")

    # Delete by filename across Airflow DAG folder (including nested subfolders).
    for filename in output_filenames:
        try:
            for candidate in AppConfig.AIRFLOW_DAGS_DIR.rglob("*.py"):
                try:
                    if candidate.name.lower() != filename.lower():
                        continue
                except Exception:  # noqa: BLE001
                    continue
                try:
                    if _safe_unlink(candidate, allowed_roots):
                        deleted_paths.append(str(candidate.resolve()))
                except Exception as exc:  # noqa: BLE001
                    delete_errors.append(f"{candidate}: {exc}")
        except Exception as exc:  # noqa: BLE001
            delete_errors.append(f"{AppConfig.AIRFLOW_DAGS_DIR}: {exc}")

    clear_result = clear_pipeline_versions(AppConfig.PIPELINE_VERSIONS_STORE_FILE, normalized_pipeline_id)
    listing_after = list_pipeline_versions(AppConfig.PIPELINE_VERSIONS_STORE_FILE, normalized_pipeline_id)

    dedup_deleted_paths = sorted(set(deleted_paths))
    return jsonify(
        {
            "success": True,
            "pipeline_id": normalized_pipeline_id,
            "versions_removed": int(clear_result.get("removed_versions_count", 0) or 0),
            "deleted_files_count": len(dedup_deleted_paths),
            "deleted_files": dedup_deleted_paths,
            "delete_errors": delete_errors,
            "versions_listing": listing_after,
        }
    )


@app.get("/api/pipeline-versions/<pipeline_id>/<version_id>")
def api_pipeline_version_details(pipeline_id: str, version_id: str):
    forbidden = _require_permission("pipeline_load", "You do not have permission to view pipeline versions.")
    if forbidden is not None:
        return forbidden

    normalized_pipeline_id = str(pipeline_id or "").strip()
    normalized_version_id = str(version_id or "").strip()
    version = get_pipeline_version(AppConfig.PIPELINE_VERSIONS_STORE_FILE, normalized_pipeline_id, normalized_version_id)
    if not version:
        return _json_error("Version not found.", 404)
    return jsonify({"success": True, "version": version})


@app.post("/api/pipeline-versions/<pipeline_id>/current")
def api_set_pipeline_current_version(pipeline_id: str):
    forbidden = _require_permission("dag_generate", "You do not have permission to switch current DAG version.")
    if forbidden is not None:
        return forbidden

    payload = request.get_json(silent=True) or {}
    version_id = str(payload.get("version_id", "")).strip()
    if not version_id:
        return _json_error("version_id is required.", 400)

    normalized_pipeline_id = str(pipeline_id or "").strip()
    try:
        result = set_current_version(
            AppConfig.PIPELINE_VERSIONS_STORE_FILE,
            pipeline_id=normalized_pipeline_id,
            version_id=version_id,
            actor=_current_actor_username(),
        )
    except ValueError as exc:
        return _json_error(str(exc), 404)

    return jsonify({"success": True, **result})


@app.get("/api/pipeline-versions/<pipeline_id>/<version_id>/last-execution")
def api_pipeline_version_last_execution(pipeline_id: str, version_id: str):
    forbidden = _require_permission("logs_view", "You do not have permission to view run history.")
    if forbidden is not None:
        return forbidden

    normalized_pipeline_id = str(pipeline_id or "").strip()
    normalized_version_id = str(version_id or "").strip()
    version = get_pipeline_version(AppConfig.PIPELINE_VERSIONS_STORE_FILE, normalized_pipeline_id, normalized_version_id)
    if not version:
        return _json_error("Version not found.", 404)

    try:
        summary = _version_last_execution_summary(version)
    except AirflowApiError as exc:
        return _json_error(
            f"Unable to fetch last execution from Airflow: {exc}",
            exc.status_code or 502,
            pipeline_id=normalized_pipeline_id,
            version_id=normalized_version_id,
        )
    except Exception as exc:  # noqa: BLE001
        return _json_error(
            f"Unable to fetch last execution: {exc}",
            500,
            pipeline_id=normalized_pipeline_id,
            version_id=normalized_version_id,
        )

    return jsonify({"success": True, "version": version, "summary": summary})


@app.get("/api/pipeline-versions/<pipeline_id>/<version_id>/execution-history")
def api_pipeline_version_execution_history(pipeline_id: str, version_id: str):
    forbidden = _require_permission("logs_view", "You do not have permission to view run history.")
    if forbidden is not None:
        return forbidden

    normalized_pipeline_id = str(pipeline_id or "").strip()
    normalized_version_id = str(version_id or "").strip()
    version = get_pipeline_version(AppConfig.PIPELINE_VERSIONS_STORE_FILE, normalized_pipeline_id, normalized_version_id)
    if not version:
        return _json_error("Version not found.", 404)

    airflow_dag_id = str(version.get("airflow_dag_id", "")).strip()
    if not airflow_dag_id:
        return _json_error("Version does not contain airflow_dag_id.", 400)

    raw_limit = request.args.get("limit", 50)
    try:
        limit = max(1, min(int(str(raw_limit).strip()), 200))
    except ValueError:
        return _json_error("limit must be an integer.", 400)

    try:
        runs = list_dag_runs_for_dag(_airflow_api_config(), airflow_dag_id, limit=limit)
    except AirflowApiError as exc:
        return _json_error(
            f"Unable to fetch execution history from Airflow: {exc}",
            exc.status_code or 502,
            pipeline_id=normalized_pipeline_id,
            version_id=normalized_version_id,
        )
    except Exception as exc:  # noqa: BLE001
        return _json_error(
            f"Unable to fetch execution history: {exc}",
            500,
            pipeline_id=normalized_pipeline_id,
            version_id=normalized_version_id,
        )

    grouped: dict[str, list[dict[str, Any]]] = {}
    for run in runs:
        start_date = str(run.get("start_date") or run.get("logical_date") or "").strip()
        group_key = start_date.split("T")[0] if "T" in start_date else (start_date or "unknown_date")
        grouped.setdefault(group_key, []).append(run)

    return jsonify(
        {
            "success": True,
            "version": version,
            "pipeline_id": normalized_pipeline_id,
            "version_id": normalized_version_id,
            "airflow_dag_id": airflow_dag_id,
            "runs": runs,
            "grouped_runs": grouped,
        }
    )


@app.get("/api/pipeline-versions/<pipeline_id>/<version_id>/tasks/<task_id>/logs")
def api_pipeline_version_task_logs(pipeline_id: str, version_id: str, task_id: str):
    forbidden = _require_permission("logs_view")
    if forbidden is not None:
        return forbidden

    normalized_pipeline_id = str(pipeline_id or "").strip()
    normalized_version_id = str(version_id or "").strip()
    normalized_task_id = str(task_id or "").strip()
    if not normalized_task_id:
        return _json_error("task_id is required.", 400)

    version = get_pipeline_version(AppConfig.PIPELINE_VERSIONS_STORE_FILE, normalized_pipeline_id, normalized_version_id)
    if not version:
        return _json_error("Version not found.", 404)

    airflow_dag_id = str(version.get("airflow_dag_id", "")).strip()
    if not airflow_dag_id:
        return _json_error("Version does not contain airflow_dag_id.", 400)

    dag_run_id = str(request.args.get("dag_run_id", "")).strip()
    try_number_raw = request.args.get("try_number")
    try:
        try_number = int(str(try_number_raw).strip()) if try_number_raw not in (None, "") else None
    except ValueError:
        return _json_error("try_number must be an integer.", 400)

    try:
        if not dag_run_id:
            runs = list_dag_runs_for_dag(_airflow_api_config(), airflow_dag_id, limit=1)
            if not runs:
                return _json_error("This DAG version has never run.", 404)
            dag_run_id = str((runs[0] or {}).get("dag_run_id", "")).strip()
        if not dag_run_id:
            return _json_error("Unable to resolve dag_run_id for this version.", 404)

        if try_number is None:
            tasks = list_dag_run_task_instances(_airflow_api_config(), airflow_dag_id, dag_run_id)
            matched_task = next((item for item in tasks if str(item.get("task_id", "")).strip() == normalized_task_id), None)
            if not matched_task:
                return _json_error("Task not found in last run.", 404)
            try_number = int(matched_task.get("try_number") or 1)

        payload = get_task_instance_log(
            _airflow_api_config(),
            airflow_dag_id,
            dag_run_id,
            normalized_task_id,
            max(1, try_number),
        )
    except AirflowApiError as exc:
        return _json_error(f"Unable to fetch task logs: {exc}", exc.status_code or 502)
    except Exception as exc:  # noqa: BLE001
        return _json_error(f"Unable to fetch task logs: {exc}", 500)

    return jsonify(
        {
            "success": True,
            "pipeline_id": normalized_pipeline_id,
            "version_id": normalized_version_id,
            "dag_id": airflow_dag_id,
            **payload,
        }
    )


@app.post("/api/workflows/<workflow_id>/pull-requests")
def api_create_workflow_pull_request(workflow_id: str):
    forbidden = _require_permission("pr_publish", "You do not have permission to create pull requests.")
    if forbidden is not None:
        return forbidden

    payload = request.get_json(silent=True) or {}
    workflow_name = str(payload.get("workflow_name", "")).strip()
    pr_description = str(payload.get("description", "") or payload.get("pr_description", "")).strip()
    if not workflow_name:
        return _json_error("workflow_name is required.", 400)

    try:
        version_identifier = select_version_identifier(payload)
        requester = _current_requester_identity()
        result = create_pull_request_for_version(
            pipeline_versions_store_path=AppConfig.PIPELINE_VERSIONS_STORE_FILE,
            publish_store_path=AppConfig.DAG_PUBLISH_STORE_FILE,
            github_client=_github_app_client(),
            pipeline_id=str(workflow_id or "").strip(),
            workflow_name=workflow_name,
            version_identifier=version_identifier,
            pr_description=pr_description,
            requester=requester,
        )
    except PublishError as exc:
        return _json_error(str(exc), exc.status_code, code=exc.code)
    except GitHubClientError as exc:
        return _json_error(f"GitHub API error: {exc}", 502, code="github_api_error")
    except Exception as exc:  # noqa: BLE001
        return _json_error(f"Unable to create pull request: {exc}", 500, code="publish_internal_error")

    return jsonify(
        {
            "success": True,
            "status": result.get("status"),
            "pipeline_id": result.get("pipeline_id"),
            "version_id": result.get("version_id"),
            "pull_request_number": result.get("pull_request_number"),
            "pull_request_url": result.get("pull_request_url"),
            "branch_name": result.get("branch_name"),
            "commit_sha": result.get("commit_sha"),
            "repo_file_path": result.get("repo_file_path"),
            "branch_reused": bool(result.get("branch_reused", False)),
            "requested_by": {
                "user_id": requester.user_id,
                "email": requester.email,
                "display_name": requester.display_name,
                "username": requester.username,
            },
        }
    )


@app.post("/api/github/webhooks")
def api_github_webhooks():
    if not AppConfig.GITHUB_WEBHOOK_SECRET:
        return _json_error("GitHub webhook secret is not configured.", 500)

    body = request.get_data(cache=False, as_text=False) or b""
    signature = str(request.headers.get("X-Hub-Signature-256", "")).strip()
    if not validate_webhook_signature(AppConfig.GITHUB_WEBHOOK_SECRET, body, signature):
        return _json_error("Invalid GitHub webhook signature.", 401)

    event_type = str(request.headers.get("X-GitHub-Event", "")).strip()
    delivery_id = str(request.headers.get("X-GitHub-Delivery", "")).strip()
    payload = request.get_json(silent=True) or {}

    if event_type == "ping":
        return jsonify({"success": True, "event": "ping"})

    if event_type != "pull_request":
        return jsonify({"success": True, "ignored": True, "reason": f"event_not_handled:{event_type}"})

    action = str(payload.get("action", "")).strip()
    if action != "closed":
        return jsonify({"success": True, "ignored": True, "reason": f"action_not_handled:{action}"})

    try:
        result = handle_pull_request_closed_event(
            payload=payload,
            delivery_id=delivery_id,
            publish_store_path=AppConfig.DAG_PUBLISH_STORE_FILE,
            pipeline_versions_store_path=AppConfig.PIPELINE_VERSIONS_STORE_FILE,
            github_client=_github_app_client(),
        )
    except PublishError as exc:
        return _json_error(str(exc), exc.status_code, code=exc.code)
    except GitHubClientError as exc:
        return _json_error(f"GitHub webhook processing failed: {exc}", 502, code="github_api_error")
    except Exception as exc:  # noqa: BLE001
        return _json_error(f"Webhook processing failed: {exc}", 500, code="webhook_internal_error")

    return jsonify({"success": True, **result})


@app.get("/api/generated-dag/<filename>")
def api_generated_dag(filename: str):
    forbidden = _require_permission("builder_view")
    if forbidden is not None:
        return forbidden
    safe_name = _safe_filename(filename, "")
    if not safe_name.endswith(".py"):
        return _json_error("filename must end with .py", 400)

    path = AppConfig.GENERATED_DAGS_DIR / safe_name
    if not path.exists():
        return _json_error("Generated DAG file not found.", 404)

    return jsonify(
        {
            "success": True,
            "filename": safe_name,
            "content": path.read_text(encoding="utf-8"),
            "path": str(path),
        }
    )


def _resolve_flask_bind() -> tuple[str, int]:
    host = str(os.getenv("FLASK_RUN_HOST") or os.getenv("APP_HOST") or "127.0.0.1").strip() or "127.0.0.1"
    raw_port = str(os.getenv("FLASK_RUN_PORT") or os.getenv("APP_PORT") or "8000").strip()
    try:
        start_port = int(raw_port)
    except ValueError:
        start_port = 8000
    start_port = max(1024, min(start_port, 65535))

    candidate_ports: list[int] = []
    for port in [start_port, 8000, 8888, 3000, 4200, 9000, 10000]:
        if 1024 <= port <= 65535 and port not in candidate_ports:
            candidate_ports.append(port)
    for port in range(start_port, min(start_port + 30, 65536)):
        if port not in candidate_ports:
            candidate_ports.append(port)

    for port in candidate_ports:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind((host, port))
            sock.listen(1)
            return host, port
        except OSError:
            continue
        finally:
            sock.close()

    return host, start_port


if __name__ == "__main__":
    # Keep debug mode available, but disable the file-watcher reloader:
    # generating DAG .py files inside the project tree can otherwise trigger a restart
    # mid-request and cause the frontend "Failed to fetch" error.
    run_host, run_port = _resolve_flask_bind()
    print(f"Starting Flask on http://{run_host}:{run_port}")
    app.run(host=run_host, port=run_port, debug=app.config["DEBUG"], use_reloader=False)
