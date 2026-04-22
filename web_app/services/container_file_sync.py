from __future__ import annotations

import os
import posixpath
import shlex
import subprocess
from pathlib import Path
from typing import Any


class ContainerFileSyncError(RuntimeError):
    pass


def _run_command(args: list[str]) -> None:
    try:
        result = subprocess.run(args, capture_output=True, text=True, check=False)
    except FileNotFoundError as exc:
        raise ContainerFileSyncError(
            "Docker CLI not found. Install Docker and ensure 'docker' is available in PATH."
        ) from exc

    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        stdout = (result.stdout or "").strip()
        details = stderr or stdout or f"exit_code={result.returncode}"
        raise ContainerFileSyncError(f"Command failed ({' '.join(args)}): {details}")


def _ensure_container_dir(container_name: str, container_dir: str) -> None:
    mkdir_cmd = f"mkdir -p {shlex.quote(container_dir)}"
    _run_command(["docker", "exec", container_name, "sh", "-lc", mkdir_cmd])


def _copy_file_to_container(host_file: Path, container_name: str, container_file_path: str) -> None:
    _run_command(["docker", "cp", str(host_file), f"{container_name}:{container_file_path}"])


def _safe_name(raw: str) -> str:
    allowed = []
    for ch in str(raw or ""):
        if ch.isalnum() or ch in {"_", "-", "."}:
            allowed.append(ch)
        else:
            allowed.append("_")
    value = "".join(allowed).strip("._")
    return value or "item"


def _normalize_container_path(value: str) -> str:
    text = str(value or "").strip().replace("\\", "/")
    while "//" in text:
        text = text.replace("//", "/")
    return text


def _is_container_managed_path(value: str, container_base_dir: str) -> bool:
    candidate = _normalize_container_path(value)
    base = _normalize_container_path(container_base_dir).rstrip("/")
    if not candidate or not base:
        return False
    if not candidate.startswith("/"):
        return False
    return candidate == base or candidate.startswith(base + "/")


def sync_host_file_to_containers(
    *,
    host_file: Path,
    pipeline_id: str,
    node_id: str,
    target_name: str | None,
    enabled: bool,
    containers: list[str],
    container_base_dir: str,
) -> str:
    if not host_file.exists() or not host_file.is_file():
        raise ContainerFileSyncError(f"Local file not found: {host_file}")

    safe_pipeline = _safe_name(pipeline_id or "pipeline")
    safe_node = _safe_name(node_id or "node")
    safe_name = _safe_name(target_name or host_file.name)
    container_dir = posixpath.join(container_base_dir.rstrip("/"), safe_pipeline, safe_node)
    container_file_path = posixpath.join(container_dir, safe_name)

    if not enabled:
        return container_file_path
    if not containers:
        raise ContainerFileSyncError("No target containers configured for Docker sync.")

    for container in containers:
        _ensure_container_dir(container, container_dir)
        _copy_file_to_container(host_file, container, container_file_path)

    return container_file_path


def sync_host_file_to_containers_at_path(
    *,
    host_file: Path,
    enabled: bool,
    containers: list[str],
    container_file_path: str,
) -> str:
    if not host_file.exists() or not host_file.is_file():
        raise ContainerFileSyncError(f"Local file not found: {host_file}")

    normalized_target = _normalize_container_path(container_file_path)
    if not normalized_target.startswith("/"):
        raise ContainerFileSyncError("container_file_path must be an absolute container path.")

    if not enabled:
        return normalized_target
    if not containers:
        raise ContainerFileSyncError("No target containers configured for Docker sync.")

    container_dir = posixpath.dirname(normalized_target.rstrip("/")) or "/"
    for container in containers:
        _ensure_container_dir(container, container_dir)
        _copy_file_to_container(host_file, container, normalized_target)

    return normalized_target


def _collect_directory_files(directory_path: str) -> list[Path]:
    path = Path(str(directory_path or "").strip())
    if not path.exists() or not path.is_dir():
        raise ContainerFileSyncError(f"Local directory not found: {path}")
    return sorted(item for item in path.iterdir() if item.is_file())


def sync_gcs_local_files_to_containers(
    payload: dict[str, Any],
    *,
    enabled: bool,
    containers: list[str],
    container_base_dir: str,
) -> dict[str, Any]:
    if not enabled:
        return {"enabled": False, "copied_files": [], "updated_nodes": []}
    if not containers:
        return {"enabled": False, "copied_files": [], "updated_nodes": []}

    nodes = payload.get("nodes")
    pipeline = payload.get("pipeline") or {}
    if not isinstance(nodes, list):
        return {"enabled": True, "copied_files": [], "updated_nodes": []}

    dag_id = _safe_name(str(pipeline.get("dag_id", "")).strip() or "pipeline")
    copied_files: list[dict[str, str]] = []
    updated_nodes: list[str] = []

    for node in nodes:
        if not isinstance(node, dict):
            continue
        if str(node.get("type", "")).strip().lower() != "gcs":
            continue
        config = node.get("config")
        if not isinstance(config, dict):
            continue

        node_id = _safe_name(str(node.get("id", "")).strip() or "node")
        action = str(config.get("action", "upload")).strip().lower()
        if action != "upload":
            continue

        upload_mode = str(config.get("upload_mode", "single")).strip().lower()
        source_type = str(config.get("source_type", "local")).strip().lower()
        multiple_source_type = str(config.get("multiple_source_type", source_type)).strip().lower()
        effective_source_type = source_type if upload_mode == "single" else multiple_source_type

        node_container_dir = posixpath.join(container_base_dir.rstrip("/"), dag_id, node_id)
        upload_container_dir = posixpath.join(node_container_dir, "upload")
        auth_container_dir = posixpath.join(node_container_dir, "auth")

        # Service account file is required in container for runtime auth.
        sa_file = str(config.get("service_account_file", "")).strip()
        if sa_file:
            if _is_container_managed_path(sa_file, container_base_dir):
                config["service_account_file"] = _normalize_container_path(sa_file)
            else:
                sa_host_path = Path(sa_file)
                if sa_host_path.exists() and sa_host_path.is_file():
                    sa_container_path = posixpath.join(auth_container_dir, _safe_name(sa_host_path.name))
                    for container in containers:
                        _ensure_container_dir(container, auth_container_dir)
                        _copy_file_to_container(sa_host_path, container, sa_container_path)
                    copied_files.append({"source": str(sa_host_path), "target": sa_container_path, "kind": "service_account"})
                    config["service_account_file"] = sa_container_path

        if effective_source_type != "local":
            continue

        host_files: list[Path] = []
        already_container_files: list[str] = []
        if upload_mode == "single":
            raw_path = str(config.get("local_file_path", "")).strip()
            if _is_container_managed_path(raw_path, container_base_dir):
                already_container_files = [_normalize_container_path(raw_path)]
            else:
                host_path = Path(raw_path)
                if not host_path.exists() or not host_path.is_file():
                    raise ContainerFileSyncError(f"Local file not found: {host_path}")
                host_files = [host_path]
        else:
            explicit_raw = [str(item).strip() for item in (config.get("local_file_paths") or []) if str(item).strip()]
            explicit_container = [
                _normalize_container_path(item)
                for item in explicit_raw
                if _is_container_managed_path(item, container_base_dir)
            ]
            explicit_host = [
                Path(item)
                for item in explicit_raw
                if not _is_container_managed_path(item, container_base_dir)
            ]
            if explicit_host:
                for item in explicit_host:
                    if not item.exists() or not item.is_file():
                        raise ContainerFileSyncError(f"Local file not found: {item}")
                host_files = explicit_host
                already_container_files = explicit_container
            elif explicit_container:
                already_container_files = explicit_container
            else:
                local_dir = str(config.get("local_directory_path", "")).strip()
                if _is_container_managed_path(local_dir, container_base_dir):
                    config["local_directory_path"] = _normalize_container_path(local_dir)
                    already_container_files = []
                    host_files = []
                else:
                    if not local_dir:
                        raise ContainerFileSyncError(
                            "Local upload expects local_file_path/local_file_paths/local_directory_path."
                        )
                    host_files = _collect_directory_files(local_dir)
                    if not host_files:
                        raise ContainerFileSyncError(f"No files found in local directory: {local_dir}")

        container_file_paths: list[str] = []
        container_file_paths.extend(already_container_files)
        for host_file in host_files:
            container_file_path = posixpath.join(upload_container_dir, _safe_name(host_file.name))
            for container in containers:
                _ensure_container_dir(container, upload_container_dir)
                _copy_file_to_container(host_file, container, container_file_path)
            copied_files.append({"source": str(host_file), "target": container_file_path, "kind": "upload"})
            container_file_paths.append(container_file_path)

        if upload_mode == "single":
            config["local_file_path"] = container_file_paths[0]
        else:
            config["local_file_paths"] = container_file_paths
            config["local_directory_path"] = ""

        updated_nodes.append(str(node.get("id", "")).strip() or node_id)

    return {
        "enabled": True,
        "copied_files": copied_files,
        "updated_nodes": updated_nodes,
    }
