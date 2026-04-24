from __future__ import annotations

import copy
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_store(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"pipelines": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        payload = {"pipelines": {}}
    if not isinstance(payload, dict):
        payload = {"pipelines": {}}
    if not isinstance(payload.get("pipelines"), dict):
        payload["pipelines"] = {}
    return payload


def _save_store(path: Path, store: dict[str, Any]) -> None:
    path.write_text(json.dumps(store, indent=2), encoding="utf-8")


def _pipeline_entry(store: dict[str, Any], pipeline_id: str) -> dict[str, Any]:
    pipelines = store.setdefault("pipelines", {})
    entry = pipelines.setdefault(
        pipeline_id,
        {
            "current_version_id": None,
            "versions": [],
            "switch_events": [],
        },
    )
    if not isinstance(entry.get("versions"), list):
        entry["versions"] = []
    if not isinstance(entry.get("switch_events"), list):
        entry["switch_events"] = []
    return entry


def _source_hash(source_definition: dict[str, Any]) -> str:
    payload = json.dumps(source_definition, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _normalize_version(version: dict[str, Any], current_version_id: str | None) -> dict[str, Any]:
    item = copy.deepcopy(version)
    item["is_current"] = bool(item.get("version_id")) and item.get("version_id") == current_version_id
    return item


def plan_next_version(store_path: Path, pipeline_id: str) -> dict[str, Any]:
    store = _load_store(store_path)
    entry = _pipeline_entry(store, pipeline_id)
    max_seen = 0
    for version in entry.get("versions", []):
        try:
            max_seen = max(max_seen, int(version.get("dag_version", 0)))
        except (TypeError, ValueError):
            continue
    dag_version = max_seen + 1
    version_id = f"v{dag_version}"
    airflow_dag_id = f"{pipeline_id}__{version_id}"
    output_filename = f"{airflow_dag_id}.py"
    return {
        "pipeline_id": pipeline_id,
        "dag_version": dag_version,
        "version_id": version_id,
        "airflow_dag_id": airflow_dag_id,
        "output_filename": output_filename,
    }


def register_version(
    store_path: Path,
    *,
    pipeline_id: str,
    dag_version: int,
    version_id: str,
    airflow_dag_id: str,
    output_filename: str,
    source_definition: dict[str, Any],
    generated_local_path: str,
    generated_airflow_path: str,
    node_task_map: dict[str, list[str]] | dict[str, Any] | None,
    node_primary_task_map: dict[str, str] | dict[str, Any] | None,
    created_by: str | None,
) -> dict[str, Any]:
    store = _load_store(store_path)
    entry = _pipeline_entry(store, pipeline_id)
    created_at = _now_iso()
    current_before = entry.get("current_version_id")

    version_record = {
        "pipeline_id": pipeline_id,
        "version_id": version_id,
        "dag_version": dag_version,
        "airflow_dag_id": airflow_dag_id,
        "output_filename": output_filename,
        "created_at": created_at,
        "created_by": created_by or "",
        "status": "active",
        "is_current": True,
        "source_hash": _source_hash(source_definition),
        "source_definition": source_definition,
        "generated_local_path": generated_local_path,
        "generated_airflow_path": generated_airflow_path,
        "node_task_map": node_task_map or {},
        "node_primary_task_map": node_primary_task_map or {},
    }

    for item in entry["versions"]:
        item["is_current"] = False
        if str(item.get("status", "")).strip().lower() == "active":
            item["status"] = "archived"

    entry["versions"].append(version_record)
    entry["current_version_id"] = version_id
    entry["switch_events"].append(
        {
            "event_id": str(uuid4()),
            "event_type": "create_and_set_current",
            "from_version_id": current_before,
            "to_version_id": version_id,
            "actor": created_by or "",
            "at": created_at,
        }
    )
    _save_store(store_path, store)
    return _normalize_version(version_record, version_id)


def list_versions(store_path: Path, pipeline_id: str) -> dict[str, Any]:
    store = _load_store(store_path)
    entry = _pipeline_entry(store, pipeline_id)
    current_version_id = entry.get("current_version_id")
    versions = [_normalize_version(item, current_version_id) for item in entry.get("versions", []) if isinstance(item, dict)]
    versions.sort(key=lambda item: int(item.get("dag_version", 0)), reverse=True)
    return {
        "pipeline_id": pipeline_id,
        "current_version_id": current_version_id,
        "versions": versions,
        "switch_events": list(entry.get("switch_events", [])),
    }


def get_version(store_path: Path, pipeline_id: str, version_id: str) -> dict[str, Any] | None:
    listing = list_versions(store_path, pipeline_id)
    for item in listing["versions"]:
        if str(item.get("version_id")) == version_id:
            return item
    return None


def find_matching_version(store_path: Path, pipeline_id: str, source_definition: dict[str, Any]) -> dict[str, Any] | None:
    listing = list_versions(store_path, pipeline_id)
    target_hash = _source_hash(source_definition)
    versions = listing.get("versions") or []
    for item in versions:
        if str(item.get("source_hash", "")).strip() == target_hash:
            return item
    return None


def set_current_version(
    store_path: Path,
    *,
    pipeline_id: str,
    version_id: str,
    actor: str | None,
) -> dict[str, Any]:
    store = _load_store(store_path)
    entry = _pipeline_entry(store, pipeline_id)
    versions = [item for item in entry.get("versions", []) if isinstance(item, dict)]
    matched = next((item for item in versions if str(item.get("version_id")) == version_id), None)
    if not matched:
        raise ValueError(f"Version '{version_id}' not found for pipeline '{pipeline_id}'.")

    previous = entry.get("current_version_id")
    if previous == version_id:
        result = _normalize_version(matched, previous)
        return {
            "changed": False,
            "pipeline_id": pipeline_id,
            "current_version_id": previous,
            "version": result,
        }

    now = _now_iso()
    for item in versions:
        item["is_current"] = str(item.get("version_id")) == version_id
        item["status"] = "active" if item["is_current"] else "archived"
    entry["current_version_id"] = version_id
    entry["switch_events"].append(
        {
            "event_id": str(uuid4()),
            "event_type": "switch_current",
            "from_version_id": previous,
            "to_version_id": version_id,
            "actor": actor or "",
            "at": now,
        }
    )
    _save_store(store_path, store)

    return {
        "changed": True,
        "pipeline_id": pipeline_id,
        "current_version_id": version_id,
        "version": _normalize_version(matched, version_id),
    }


def update_version_definition(
    store_path: Path,
    *,
    pipeline_id: str,
    version_id: str,
    source_definition: dict[str, Any],
    generated_local_path: str,
    generated_airflow_path: str,
    node_task_map: dict[str, list[str]] | dict[str, Any] | None,
    node_primary_task_map: dict[str, str] | dict[str, Any] | None,
    actor: str | None,
) -> dict[str, Any]:
    store = _load_store(store_path)
    entry = _pipeline_entry(store, pipeline_id)
    current_version_id = str(entry.get("current_version_id") or "").strip()
    versions = [item for item in entry.get("versions", []) if isinstance(item, dict)]
    matched = next((item for item in versions if str(item.get("version_id", "")).strip() == version_id), None)
    if not matched:
        raise ValueError(f"Version '{version_id}' not found for pipeline '{pipeline_id}'.")

    now = _now_iso()
    matched["source_definition"] = source_definition
    matched["source_hash"] = _source_hash(source_definition)
    matched["generated_local_path"] = generated_local_path
    matched["generated_airflow_path"] = generated_airflow_path
    matched["node_task_map"] = node_task_map or {}
    matched["node_primary_task_map"] = node_primary_task_map or {}
    matched["updated_at"] = now
    if actor:
        matched["updated_by"] = actor

    entry["switch_events"].append(
        {
            "event_id": str(uuid4()),
            "event_type": "update_version_definition",
            "from_version_id": version_id,
            "to_version_id": version_id,
            "actor": actor or "",
            "at": now,
        }
    )
    _save_store(store_path, store)
    return _normalize_version(matched, current_version_id or version_id)


def clear_versions(store_path: Path, pipeline_id: str) -> dict[str, Any]:
    store = _load_store(store_path)
    pipelines = store.setdefault("pipelines", {})
    existing = pipelines.get(pipeline_id)
    if not isinstance(existing, dict):
        return {
            "pipeline_id": pipeline_id,
            "removed": False,
            "removed_versions": [],
            "removed_versions_count": 0,
        }

    removed_versions = [
        _normalize_version(item, str(existing.get("current_version_id") or ""))
        for item in existing.get("versions", [])
        if isinstance(item, dict)
    ]
    pipelines.pop(pipeline_id, None)
    _save_store(store_path, store)
    return {
        "pipeline_id": pipeline_id,
        "removed": True,
        "removed_versions": removed_versions,
        "removed_versions_count": len(removed_versions),
    }


def patch_version_fields(
    store_path: Path,
    *,
    pipeline_id: str,
    version_id: str,
    patch: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(patch, dict) or not patch:
        raise ValueError("patch must be a non-empty dictionary.")

    store = _load_store(store_path)
    entry = _pipeline_entry(store, pipeline_id)
    current_version_id = str(entry.get("current_version_id") or "").strip()
    versions = [item for item in entry.get("versions", []) if isinstance(item, dict)]
    matched = next((item for item in versions if str(item.get("version_id", "")).strip() == version_id), None)
    if not matched:
        raise ValueError(f"Version '{version_id}' not found for pipeline '{pipeline_id}'.")

    matched.update(copy.deepcopy(patch))
    matched["updated_at"] = _now_iso()
    _save_store(store_path, store)
    return _normalize_version(matched, current_version_id)
