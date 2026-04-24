from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


STATUS_LOCAL_ONLY = "local_only"
STATUS_PR_OPEN = "pr_open"
STATUS_PR_CLOSED_UNMERGED = "pr_closed_unmerged"
STATUS_MERGED_TO_GIT = "merged_to_git"
STATUS_LOCAL_DELETED = "local_deleted"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_publish_store(path: Path) -> dict[str, Any]:
    if not path.exists():
        payload = {"records": [], "processed_webhook_deliveries": []}
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return payload
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        payload = {"records": [], "processed_webhook_deliveries": []}
    if not isinstance(payload, dict):
        payload = {"records": [], "processed_webhook_deliveries": []}
    if not isinstance(payload.get("records"), list):
        payload["records"] = []
    if not isinstance(payload.get("processed_webhook_deliveries"), list):
        payload["processed_webhook_deliveries"] = []
    return payload


def save_publish_store(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _find_record(records: list[dict[str, Any]], *, pipeline_id: str, version_id: str) -> dict[str, Any] | None:
    for item in records:
        if not isinstance(item, dict):
            continue
        if str(item.get("pipeline_id", "")).strip() != pipeline_id:
            continue
        if str(item.get("version_id", "")).strip() != version_id:
            continue
        return item
    return None


def get_record_by_pipeline_version(store_path: Path, *, pipeline_id: str, version_id: str) -> dict[str, Any] | None:
    store = ensure_publish_store(store_path)
    rows = [item for item in store.get("records", []) if isinstance(item, dict)]
    return _find_record(rows, pipeline_id=pipeline_id, version_id=version_id)


def get_record_by_pull_request_number(store_path: Path, *, pull_request_number: int) -> dict[str, Any] | None:
    store = ensure_publish_store(store_path)
    rows = [item for item in store.get("records", []) if isinstance(item, dict)]
    for item in rows:
        if int(item.get("pull_request_number") or 0) == pull_request_number:
            return item
    return None


def upsert_record(store_path: Path, record: dict[str, Any]) -> dict[str, Any]:
    pipeline_id = str(record.get("pipeline_id", "")).strip()
    version_id = str(record.get("version_id", "")).strip()
    if not pipeline_id or not version_id:
        raise ValueError("Publish record must include pipeline_id and version_id.")

    store = ensure_publish_store(store_path)
    rows = [item for item in store.get("records", []) if isinstance(item, dict)]
    existing = _find_record(rows, pipeline_id=pipeline_id, version_id=version_id)
    now = utc_now_iso()

    if existing is None:
        item = dict(record)
        item.setdefault("status", STATUS_LOCAL_ONLY)
        item.setdefault("created_at", now)
        item["updated_at"] = now
        rows.append(item)
        store["records"] = rows
        save_publish_store(store_path, store)
        return item

    existing.update(record)
    existing["updated_at"] = now
    store["records"] = rows
    save_publish_store(store_path, store)
    return existing


def mark_webhook_delivery_processed(store_path: Path, delivery_id: str) -> bool:
    safe_delivery_id = str(delivery_id or "").strip()
    if not safe_delivery_id:
        return False
    store = ensure_publish_store(store_path)
    items = [str(item).strip() for item in store.get("processed_webhook_deliveries", []) if str(item).strip()]
    if safe_delivery_id in items:
        return False
    items.append(safe_delivery_id)
    store["processed_webhook_deliveries"] = items[-2000:]
    save_publish_store(store_path, store)
    return True
