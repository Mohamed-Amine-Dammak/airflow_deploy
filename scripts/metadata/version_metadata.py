#!/usr/bin/env python3
"""Per-version metadata storage helpers."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


METADATA_DIR = Path("airflow") / "web_app_data" / "metadata"
INDEX_FILE = METADATA_DIR / "index.json"


def metadata_path(root: Path, pipeline_id: str, version_id: str) -> Path:
    return root / METADATA_DIR / str(pipeline_id).strip() / f"{str(version_id).strip()}.json"


def _diff_marker_present(text: str) -> bool:
    bad_markers = ("@@", "diff --git", "+++", "---")
    return any(marker in text for marker in bad_markers)


def validate_metadata_file(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if _diff_marker_present(text):
        raise ValueError(f"Rejected diff markers in metadata file: {path}")
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError(f"Metadata file must contain a JSON object: {path}")
    required = {
        "pipeline_id",
        "version_id",
        "base_pipeline_id",
        "local_dag_id",
        "git_dag_id",
        "local_dag_file",
        "git_dag_file",
        "promotion_status",
    }
    missing = [key for key in sorted(required) if key not in payload]
    if missing:
        raise ValueError(f"Missing required fields in {path}: {', '.join(missing)}")
    local_stem = Path(str(payload["local_dag_file"])).name.replace(".py", "")
    git_stem = Path(str(payload["git_dag_file"])).name.replace(".py", "")
    if local_stem != str(payload["local_dag_id"]):
        raise ValueError(f"local_dag_file/local_dag_id mismatch in {path}")
    if git_stem != str(payload["git_dag_id"]):
        raise ValueError(f"git_dag_file/git_dag_id mismatch in {path}")
    return payload


def atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    # Validate written JSON and content before replacing.
    validate_metadata_file(tmp_path)
    os.replace(tmp_path, path)


def save_version_metadata(root: Path, pipeline_id: str, version_id: str, data: dict[str, Any]) -> Path:
    path = metadata_path(root, pipeline_id, version_id)
    atomic_write_json(path, data)
    return path


def load_version_metadata(root: Path, pipeline_id: str, version_id: str) -> dict[str, Any]:
    path = metadata_path(root, pipeline_id, version_id)
    if not path.exists():
        raise FileNotFoundError(str(path))
    return validate_metadata_file(path)


def list_pipeline_versions(root: Path, pipeline_id: str) -> list[dict[str, Any]]:
    pdir = root / METADATA_DIR / str(pipeline_id).strip()
    if not pdir.exists():
        return []
    rows: list[dict[str, Any]] = []
    for path in sorted(pdir.glob("*.json")):
        try:
            rows.append(validate_metadata_file(path))
        except Exception:
            continue
    return rows


def list_all_versions(root: Path) -> list[dict[str, Any]]:
    mdir = root / METADATA_DIR
    if not mdir.exists():
        return []
    rows: list[dict[str, Any]] = []
    for path in sorted(mdir.glob("*/*.json")):
        try:
            rows.append(validate_metadata_file(path))
        except Exception:
            continue
    return rows


def find_version_metadata(root: Path, pipeline_id: str, dag_id_or_version_id: str) -> dict[str, Any] | None:
    key = str(dag_id_or_version_id or "").strip()
    for item in list_pipeline_versions(root, pipeline_id):
        candidates = {
            str(item.get("version_id", "")).strip(),
            str(item.get("local_dag_id", "")).strip(),
            str(item.get("git_dag_id", "")).strip(),
            str(item.get("dag_id", "")).strip(),
            str(item.get("airflow_dag_id", "")).strip(),
        }
        if key in candidates:
            return item
    return None
