#!/usr/bin/env python3
"""Migrate legacy monolithic pipeline store to per-version metadata files."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from version_metadata import INDEX_FILE, METADATA_DIR, save_version_metadata


def _inject_git_marker_before_version(name: str) -> str:
    base = str(name or "").strip()
    if not base:
        return ""
    if "_git__" in base or base.endswith("_git"):
        return base
    if "__v" in base:
        left, right = base.rsplit("__v", 1)
        return f"{left}_git__v{right}"
    return f"{base}_git"


def _normalize_version(pipeline_id: str, version: dict[str, Any]) -> dict[str, Any]:
    version_id = str(version.get("version_id", "")).strip()
    local_dag_id = str(version.get("local_dag_id") or version.get("airflow_dag_id") or version.get("dag_id") or "").strip()
    git_dag_id = str(version.get("git_dag_id") or _inject_git_marker_before_version(local_dag_id)).strip()
    local_dag_file = str(version.get("local_dag_file") or f"airflow/dags/{local_dag_id}.py").strip()
    git_dag_file = str(version.get("git_dag_file") or version.get("repo_file_path") or f"dags/{git_dag_id}.py").strip()
    out = dict(version)
    out["pipeline_id"] = pipeline_id
    out["version_id"] = version_id
    out["base_pipeline_id"] = str(version.get("base_pipeline_id") or pipeline_id).strip()
    out["local_dag_id"] = local_dag_id
    out["local_dag_file"] = local_dag_file
    out["git_dag_id"] = git_dag_id
    out["git_dag_file"] = git_dag_file
    out["deployment_target"] = str(version.get("deployment_target") or "local").strip().lower()
    if out["deployment_target"] not in {"local", "git"}:
        out["deployment_target"] = "git"
    # Backward compatibility aliases
    out["airflow_dag_id"] = local_dag_id
    out["dag_id"] = local_dag_id
    out["repo_file_path"] = git_dag_file
    if "warnings" not in out:
        out["warnings"] = []
    if "critical_failures" not in out:
        out["critical_failures"] = []
    if "score_breakdown" not in out:
        out["score_breakdown"] = {}
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrate pipeline_versions_store.json to per-version metadata files")
    parser.add_argument("--root", type=Path, default=Path("."), help="Repository root")
    args = parser.parse_args()

    root = args.root.resolve()
    legacy = root / "airflow" / "web_app_data" / "pipeline_versions_store.json"
    if not legacy.exists():
        print(f"No legacy store found at {legacy}; nothing to migrate.")
        return 0

    payload = json.loads(legacy.read_text(encoding="utf-8"))
    pipelines = payload.get("pipelines", {}) if isinstance(payload, dict) else {}
    if not isinstance(pipelines, dict):
        pipelines = {}

    created = 0
    index_rows: list[dict[str, str]] = []
    for pipeline_id, entry in pipelines.items():
        versions = entry.get("versions", []) if isinstance(entry, dict) else []
        if not isinstance(versions, list):
            continue
        for version in versions:
            if not isinstance(version, dict):
                continue
            version_id = str(version.get("version_id", "")).strip()
            if not version_id:
                continue
            normalized = _normalize_version(str(pipeline_id).strip(), version)
            save_version_metadata(root, str(pipeline_id).strip(), version_id, normalized)
            created += 1
            index_rows.append(
                {
                    "pipeline_id": str(pipeline_id).strip(),
                    "version_id": version_id,
                    "path": str((METADATA_DIR / str(pipeline_id).strip() / f"{version_id}.json").as_posix()),
                }
            )

    index_path = root / INDEX_FILE
    index_path.parent.mkdir(parents=True, exist_ok=True)
    index_path.write_text(json.dumps({"versions": index_rows}, indent=2), encoding="utf-8")
    print(f"Migrated {created} version(s) into {root / METADATA_DIR}")
    print(f"Index written: {index_path}")
    print(f"Legacy store retained: {legacy}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
