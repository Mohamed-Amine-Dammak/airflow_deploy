#!/usr/bin/env python3
"""Promote changed metadata entries to eval status."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
METADATA_DIR = SCRIPT_DIR.parent / "metadata"
if str(METADATA_DIR) not in sys.path:
    sys.path.insert(0, str(METADATA_DIR))

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def discover_changed_metadata_files(root: Path) -> list[str]:
    cmd = ["git", "-C", str(root), "diff", "--name-only", "HEAD~1", "HEAD"]
    proc = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if proc.returncode != 0:
        return []
    files: list[str] = []
    for raw in proc.stdout.splitlines():
        path = str(raw or "").strip().replace("\\", "/")
        if path.startswith("airflow/web_app_data/metadata/") and path.endswith(".json"):
            files.append(path)
    return files


def _is_skippable_status(promotion_status: str) -> bool:
    return promotion_status in {"champion", "archived"}


def _is_eligible(publish_status: str, promotion_status: str) -> bool:
    if publish_status == "merged_to_git":
        return True
    if publish_status == "pr_open":
        return True
    if promotion_status == "draft" and publish_status in {"pr_open", "merged_to_git"}:
        return True
    return promotion_status == "submitted"


def process_metadata_file(root: Path, rel_path: str) -> tuple[bool, str]:
    path = root / rel_path
    if not path.exists():
        return False, f"missing metadata file: {rel_path}"
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return False, f"invalid metadata payload: {rel_path}"

    pipeline_id = str(payload.get("pipeline_id", "")).strip()
    version_id = str(payload.get("version_id", "")).strip()
    local_dag_id = str(payload.get("local_dag_id", "")).strip()
    git_dag_id = str(payload.get("git_dag_id", "")).strip()
    git_dag_file = str(payload.get("git_dag_file", "")).strip()
    publish_status = str(payload.get("publish_status", "")).strip().lower()
    promotion_status = str(payload.get("promotion_status", "")).strip().lower()

    print("Promoting:")
    print(f"pipeline_id={pipeline_id}")
    print(f"version_id={version_id}")
    print(f"git_dag_id={git_dag_id}")
    print(f"git_dag_file={git_dag_file}")
    print(f"Before update: promotion_status={promotion_status}, publish_status={publish_status}")

    if _is_skippable_status(promotion_status):
        return False, f"skipped {rel_path}: promotion_status={promotion_status}"
    if not _is_eligible(publish_status, promotion_status):
        return False, (
            f"skipped {rel_path}: publish_status={publish_status}, "
            f"promotion_status={promotion_status}"
        )
    if git_dag_file and not (root / git_dag_file).exists():
        return False, f"missing DAG file for metadata: {git_dag_file}"

    mark_cmd = [
        sys.executable,
        str(METADATA_DIR / "update_pipeline_metadata.py"),
        "mark_eval_file",
        "--root",
        str(root),
        "--metadata-file",
        rel_path,
    ]
    proc = subprocess.run(mark_cmd, check=False, capture_output=True, text=True)
    if proc.returncode != 0:
        return False, f"failed to update {rel_path}: {proc.stdout}{proc.stderr}"
    payload = json.loads(path.read_text(encoding="utf-8"))
    print(
        "After update: "
        f"promotion_status={str(payload.get('promotion_status', '')).strip().lower()}, "
        f"publish_status={str(payload.get('publish_status', '')).strip().lower()}"
    )
    return True, f"updated {rel_path}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Promote changed metadata entries to eval")
    parser.add_argument("--root", type=Path, required=True, help="Repository root")
    parser.add_argument(
        "--metadata-file",
        action="append",
        default=[],
        help="Repo-relative metadata JSON file path (repeatable)",
    )
    args = parser.parse_args()

    root = args.root.resolve()
    metadata_files = [str(p).strip().replace("\\", "/") for p in (args.metadata_file or []) if str(p).strip()]
    if not metadata_files:
        metadata_files = discover_changed_metadata_files(root)

    if not metadata_files:
        print("Detected metadata files:")
        print("- <none>")
        return 0

    print("Detected metadata files:")
    for rel in metadata_files:
        print(f"- {rel}")

    updated = 0
    failures = 0
    for rel in metadata_files:
        changed, message = process_metadata_file(root, rel)
        print(message)
        if message.startswith("failed to update"):
            failures += 1
        if changed:
            updated += 1

    if failures:
        return 1

    validate_cmd = [
        sys.executable,
        str(METADATA_DIR / "validate_metadata.py"),
        "--root",
        str(root),
    ]
    validate_proc = subprocess.run(validate_cmd, check=False)
    if validate_proc.returncode != 0:
        return validate_proc.returncode
    print(f"Promotion update complete. Updated {updated} metadata file(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
