#!/usr/bin/env python3
"""Repair prod champion consistency per base_pipeline_id."""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import sys

SCRIPT_DIR = Path(__file__).resolve().parent
METADATA_DIR = SCRIPT_DIR.parent / "metadata"
if str(METADATA_DIR) not in sys.path:
    sys.path.insert(0, str(METADATA_DIR))

from version_metadata import list_all_versions, save_version_metadata  # type: ignore


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _score(row: dict[str, Any]) -> float:
    try:
        return float(row.get("score") or 0.0)
    except Exception:
        return 0.0


def _has_critical_failures(row: dict[str, Any]) -> bool:
    return bool(list(row.get("critical_failures") or []))


def _to_bool(value: str) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _remove_dag_if_present(root: Path, rel_path: str) -> tuple[bool, str]:
    rel = str(rel_path or "").strip().replace("\\", "/")
    if not rel.startswith("dags/"):
        return False, f"skip invalid dag path: {rel}"
    path = root / rel
    if not path.exists():
        return False, f"dag already absent: {rel}"
    path.unlink()
    return True, f"removed dag: {rel}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Repair prod champion metadata state")
    parser.add_argument("--root", type=Path, required=True, help="Prod worktree root")
    parser.add_argument(
        "--rollback-retention-enabled",
        type=str,
        default="true",
        help="Keep non-champion DAG files when true",
    )
    args = parser.parse_args()

    root = args.root.resolve()
    keep_old_dags = _to_bool(args.rollback_retention_enabled)

    rows = list_all_versions(root)
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        base = str(row.get("base_pipeline_id") or row.get("pipeline_id") or "").strip()
        if not base:
            continue
        groups[base].append(row)

    champion_updates = 0
    archived_updates = 0
    removed_dags = 0
    for base_id, versions in sorted(groups.items()):
        champions = [
            row
            for row in versions
            if str(row.get("promotion_status") or "").strip().lower() == "champion"
        ]
        if not champions:
            continue

        valid = [row for row in champions if not _has_critical_failures(row)]
        pool = valid if valid else champions
        selected = max(pool, key=_score)
        selected_key = (
            str(selected.get("pipeline_id") or "").strip(),
            str(selected.get("version_id") or "").strip(),
        )
        print(
            f"base_pipeline_id={base_id} selected_champion={selected_key[0]}/{selected_key[1]} "
            f"score={_score(selected)} valid_pool={len(valid)} total_champions={len(champions)}"
        )

        for row in champions:
            pipeline_id = str(row.get("pipeline_id") or "").strip()
            version_id = str(row.get("version_id") or "").strip()
            if not pipeline_id or not version_id:
                continue
            key = (pipeline_id, version_id)
            if key == selected_key:
                if str(row.get("promotion_status") or "").strip().lower() != "champion":
                    row["promotion_status"] = "champion"
                    champion_updates += 1
                row["updated_at"] = _now_iso()
                save_version_metadata(root, pipeline_id, version_id, row)
                continue

            row["promotion_status"] = "archived"
            row["archived_at"] = _now_iso()
            row["updated_at"] = _now_iso()
            save_version_metadata(root, pipeline_id, version_id, row)
            archived_updates += 1

        if keep_old_dags:
            continue
        for row in versions:
            pipeline_id = str(row.get("pipeline_id") or "").strip()
            version_id = str(row.get("version_id") or "").strip()
            key = (pipeline_id, version_id)
            if key == selected_key:
                continue
            changed, msg = _remove_dag_if_present(root, str(row.get("git_dag_file") or ""))
            print(msg)
            if changed:
                removed_dags += 1

    print(
        f"repair_summary champion_updates={champion_updates} "
        f"archived_updates={archived_updates} removed_dags={removed_dags} "
        f"rollback_retention_enabled={keep_old_dags}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
