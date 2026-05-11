#!/usr/bin/env python3
"""Update per-version DAG metadata files."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from version_metadata import (
    find_version_metadata,
    list_all_versions,
    list_pipeline_versions,
    load_version_metadata,
    metadata_path,
    save_version_metadata,
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _inject_git_marker_before_version(name: str) -> str:
    base = str(name or "").strip()
    if not base:
        return ""
    if "_git__" in base:
        return base
    if "__v" in base:
        left, right = base.rsplit("__v", 1)
        return f"{left}_git__v{right}"
    return f"{base}_git"


def _legacy_store(root: Path) -> Path:
    return root / "airflow" / "web_app_data" / "pipeline_versions_store.json"


def _load_legacy_version(root: Path, pipeline_id: str, version_id: str | None = None, dag_id: str | None = None) -> dict[str, Any] | None:
    path = _legacy_store(root)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    pipelines = payload.get("pipelines", {}) if isinstance(payload, dict) else {}
    entry = pipelines.get(str(pipeline_id).strip(), {}) if isinstance(pipelines, dict) else {}
    versions = entry.get("versions", []) if isinstance(entry, dict) else []
    for version in versions if isinstance(versions, list) else []:
        if not isinstance(version, dict):
            continue
        if version_id and str(version.get("version_id", "")).strip() == version_id:
            return version
        if dag_id and dag_id in {
            str(version.get("local_dag_id", "")).strip(),
            str(version.get("git_dag_id", "")).strip(),
            str(version.get("airflow_dag_id", "")).strip(),
            str(version.get("dag_id", "")).strip(),
        }:
            return version
    return None


def _normalize_version(pipeline_id: str, version_id: str, version: dict[str, Any], dag_id: str | None = None, dag_file: str | None = None) -> dict[str, Any]:
    local_dag_id = str(version.get("local_dag_id") or version.get("airflow_dag_id") or version.get("dag_id") or dag_id or "").strip()
    git_dag_file = str(version.get("git_dag_file") or version.get("repo_file_path") or dag_file or "").strip()
    if not git_dag_file:
        git_dag_id_seed = str(version.get("git_dag_id") or _inject_git_marker_before_version(local_dag_id)).strip()
        git_dag_file = f"dags/{git_dag_id_seed}.py" if git_dag_id_seed else ""
    git_dag_id = str(version.get("git_dag_id") or Path(git_dag_file).stem or _inject_git_marker_before_version(local_dag_id)).strip()
    local_dag_file = str(version.get("local_dag_file") or f"airflow/dags/{local_dag_id}.py").strip()
    deployment_target = str(version.get("deployment_target") or "local").strip().lower()
    if deployment_target not in {"local", "git"}:
        deployment_target = "git"
    out = dict(version)
    out.update(
        {
            "pipeline_id": pipeline_id,
            "version_id": version_id,
            "base_pipeline_id": str(version.get("base_pipeline_id") or pipeline_id).strip(),
            "local_dag_id": local_dag_id,
            "local_dag_file": local_dag_file,
            "git_dag_id": git_dag_id,
            "git_dag_file": git_dag_file,
            "deployment_target": deployment_target,
            "airflow_dag_id": local_dag_id,
            "dag_id": local_dag_id,
            "repo_file_path": git_dag_file,
            "output_filename": str(version.get("output_filename") or Path(local_dag_file).name),
            "warnings": list(version.get("warnings") or []),
            "critical_failures": list(version.get("critical_failures") or []),
            "score_breakdown": dict(version.get("score_breakdown") or {}),
            "updated_at": _now_iso(),
        }
    )
    out["created_at"] = str(version.get("created_at") or _now_iso())
    return out


def _load_or_create(root: Path, pipeline_id: str, version_id: str, dag_id: str | None = None, dag_file: str | None = None) -> dict[str, Any]:
    try:
        existing = load_version_metadata(root, pipeline_id, version_id)
        return _normalize_version(pipeline_id, version_id, existing, dag_id=dag_id, dag_file=dag_file)
    except FileNotFoundError:
        legacy = _load_legacy_version(root, pipeline_id, version_id=version_id, dag_id=dag_id) or {}
        return _normalize_version(pipeline_id, version_id, legacy, dag_id=dag_id, dag_file=dag_file)


def _write(root: Path, version: dict[str, Any]) -> Path:
    return save_version_metadata(root, str(version["pipeline_id"]), str(version["version_id"]), version)


def _find_for_merge(root: Path, pipeline_id: str | None, version_id: str | None, dag_id: str | None, pr_number: int | None, branch: str | None) -> dict[str, Any] | None:
    if pipeline_id and version_id:
        path = metadata_path(root, pipeline_id, version_id)
        if path.exists():
            return load_version_metadata(root, pipeline_id, version_id)
    rows = list_all_versions(root)
    # 1) github_pr_number
    if pr_number is not None:
        for item in rows:
            if int(item.get("github_pr_number") or item.get("pull_request_number") or 0) == int(pr_number):
                return item
    # 2) github_branch
    if branch:
        for item in rows:
            if str(item.get("github_branch") or item.get("git_branch_name") or "").strip() == branch:
                return item
    # 3) dag ids
    if dag_id:
        for item in rows:
            if dag_id in {
                str(item.get("local_dag_id", "")).strip(),
                str(item.get("git_dag_id", "")).strip(),
                str(item.get("dag_id", "")).strip(),
                str(item.get("airflow_dag_id", "")).strip(),
            }:
                return item
    return None


def cmd_pr_open(args: argparse.Namespace) -> int:
    root = args.root.resolve()
    version = _load_or_create(root, args.pipeline_id, args.version_id, dag_id=args.dag_id, dag_file=args.dag_file)
    version["publish_status"] = args.publish_status or "pr_open"
    version["promotion_status"] = args.promotion_status or str(version.get("promotion_status") or "draft")
    version["deployment_target"] = "git"
    if args.pr_number is not None:
        version["github_pr_number"] = int(args.pr_number)
        version["pull_request_number"] = int(args.pr_number)
    if args.pr_url:
        version["github_pr_url"] = args.pr_url
        version["pull_request_url"] = args.pr_url
    if args.branch:
        version["github_branch"] = args.branch
        version["git_branch_name"] = args.branch
    if args.commit_sha:
        version["github_commit_sha"] = args.commit_sha
        version["commit_sha"] = args.commit_sha
    _write(root, version)
    return 0


def cmd_merged(args: argparse.Namespace) -> int:
    root = args.root.resolve()
    item = _find_for_merge(root, args.pipeline_id, args.version_id, args.dag_id, args.pr_number, args.branch)
    if not item:
        print("Version not found for merged update")
        for row in list_all_versions(root):
            print(
                {
                    "pipeline_id": row.get("pipeline_id"),
                    "version_id": row.get("version_id"),
                    "local_dag_id": row.get("local_dag_id"),
                    "git_dag_id": row.get("git_dag_id"),
                    "github_branch": row.get("github_branch"),
                    "github_pr_number": row.get("github_pr_number"),
                    "publish_status": row.get("publish_status"),
                    "promotion_status": row.get("promotion_status"),
                }
            )
        return 1
    version = _load_or_create(root, str(item["pipeline_id"]), str(item["version_id"]))
    version["publish_status"] = args.publish_status or "merged_to_git"
    version["promotion_status"] = args.promotion_status or "submitted"
    version["deployment_target"] = "git"
    version["merged_at"] = _now_iso()
    if args.commit_sha:
        version["merge_commit_sha"] = args.commit_sha
        version["github_commit_sha"] = args.commit_sha
    if args.branch:
        version["github_branch"] = args.branch
        version["git_branch_name"] = args.branch
    if args.pr_number is not None:
        version["github_pr_number"] = int(args.pr_number)
        version["pull_request_number"] = int(args.pr_number)
    if args.pr_url:
        version["github_pr_url"] = args.pr_url
        version["pull_request_url"] = args.pr_url
    _write(root, version)
    return 0


def cmd_mark_eval(args: argparse.Namespace) -> int:
    root = args.root.resolve()
    lookup_key = args.dag_id or args.version_id or ""
    item = find_version_metadata(root, args.pipeline_id, lookup_key)
    if not item:
        print(f"metadata file not found for pipeline_id={args.pipeline_id}, dag_id={args.dag_id}")
        return 1
    version = dict(item)
    version["promotion_status"] = "eval"
    version["evaluated_branch"] = "eval"
    version["updated_at"] = _now_iso()
    save_version_metadata(root, str(version["pipeline_id"]), str(version["version_id"]), version)
    print(f"Updated 1 version(s): {version['pipeline_id']}/{version['version_id']}")
    return 0


def cmd_scored(args: argparse.Namespace) -> int:
    root = args.root.resolve()
    item = find_version_metadata(root, args.pipeline_id, args.dag_id or args.version_id or "")
    if not item and args.version_id:
        try:
            item = load_version_metadata(root, args.pipeline_id, args.version_id)
        except FileNotFoundError:
            item = None
    if not item:
        return 1
    version = _load_or_create(root, str(item["pipeline_id"]), str(item["version_id"]))
    score_payload = json.loads(args.score_json) if args.score_json else {}
    version["promotion_status"] = args.promotion_status or "challenger"
    version["deployment_target"] = "git"
    version["score"] = score_payload.get("score")
    version["score_type"] = score_payload.get("score_type")
    version["score_breakdown"] = score_payload.get("breakdown", {})
    version["warnings"] = score_payload.get("warnings", [])
    version["critical_failures"] = score_payload.get("critical_failures", [])
    version["last_evaluated_at"] = _now_iso()
    _write(root, version)
    return 0


def _champion_for_base(root: Path, base_pipeline_id: str) -> dict[str, Any] | None:
    for item in list_all_versions(root):
        if str(item.get("base_pipeline_id", "")).strip() == base_pipeline_id and str(item.get("promotion_status", "")).strip() == "champion":
            return item
    return None


def cmd_promote(args: argparse.Namespace) -> int:
    root = args.root.resolve()
    target = _load_or_create(root, args.pipeline_id, args.version_id, dag_id=args.dag_id, dag_file=args.dag_file)
    base_id = str(target.get("base_pipeline_id") or target.get("pipeline_id")).strip()
    champion = _champion_for_base(root, base_id)
    if champion and not (champion["pipeline_id"] == target["pipeline_id"] and champion["version_id"] == target["version_id"]):
        old = _load_or_create(root, str(champion["pipeline_id"]), str(champion["version_id"]))
        old["promotion_status"] = "archived"
        old["deployment_target"] = "git"
        old["archived_at"] = _now_iso()
        _write(root, old)
    target["promotion_status"] = args.promotion_status or "champion"
    target["deployment_target"] = "git"
    target["promoted_at"] = _now_iso()
    hist = list(target.get("promotion_history") or [])
    hist.append(
        {
            "at": _now_iso(),
            "promoted_version_id": str(target.get("version_id")),
            "base_pipeline_id": base_id,
            "previous_champion_version_id": str(champion.get("version_id")) if champion else None,
        }
    )
    target["promotion_history"] = hist
    _write(root, target)
    return 0


def cmd_rollback(args: argparse.Namespace) -> int:
    root = args.root.resolve()
    target = _load_or_create(root, args.pipeline_id, args.version_id, dag_id=args.dag_id, dag_file=args.dag_file)
    base_id = str(target.get("base_pipeline_id") or target.get("pipeline_id")).strip()
    champion = _champion_for_base(root, base_id)
    if champion and not (champion["pipeline_id"] == target["pipeline_id"] and champion["version_id"] == target["version_id"]):
        old = _load_or_create(root, str(champion["pipeline_id"]), str(champion["version_id"]))
        old["promotion_status"] = "archived"
        old["deployment_target"] = "git"
        old["archived_at"] = _now_iso()
        _write(root, old)
    target["promotion_status"] = "champion"
    target["deployment_target"] = "git"
    target["restored_at"] = _now_iso()
    _write(root, target)
    return 0


def cmd_migrate(args: argparse.Namespace) -> int:
    from migrate_store_to_version_files import main as migrate_main

    return migrate_main()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Update version metadata files")
    parser.add_argument("command", choices=["pr_open", "merged", "mark_eval", "scored", "promote", "rollback", "migrate"])
    parser.add_argument("--root", type=Path, default=Path("."), help="Repository root")
    parser.add_argument("--pipeline-id", type=str, default="")
    parser.add_argument("--version-id", type=str, default="")
    parser.add_argument("--dag-id", type=str, default="")
    parser.add_argument("--dag-file", type=str, default="")
    parser.add_argument("--pr-number", type=int)
    parser.add_argument("--pr-url", type=str, default="")
    parser.add_argument("--branch", type=str, default="")
    parser.add_argument("--commit-sha", type=str, default="")
    parser.add_argument("--score-json", type=str, default="")
    parser.add_argument("--promotion-status", type=str, default="")
    parser.add_argument("--publish-status", type=str, default="")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    handlers = {
        "pr_open": cmd_pr_open,
        "merged": cmd_merged,
        "mark_eval": cmd_mark_eval,
        "scored": cmd_scored,
        "promote": cmd_promote,
        "rollback": cmd_rollback,
        "migrate": cmd_migrate,
    }
    return handlers[args.command](args)


if __name__ == "__main__":
    raise SystemExit(main())
