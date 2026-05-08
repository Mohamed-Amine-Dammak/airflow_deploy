#!/usr/bin/env python3
"""Update DAG pipeline metadata in the canonical JSON store."""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_store(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"pipelines": {}}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return {"pipelines": {}}
    pipelines = payload.get("pipelines")
    if not isinstance(pipelines, dict):
        payload["pipelines"] = {}
    return payload


def _save_store(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Overwrite the whole file from a single in-memory JSON object.
    normalized = dict(payload if isinstance(payload, dict) else {})
    pipelines = normalized.get("pipelines")
    if not isinstance(pipelines, dict):
        normalized["pipelines"] = {}
    path.write_text(json.dumps(normalized, indent=2), encoding="utf-8")


def _inject_git_marker_before_version(name: str) -> str:
    base = str(name or "").strip()
    if not base:
        return ""
    if re.search(r"(^|[_-])git($|[_-])", base, flags=re.IGNORECASE):
        return base
    match = re.search(r"(__v\d+)$", base, flags=re.IGNORECASE)
    if not match:
        match = re.search(r"(_v\d+)$", base, flags=re.IGNORECASE)
    if not match:
        match = re.search(r"(-v\d+)$", base, flags=re.IGNORECASE)
    if match:
        start = match.start()
        return f"{base[:start]}_git{base[start:]}"
    return f"{base}_git"


def _strip_py(path_text: str) -> str:
    name = Path(str(path_text or "").strip()).name
    return name[:-3] if name.endswith(".py") else name


def _infer_deployment_target(version: dict[str, Any]) -> str:
    current = str(version.get("deployment_target", "")).strip().lower()
    if current in {"git", "local", "eval", "prod"}:
        if current in {"eval", "prod"}:
            return "git"
        return current
    publish_status = str(version.get("publish_status", "")).strip().lower()
    promotion_status = str(version.get("promotion_status", "")).strip().lower()
    has_git = bool(str(version.get("git_dag_file", "") or version.get("repo_file_path", "")).strip())
    if has_git or publish_status in {"pr_open", "merged_to_git", "local_deleted"} or promotion_status in {
        "submitted",
        "eval",
        "challenger",
        "champion",
        "archived",
    }:
        return "git"
    return "local"


def _validate_identity(version: dict[str, Any]) -> None:
    local_dag_id = str(version.get("local_dag_id", "")).strip()
    local_dag_file = str(version.get("local_dag_file", "")).strip()
    git_dag_id = str(version.get("git_dag_id", "")).strip()
    git_dag_file = str(version.get("git_dag_file", "")).strip()
    if local_dag_id and local_dag_file and _strip_py(local_dag_file) != local_dag_id:
        raise SystemExit(f"Invalid local identity: file '{local_dag_file}' does not match local_dag_id '{local_dag_id}'")
    if git_dag_id and git_dag_file and _strip_py(git_dag_file) != git_dag_id:
        raise SystemExit(f"Invalid git identity: file '{git_dag_file}' does not match git_dag_id '{git_dag_id}'")


def _iter_versions(payload: dict[str, Any]):
    for pipeline_id, entry in payload.get("pipelines", {}).items():
        versions = entry.get("versions", [])
        if not isinstance(versions, list):
            continue
        for version in versions:
            if isinstance(version, dict):
                yield pipeline_id, entry, version


def _summarize_versions(payload: dict[str, Any]) -> str:
    rows = []
    for pid, _, version in _iter_versions(payload):
        rows.append(
            {
                "pipeline_id": pid,
                "version_id": str(version.get("version_id", "")).strip(),
                "dag_id": str(version.get("dag_id", "") or version.get("airflow_dag_id", "")).strip(),
                "github_branch": str(version.get("github_branch", "") or version.get("git_branch_name", "")).strip(),
                "github_pr_number": int(version.get("github_pr_number") or version.get("pull_request_number") or 0),
                "publish_status": str(version.get("publish_status", "")).strip(),
                "promotion_status": str(version.get("promotion_status", "")).strip(),
            }
        )
    return json.dumps(rows, indent=2)


def _find_version(
    payload: dict[str, Any],
    *,
    pipeline_id: str | None,
    dag_id: str | None,
    version_id: str | None,
    pr_number: int | None,
    branch: str | None,
) -> tuple[str, dict[str, Any], dict[str, Any]] | None:
    for pid, entry, version in _iter_versions(payload):
        if pipeline_id and pid != pipeline_id:
            continue
        if version_id and str(version.get("version_id", "")).strip() != version_id:
            continue
        if dag_id and str(version.get("airflow_dag_id", "")).strip() != dag_id and str(version.get("dag_id", "")).strip() != dag_id:
            continue
        if pr_number is not None:
            pnums = {
                int(version.get("pull_request_number") or 0),
                int(version.get("github_pr_number") or 0),
            }
            if int(pr_number) not in pnums:
                continue
        if branch:
            branches = {
                str(version.get("git_branch_name", "")).strip(),
                str(version.get("github_branch", "")).strip(),
            }
            if branch not in branches:
                continue
        return pid, entry, version
    return None


def _find_version_for_merged(
    payload: dict[str, Any],
    *,
    pr_number: int | None,
    branch: str | None,
    dag_id: str | None,
    pipeline_id: str | None,
    version_id: str | None,
) -> tuple[str, dict[str, Any], dict[str, Any]] | None:
    # 1) Match by github_pr_number first
    if pr_number is not None:
        for pid, entry, version in _iter_versions(payload):
            if pipeline_id and pid != pipeline_id:
                continue
            if version_id and str(version.get("version_id", "")).strip() != version_id:
                continue
            if int(version.get("github_pr_number") or version.get("pull_request_number") or 0) == int(pr_number):
                return pid, entry, version
    # 2) Fallback by github_branch
    if branch:
        for pid, entry, version in _iter_versions(payload):
            if pipeline_id and pid != pipeline_id:
                continue
            if version_id and str(version.get("version_id", "")).strip() != version_id:
                continue
            if str(version.get("github_branch", "") or version.get("git_branch_name", "")).strip() == branch:
                return pid, entry, version
    # 3) Fallback by dag_id
    if dag_id:
        for pid, entry, version in _iter_versions(payload):
            if pipeline_id and pid != pipeline_id:
                continue
            if version_id and str(version.get("version_id", "")).strip() != version_id:
                continue
            if str(version.get("dag_id", "") or version.get("airflow_dag_id", "")).strip() == dag_id:
                return pid, entry, version
    return None


def _versions_for_base_pipeline(payload: dict[str, Any], base_pipeline_id: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for _, _, version in _iter_versions(payload):
        if str(version.get("base_pipeline_id", "")).strip() == str(base_pipeline_id).strip():
            items.append(version)
    return items


def _apply_common(version: dict[str, Any], *, commit_sha: str, branch: str, pr_number: int | None, pr_url: str) -> None:
    if pr_number is not None:
        version["pull_request_number"] = pr_number
        version["github_pr_number"] = pr_number
    if pr_url:
        version["pull_request_url"] = pr_url
        version["github_pr_url"] = pr_url
    if branch:
        version["git_branch_name"] = branch
        version["github_branch"] = branch
    if commit_sha:
        version["commit_sha"] = commit_sha
        version["github_commit_sha"] = commit_sha


def _normalize_version_fields(
    version: dict[str, Any],
    *,
    pipeline_id: str,
    dag_id: str | None,
    dag_file: str | None,
) -> None:
    safe_pipeline_id = str(pipeline_id or "").strip()
    safe_local_dag_id = str(
        dag_id
        or version.get("local_dag_id")
        or version.get("airflow_dag_id")
        or version.get("dag_id")
        or ""
    ).strip()
    safe_git_dag_id = str(version.get("git_dag_id") or _inject_git_marker_before_version(safe_local_dag_id)).strip()
    provided_dag_file = str(dag_file or "").strip()
    safe_git_dag_file = str(
        version.get("git_dag_file")
        or version.get("repo_file_path")
        or provided_dag_file
        or (f"dags/{safe_git_dag_id}.py" if safe_git_dag_id else "")
    ).strip()
    safe_local_dag_file = str(
        version.get("local_dag_file")
        or (f"airflow/dags/{safe_local_dag_id}.py" if safe_local_dag_id else "")
    ).strip()
    # If an explicit repo DAG file is provided, let that drive git_dag_id.
    if provided_dag_file:
        provided_stem = _strip_py(provided_dag_file)
        if provided_stem:
            safe_git_dag_id = provided_stem

    version["pipeline_id"] = safe_pipeline_id
    version["base_pipeline_id"] = str(version.get("base_pipeline_id") or safe_pipeline_id).strip()
    if safe_local_dag_id:
        version["local_dag_id"] = safe_local_dag_id
        version["airflow_dag_id"] = safe_local_dag_id
        version["dag_id"] = safe_local_dag_id
    if safe_git_dag_id:
        version["git_dag_id"] = safe_git_dag_id
    if safe_local_dag_file:
        version["local_dag_file"] = safe_local_dag_file
    if safe_git_dag_file:
        version["git_dag_file"] = safe_git_dag_file
        version["repo_file_path"] = safe_git_dag_file  # alias
        version["dag_file"] = safe_git_dag_file
        if not str(version.get("output_filename", "")).strip():
            version["output_filename"] = Path(safe_local_dag_file or safe_git_dag_file).name
    version["deployment_target"] = _infer_deployment_target(version)
    _validate_identity(version)


def _migrate_all_versions(payload: dict[str, Any]) -> None:
    pipelines = payload.setdefault("pipelines", {})
    if not isinstance(pipelines, dict):
        payload["pipelines"] = {}
        pipelines = payload["pipelines"]
    for pid, entry in pipelines.items():
        if not isinstance(entry, dict):
            continue
        versions = entry.get("versions", [])
        if not isinstance(versions, list):
            continue
        for version in versions:
            if not isinstance(version, dict):
                continue
            _normalize_version_fields(
                version,
                pipeline_id=str(version.get("pipeline_id") or pid or ""),
                dag_id=str(version.get("local_dag_id") or version.get("airflow_dag_id") or version.get("dag_id") or ""),
                dag_file=str(version.get("git_dag_file") or version.get("repo_file_path") or version.get("dag_file") or ""),
            )


def cmd_pr_open(args: argparse.Namespace) -> int:
    payload = _load_store(args.store)
    _migrate_all_versions(payload)
    found = _find_version(
        payload,
        pipeline_id=args.pipeline_id,
        dag_id=args.dag_id,
        version_id=args.version_id,
        pr_number=args.pr_number,
        branch=args.branch,
    )
    if not found:
        if not args.pipeline_id or not args.version_id:
            raise SystemExit("Version not found for pr_open update")
        pipelines = payload.setdefault("pipelines", {})
        entry = pipelines.setdefault(args.pipeline_id, {"versions": []})
        versions = entry.setdefault("versions", [])
        version = {
            "pipeline_id": args.pipeline_id,
            "base_pipeline_id": args.pipeline_id,
            "version_id": args.version_id,
            "airflow_dag_id": args.dag_id or "",
            "dag_id": args.dag_id or "",
            "version": args.version_id,
            "output_filename": (Path(args.dag_file).name if args.dag_file else ""),
        }
        versions.append(version)
        pid = args.pipeline_id
    else:
        pid, _, version = found
    now = _now_iso()
    _normalize_version_fields(
        version,
        pipeline_id=pid,
        dag_id=args.dag_id,
        dag_file=args.dag_file,
    )
    version["version"] = str(version.get("version_id") or args.version_id or "")
    version["publish_status"] = args.publish_status or "pr_open"
    version["promotion_status"] = args.promotion_status or str(version.get("promotion_status") or "draft")
    version["deployment_target"] = "git"
    _apply_common(version, commit_sha=args.commit_sha or "", branch=args.branch or "", pr_number=args.pr_number, pr_url=args.pr_url or "")
    version["created_at"] = str(version.get("created_at") or now)
    version["updated_at"] = now
    _save_store(args.store, payload)
    return 0


def cmd_merged(args: argparse.Namespace) -> int:
    payload = _load_store(args.store)
    _migrate_all_versions(payload)
    found = _find_version_for_merged(
        payload,
        pr_number=args.pr_number,
        branch=args.branch,
        dag_id=args.dag_id,
        pipeline_id=args.pipeline_id,
        version_id=args.version_id,
    )
    if not found:
        print("Version not found for merged update")
        print("Known versions:")
        print(_summarize_versions(payload))
        raise SystemExit(1)
    _, _, version = found
    now = _now_iso()
    _normalize_version_fields(
        version,
        pipeline_id=str(version.get("pipeline_id") or args.pipeline_id or ""),
        dag_id=args.dag_id,
        dag_file=args.dag_file,
    )
    version["publish_status"] = args.publish_status or "merged_to_git"
    version["promotion_status"] = args.promotion_status or "submitted"
    version["deployment_target"] = "git"
    if args.commit_sha:
        version["merge_commit_sha"] = args.commit_sha
    version["merged_at"] = now
    version["updated_at"] = now
    _apply_common(version, commit_sha=args.commit_sha or "", branch=args.branch or "", pr_number=args.pr_number, pr_url=args.pr_url or "")
    _save_store(args.store, payload)
    return 0


def cmd_mark_eval(args: argparse.Namespace) -> int:
    payload = _load_store(args.store)
    _migrate_all_versions(payload)
    now = _now_iso()
    changed = 0
    for pid, _, version in _iter_versions(payload):
        if args.pipeline_id and pid != args.pipeline_id:
            continue
        if args.version_id and str(version.get("version_id", "")).strip() != args.version_id:
            continue
        if str(version.get("promotion_status", "")).strip().lower() in {"champion", "archived"}:
            continue
        version["promotion_status"] = "eval"
        version["deployment_target"] = "eval"
        version["updated_at"] = now
        if args.branch:
            version["github_branch"] = args.branch
        changed += 1
    if changed:
        _save_store(args.store, payload)
    return 0


def cmd_scored(args: argparse.Namespace) -> int:
    payload = _load_store(args.store)
    _migrate_all_versions(payload)
    found = _find_version(payload, pipeline_id=args.pipeline_id, dag_id=args.dag_id, version_id=args.version_id, pr_number=None, branch=None)
    if not found:
        raise SystemExit("Version not found for scored update")
    _, _, version = found
    now = _now_iso()
    score_payload = {}
    if args.score_json:
        score_payload = json.loads(args.score_json)
    version["promotion_status"] = args.promotion_status or "challenger"
    version["deployment_target"] = "eval"
    version["score"] = score_payload.get("score")
    version["score_type"] = score_payload.get("score_type")
    version["score_breakdown"] = score_payload.get("breakdown", {})
    version["warnings"] = score_payload.get("warnings", [])
    version["critical_failures"] = score_payload.get("critical_failures", [])
    version["last_evaluated_at"] = now
    version["updated_at"] = now
    _save_store(args.store, payload)
    return 0


def cmd_promote(args: argparse.Namespace) -> int:
    payload = _load_store(args.store)
    _migrate_all_versions(payload)
    found = _find_version(payload, pipeline_id=args.pipeline_id, dag_id=args.dag_id, version_id=args.version_id, pr_number=None, branch=None)
    if not found:
        raise SystemExit("Version not found for promote update")
    pid, entry, target = found
    now = _now_iso()
    base_id = str(target.get("base_pipeline_id") or pid).strip()
    family_versions = []
    for version in entry.get("versions", []):
        version_base = str(version.get("base_pipeline_id", "")).strip()
        if not version_base or version_base == base_id:
            family_versions.append(version)
    if not family_versions:
        family_versions = _versions_for_base_pipeline(payload, base_id) or entry.get("versions", [])
    prev = None
    for version in family_versions:
        if str(version.get("promotion_status", "")).strip().lower() == "champion":
            prev = str(version.get("version_id", ""))
            version["promotion_status"] = "archived"
            version["updated_at"] = now
    target["promotion_status"] = args.promotion_status or "champion"
    target["deployment_target"] = "prod"
    target["promoted_at"] = now
    target["updated_at"] = now
    history = entry.setdefault("promotion_history", [])
    if isinstance(history, list):
        history.append(
            {
                "at": now,
                "pipeline_id": base_id,
                "promoted_version_id": str(target.get("version_id", "")),
                "previous_champion_version_id": prev,
            }
        )
    _save_store(args.store, payload)
    return 0


def cmd_rollback(args: argparse.Namespace) -> int:
    payload = _load_store(args.store)
    _migrate_all_versions(payload)
    found = _find_version(payload, pipeline_id=args.pipeline_id, dag_id=args.dag_id, version_id=args.version_id, pr_number=None, branch=None)
    if not found:
        raise SystemExit("Version not found for rollback update")
    _, entry, target = found
    now = _now_iso()
    for version in entry.get("versions", []):
        if str(version.get("promotion_status", "")).strip().lower() == "champion":
            version["promotion_status"] = "archived"
            version["updated_at"] = now
    target["promotion_status"] = "champion"
    target["restored_at"] = now
    target["updated_at"] = now
    _save_store(args.store, payload)
    return 0


def cmd_migrate(args: argparse.Namespace) -> int:
    payload = _load_store(args.store)
    _migrate_all_versions(payload)
    _save_store(args.store, payload)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Update pipeline metadata JSON")
    parser.add_argument("command", choices=["pr_open", "merged", "mark_eval", "scored", "promote", "rollback", "migrate"])
    parser.add_argument("--store", type=Path, required=True)
    parser.add_argument("--pipeline-id", type=str)
    parser.add_argument("--dag-id", type=str)
    parser.add_argument("--version-id", type=str)
    parser.add_argument("--pr-number", type=int)
    parser.add_argument("--pr-url", type=str, default="")
    parser.add_argument("--branch", type=str)
    parser.add_argument("--commit-sha", type=str)
    parser.add_argument("--score-json", type=str, default="")
    parser.add_argument("--promotion-status", type=str)
    parser.add_argument("--publish-status", type=str)
    parser.add_argument("--dag-file", type=str)
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
