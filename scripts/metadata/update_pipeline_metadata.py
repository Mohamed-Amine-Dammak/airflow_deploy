#!/usr/bin/env python3
"""Update DAG pipeline metadata in the canonical JSON store."""

from __future__ import annotations

import argparse
import json
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
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _iter_versions(payload: dict[str, Any]):
    for pipeline_id, entry in payload.get("pipelines", {}).items():
        versions = entry.get("versions", [])
        if not isinstance(versions, list):
            continue
        for version in versions:
            if isinstance(version, dict):
                yield pipeline_id, entry, version


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


def cmd_pr_open(args: argparse.Namespace) -> int:
    payload = _load_store(args.store)
    found = _find_version(
        payload,
        pipeline_id=args.pipeline_id,
        dag_id=args.dag_id,
        version_id=args.version_id,
        pr_number=args.pr_number,
        branch=args.branch,
    )
    if not found:
        raise SystemExit("Version not found for pr_open update")
    pid, _, version = found
    now = _now_iso()
    version["pipeline_id"] = pid
    version["base_pipeline_id"] = str(version.get("base_pipeline_id") or pid)
    version["dag_id"] = str(version.get("airflow_dag_id") or args.dag_id or "")
    version["version"] = str(version.get("version_id") or args.version_id or "")
    version["publish_status"] = args.publish_status or "pr_open"
    version["promotion_status"] = args.promotion_status or str(version.get("promotion_status") or "draft")
    _apply_common(version, commit_sha=args.commit_sha or "", branch=args.branch or "", pr_number=args.pr_number, pr_url=args.pr_url or "")
    version["created_at"] = str(version.get("created_at") or now)
    version["updated_at"] = now
    _save_store(args.store, payload)
    return 0


def cmd_merged(args: argparse.Namespace) -> int:
    payload = _load_store(args.store)
    found = _find_version(payload, pipeline_id=args.pipeline_id, dag_id=args.dag_id, version_id=args.version_id, pr_number=args.pr_number, branch=args.branch)
    if not found:
        raise SystemExit("Version not found for merged update")
    _, _, version = found
    now = _now_iso()
    version["publish_status"] = args.publish_status or "merged_to_git"
    version["promotion_status"] = args.promotion_status or "submitted"
    if args.commit_sha:
        version["merge_commit_sha"] = args.commit_sha
    version["merged_at"] = now
    version["updated_at"] = now
    _apply_common(version, commit_sha=args.commit_sha or "", branch=args.branch or "", pr_number=args.pr_number, pr_url=args.pr_url or "")
    _save_store(args.store, payload)
    return 0


def cmd_mark_eval(args: argparse.Namespace) -> int:
    payload = _load_store(args.store)
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
        version["updated_at"] = now
        if args.branch:
            version["github_branch"] = args.branch
        changed += 1
    if changed:
        _save_store(args.store, payload)
    return 0


def cmd_scored(args: argparse.Namespace) -> int:
    payload = _load_store(args.store)
    found = _find_version(payload, pipeline_id=args.pipeline_id, dag_id=args.dag_id, version_id=args.version_id, pr_number=None, branch=None)
    if not found:
        raise SystemExit("Version not found for scored update")
    _, _, version = found
    now = _now_iso()
    score_payload = {}
    if args.score_json:
        score_payload = json.loads(args.score_json)
    version["promotion_status"] = args.promotion_status or "challenger"
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
    found = _find_version(payload, pipeline_id=args.pipeline_id, dag_id=args.dag_id, version_id=args.version_id, pr_number=None, branch=None)
    if not found:
        raise SystemExit("Version not found for promote update")
    pid, entry, target = found
    now = _now_iso()
    prev = None
    for version in entry.get("versions", []):
        if str(version.get("promotion_status", "")).strip().lower() == "champion":
            prev = str(version.get("version_id", ""))
            version["promotion_status"] = "archived"
            version["updated_at"] = now
    target["promotion_status"] = args.promotion_status or "champion"
    target["promoted_at"] = now
    target["updated_at"] = now
    history = entry.setdefault("promotion_history", [])
    if isinstance(history, list):
        history.append(
            {
                "at": now,
                "pipeline_id": pid,
                "promoted_version_id": str(target.get("version_id", "")),
                "previous_champion_version_id": prev,
            }
        )
    _save_store(args.store, payload)
    return 0


def cmd_rollback(args: argparse.Namespace) -> int:
    payload = _load_store(args.store)
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Update pipeline metadata JSON")
    parser.add_argument("command", choices=["pr_open", "merged", "mark_eval", "scored", "promote", "rollback"])
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
    }
    return handlers[args.command](args)


if __name__ == "__main__":
    raise SystemExit(main())
