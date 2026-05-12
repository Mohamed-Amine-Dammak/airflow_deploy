#!/usr/bin/env python3
"""Promote changed challenger metadata rows to prod when eligible."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


def _discover(eval_root: Path, rev_from: str, rev_to: str, metadata_files: list[str]) -> list[dict]:
    cmd = [
        sys.executable,
        str(eval_root / "scripts" / "metadata" / "discover_changed_metadata.py"),
        "--root",
        str(eval_root),
        "--from",
        rev_from,
        "--to",
        rev_to,
    ]
    for mf in metadata_files:
        cmd.extend(["--metadata-file", mf])
    proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
    payload = json.loads(proc.stdout or "[]")
    return payload if isinstance(payload, list) else []


def _copy_file_from_branch(repo_root: Path, branch: str, rel_path: str) -> tuple[bool, str]:
    rel = str(rel_path or "").strip().replace("\\", "/")
    if not rel:
        return False, "empty path"
    result = subprocess.run(
        ["git", "show", f"{branch}:{rel}"],
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return False, (result.stderr or result.stdout or "").strip()
    target = repo_root / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(result.stdout, encoding="utf-8")
    return True, ""


def main() -> int:
    parser = argparse.ArgumentParser(description="Promote changed challengers")
    parser.add_argument("--eval-root", type=Path, required=True)
    parser.add_argument("--prod-root", type=Path, required=True)
    parser.add_argument("--from", dest="rev_from", type=str, default="HEAD~1")
    parser.add_argument("--to", dest="rev_to", type=str, default="HEAD")
    parser.add_argument("--metadata-file", action="append", default=[])
    parser.add_argument("--decision-file", type=Path, default=Path("evaluation-results/promotion-decisions.json"))
    parser.add_argument("--rollback-retention-enabled", type=str, default="false")
    args = parser.parse_args()

    eval_root = args.eval_root.resolve()
    prod_root = args.prod_root.resolve()
    rows = _discover(eval_root, args.rev_from, args.rev_to, [str(x) for x in args.metadata_file if str(x).strip()])
    decisions: list[dict] = []
    for row in rows:
        if str(row.get("promotion_status", "")).strip().lower() != "challenger":
            continue
        pipeline_id = str(row.get("pipeline_id", "")).strip()
        version_id = str(row.get("version_id", "")).strip()
        meta_file = str(row.get("metadata_file", "")).strip()
        if not pipeline_id or not version_id:
            continue
        elig_cmd = [
            sys.executable,
            str(eval_root / "scripts" / "promotion" / "check_promotion_eligibility.py"),
            "--root",
            str(eval_root),
            "--pipeline-id",
            pipeline_id,
            "--version-id",
            version_id,
        ]
        elig = subprocess.run(elig_cmd, check=False, capture_output=True, text=True)
        if elig.returncode != 0:
            decisions.append(
                {
                    "pipeline_id": pipeline_id,
                    "version_id": version_id,
                    "eligible": False,
                    "reason": "blocked_by_score_policy",
                    "details": (elig.stdout + "\n" + elig.stderr).strip(),
                    "metadata_file": meta_file,
                }
            )
            continue

        meta_copy_ok, meta_copy_err = _copy_file_from_branch(prod_root, "eval", meta_file)
        if not meta_copy_ok:
            decisions.append(
                {
                    "pipeline_id": pipeline_id,
                    "version_id": version_id,
                    "eligible": True,
                    "promoted": False,
                    "reason": "metadata_copy_failed",
                    "details": meta_copy_err,
                    "metadata_file": meta_file,
                }
            )
            continue

        copy_cmd = [
            sys.executable,
            str(eval_root / "scripts" / "promotion" / "copy_dag_to_prod.py"),
            "--root",
            str(prod_root),
            "--pipeline-id",
            pipeline_id,
            "--version-id",
            version_id,
            "--eval-branch",
            "eval",
            "--prod-branch",
            "prod",
        ]
        copy_proc = subprocess.run(copy_cmd, check=False, capture_output=True, text=True)
        if copy_proc.returncode != 0:
            decisions.append(
                {
                    "pipeline_id": pipeline_id,
                    "version_id": version_id,
                    "eligible": True,
                    "promoted": False,
                    "reason": "copy_failed",
                    "details": (copy_proc.stdout + "\n" + copy_proc.stderr).strip(),
                    "metadata_file": meta_file,
                }
            )
            continue

        promote_cmd = [
            sys.executable,
            str(eval_root / "scripts" / "metadata" / "update_pipeline_metadata.py"),
            "promote",
            "--root",
            str(prod_root),
            "--pipeline-id",
            pipeline_id,
            "--version-id",
            version_id,
            "--promotion-status",
            "champion",
        ]
        prom = subprocess.run(promote_cmd, check=False, capture_output=True, text=True)
        if prom.returncode != 0:
            decisions.append(
                {
                    "pipeline_id": pipeline_id,
                    "version_id": version_id,
                    "eligible": True,
                    "promoted": False,
                    "reason": "metadata_promote_failed",
                    "details": (prom.stdout + "\n" + prom.stderr).strip(),
                    "metadata_file": meta_file,
                }
            )
            continue

        decisions.append(
            {
                "pipeline_id": pipeline_id,
                "version_id": version_id,
                "eligible": True,
                "promoted": True,
                "reason": "challenger_exceeds_threshold",
                "metadata_file": meta_file,
            }
        )

    out = args.decision_file if args.decision_file.is_absolute() else eval_root / args.decision_file
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"decisions": decisions}, indent=2), encoding="utf-8")
    print(f"Wrote decisions: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

