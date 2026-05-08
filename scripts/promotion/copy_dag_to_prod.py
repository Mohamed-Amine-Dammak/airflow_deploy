#!/usr/bin/env python3
"""Copy a promoted DAG from eval branch to prod worktree using per-version metadata."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
METADATA_DIR = SCRIPT_DIR.parent / "metadata"
if str(METADATA_DIR) not in sys.path:
    sys.path.insert(0, str(METADATA_DIR))

from version_metadata import load_version_metadata  # type: ignore


def main() -> int:
    parser = argparse.ArgumentParser(description="Copy DAG to prod branch")
    parser.add_argument("--root", type=str, required=True)
    parser.add_argument("--pipeline-id", type=str, required=True)
    parser.add_argument("--version-id", type=str, required=True)
    parser.add_argument("--eval-branch", type=str, default="eval")
    parser.add_argument("--prod-branch", type=str, default="prod")
    args = parser.parse_args()

    root = Path(args.root)
    version = load_version_metadata(root, args.pipeline_id, args.version_id)
    git_dag_file = str(version.get("git_dag_file") or version.get("repo_file_path") or "").strip()
    if not git_dag_file.startswith("dags/"):
        print(f"ERROR: invalid git_dag_file: {git_dag_file}")
        return 1

    result = subprocess.run(
        ["git", "show", f"{args.eval_branch}:{git_dag_file}"],
        cwd=str(root),
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        print(f"ERROR: could not read {git_dag_file} from {args.eval_branch}: {result.stderr}")
        return 1

    target = root / git_dag_file
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(result.stdout, encoding="utf-8")
    subprocess.run(["git", "add", git_dag_file], cwd=str(root), check=True)
    subprocess.run(["git", "commit", "-m", f"Promote {args.version_id} DAG artifact"], cwd=str(root), check=False)
    print(f"Copied {git_dag_file} to prod worktree")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
