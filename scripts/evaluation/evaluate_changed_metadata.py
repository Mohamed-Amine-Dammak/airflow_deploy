#!/usr/bin/env python3
"""Evaluate changed metadata rows automatically."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


def _discover(root: Path, rev_from: str, rev_to: str, metadata_files: list[str]) -> list[dict]:
    cmd = [
        sys.executable,
        str(root / "scripts" / "metadata" / "discover_changed_metadata.py"),
        "--root",
        str(root),
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate changed metadata files")
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--from", dest="rev_from", type=str, default="HEAD~1")
    parser.add_argument("--to", dest="rev_to", type=str, default="HEAD")
    parser.add_argument("--metadata-file", action="append", default=[])
    parser.add_argument("--evaluation-mode", type=str, default="run_once")
    parser.add_argument("--run-count", type=str, default="1")
    args = parser.parse_args()

    root = args.root.resolve()
    rows = _discover(root, args.rev_from, args.rev_to, [str(x) for x in args.metadata_file if str(x).strip()])
    if not rows:
        print("No changed metadata rows discovered.")
        return 0
    failures = 0
    for row in rows:
        if str(row.get("promotion_status", "")).strip().lower() != "eval":
            continue
        pipeline_id = str(row.get("pipeline_id", "")).strip()
        dag_id = str(row.get("git_dag_id", "")).strip() or str(row.get("local_dag_id", "")).strip()
        if not pipeline_id or not dag_id:
            print(f"Skipping row with missing ids: {row}")
            continue
        print(f"Evaluating pipeline_id={pipeline_id} dag_id={dag_id}")
        cmd = [
            sys.executable,
            str(root / "scripts" / "evaluation" / "evaluate_dag.py"),
            "--root",
            str(root),
            "--pipeline-id",
            pipeline_id,
            "--dag-id",
            dag_id,
            "--evaluation-mode",
            args.evaluation_mode,
            "--run-count",
            args.run_count,
        ]
        proc = subprocess.run(cmd, check=False)
        if proc.returncode != 0:
            failures += 1
    if failures:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

