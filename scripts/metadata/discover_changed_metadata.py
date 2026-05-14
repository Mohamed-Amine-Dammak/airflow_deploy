#!/usr/bin/env python3
"""Discover changed metadata files and summarize promotion fields."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from metadata_path_normalization import normalize_metadata_path, normalize_metadata_paths


def _changed_files(root: Path, rev_from: str, rev_to: str) -> list[str]:
    proc = subprocess.run(
        ["git", "-C", str(root), "diff", "--name-only", rev_from, rev_to],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return []
    files: list[str] = []
    for line in proc.stdout.splitlines():
        p = normalize_metadata_path(str(line or ""))
        if p.startswith("airflow/web_app_data/metadata/") and p.endswith(".json"):
            files.append(p)
    return normalize_metadata_paths(files)


def _row(root: Path, rel: str) -> dict[str, Any] | None:
    normalized_rel = normalize_metadata_path(rel)
    print(
        f"[discover_changed_metadata] metadata_file_raw={rel!r} normalized={normalized_rel!r}",
        file=sys.stderr,
    )
    path = root / normalized_rel
    if not path.exists():
        print(
            f"[discover_changed_metadata] metadata_file_exists=false normalized={normalized_rel!r}",
            file=sys.stderr,
        )
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    return {
        "metadata_file": normalized_rel,
        "pipeline_id": str(payload.get("pipeline_id", "")).strip(),
        "version_id": str(payload.get("version_id", "")).strip(),
        "base_pipeline_id": str(payload.get("base_pipeline_id", "")).strip(),
        "local_dag_id": str(payload.get("local_dag_id", "")).strip(),
        "git_dag_id": str(payload.get("git_dag_id", "")).strip(),
        "git_dag_file": str(payload.get("git_dag_file", "")).strip(),
        "promotion_status": str(payload.get("promotion_status", "")).strip(),
        "publish_status": str(payload.get("publish_status", "")).strip(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Discover changed metadata files")
    parser.add_argument("--root", type=Path, default=Path("."), help="Repository root")
    parser.add_argument("--from", dest="rev_from", type=str, default="HEAD~1")
    parser.add_argument("--to", dest="rev_to", type=str, default="HEAD")
    parser.add_argument("--metadata-file", action="append", default=[], help="Explicit metadata file(s)")
    args = parser.parse_args()

    root = args.root.resolve()
    files = normalize_metadata_paths(str(x) for x in args.metadata_file if str(x).strip())
    if not files:
        files = _changed_files(root, args.rev_from, args.rev_to)
    out: list[dict[str, Any]] = []
    for rel in files:
        row = _row(root, rel)
        if row:
            out.append(row)
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

