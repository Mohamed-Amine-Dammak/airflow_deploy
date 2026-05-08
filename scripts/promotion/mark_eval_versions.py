#!/usr/bin/env python3
"""Mark version metadata files as eval."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
METADATA_DIR = SCRIPT_DIR.parent / "metadata"
if str(METADATA_DIR) not in sys.path:
    sys.path.insert(0, str(METADATA_DIR))

from version_metadata import list_all_versions, list_pipeline_versions, load_version_metadata, save_version_metadata  # type: ignore


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> int:
    parser = argparse.ArgumentParser(description="Mark versions as eval")
    parser.add_argument("--root", type=str, required=True)
    parser.add_argument("--pipeline-id", type=str, default="")
    parser.add_argument("--version-id", type=str, default="")
    parser.add_argument("--branch", type=str, default="eval")
    args = parser.parse_args()

    root = Path(args.root)
    rows = list_pipeline_versions(root, args.pipeline_id) if args.pipeline_id else list_all_versions(root)
    updated = 0
    for row in rows:
        if args.version_id and str(row.get("version_id", "")) != args.version_id:
            continue
        version = load_version_metadata(root, str(row["pipeline_id"]), str(row["version_id"]))
        status = str(version.get("promotion_status", "")).strip().lower()
        if status in {"champion", "archived"}:
            continue
        version["promotion_status"] = "eval"
        version["deployment_target"] = "git"
        version["github_branch"] = args.branch
        version["updated_at"] = _now()
        save_version_metadata(root, str(version["pipeline_id"]), str(version["version_id"]), version)
        updated += 1
    print(f"Updated {updated} version(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
