#!/usr/bin/env python3
"""Mark champion metadata using per-version files."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
METADATA_DIR = SCRIPT_DIR.parent / "metadata"
if str(METADATA_DIR) not in sys.path:
    sys.path.insert(0, str(METADATA_DIR))

from version_metadata import list_pipeline_versions, load_version_metadata, save_version_metadata  # type: ignore


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> int:
    parser = argparse.ArgumentParser(description="Mark DAG version as champion")
    parser.add_argument("--root", type=str, required=True)
    parser.add_argument("--pipeline-id", type=str, required=True)
    parser.add_argument("--version-id", type=str, required=True)
    args = parser.parse_args()

    root = Path(args.root)
    target = load_version_metadata(root, args.pipeline_id, args.version_id)
    base_id = str(target.get("base_pipeline_id") or args.pipeline_id).strip()
    now = _now()
    archived = None
    for row in list_pipeline_versions(root, args.pipeline_id):
        if str(row.get("base_pipeline_id", "")).strip() == base_id and str(row.get("promotion_status", "")).strip() == "champion":
            old = load_version_metadata(root, str(row["pipeline_id"]), str(row["version_id"]))
            old["promotion_status"] = "archived"
            old["archived_at"] = now
            save_version_metadata(root, str(old["pipeline_id"]), str(old["version_id"]), old)
            archived = str(old["version_id"])
    target["promotion_status"] = "champion"
    target["promoted_at"] = now
    target["promoted_by"] = "github-actions"
    save_version_metadata(root, args.pipeline_id, args.version_id, target)
    print(f"Promoted {args.version_id} to champion; archived={archived}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
