#!/usr/bin/env python3
"""
Mark a DAG version as champion when promoted to prod.
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Mark DAG version as champion")
    parser.add_argument("--root", type=str, required=True, help="Repository root directory")
    parser.add_argument("--pipeline-id", type=str, required=True, help="Base pipeline ID")
    parser.add_argument("--version-id", type=str, required=True, help="Version ID to promote")
    args = parser.parse_args()
    
    root = Path(args.root)
    versions_file = root / "web_app" / "pipeline_versions_store.json"
    
    if not versions_file.exists():
        print(f"ERROR: Version store not found: {versions_file}")
        sys.exit(1)
    
    try:
        payload = json.loads(versions_file.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"ERROR: Could not read versions file: {e}")
        sys.exit(1)
    
    pipelines = payload.get("pipelines", {})
    entry = pipelines.get(args.pipeline_id)
    if not entry:
        print(f"ERROR: Pipeline not found: {args.pipeline_id}")
        sys.exit(1)
    
    versions = entry.get("versions", [])
    champion_updated = False
    archived_version_id = None
    
    # Archive previous champion
    for version in versions:
        if version.get("promotion_status") == "champion":
            archived_version_id = version.get("version_id")
            version["promotion_status"] = "archived"
            version["archived_at"] = datetime.now(timezone.utc).isoformat()
            print(f"Archived previous champion: {archived_version_id}")
    
    # Promote new champion
    for version in versions:
        if version.get("version_id") == args.version_id:
            version["promotion_status"] = "champion"
            version["promoted_at"] = datetime.now(timezone.utc).isoformat()
            version["promoted_by"] = "github-actions"
            champion_updated = True
            print(f"Promoted {args.version_id} to champion")
            break
    
    if not champion_updated:
        print(f"ERROR: Version not found: {args.version_id}")
        sys.exit(1)
    
    try:
        versions_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"Successfully marked {args.version_id} as champion")
    except Exception as e:
        print(f"ERROR: Could not write versions file: {e}")
        sys.exit(1)
    
    sys.exit(0)


if __name__ == "__main__":
    main()
