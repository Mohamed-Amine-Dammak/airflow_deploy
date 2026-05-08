#!/usr/bin/env python3
"""
Mark DAG versions as 'eval' status when promoted to eval branch.
"""

import argparse
import json
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Mark DAG versions as eval")
    parser.add_argument("--root", type=str, required=True, help="Repository root directory")
    parser.add_argument("--pipeline-id", type=str, help="Base pipeline ID (optional)")
    parser.add_argument("--version-id", type=str, help="Specific version ID (optional)")
    args = parser.parse_args()
    
    root = Path(args.root)
    versions_file = root / "airflow" / "web_app_data" / "pipeline_versions_store.json"
    
    if not versions_file.exists():
        print(f"WARNING: Version store not found: {versions_file}")
        sys.exit(0)
    
    try:
        payload = json.loads(versions_file.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"ERROR: Could not read versions file: {e}")
        sys.exit(1)
    
    pipelines = payload.get("pipelines", {})
    updated_count = 0
    
    for pipeline_id, entry in pipelines.items():
        if args.pipeline_id and pipeline_id != args.pipeline_id:
            continue
        
        versions = entry.get("versions", [])
        for version in versions:
            if args.version_id and version.get("version_id") != args.version_id:
                continue
            
            current_status = str(version.get("promotion_status", "")).strip().lower()
            if current_status not in {"champion", "archived"}:
                version["promotion_status"] = "eval"
                updated_count += 1
                print(f"Marked {pipeline_id}/{version.get('version_id')} as eval")
    
    if updated_count > 0:
        try:
            versions_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            print(f"Updated {updated_count} version(s)")
        except Exception as e:
            print(f"ERROR: Could not write versions file: {e}")
            sys.exit(1)
    else:
        print("No versions updated")
    
    sys.exit(0)


if __name__ == "__main__":
    main()
