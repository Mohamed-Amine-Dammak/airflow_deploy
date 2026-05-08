#!/usr/bin/env python3
"""
Copy DAG version from eval branch to prod branch.
"""

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Copy DAG to prod branch")
    parser.add_argument("--root", type=str, required=True, help="Repository root directory")
    parser.add_argument("--pipeline-id", type=str, required=True, help="Base pipeline ID")
    parser.add_argument("--version-id", type=str, required=True, help="Version ID to promote")
    parser.add_argument("--eval-branch", type=str, default="eval", help="Eval branch name")
    parser.add_argument("--prod-branch", type=str, default="prod", help="Prod branch name")
    args = parser.parse_args()
    
    root = Path(args.root)
    versions_file = root / "airflow" / "web_app_data" / "pipeline_versions_store.json"
    dags_dir = root / "dags"
    
    if not versions_file.exists():
        print(f"ERROR: Version store not found: {versions_file}")
        sys.exit(1)
    
    try:
        payload = json.loads(versions_file.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"ERROR: Could not read versions file: {e}")
        sys.exit(1)
    
    # Find the version
    pipelines = payload.get("pipelines", {})
    entry = pipelines.get(args.pipeline_id)
    if not entry:
        print(f"ERROR: Pipeline not found: {args.pipeline_id}")
        sys.exit(1)
    
    target_version = None
    for version in entry.get("versions", []):
        if version.get("version_id") == args.version_id:
            target_version = version
            break
    
    if not target_version:
        print(f"ERROR: Version not found: {args.version_id}")
        sys.exit(1)
    
    output_filename = target_version.get("output_filename")
    if not output_filename:
        print(f"ERROR: Version has no output_filename")
        sys.exit(1)
    
    # Get DAG from eval branch
    try:
        # Fetch eval branch content
        result = subprocess.run(
            ["git", "show", f"{args.eval_branch}:dags/{output_filename}"],
            cwd=str(root),
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            print(f"ERROR: Could not fetch {output_filename} from {args.eval_branch} branch: {result.stderr}")
            sys.exit(1)
        
        dag_content = result.stdout
    except Exception as e:
        print(f"ERROR: Failed to get DAG from eval branch: {e}")
        sys.exit(1)
    
    # Write to prod branch working tree
    prod_dag_path = dags_dir / output_filename
    try:
        prod_dag_path.parent.mkdir(parents=True, exist_ok=True)
        prod_dag_path.write_text(dag_content, encoding="utf-8")
        print(f"Copied {output_filename} to prod working tree")
    except Exception as e:
        print(f"ERROR: Could not write DAG file: {e}")
        sys.exit(1)
    
    # Stage and commit
    try:
        subprocess.run(
            ["git", "add", f"dags/{output_filename}"],
            cwd=str(root),
            check=True,
        )
        subprocess.run(
            ["git", "commit", "-m", f"Promote {args.version_id} to production"],
            cwd=str(root),
            check=False,
        )
        print(f"Committed {output_filename} to prod branch")
    except Exception as e:
        print(f"ERROR: Git operations failed: {e}")
        sys.exit(1)
    
    sys.exit(0)


if __name__ == "__main__":
    main()
