#!/usr/bin/env python3
"""
Validate DAG metadata and structure for promotion system.
"""

import argparse
import ast
import json
import sys
from pathlib import Path


def validate_dag_file(dag_path: Path) -> tuple[bool, list[str]]:
    """
    Validate a single DAG file for syntax and metadata.
    Returns (is_valid, errors).
    """
    errors = []
    
    if not dag_path.exists():
        return False, [f"File not found: {dag_path}"]
    
    try:
        content = dag_path.read_text(encoding="utf-8")
    except Exception as e:
        return False, [f"Could not read file: {e}"]
    
    # Check syntax
    try:
        ast.parse(content)
    except SyntaxError as e:
        errors.append(f"Syntax error in {dag_path.name}: {e}")
        return False, errors
    
    # Check for DAG instantiation (basic heuristic)
    if "DAG(" not in content and "dag" not in content.lower():
        errors.append(f"No DAG definition found in {dag_path.name}")
    
    return len(errors) == 0, errors


def main():
    parser = argparse.ArgumentParser(description="Validate DAG metadata and structure")
    parser.add_argument("--dags-dir", type=str, required=True, help="Path to DAGs directory")
    parser.add_argument("--strict", action="store_true", help="Fail on any warning")
    args = parser.parse_args()
    
    dags_dir = Path(args.dags_dir)
    if not dags_dir.exists():
        print(f"ERROR: DAGs directory not found: {dags_dir}")
        sys.exit(1)
    
    # Find all DAG files
    dag_files = list(dags_dir.glob("**/*.py"))
    if not dag_files:
        print("WARNING: No DAG files found")
        sys.exit(0 if not args.strict else 1)
    
    all_valid = True
    total_errors = []
    
    for dag_file in dag_files:
        # Skip __pycache__ and hidden files
        if "__pycache__" in str(dag_file) or dag_file.name.startswith("."):
            continue
        
        is_valid, errors = validate_dag_file(dag_file)
        if not is_valid:
            all_valid = False
            total_errors.extend(errors)
            print(f"FAIL: {dag_file.name}")
            for error in errors:
                print(f"  - {error}")
        else:
            print(f"PASS: {dag_file.name}")
    
    if not all_valid:
        print(f"\n{len(total_errors)} validation error(s) found")
        sys.exit(1)
    
    print(f"\nAll {len(dag_files)} DAG files validated successfully")
    sys.exit(0)


if __name__ == "__main__":
    main()
