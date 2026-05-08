#!/usr/bin/env python3
"""Validate per-version metadata files."""

from __future__ import annotations

import argparse
from pathlib import Path

from version_metadata import METADATA_DIR, validate_metadata_file


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate metadata/*.json files")
    parser.add_argument("--root", type=Path, default=Path("."), help="Repository root")
    args = parser.parse_args()

    root = args.root.resolve()
    meta_root = root / METADATA_DIR
    if not meta_root.exists():
        print(f"Metadata directory missing: {meta_root}")
        return 1

    files = sorted(meta_root.glob("*/*.json"))
    if not files:
        print(f"No metadata files found in: {meta_root}")
        return 1

    bad = 0
    for path in files:
        try:
            validate_metadata_file(path)
        except Exception as exc:  # noqa: BLE001
            bad += 1
            print(f"INVALID: {path}: {exc}")
    if bad:
        print(f"Validation failed: {bad} invalid metadata file(s)")
        return 1
    print(f"Validation OK: {len(files)} metadata file(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
