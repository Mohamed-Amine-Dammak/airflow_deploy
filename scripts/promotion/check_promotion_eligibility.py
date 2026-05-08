#!/usr/bin/env python3
"""Block prod promotion when challenger does not satisfy policy."""

import argparse
import os
import sys
from pathlib import Path


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def main() -> None:
    parser = argparse.ArgumentParser(description="Check if a DAG version can be promoted to prod")
    parser.add_argument("--root", type=str, required=True, help="Repository root directory")
    parser.add_argument("--pipeline-id", type=str, required=True, help="Base pipeline ID")
    parser.add_argument("--version-id", type=str, required=True, help="Version ID to evaluate for promotion")
    parser.add_argument("--promotion-threshold", type=float, default=None, help="Required relative improvement (e.g. 0.05)")
    parser.add_argument(
        "--allow-static-only",
        type=str,
        default=None,
        help="Allow static_only score_type for promotion (true/false).",
    )
    args = parser.parse_args()

    root = Path(args.root)
    versions_file = root / "web_app" / "pipeline_versions_store.json"
    if not versions_file.exists():
        print(f"ERROR: Version store not found: {versions_file}")
        sys.exit(1)

    web_app_dir = root / "web_app"
    if str(web_app_dir) not in sys.path:
        sys.path.insert(0, str(web_app_dir))

    from services.promotion import compare_versions  # noqa: PLC0415

    threshold = args.promotion_threshold
    if threshold is None:
        threshold = float(os.getenv("PROMOTION_THRESHOLD", "0.05"))

    if args.allow_static_only is None:
        allow_static_only = _env_bool("ALLOW_STATIC_ONLY_PROMOTION", False)
    else:
        allow_static_only = str(args.allow_static_only).strip().lower() in {"1", "true", "yes", "y", "on"}

    comparison = compare_versions(
        versions_file,
        args.pipeline_id,
        args.version_id,
        promotion_threshold=float(threshold),
        allow_static_only=allow_static_only,
    )

    print(
        "Promotion check:",
        f"should_promote={comparison.get('should_promote')}",
        f"reason={comparison.get('reason')}",
        f"challenger_score={comparison.get('challenger_score')}",
        f"champion_score={comparison.get('current_champion_score')}",
        f"threshold_pct={comparison.get('threshold_pct')}",
    )
    blocking = comparison.get("blocking_issues", [])
    if blocking:
        print("Blocking issues:", "; ".join(str(item) for item in blocking))

    if not comparison.get("should_promote"):
        sys.exit(1)


if __name__ == "__main__":
    main()
