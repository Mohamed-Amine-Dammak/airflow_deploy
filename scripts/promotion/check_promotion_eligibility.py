#!/usr/bin/env python3
"""Check promotion eligibility using per-version metadata files."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
METADATA_DIR = SCRIPT_DIR.parent / "metadata"
if str(METADATA_DIR) not in sys.path:
    sys.path.insert(0, str(METADATA_DIR))

from version_metadata import load_version_metadata, list_pipeline_versions  # type: ignore


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _champion(rows: list[dict], base_pipeline_id: str) -> dict | None:
    for row in rows:
        if str(row.get("base_pipeline_id", "")).strip() == base_pipeline_id and str(row.get("promotion_status", "")).strip() == "champion":
            return row
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Check if a DAG version can be promoted to prod")
    parser.add_argument("--root", type=str, required=True)
    parser.add_argument("--pipeline-id", type=str, required=True)
    parser.add_argument("--version-id", type=str, required=True)
    parser.add_argument("--promotion-threshold", type=float, default=None)
    parser.add_argument("--allow-static-only", type=str, default=None)
    args = parser.parse_args()

    root = Path(args.root)
    threshold = float(args.promotion_threshold if args.promotion_threshold is not None else os.getenv("PROMOTION_THRESHOLD", "0.05"))
    if args.allow_static_only is None:
        allow_static_only = _env_bool("ALLOW_STATIC_ONLY_PROMOTION", False)
    else:
        allow_static_only = str(args.allow_static_only).strip().lower() in {"1", "true", "yes", "y", "on"}

    try:
        challenger = load_version_metadata(root, args.pipeline_id, args.version_id)
    except FileNotFoundError:
        print(f"ERROR: version file not found for {args.pipeline_id}/{args.version_id}")
        sys.exit(1)

    challenger_score = float(challenger.get("score") or 0.0)
    challenger_score_type = str(challenger.get("score_type") or "").strip()
    blocking: list[str] = []
    if challenger_score_type == "static_only" and not allow_static_only:
        blocking.append("Challenger score is static-only; auto-promotion not allowed by policy.")
    if list(challenger.get("critical_failures") or []):
        blocking.append("Challenger has critical failures.")

    rows = list_pipeline_versions(root, args.pipeline_id)
    base_id = str(challenger.get("base_pipeline_id") or args.pipeline_id).strip()
    champ = _champion(rows, base_id)
    if not champ:
        ok = not blocking
        print(f"Promotion check: should_promote={ok} reason={'no_current_champion' if ok else 'blocked_by_issues'} challenger_score={challenger_score}")
        if blocking:
            print("Blocking issues:", "; ".join(blocking))
        if not ok:
            sys.exit(1)
        return

    champion_score = float(champ.get("score") or 0.0)
    threshold_min = champion_score * (1.0 + threshold)
    should_promote = challenger_score >= threshold_min and not blocking
    reason = "challenger_exceeds_threshold" if should_promote else ("blocked_by_issues" if blocking else "challenger_below_threshold")
    print(
        "Promotion check:",
        f"should_promote={should_promote}",
        f"reason={reason}",
        f"challenger_score={challenger_score}",
        f"champion_score={champion_score}",
        f"threshold_pct={threshold*100}",
    )
    if blocking:
        print("Blocking issues:", "; ".join(blocking))
    if not should_promote:
        sys.exit(1)


if __name__ == "__main__":
    main()
