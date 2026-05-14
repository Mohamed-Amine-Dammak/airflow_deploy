#!/usr/bin/env python3
"""Check promotion eligibility using per-version metadata files."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
METADATA_DIR = SCRIPT_DIR.parent / "metadata"
if str(METADATA_DIR) not in sys.path:
    sys.path.insert(0, str(METADATA_DIR))

from version_metadata import load_version_metadata, list_all_versions  # type: ignore


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _champion(rows: list[dict[str, Any]], base_pipeline_id: str) -> dict[str, Any] | None:
    for row in rows:
        if (
            str(row.get("base_pipeline_id", "")).strip() == base_pipeline_id
            and str(row.get("promotion_status", "")).strip().lower() == "champion"
        ):
            return row
    return None


def _score(value: Any) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


def _decision_payload(
    should_promote: bool,
    reason: str,
    pipeline_id: str,
    version_id: str,
    candidate_score: float,
    threshold: float,
    required_score: float | None,
    prod_champion: dict[str, Any] | None,
    blocking: list[str],
) -> dict[str, Any]:
    return {
        "should_promote": bool(should_promote),
        "reason": str(reason or "").strip(),
        "pipeline_id": pipeline_id,
        "candidate_version": version_id,
        "candidate_score": round(float(candidate_score), 3),
        "prod_champion_version": str(prod_champion.get("version_id") or "").strip() if prod_champion else None,
        "prod_champion_score": round(_score(prod_champion.get("score")), 3) if prod_champion else None,
        "threshold": float(threshold),
        "required_score": round(float(required_score), 3) if required_score is not None else None,
        "blocking_issues": list(blocking or []),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Check if a DAG version can be promoted to prod")
    parser.add_argument("--root", type=str, default="")
    parser.add_argument("--candidate-root", type=str, default="")
    parser.add_argument("--prod-root", type=str, default="")
    parser.add_argument("--pipeline-id", type=str, required=True)
    parser.add_argument("--version-id", type=str, required=True)
    parser.add_argument("--promotion-threshold", type=float, default=None)
    parser.add_argument("--allow-static-only", type=str, default=None)
    parser.add_argument(
        "--decision-out",
        type=Path,
        default=None,
        help="Optional JSON file to write the decision payload.",
    )
    args = parser.parse_args()

    root_fallback = Path(args.root).resolve() if str(args.root or "").strip() else None
    candidate_root = (
        Path(args.candidate_root).resolve()
        if str(args.candidate_root or "").strip()
        else (root_fallback or Path(".").resolve())
    )
    prod_root = (
        Path(args.prod_root).resolve()
        if str(args.prod_root or "").strip()
        else (root_fallback or candidate_root)
    )
    threshold = float(args.promotion_threshold if args.promotion_threshold is not None else os.getenv("PROMOTION_THRESHOLD", "0.05"))
    if args.allow_static_only is None:
        allow_static_only = _env_bool("ALLOW_STATIC_ONLY_PROMOTION", False)
    else:
        allow_static_only = str(args.allow_static_only).strip().lower() in {"1", "true", "yes", "y", "on"}

    try:
        challenger = load_version_metadata(candidate_root, args.pipeline_id, args.version_id)
    except FileNotFoundError:
        decision = _decision_payload(
            should_promote=False,
            reason="candidate_not_found",
            pipeline_id=args.pipeline_id,
            version_id=args.version_id,
            candidate_score=0.0,
            threshold=threshold,
            required_score=None,
            prod_champion=None,
            blocking=[f"Candidate metadata missing in candidate root: {args.pipeline_id}/{args.version_id}"],
        )
        if args.decision_out:
            args.decision_out.parent.mkdir(parents=True, exist_ok=True)
            args.decision_out.write_text(json.dumps(decision, indent=2), encoding="utf-8")
        print(f"ERROR: version file not found for {args.pipeline_id}/{args.version_id}")
        print(json.dumps(decision, indent=2))
        sys.exit(1)

    challenger_score = _score(challenger.get("score"))
    challenger_score_type = str(challenger.get("score_type") or "").strip()
    blocking: list[str] = []
    if challenger_score_type == "static_only" and not allow_static_only:
        blocking.append("Challenger score is static-only; auto-promotion not allowed by policy.")
    if list(challenger.get("critical_failures") or []):
        blocking.append("Challenger has critical failures.")

    rows = list_all_versions(prod_root)
    base_id = str(challenger.get("base_pipeline_id") or args.pipeline_id).strip()
    champ = _champion(rows, base_id)
    required_score: float | None = None
    should_promote = False
    reason = "blocked_by_issues" if blocking else "unknown"

    if not champ:
        should_promote = not blocking
        reason = "no_current_prod_champion" if should_promote else "blocked_by_issues"
    else:
        champion_score = _score(champ.get("score"))
        required_score = champion_score * (1.0 + threshold)
        should_promote = challenger_score >= required_score and not blocking
        reason = "challenger_exceeds_threshold" if should_promote else ("blocked_by_issues" if blocking else "score_below_prod_champion_threshold")

    decision = _decision_payload(
        should_promote=should_promote,
        reason=reason,
        pipeline_id=args.pipeline_id,
        version_id=args.version_id,
        candidate_score=challenger_score,
        threshold=threshold,
        required_score=required_score,
        prod_champion=champ,
        blocking=blocking,
    )
    if args.decision_out:
        args.decision_out.parent.mkdir(parents=True, exist_ok=True)
        args.decision_out.write_text(json.dumps(decision, indent=2), encoding="utf-8")

    print(
        "Promotion check:",
        f"should_promote={should_promote}",
        f"reason={reason}",
        f"challenger_score={challenger_score}",
        f"champion_score={decision.get('prod_champion_score')}",
        f"threshold_pct={threshold*100}",
        f"required_score={decision.get('required_score')}",
    )
    if blocking:
        print("Blocking issues:", "; ".join(blocking))
    print(json.dumps(decision, indent=2))
    if not should_promote:
        sys.exit(1)


if __name__ == "__main__":
    main()
