#!/usr/bin/env python3
"""Evaluate a DAG version and update version metadata with score/status."""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_eval_metrics_from_env() -> dict:
    raw = str(os.getenv("EVAL_METRICS_JSON", "")).strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate a DAG version")
    parser.add_argument("--root", type=str, required=True, help="Repository root directory")
    parser.add_argument("--pipeline-id", type=str, required=True, help="Base pipeline ID")
    parser.add_argument("--dag-id", type=str, required=True, help="DAG ID to evaluate")
    parser.add_argument("--evaluation-mode", type=str, default="static_only", help="Evaluation mode")
    parser.add_argument("--run-count", type=int, default=1, help="Number of runs")
    args = parser.parse_args()

    root = Path(args.root)
    versions_file = root / "web_app" / "pipeline_versions_store.json"
    if not versions_file.exists():
        print(f"ERROR: Version store not found: {versions_file}")
        sys.exit(1)

    web_app_dir = root / "web_app"
    if str(web_app_dir) not in sys.path:
        sys.path.insert(0, str(web_app_dir))

    from services.pipeline_versions import get_version, list_versions, patch_version_fields  # noqa: PLC0415
    from services.scoring.dag_score import score_dag_static_only, score_dag_with_eval_metrics  # noqa: PLC0415

    listing = list_versions(versions_file, args.pipeline_id)
    target_version = None
    for version in listing.get("versions", []):
        if str(version.get("airflow_dag_id", "")).strip() == str(args.dag_id).strip():
            target_version = version
            break

    if not target_version:
        print(f"ERROR: Could not find a version for pipeline '{args.pipeline_id}' with dag_id '{args.dag_id}'")
        sys.exit(1)

    version_id = str(target_version.get("version_id", "")).strip()
    if not version_id:
        print("ERROR: Target version is missing version_id")
        sys.exit(1)

    version = get_version(versions_file, args.pipeline_id, version_id)
    if not version:
        print(f"ERROR: Version not found: {args.pipeline_id}/{version_id}")
        sys.exit(1)

    local_path = Path(str(version.get("generated_local_path", "")).strip())
    if not local_path.exists():
        print(f"ERROR: DAG version file not found: {local_path}")
        sys.exit(1)

    dag_content = local_path.read_text(encoding="utf-8")
    evaluation_mode = str(args.evaluation_mode or "static_only").strip().lower()
    eval_metrics = _load_eval_metrics_from_env() if evaluation_mode != "static_only" else {}

    if eval_metrics:
        score_result = score_dag_with_eval_metrics(dag_content, eval_metrics, args.dag_id)
    else:
        score_result = score_dag_static_only(dag_content, args.dag_id)

    current_promotion_status = str(version.get("promotion_status", "")).strip().lower()
    next_promotion_status = "challenger"
    if current_promotion_status in {"champion", "archived"}:
        next_promotion_status = current_promotion_status

    eval_run_id = f"eval-{args.pipeline_id}-{version_id}-{int(datetime.now(timezone.utc).timestamp())}"
    patch_version_fields(
        versions_file,
        pipeline_id=args.pipeline_id,
        version_id=version_id,
        patch={
            "score": score_result.get("score"),
            "score_type": score_result.get("score_type"),
            "score_breakdown": score_result.get("breakdown", {}),
            "warnings": score_result.get("warnings", []),
            "critical_failures": score_result.get("critical_failures", []),
            "promotion_status": next_promotion_status,
            "last_eval_run_id": eval_run_id,
            "evaluated_at": _now_iso(),
            "evaluation_mode": evaluation_mode,
            "evaluation_run_count": int(args.run_count),
        },
    )

    eval_results_dir = root / "evaluation-results"
    eval_results_dir.mkdir(parents=True, exist_ok=True)
    result = {
        "dag_id": args.dag_id,
        "pipeline_id": args.pipeline_id,
        "version_id": version_id,
        "evaluation_mode": evaluation_mode,
        "run_count": int(args.run_count),
        "status": "completed",
        "score": score_result.get("score"),
        "score_type": score_result.get("score_type"),
        "breakdown": score_result.get("breakdown", {}),
        "warnings": score_result.get("warnings", []),
        "critical_failures": score_result.get("critical_failures", []),
        "last_eval_run_id": eval_run_id,
        "updated_store": str(versions_file),
    }

    result_file = eval_results_dir / f"eval_{args.pipeline_id}_{version_id}.json"
    result_file.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"Evaluation complete for {args.pipeline_id}/{version_id} with score={result.get('score')}")
    print(f"Evaluation results saved to {result_file}")


if __name__ == "__main__":
    main()
