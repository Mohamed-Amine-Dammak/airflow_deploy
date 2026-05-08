#!/usr/bin/env python3
"""Evaluate a DAG version and persist score metadata in the canonical store."""

from __future__ import annotations

import argparse
import ast
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_store(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"pipelines": {}}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return {"pipelines": {}}
    if not isinstance(payload.get("pipelines"), dict):
        payload["pipelines"] = {}
    return payload


def save_store(path: Path, store: dict[str, Any]) -> None:
    normalized = dict(store if isinstance(store, dict) else {})
    if not isinstance(normalized.get("pipelines"), dict):
        normalized["pipelines"] = {}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(normalized, indent=2), encoding="utf-8")


def find_version(store: dict[str, Any], pipeline_id: str, dag_or_version_id: str) -> tuple[dict[str, Any] | None, str | None]:
    safe_pid = str(pipeline_id or "").strip()
    safe_key = str(dag_or_version_id or "").strip()
    entry = store.get("pipelines", {}).get(safe_pid)
    if not isinstance(entry, dict):
        return None, None
    versions = entry.get("versions", [])
    if not isinstance(versions, list):
        return None, None
    for version in versions:
        if not isinstance(version, dict):
            continue
        candidates = {
            str(version.get("local_dag_id", "")).strip(),
            str(version.get("git_dag_id", "")).strip(),
            str(version.get("dag_id", "")).strip(),
            str(version.get("airflow_dag_id", "")).strip(),
            str(version.get("version_id", "")).strip(),
        }
        if safe_key in candidates:
            return version, str(version.get("version_id", "")).strip()
    return None, None


def patch_version(version: dict[str, Any], fields: dict[str, Any]) -> None:
    version.update(dict(fields))


def _load_eval_metrics_from_env() -> dict[str, Any]:
    raw = str(os.getenv("EVAL_METRICS_JSON", "")).strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_ok(content: str) -> tuple[bool, list[str]]:
    errs: list[str] = []
    try:
        ast.parse(content)
        return True, errs
    except SyntaxError as exc:
        errs.append(f"SyntaxError: {exc}")
    except Exception as exc:
        errs.append(f"ParseError: {exc}")
    return False, errs


def _score_static(dag_content: str) -> dict[str, Any]:
    text = str(dag_content or "")
    parse_success, parse_errors = _parse_ok(text)
    if not parse_success:
        return {
            "score": 0.0,
            "score_type": "static_only",
            "breakdown": {
                "reliability": 0.0,
                "runtime_efficiency": 0.0,
                "retry_behavior": 0.0,
                "scheduler_behavior": 0.0,
                "complexity": 0.0,
                "maintainability": 0.0,
                "resource_efficiency": 0.0,
            },
            "warnings": parse_errors,
            "critical_failures": ["DAG cannot be parsed as valid Python syntax."],
        }

    lower = text.lower()
    has_owner = "owner" in lower
    has_tags = "tags" in lower
    has_retries = "retries" in lower
    has_timeout = "timeout" in lower
    task_count = text.count("Operator(") + text.count("Task(")

    props_score = 0.0
    for flag in (has_owner, has_tags, has_retries, has_timeout):
        if flag:
            props_score += 2.5

    breakdown = {
        "reliability": 25.0 if has_owner and has_retries else 15.0,
        "runtime_efficiency": 20.0,
        "retry_behavior": 15.0 if has_retries else 5.0,
        "scheduler_behavior": 15.0 if has_owner and has_tags else 8.0,
        "complexity": max(0.0, 10.0 - min(2.0, task_count * 0.2)),
        "maintainability": 10.0,
        "resource_efficiency": min(5.0, props_score * 0.2),
    }
    total = round(sum(float(v) for v in breakdown.values()), 1)
    warnings: list[str] = []
    if task_count == 0:
        warnings.append("No tasks detected; DAG may be empty.")
    return {
        "score": total,
        "score_type": "static_only",
        "breakdown": {k: round(float(v), 1) for k, v in breakdown.items()},
        "warnings": warnings,
        "critical_failures": [],
    }


def _score_with_eval_metrics(dag_content: str, eval_metrics: dict[str, Any]) -> dict[str, Any]:
    static_result = _score_static(dag_content)
    if static_result.get("critical_failures"):
        return {**static_result, "score_type": "eval_blocked"}
    if not eval_metrics:
        return {**static_result, "score_type": "eval_incomplete"}

    breakdown = dict(static_result.get("breakdown", {}))
    task_failure_rate = float(eval_metrics.get("task_failure_rate", 0.5))
    retry_count = int(eval_metrics.get("retry_count", 0))
    runtime = float(eval_metrics.get("total_runtime_seconds", 300))
    scheduler_delay = float(eval_metrics.get("scheduler_delay_seconds", 0))
    sla_misses = int(eval_metrics.get("sla_misses", 0))
    resource_usage = float(eval_metrics.get("resource_usage_pct", 50))

    breakdown["reliability"] = min(25.0, 25.0 * max(0.0, 1.0 - task_failure_rate))
    breakdown["retry_behavior"] = min(15.0, 15.0 * max(0.0, 1.0 - (retry_count * 0.05)))
    breakdown["runtime_efficiency"] = min(20.0, 20.0 * max(0.0, 1.0 - (runtime / 600.0)))
    scheduler = 15.0 * max(0.0, 1.0 - (scheduler_delay / 600.0))
    if sla_misses > 0:
        scheduler -= float(sla_misses) * 2.0
    breakdown["scheduler_behavior"] = max(0.0, min(15.0, scheduler))
    breakdown["resource_efficiency"] = min(5.0, 5.0 * max(0.0, 1.0 - (resource_usage / 100.0)))

    total = round(sum(float(v) for v in breakdown.values()), 1)
    return {
        **static_result,
        "score": total,
        "score_type": "eval_complete",
        "breakdown": {k: round(float(v), 1) for k, v in breakdown.items()},
    }


def _resolve_dag_file(root: Path, version: dict[str, Any], evaluation_mode: str) -> Path:
    # GitHub eval branch should prefer git DAG file.
    if evaluation_mode != "local_only":
        git_file = str(version.get("git_dag_file", "") or version.get("repo_file_path", "")).strip()
        if git_file:
            candidate = root / git_file
            if candidate.exists():
                return candidate
    local_file = str(version.get("local_dag_file", "")).strip()
    if local_file:
        candidate = root / local_file
        if candidate.exists():
            return candidate
    legacy = str(version.get("generated_local_path", "")).strip()
    if legacy:
        candidate = Path(legacy)
        if candidate.exists():
            return candidate
    raise FileNotFoundError("No readable DAG file found from git_dag_file/local_dag_file/generated_local_path")


def _validate_json_with_tool(path: Path) -> None:
    subprocess.run([sys.executable, "-m", "json.tool", str(path)], check=True, stdout=subprocess.DEVNULL)


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate a DAG version")
    parser.add_argument("--root", type=str, required=True, help="Repository root directory")
    parser.add_argument("--pipeline-id", type=str, required=True, help="Base pipeline ID")
    parser.add_argument("--dag-id", type=str, required=True, help="DAG ID or version ID to evaluate")
    parser.add_argument("--evaluation-mode", type=str, default="static_only", help="Evaluation mode")
    parser.add_argument("--run-count", type=int, default=1, help="Number of runs")
    args = parser.parse_args()

    root = Path(args.root)
    versions_file = root / "airflow" / "web_app_data" / "pipeline_versions_store.json"
    if not versions_file.exists():
        print(f"ERROR: Version store not found: {versions_file}")
        sys.exit(1)

    store = load_store(versions_file)
    version, version_id = find_version(store, args.pipeline_id, args.dag_id)
    if not version or not version_id:
        print(f"ERROR: Could not find version for pipeline '{args.pipeline_id}' with key '{args.dag_id}'")
        sys.exit(1)

    evaluation_mode = str(args.evaluation_mode or "static_only").strip().lower()
    dag_path = _resolve_dag_file(root, version, evaluation_mode)
    dag_content = dag_path.read_text(encoding="utf-8")

    eval_metrics = _load_eval_metrics_from_env() if evaluation_mode != "static_only" else {}
    if eval_metrics:
        score_result = _score_with_eval_metrics(dag_content, eval_metrics)
    else:
        score_result = _score_static(dag_content)

    current = str(version.get("promotion_status", "")).strip().lower()
    next_status = "challenger" if current not in {"champion", "archived"} else current
    eval_run_id = f"eval-{args.pipeline_id}-{version_id}-{int(datetime.now(timezone.utc).timestamp())}"
    patch_version(
        version,
        {
            "score": score_result.get("score"),
            "score_type": score_result.get("score_type"),
            "score_breakdown": score_result.get("breakdown", {}),
            "warnings": score_result.get("warnings", []),
            "critical_failures": score_result.get("critical_failures", []),
            "promotion_status": next_status,
            "deployment_target": "git",
            "last_evaluated_at": _now_iso(),
            "last_eval_run_id": eval_run_id,
        },
    )
    save_store(versions_file, store)
    _validate_json_with_tool(versions_file)

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
        "dag_file_used": str(dag_path),
    }
    result_file = eval_results_dir / f"eval_{args.pipeline_id}_{version_id}.json"
    result_file.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"Evaluation complete for {args.pipeline_id}/{version_id} with score={result.get('score')}")
    print(f"Evaluation results saved to {result_file}")


if __name__ == "__main__":
    main()
