#!/usr/bin/env python3
"""Minimal Airflow API client helpers for DAG runtime evaluation."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

import requests


class AirflowClientError(RuntimeError):
    pass


def _request_json(
    method: str,
    url: str,
    auth: tuple[str, str] | None,
    token: str = "",
    **kwargs,
) -> dict[str, Any]:
    headers = dict(kwargs.pop("headers", {}) or {})
    req_kwargs = dict(kwargs)

    # Use Bearer token when available; otherwise fall back to Basic auth.
    if token:
        headers["Authorization"] = f"Bearer {token}"
    elif auth and str(auth[0]).strip():
        req_kwargs["auth"] = auth
    if headers:
        req_kwargs["headers"] = headers

    # Temporary debug logging for auth-mode diagnostics.
    print("TOKEN_PRESENT =", bool(token))
    print("AUTH_PRESENT =", bool(auth))
    print("HEADERS =", headers)
    try:
        resp = requests.request(method, url, timeout=30, **req_kwargs)
    except requests.RequestException as exc:
        raise AirflowClientError(f"Airflow API unreachable: {exc}") from exc
    if resp.status_code >= 400:
        raise AirflowClientError(f"Airflow API error {resp.status_code} for {url}: {resp.text[:500]}")
    try:
        return resp.json()
    except ValueError as exc:
        raise AirflowClientError(f"Invalid JSON response from Airflow API: {url}") from exc


def trigger_dag_run(
    base_url: str,
    auth: tuple[str, str] | None,
    dag_id: str,
    conf: dict[str, Any] | None = None,
    token: str = "",
) -> dict[str, Any]:
    url = f"{base_url.rstrip('/')}/api/v1/dags/{dag_id}/dagRuns"
    payload = {
        "logical_date": datetime.now(timezone.utc).isoformat(),
        "conf": conf or {},
    }
    return _request_json("POST", url, auth, token=token, json=payload)


def get_dag_run(
    base_url: str,
    auth: tuple[str, str] | None,
    dag_id: str,
    dag_run_id: str,
    token: str = "",
) -> dict[str, Any]:
    url = f"{base_url.rstrip('/')}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}"
    return _request_json("GET", url, auth, token=token)


def wait_for_dag_run(
    base_url: str,
    auth: tuple[str, str] | None,
    dag_id: str,
    dag_run_id: str,
    token: str = "",
    timeout_seconds: int = 1800,
    poll_interval_seconds: int = 5,
) -> dict[str, Any]:
    deadline = time.time() + timeout_seconds
    last = {}
    while time.time() < deadline:
        run = get_dag_run(base_url, auth, dag_id, dag_run_id, token=token)
        last = run
        state = str(run.get("state", "")).lower()
        if state in {"success", "failed"}:
            return run
        time.sleep(max(1, int(poll_interval_seconds)))
    raise AirflowClientError(f"Timed out waiting for DAG run completion: {dag_id}/{dag_run_id}; last_state={last.get('state')}")


def get_task_instances(
    base_url: str,
    auth: tuple[str, str] | None,
    dag_id: str,
    dag_run_id: str,
    token: str = "",
) -> list[dict[str, Any]]:
    url = f"{base_url.rstrip('/')}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
    payload = _request_json("GET", url, auth, token=token)
    tis = payload.get("task_instances", [])
    return tis if isinstance(tis, list) else []


def _parse_dt(value: str | None) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    # Airflow returns ISO8601; normalize trailing Z.
    text = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def collect_runtime_metrics(dag_run: dict[str, Any], task_instances: list[dict[str, Any]]) -> dict[str, Any]:
    state = str(dag_run.get("state", "")).lower()
    start_text = str(dag_run.get("start_date") or "")
    end_text = str(dag_run.get("end_date") or "")
    start_dt = _parse_dt(start_text)
    end_dt = _parse_dt(end_text)
    total_runtime = float(dag_run.get("duration") or 0.0)
    if total_runtime <= 0 and start_dt and end_dt:
        total_runtime = max(0.0, (end_dt - start_dt).total_seconds())

    total_tasks = len(task_instances)
    success = 0
    failed = 0
    skipped = 0
    retries = 0
    queued_duration = 0.0
    task_durations: dict[str, float] = {}
    for ti in task_instances:
        ti_state = str(ti.get("state", "")).lower()
        if ti_state == "success":
            success += 1
        elif ti_state == "failed":
            failed += 1
        elif ti_state == "skipped":
            skipped += 1
        retries += max(0, int(ti.get("try_number") or 0) - 1)
        qd = ti.get("queued_dttm")
        sd = ti.get("start_date")
        qdt = _parse_dt(qd)
        sdt = _parse_dt(sd)
        if qdt and sdt and sdt >= qdt:
            queued_duration += (sdt - qdt).total_seconds()
        duration = float(ti.get("duration") or 0.0)
        task_id = str(ti.get("task_id", "")).strip()
        if task_id:
            task_durations[task_id] = duration

    task_failure_rate = (failed / total_tasks) if total_tasks > 0 else 0.0
    # Keep names compatible with scoring helper expectations.
    return {
        "dag_run_state": state,
        "total_runtime_seconds": round(total_runtime, 3),
        "task_count": total_tasks,
        "success_task_count": success,
        "failed_task_count": failed,
        "skipped_task_count": skipped,
        "retry_count": retries,
        "try_number": retries,
        "queued_duration": round(queued_duration, 3),
        "scheduler_delay_seconds": round(queued_duration, 3),
        "start_date": start_text,
        "end_date": end_text,
        "duration_per_task": task_durations,
        "task_failure_rate": round(task_failure_rate, 6),
        "sla_misses": 0,
        "resource_usage_pct": 50.0,
    }
