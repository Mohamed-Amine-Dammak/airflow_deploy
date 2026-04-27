from __future__ import annotations

import ast
import base64
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
from requests import RequestException


@dataclass
class AirflowApiConfig:
    base_url: str
    username: str
    password: str
    verify_tls: bool = True
    timeout_seconds: int = 10


class AirflowApiError(RuntimeError):
    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


def _safe_json(resp: requests.Response) -> dict[str, Any]:
    if not resp.text:
        return {}
    try:
        data = resp.json()
    except ValueError:
        return {}
    return data if isinstance(data, dict) else {}


def _to_pretty_json(value: Any) -> str:
    try:
        return json.dumps(value, indent=2, ensure_ascii=False, sort_keys=False)
    except TypeError:
        return str(value)


def _looks_like_json_fragment_line(value: str) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    if text in {"{", "}", "[", "]", "{,", "},", "],"}:
        return True
    if text.startswith('"') or text.startswith("}"):
        return True
    return False


def _render_fragment_block(fragment_lines: list[str], block_idx: int) -> list[str]:
    lines: list[str] = []
    if not fragment_lines:
        return lines

    lines.append("")
    lines.append(f"[N8N Payload Block {block_idx}] {len(fragment_lines)} lines")
    raw = "\n".join(fragment_lines)
    pretty = None
    try:
        pretty_obj = json.loads(raw)
        pretty = _to_pretty_json(pretty_obj)
    except Exception:  # noqa: BLE001
        try:
            pretty_obj = ast.literal_eval(raw)
            pretty = _to_pretty_json(pretty_obj)
        except Exception:  # noqa: BLE001
            pretty = None

    source = pretty if pretty is not None else raw
    for row in source.splitlines():
        lines.append(f"  {row}")
    return lines


def _should_hide_structured_entry(item: dict[str, Any]) -> bool:
    event = str(item.get("event") or "").strip()
    logger = str(item.get("logger") or "").strip()
    level = str(item.get("level") or "").strip().lower()

    if level == "error":
        return False

    if event.startswith("::group::") or event.startswith("::endgroup::"):
        return True

    if logger in {
        "airflow.dag_processing.bundles.manager.DagBundlesManager",
        "airflow.models.dagbag.BundleDagBag",
    }:
        return True

    if logger == "py.warnings" and "deprecated" in event.lower():
        return True

    if logger == "task.stdout":
        noisy_prefixes = (
            "Task instance is in running state",
            "Previous state of the Task instance:",
            "Current task name:",
            "Dag name:",
            "Task instance in failure state",
            "Task start",
            "Task:<",
        )
        normalized = event.lstrip()
        if normalized.startswith(noisy_prefixes):
            return True

    return False


def _coerce_structured_log_payload(value: Any) -> dict[str, Any] | list[Any] | None:
    if isinstance(value, (dict, list)):
        return value
    if not isinstance(value, str):
        return None

    text = value.strip()
    if not text or text[0] not in {"{", "["}:
        return None

    try:
        parsed_json = json.loads(text)
        if isinstance(parsed_json, (dict, list)):
            return parsed_json
    except Exception:  # noqa: BLE001
        pass

    try:
        parsed_literal = ast.literal_eval(text)
        if isinstance(parsed_literal, (dict, list)):
            return parsed_literal
    except Exception:  # noqa: BLE001
        pass

    return None


def _format_structured_log_payload(payload: dict[str, Any] | list[Any]) -> str:
    lines: list[str] = []
    if isinstance(payload, list):
        lines.append(f"Task log entries: {len(payload)}")
        fragment_buffer: list[str] = []
        fragment_block_count = 0
        hidden_count = 0

        def flush_fragments() -> None:
            nonlocal fragment_buffer, fragment_block_count
            if not fragment_buffer:
                return
            fragment_block_count += 1
            lines.extend(_render_fragment_block(fragment_buffer, fragment_block_count))
            fragment_buffer = []

        for idx, item in enumerate(payload, start=1):
            if not isinstance(item, dict):
                flush_fragments()
                lines.append(f"[{idx}] {_to_pretty_json(item)}")
                continue
            if _should_hide_structured_entry(item):
                hidden_count += 1
                continue

            event = str(item.get("event") or "")
            logger = str(item.get("logger") or "")
            if logger == "task.stdout" and _looks_like_json_fragment_line(event):
                fragment_buffer.append(event)
                continue

            flush_fragments()
            timestamp = item.get("timestamp") or "-"
            level = str(item.get("level") or "info").upper()
            message = event or "-"
            lines.append(f"[{idx}] {timestamp} {level} {logger} | {message}")

            extra_keys = [key for key in item.keys() if key not in {"timestamp", "event", "level", "logger"}]
            for key in extra_keys:
                value = item.get(key)
                if isinstance(value, (dict, list)):
                    lines.append(f"  {key}:")
                    for raw_line in _to_pretty_json(value).splitlines():
                        lines.append(f"    {raw_line}")
                else:
                    lines.append(f"  {key}: {value}")

        flush_fragments()
        if hidden_count:
            lines.append("")
            lines.append(f"[hidden] {hidden_count} boilerplate entries")
    else:
        lines.append("Structured log payload:")
        for key, value in payload.items():
            if isinstance(value, (dict, list)):
                lines.append(f"{key}:")
                for raw_line in _to_pretty_json(value).splitlines():
                    lines.append("  " + raw_line)
            else:
                lines.append(f"{key}: {value}")

    return "\n".join(lines)


def _request_connection_api(
    config: AirflowApiConfig,
    method: str,
    path_suffix: str,
    headers: dict[str, str],
    json_payload: dict[str, Any] | None = None,
    expected_statuses: set[int] | None = None,
) -> requests.Response:
    suffix = path_suffix if path_suffix.startswith("/") else f"/{path_suffix}"
    versions = ("/api/v2", "/api/v1")
    last_resp: requests.Response | None = None

    for version in versions:
        url = f"{config.base_url}{version}{suffix}"
        try:
            resp = requests.request(
                method=method.upper(),
                url=url,
                headers=headers,
                json=json_payload,
                timeout=config.timeout_seconds,
                verify=config.verify_tls,
            )
        except RequestException as exc:
            raise AirflowApiError(f"Unable to reach Airflow connections endpoint: {exc}") from exc

        if resp.status_code == 404:
            last_resp = resp
            continue

        if expected_statuses and resp.status_code not in expected_statuses:
            raise AirflowApiError(
                f"Airflow connection API failed: {resp.status_code} {resp.text}",
                status_code=resp.status_code,
            )
        return resp

    if last_resp is not None:
        raise AirflowApiError(
            f"Airflow connection API path not found for '{suffix}' (checked /api/v2 and /api/v1).",
            status_code=404,
        )
    raise AirflowApiError(f"Airflow connection API call failed for '{suffix}'.")


def _normalize_connection_item(item: dict[str, Any]) -> dict[str, Any]:
    conn_type = str(item.get("conn_type") or "").lower()
    normalized_type = conn_type
    if conn_type in {"azure_blob_storage", "wasb"}:
        normalized_type = "wasb"

    return {
        "connection_id": item.get("connection_id"),
        "conn_type": conn_type,
        "normalized_type": normalized_type,
        "description": item.get("description") or "",
        "host": item.get("host") or "",
        "login": item.get("login") or "",
        "port": item.get("port"),
        "schema": item.get("schema") or "",
        "extra": item.get("extra"),
        "raw": item,
    }


def _build_auth_header(config: AirflowApiConfig) -> dict[str, str]:
    def _basic_auth_header() -> dict[str, str]:
        raw = f"{config.username}:{config.password}".encode("utf-8")
        token = base64.b64encode(raw).decode("ascii")
        return {"Authorization": f"Basic {token}"}

    # Primary mode: token auth endpoint (supported in some deployments).
    try:
        token_resp = requests.post(
            f"{config.base_url}/auth/token",
            json={"username": config.username, "password": config.password},
            timeout=config.timeout_seconds,
            verify=config.verify_tls,
        )
    except RequestException as exc:
        # Fallback: some Airflow setups rely on basic auth without token endpoint.
        return _basic_auth_header()

    if token_resp.status_code in (200, 201):
        access_token = token_resp.json().get("access_token")
        if access_token:
            return {"Authorization": f"Bearer {access_token}"}

    # Token endpoint failed (often 404/405/500 depending on auth backend). Try basic auth.
    return _basic_auth_header()


def _auth_header_candidates(config: AirflowApiConfig) -> list[dict[str, str]]:
    # Prefer Basic first for compatibility with Airflow deployments where
    # write endpoints reject bearer tokens but accept basic auth.
    basic_raw = f"{config.username}:{config.password}".encode("utf-8")
    basic_token = base64.b64encode(basic_raw).decode("ascii")
    basic = {"Authorization": f"Basic {basic_token}"}
    discovered = _build_auth_header(config)
    ordered = [basic, discovered]
    unique: list[dict[str, str]] = []
    for item in ordered:
        auth = str(item.get("Authorization") or "").strip()
        if not auth:
            continue
        if any(str(existing.get("Authorization") or "").strip() == auth for existing in unique):
            continue
        unique.append({"Authorization": auth})
    return unique


def _get_dag_details(config: AirflowApiConfig, dag_id: str, headers: dict[str, str]) -> dict[str, Any] | None:
    try:
        dag_resp = requests.get(
            f"{config.base_url}/api/v2/dags/{dag_id}",
            headers=headers,
            timeout=config.timeout_seconds,
            verify=config.verify_tls,
        )
    except RequestException as exc:
        raise AirflowApiError(f"Unable to reach Airflow DAG endpoint: {exc}") from exc
    if dag_resp.status_code == 404:
        return None
    if dag_resp.status_code != 200:
        raise AirflowApiError(
            f"Airflow DAG lookup failed: {dag_resp.status_code} {dag_resp.text}",
            status_code=dag_resp.status_code,
        )
    return dag_resp.json() if dag_resp.text else {}


def _unpause_dag_if_needed(
    config: AirflowApiConfig,
    dag_id: str,
    headers: dict[str, str],
    dag_details: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not dag_details:
        return dag_details
    if not dag_details.get("is_paused"):
        return dag_details

    try:
        patch_resp = requests.patch(
            f"{config.base_url}/api/v2/dags/{dag_id}",
            headers={**headers, "Content-Type": "application/json"},
            json={"is_paused": False},
            timeout=config.timeout_seconds,
            verify=config.verify_tls,
        )
    except RequestException as exc:
        raise AirflowApiError(f"Unable to reach Airflow DAG unpause endpoint: {exc}") from exc
    if patch_resp.status_code not in (200, 204):
        raise AirflowApiError(
            f"Failed to unpause DAG '{dag_id}': {patch_resp.status_code} {patch_resp.text}",
            status_code=patch_resp.status_code,
        )

    return _get_dag_details(config, dag_id, headers)


def check_dag_readiness(
    config: AirflowApiConfig,
    dag_id: str,
    airflow_dag_path: str | None,
    auto_unpause: bool = True,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "dag_id": dag_id,
        "file_exists": False,
        "airflow_registered": False,
        "airflow_reachable": False,
        "is_paused": None,
        "ready": False,
        "message": "Not generated",
    }

    if airflow_dag_path:
        result["file_exists"] = Path(airflow_dag_path).exists()

    headers = _build_auth_header(config)
    result["airflow_reachable"] = True

    dag_details = _get_dag_details(config, dag_id, headers)
    if not dag_details:
        result["message"] = "Waiting for Airflow DAG registration."
        return result

    result["airflow_registered"] = True
    result["is_paused"] = bool(dag_details.get("is_paused"))

    if auto_unpause and result["is_paused"]:
        dag_details = _unpause_dag_if_needed(config, dag_id, headers, dag_details)
        result["is_paused"] = bool((dag_details or {}).get("is_paused"))

    result["ready"] = bool(result["file_exists"] and result["airflow_registered"] and not result["is_paused"])
    result["message"] = "Ready to run" if result["ready"] else "Waiting for Airflow"
    return result


def trigger_dag_run(
    config: AirflowApiConfig,
    dag_id: str,
    conf: dict[str, Any] | None = None,
    logical_date: str | None = None,
    auto_unpause: bool = True,
) -> dict[str, Any]:
    headers = _build_auth_header(config)
    dag_details = _get_dag_details(config, dag_id, headers)
    if not dag_details:
        raise AirflowApiError(f"DAG '{dag_id}' is not registered in Airflow yet.", status_code=404)

    if auto_unpause:
        dag_details = _unpause_dag_if_needed(config, dag_id, headers, dag_details)

    payload = {
        "logical_date": logical_date or datetime.now(timezone.utc).isoformat(),
        "conf": conf or {},
    }

    try:
        run_resp = requests.post(
            f"{config.base_url}/api/v2/dags/{dag_id}/dagRuns",
            headers={**headers, "Content-Type": "application/json"},
            json=payload,
            timeout=config.timeout_seconds,
            verify=config.verify_tls,
        )
    except RequestException as exc:
        raise AirflowApiError(f"Unable to reach Airflow DAG run endpoint: {exc}") from exc
    if run_resp.status_code not in (200, 201):
        raise AirflowApiError(
            f"Failed to trigger DAG run: {run_resp.status_code} {run_resp.text}",
            status_code=run_resp.status_code,
        )

    run_data = run_resp.json() if run_resp.text else {}
    return {
        "dag_id": dag_id,
        "dag_run_id": run_data.get("dag_run_id") or run_data.get("run_id") or run_data.get("id"),
        "state": run_data.get("state"),
        "logical_date": run_data.get("logical_date") or payload["logical_date"],
        "raw_response": run_data,
    }


def get_dag_run_status(config: AirflowApiConfig, dag_id: str, dag_run_id: str) -> dict[str, Any]:
    headers = _build_auth_header(config)
    try:
        resp = requests.get(
            f"{config.base_url}/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}",
            headers=headers,
            timeout=config.timeout_seconds,
            verify=config.verify_tls,
        )
    except RequestException as exc:
        raise AirflowApiError(f"Unable to reach Airflow DAG run endpoint: {exc}") from exc

    if resp.status_code == 404:
        raise AirflowApiError(f"DAG run '{dag_run_id}' for DAG '{dag_id}' was not found.", status_code=404)
    if resp.status_code != 200:
        raise AirflowApiError(
            f"Airflow DAG run lookup failed: {resp.status_code} {resp.text}",
            status_code=resp.status_code,
        )

    run_data = _safe_json(resp)
    return {
        "dag_id": dag_id,
        "dag_run_id": dag_run_id,
        "state": run_data.get("state"),
        "start_date": run_data.get("start_date"),
        "end_date": run_data.get("end_date"),
        "logical_date": run_data.get("logical_date"),
        "raw_response": run_data,
    }


def stop_dag_run(config: AirflowApiConfig, dag_id: str, dag_run_id: str) -> dict[str, Any]:
    auth_candidates = _auth_header_candidates(config)

    encoded_dag_id = quote(dag_id, safe="")
    encoded_run_id = quote(dag_run_id, safe="")
    patch_urls = (
        f"{config.base_url}/api/v2/dags/{encoded_dag_id}/dagRuns/{encoded_run_id}",
        f"{config.base_url}/api/v1/dags/{encoded_dag_id}/dagRuns/{encoded_run_id}",
    )
    payload = {"state": "failed"}
    attempts: list[str] = []
    last_error: AirflowApiError | None = None

    # Strategy 1: set dag-run state directly.
    for url in patch_urls:
        for method in ("PATCH", "PUT", "POST"):
            for auth in auth_candidates:
                headers = {**auth, "Content-Type": "application/json"}
                try:
                    resp = requests.request(
                        method,
                        url,
                        headers=headers,
                        json=payload,
                        timeout=config.timeout_seconds,
                        verify=config.verify_tls,
                    )
                except RequestException as exc:
                    last_error = AirflowApiError(f"Unable to reach Airflow DAG run stop endpoint: {exc}")
                    auth_type = "basic" if str(auth.get("Authorization", "")).lower().startswith("basic ") else "bearer"
                    attempts.append(f"{url} {method} [{auth_type}] -> request_error")
                    continue

                if resp.status_code in (200, 202, 204):
                    body = _safe_json(resp)
                    return {
                        "dag_id": dag_id,
                        "dag_run_id": dag_run_id,
                        "state": body.get("state") or "failed",
                        "strategy": f"{method.lower()}_dag_run_state",
                        "raw_response": body,
                    }
                if resp.status_code == 404:
                    auth_type = "basic" if str(auth.get("Authorization", "")).lower().startswith("basic ") else "bearer"
                    attempts.append(f"{url} {method} [{auth_type}] -> 404")
                    continue
                auth_type = "basic" if str(auth.get("Authorization", "")).lower().startswith("basic ") else "bearer"
                attempts.append(f"{url} {method} [{auth_type}] -> {resp.status_code}")
                last_error = AirflowApiError(
                    f"Failed to stop DAG run: {resp.status_code} {resp.text}",
                    status_code=resp.status_code,
                )

    # Strategy 2: compatibility endpoints used by some builds/extensions.
    action_endpoints = (
        f"{config.base_url}/api/v2/dags/{encoded_dag_id}/dagRuns/{encoded_run_id}/cancel",
        f"{config.base_url}/api/v1/dags/{encoded_dag_id}/dagRuns/{encoded_run_id}/cancel",
        f"{config.base_url}/api/v2/dags/{encoded_dag_id}/dagRuns/{encoded_run_id}/stop",
        f"{config.base_url}/api/v1/dags/{encoded_dag_id}/dagRuns/{encoded_run_id}/stop",
    )
    for url in action_endpoints:
        for auth in auth_candidates:
            headers = {**auth, "Content-Type": "application/json"}
            try:
                resp = requests.post(
                    url,
                    headers=headers,
                    json={},
                    timeout=config.timeout_seconds,
                    verify=config.verify_tls,
                )
            except RequestException:
                auth_type = "basic" if str(auth.get("Authorization", "")).lower().startswith("basic ") else "bearer"
                attempts.append(f"{url} POST [{auth_type}] -> request_error")
                continue

            if resp.status_code in (200, 202, 204):
                body = _safe_json(resp)
                return {
                    "dag_id": dag_id,
                    "dag_run_id": dag_run_id,
                    "state": body.get("state") or "failed",
                    "strategy": "action_endpoint_stop_or_cancel",
                    "raw_response": body,
                }
            auth_type = "basic" if str(auth.get("Authorization", "")).lower().startswith("basic ") else "bearer"
            attempts.append(f"{url} POST [{auth_type}] -> {resp.status_code}")

    # Strategy 3: fail active task instances in the run (best-effort fallback).
    active_states = {"running", "queued", "scheduled", "up_for_retry", "retrying", "retry", "deferred"}
    try:
        task_instances = list_dag_run_task_instances(config, dag_id, dag_run_id)
    except AirflowApiError:
        task_instances = []

    changed = 0
    task_patch_attempts: list[str] = []
    for task in task_instances:
        task_id = str(task.get("task_id") or "").strip()
        state = str(task.get("state") or "").strip().lower()
        if not task_id or state not in active_states:
            continue
        encoded_task_id = quote(task_id, safe="")
        task_urls = (
            f"{config.base_url}/api/v2/dags/{encoded_dag_id}/dagRuns/{encoded_run_id}/taskInstances/{encoded_task_id}",
            f"{config.base_url}/api/v1/dags/{encoded_dag_id}/dagRuns/{encoded_run_id}/taskInstances/{encoded_task_id}",
        )
        for task_url in task_urls:
            updated = False
            for method in ("PATCH", "PUT", "POST"):
                for auth in auth_candidates:
                    headers = {**auth, "Content-Type": "application/json"}
                    try:
                        resp = requests.request(
                            method,
                            task_url,
                            headers=headers,
                            json={"state": "failed"},
                            timeout=config.timeout_seconds,
                            verify=config.verify_tls,
                        )
                    except RequestException:
                        auth_type = "basic" if str(auth.get("Authorization", "")).lower().startswith("basic ") else "bearer"
                        task_patch_attempts.append(f"{task_url} {method} [{auth_type}] -> request_error")
                        continue

                    if resp.status_code in (200, 202, 204):
                        changed += 1
                        updated = True
                        break
                    auth_type = "basic" if str(auth.get("Authorization", "")).lower().startswith("basic ") else "bearer"
                    task_patch_attempts.append(f"{task_url} {method} [{auth_type}] -> {resp.status_code}")
                if updated:
                    break

    if changed > 0:
        return {
            "dag_id": dag_id,
            "dag_run_id": dag_run_id,
            "state": "stopping",
            "strategy": "fail_active_task_instances",
            "updated_tasks": changed,
            "raw_response": {"attempts": attempts, "task_attempts": task_patch_attempts},
        }

    if last_error:
        raise AirflowApiError(
            f"Unable to stop DAG run with available API methods. Attempts: {' | '.join(attempts)}",
            status_code=last_error.status_code,
        )
    raise AirflowApiError(
        "Unable to stop DAG run via Airflow API. "
        + (f"Attempts: {' | '.join(attempts)}" if attempts else "")
    )


def list_dag_runs_for_dag(config: AirflowApiConfig, dag_id: str, limit: int = 10) -> list[dict[str, Any]]:
    headers = _build_auth_header(config)
    safe_limit = max(1, min(int(limit or 10), 100))
    encoded_dag_id = quote(dag_id, safe="")
    urls = (
        f"{config.base_url}/api/v2/dags/{encoded_dag_id}/dagRuns?limit={safe_limit}&order_by=-start_date",
        f"{config.base_url}/api/v1/dags/{encoded_dag_id}/dagRuns?limit={safe_limit}&order_by=-start_date",
    )

    payload: Any = None
    last_404 = False
    for url in urls:
        try:
            resp = requests.get(
                url,
                headers=headers,
                timeout=config.timeout_seconds,
                verify=config.verify_tls,
            )
        except RequestException as exc:
            raise AirflowApiError(f"Unable to reach Airflow DAG runs endpoint: {exc}") from exc

        if resp.status_code == 404:
            last_404 = True
            continue
        if resp.status_code != 200:
            raise AirflowApiError(
                f"Airflow DAG runs lookup failed: {resp.status_code} {resp.text}",
                status_code=resp.status_code,
            )
        payload = _safe_json(resp)
        break

    if payload is None:
        if last_404:
            return []
        raise AirflowApiError("Airflow DAG runs response was empty.")

    runs_raw: list[dict[str, Any]] = []
    if isinstance(payload, dict):
        for key in ("dag_runs", "dag_runs_list", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                runs_raw = [item for item in value if isinstance(item, dict)]
                break
    elif isinstance(payload, list):
        runs_raw = [item for item in payload if isinstance(item, dict)]

    normalized: list[dict[str, Any]] = []
    for item in runs_raw[:safe_limit]:
        normalized.append(
            {
                "dag_id": item.get("dag_id") or dag_id,
                "dag_run_id": item.get("dag_run_id") or item.get("run_id") or item.get("id"),
                "state": str(item.get("state") or "").strip().lower() or "unknown",
                "start_date": item.get("start_date"),
                "end_date": item.get("end_date"),
                "logical_date": item.get("logical_date"),
                "raw_response": item,
            }
        )
    return normalized


def list_dag_run_task_instances(config: AirflowApiConfig, dag_id: str, dag_run_id: str) -> list[dict[str, Any]]:
    auth_candidates = _auth_header_candidates(config)
    encoded_dag_id = quote(str(dag_id or ""), safe="")
    encoded_dag_run_id = quote(str(dag_run_id or ""), safe="")
    urls = (
        f"{config.base_url}/api/v2/dags/{encoded_dag_id}/dagRuns/{encoded_dag_run_id}/taskInstances",
        f"{config.base_url}/api/v1/dags/{encoded_dag_id}/dagRuns/{encoded_dag_run_id}/taskInstances",
    )
    resp: requests.Response | None = None
    last_error: AirflowApiError | None = None
    for url in urls:
        for auth in auth_candidates:
            try:
                candidate = requests.get(
                    url,
                    headers=auth,
                    timeout=config.timeout_seconds,
                    verify=config.verify_tls,
                )
            except RequestException as exc:
                raise AirflowApiError(f"Unable to reach Airflow task instances endpoint: {exc}") from exc

            if candidate.status_code == 404:
                last_error = AirflowApiError(
                    f"Task instances not found for DAG '{dag_id}' run '{dag_run_id}'.",
                    status_code=404,
                )
                continue
            if candidate.status_code != 200:
                last_error = AirflowApiError(
                    f"Airflow task instance lookup failed: {candidate.status_code} {candidate.text}",
                    status_code=candidate.status_code,
                )
                continue
            resp = candidate
            break
        if resp is not None:
            break

    if resp is None:
        if last_error:
            raise last_error
        raise AirflowApiError("Airflow task instance lookup failed: no response.")

    payload: Any
    try:
        payload = resp.json()
    except ValueError:
        raise AirflowApiError("Airflow task instance response is not valid JSON.")

    task_instances: list[dict[str, Any]] = []
    if isinstance(payload, dict):
        if isinstance(payload.get("task_instances"), list):
            task_instances = payload.get("task_instances", [])
        elif isinstance(payload.get("task_instances_list"), list):
            task_instances = payload.get("task_instances_list", [])
    elif isinstance(payload, list):
        task_instances = payload

    normalized: list[dict[str, Any]] = []
    for item in task_instances:
        if not isinstance(item, dict):
            continue
        normalized.append(
            {
                "task_id": item.get("task_id"),
                "state": item.get("state"),
                "try_number": item.get("try_number") or 1,
                "start_date": item.get("start_date"),
                "end_date": item.get("end_date"),
                "raw": item,
            }
        )
    return normalized


def get_task_instance_log(
    config: AirflowApiConfig,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    try_number: int,
) -> dict[str, Any]:
    headers = _build_auth_header(config)
    encoded_dag_id = quote(str(dag_id or ""), safe="")
    encoded_dag_run_id = quote(str(dag_run_id or ""), safe="")
    encoded_task_id = quote(str(task_id or ""), safe="")
    try:
        resp = requests.get(
            f"{config.base_url}/api/v2/dags/{encoded_dag_id}/dagRuns/{encoded_dag_run_id}/taskInstances/{encoded_task_id}/logs/{try_number}",
            headers=headers,
            timeout=max(config.timeout_seconds, 20),
            verify=config.verify_tls,
        )
    except RequestException as exc:
        raise AirflowApiError(f"Unable to reach Airflow logs endpoint: {exc}") from exc

    content_type = (resp.headers.get("Content-Type") or "").lower()
    if resp.status_code == 404:
        return {
            "dag_id": dag_id,
            "dag_run_id": dag_run_id,
            "task_id": task_id,
            "try_number": try_number,
            "content_type": content_type,
            "content": f"[No log found for task={task_id}, try_number={try_number}]",
            "not_found": True,
        }
    if resp.status_code != 200:
        raise AirflowApiError(
            f"Airflow task log fetch failed: {resp.status_code} {resp.text}",
            status_code=resp.status_code,
        )

    content = ""
    if "application/json" in content_type:
        parsed: Any
        try:
            parsed = resp.json()
        except ValueError:
            parsed = None

        content_value: Any = parsed
        if isinstance(parsed, dict):
            if parsed.get("content") is not None:
                content_value = parsed.get("content")
            elif parsed.get("message") is not None:
                content_value = parsed.get("message")

        structured_payload = _coerce_structured_log_payload(content_value)
        if structured_payload is not None:
            content = _format_structured_log_payload(structured_payload)
        else:
            content = str(content_value)
    else:
        content = resp.text
        structured_payload = _coerce_structured_log_payload(content)
        if structured_payload is not None:
            content = _format_structured_log_payload(structured_payload)

    return {
        "dag_id": dag_id,
        "dag_run_id": dag_run_id,
        "task_id": task_id,
        "try_number": try_number,
        "content_type": content_type,
        "content": content,
        "not_found": False,
    }


def retry_task_instance(
    config: AirflowApiConfig,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
) -> dict[str, Any]:
    headers = {**_build_auth_header(config), "Content-Type": "application/json"}
    encoded_dag_id = quote(dag_id, safe="")
    encoded_run_id = quote(dag_run_id, safe="")
    encoded_task_id = quote(task_id, safe="")

    attempts: list[str] = []
    # Retry scope: keep already-successful upstream work, rerun from the selected
    # failed module, and allow its downstream path to continue in the same dag run.
    clear_scope_payload = {
        "dry_run": False,
        "dag_run_id": dag_run_id,
        "task_ids": [task_id],
        "include_upstream": False,
        "include_downstream": True,
        "include_future": False,
        "include_past": False,
        "reset_dag_runs": False,
        "only_failed": False,
        "only_running": False,
    }
    dry_scope_payload = dict(clear_scope_payload)
    dry_scope_payload["dry_run"] = True

    # Primary strategy: clearTaskInstances with explicit scope flags.
    clear_endpoints = (
        f"{config.base_url}/api/v2/dags/{encoded_dag_id}/clearTaskInstances",
        f"{config.base_url}/api/v1/dags/{encoded_dag_id}/clearTaskInstances",
    )
    for base_endpoint in clear_endpoints:
        try:
            dry_resp = requests.post(
                base_endpoint,
                headers=headers,
                json=dry_scope_payload,
                timeout=config.timeout_seconds,
                verify=config.verify_tls,
            )
        except RequestException as exc:
            raise AirflowApiError(f"Unable to reach Airflow clearTaskInstances endpoint: {exc}") from exc

        if dry_resp.status_code in {200, 201, 202}:
            dry_data = _safe_json(dry_resp)
            dry_items = dry_data.get("task_instances")
            if not isinstance(dry_items, list):
                dry_items = []

            matched = any(
                isinstance(item, dict)
                and str(item.get("task_id", "")) == task_id
                and str(item.get("dag_run_id", "")) == dag_run_id
                for item in dry_items
            )
            if not matched:
                attempts.append(f"{base_endpoint} (json dry_run) -> 200(no-matching-task)")
                continue

            try:
                apply_resp = requests.post(
                    base_endpoint,
                    headers=headers,
                    json=clear_scope_payload,
                    timeout=config.timeout_seconds,
                    verify=config.verify_tls,
                )
            except RequestException as exc:
                raise AirflowApiError(f"Unable to reach Airflow clearTaskInstances endpoint: {exc}") from exc

            if apply_resp.status_code in {200, 201, 202, 204}:
                return {
                    "dag_id": dag_id,
                    "dag_run_id": dag_run_id,
                    "task_id": task_id,
                    "strategy": "clear_task_instances_scoped_json",
                    "endpoint": base_endpoint,
                    "status_code": apply_resp.status_code,
                }
            attempts.append(f"{base_endpoint} (json apply) -> {apply_resp.status_code}")
            continue

        attempts.append(f"{base_endpoint} (json dry_run) -> {dry_resp.status_code}")

    # Compatibility fallback: some Airflow builds reject JSON body but accept query params.
    query_filters = (
        "include_upstream=false&include_downstream=true&include_future=false&include_past=false",
        "include_upstream=false&include_downstream=true",
    )
    task_param_variants = (
        f"task_ids={encoded_task_id}&dag_run_id={encoded_run_id}",
        f"task_id={encoded_task_id}&dag_run_id={encoded_run_id}",
    )

    for base_endpoint in clear_endpoints:
        for query_scope in query_filters:
            for task_filters in task_param_variants:
                dry_run_url = f"{base_endpoint}?dry_run=true&{query_scope}&{task_filters}"
                try:
                    dry_resp = requests.post(
                        dry_run_url,
                        headers=headers,
                        json={},
                        timeout=config.timeout_seconds,
                        verify=config.verify_tls,
                    )
                except RequestException as exc:
                    raise AirflowApiError(f"Unable to reach Airflow clearTaskInstances endpoint: {exc}") from exc

                if dry_resp.status_code in {200, 201, 202}:
                    dry_data = _safe_json(dry_resp)
                    dry_items = dry_data.get("task_instances")
                    if not isinstance(dry_items, list):
                        dry_items = []

                    matched = any(
                        isinstance(item, dict)
                        and str(item.get("task_id", "")) == task_id
                        and str(item.get("dag_run_id", "")) == dag_run_id
                        for item in dry_items
                    )
                    if not matched:
                        attempts.append(f"{dry_run_url} -> 200(no-matching-task)")
                        continue

                    apply_url = f"{base_endpoint}?dry_run=false&{query_scope}&{task_filters}"
                    try:
                        apply_resp = requests.post(
                            apply_url,
                            headers=headers,
                            json={},
                            timeout=config.timeout_seconds,
                            verify=config.verify_tls,
                        )
                    except RequestException as exc:
                        raise AirflowApiError(f"Unable to reach Airflow clearTaskInstances endpoint: {exc}") from exc

                    if apply_resp.status_code in {200, 201, 202, 204}:
                        return {
                            "dag_id": dag_id,
                            "dag_run_id": dag_run_id,
                            "task_id": task_id,
                            "strategy": "clear_task_instances_scoped_query",
                            "endpoint": apply_url,
                            "status_code": apply_resp.status_code,
                        }
                    attempts.append(f"{apply_url} -> {apply_resp.status_code}")
                    continue

                attempts.append(f"{dry_run_url} -> {dry_resp.status_code}")

    # Last-resort fallback for builds that support deleting a single task instance.
    delete_endpoints = (
        f"{config.base_url}/api/v2/dags/{encoded_dag_id}/dagRuns/{encoded_run_id}/taskInstances/{encoded_task_id}",
        f"{config.base_url}/api/v1/dags/{encoded_dag_id}/dagRuns/{encoded_run_id}/taskInstances/{encoded_task_id}",
    )
    for url in delete_endpoints:
        try:
            resp = requests.delete(
                url,
                headers=headers,
                timeout=config.timeout_seconds,
                verify=config.verify_tls,
            )
        except RequestException as exc:
            raise AirflowApiError(f"Unable to reach Airflow task delete endpoint: {exc}") from exc

        if resp.status_code in {200, 202, 204}:
            return {
                "dag_id": dag_id,
                "dag_run_id": dag_run_id,
                "task_id": task_id,
                "strategy": "delete_task_instance",
                "endpoint": url,
                "status_code": resp.status_code,
            }
        attempts.append(f"{url} -> {resp.status_code}")

    raise AirflowApiError(
        "Task retry request was rejected by Airflow. "
        + ("Attempts: " + " | ".join(attempts) if attempts else "No retry endpoint accepted the request.")
    )


def list_connections(config: AirflowApiConfig) -> list[dict[str, Any]]:
    headers = _build_auth_header(config)
    resp = _request_connection_api(
        config=config,
        method="GET",
        path_suffix="/connections",
        headers=headers,
        expected_statuses={200},
    )

    payload: Any
    try:
        payload = resp.json()
    except ValueError:
        raise AirflowApiError("Airflow connection list response is not valid JSON.")

    raw_connections: list[dict[str, Any]] = []
    if isinstance(payload, dict):
        if isinstance(payload.get("connections"), list):
            raw_connections = [item for item in payload.get("connections", []) if isinstance(item, dict)]
        elif isinstance(payload.get("items"), list):
            raw_connections = [item for item in payload.get("items", []) if isinstance(item, dict)]
    elif isinstance(payload, list):
        raw_connections = [item for item in payload if isinstance(item, dict)]

    return [_normalize_connection_item(item) for item in raw_connections]


def create_connection(config: AirflowApiConfig, connection_payload: dict[str, Any]) -> dict[str, Any]:
    headers = {**_build_auth_header(config), "Content-Type": "application/json"}
    resp = _request_connection_api(
        config=config,
        method="POST",
        path_suffix="/connections",
        headers=headers,
        json_payload=connection_payload,
        expected_statuses={200, 201},
    )

    parsed = _safe_json(resp)
    if not parsed:
        parsed = dict(connection_payload)
    return _normalize_connection_item(parsed)


def update_connection(
    config: AirflowApiConfig,
    connection_id: str,
    connection_payload: dict[str, Any],
) -> dict[str, Any]:
    headers = {**_build_auth_header(config), "Content-Type": "application/json"}
    encoded_id = quote(connection_id, safe="")
    update_payload = dict(connection_payload or {})
    # Some Airflow builds validate update bodies strictly and require connection_id.
    update_payload["connection_id"] = connection_id

    try:
        resp = _request_connection_api(
            config=config,
            method="PATCH",
            path_suffix=f"/connections/{encoded_id}",
            headers=headers,
            json_payload=update_payload,
            expected_statuses={200, 204},
        )
    except AirflowApiError as exc:
        if exc.status_code not in {405, 422}:
            raise
        resp = _request_connection_api(
            config=config,
            method="PUT",
            path_suffix=f"/connections/{encoded_id}",
            headers=headers,
            json_payload=update_payload,
            expected_statuses={200, 204},
        )

    parsed = _safe_json(resp)
    if not parsed:
        parsed = dict(connection_payload)
        parsed["connection_id"] = connection_id
    return _normalize_connection_item(parsed)


def delete_connection(config: AirflowApiConfig, connection_id: str) -> None:
    headers = _build_auth_header(config)
    encoded_id = quote(connection_id, safe="")
    _request_connection_api(
        config=config,
        method="DELETE",
        path_suffix=f"/connections/{encoded_id}",
        headers=headers,
        expected_statuses={200, 204},
    )


def list_recent_dag_runs(config: AirflowApiConfig, limit: int = 20) -> dict[str, Any]:
    headers = _build_auth_header(config)
    safe_limit = max(1, min(int(limit or 20), 100))
    urls = (
        f"{config.base_url}/api/v2/dagRuns?limit={safe_limit}&order_by=-start_date",
        f"{config.base_url}/api/v1/dags/~/dagRuns?limit={safe_limit}&order_by=-start_date",
    )

    last_404 = False
    payload: Any = None
    for url in urls:
        try:
            resp = requests.get(
                url,
                headers=headers,
                timeout=config.timeout_seconds,
                verify=config.verify_tls,
            )
        except RequestException as exc:
            raise AirflowApiError(f"Unable to reach Airflow DAG runs endpoint: {exc}") from exc

        if resp.status_code == 404:
            last_404 = True
            continue
        if resp.status_code != 200:
            raise AirflowApiError(
                f"Airflow DAG runs lookup failed: {resp.status_code} {resp.text}",
                status_code=resp.status_code,
            )
        try:
            payload = resp.json()
        except ValueError:
            raise AirflowApiError("Airflow DAG runs response is not valid JSON.")
        break

    if payload is None:
        if last_404:
            raise AirflowApiError(
                "Airflow DAG runs endpoint not found for this Airflow version.",
                status_code=404,
            )
        raise AirflowApiError("Airflow DAG runs response was empty.")

    runs_raw: list[dict[str, Any]] = []
    if isinstance(payload, dict):
        if isinstance(payload.get("dag_runs"), list):
            runs_raw = [item for item in payload.get("dag_runs", []) if isinstance(item, dict)]
        elif isinstance(payload.get("dag_runs_list"), list):
            runs_raw = [item for item in payload.get("dag_runs_list", []) if isinstance(item, dict)]
    elif isinstance(payload, list):
        runs_raw = [item for item in payload if isinstance(item, dict)]

    normalized_runs: list[dict[str, Any]] = []
    state_counts: dict[str, int] = {}
    for item in runs_raw[:safe_limit]:
        state = str(item.get("state") or "").strip().lower() or "unknown"
        state_counts[state] = state_counts.get(state, 0) + 1
        normalized_runs.append(
            {
                "dag_id": item.get("dag_id"),
                "dag_run_id": item.get("dag_run_id") or item.get("run_id") or item.get("id"),
                "state": state,
                "start_date": item.get("start_date"),
                "end_date": item.get("end_date"),
                "logical_date": item.get("logical_date"),
            }
        )

    return {
        "runs": normalized_runs,
        "state_counts": state_counts,
        "total_entries": len(normalized_runs),
    }


def _request_api_with_versions(
    config: AirflowApiConfig,
    *,
    method: str,
    path_suffix: str,
    headers: dict[str, str],
    expected_statuses: set[int] | None = None,
) -> requests.Response:
    suffix = path_suffix if path_suffix.startswith("/") else f"/{path_suffix}"
    versions = ("/api/v2", "/api/v1")
    last_404: requests.Response | None = None

    for version in versions:
        url = f"{config.base_url}{version}{suffix}"
        try:
            resp = requests.request(
                method=method.upper(),
                url=url,
                headers=headers,
                timeout=config.timeout_seconds,
                verify=config.verify_tls,
            )
        except RequestException as exc:
            raise AirflowApiError(f"Unable to reach Airflow API endpoint '{suffix}': {exc}") from exc

        if resp.status_code == 404:
            last_404 = resp
            continue

        if expected_statuses and resp.status_code not in expected_statuses:
            raise AirflowApiError(
                f"Airflow API call failed for '{suffix}': {resp.status_code} {resp.text}",
                status_code=resp.status_code,
            )
        return resp

    if last_404 is not None:
        raise AirflowApiError(f"Airflow API path not found for '{suffix}' (checked /api/v2 and /api/v1).", status_code=404)
    raise AirflowApiError(f"Airflow API call failed for '{suffix}'.")


def _payload_list(payload: Any, *keys: str) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def get_airflow_platform_snapshot(config: AirflowApiConfig) -> dict[str, Any]:
    headers = _build_auth_header(config)

    snapshot: dict[str, Any] = {
        "dags_total": 0,
        "dags_paused": 0,
        "jobs_total": 0,
        "jobs_running": 0,
        "pools_total": 0,
        "connections_total": 0,
        "import_errors_total": 0,
        "health": {},
        "errors": [],
    }

    try:
        dags_resp = _request_api_with_versions(
            config=config,
            method="GET",
            path_suffix="/dags?limit=200",
            headers=headers,
            expected_statuses={200},
        )
        dags_payload = dags_resp.json() if dags_resp.text else {}
        dags = _payload_list(dags_payload, "dags", "items")
        snapshot["dags_total"] = len(dags)
        snapshot["dags_paused"] = sum(1 for item in dags if bool(item.get("is_paused")))
    except Exception as exc:  # noqa: BLE001
        snapshot["errors"].append(f"dags: {exc}")

    try:
        jobs_resp = _request_api_with_versions(
            config=config,
            method="GET",
            path_suffix="/jobs?limit=200",
            headers=headers,
            expected_statuses={200},
        )
        jobs_payload = jobs_resp.json() if jobs_resp.text else {}
        jobs = _payload_list(jobs_payload, "jobs", "items")
        snapshot["jobs_total"] = len(jobs)
        snapshot["jobs_running"] = sum(1 for item in jobs if str(item.get("state", "")).strip().lower() == "running")
    except Exception as exc:  # noqa: BLE001
        snapshot["errors"].append(f"jobs: {exc}")

    try:
        pools_resp = _request_api_with_versions(
            config=config,
            method="GET",
            path_suffix="/pools?limit=200",
            headers=headers,
            expected_statuses={200},
        )
        pools_payload = pools_resp.json() if pools_resp.text else {}
        pools = _payload_list(pools_payload, "pools", "items")
        snapshot["pools_total"] = len(pools)
    except Exception as exc:  # noqa: BLE001
        snapshot["errors"].append(f"pools: {exc}")

    try:
        import_errors_resp = _request_api_with_versions(
            config=config,
            method="GET",
            path_suffix="/importErrors?limit=200",
            headers=headers,
            expected_statuses={200},
        )
        import_errors_payload = import_errors_resp.json() if import_errors_resp.text else {}
        import_errors = _payload_list(import_errors_payload, "import_errors", "items")
        snapshot["import_errors_total"] = len(import_errors)
    except Exception as exc:  # noqa: BLE001
        snapshot["errors"].append(f"importErrors: {exc}")

    try:
        snapshot["connections_total"] = len(list_connections(config))
    except Exception as exc:  # noqa: BLE001
        snapshot["errors"].append(f"connections: {exc}")

    health_urls = (
        f"{config.base_url}/api/v1/health",
        f"{config.base_url}/api/v2/monitor/health",
    )
    health_payload: dict[str, Any] = {}
    for url in health_urls:
        try:
            health_resp = requests.get(
                url,
                headers=headers,
                timeout=config.timeout_seconds,
                verify=config.verify_tls,
            )
        except RequestException as exc:
            snapshot["errors"].append(f"health: {exc}")
            break

        if health_resp.status_code == 404:
            continue
        if health_resp.status_code != 200:
            snapshot["errors"].append(f"health: {health_resp.status_code} {health_resp.text}")
            break
        health_payload = _safe_json(health_resp)
        break

    if health_payload:
        normalized_health: dict[str, str] = {}
        for key in ("metadatabase", "scheduler", "triggerer", "dag_processor"):
            value = health_payload.get(key)
            if isinstance(value, dict):
                normalized_health[key] = str(value.get("status") or "").strip() or "unknown"
            elif isinstance(value, str):
                normalized_health[key] = value.strip() or "unknown"
        snapshot["health"] = normalized_health

    return snapshot
