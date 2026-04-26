from airflow.exceptions import AirflowException
from airflow.exceptions import AirflowSkipException
from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook
from datetime import datetime, timedelta, timezone
import json
import os
import re
import requests
import time
from utils.etl_tasks import custom_failure_email


def _normalize_schedule(schedule):
    if schedule in (None, "", "none", "null", "None"):
        return None
    return schedule


_TASK_RESUME_SCOPE_BY_ENTRY = {'n8n_1_node_4': ['n8n_1_node_4', 'talend_2_node_5'], 'talend_2_node_5': ['talend_2_node_5']}


def _resolve_resume_from_task_id(context):
    dag_run = context.get("dag_run")
    dag_conf = getattr(dag_run, "conf", {}) if dag_run else {}
    if not isinstance(dag_conf, dict):
        return ""
    raw = dag_conf.get("resume_from_task_id")
    return str(raw or "").strip()


def _apply_resume_scope(context):
    resume_from_task_id = _resolve_resume_from_task_id(context)
    if not resume_from_task_id:
        return

    allowed_task_ids = _TASK_RESUME_SCOPE_BY_ENTRY.get(resume_from_task_id)
    if not isinstance(allowed_task_ids, list):
        return

    task = context.get("task")
    current_task_id = str(getattr(task, "task_id", "") or "").strip()
    if not current_task_id:
        return

    if current_task_id not in _TASK_RESUME_SCOPE_BY_ENTRY:
        return
    if current_task_id in allowed_task_ids:
        return

    raise AirflowSkipException(
        f"Skipping task '{current_task_id}' because run is resuming from '{resume_from_task_id}'."
    )


default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "pre_execute": _apply_resume_scope,
}

_alert_emails = []
_alert_mode = 'both'
if _alert_emails and _alert_mode in {"on_failure", "both"}:
    default_args["on_failure_callback"] = custom_failure_email(_alert_emails)
if _alert_emails and _alert_mode in {"on_retry", "both"}:
    default_args["on_retry_callback"] = custom_failure_email(_alert_emails)


@dag(
    dag_id='my_pipeline__v5',
    start_date=datetime(
        2026,
        1,
        1
    ),
    schedule=_normalize_schedule(None),
    catchup=False,
    default_args=default_args,
    tags=['demo', 'orchestration'],
    description='',
)
def my_pipeline_v5():
    @task(task_id='n8n_1_node_4')
    def run_node_4():
        workflow_name = 'ok'
        http_method = str('GET').upper()
        query_params = None or {}
        request_body = None or {}
        use_test_webhook = True
        monitor_poll_interval_seconds = int(5)
        monitor_timeout_seconds = int(300)

        conn = BaseHook.get_connection("n8n_local")
        base_url = conn.host.rstrip("/")
        api_key = conn.password or ""

        headers = {
            "X-N8N-API-KEY": api_key,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        if monitor_poll_interval_seconds <= 0:
            raise AirflowException("n8n.monitor_poll_interval_seconds must be > 0")
        if monitor_timeout_seconds <= 0:
            raise AirflowException("n8n.monitor_timeout_seconds must be > 0")

        def _parse_ts(raw_value):
            if not raw_value:
                return None
            value = str(raw_value).strip()
            if not value:
                return None
            value = value.replace("Z", "+00:00")
            dt = datetime.fromisoformat(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt

        def _print_n8n_execution_details(exec_data):
            print("[N8N] execution summary")
            summary_fields = [
                ("workflow_name", workflow_name),
                ("workflow_id", workflow_id),
                ("execution_id", exec_data.get("id")),
                ("status", exec_data.get("status")),
                ("mode", exec_data.get("mode")),
                ("startedAt", exec_data.get("startedAt")),
                ("stoppedAt", exec_data.get("stoppedAt")),
                ("finished", exec_data.get("finished")),
                ("retryOf", exec_data.get("retryOf")),
            ]
            for key, value in summary_fields:
                print(f"[N8N] {key}: {value}")

            run_data = (((exec_data.get("data") or {}).get("resultData") or {}).get("runData") or {})
            if run_data:
                nodes_total = len(run_data.keys())
                failed_nodes = []
                for node_name, runs in run_data.items():
                    for run in (runs or []):
                        run_status = str(run.get("status") or "").strip().lower()
                        if run_status in {"error", "failed", "crashed"}:
                            failed_nodes.append(node_name)
                            break
                print(f"[N8N] nodes_executed={nodes_total}")
                if failed_nodes:
                    print(f"[N8N] failed_nodes={','.join(failed_nodes)}")
            top_error = (((exec_data.get("data") or {}).get("resultData") or {}).get("error")) or {}
            if top_error:
                print("[N8N] error=" + json.dumps(top_error, ensure_ascii=True, default=str))

        list_resp = requests.get(
            f"{base_url}/api/v1/workflows",
            headers=headers,
            params={"limit": 250},
            timeout=30,
        )
        list_resp.raise_for_status()
        data = list_resp.json()
        workflows = data.get("data", []) if isinstance(data, dict) else data
        matched = [wf for wf in workflows if str(wf.get("name", "")).strip() == str(workflow_name).strip()]
        if not matched:
            raise AirflowException(f"n8n workflow not found by name: {workflow_name}")
        if len(matched) > 1:
            raise AirflowException(f"Multiple n8n workflows found with name '{workflow_name}'.")

        workflow_id = str(matched[0].get("id"))
        details_resp = requests.get(f"{base_url}/api/v1/workflows/{workflow_id}", headers=headers, timeout=30)
        details_resp.raise_for_status()
        details = details_resp.json()

        nodes = details.get("nodes", []) or details.get("data", {}).get("nodes", [])
        webhook_paths = []
        for node in nodes:
            node_type = str(node.get("type", "")).lower()
            if "webhook" not in node_type:
                continue
            path = (node.get("parameters", {}) or {}).get("path")
            if isinstance(path, str) and path.strip():
                webhook_paths.append(path.strip().strip("/"))

        if not webhook_paths:
            raise AirflowException(f"n8n workflow '{workflow_name}' has no webhook path.")
        if len(webhook_paths) > 1:
            raise AirflowException(f"n8n workflow '{workflow_name}' has multiple webhook paths: {webhook_paths}")

        webhook_path = webhook_paths[0]
        prefix = "webhook-test" if use_test_webhook else "webhook"
        webhook_url = f"{base_url}/{prefix}/{webhook_path}"
        triggered_at = datetime.now(timezone.utc).isoformat()

        if http_method == "POST":
            trigger_resp = requests.post(webhook_url, params=query_params, json=request_body, timeout=45)
        else:
            trigger_resp = requests.get(webhook_url, params=query_params, timeout=45)

        if trigger_resp.status_code not in (200, 201, 202):
            raise AirflowException(
                f"n8n trigger failed for workflow '{workflow_name}': "
                    f"status={trigger_resp.status_code}, body={trigger_resp.text}"
            )

        trigger_json = {}
        try:
            trigger_json = trigger_resp.json() if trigger_resp.text else {}
        except Exception:
            trigger_json = {}

        print(
            f"[AIRFLOW] n8n triggered workflow_name={workflow_name}, workflow_id={workflow_id}, "
            f"webhook_path={webhook_path}, status={trigger_resp.status_code}, triggered_at={triggered_at}"
        )

        execution_id = trigger_json.get("executionId") or trigger_json.get("id")
        terminal_statuses = {"success", "error", "failed", "crashed", "canceled"}
        last_seen_status = None
        matched_execution = None
        deadline = time.time() + monitor_timeout_seconds
        trigger_dt = _parse_ts(triggered_at)

        while time.time() < deadline:
            if execution_id:
                exec_resp = requests.get(
                    f"{base_url}/api/v1/executions/{execution_id}",
                    headers=headers,
                    params={"includeData": "true"},
                    timeout=30,
                )
                exec_resp.raise_for_status()
                payload = exec_resp.json()
                if isinstance(payload, dict) and "data" in payload and isinstance(payload.get("data"), dict):
                    matched_execution = payload["data"]
                else:
                    matched_execution = payload
            else:
                list_exec_resp = requests.get(
                    f"{base_url}/api/v1/executions",
                    headers=headers,
                    params={
                        "workflowId": workflow_id,
                        "limit": 20,
                        "includeData": "true",
                    },
                    timeout=30,
                )
                list_exec_resp.raise_for_status()
                payload = list_exec_resp.json()
                executions = payload.get("data", []) if isinstance(payload, dict) else payload
                executions = executions if isinstance(executions, list) else []
                candidates = []
                for execution in executions:
                    started = _parse_ts(execution.get("startedAt"))
                    if started is None:
                        continue
                    if trigger_dt is None or started >= trigger_dt:
                        candidates.append((started, execution))

                if candidates:
                    candidates.sort(key=lambda item: item[0], reverse=True)
                    matched_execution = candidates[0][1]
                    execution_id = matched_execution.get("id") or execution_id

            if matched_execution:
                status = str(matched_execution.get("status") or "").lower()
                if status and status != last_seen_status:
                    print(f"[N8N] status={status} execution_id={execution_id}")
                last_seen_status = status or last_seen_status
                if status in terminal_statuses:
                    break

            time.sleep(monitor_poll_interval_seconds)

        if not matched_execution:
            raise AirflowException(
                f"n8n execution was triggered but not found before timeout. "
                f"workflow_name={workflow_name}, workflow_id={workflow_id}, timeout={monitor_timeout_seconds}s"
            )

        _print_n8n_execution_details(matched_execution)

        final_status = str(matched_execution.get("status") or "").lower()
        if final_status != "success":
            raise AirflowException(
                f"n8n execution did not complete successfully. "
                f"workflow_name={workflow_name}, workflow_id={workflow_id}, execution_id={execution_id}, "
                f"status={final_status or last_seen_status or 'unknown'}"
            )

        return {
            "workflow_name": workflow_name,
            "workflow_id": workflow_id,
            "webhook_path": webhook_path,
            "status_code": trigger_resp.status_code,
            "execution_id": execution_id,
            "execution_status": final_status,
            "triggered_at": triggered_at,
        }


    task_node_4 = run_node_4()

    @task(task_id='talend_2_node_5')
    def run_node_5():
        job_name = 'pl'
        workspace_id = None

        conn = BaseHook.get_connection("talend_cloud")
        base_url = conn.host.rstrip("/")
        headers = {
            "Authorization": f"Bearer {conn.password}",
            "Content-Type": "application/json",
        }

        params = {"limit": 100, "offset": 0, "name": job_name}
        if workspace_id:
            params["workspaceId"] = workspace_id

        executable_resp = requests.get(
            f"{base_url}/orchestration/executables/tasks",
            headers=headers,
            params=params,
            timeout=60,
        )
        if executable_resp.status_code != 200:
            raise AirflowException(f"Talend executable lookup failed: {executable_resp.status_code} {executable_resp.text}")

        items = executable_resp.json().get("items", [])
        matched = [item for item in items if str(item.get("name", "")).strip() == str(job_name).strip()]
        if not matched:
            raise AirflowException(f"Talend job not found by name: {job_name}")
        if len(matched) > 1:
            raise AirflowException(f"Talend job name '{job_name}' is ambiguous.")

        executable_id = matched[0].get("executable")
        if not executable_id:
            raise AirflowException(f"Talend executable id is empty for job: {job_name}")

        trigger_resp = requests.post(
            f"{base_url}/processing/executions",
            headers=headers,
            json={"executable": executable_id},
            timeout=60,
        )
        if trigger_resp.status_code not in (200, 201):
            raise AirflowException(f"Talend trigger failed: {trigger_resp.status_code} {trigger_resp.text}")

        trigger_body = trigger_resp.json()
        execution_id = trigger_body.get("executionId") or trigger_body.get("id")
        if not execution_id:
            raise AirflowException(f"Talend trigger succeeded but no execution id was returned: {trigger_body}")

        print(
            f"[AIRFLOW] Talend trigger accepted "
            f"job_name={job_name}, executable_id={executable_id}, execution_id={execution_id}"
        )

        logs_url = f"{base_url}/monitoring/executions/{execution_id}/logs"
        start = time.time()
        timeout_seconds = 300
        poll_interval_seconds = 10
        terminal_markers = ("completed", "success", "failed", "error")
        failure_markers = ("failed", "error")
        final_seen = None
        last_logged_message = None

        while True:
            logs_resp = requests.get(logs_url, headers={"Authorization": f"Bearer {conn.password}"}, timeout=60)
            if logs_resp.status_code != 200:
                raise AirflowException(f"Talend log polling failed: {logs_resp.status_code} {logs_resp.text}")

            payload = logs_resp.json()
            latest_logs = payload.get("data", [])
            if latest_logs:
                final_seen = latest_logs[0]
                latest_msg_raw = str(latest_logs[0].get("logMessage", "")).strip()
                latest_msg = latest_msg_raw.lower()
                latest_ts = latest_logs[0].get("logTimestamp") or "-"
                latest_severity = latest_logs[0].get("severity") or "-"

                if latest_msg_raw and latest_msg_raw != last_logged_message:
                    print(f"[TALEND] {latest_ts} {latest_severity} | {latest_msg_raw}")
                    last_logged_message = latest_msg_raw

                if any(marker in latest_msg for marker in terminal_markers):
                    break

            if time.time() - start > timeout_seconds:
                break
            time.sleep(poll_interval_seconds)

        if not final_seen:
            raise AirflowException(
                f"Talend monitoring timed out before any log appeared. "
                f"job_name={job_name}, execution_id={execution_id}, timeout={timeout_seconds}s"
            )

        final_msg = str(final_seen.get("logMessage", "")).strip()
        final_ts = final_seen.get("logTimestamp") or "-"
        final_severity = final_seen.get("severity") or "-"
        final_msg_lc = final_msg.lower()

        print(f"[TALEND] final: {final_ts} {final_severity} | {final_msg}")

        if any(marker in final_msg_lc for marker in failure_markers):
            raise AirflowException(
                f"Talend execution failed. job_name={job_name}, execution_id={execution_id}, last_log={final_msg}"
            )

        return {
            "job_name": job_name,
            "executable_id": executable_id,
            "execution_id": execution_id,
            "last_log_timestamp": final_ts,
            "last_log_severity": final_severity,
            "last_log_message": final_msg,
        }


    task_node_5 = run_node_5()

    task_node_4 >> task_node_5


dag = my_pipeline_v5()
