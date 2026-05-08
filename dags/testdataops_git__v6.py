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


_TASK_RESUME_SCOPE_BY_ENTRY = {'talend_1_node_9': ['delay_2_node_8', 'delay_3_node_7', 'delay_4_node_6', 'talend_1_node_9'],
 'delay_2_node_8': ['delay_2_node_8', 'delay_3_node_7', 'delay_4_node_6'],
 'delay_3_node_7': ['delay_3_node_7', 'delay_4_node_6'],
 'delay_4_node_6': ['delay_4_node_6']}


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
    dag_id='testdataops_git__v6',
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
def testdataops_v6():
    @task(task_id='talend_1_node_9')
    def run_node_9():
        job_name = 'ok'
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


    task_node_9 = run_node_9()

    @task(task_id='delay_2_node_8')
    def run_node_8():
        delay_minutes = int(5)
        if delay_minutes <= 0:
            raise AirflowException("delay.delay_minutes must be > 0.")
        time.sleep(delay_minutes * 60)
        return {"delayed_minutes": delay_minutes, "status": "completed"}


    task_node_8 = run_node_8()

    @task(task_id='delay_3_node_7')
    def run_node_7():
        delay_minutes = int(5)
        if delay_minutes <= 0:
            raise AirflowException("delay.delay_minutes must be > 0.")
        time.sleep(delay_minutes * 60)
        return {"delayed_minutes": delay_minutes, "status": "completed"}


    task_node_7 = run_node_7()

    @task(task_id='delay_4_node_6')
    def run_node_6():
        delay_minutes = int(1)
        if delay_minutes <= 0:
            raise AirflowException("delay.delay_minutes must be > 0.")
        time.sleep(delay_minutes * 60)
        return {"delayed_minutes": delay_minutes, "status": "completed"}


    task_node_6 = run_node_6()

    task_node_7 >> task_node_6
    task_node_8 >> task_node_7
    task_node_9 >> task_node_8


dag = testdataops_v6()
