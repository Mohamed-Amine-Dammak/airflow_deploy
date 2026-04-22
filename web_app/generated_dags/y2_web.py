from airflow.exceptions import AirflowException
from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook
from datetime import datetime, timedelta, timezone
import json
import os
import requests
import time


def _build_alert_callback(emails: list[str], mode: str):
    def _callback(context):
        dag_id = context.get("dag").dag_id if context.get("dag") else "unknown_dag"
        task_id = context.get("task_instance").task_id if context.get("task_instance") else "unknown_task"
        print(
            f"[ALERT] mode={mode} dag_id={dag_id} task_id={task_id} "
            f"emails={emails} run_id={context.get('run_id')}"
        )

    return _callback


def _normalize_schedule(schedule):
    if schedule in (None, "", "none", "null", "None"):
        return None
    return schedule


default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

_alert_emails = ['amineelkpfe@gmail.com']
_alert_mode = 'both'
if _alert_emails and _alert_mode in {"on_failure", "both"}:
    default_args["on_failure_callback"] = _build_alert_callback(_alert_emails, "on_failure")
if _alert_emails and _alert_mode in {"on_retry", "both"}:
    default_args["on_retry_callback"] = _build_alert_callback(_alert_emails, "on_retry")


@dag(
    dag_id='y2_web',
    start_date=datetime(2026, 1, 1),
    schedule=_normalize_schedule(None),
    catchup=False,
    default_args=default_args,
    tags=['y2', 'orchestration'],
    description='Sequential orchestration pipeline',
)
def y2_web():
    @task(task_id='y2_1_node_2')
    def run_node_2():
        folder_id_value = '90039148_003_TEST'
        task_ids = ['436']

        conn = BaseHook.get_connection("cegid_y2")
        base_host = (conn.host or "").rstrip("/")
        username = conn.login
        password = conn.password or ""
        if not base_host or not username:
            raise AirflowException("cegid_y2 connection must include host/login.")

        from requests.auth import HTTPBasicAuth

        auth = HTTPBasicAuth(f"{folder_id_value}\\{username}", password)
        session = requests.Session()
        session.headers.update({"Accept": "application/json"})

        base_task_url = f"{base_host}/{folder_id_value}/api/scheduled-tasks/v1"
        monitor_results = []

        for task_id_value in task_ids:
            details_resp = session.get(
                f"{base_task_url}/{task_id_value}",
                auth=auth,
                params=[("fields", "Execution")],
                timeout=60,
            )
            if details_resp.status_code != 200:
                raise AirflowException(
                    f"Y2 task details lookup failed for id={task_id_value}: "
                    f"{details_resp.status_code} {details_resp.text}"
                )

            enable_resp = session.post(
                f"{base_task_url}/enable",
                auth=auth,
                params=[("ids", task_id_value)],
                timeout=60,
            )
            if enable_resp.status_code != 200:
                raise AirflowException(
                    f"Y2 enable failed for id={task_id_value}: {enable_resp.status_code} {enable_resp.text}"
                )

            run_resp = session.post(
                f"{base_task_url}/run",
                auth=auth,
                params=[("ids", task_id_value)],
                timeout=60,
            )
            if run_resp.status_code != 200:
                raise AirflowException(
                    f"Y2 run failed for id={task_id_value}: {run_resp.status_code} {run_resp.text}"
                )

            started_at = time.time()
            timeout_seconds = 600
            while True:
                status_resp = session.get(
                    f"{base_task_url}/{task_id_value}",
                    auth=auth,
                    params=[("fields", "Execution")],
                    timeout=60,
                )
                if status_resp.status_code != 200:
                    raise AirflowException(
                        f"Y2 monitoring failed for id={task_id_value}: {status_resp.status_code} {status_resp.text}"
                    )

                payload = status_resp.json()
                in_progress = bool(payload.get("inProgress"))
                status = payload.get("status")
                execution = payload.get("execution") or {}
                last_execution = (execution.get("lastExecution") or {}).get("date")
                print(
                    f"Y2 monitor task_id={task_id_value} status={status} "
                    f"in_progress={in_progress} last_execution={last_execution}"
                )

                if last_execution and not in_progress and status in ("Success", "None", "Waiting", "Error"):
                    monitor_results.append(
                        {
                            "task_id": task_id_value,
                            "status": status,
                            "last_execution": last_execution,
                        }
                    )
                    break

                if time.time() - started_at > timeout_seconds:
                    raise AirflowException(f"Y2 monitor timeout for task_id={task_id_value}")
                time.sleep(15)

        return {"results": monitor_results}


    task_node_2 = run_node_2()

    # Single task DAG (no chaining required).


dag = y2_web()
