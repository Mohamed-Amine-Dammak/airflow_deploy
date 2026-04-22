from airflow.exceptions import AirflowException
from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
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

_alert_emails = []
_alert_mode = 'both'
if _alert_emails and _alert_mode in {"on_failure", "both"}:
    default_args["on_failure_callback"] = _build_alert_callback(_alert_emails, "on_failure")
if _alert_emails and _alert_mode in {"on_retry", "both"}:
    default_args["on_retry_callback"] = _build_alert_callback(_alert_emails, "on_retry")


@dag(
    dag_id='powerbi_web',
    start_date=datetime(2026, 1, 1),
    schedule=_normalize_schedule(None),
    catchup=False,
    default_args=default_args,
    tags=['demo', 'orchestration'],
    description='Sequential orchestration pipeline',
)
def powerbi_web():
    @task(task_id='powerbi_1_node_1')
    def run_node_1():
        workspace_id = '5992b264-2c31-4087-9f57-3b3baa1e7b0d'
        dataset_id = 'b4350df9-bdcc-43fd-b458-1b1a4cb42831'
        conn = BaseHook.get_connection("powerbi_api")
        extra = conn.extra_dejson

        tenant_id = extra.get("tenant_id")
        client_id = extra.get("client_id")
        client_secret = extra.get("client_secret")
        if not all([tenant_id, client_id, client_secret]):
            raise AirflowException("powerbi_api connection extra must include tenant_id, client_id, client_secret.")

        token_resp = requests.post(
            f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": "https://analysis.windows.net/powerbi/api/.default",
            },
            timeout=60,
        )
        token_resp.raise_for_status()
        access_token = token_resp.json()["access_token"]

        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

        list_resp = requests.get(
            f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets",
            headers=headers,
            timeout=60,
        )
        if list_resp.status_code != 200:
            raise AirflowException(
                f"Power BI dataset listing failed: status={list_resp.status_code}, body={list_resp.text}"
            )
        datasets = list_resp.json().get("value", [])
        if dataset_id not in [item.get("id") for item in datasets]:
            raise AirflowException(f"Dataset id '{dataset_id}' not found in workspace '{workspace_id}'.")

        refresh_resp = requests.post(
            f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes",
            headers=headers,
            timeout=60,
        )
        if refresh_resp.status_code not in (200, 202):
            raise AirflowException(
                f"Power BI refresh failed: status={refresh_resp.status_code}, body={refresh_resp.text}"
            )

        return {
            "workspace_id": workspace_id,
            "dataset_id": dataset_id,
            "status_code": refresh_resp.status_code,
        }


    task_node_1 = run_node_1()

    # Single task DAG (no chaining required).


dag = powerbi_web()
