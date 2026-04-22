from airflow.exceptions import AirflowException
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator
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
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

_alert_emails = ['amineelkpfe@gmail.com']
_alert_mode = 'both'
if _alert_emails and _alert_mode in {"on_failure", "both"}:
    default_args["on_failure_callback"] = _build_alert_callback(_alert_emails, "on_failure")
if _alert_emails and _alert_mode in {"on_retry", "both"}:
    default_args["on_retry_callback"] = _build_alert_callback(_alert_emails, "on_retry")


@dag(
    dag_id='test_azure_web',
    start_date=datetime(2026, 1, 1),
    schedule=_normalize_schedule(None),
    catchup=False,
    default_args=default_args,
    tags=['demo', 'orchestration'],
    description='Sequential orchestration pipeline',
)
def test_azure_web():
    @task(task_id='azure_1_node_1')
    def run_node_1():
        local_file_path = '/mnt/cegid/CUSTOMER_CUS_TEST.csv'
        container_name = 'my-container'
        blob_name = 'uploads/CUSTOMER_CUS_TEST.csv'
        create_container = False

        if not os.path.exists(local_file_path):
            raise AirflowException(f"Local file does not exist for Azure upload: {local_file_path}")
        if not blob_name:
            raise AirflowException("Azure blob_name is required for upload.")

        LocalFilesystemToWasbOperator(
            task_id='azure_1_node_1_inner_upload',
            wasb_conn_id="wasb_default",
            file_path=local_file_path,
            container_name=container_name,
            blob_name=blob_name,
            create_container=bool(create_container),
        ).execute(context={})

        return {"uploaded": True, "blob_name": blob_name, "container_name": container_name, "create_container": bool(create_container)}


    task_node_1 = run_node_1()

    # Single task DAG (no chaining required).


dag = test_azure_web()
