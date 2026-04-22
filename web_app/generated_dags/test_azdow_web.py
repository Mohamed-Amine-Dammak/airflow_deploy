from airflow.exceptions import AirflowException
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
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

_alert_emails = []
_alert_mode = 'both'
if _alert_emails and _alert_mode in {"on_failure", "both"}:
    default_args["on_failure_callback"] = _build_alert_callback(_alert_emails, "on_failure")
if _alert_emails and _alert_mode in {"on_retry", "both"}:
    default_args["on_retry_callback"] = _build_alert_callback(_alert_emails, "on_retry")


@dag(
    dag_id='test_azdow_web',
    start_date=datetime(2026, 1, 1),
    schedule=_normalize_schedule(None),
    catchup=False,
    default_args=default_args,
    tags=['demo', 'orchestration'],
    description='Sequential orchestration pipeline',
)
def test_azdow_web():
    @task(task_id='azure_1_node_1')
    def run_node_1():
        container_name = 'my-container'
        download_mode = 'single'
        file_name = 'CUSTOMER_CUS_TEST'
        file_prefix = '' or ""
        file_extension = '.csv'
        local_dir_download = '/opt/airflow/dags/downloads'

        hook = WasbHook(wasb_conn_id="wasb_default")
        os.makedirs(local_dir_download, exist_ok=True)

        if download_mode == "single":
            if not file_name:
                raise AirflowException("Azure download mode 'single' requires file_name.")
            expected_blob = file_name if str(file_name).lower().endswith(str(file_extension).lower()) else f"{file_name}{file_extension}"
            blobs = hook.get_blobs_list(container_name=container_name, prefix=file_name) or []
            matching_blobs = [blob_name for blob_name in blobs if blob_name.lower().endswith(str(expected_blob).lower())]
            if len(matching_blobs) > 1:
                raise AirflowException(f"Multiple blobs matched single file request '{expected_blob}': {matching_blobs}")
        else:
            if not file_prefix:
                raise AirflowException("Azure download mode 'multiple' requires file_prefix.")
            blobs = hook.get_blobs_list(container_name=container_name, prefix=file_prefix) or []
            matching_blobs = [blob_name for blob_name in blobs if blob_name.lower().endswith(file_extension.lower())]

        if not matching_blobs:
            raise AirflowException(
                f"No Azure blobs matched download criteria mode={download_mode}, prefix='{file_prefix}', extension='{file_extension}'."
            )

        downloaded = []
        for blob_name in matching_blobs:
            filename = os.path.basename(blob_name.rstrip("/"))
            if not filename:
                continue
            local_file_path = os.path.join(local_dir_download, filename)
            hook.get_file(file_path=local_file_path, container_name=container_name, blob_name=blob_name)
            downloaded.append({"blob_name": blob_name, "local_path": local_file_path})

        return {"downloaded_files": downloaded}


    task_node_1 = run_node_1()

    # Single task DAG (no chaining required).


dag = test_azdow_web()
