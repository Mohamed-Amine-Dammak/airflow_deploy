from airflow.exceptions import AirflowException
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.microsoft.azure.sensors.wasb import WasbPrefixSensor
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
    dag_id='my_pipeline_azure_sensor',
    start_date=datetime(2026, 1, 1),
    schedule=_normalize_schedule(None),
    catchup=False,
    default_args=default_args,
    tags=['demo', 'orchestration'],
    description='Sequential orchestration pipeline',
)
def my_pipeline_azure_sensor():
    task_node_1 = WasbPrefixSensor(
        task_id='azure_blob_sensor_1_node_1',
        wasb_conn_id='wasb_default',
        container_name='my-container',
        prefix='CUS_',
        poke_interval=20,
        timeout=600,
        mode="poke",
    )

    @task(task_id='azure_2_node_2')
    def run_node_2():
        container_name = 'my-container'
        download_mode = 'single'
        file_name = 'uploads/CUSTOMER_CUS_TEST'
        file_prefix = '' or ""
        file_extension = '.csv'
        local_dir_download = '/opt/airflow/dags/downloads'

        if file_extension and not str(file_extension).startswith("."):
            file_extension = f".{file_extension}"

        hook = WasbHook(wasb_conn_id="wasb_default")
        os.makedirs(local_dir_download, exist_ok=True)

        if download_mode == "single":
            if not file_name:
                raise AirflowException("Azure download mode 'single' requires file_name.")
            expected_blob = file_name if str(file_name).lower().endswith(str(file_extension).lower()) else f"{file_name}{file_extension}"
            expected_blob = str(expected_blob).lstrip("/")

            # Fast path: match exact path by prefix first.
            blobs = hook.get_blobs_list(container_name=container_name, prefix=expected_blob) or []
            matching_blobs = [blob_name for blob_name in blobs if str(blob_name).lower() == expected_blob.lower()]

            # Fallback: user enters only filename (without folder), match by basename.
            if not matching_blobs:
                all_blobs = hook.get_blobs_list(container_name=container_name, prefix="") or []
                matching_blobs = [
                    blob_name
                    for blob_name in all_blobs
                    if os.path.basename(str(blob_name).rstrip("/")).lower() == expected_blob.lower()
                ]

            if len(matching_blobs) > 1:
                raise AirflowException(
                    f"Multiple blobs matched '{expected_blob}'. Use a folder path in file_name to disambiguate. Matches={matching_blobs}"
                )
        else:
            if not file_prefix:
                raise AirflowException("Azure download mode 'multiple' requires file_prefix.")
            blobs = hook.get_blobs_list(container_name=container_name, prefix=file_prefix) or []
            matching_blobs = [blob_name for blob_name in blobs if blob_name.lower().endswith(file_extension.lower())]

        if not matching_blobs:
            if download_mode == "single":
                raise AirflowException(
                    f"No Azure blob matched single file '{expected_blob}' in container '{container_name}'. "
                    "If the file is in a folder, use folder path in file_name (without extension), e.g. uploads/CUSTOMER_CUS_TEST."
                )
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


    task_node_2 = run_node_2()

    task_node_1 >> task_node_2


dag = my_pipeline_azure_sensor()
