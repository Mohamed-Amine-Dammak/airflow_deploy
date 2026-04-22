from airflow.exceptions import AirflowException
from airflow.exceptions import AirflowSkipException
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator
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


_TASK_RESUME_SCOPE_BY_ENTRY = {'azure_1_node_1': ['azure_1_node_1']}


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
    dag_id='my_pipeline__v1',
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
def my_pipeline_v1():
    @task(task_id='azure_1_node_1')
    def run_node_1():
        local_file_path = 'dataops.png'
        local_file_paths = []
        container_name = 'my-container'
        upload_mode = 'single'
        blob_name = 'uploads/testdata'
        blob_prefix = ''
        create_container = False

        if upload_mode == "multiple":
            files = [str(item).strip() for item in (local_file_paths or []) if str(item).strip()]
            if not files:
                raise AirflowException("Azure multiple upload requires local_file_paths.")
            prefix = str(blob_prefix or "").strip().strip("/")

            uploaded_blobs = []
            for idx, one_file in enumerate(files):
                if not os.path.exists(one_file):
                    raise AirflowException(f"Local file does not exist for Azure upload: {one_file}")
                file_name = os.path.basename(one_file)
                target_blob = f"{prefix}/{file_name}" if prefix else file_name
                LocalFilesystemToWasbOperator(
                    task_id='azure_1_node_1_inner_upload_' + str(idx + 1),
                    wasb_conn_id="wasb_default",
                    file_path=one_file,
                    container_name=container_name,
                    blob_name=target_blob,
                    create_container=bool(create_container),
                ).execute(context={})
                uploaded_blobs.append(target_blob)

            return {
                "uploaded": True,
                "upload_mode": "multiple",
                "uploaded_count": len(uploaded_blobs),
                "uploaded_blobs": uploaded_blobs,
                "container_name": container_name,
                "create_container": bool(create_container),
            }

        if not os.path.exists(local_file_path):
            raise AirflowException(f"Local file does not exist for Azure upload: {local_file_path}")
        if not blob_name:
            raise AirflowException("Azure blob_name is required for single upload.")

        LocalFilesystemToWasbOperator(
            task_id='azure_1_node_1_inner_upload',
            wasb_conn_id="wasb_default",
            file_path=local_file_path,
            container_name=container_name,
            blob_name=blob_name,
            create_container=bool(create_container),
        ).execute(context={})

        return {
            "uploaded": True,
            "upload_mode": "single",
            "blob_name": blob_name,
            "container_name": container_name,
            "create_container": bool(create_container),
        }


    task_node_1 = run_node_1()

    # Single task DAG (no chaining required).


dag = my_pipeline_v1()
