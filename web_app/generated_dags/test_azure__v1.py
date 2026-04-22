from airflow.exceptions import AirflowException
from airflow.exceptions import AirflowSkipException
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
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


_TASK_RESUME_SCOPE_BY_ENTRY = {'azure_1_node_3': ['azure_1_node_3']}


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
    dag_id='test_azure__v1',
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
def test_azure_v1():
    @task(task_id='azure_1_node_3')
    def run_node_3():
        container_name = 'my-container'
        download_mode = 'single'
        file_name = 'app'
        file_prefix = '' or ""
        file_extension = '.txt'
        local_dir_download = '/opt'
        default_download_dir = "/opt/airflow/local_downloads/azure"

        if file_extension and not str(file_extension).startswith("."):
            file_extension = f".{file_extension}"
        if file_name:
            file_name = str(file_name).lstrip("/")
        if file_prefix:
            file_prefix = str(file_prefix).lstrip("/")

        hook = WasbHook(wasb_conn_id="wasb_default")

        def _is_writable_directory(path_value):
            try:
                if not path_value:
                    return False
                os.makedirs(path_value, exist_ok=True)
                probe = os.path.join(path_value, ".write_test")
                with open(probe, "w", encoding="utf-8") as fh:
                    fh.write("ok")
                os.remove(probe)
                return True
            except Exception:
                return False

        requested_download_dir = str(local_dir_download or "").strip()
        if _is_writable_directory(requested_download_dir):
            local_dir_download = requested_download_dir
        elif _is_writable_directory(default_download_dir):
            local_dir_download = default_download_dir
        else:
            raise AirflowException(
                f"Azure download destination is not writable: '{requested_download_dir or '(empty)'}'. "
                f"Also failed fallback '{default_download_dir}'."
            )

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


    task_node_3 = run_node_3()

    # Single task DAG (no chaining required).


dag = test_azure_v1()
