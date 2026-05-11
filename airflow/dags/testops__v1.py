from airflow.exceptions import AirflowException
from airflow.exceptions import AirflowSkipException
from airflow.providers.sftp.hooks.sftp import SFTPHook
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


_TASK_RESUME_SCOPE_BY_ENTRY = {'sftp_upload_1_node_4': ['delay_2_node_1', 'delay_3_node_2', 'sftp_upload_1_node_4', 'sftp_upload_4_node_5'],
 'delay_2_node_1': ['delay_2_node_1', 'delay_3_node_2', 'sftp_upload_4_node_5'],
 'delay_3_node_2': ['delay_3_node_2', 'sftp_upload_4_node_5'],
 'sftp_upload_4_node_5': ['sftp_upload_4_node_5']}


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
    dag_id='testops__v1',
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
def testops_v1():
    @task(task_id='sftp_upload_1_node_4')
    def run_node_4():
        conn_name = 'sftp'
        action = 'upload'
        local_file_path = 'ok'
        local_file_paths = []
        local_directory_path = ''
        remote_dir = '/uploads'
        upload_mode = 'single'
        file_prefix = ''
        download_mode = 'single'
        remote_file_name = ''
        remote_file_prefix = ''
        local_dir_download = '/opt/airflow/local_downloads/sftp'
        delete_mode = 'single'
        delete_file_name = ''
        delete_prefix = ''
        hook = SFTPHook(ssh_conn_id=conn_name)

        if action == "upload":
            if upload_mode == "multiple":
                explicit_files = [str(item).strip() for item in (local_file_paths or []) if str(item).strip()]
                matched_files = []
                if explicit_files:
                    for full_path in explicit_files:
                        if not os.path.isfile(full_path):
                            raise AirflowException(f"SFTP upload file not found in local_file_paths: {full_path}")
                        matched_files.append(full_path)
                else:
                    base_dir = str(local_directory_path or local_file_path or "").strip()
                    if not os.path.isdir(base_dir):
                        raise AirflowException(f"SFTP upload directory not found for multiple mode: {base_dir}")
                    prefix = str(file_prefix or "")
                    if not prefix:
                        raise AirflowException("sftp_upload.file_prefix is required when upload_mode is multiple and local_file_paths is empty.")

                    for one_name in sorted(os.listdir(base_dir)):
                        full_path = os.path.join(base_dir, one_name)
                        if not os.path.isfile(full_path):
                            continue
                        if not one_name.startswith(prefix):
                            continue
                        matched_files.append(full_path)

                if not matched_files:
                    raise AirflowException("No local files resolved for SFTP upload in multiple mode.")

                uploaded_remote_paths = []
                for full_path in matched_files:
                    one_name = os.path.basename(full_path)
                    remote_path = f"{remote_dir.rstrip('/')}/{one_name}"
                    hook.store_file(remote_full_path=remote_path, local_full_path=full_path)
                    uploaded_remote_paths.append(remote_path)

                return {
                    "action": "upload",
                    "uploaded": True,
                    "upload_mode": "multiple",
                    "local_directory": str(local_directory_path or local_file_path or ""),
                    "file_prefix": str(file_prefix or ""),
                    "uploaded_count": len(uploaded_remote_paths),
                    "remote_paths": uploaded_remote_paths,
                }

            if not os.path.exists(local_file_path):
                raise AirflowException(f"SFTP upload file not found: {local_file_path}")

            one_name = os.path.basename(local_file_path)
            remote_path = f"{remote_dir.rstrip('/')}/{one_name}"
            hook.store_file(remote_full_path=remote_path, local_full_path=local_file_path)

            return {
                "action": "upload",
                "uploaded": True,
                "upload_mode": "single",
                "local_file_path": local_file_path,
                "remote_path": remote_path,
            }

        if action == "download":
            os.makedirs(local_dir_download, exist_ok=True)
            entries = hook.list_directory(remote_dir) or []
            file_entries = [str(item) for item in entries if str(item) not in {".", ".."}]
            remote_dir_norm = str(remote_dir or "").strip().strip("/")

            def _name_from_remote_input(raw_value):
                candidate = str(raw_value or "").strip().lstrip("/")
                if not candidate:
                    return ""
                if remote_dir_norm and (candidate == remote_dir_norm or candidate.startswith(remote_dir_norm + "/")):
                    candidate = candidate[len(remote_dir_norm):].lstrip("/")
                return candidate.split("/")[-1].strip()

            def _prefix_from_remote_input(raw_value):
                candidate = str(raw_value or "").strip().lstrip("/")
                if not candidate:
                    return ""
                if remote_dir_norm and (candidate == remote_dir_norm or candidate.startswith(remote_dir_norm + "/")):
                    candidate = candidate[len(remote_dir_norm):].lstrip("/")
                return candidate

            def _entry_basename(entry_value):
                return str(entry_value or "").replace("\\", "/").rstrip("/").split("/")[-1].strip()

            if download_mode == "single":
                target_name = _name_from_remote_input(remote_file_name)
                if not target_name:
                    raise AirflowException("sftp_upload.remote_file_name is required for single download mode.")
                matches = [name for name in file_entries if name == target_name or _entry_basename(name) == target_name]
            else:
                prefix = _prefix_from_remote_input(remote_file_prefix)
                if not prefix:
                    raise AirflowException("sftp_upload.remote_file_prefix is required for multiple download mode.")
                matches = [
                    name for name in file_entries
                    if name.startswith(prefix) or _entry_basename(name).startswith(prefix)
                ]

            if not matches:
                samples = ", ".join(file_entries[:10]) if file_entries else "<empty>"
                raise AirflowException(
                    f"No SFTP files matched in '{remote_dir}' for mode={download_mode}. "
                    f"prefix/name input='{remote_file_prefix if download_mode == 'multiple' else remote_file_name}'. "
                    f"Listed entries(sample): {samples}"
                )

            downloaded_files = []
            for one_name in sorted(matches):
                remote_path = f"{remote_dir.rstrip('/')}/{one_name}"
                local_path = os.path.join(local_dir_download, one_name)
                hook.retrieve_file(remote_full_path=remote_path, local_full_path=local_path)
                downloaded_files.append({"remote_path": remote_path, "local_path": local_path})

            return {
                "action": "download",
                "downloaded_count": len(downloaded_files),
                "downloaded_files": downloaded_files,
                "local_dir_download": local_dir_download,
            }

        if action == "delete":
            entries = hook.list_directory(remote_dir) or []
            file_entries = [str(item) for item in entries if str(item) not in {".", ".."}]
            remote_dir_norm = str(remote_dir or "").strip().strip("/")

            def _name_from_remote_input(raw_value):
                candidate = str(raw_value or "").strip().lstrip("/")
                if not candidate:
                    return ""
                if remote_dir_norm and (candidate == remote_dir_norm or candidate.startswith(remote_dir_norm + "/")):
                    candidate = candidate[len(remote_dir_norm):].lstrip("/")
                return candidate.split("/")[-1].strip()

            def _prefix_from_remote_input(raw_value):
                candidate = str(raw_value or "").strip().lstrip("/")
                if not candidate:
                    return ""
                if remote_dir_norm and (candidate == remote_dir_norm or candidate.startswith(remote_dir_norm + "/")):
                    candidate = candidate[len(remote_dir_norm):].lstrip("/")
                return candidate

            def _entry_basename(entry_value):
                return str(entry_value or "").replace("\\", "/").rstrip("/").split("/")[-1].strip()

            if delete_mode == "single":
                target_name = _name_from_remote_input(delete_file_name)
                if not target_name:
                    raise AirflowException("sftp_upload.delete_file_name is required for single delete mode.")
                matches = [name for name in file_entries if name == target_name or _entry_basename(name) == target_name]
            else:
                prefix = _prefix_from_remote_input(delete_prefix)
                if not prefix:
                    raise AirflowException("sftp_upload.delete_prefix is required for multiple delete mode.")
                matches = [
                    name for name in file_entries
                    if name.startswith(prefix) or _entry_basename(name).startswith(prefix)
                ]

            if not matches:
                raise AirflowException(f"No SFTP files matched delete criteria in '{remote_dir}' for mode={delete_mode}.")

            deleted_files = []
            for one_name in sorted(matches):
                remote_path = f"{remote_dir.rstrip('/')}/{one_name}"
                hook.delete_file(remote_path)
                deleted_files.append(remote_path)

            return {
                "action": "delete",
                "deleted_count": len(deleted_files),
                "deleted_files": deleted_files,
            }

        raise AirflowException(f"Unsupported sftp_upload.action: {action}")


    task_node_4 = run_node_4()

    @task(task_id='delay_2_node_1')
    def run_node_1():
        delay_minutes = int(1)
        if delay_minutes <= 0:
            raise AirflowException("delay.delay_minutes must be > 0.")
        time.sleep(delay_minutes * 60)
        return {"delayed_minutes": delay_minutes, "status": "completed"}


    task_node_1 = run_node_1()

    @task(task_id='delay_3_node_2')
    def run_node_2():
        delay_minutes = int(1)
        if delay_minutes <= 0:
            raise AirflowException("delay.delay_minutes must be > 0.")
        time.sleep(delay_minutes * 60)
        return {"delayed_minutes": delay_minutes, "status": "completed"}


    task_node_2 = run_node_2()

    @task(task_id='sftp_upload_4_node_5')
    def run_node_5():
        conn_name = 'sftp'
        action = 'upload'
        local_file_path = 'ok'
        local_file_paths = []
        local_directory_path = ''
        remote_dir = '/uploads'
        upload_mode = 'single'
        file_prefix = ''
        download_mode = 'single'
        remote_file_name = ''
        remote_file_prefix = ''
        local_dir_download = '/opt/airflow/local_downloads/sftp'
        delete_mode = 'single'
        delete_file_name = ''
        delete_prefix = ''
        hook = SFTPHook(ssh_conn_id=conn_name)

        if action == "upload":
            if upload_mode == "multiple":
                explicit_files = [str(item).strip() for item in (local_file_paths or []) if str(item).strip()]
                matched_files = []
                if explicit_files:
                    for full_path in explicit_files:
                        if not os.path.isfile(full_path):
                            raise AirflowException(f"SFTP upload file not found in local_file_paths: {full_path}")
                        matched_files.append(full_path)
                else:
                    base_dir = str(local_directory_path or local_file_path or "").strip()
                    if not os.path.isdir(base_dir):
                        raise AirflowException(f"SFTP upload directory not found for multiple mode: {base_dir}")
                    prefix = str(file_prefix or "")
                    if not prefix:
                        raise AirflowException("sftp_upload.file_prefix is required when upload_mode is multiple and local_file_paths is empty.")

                    for one_name in sorted(os.listdir(base_dir)):
                        full_path = os.path.join(base_dir, one_name)
                        if not os.path.isfile(full_path):
                            continue
                        if not one_name.startswith(prefix):
                            continue
                        matched_files.append(full_path)

                if not matched_files:
                    raise AirflowException("No local files resolved for SFTP upload in multiple mode.")

                uploaded_remote_paths = []
                for full_path in matched_files:
                    one_name = os.path.basename(full_path)
                    remote_path = f"{remote_dir.rstrip('/')}/{one_name}"
                    hook.store_file(remote_full_path=remote_path, local_full_path=full_path)
                    uploaded_remote_paths.append(remote_path)

                return {
                    "action": "upload",
                    "uploaded": True,
                    "upload_mode": "multiple",
                    "local_directory": str(local_directory_path or local_file_path or ""),
                    "file_prefix": str(file_prefix or ""),
                    "uploaded_count": len(uploaded_remote_paths),
                    "remote_paths": uploaded_remote_paths,
                }

            if not os.path.exists(local_file_path):
                raise AirflowException(f"SFTP upload file not found: {local_file_path}")

            one_name = os.path.basename(local_file_path)
            remote_path = f"{remote_dir.rstrip('/')}/{one_name}"
            hook.store_file(remote_full_path=remote_path, local_full_path=local_file_path)

            return {
                "action": "upload",
                "uploaded": True,
                "upload_mode": "single",
                "local_file_path": local_file_path,
                "remote_path": remote_path,
            }

        if action == "download":
            os.makedirs(local_dir_download, exist_ok=True)
            entries = hook.list_directory(remote_dir) or []
            file_entries = [str(item) for item in entries if str(item) not in {".", ".."}]
            remote_dir_norm = str(remote_dir or "").strip().strip("/")

            def _name_from_remote_input(raw_value):
                candidate = str(raw_value or "").strip().lstrip("/")
                if not candidate:
                    return ""
                if remote_dir_norm and (candidate == remote_dir_norm or candidate.startswith(remote_dir_norm + "/")):
                    candidate = candidate[len(remote_dir_norm):].lstrip("/")
                return candidate.split("/")[-1].strip()

            def _prefix_from_remote_input(raw_value):
                candidate = str(raw_value or "").strip().lstrip("/")
                if not candidate:
                    return ""
                if remote_dir_norm and (candidate == remote_dir_norm or candidate.startswith(remote_dir_norm + "/")):
                    candidate = candidate[len(remote_dir_norm):].lstrip("/")
                return candidate

            def _entry_basename(entry_value):
                return str(entry_value or "").replace("\\", "/").rstrip("/").split("/")[-1].strip()

            if download_mode == "single":
                target_name = _name_from_remote_input(remote_file_name)
                if not target_name:
                    raise AirflowException("sftp_upload.remote_file_name is required for single download mode.")
                matches = [name for name in file_entries if name == target_name or _entry_basename(name) == target_name]
            else:
                prefix = _prefix_from_remote_input(remote_file_prefix)
                if not prefix:
                    raise AirflowException("sftp_upload.remote_file_prefix is required for multiple download mode.")
                matches = [
                    name for name in file_entries
                    if name.startswith(prefix) or _entry_basename(name).startswith(prefix)
                ]

            if not matches:
                samples = ", ".join(file_entries[:10]) if file_entries else "<empty>"
                raise AirflowException(
                    f"No SFTP files matched in '{remote_dir}' for mode={download_mode}. "
                    f"prefix/name input='{remote_file_prefix if download_mode == 'multiple' else remote_file_name}'. "
                    f"Listed entries(sample): {samples}"
                )

            downloaded_files = []
            for one_name in sorted(matches):
                remote_path = f"{remote_dir.rstrip('/')}/{one_name}"
                local_path = os.path.join(local_dir_download, one_name)
                hook.retrieve_file(remote_full_path=remote_path, local_full_path=local_path)
                downloaded_files.append({"remote_path": remote_path, "local_path": local_path})

            return {
                "action": "download",
                "downloaded_count": len(downloaded_files),
                "downloaded_files": downloaded_files,
                "local_dir_download": local_dir_download,
            }

        if action == "delete":
            entries = hook.list_directory(remote_dir) or []
            file_entries = [str(item) for item in entries if str(item) not in {".", ".."}]
            remote_dir_norm = str(remote_dir or "").strip().strip("/")

            def _name_from_remote_input(raw_value):
                candidate = str(raw_value or "").strip().lstrip("/")
                if not candidate:
                    return ""
                if remote_dir_norm and (candidate == remote_dir_norm or candidate.startswith(remote_dir_norm + "/")):
                    candidate = candidate[len(remote_dir_norm):].lstrip("/")
                return candidate.split("/")[-1].strip()

            def _prefix_from_remote_input(raw_value):
                candidate = str(raw_value or "").strip().lstrip("/")
                if not candidate:
                    return ""
                if remote_dir_norm and (candidate == remote_dir_norm or candidate.startswith(remote_dir_norm + "/")):
                    candidate = candidate[len(remote_dir_norm):].lstrip("/")
                return candidate

            def _entry_basename(entry_value):
                return str(entry_value or "").replace("\\", "/").rstrip("/").split("/")[-1].strip()

            if delete_mode == "single":
                target_name = _name_from_remote_input(delete_file_name)
                if not target_name:
                    raise AirflowException("sftp_upload.delete_file_name is required for single delete mode.")
                matches = [name for name in file_entries if name == target_name or _entry_basename(name) == target_name]
            else:
                prefix = _prefix_from_remote_input(delete_prefix)
                if not prefix:
                    raise AirflowException("sftp_upload.delete_prefix is required for multiple delete mode.")
                matches = [
                    name for name in file_entries
                    if name.startswith(prefix) or _entry_basename(name).startswith(prefix)
                ]

            if not matches:
                raise AirflowException(f"No SFTP files matched delete criteria in '{remote_dir}' for mode={delete_mode}.")

            deleted_files = []
            for one_name in sorted(matches):
                remote_path = f"{remote_dir.rstrip('/')}/{one_name}"
                hook.delete_file(remote_path)
                deleted_files.append(remote_path)

            return {
                "action": "delete",
                "deleted_count": len(deleted_files),
                "deleted_files": deleted_files,
            }

        raise AirflowException(f"Unsupported sftp_upload.action: {action}")


    task_node_5 = run_node_5()

    task_node_1 >> task_node_2
    task_node_4 >> task_node_1
    task_node_2 >> task_node_5


dag = testops_v1()
