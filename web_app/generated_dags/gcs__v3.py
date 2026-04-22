from airflow.exceptions import AirflowException
from airflow.exceptions import AirflowSkipException
from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook
from datetime import datetime, timedelta, timezone
from google.cloud import storage
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import json
import os
import re
import requests
import tempfile
import time
from utils.etl_tasks import custom_failure_email


def _normalize_schedule(schedule):
    if schedule in (None, "", "none", "null", "None"):
        return None
    return schedule


_TASK_RESUME_SCOPE_BY_ENTRY = {'gcs_1_node_1': ['gcs_1_node_1']}


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
    dag_id='gcs__v3',
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
def gcs_v3():
    @task(task_id='gcs_1_node_1')
    def run_node_1():
        action = 'upload'
        bucket_name = 'os-dpf-ariflow-prj-shr-dev'
        service_account_file = '/opt/airflow/credgcp.json'
        destination_path = 'CUSTOMER_CUS_TEST.csv'
        upload_mode = 'single'
        source_type = 'local'
        local_file_path = '/mnt/cegid/CUSTOMER_CUS_TEST.csv'
        local_directory_path = ''
        local_file_paths = []
        drive_file_link = ''
        drive_input_mode = 'directory'
        drive_directory_link = ''
        drive_file_links = []
        source_object = 'sample_upload.txt/CUSTOMER_CUS_TEST.csv'
        destination_bucket_name = 'os-dpf-ariflow-prj-shr-dev'
        destination_object = 'CUSTOMER_CUS_TEST.csv'
        delete_mode = 'single'
        object_name = 'CUSTOMER_CUS_TEST.csv'
        object_names = []

        def _require_service_account_file(path):
            safe_path = str(path or "").strip()
            if not safe_path:
                raise AirflowException("gcs.service_account_file is required.")
            if not os.path.exists(safe_path):
                raise AirflowException(f"Service account file not found: {safe_path}")
            return safe_path

        def _build_storage_client(path):
            credentials = service_account.Credentials.from_service_account_file(
                path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            return storage.Client(credentials=credentials)

        def _build_drive_service(path):
            credentials = service_account.Credentials.from_service_account_file(
                path,
                scopes=["https://www.googleapis.com/auth/drive.readonly"],
            )
            return build("drive", "v3", credentials=credentials, cache_discovery=False)

        def _sanitize_prefix(prefix):
            return str(prefix or "").strip().strip("/")

        def _join_object(prefix, file_name):
            clean_name = str(file_name or "").strip().lstrip("/")
            if not clean_name:
                raise AirflowException("GCS object name cannot be empty.")
            clean_prefix = _sanitize_prefix(prefix)
            return f"{clean_prefix}/{clean_name}" if clean_prefix else clean_name

        def _extract_drive_id(link, kind):
            text = str(link or "").strip()
            if not text:
                return ""
            patterns = [
                rf"/{kind}s?/d/([A-Za-z0-9_-]+)",
                rf"/{kind}s?/([A-Za-z0-9_-]+)",
                r"[?&]id=([A-Za-z0-9_-]+)",
            ]
            for pattern in patterns:
                match = re.search(pattern, text)
                if match:
                    return match.group(1)
            if re.match(r"^[A-Za-z0-9_-]+$", text):
                return text
            return ""

        def _download_drive_file_to_tmp(drive_service, file_id, target_dir):
            metadata = drive_service.files().get(fileId=file_id, fields="id,name,mimeType").execute()
            file_name = str(metadata.get("name") or file_id)
            local_path = os.path.join(target_dir, file_name)
            request = drive_service.files().get_media(fileId=file_id)
            with open(local_path, "wb") as f:
                downloader = MediaIoBaseDownload(f, request)
                done = False
                while not done:
                    _status, done = downloader.next_chunk()
            return local_path, metadata

        service_account_file = _require_service_account_file(service_account_file)

        if action == "upload":
            if not bucket_name:
                raise AirflowException("gcs.bucket_name is required for upload.")

            source_files = []
            downloaded_tmp_dir = None

            if source_type == "local":
                if upload_mode == "single":
                    resolved_single = str(local_file_path or "").strip()
                    if not resolved_single:
                        raise AirflowException("gcs.local_file_path is required for single local upload.")
                    source_files = [resolved_single]
                else:
                    explicit_files = [str(item).strip() for item in (local_file_paths or []) if str(item).strip()]
                    if explicit_files:
                        source_files = explicit_files
                    else:
                        directory = str(local_directory_path or "").strip()
                        if not directory:
                            raise AirflowException(
                                "Provide gcs.local_file_paths or gcs.local_directory_path for multiple local upload."
                            )
                        if not os.path.isdir(directory):
                            raise AirflowException(f"Local directory not found: {directory}")
                        source_files = [
                            os.path.join(directory, file_name)
                            for file_name in sorted(os.listdir(directory))
                            if os.path.isfile(os.path.join(directory, file_name))
                        ]
            elif source_type == "drive":
                drive_service = _build_drive_service(service_account_file)
                downloaded_tmp_dir = tempfile.mkdtemp(prefix="gcs_drive_upload_")

                drive_file_ids = []
                if upload_mode == "single":
                    single_id = _extract_drive_id(drive_file_link, "file")
                    if not single_id:
                        raise AirflowException("Invalid gcs.drive_file_link for single Drive upload.")
                    drive_file_ids = [single_id]
                else:
                    if drive_input_mode == "directory":
                        folder_id = _extract_drive_id(drive_directory_link, "folder")
                        if not folder_id:
                            raise AirflowException("Invalid gcs.drive_directory_link for directory upload.")
                        page_token = None
                        while True:
                            resp = drive_service.files().list(
                                q=f"'{folder_id}' in parents and trashed = false",
                                fields="nextPageToken, files(id, name, mimeType)",
                                pageToken=page_token,
                            ).execute()
                            for item in resp.get("files", []):
                                if item.get("mimeType") == "application/vnd.google-apps.folder":
                                    continue
                                if item.get("id"):
                                    drive_file_ids.append(item["id"])
                            page_token = resp.get("nextPageToken")
                            if not page_token:
                                break
                    else:
                        drive_file_ids = [
                            _extract_drive_id(link, "file")
                            for link in (drive_file_links or [])
                            if str(link or "").strip()
                        ]
                        drive_file_ids = [item for item in drive_file_ids if item]

                    if not drive_file_ids:
                        raise AirflowException("No Drive files resolved for multiple upload.")

                for file_id in drive_file_ids:
                    local_path, _metadata = _download_drive_file_to_tmp(drive_service, file_id, downloaded_tmp_dir)
                    source_files.append(local_path)
            else:
                raise AirflowException(f"Unsupported gcs.source_type: {source_type}")

            if not source_files:
                raise AirflowException("No source files resolved for GCS upload.")

            client = _build_storage_client(service_account_file)
            bucket_ref = client.bucket(bucket_name)

            uploaded_objects = []
            for local_path in source_files:
                if not os.path.isfile(local_path):
                    raise AirflowException(f"Local file not found for GCS upload: {local_path}")
                file_name = os.path.basename(local_path)
                object_key = _join_object(destination_path, file_name)
                blob = bucket_ref.blob(object_key)
                blob.upload_from_filename(local_path)
                uploaded_objects.append({"local_path": local_path, "object_name": object_key})

            return {
                "action": "upload",
                "bucket_name": bucket_name,
                "service_account_file": service_account_file,
                "upload_mode": upload_mode,
                "source_type": source_type,
                "uploaded_count": len(uploaded_objects),
                "uploaded_objects": uploaded_objects,
                "tmp_download_dir": downloaded_tmp_dir,
            }

        if action == "move":
            if not source_object:
                raise AirflowException("gcs.source_object is required for move.")
            if not destination_bucket_name:
                raise AirflowException("gcs.destination_bucket_name is required for move.")
            if not destination_object:
                raise AirflowException("gcs.destination_object is required for move.")

            client = _build_storage_client(service_account_file)
            source_bucket_ref = client.bucket(bucket_name)
            destination_bucket_ref = client.bucket(destination_bucket_name)

            source_blob = source_bucket_ref.blob(source_object)
            source_bucket_ref.copy_blob(source_blob, destination_bucket_ref, destination_object)
            source_blob.delete()

            return {
                "action": "move",
                "service_account_file": service_account_file,
                "source_bucket": bucket_name,
                "source_object": source_object,
                "destination_bucket": destination_bucket_name,
                "destination_object": destination_object,
                "moved": True,
            }

        if action == "delete":
            if delete_mode == "single":
                targets = [str(object_name or "").strip()]
            else:
                targets = [str(item).strip() for item in (object_names or []) if str(item).strip()]

            targets = [item for item in targets if item]
            if not targets:
                raise AirflowException("No object names provided for GCS delete.")

            client = _build_storage_client(service_account_file)
            bucket_ref = client.bucket(bucket_name)

            deleted = []
            for target in targets:
                blob = bucket_ref.blob(target)
                blob.delete()
                deleted.append(target)

            return {
                "action": "delete",
                "service_account_file": service_account_file,
                "bucket_name": bucket_name,
                "delete_mode": delete_mode,
                "deleted_count": len(deleted),
                "deleted_objects": deleted,
            }

        raise AirflowException(f"Unsupported gcs.action: {action}")


    task_node_1 = run_node_1()

    # Single task DAG (no chaining required).


dag = gcs_v3()
