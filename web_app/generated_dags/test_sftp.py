from airflow.exceptions import AirflowException
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.microsoft.azure.sensors.wasb import WasbPrefixSensor
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
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


default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

_alert_emails = []
_alert_mode = 'both'
if _alert_emails and _alert_mode in {"on_failure", "both"}:
    default_args["on_failure_callback"] = custom_failure_email(_alert_emails)
if _alert_emails and _alert_mode in {"on_retry", "both"}:
    default_args["on_retry_callback"] = custom_failure_email(_alert_emails)


@dag(
    dag_id='test_sftp',
    start_date=datetime(2026, 1, 1),
    schedule=_normalize_schedule(None),
    catchup=False,
    default_args=default_args,
    tags=['demo', 'orchestration'],
    description='Orchestration pipeline',
)
def test_sftp():
    task_node_2 = WasbPrefixSensor(
        task_id='azure_blob_sensor_1_node_2',
        wasb_conn_id='wasb_default',
        container_name='my-container',
        prefix='uploads/CUSTOMER_',
        poke_interval=30,
        timeout=600,
        mode="poke",
    )

    @task(task_id='azure_2_node_1')
    def run_node_1():
        container_name = 'my-container'
        download_mode = 'multiple'
        file_name = ''
        file_prefix = 'uploads/CUSTOMER_' or ""
        file_extension = '.csv'
        local_dir_download = '/opt/airflow/dags/downloads'

        if file_extension and not str(file_extension).startswith("."):
            file_extension = f".{file_extension}"
        if file_name:
            file_name = str(file_name).lstrip("/")
        if file_prefix:
            file_prefix = str(file_prefix).lstrip("/")

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


    task_node_1 = run_node_1()

    task_node_3 = EmptyOperator(
        task_id='parallel_fork_3_node_3',
    )

    @task(task_id='sftp_sensor_4_node_4')
    def run_node_4():
        remote_dir = '/uploads'
        prefix = 'CUSTOMER_CUS_'
        normalized_prefix = str(prefix or "")
        sensor = SFTPSensor(
            task_id='sftp_sensor_4_node_4_sensor_inner',
            sftp_conn_id="sftp",
            path=remote_dir,
            file_pattern=f"{normalized_prefix}*",
            poke_interval=30,
            timeout=600,
            mode="poke",
        )
        sensor.execute(context={})

        hook = SFTPHook(ssh_conn_id="sftp")
        entries = hook.list_directory(remote_dir) or []
        matched = sorted([name for name in entries if str(name).startswith(normalized_prefix)])
        if not matched:
            raise AirflowException(
                f"SFTP sensor detected availability but no file matched prefix '{normalized_prefix}' in '{remote_dir}'."
            )

        detected_file_name = str(matched[0])
        detected_file_path = f"{remote_dir.rstrip('/')}/{detected_file_name}"
        return {
            "remote_dir": remote_dir,
            "prefix": normalized_prefix,
            "detected": True,
            "detected_file_name": detected_file_name,
            "detected_file_path": detected_file_path,
        }


    task_node_4 = run_node_4()

    @task(task_id='sftp_upload_5_node_5')
    def run_node_5():
        local_file_path = '/opt/airflow/dags/downloads'
        remote_dir = '/uploads'
        upload_mode = 'multiple'
        file_prefix = 'CUSTOMER_'
        hook = SFTPHook(ssh_conn_id="sftp")

        if upload_mode == "multiple":
            if not os.path.isdir(local_file_path):
                raise AirflowException(f"SFTP upload directory not found for multiple mode: {local_file_path}")
            prefix = str(file_prefix or "")
            if not prefix:
                raise AirflowException("sftp_upload.file_prefix is required when upload_mode is multiple.")

            matched_files = []
            for file_name in sorted(os.listdir(local_file_path)):
                full_path = os.path.join(local_file_path, file_name)
                if not os.path.isfile(full_path):
                    continue
                if not file_name.startswith(prefix):
                    continue
                matched_files.append(full_path)

            if not matched_files:
                raise AirflowException(
                    f"No local files found with prefix '{prefix}' in directory: {local_file_path}"
                )

            uploaded_remote_paths = []
            for full_path in matched_files:
                file_name = os.path.basename(full_path)
                remote_path = f"{remote_dir.rstrip('/')}/{file_name}"
                hook.store_file(remote_full_path=remote_path, local_full_path=full_path)
                uploaded_remote_paths.append(remote_path)

            return {
                "uploaded": True,
                "upload_mode": "multiple",
                "local_directory": local_file_path,
                "file_prefix": prefix,
                "uploaded_count": len(uploaded_remote_paths),
                "remote_paths": uploaded_remote_paths,
            }

        if not os.path.exists(local_file_path):
            raise AirflowException(f"SFTP upload file not found: {local_file_path}")

        file_name = os.path.basename(local_file_path)
        remote_path = f"{remote_dir.rstrip('/')}/{file_name}"
        hook.store_file(remote_full_path=remote_path, local_full_path=local_file_path)

        return {
            "uploaded": True,
            "upload_mode": "single",
            "local_file_path": local_file_path,
            "remote_path": remote_path,
        }


    task_node_5 = run_node_5()

    task_node_6 = EmptyOperator(
        task_id='parallel_join_6_node_6',
        trigger_rule='all_success',
    )

    router_target_task_ids_run_node_7 = {
        'node_8': 'n8n_8_node_8',
        'node_9': 'n8n_9_node_9',
    }

    router_source_task_ids_run_node_7 = {
        'node_2': 'azure_blob_sensor_1_node_2',
        'node_1': 'azure_2_node_1',
        'node_3': 'parallel_fork_3_node_3',
        'node_4': 'sftp_sensor_4_node_4',
        'node_5': 'sftp_upload_5_node_5',
        'node_6': 'parallel_join_6_node_6',
        'node_7': 'conditional_router_7_node_7',
        'node_8': 'n8n_8_node_8',
        'node_9': 'n8n_9_node_9',
    }


    def _resolve_router_input_run_node_7(source: str, context: dict):
        source_key = str(source or "").strip()
        if source_key.startswith("xcom_node:"):
            node_ref = source_key.split(":", 1)[1].strip()
            node_id, sep, dict_key = node_ref.partition(".")
            resolved_task_id = router_source_task_ids_run_node_7.get(node_id, "")
            if not resolved_task_id:
                raise AirflowException(
                    f"conditional_router unknown xcom_node reference '{node_id}'. "
                    f"Known node ids: {sorted(router_source_task_ids_run_node_7.keys())}"
                )
            pulled = context["ti"].xcom_pull(task_ids=resolved_task_id)
            if sep and dict_key and isinstance(pulled, dict):
                return pulled.get(dict_key)
            return pulled

        if source_key.startswith("xcom:"):
            task_ref = source_key.split(":", 1)[1].strip()
            source_task_id, sep, dict_key = task_ref.partition(".")
            if not source_task_id:
                raise AirflowException("conditional_router input_source xcom reference is empty.")
            pulled = context["ti"].xcom_pull(task_ids=source_task_id)
            if sep and dict_key and isinstance(pulled, dict):
                return pulled.get(dict_key)
            return pulled

        dag_run = context.get("dag_run")
        dag_conf = getattr(dag_run, "conf", {}) if dag_run else {}
        if isinstance(dag_conf, dict) and source_key in dag_conf:
            return dag_conf.get(source_key)

        return source_key


    def _router_rule_match_run_node_7(match_type: str, candidate: str, expected: str) -> bool:
        if match_type == "starts_with":
            return candidate.startswith(expected)
        if match_type == "contains":
            return expected in candidate
        if match_type == "equals":
            return candidate == expected
        if match_type == "regex":
            return re.search(expected, candidate) is not None
        return False


    def _route_callable_run_node_7(**context):
        input_source = 'xcom_node:node_4.detected_file_name'
        rules = [{'match_type': 'starts_with', 'value': 'CUS', 'target_branch_node_id': 'node_8'},
     {'match_type': 'starts_with', 'value': 'ok', 'target_branch_node_id': 'node_9'}]
        router_input = _resolve_router_input_run_node_7(input_source, context)
        candidate = "" if router_input is None else str(router_input)

        for rule in rules:
            match_type = str(rule.get("match_type", "")).strip().lower()
            expected = str(rule.get("value", ""))
            target_node_id = str(rule.get("target_branch_node_id", "")).strip()
            target_task_id = router_target_task_ids_run_node_7.get(target_node_id)
            if not target_task_id:
                continue
            if _router_rule_match_run_node_7(match_type, candidate, expected):
                print(
                    f"Router conditional_router_7_node_7 matched rule match_type={match_type}, value={expected}, "
                    f"target_node_id={target_node_id}, target_task_id={target_task_id}"
                )
                return target_task_id

        raise AirflowException(
            f"Router conditional_router_7_node_7 found no matching rule for input_source={input_source}, value={candidate}."
        )


    task_node_7 = BranchPythonOperator(
        task_id='conditional_router_7_node_7',
        python_callable=_route_callable_run_node_7,
    )

    @task(task_id='n8n_8_node_8')
    def run_node_8():
        workflow_name = 'test_airflow'
        http_method = str('GET').upper()
        query_params = None or {}
        request_body = None or {}
        use_test_webhook = True

        conn = BaseHook.get_connection("n8n_local")
        base_url = conn.host.rstrip("/")
        api_key = conn.password or ""

        headers = {
            "X-N8N-API-KEY": api_key,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        list_resp = requests.get(
            f"{base_url}/api/v1/workflows",
            headers=headers,
            params={"limit": 250},
            timeout=30,
        )
        list_resp.raise_for_status()
        data = list_resp.json()
        workflows = data.get("data", []) if isinstance(data, dict) else data
        matched = [wf for wf in workflows if str(wf.get("name", "")).strip() == str(workflow_name).strip()]
        if not matched:
            raise AirflowException(f"n8n workflow not found by name: {workflow_name}")
        if len(matched) > 1:
            raise AirflowException(f"Multiple n8n workflows found with name '{workflow_name}'.")

        workflow_id = str(matched[0].get("id"))
        details_resp = requests.get(f"{base_url}/api/v1/workflows/{workflow_id}", headers=headers, timeout=30)
        details_resp.raise_for_status()
        details = details_resp.json()

        nodes = details.get("nodes", []) or details.get("data", {}).get("nodes", [])
        webhook_paths = []
        for node in nodes:
            node_type = str(node.get("type", "")).lower()
            if "webhook" not in node_type:
                continue
            path = (node.get("parameters", {}) or {}).get("path")
            if isinstance(path, str) and path.strip():
                webhook_paths.append(path.strip().strip("/"))

        if not webhook_paths:
            raise AirflowException(f"n8n workflow '{workflow_name}' has no webhook path.")
        if len(webhook_paths) > 1:
            raise AirflowException(f"n8n workflow '{workflow_name}' has multiple webhook paths: {webhook_paths}")

        webhook_path = webhook_paths[0]
        prefix = "webhook-test" if use_test_webhook else "webhook"
        webhook_url = f"{base_url}/{prefix}/{webhook_path}"

        if http_method == "POST":
            trigger_resp = requests.post(webhook_url, params=query_params, json=request_body, timeout=45)
        else:
            trigger_resp = requests.get(webhook_url, params=query_params, timeout=45)

        if trigger_resp.status_code not in (200, 201, 202):
            raise AirflowException(
                f"n8n trigger failed for workflow '{workflow_name}': "
                f"status={trigger_resp.status_code}, body={trigger_resp.text}"
            )

        print(
            f"n8n triggered workflow_name={workflow_name}, workflow_id={workflow_id}, "
            f"webhook_path={webhook_path}, status={trigger_resp.status_code}"
        )
        return {
            "workflow_name": workflow_name,
            "workflow_id": workflow_id,
            "webhook_path": webhook_path,
            "status_code": trigger_resp.status_code,
        }


    task_node_8 = run_node_8()

    @task(task_id='n8n_9_node_9')
    def run_node_9():
        workflow_name = 'test_airflow1'
        http_method = str('GET').upper()
        query_params = None or {}
        request_body = None or {}
        use_test_webhook = True

        conn = BaseHook.get_connection("n8n_local")
        base_url = conn.host.rstrip("/")
        api_key = conn.password or ""

        headers = {
            "X-N8N-API-KEY": api_key,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        list_resp = requests.get(
            f"{base_url}/api/v1/workflows",
            headers=headers,
            params={"limit": 250},
            timeout=30,
        )
        list_resp.raise_for_status()
        data = list_resp.json()
        workflows = data.get("data", []) if isinstance(data, dict) else data
        matched = [wf for wf in workflows if str(wf.get("name", "")).strip() == str(workflow_name).strip()]
        if not matched:
            raise AirflowException(f"n8n workflow not found by name: {workflow_name}")
        if len(matched) > 1:
            raise AirflowException(f"Multiple n8n workflows found with name '{workflow_name}'.")

        workflow_id = str(matched[0].get("id"))
        details_resp = requests.get(f"{base_url}/api/v1/workflows/{workflow_id}", headers=headers, timeout=30)
        details_resp.raise_for_status()
        details = details_resp.json()

        nodes = details.get("nodes", []) or details.get("data", {}).get("nodes", [])
        webhook_paths = []
        for node in nodes:
            node_type = str(node.get("type", "")).lower()
            if "webhook" not in node_type:
                continue
            path = (node.get("parameters", {}) or {}).get("path")
            if isinstance(path, str) and path.strip():
                webhook_paths.append(path.strip().strip("/"))

        if not webhook_paths:
            raise AirflowException(f"n8n workflow '{workflow_name}' has no webhook path.")
        if len(webhook_paths) > 1:
            raise AirflowException(f"n8n workflow '{workflow_name}' has multiple webhook paths: {webhook_paths}")

        webhook_path = webhook_paths[0]
        prefix = "webhook-test" if use_test_webhook else "webhook"
        webhook_url = f"{base_url}/{prefix}/{webhook_path}"

        if http_method == "POST":
            trigger_resp = requests.post(webhook_url, params=query_params, json=request_body, timeout=45)
        else:
            trigger_resp = requests.get(webhook_url, params=query_params, timeout=45)

        if trigger_resp.status_code not in (200, 201, 202):
            raise AirflowException(
                f"n8n trigger failed for workflow '{workflow_name}': "
                f"status={trigger_resp.status_code}, body={trigger_resp.text}"
            )

        print(
            f"n8n triggered workflow_name={workflow_name}, workflow_id={workflow_id}, "
            f"webhook_path={webhook_path}, status={trigger_resp.status_code}"
        )
        return {
            "workflow_name": workflow_name,
            "workflow_id": workflow_id,
            "webhook_path": webhook_path,
            "status_code": trigger_resp.status_code,
        }


    task_node_9 = run_node_9()

    task_node_2 >> task_node_1
    task_node_1 >> task_node_3
    task_node_3 >> task_node_4
    task_node_3 >> task_node_5
    task_node_5 >> task_node_6
    task_node_4 >> task_node_6
    task_node_6 >> task_node_7
    task_node_7 >> task_node_8
    task_node_7 >> task_node_9


dag = test_sftp()
