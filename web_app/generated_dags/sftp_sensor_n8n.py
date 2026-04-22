from airflow.exceptions import AirflowException
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.sftp.sensors.sftp import SFTPSensor
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
    dag_id='sftp_sensor_n8n',
    start_date=datetime(2026, 1, 1),
    schedule=_normalize_schedule(None),
    catchup=False,
    default_args=default_args,
    tags=['demo', 'orchestration'],
    description='Orchestration pipeline',
)
def sftp_sensor_n8n():
    @task(task_id='sftp_1_node_2')
    def run_node_2():
        remote_dir = '/uploads'
        prefix = 'CUSTOMER_CUS_'
        normalized_prefix = str(prefix or "")
        sensor = SFTPSensor(
            task_id='sftp_1_node_2_sensor_inner',
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


    task_node_2 = run_node_2()

    @task(task_id='n8n_2_node_3')
    def run_node_3():
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


    task_node_3 = run_node_3()

    task_node_2 >> task_node_3


dag = sftp_sensor_n8n()
