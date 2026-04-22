from airflow.exceptions import AirflowException
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook
from datetime import datetime, timedelta, timezone
import json
import os
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
    dag_id='tetst_sftp_web',
    start_date=datetime(2026, 1, 1),
    schedule=_normalize_schedule(None),
    catchup=False,
    default_args=default_args,
    tags=['demo', 'orchestration'],
    description='Sequential orchestration pipeline',
)
def tetst_sftp_web():
    @task(task_id='sftp_1_node_1')
    def run_node_1():
        local_file_path = '/mnt/cegid/CUSTOMER_CUS_TEST.csv'
        remote_dir = '/uploads'
        if not os.path.exists(local_file_path):
            raise AirflowException(f"SFTP upload file not found: {local_file_path}")

        file_name = os.path.basename(local_file_path)
        remote_path = f"{remote_dir.rstrip('/')}/{file_name}"
        hook = SFTPHook(ssh_conn_id="sftp")
        hook.store_file(remote_full_path=remote_path, local_full_path=local_file_path)

        return {"uploaded": True, "local_file_path": local_file_path, "remote_path": remote_path}


    task_node_1 = run_node_1()

    # Single task DAG (no chaining required).


dag = tetst_sftp_web()
