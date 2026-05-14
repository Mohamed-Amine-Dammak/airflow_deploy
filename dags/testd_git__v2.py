from airflow.exceptions import AirflowException
from airflow.exceptions import AirflowSkipException
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


_TASK_RESUME_SCOPE_BY_ENTRY = {'delay_1_node_1': ['delay_1_node_1', 'delay_2_node_2'], 'delay_2_node_2': ['delay_2_node_2']}


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
    "retries": 1,
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
    dag_id='testd_git__v2',
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
def testd_v2():
    @task(task_id='delay_1_node_1')
    def run_node_1():
        delay_minutes = int(1)
        if delay_minutes <= 0:
            raise AirflowException("delay.delay_minutes must be > 0.")
        time.sleep(delay_minutes * 60)
        return {"delayed_minutes": delay_minutes, "status": "completed"}


    task_node_1 = run_node_1()

    @task(task_id='delay_2_node_2')
    def run_node_2():
        delay_minutes = int(1)
        if delay_minutes <= 0:
            raise AirflowException("delay.delay_minutes must be > 0.")
        time.sleep(delay_minutes * 60)
        return {"delayed_minutes": delay_minutes, "status": "completed"}


    task_node_2 = run_node_2()

    task_node_1 >> task_node_2


dag = testd_v2()
