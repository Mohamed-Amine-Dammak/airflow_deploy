from airflow.exceptions import AirflowException
from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook
from datetime import datetime, timedelta, timezone
from google.auth.transport.requests import Request
from google.oauth2 import service_account
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
    dag_id='test_dataform_web',
    start_date=datetime(2026, 1, 1),
    schedule=_normalize_schedule(None),
    catchup=False,
    default_args=default_args,
    tags=['demo', 'orchestration'],
    description='Sequential orchestration pipeline',
)
def test_dataform_web():
    @task
    def run_node_3_single_tag(
        tag: str,
        project_id: str,
        region: str,
        repository: str,
        compilation_id: str,
        service_account_file: str,
    ):
        credentials = service_account.Credentials.from_service_account_file(
            service_account_file,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        credentials.refresh(Request())

        with open(service_account_file, encoding="utf-8") as handle:
            service_account_email = json.load(handle)["client_email"]

        base_repo = f"projects/{project_id}/locations/{region}/repositories/{repository}"
        headers = {
            "Authorization": f"Bearer {credentials.token}",
            "Content-Type": "application/json",
        }

        resolved_compilation_id = compilation_id
        if not resolved_compilation_id:
            latest_compilation_resp = requests.get(
                f"https://dataform.googleapis.com/v1/{base_repo}/compilationResults",
                headers=headers,
                params={"pageSize": 1},
                timeout=60,
            )
            if latest_compilation_resp.status_code != 200:
                raise AirflowException(
                    "Dataform compilation_id is not provided and latest compilation lookup failed: "
                    f"{latest_compilation_resp.status_code} {latest_compilation_resp.text}"
                )
            latest_results = latest_compilation_resp.json().get("compilationResults", [])
            if not latest_results:
                raise AirflowException("No Dataform compilation results found. Provide compilation_id.")
            full_name = latest_results[0]["name"]
            resolved_compilation_id = full_name.split("/")[-1]

        body = {
            "compilationResult": f"{base_repo}/compilationResults/{resolved_compilation_id}",
            "invocationConfig": {
                "serviceAccount": service_account_email,
                "includedTags": [tag],
                "transitiveDependenciesIncluded": True,
                "transitiveDependentsIncluded": False,
            },
        }

        url = f"https://dataform.googleapis.com/v1/{base_repo}/workflowInvocations"
        create_resp = requests.post(url, headers=headers, json=body, timeout=60)
        if create_resp.status_code != 200:
            raise AirflowException(
                f"Dataform invocation failed for tag '{tag}': {create_resp.status_code} {create_resp.text}"
            )

        invocation_name = create_resp.json().get("name")
        if not invocation_name:
            raise AirflowException(f"Dataform invocation missing name for tag '{tag}'.")

        status_url = f"https://dataform.googleapis.com/v1/{invocation_name}"
        while True:
            status_resp = requests.get(status_url, headers=headers, timeout=60)
            if status_resp.status_code != 200:
                raise AirflowException(
                    f"Dataform status polling failed for tag '{tag}': "
                    f"{status_resp.status_code} {status_resp.text}"
                )
            state = status_resp.json().get("state")
            print(f"Dataform tag={tag} state={state} invocation={invocation_name}")
            if state == "SUCCEEDED":
                return {"tag": tag, "state": state, "invocation_name": invocation_name}
            if state in {"FAILED", "CANCELLED", "CANCELING"}:
                raise AirflowException(f"Dataform tag '{tag}' failed with state={state}")
            time.sleep(15)


    project_id_for_tags = 'os-dpf-ariflow-prj-dev' or ([][0] if [] else "")
    region_for_tags = 'europe-west1'
    repository_for_tags = 'airflow-dataform'
    compilation_id_for_tags = '9b22a514-c43e-4b07-b75d-f38d19ba0760'
    service_account_file_for_tags = '/opt/airflow/os-dpf-ariflow-prj-dev-3b50cbb16478.json'
    dataform_tags_list = ['STG', 'DH', 'DW']

    if not dataform_tags_list:
        raise AirflowException("Dataform tags list is empty for tag-based mode.")


    def _safe_tag_task_id(tag: str) -> str:
        safe = "".join(ch if ch.isalnum() else "_" for ch in str(tag).lower())
        safe = "_".join(part for part in safe.split("_") if part)
        return f"dataform_{safe or 'tag'}"


    _previous_dataform_tag_task = None
    task_node_3_entry = None
    task_node_3_exit = None

    for _tag in dataform_tags_list:
        _current_dataform_tag_task = run_node_3_single_tag.override(task_id=_safe_tag_task_id(_tag))(
            tag=_tag,
            project_id=project_id_for_tags,
            region=region_for_tags,
            repository=repository_for_tags,
            compilation_id=compilation_id_for_tags,
            service_account_file=service_account_file_for_tags,
        )

        if task_node_3_entry is None:
            task_node_3_entry = _current_dataform_tag_task

        if _previous_dataform_tag_task is not None:
            _previous_dataform_tag_task >> _current_dataform_tag_task

        _previous_dataform_tag_task = _current_dataform_tag_task
        task_node_3_exit = _current_dataform_tag_task

    if task_node_3_entry is None or task_node_3_exit is None:
        raise AirflowException("Unable to build Dataform tag tasks.")

    # Single task DAG (no chaining required).


dag = test_dataform_web()
