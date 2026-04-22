from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook
from datetime import datetime, timedelta, timezone
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from urllib.parse import urlencode
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
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

_alert_emails = []
_alert_mode = 'both'
if _alert_emails and _alert_mode in {"on_failure", "both"}:
    default_args["on_failure_callback"] = custom_failure_email(_alert_emails)
if _alert_emails and _alert_mode in {"on_retry", "both"}:
    default_args["on_retry_callback"] = custom_failure_email(_alert_emails)


@dag(
    dag_id='api_parallel_router',
    start_date=datetime(2026, 1, 1),
    schedule=_normalize_schedule(None),
    catchup=False,
    default_args=default_args,
    tags=['demo'],
    description='demo',
)
def api_parallel_router():
    @task(task_id='n8n_1_node_1')
    def run_node_1():
        workflow_name = 'wf'
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


    task_node_1 = run_node_1()

    task_fork = EmptyOperator(
        task_id='parallel_fork_2_fork',
    )

    @task(task_id='cloudrun_3_task_a')
    def run_task_a():
        cloud_run_url = 'https://x'
        service_account_file = 'k.json'
        payload = {}

        credentials = service_account.IDTokenCredentials.from_service_account_file(
            service_account_file,
            target_audience=cloud_run_url,
        )
        credentials.refresh(Request())

        headers = {
            "Authorization": f"Bearer {credentials.token}",
            "Content-Type": "application/json",
            "User-Agent": "Airflow",
        }

        response = requests.post(
            cloud_run_url,
            headers=headers,
            data=json.dumps(payload),
            timeout=300,
        )

        if response.status_code not in (200, 201, 202):
            raise AirflowException(
                f"Cloud Run call failed. status={response.status_code}, body={response.text}, url={cloud_run_url}"
            )

        return {
            "cloud_run_url": cloud_run_url,
            "status_code": response.status_code,
            "response_text": response.text,
        }


    task_task_a = run_task_a()

    @task(task_id='powerbi_4_task_b')
    def run_task_b():
        workspace_id = 'w'
        dataset_id = 'd'
        conn = BaseHook.get_connection("powerbi_api")
        extra = conn.extra_dejson

        tenant_id = extra.get("tenant_id")
        client_id = extra.get("client_id")
        client_secret = extra.get("client_secret")
        if not all([tenant_id, client_id, client_secret]):
            raise AirflowException("powerbi_api connection extra must include tenant_id, client_id, client_secret.")

        token_resp = requests.post(
            f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": "https://analysis.windows.net/powerbi/api/.default",
            },
            timeout=60,
        )
        token_resp.raise_for_status()
        access_token = token_resp.json()["access_token"]

        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

        list_resp = requests.get(
            f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets",
            headers=headers,
            timeout=60,
        )
        if list_resp.status_code != 200:
            raise AirflowException(
                f"Power BI dataset listing failed: status={list_resp.status_code}, body={list_resp.text}"
            )
        datasets = list_resp.json().get("value", [])
        if dataset_id not in [item.get("id") for item in datasets]:
            raise AirflowException(f"Dataset id '{dataset_id}' not found in workspace '{workspace_id}'.")

        refresh_resp = requests.post(
            f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes",
            headers=headers,
            timeout=60,
        )
        if refresh_resp.status_code not in (200, 202):
            raise AirflowException(
                f"Power BI refresh failed: status={refresh_resp.status_code}, body={refresh_resp.text}"
            )

        return {
            "workspace_id": workspace_id,
            "dataset_id": dataset_id,
            "status_code": refresh_resp.status_code,
        }


    task_task_b = run_task_b()

    task_join = EmptyOperator(
        task_id='parallel_join_5_join',
        trigger_rule='all_success',
    )

    router_target_task_ids_run_router = {
        'branch_1': 'talend_7_branch_1',
        'branch_2': 'talend_8_branch_2',
    }


    def _resolve_router_input_run_router(source: str, context: dict):
        source_key = str(source or "").strip()
        if source_key.startswith("xcom:"):
            source_task_id = source_key.split(":", 1)[1].strip()
            if not source_task_id:
                raise AirflowException("conditional_router input_source xcom reference is empty.")
            return context["ti"].xcom_pull(task_ids=source_task_id)

        dag_run = context.get("dag_run")
        dag_conf = getattr(dag_run, "conf", {}) if dag_run else {}
        if isinstance(dag_conf, dict) and source_key in dag_conf:
            return dag_conf.get(source_key)

        return source_key


    def _router_rule_match_run_router(match_type: str, candidate: str, expected: str) -> bool:
        if match_type == "starts_with":
            return candidate.startswith(expected)
        if match_type == "contains":
            return expected in candidate
        if match_type == "equals":
            return candidate == expected
        if match_type == "regex":
            return re.search(expected, candidate) is not None
        return False


    def _route_callable_run_router(**context):
        input_source = 'customer_type'
        rules = [{'match_type': 'equals', 'value': 'A', 'target_branch_node_id': 'branch_1'},
     {'match_type': 'equals', 'value': 'B', 'target_branch_node_id': 'branch_2'}]
        router_input = _resolve_router_input_run_router(input_source, context)
        candidate = "" if router_input is None else str(router_input)

        for rule in rules:
            match_type = str(rule.get("match_type", "")).strip().lower()
            expected = str(rule.get("value", ""))
            target_node_id = str(rule.get("target_branch_node_id", "")).strip()
            target_task_id = router_target_task_ids_run_router.get(target_node_id)
            if not target_task_id:
                continue
            if _router_rule_match_run_router(match_type, candidate, expected):
                print(
                    f"Router conditional_router_6_router matched rule match_type={match_type}, value={expected}, "
                    f"target_node_id={target_node_id}, target_task_id={target_task_id}"
                )
                return target_task_id

        raise AirflowException(
            f"Router conditional_router_6_router found no matching rule for input_source={input_source}, value={candidate}."
        )


    task_router = BranchPythonOperator(
        task_id='conditional_router_6_router',
        python_callable=_route_callable_run_router,
    )

    @task(task_id='talend_7_branch_1')
    def run_branch_1():
        job_name = 'j1'
        workspace_id = None

        conn = BaseHook.get_connection("talend_cloud")
        base_url = conn.host.rstrip("/")
        headers = {
            "Authorization": f"Bearer {conn.password}",
            "Content-Type": "application/json",
        }

        params = {"limit": 100, "offset": 0, "name": job_name}
        if workspace_id:
            params["workspaceId"] = workspace_id

        executable_resp = requests.get(
            f"{base_url}/orchestration/executables/tasks",
            headers=headers,
            params=params,
            timeout=60,
        )
        if executable_resp.status_code != 200:
            raise AirflowException(f"Talend executable lookup failed: {executable_resp.status_code} {executable_resp.text}")

        items = executable_resp.json().get("items", [])
        matched = [item for item in items if str(item.get("name", "")).strip() == str(job_name).strip()]
        if not matched:
            raise AirflowException(f"Talend job not found by name: {job_name}")
        if len(matched) > 1:
            raise AirflowException(f"Talend job name '{job_name}' is ambiguous.")

        executable_id = matched[0].get("executable")
        if not executable_id:
            raise AirflowException(f"Talend executable id is empty for job: {job_name}")

        trigger_resp = requests.post(
            f"{base_url}/processing/executions",
            headers=headers,
            json={"executable": executable_id},
            timeout=60,
        )
        if trigger_resp.status_code not in (200, 201):
            raise AirflowException(f"Talend trigger failed: {trigger_resp.status_code} {trigger_resp.text}")

        trigger_body = trigger_resp.json()
        execution_id = trigger_body.get("executionId") or trigger_body.get("id")
        if not execution_id:
            raise AirflowException(f"Talend trigger succeeded but no execution id was returned: {trigger_body}")

        logs_url = f"{base_url}/monitoring/executions/{execution_id}/logs"
        start = time.time()
        timeout_seconds = 300
        done_markers = ("completed", "success", "failed", "error")
        final_seen = None

        while True:
            logs_resp = requests.get(logs_url, headers={"Authorization": f"Bearer {conn.password}"}, timeout=60)
            if logs_resp.status_code != 200:
                raise AirflowException(f"Talend log polling failed: {logs_resp.status_code} {logs_resp.text}")

            payload = logs_resp.json()
            latest_logs = payload.get("data", [])
            if latest_logs:
                latest_msg = str(latest_logs[0].get("logMessage", "")).lower()
                final_seen = latest_logs[0]
                if any(marker in latest_msg for marker in done_markers):
                    break

            if time.time() - start > timeout_seconds:
                break
            time.sleep(10)

        print(f"Talend job '{job_name}' execution_id={execution_id} completed or timed out for monitor window.")
        return {
            "job_name": job_name,
            "executable_id": executable_id,
            "execution_id": execution_id,
            "last_log": final_seen,
        }


    task_branch_1 = run_branch_1()

    @task(task_id='talend_8_branch_2')
    def run_branch_2():
        job_name = 'j2'
        workspace_id = None

        conn = BaseHook.get_connection("talend_cloud")
        base_url = conn.host.rstrip("/")
        headers = {
            "Authorization": f"Bearer {conn.password}",
            "Content-Type": "application/json",
        }

        params = {"limit": 100, "offset": 0, "name": job_name}
        if workspace_id:
            params["workspaceId"] = workspace_id

        executable_resp = requests.get(
            f"{base_url}/orchestration/executables/tasks",
            headers=headers,
            params=params,
            timeout=60,
        )
        if executable_resp.status_code != 200:
            raise AirflowException(f"Talend executable lookup failed: {executable_resp.status_code} {executable_resp.text}")

        items = executable_resp.json().get("items", [])
        matched = [item for item in items if str(item.get("name", "")).strip() == str(job_name).strip()]
        if not matched:
            raise AirflowException(f"Talend job not found by name: {job_name}")
        if len(matched) > 1:
            raise AirflowException(f"Talend job name '{job_name}' is ambiguous.")

        executable_id = matched[0].get("executable")
        if not executable_id:
            raise AirflowException(f"Talend executable id is empty for job: {job_name}")

        trigger_resp = requests.post(
            f"{base_url}/processing/executions",
            headers=headers,
            json={"executable": executable_id},
            timeout=60,
        )
        if trigger_resp.status_code not in (200, 201):
            raise AirflowException(f"Talend trigger failed: {trigger_resp.status_code} {trigger_resp.text}")

        trigger_body = trigger_resp.json()
        execution_id = trigger_body.get("executionId") or trigger_body.get("id")
        if not execution_id:
            raise AirflowException(f"Talend trigger succeeded but no execution id was returned: {trigger_body}")

        logs_url = f"{base_url}/monitoring/executions/{execution_id}/logs"
        start = time.time()
        timeout_seconds = 300
        done_markers = ("completed", "success", "failed", "error")
        final_seen = None

        while True:
            logs_resp = requests.get(logs_url, headers={"Authorization": f"Bearer {conn.password}"}, timeout=60)
            if logs_resp.status_code != 200:
                raise AirflowException(f"Talend log polling failed: {logs_resp.status_code} {logs_resp.text}")

            payload = logs_resp.json()
            latest_logs = payload.get("data", [])
            if latest_logs:
                latest_msg = str(latest_logs[0].get("logMessage", "")).lower()
                final_seen = latest_logs[0]
                if any(marker in latest_msg for marker in done_markers):
                    break

            if time.time() - start > timeout_seconds:
                break
            time.sleep(10)

        print(f"Talend job '{job_name}' execution_id={execution_id} completed or timed out for monitor window.")
        return {
            "job_name": job_name,
            "executable_id": executable_id,
            "execution_id": execution_id,
            "last_log": final_seen,
        }


    task_branch_2 = run_branch_2()

    task_node_1 >> task_fork
    task_fork >> task_task_a
    task_fork >> task_task_b
    task_task_a >> task_join
    task_task_b >> task_join
    task_join >> task_router
    task_router >> task_branch_1
    task_router >> task_branch_2


dag = api_parallel_router()
