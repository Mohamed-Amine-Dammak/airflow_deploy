from airflow.exceptions import AirflowException
from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook
from datetime import datetime, timedelta, timezone
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from urllib.parse import urlencode
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

_alert_emails = ['test@example.com']
_alert_mode = 'both'
if _alert_emails and _alert_mode in {"on_failure", "both"}:
    default_args["on_failure_callback"] = _build_alert_callback(_alert_emails, "on_failure")
if _alert_emails and _alert_mode in {"on_retry", "both"}:
    default_args["on_retry_callback"] = _build_alert_callback(_alert_emails, "on_retry")


@dag(
    dag_id='demo_n8n_cloudrun_dataform_powerbi',
    start_date=datetime(2026, 1, 1),
    schedule=_normalize_schedule(None),
    catchup=False,
    default_args=default_args,
    tags=['demo', 'orchestration'],
    description='Demo sequential orchestration pipeline',
)
def demo_n8n_cloudrun_dataform_powerbi():
    @task(task_id='n8n_1_node_1')
    def run_node_1():
        workflow_name = 'my_workflow'
        http_method = str('GET').upper()
        query_params = {'env': 'prod'} or {}
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

    @task(task_id='cloudrun_2_node_2')
    def run_node_2():
        cloud_run_url = 'https://example-cloudrun-url.run.app/process'
        service_account_file = 'C:/keys/sa-cloudrun.json'
        payload = {'dataset': 'raw', 'integration_source': 'n8n'}

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


    task_node_2 = run_node_2()

    @task(task_id='dataform_3_node_3')
    def run_node_3():
        project_id = 'os-dpf-airflow-prj-dev'
        region = 'europe-west1'
        repository = 'airflow-dataform'
        compilation_id = ''
        service_account_file = 'C:/keys/sa-dataform.json'
        tags = ['STG', 'DH', 'DW']

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

        if not compilation_id:
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
            compilation_id = full_name.split("/")[-1]

        url = f"https://dataform.googleapis.com/v1/{base_repo}/workflowInvocations"
        completed_tags = []

        for tag in tags:
            body = {
                "compilationResult": f"{base_repo}/compilationResults/{compilation_id}",
                "invocationConfig": {
                    "serviceAccount": service_account_email,
                    "includedTags": [tag],
                    "transitiveDependenciesIncluded": True,
                    "transitiveDependentsIncluded": False,
                },
            }

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
                    completed_tags.append(tag)
                    break
                if state in {"FAILED", "CANCELLED", "CANCELING"}:
                    raise AirflowException(f"Dataform tag '{tag}' failed with state={state}")
                time.sleep(15)

        return {"completed_tags": completed_tags}


    task_node_3 = run_node_3()

    @task(task_id='powerbi_4_node_4')
    def run_node_4():
        workspace_id = 'workspace-guid'
        dataset_id = 'dataset-guid'
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


    task_node_4 = run_node_4()

    task_node_1 >> task_node_2 >> task_node_3 >> task_node_4


dag = demo_n8n_cloudrun_dataform_powerbi()
