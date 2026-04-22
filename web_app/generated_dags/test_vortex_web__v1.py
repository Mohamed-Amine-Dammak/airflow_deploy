from airflow.exceptions import AirflowException
from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.empty import EmptyOperator
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


_TASK_RESUME_SCOPE_BY_ENTRY = {'n8n_1_node_1': ['cloudrun_2_node_2',
                  'dataform_dh_boond',
                  'dataform_dw_boond',
                  'dataform_sh_brevo',
                  'dataform_stg_boond',
                  'n8n_1_node_1',
                  'parallel_fork_3_node_7',
                  'parallel_join_8_node_9',
                  'powerbi_9_node_4'],
 'cloudrun_2_node_2': ['cloudrun_2_node_2',
                       'dataform_dh_boond',
                       'dataform_dw_boond',
                       'dataform_sh_brevo',
                       'dataform_stg_boond',
                       'parallel_fork_3_node_7',
                       'parallel_join_8_node_9',
                       'powerbi_9_node_4'],
 'parallel_fork_3_node_7': ['dataform_dh_boond',
                            'dataform_dw_boond',
                            'dataform_sh_brevo',
                            'dataform_stg_boond',
                            'parallel_fork_3_node_7',
                            'parallel_join_8_node_9',
                            'powerbi_9_node_4'],
 'dataform_stg_boond': ['dataform_dh_boond',
                        'dataform_dw_boond',
                        'dataform_stg_boond',
                        'parallel_join_8_node_9',
                        'powerbi_9_node_4'],
 'dataform_sh_brevo': ['dataform_sh_brevo', 'parallel_join_8_node_9', 'powerbi_9_node_4'],
 'dataform_dh_boond': ['dataform_dh_boond', 'dataform_dw_boond', 'parallel_join_8_node_9', 'powerbi_9_node_4'],
 'dataform_dw_boond': ['dataform_dw_boond', 'parallel_join_8_node_9', 'powerbi_9_node_4'],
 'parallel_join_8_node_9': ['parallel_join_8_node_9', 'powerbi_9_node_4'],
 'powerbi_9_node_4': ['powerbi_9_node_4']}


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
    dag_id='test_vortex_web__v1',
    start_date=datetime(
        2026,
        1,
        1
    ),
    schedule=_normalize_schedule(None),
    catchup=False,
    default_args=default_args,
    tags=['demo', 'orchestration'],
    description='Sequential orchestration pipeline',
)
def test_vortex_web_v1():
    @task(task_id='n8n_1_node_1')
    def run_node_1():
        workflow_name = 'VORTEX_DataSyncFlow_Full'
        http_method = str('GET').upper()
        query_params = None or {}
        request_body = None or {}
        use_test_webhook = True
        monitor_poll_interval_seconds = int(5)
        monitor_timeout_seconds = int(300)

        conn = BaseHook.get_connection("n8n_local")
        base_url = conn.host.rstrip("/")
        api_key = conn.password or ""

        headers = {
            "X-N8N-API-KEY": api_key,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        if monitor_poll_interval_seconds <= 0:
            raise AirflowException("n8n.monitor_poll_interval_seconds must be > 0")
        if monitor_timeout_seconds <= 0:
            raise AirflowException("n8n.monitor_timeout_seconds must be > 0")

        def _parse_ts(raw_value):
            if not raw_value:
                return None
            value = str(raw_value).strip()
            if not value:
                return None
            value = value.replace("Z", "+00:00")
            dt = datetime.fromisoformat(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt

        def _print_n8n_execution_details(exec_data):
            print("[N8N] execution summary")
            summary_fields = [
                ("workflow_name", workflow_name),
                ("workflow_id", workflow_id),
                ("execution_id", exec_data.get("id")),
                ("status", exec_data.get("status")),
                ("mode", exec_data.get("mode")),
                ("startedAt", exec_data.get("startedAt")),
                ("stoppedAt", exec_data.get("stoppedAt")),
                ("finished", exec_data.get("finished")),
                ("retryOf", exec_data.get("retryOf")),
            ]
            for key, value in summary_fields:
                print(f"[N8N] {key}: {value}")

            run_data = (((exec_data.get("data") or {}).get("resultData") or {}).get("runData") or {})
            if run_data:
                nodes_total = len(run_data.keys())
                failed_nodes = []
                for node_name, runs in run_data.items():
                    for run in (runs or []):
                        run_status = str(run.get("status") or "").strip().lower()
                        if run_status in {"error", "failed", "crashed"}:
                            failed_nodes.append(node_name)
                            break
                print(f"[N8N] nodes_executed={nodes_total}")
                if failed_nodes:
                    print(f"[N8N] failed_nodes={','.join(failed_nodes)}")
            top_error = (((exec_data.get("data") or {}).get("resultData") or {}).get("error")) or {}
            if top_error:
                print("[N8N] error=" + json.dumps(top_error, ensure_ascii=True, default=str))

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
        triggered_at = datetime.now(timezone.utc).isoformat()

        if http_method == "POST":
            trigger_resp = requests.post(webhook_url, params=query_params, json=request_body, timeout=45)
        else:
            trigger_resp = requests.get(webhook_url, params=query_params, timeout=45)

        if trigger_resp.status_code not in (200, 201, 202):
            raise AirflowException(
                f"n8n trigger failed for workflow '{workflow_name}': "
                    f"status={trigger_resp.status_code}, body={trigger_resp.text}"
            )

        trigger_json = {}
        try:
            trigger_json = trigger_resp.json() if trigger_resp.text else {}
        except Exception:
            trigger_json = {}

        print(
            f"[AIRFLOW] n8n triggered workflow_name={workflow_name}, workflow_id={workflow_id}, "
            f"webhook_path={webhook_path}, status={trigger_resp.status_code}, triggered_at={triggered_at}"
        )

        execution_id = trigger_json.get("executionId") or trigger_json.get("id")
        terminal_statuses = {"success", "error", "failed", "crashed", "canceled"}
        last_seen_status = None
        matched_execution = None
        deadline = time.time() + monitor_timeout_seconds
        trigger_dt = _parse_ts(triggered_at)

        while time.time() < deadline:
            if execution_id:
                exec_resp = requests.get(
                    f"{base_url}/api/v1/executions/{execution_id}",
                    headers=headers,
                    params={"includeData": "true"},
                    timeout=30,
                )
                exec_resp.raise_for_status()
                payload = exec_resp.json()
                if isinstance(payload, dict) and "data" in payload and isinstance(payload.get("data"), dict):
                    matched_execution = payload["data"]
                else:
                    matched_execution = payload
            else:
                list_exec_resp = requests.get(
                    f"{base_url}/api/v1/executions",
                    headers=headers,
                    params={
                        "workflowId": workflow_id,
                        "limit": 20,
                        "includeData": "true",
                    },
                    timeout=30,
                )
                list_exec_resp.raise_for_status()
                payload = list_exec_resp.json()
                executions = payload.get("data", []) if isinstance(payload, dict) else payload
                executions = executions if isinstance(executions, list) else []
                candidates = []
                for execution in executions:
                    started = _parse_ts(execution.get("startedAt"))
                    if started is None:
                        continue
                    if trigger_dt is None or started >= trigger_dt:
                        candidates.append((started, execution))

                if candidates:
                    candidates.sort(key=lambda item: item[0], reverse=True)
                    matched_execution = candidates[0][1]
                    execution_id = matched_execution.get("id") or execution_id

            if matched_execution:
                status = str(matched_execution.get("status") or "").lower()
                if status and status != last_seen_status:
                    print(f"[N8N] status={status} execution_id={execution_id}")
                last_seen_status = status or last_seen_status
                if status in terminal_statuses:
                    break

            time.sleep(monitor_poll_interval_seconds)

        if not matched_execution:
            raise AirflowException(
                f"n8n execution was triggered but not found before timeout. "
                f"workflow_name={workflow_name}, workflow_id={workflow_id}, timeout={monitor_timeout_seconds}s"
            )

        _print_n8n_execution_details(matched_execution)

        final_status = str(matched_execution.get("status") or "").lower()
        if final_status != "success":
            raise AirflowException(
                f"n8n execution did not complete successfully. "
                f"workflow_name={workflow_name}, workflow_id={workflow_id}, execution_id={execution_id}, "
                f"status={final_status or last_seen_status or 'unknown'}"
            )

        return {
            "workflow_name": workflow_name,
            "workflow_id": workflow_id,
            "webhook_path": webhook_path,
            "status_code": trigger_resp.status_code,
            "execution_id": execution_id,
            "execution_status": final_status,
            "triggered_at": triggered_at,
        }


    task_node_1 = run_node_1()

    @task(task_id='cloudrun_2_node_2')
    def run_node_2():
        cloud_run_url = 'https://ingestioncloudrun-193541357649.europe-west1.run.app/primary_keys'
        service_account_file = '/opt/airflow/cred.json'
        payload = {'dataset': 'raw',
     'bucket_name': 'os-dpf-vortex-prj-dev',
     'integration_source': 'n8n',
     'data_source': 'boond',
     'tables': {'ABSENCES': {'separator': ';', 'mode': 'full'},
                'ADMINISTRATIVE': {'separator': ';', 'mode': 'delta'},
                'AGENCIES': {'separator': ';', 'mode': 'full'},
                'COMPANIES': {'separator': ';', 'mode': 'full'},
                'CONTACTS': {'separator': ';', 'mode': 'full'},
                'DAYS_OFF': {'separator': ';', 'mode': 'full'},
                'DELIVERIES': {'separator': ';', 'mode': 'full'},
                'DICTIONARY': {'separator': ';', 'mode': 'full'},
                'INVOICES': {'separator': ';', 'mode': 'full'},
                'POSTPRODUCTION_DETAILS': {'separator': ';', 'mode': 'delta'},
                'PREV_YEAR_LEAVE_BALANCE': {'separator': ';', 'mode': 'full'},
                'PROJECTS': {'separator': ';', 'mode': 'full'},
                'RESOURCES': {'separator': ';', 'mode': 'full'},
                'EXTERNAL_DELIVERIES': {'separator': '|', 'mode': 'full'},
                'TIMESHEET': {'separator': ';', 'mode': 'full'},
                'RESOURCES_IMAGES': {'separator': ';', 'mode': 'delta'}}}

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

    task_node_7 = EmptyOperator(
        task_id='parallel_fork_3_node_7',
    )

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


    project_id_for_tags = 'os-dpf-vortex-prj-dev' or ([][0] if [] else "")
    region_for_tags = 'europe-west1'
    repository_for_tags = 'vortex_dev'
    compilation_id_for_tags = '2ceb50cd-f9a1-4a81-8e60-3a36b2b32466'
    service_account_file_for_tags = '/opt/airflow/cred.json'
    dataform_tags_list = ['STG_BOOND']

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

    @task
    def run_node_8_single_tag(
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


    project_id_for_tags = 'os-dpf-vortex-prj-dev' or ([][0] if [] else "")
    region_for_tags = 'europe-west1'
    repository_for_tags = 'vortex_dev'
    compilation_id_for_tags = '2ceb50cd-f9a1-4a81-8e60-3a36b2b32466'
    service_account_file_for_tags = '/opt/airflow/cred.json'
    dataform_tags_list = ['SH_BREVO']

    if not dataform_tags_list:
        raise AirflowException("Dataform tags list is empty for tag-based mode.")


    def _safe_tag_task_id(tag: str) -> str:
        safe = "".join(ch if ch.isalnum() else "_" for ch in str(tag).lower())
        safe = "_".join(part for part in safe.split("_") if part)
        return f"dataform_{safe or 'tag'}"


    _previous_dataform_tag_task = None
    task_node_8_entry = None
    task_node_8_exit = None

    for _tag in dataform_tags_list:
        _current_dataform_tag_task = run_node_8_single_tag.override(task_id=_safe_tag_task_id(_tag))(
            tag=_tag,
            project_id=project_id_for_tags,
            region=region_for_tags,
            repository=repository_for_tags,
            compilation_id=compilation_id_for_tags,
            service_account_file=service_account_file_for_tags,
        )

        if task_node_8_entry is None:
            task_node_8_entry = _current_dataform_tag_task

        if _previous_dataform_tag_task is not None:
            _previous_dataform_tag_task >> _current_dataform_tag_task

        _previous_dataform_tag_task = _current_dataform_tag_task
        task_node_8_exit = _current_dataform_tag_task

    if task_node_8_entry is None or task_node_8_exit is None:
        raise AirflowException("Unable to build Dataform tag tasks.")

    @task
    def run_node_6_single_tag(
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


    project_id_for_tags = 'os-dpf-vortex-prj-dev' or ([][0] if [] else "")
    region_for_tags = 'europe-west1'
    repository_for_tags = 'vortex_dev'
    compilation_id_for_tags = '2ceb50cd-f9a1-4a81-8e60-3a36b2b32466'
    service_account_file_for_tags = '/opt/airflow/cred.json'
    dataform_tags_list = ['DH_BOOND']

    if not dataform_tags_list:
        raise AirflowException("Dataform tags list is empty for tag-based mode.")


    def _safe_tag_task_id(tag: str) -> str:
        safe = "".join(ch if ch.isalnum() else "_" for ch in str(tag).lower())
        safe = "_".join(part for part in safe.split("_") if part)
        return f"dataform_{safe or 'tag'}"


    _previous_dataform_tag_task = None
    task_node_6_entry = None
    task_node_6_exit = None

    for _tag in dataform_tags_list:
        _current_dataform_tag_task = run_node_6_single_tag.override(task_id=_safe_tag_task_id(_tag))(
            tag=_tag,
            project_id=project_id_for_tags,
            region=region_for_tags,
            repository=repository_for_tags,
            compilation_id=compilation_id_for_tags,
            service_account_file=service_account_file_for_tags,
        )

        if task_node_6_entry is None:
            task_node_6_entry = _current_dataform_tag_task

        if _previous_dataform_tag_task is not None:
            _previous_dataform_tag_task >> _current_dataform_tag_task

        _previous_dataform_tag_task = _current_dataform_tag_task
        task_node_6_exit = _current_dataform_tag_task

    if task_node_6_entry is None or task_node_6_exit is None:
        raise AirflowException("Unable to build Dataform tag tasks.")

    @task
    def run_node_5_single_tag(
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


    project_id_for_tags = 'os-dpf-vortex-prj-dev' or ([][0] if [] else "")
    region_for_tags = 'europe-west1'
    repository_for_tags = 'vortex_dev'
    compilation_id_for_tags = '2ceb50cd-f9a1-4a81-8e60-3a36b2b32466'
    service_account_file_for_tags = '/opt/airflow/cred.json'
    dataform_tags_list = ['DW_BOOND']

    if not dataform_tags_list:
        raise AirflowException("Dataform tags list is empty for tag-based mode.")


    def _safe_tag_task_id(tag: str) -> str:
        safe = "".join(ch if ch.isalnum() else "_" for ch in str(tag).lower())
        safe = "_".join(part for part in safe.split("_") if part)
        return f"dataform_{safe or 'tag'}"


    _previous_dataform_tag_task = None
    task_node_5_entry = None
    task_node_5_exit = None

    for _tag in dataform_tags_list:
        _current_dataform_tag_task = run_node_5_single_tag.override(task_id=_safe_tag_task_id(_tag))(
            tag=_tag,
            project_id=project_id_for_tags,
            region=region_for_tags,
            repository=repository_for_tags,
            compilation_id=compilation_id_for_tags,
            service_account_file=service_account_file_for_tags,
        )

        if task_node_5_entry is None:
            task_node_5_entry = _current_dataform_tag_task

        if _previous_dataform_tag_task is not None:
            _previous_dataform_tag_task >> _current_dataform_tag_task

        _previous_dataform_tag_task = _current_dataform_tag_task
        task_node_5_exit = _current_dataform_tag_task

    if task_node_5_entry is None or task_node_5_exit is None:
        raise AirflowException("Unable to build Dataform tag tasks.")

    task_node_9 = EmptyOperator(
        task_id='parallel_join_8_node_9',
        trigger_rule='all_success',
    )

    @task(task_id='powerbi_9_node_4')
    def run_node_4():
        workspace_id = '3c1c9bef-4e81-4e61-bf0f-f0d5bc4d515a'
        dataset_id = '9a7c48a2-ac45-4850-a607-1c0e5d68664c'
        monitor_poll_interval_seconds = int(10)
        monitor_timeout_seconds = int(900)
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

        if monitor_poll_interval_seconds <= 0:
            raise AirflowException("powerbi.monitor_poll_interval_seconds must be > 0")
        if monitor_timeout_seconds <= 0:
            raise AirflowException("powerbi.monitor_timeout_seconds must be > 0")

        trigger_time = datetime.now(timezone.utc)
        refresh_resp = requests.post(
            f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes",
            headers=headers,
            timeout=60,
        )
        if refresh_resp.status_code not in (200, 202):
            raise AirflowException(
                f"Power BI refresh failed: status={refresh_resp.status_code}, body={refresh_resp.text}"
            )

        trigger_request_id = ""
        for header_name in ("x-ms-request-id", "request-id", "requestid", "activity-id", "activityid"):
            value = str(refresh_resp.headers.get(header_name, "")).strip()
            if value:
                trigger_request_id = value
                break
        if not trigger_request_id:
            location_header = str(refresh_resp.headers.get("Location", "") or refresh_resp.headers.get("location", "")).strip()
            match = re.search(r"/refreshes/([0-9a-fA-F-]{16,})", location_header)
            if match:
                trigger_request_id = match.group(1)

        print(
            f"[AIRFLOW] Power BI refresh trigger accepted "
            f"workspace_id={workspace_id}, dataset_id={dataset_id}, status={refresh_resp.status_code}, "
            f"request_id={trigger_request_id or '-'}"
        )

        refreshes_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes"
        terminal_success = {"completed", "success"}
        terminal_failed = {"failed", "cancelled", "canceled", "disabled", "timedout", "timeout", "error"}
        matched_refresh = None
        last_logged_status = None
        started = time.time()

        while True:
            if time.time() - started > monitor_timeout_seconds:
                raise AirflowException(
                    f"Power BI refresh monitoring timed out after {monitor_timeout_seconds}s "
                    f"(workspace_id={workspace_id}, dataset_id={dataset_id})."
                )

            logs_resp = requests.get(refreshes_url, headers=headers, timeout=60)
            if logs_resp.status_code != 200:
                raise AirflowException(
                    f"Power BI refresh monitor failed: status={logs_resp.status_code}, body={logs_resp.text}"
                )

            payload = logs_resp.json()
            entries = payload.get("value", []) if isinstance(payload, dict) else []
            candidates = []
            if trigger_request_id:
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    request_id = str(entry.get("requestId") or "").strip()
                    if request_id and request_id.lower() == trigger_request_id.lower():
                        start_raw = entry.get("startTime")
                        try:
                            dt = datetime.fromisoformat(str(start_raw).replace("Z", "+00:00")) if start_raw else trigger_time
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                        except Exception:  # noqa: BLE001
                            dt = trigger_time
                        candidates.append((dt, entry))

            if not candidates:
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    start_raw = entry.get("startTime")
                    if not start_raw:
                        continue
                    try:
                        dt = datetime.fromisoformat(str(start_raw).replace("Z", "+00:00"))
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                    except Exception:  # noqa: BLE001
                        continue
                    if dt >= trigger_time:
                        candidates.append((dt, entry))

            if candidates:
                candidates.sort(key=lambda row: row[0], reverse=True)
                matched_refresh = candidates[0][1]

                status = str(matched_refresh.get("status") or "").strip().lower()
                request_id = matched_refresh.get("requestId") or "-"
                refresh_type = matched_refresh.get("refreshType") or "-"
                start_time = matched_refresh.get("startTime") or "-"
                end_time = matched_refresh.get("endTime") or "-"

                if status != last_logged_status:
                    print(
                        f"[POWERBI] status={status or '-'} request_id={request_id} "
                        f"refresh_type={refresh_type} start={start_time} end={end_time}"
                    )
                    service_error = matched_refresh.get("serviceExceptionJson")
                    if service_error:
                        if isinstance(service_error, (dict, list)):
                            print("[POWERBI] service_exception=" + json.dumps(service_error, ensure_ascii=True, default=str))
                        else:
                            print(f"[POWERBI] service_exception={service_error}")
                    last_logged_status = status

                if status in terminal_success:
                    print(
                        f"[POWERBI] refresh completed successfully "
                        f"workspace_id={workspace_id}, dataset_id={dataset_id}, request_id={request_id}"
                    )
                    return {
                        "workspace_id": workspace_id,
                        "dataset_id": dataset_id,
                        "status_code": refresh_resp.status_code,
                        "refresh_status": status,
                        "request_id": request_id,
                        "start_time": start_time,
                        "end_time": end_time,
                    }

                if status in terminal_failed:
                    raise AirflowException(
                        f"Power BI refresh failed with status='{status}' "
                        f"(workspace_id={workspace_id}, dataset_id={dataset_id}, request_id={request_id})."
                    )

            else:
                if last_logged_status != "__waiting__":
                    print("[POWERBI] waiting for refresh entry to appear...")
                    last_logged_status = "__waiting__"

            time.sleep(monitor_poll_interval_seconds)


    task_node_4 = run_node_4()

    task_node_1 >> task_node_2
    task_node_3_exit >> task_node_6_entry
    task_node_6_exit >> task_node_5_entry
    task_node_2 >> task_node_7
    task_node_7 >> task_node_3_entry
    task_node_7 >> task_node_8_entry
    task_node_8_exit >> task_node_9
    task_node_5_exit >> task_node_9
    task_node_9 >> task_node_4


dag = test_vortex_web_v1()
