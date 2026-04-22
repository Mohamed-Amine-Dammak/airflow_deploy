from airflow.sdk import dag, task
from airflow.models import Variable
from datetime import datetime
import requests
import json
import time
import re

from google.oauth2 import service_account
from google.auth.transport.requests import Request


PROJECT_ID = Variable.get("dataform_project_id")
REGION = Variable.get("dataform_region")
REPOSITORY = Variable.get("dataform_repository")
COMPILATION_ID = Variable.get("dataform_compilation_id")
SERVICE_ACCOUNT_FILE = Variable.get("dataform_service_account_file")

# Example Airflow Variable value:
# ["STG", "DH", "DW"]
DATAFORM_TAGS = Variable.get("dataform_tags", deserialize_json=True)

BASE_URL = (
    f"https://dataform.googleapis.com/v1/projects/{PROJECT_ID}"
    f"/locations/{REGION}/repositories/{REPOSITORY}"
)

WORKFLOW_INVOCATIONS_URL = f"{BASE_URL}/workflowInvocations"


def _safe_task_id(tag: str) -> str:
    tag = tag.strip().lower()
    tag = re.sub(r"[^a-z0-9_]+", "_", tag)
    tag = re.sub(r"_+", "_", tag).strip("_")
    return f"dataform_{tag}"


def _build_headers():
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    credentials.refresh(Request())

    headers = {
        "Authorization": f"Bearer {credentials.token}",
        "Content-Type": "application/json",
    }

    with open(SERVICE_ACCOUNT_FILE) as f:
        sa_info = json.load(f)

    service_account_email = sa_info["client_email"]
    return headers, service_account_email


def _log_invocation_actions(headers: dict, invocation_name: str):
    query_url = f"https://dataform.googleapis.com/v1/{invocation_name}:query"

    resp = requests.get(query_url, headers=headers, timeout=60)
    print(f"Dataform query URL: {query_url}")
    print(f"Dataform query status: {resp.status_code}")

    if resp.status_code != 200:
        print("Could not fetch invocation action details.")
        print(resp.text)
        return

    payload = resp.json()
    actions = payload.get("workflowInvocationActions", [])

    if not actions:
        print("No workflowInvocationActions returned.")
        print(json.dumps(payload, indent=2))
        return

    print("===== DATAFORM ACTION DETAILS =====")
    for action in actions:
        target = action.get("target", {})
        state = action.get("state", "UNKNOWN")
        failure_reason = action.get("failureReason", "")

        target_str = ".".join(
            [
                part
                for part in [
                    target.get("database"),
                    target.get("schema"),
                    target.get("name"),
                ]
                if part
            ]
        )

        print(f"Action target: {target_str or '[unknown target]'}")
        print(f"Action state : {state}")

        if failure_reason:
            print(f"Failure reason: {failure_reason}")

        if action.get("invocationTiming"):
            print(f"Timing: {action['invocationTiming']}")

        print("----------------------------------")


@dag(
    dag_id="trigger_dataform_by_layer",
    start_date=datetime(2026, 2, 2),
    schedule=None,
    catchup=False,
    tags=["dataform"],
)
def trigger_dataform_by_layer():

    @task
    def invoke_single_tag(tag: str, include_dependencies: bool = True):
        headers, service_account_email = _build_headers()

        compilation_result = (
            f"projects/{PROJECT_ID}/locations/{REGION}/repositories/{REPOSITORY}"
            f"/compilationResults/{COMPILATION_ID}"
        )

        body = {
            "compilationResult": compilation_result,
            "invocationConfig": {
                "serviceAccount": service_account_email,
                "includedTags": [tag],
                "transitiveDependenciesIncluded": include_dependencies,
                "transitiveDependentsIncluded": False,
            },
        }

        print("==================================")
        print(f"Starting Dataform tag: {tag}")
        print(f"Repository         : {REPOSITORY}")
        print(f"Compilation ID     : {COMPILATION_ID}")
        print(f"Included tag       : {tag}")
        print(f"Include dependencies: {include_dependencies}")
        print("==================================")

        create_resp = requests.post(
            WORKFLOW_INVOCATIONS_URL,
            headers=headers,
            json=body,
            timeout=60,
        )

        print(f"Create invocation status: {create_resp.status_code}")
        print(create_resp.text)

        if create_resp.status_code != 200:
            raise Exception(
                f"Failed to start Dataform invocation for tag '{tag}': "
                f"{create_resp.status_code} {create_resp.text}"
            )

        invocation = create_resp.json()
        invocation_name = invocation["name"]
        print(f"Invocation created for tag '{tag}': {invocation_name}")

        # Poll until finished
        while True:
            status_resp = requests.get(
                f"https://dataform.googleapis.com/v1/{invocation_name}",
                headers=headers,
                timeout=60,
            )

            print(f"Poll status code for tag '{tag}': {status_resp.status_code}")

            if status_resp.status_code != 200:
                raise Exception(
                    f"Failed to get status for tag '{tag}': "
                    f"{status_resp.status_code} {status_resp.text}"
                )

            status_json = status_resp.json()
            state = status_json.get("state")
            print(f"Tag '{tag}' state: {state}")

            if state == "SUCCEEDED":
                print(f"Tag '{tag}' finished successfully.")
                return {
                    "tag": tag,
                    "state": state,
                    "invocation_name": invocation_name,
                }

            if state in {"FAILED", "CANCELLED", "CANCELING"}:
                print(f"Tag '{tag}' ended with state {state}")
                _log_invocation_actions(headers, invocation_name)
                raise Exception(f"Tag '{tag}' ended with state {state}")

            time.sleep(15)

    previous = None

    for tag in DATAFORM_TAGS:
        current = invoke_single_tag.override(task_id=_safe_task_id(tag))(tag=tag)

        if previous:
            previous >> current

        previous = current


dag = trigger_dataform_by_layer()