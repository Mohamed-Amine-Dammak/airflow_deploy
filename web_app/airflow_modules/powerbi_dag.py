from datetime import datetime, timedelta

import requests
from airflow.sdk import dag, task
from airflow.exceptions import AirflowException
from airflow.sdk.bases.hook import BaseHook
from airflow.models import Variable


default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def get_powerbi_token(conn_id: str = "powerbi_api") -> str:
    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson

    tenant_id = extra["tenant_id"]
    client_id = extra["client_id"]
    client_secret = extra["client_secret"]

    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

    response = requests.post(
        url,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://analysis.windows.net/powerbi/api/.default",
        },
        timeout=60,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def get_headers(conn_id: str = "powerbi_api") -> dict:
    token = get_powerbi_token(conn_id=conn_id)
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def check_dataset_exists(
    workspace_id: str,
    dataset_id: str,
    conn_id: str = "powerbi_api",
) -> bool:
    headers = get_headers(conn_id=conn_id)

    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"
    response = requests.get(url, headers=headers, timeout=60)

    if response.status_code != 200:
        raise AirflowException(
            f"Failed to list datasets. "
            f"status={response.status_code}, body={response.text}, workspace_id={workspace_id}"
        )

    datasets = response.json().get("value", [])
    dataset_ids = [d.get("id") for d in datasets]

    print("Datasets visible in workspace:")
    for d in datasets:
        print({"id": d.get("id"), "name": d.get("name")})

    if dataset_id not in dataset_ids:
        raise AirflowException(
            f"Dataset not found in workspace listing. "
            f"workspace_id={workspace_id}, dataset_id={dataset_id}, visible_dataset_ids={dataset_ids}"
        )

    return True


def trigger_powerbi_refresh(
    workspace_id: str,
    dataset_id: str,
    conn_id: str = "powerbi_api",
) -> dict:
    check_dataset_exists(
        workspace_id=workspace_id,
        dataset_id=dataset_id,
        conn_id=conn_id,
    )

    headers = get_headers(conn_id=conn_id)
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes"

    response = requests.post(
        url,
        headers=headers,
        timeout=60,
    )

    if response.status_code not in (200, 202):
        raise AirflowException(
            f"Power BI refresh failed. "
            f"status={response.status_code}, "
            f"body={response.text}, "
            f"workspace_id={workspace_id}, "
            f"dataset_id={dataset_id}"
        )

    return {
        "status_code": response.status_code,
        "body": response.text,
        "request_id": response.headers.get("x-ms-request-id"),
        "location": response.headers.get("Location"),
        "workspace_id": workspace_id,
        "dataset_id": dataset_id,
    }


@dag(
    dag_id="powerbi_dataset_refresh",
    start_date=datetime(2026, 2, 2),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["powerbi", "template", "refresh"],
)
def powerbi_dataset_refresh_template():
    @task()
    def refresh_powerbi_dataset(
        workspace_id: str,
        dataset_id: str,
        conn_id: str = "powerbi_api",
    ):
        return trigger_powerbi_refresh(
            workspace_id=workspace_id,
            dataset_id=dataset_id,
            conn_id=conn_id,
        )

    refresh_powerbi_dataset(
        workspace_id=Variable.get("powerbi_workspaceID"),
        dataset_id=Variable.get("powerbi_datasetID"),
        conn_id="powerbi_api",
    )


powerbi_dataset_refresh_template()