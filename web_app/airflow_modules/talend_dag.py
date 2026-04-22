from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import requests
import time
from utils.etl_tasks import custom_failure_email

# Default args for retries, emails, etc.
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": custom_failure_email(["amineelkpfe@gmail.com"]),
    "on_retry_callback": custom_failure_email(["amineelkpfe@gmail.com"]),
}


@dag(
    dag_id="talend_cloud_job_dynamic",
    start_date=datetime(2026, 2, 2),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["talend", "cloud", "etl"],
)
def talend_cloud_job_dag():

    def _pause(label: str, seconds: int = 10):
        print(f"{label} -> sleeping for {seconds} seconds...")
        time.sleep(seconds)
        print(f"{label} -> resumed.")

    @task()
    def get_executable_id(job_name: str, workspace_id: str | None = None, sleep_after: int = 10) -> str:
        conn = BaseHook.get_connection("talend_cloud")
        base_url = conn.host.rstrip("/")

        headers = {
            "Authorization": f"Bearer {conn.password}",
            "Content-Type": "application/json",
        }

        url = f"{base_url}/orchestration/executables/tasks"
        params = {
            "limit": 100,
            "offset": 0,
        }

        params["name"] = job_name

        if workspace_id:
            params["workspaceId"] = workspace_id

        response = requests.get(url, headers=headers, params=params, timeout=60)

        if response.status_code != 200:
            raise AirflowException(
                f"Failed to fetch Talend tasks: {response.status_code} - {response.text}"
            )

        payload = response.json()
        items = payload.get("items", []) if isinstance(payload, dict) else []

        exact_matches = [
            item for item in items
            if str(item.get("name", "")).strip() == str(job_name).strip()
        ]

        if not exact_matches:
            raise AirflowException(f"No exact task match found for job_name='{job_name}'")

        if len(exact_matches) > 1:
            raise AirflowException(
                f"Multiple exact task matches found for job_name='{job_name}': "
                f"{[i.get('id') for i in exact_matches]}"
            )

        executable_id = exact_matches[0].get("executable")
        if not executable_id:
            raise AirflowException(
                f"Task found but no executable returned: {exact_matches[0]}"
            )

        return executable_id

    @task()
    def trigger_job(executable_id: str, sleep_after: int = 10) -> str:
        """
        Trigger a Talend Cloud job and return the execution ID dynamically.
        """
        conn = BaseHook.get_connection("talend_cloud")
        base_url = conn.host.rstrip("/")

        headers = {
            "Authorization": f"Bearer {conn.password}",
            "Content-Type": "application/json",
        }
        payload = {"executable": executable_id}

        url = f"{base_url}/processing/executions"
        print(f"DEBUG trigger URL = {url}")
        print(f"DEBUG trigger payload = {payload}")

        response = requests.post(url, headers=headers, json=payload, timeout=60)

        if response.status_code not in (200, 201):
            raise AirflowException(f"Failed to trigger Talend job: {response.text}")

        body = response.json()
        execution_id = body.get("executionId") or body.get("id")

        if not execution_id:
            raise AirflowException(
                f"Talend trigger succeeded but no execution ID was returned: {body}"
            )

        print(f"Talend job triggered. Execution ID: {execution_id}")

        _pause(f"After trigger_job [execution_id={execution_id}]", sleep_after)

        return execution_id

    @task()
    def monitor_job(
        execution_id: str,
        poll_interval: int = 10,
        timeout: int = 50,
        sleep_after: int = 10,
    ):
        """
        Monitor Talend Cloud job execution logs AND fetch component metrics dynamically
        using the execution ID.
        """
        conn = BaseHook.get_connection("talend_cloud")
        base_url = conn.host.rstrip("/")
        headers = {"Authorization": f"Bearer {conn.password}"}

        logs_url = f"{base_url}/monitoring/executions/{execution_id}/logs"
        metrics_url = f"{base_url}/monitoring/observability/executions/{execution_id}/component"

        print(f"DEBUG logs URL = {logs_url}")
        print(f"DEBUG metrics URL = {metrics_url}")

        start_time = time.time()
        all_logs = []
        all_metrics = []

        while True:
            # FETCH LOGS
            log_resp = requests.get(
                logs_url,
                headers=headers,
                params={"count": 50, "order": "DESC"},
                timeout=60,
            )
            if log_resp.status_code != 200:
                raise AirflowException(f"Failed fetching logs: {log_resp.text}")

            logs = log_resp.json().get("data", [])
            if logs:
                all_logs.extend(logs)
                print("\n[LOGS] Latest entries:")
                for log in logs[-30:]:
                    ts = log.get("logTimestamp")
                    sev = log.get("severity")
                    msg = log.get("logMessage")
                    print(f"[{ts}] {sev}: {msg}")

            # FETCH METRICS
            metrics_resp = requests.get(
                metrics_url,
                headers=headers,
                params={"limit": 200, "offset": 0},
                timeout=60,
            )
            if metrics_resp.status_code == 200:
                metrics_data = metrics_resp.json()
                items = metrics_data.get("metrics", {}).get("items", [])
                if items:
                    all_metrics.extend(items)
                    print("\n[METRICS] Component-level Observability:")
                    for item in items:
                        print(
                            f"component={item.get('connector_label', 'N/A')} | "
                            f"duration_ms={item.get('component_execution_duration_milliseconds', 'N/A')} | "
                            f"rows_processed={item.get('component_connection_rows_total', 'N/A')}"
                        )
            else:
                print(f"Failed fetching metrics: {metrics_resp.text}")

            if time.time() - start_time > timeout:
                print("Timeout reached, stopping monitoring.")
                break

            time.sleep(poll_interval)

        _pause(f"After monitor_job [execution_id={execution_id}]", sleep_after)

        return {"logs": all_logs, "metrics": all_metrics}

    # JOB 1 by name
    executable_id_1 = get_executable_id(
        "J_Test_Airflow_v0",
        sleep_after=10,
    )
    execution_id_1 = trigger_job(
        executable_id_1,
        sleep_after=10,
    )
    monitor_1 = monitor_job(
        execution_id_1,
        sleep_after=5,
    )

    # JOB 2 by name
    executable_id_2 = get_executable_id(
        "J_Test_Airflow_v1",
        sleep_after=10,
    )
    execution_id_2 = trigger_job(
        executable_id_2,
        sleep_after=10,
    )
    monitor_2 = monitor_job(
        execution_id_2,
        sleep_after=5,
    )

    # Dependency management
    monitor_1 >> execution_id_2


dag = talend_cloud_job_dag()