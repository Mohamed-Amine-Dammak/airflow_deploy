from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta, timezone
import requests
import time
from airflow.models import Variable
# Optional: keep your callback if you already have it
from utils.etl_tasks import custom_failure_email


# =========================
# DEFAULT ARGS
# =========================
default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    # "on_failure_callback": custom_failure_email(["you@example.com"]),
    # "on_retry_callback": custom_failure_email(["you@example.com"]),
}


@dag(
    dag_id="cegid_y2_scheduled_tasks_dynamic",
    start_date=datetime(2026, 3, 12),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["cegid", "y2", "scheduled-tasks", "etl"],
)
def cegid_y2_scheduled_tasks_dag():
    """
    Airflow DAG to:
      1) optionally enable one or more Cegid Y2 scheduled tasks
      2) run them immediately
      3) monitor them by polling GET /scheduled-tasks/v1/{id}

    Assumptions:
      - Airflow connection id: cegid_y2 (It can be configured in Airflow UI in Connections)
      - Connection host example:
            https://90039148-partner-retail-ondemand.cegid.cloud/Y2 (Airflow UI in Connections)
      - Connection login: olivesoft
      - Connection password: your_password
      - folder_id is passed as a DAG variable below

    """

    def _pause(label: str, seconds: int = 5):
        print(f"{label} -> sleeping for {seconds} seconds...")
        time.sleep(seconds)
        print(f"{label} -> resumed.")

    def _get_client():
        """
        Builds a requests session from Airflow connection 'cegid_y2'.
        Expected:
          conn.host     = https://90039148-partner-retail-ondemand.cegid.cloud/Y2
          conn.login    = olivesoft
          conn.password = your_password
        """
        conn = BaseHook.get_connection("cegid_y2")
        base_host = conn.host.rstrip("/")
        username = conn.login
        password = conn.password or ""

        if not base_host:
            raise AirflowException("Airflow connection 'cegid_y2' must define host")

        if not username:
            raise AirflowException("Airflow connection 'cegid_y2' must define login")

        session = requests.Session()
        session.headers.update({"Accept": "application/json"})

        return {
            "session": session,
            "base_host": base_host,
            "username": username,
            "password": password,
        }

    def _build_auth(folder_id: str, username: str, password: str):
        from requests.auth import HTTPBasicAuth
        return HTTPBasicAuth(f"{folder_id}\\{username}", password)

    def _task_url(base_host: str, folder_id: str) -> str:
        return f"{base_host}/{folder_id}/api/scheduled-tasks/v1"

    @task()
    def list_tasks(folder_id: str, page_index: int = 1, page_size: int = 20):
        """
        Optional helper to verify connectivity and inspect available tasks.
        """
        client = _get_client()
        auth = _build_auth(folder_id, client["username"], client["password"])
        url = _task_url(client["base_host"], folder_id)

        response = client["session"].get(
            url,
            auth=auth,
            params={
                "paging.pageIndex": page_index,
                "paging.pageSize": page_size,
            },
            timeout=60,
        )

        print(f"LIST URL = {response.url}")
        print(f"LIST STATUS = {response.status_code}")
        print(f"LIST CONTENT-RANGE = {response.headers.get('content-range')}")
        print(f"LIST BODY = {response.text[:2000]}")

        if response.status_code not in (200, 206):
            raise AirflowException(
                f"Failed to list scheduled tasks: {response.status_code} - {response.text}"
            )

        return response.json()

    @task()
    def get_task(folder_id: str, task_id: int, sleep_after: int = 3):
        """
        Returns one scheduled task with optional Execution details.
        """
        client = _get_client()
        auth = _build_auth(folder_id, client["username"], client["password"])
        url = f"{_task_url(client['base_host'], folder_id)}/{task_id}"

        # Ask for Execution details when supported by the API.
        response = client["session"].get(
            url,
            auth=auth,
            params=[("fields", "Execution")],
            timeout=60,
        )

        print(f"GET TASK URL = {response.url}")
        print(f"GET TASK STATUS = {response.status_code}")
        print(f"GET TASK BODY = {response.text}")

        if response.status_code != 200:
            raise AirflowException(
                f"Failed to fetch scheduled task {task_id}: "
                f"{response.status_code} - {response.text}"
            )

        _pause(f"After get_task [task_id={task_id}]", sleep_after)
        return response.json()

    @task()
    def enable_task(folder_id: str, task_id: int, sleep_after: int = 3):
        """
        Activates a scheduled task.
        Successful responses can be:
          - Success
          - AlreadyProcessed  -> already enabled, still OK
        """
        client = _get_client()
        auth = _build_auth(folder_id, client["username"], client["password"])
        url = f"{_task_url(client['base_host'], folder_id)}/enable"

        response = client["session"].post(
            url,
            auth=auth,
            params=[("ids", task_id)],
            timeout=60,
        )

        print(f"ENABLE URL = {response.url}")
        print(f"ENABLE STATUS = {response.status_code}")
        print(f"ENABLE BODY = {response.text}")

        if response.status_code != 200:
            raise AirflowException(
                f"Failed to enable scheduled task {task_id}: "
                f"{response.status_code} - {response.text}"
            )

        body = response.json()
        if not isinstance(body, list) or not body:
            raise AirflowException(
                f"Enable returned unexpected payload for task {task_id}: {body}"
            )

        result = body[0]
        status = result.get("status")

        if status not in ("Success", "AlreadyProcessed"):
            raise AirflowException(
                f"Enable failed for task {task_id}: {result}"
            )

        _pause(f"After enable_task [task_id={task_id}]", sleep_after)
        return result

    @task()
    def run_task(folder_id: str, task_id: int, sleep_after: int = 3):
        """
        Immediately executes a scheduled task.
        Successful responses can be:
          - Success
          - AlreadyProcessed
        """
        client = _get_client()
        auth = _build_auth(folder_id, client["username"], client["password"])
        url = f"{_task_url(client['base_host'], folder_id)}/run"

        trigger_time_utc = datetime.now(timezone.utc).isoformat()

        response = client["session"].post(
            url,
            auth=auth,
            params=[("ids", task_id)],
            timeout=60,
        )

        print(f"RUN URL = {response.url}")
        print(f"RUN STATUS = {response.status_code}")
        print(f"RUN BODY = {response.text}")

        if response.status_code != 200:
            raise AirflowException(
                f"Failed to run scheduled task {task_id}: "
                f"{response.status_code} - {response.text}"
            )

        body = response.json()
        if not isinstance(body, list) or not body:
            raise AirflowException(
                f"Run returned unexpected payload for task {task_id}: {body}"
            )

        result = body[0]
        status = result.get("status")

        if status not in ("Success", "AlreadyProcessed"):
            raise AirflowException(
                f"Run failed for task {task_id}: {result}"
            )

        _pause(f"After run_task [task_id={task_id}]", sleep_after)

        return {
            "task_id": task_id,
            "trigger_time_utc": trigger_time_utc,
            "run_response": result,
        }

    @task()
    def monitor_task(
        folder_id: str,
        task_id: int,
        trigger_meta: dict,
        poll_interval: int = 15,
        timeout: int = 600,
        sleep_after: int = 3,
    ):
        """
        Polls GET /{id}?fields=Execution until the task appears stable.

        Since the API examples you shared do not expose dedicated execution logs,
        this monitoring strategy checks:
          - inProgress
          - status
          - execution.lastExecution.date (if returned)
        """
        client = _get_client()
        auth = _build_auth(folder_id, client["username"], client["password"])
        url = f"{_task_url(client['base_host'], folder_id)}/{task_id}"

        started_at = time.time()
        previous_last_execution = None
        seen_progress = False

        while True:
            response = client["session"].get(
                url,
                auth=auth,
                params=[("fields", "Execution")],
                timeout=60,
            )

            print(f"MONITOR URL = {response.url}")
            print(f"MONITOR STATUS = {response.status_code}")

            if response.status_code != 200:
                raise AirflowException(
                    f"Failed monitoring task {task_id}: "
                    f"{response.status_code} - {response.text}"
                )

            body = response.json()
            status = body.get("status")
            in_progress = body.get("inProgress")
            active = body.get("active")
            execution = body.get("execution") or {}
            last_execution = (execution.get("lastExecution") or {}).get("date")
            next_execution = execution.get("nextExecutionDate")

            print(
                f"[MONITOR] task_id={task_id} | "
                f"status={status} | inProgress={in_progress} | active={active} | "
                f"lastExecution={last_execution} | nextExecution={next_execution}"
            )

            if in_progress:
                seen_progress = True

            # If we see a change in lastExecution, we consider the immediate run observed.
            if previous_last_execution and last_execution and last_execution != previous_last_execution:
                print(
                    f"[MONITOR] task_id={task_id} lastExecution changed: "
                    f"{previous_last_execution} -> {last_execution}"
                )
                break

            # If execution timestamp is available and progress is done, accept completion.
            if last_execution and not in_progress and status in ("Success", "None", "Waiting", "Error"):
                # We cannot prove full job logs from this API, but this is the best observable state.
                if seen_progress or status in ("Success", "None"):
                    print(f"[MONITOR] task_id={task_id} reached stable state.")
                    break

            previous_last_execution = last_execution

            if time.time() - started_at > timeout:
                raise AirflowException(
                    f"Timeout while monitoring scheduled task {task_id}"
                )

            time.sleep(poll_interval)

        _pause(f"After monitor_task [task_id={task_id}]", sleep_after)

        return {
            "task_id": task_id,
            "final_status": status,
            "inProgress": in_progress,
            "active": active,
            "lastExecution": last_execution,
            "nextExecutionDate": next_execution,
            "trigger_meta": trigger_meta,
        }

    # =========================
    # VARIABLES TO EDIT
    # =========================

    folder_id_value = Variable.get("y2_folder_id_value")
    task_ids_value = Variable.get("y2_task_ids_value", deserialize_json=True)

    # =========================
    # OPTIONAL CONNECTIVITY CHECK
    # =========================
    listed = list_tasks(
        folder_id=folder_id_value,
        page_index=1,
        page_size=20,
    )

    # =========================
    # ONE / MULTIPLE TASKS
    # =========================
    previous_monitor = listed

    for current_task_id in task_ids_value:
        task_details = get_task(
            folder_id=folder_id_value,
            task_id=current_task_id,
            sleep_after=5,
        )

        enabled = enable_task(
            folder_id=folder_id_value,
            task_id=current_task_id,
            sleep_after=5,
        )

        triggered = run_task(
            folder_id=folder_id_value,
            task_id=current_task_id,
            sleep_after=5,
        )

        monitored = monitor_task(
            folder_id=folder_id_value,
            task_id=current_task_id,
            trigger_meta=triggered,
            poll_interval=15,
            timeout=600,
            sleep_after=5,
        )

        previous_monitor >> task_details
        task_details >> enabled >> triggered >> monitored
        previous_monitor = monitored


dag = cegid_y2_scheduled_tasks_dag()