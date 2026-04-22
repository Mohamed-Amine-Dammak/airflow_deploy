from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.providers.sftp.hooks.sftp import SFTPHook

from datetime import datetime, timedelta, timezone
from pathlib import Path
import requests
import time
import fnmatch
import os


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="cegid_y2_export_to_sftp",
    start_date=datetime(2026, 3, 12),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["cegid", "y2", "export", "sftp"],
)
def cegid_y2_export_to_sftp_dag():
    """
    Flow:
      1) trigger Y2 scheduled export task
      2) monitor Y2 task until stable
      3) wait for a new file in local export folder
      4) upload that file to SFTP

    Requirements:
      - Airflow connection: cegid_y2
      - Airflow connection: cegid_sftp
      - Airflow Variables:
          y2_folder_id_value
          y2_export_task_id
          y2_export_local_dir (optional, default C:\\ProgramData\\Cegid)
          y2_export_file_glob (optional, default *)
          sftp_remote_dir (optional, default /incoming)

    Important:
      The Airflow worker must be able to access the local export folder.
      If Airflow runs on another machine/container, C:\\ProgramData\\Cegid
      will not be visible unless shared/mounted.
    """

    def _pause(label: str, seconds: int = 5):
        print(f"{label} -> sleeping for {seconds} seconds...")
        time.sleep(seconds)
        print(f"{label} -> resumed.")

    def _get_y2_client():
        conn = BaseHook.get_connection("cegid_y2")
        base_host = (conn.host or "").rstrip("/")
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
    def get_task(folder_id: str, task_id: int):
        client = _get_y2_client()
        auth = _build_auth(folder_id, client["username"], client["password"])
        url = f"{_task_url(client['base_host'], folder_id)}/{task_id}"

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

        return response.json()

    @task()
    def enable_task(folder_id: str, task_id: int, sleep_after: int = 3):
        client = _get_y2_client()
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
            raise AirflowException(f"Enable failed for task {task_id}: {result}")

        _pause(f"After enable_task [task_id={task_id}]", sleep_after)
        return result

    @task()
    def run_task(folder_id: str, task_id: int, sleep_after: int = 3):
        client = _get_y2_client()
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
            raise AirflowException(f"Run failed for task {task_id}: {result}")

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
        client = _get_y2_client()
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

            if previous_last_execution and last_execution and last_execution != previous_last_execution:
                print(
                    f"[MONITOR] task_id={task_id} lastExecution changed: "
                    f"{previous_last_execution} -> {last_execution}"
                )
                break

            if last_execution and not in_progress and status in ("Success", "None", "Waiting", "Error"):
                if seen_progress or status in ("Success", "None"):
                    print(f"[MONITOR] task_id={task_id} reached stable state.")
                    break

            previous_last_execution = last_execution

            if time.time() - started_at > timeout:
                raise AirflowException(f"Timeout while monitoring scheduled task {task_id}")

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

    @task()
    def snapshot_existing_files(local_dir: str, file_glob: str = "*"):
        base = Path(local_dir)
        if not base.exists():
            raise AirflowException(f"Local export directory does not exist: {local_dir}")

        snapshot = {}

        for entry in base.iterdir():
            if entry.is_file() and fnmatch.fnmatch(entry.name, file_glob):
                snapshot[str(entry.resolve())] = {
                    "mtime": entry.stat().st_mtime,
                    "size": entry.stat().st_size,
                }

        print(f"Snapshot contains {len(snapshot)} file(s)")
        for path, meta in snapshot.items():
            print(f"BEFORE: {path} | mtime={meta['mtime']} | size={meta['size']}")

        return snapshot


    @task()
    def wait_for_new_or_updated_export_file(
        local_dir: str,
        existing_files: dict,
        file_glob: str = "*.csv",
        poke_interval: int = 10,
        timeout: int = 300,
        min_age_seconds: int = 5,
    ):
        base = Path(local_dir)
        if not base.exists():
            raise AirflowException(f"Local export directory does not exist: {local_dir}")

        existing_files = existing_files or {}
        started_at = time.time()

        while True:
            candidates = []

            for entry in base.iterdir():
                if not entry.is_file():
                    continue
                if not fnmatch.fnmatch(entry.name, file_glob):
                    continue

                full_path = str(entry.resolve())
                stat = entry.stat()
                current_mtime = stat.st_mtime
                current_size = stat.st_size

                previous = existing_files.get(full_path)
                is_new = previous is None
                is_updated = previous is not None and current_mtime > previous.get("mtime", 0)

                print(
                    f"SEEN: {entry.name} | mtime={current_mtime} | size={current_size} | "
                    f"is_new={is_new} | is_updated={is_updated}"
                )

                if is_new or is_updated:
                    candidates.append(entry)

            if candidates:
                candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
                candidate = candidates[0]
                age = time.time() - candidate.stat().st_mtime

                if age >= min_age_seconds:
                    result = {
                        "local_path": str(candidate.resolve()),
                        "filename": candidate.name,
                        "size_bytes": candidate.stat().st_size,
                        "modified_time_epoch": candidate.stat().st_mtime,
                    }
                    print(f"Selected file: {result}")
                    return result

            if time.time() - started_at > timeout:
                raise AirflowException(
                    f"No new or updated file matching '{file_glob}' found in {local_dir} within {timeout} seconds"
                )

            print(f"Waiting for new or updated file in {local_dir} matching {file_glob}...")
            time.sleep(poke_interval)

    @task()
    def upload_file_to_sftp(file_info: dict, remote_dir: str):
        local_path = file_info["local_path"]
        filename = file_info["filename"]

        if not os.path.exists(local_path):
            raise AirflowException(f"Local file not found: {local_path}")

        sftp_hook = SFTPHook(ssh_conn_id="sftp")

        remote_dir_clean = remote_dir.rstrip("/")
        remote_path = f"{remote_dir_clean}/{filename}"

        with sftp_hook.get_conn() as sftp:
            try:
                sftp.chdir(remote_dir_clean)
            except Exception:
                raise AirflowException(
                    f"Remote SFTP directory does not exist or is not accessible: {remote_dir_clean}"
                )

        print(f"Uploading {local_path} -> {remote_path}")
        sftp_hook.store_file(remote_full_path=remote_path, local_full_path=local_path)

        return {
            "uploaded": True,
            "local_path": local_path,
            "remote_path": remote_path,
            "filename": filename,
        }

    
    export_task_ids_value = int(Variable.get("y2_task_ids_value", deserialize_json=True)[0])
    folder_id_value = Variable.get("y2_folder_id_value")
    local_dir_value = Variable.get("y2_export_local_dir", default_var=r"/mnt/cegid")
    file_glob_value = Variable.get("y2_export_file_glob", default_var="*")
    sftp_remote_dir_value = Variable.get("sftp_remote_dir", default_var="/incoming")

    before_files = snapshot_existing_files(
        local_dir=local_dir_value,
        file_glob=file_glob_value,
    )

    task_details = get_task(
        folder_id=folder_id_value,
        task_id=export_task_ids_value,
    )

    enabled = enable_task(
        folder_id=folder_id_value,
        task_id=export_task_ids_value,
    )

    triggered = run_task(
        folder_id=folder_id_value,
        task_id=export_task_ids_value,
    )

    monitored = monitor_task(
        folder_id=folder_id_value,
        task_id=export_task_ids_value,
        trigger_meta=triggered,
        poll_interval=15,
        timeout=600,
    )

    exported_file = wait_for_new_or_updated_export_file(
        local_dir=local_dir_value,
        existing_files=before_files,
        file_glob=file_glob_value,
        poke_interval=10,
        timeout=300,
        min_age_seconds=5,
    )

    uploaded = upload_file_to_sftp(
        file_info=exported_file,
        remote_dir=sftp_remote_dir_value,
    )

    before_files >> task_details >> enabled >> triggered >> monitored >> exported_file >> uploaded


dag = cegid_y2_export_to_sftp_dag()