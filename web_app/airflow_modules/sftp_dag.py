from airflow.sdk import dag, task
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.providers.sftp.hooks.sftp import SFTPHook

from datetime import datetime, timedelta
import os


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="simple_sftp_upload",
    start_date=datetime(2026, 3, 12),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["sftp", "upload"],
)
def simple_sftp_upload_dag():

    @task()
    def upload_file():
        local_path = Variable.get("sftp_local_file_path")  # e.g. /mnt/cegid/file.csv
        remote_dir = Variable.get("sftp_remote_dir", default_var="/uploads")

        if not os.path.exists(local_path):
            raise AirflowException(f"Local file not found: {local_path}")

        filename = os.path.basename(local_path)
        remote_path = f"{remote_dir.rstrip('/')}/{filename}"

        sftp_hook = SFTPHook(ssh_conn_id="sftp")

        print(f"Uploading {local_path} -> {remote_path}")
        sftp_hook.store_file(
            remote_full_path=remote_path,
            local_full_path=local_path,
        )

        return {
            "uploaded": True,
            "local_path": local_path,
            "remote_path": remote_path,
        }

    upload_file()


dag = simple_sftp_upload_dag()