from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.sftp.sensors.sftp import SFTPSensor


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="sftp_prefix_sensor",
    start_date=datetime(2026, 3, 12),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["sftp", "sensor"],
) as dag:

    remote_dir = Variable.get("sftp_remote_dir", default_var="/uploads")
    prefix = Variable.get("sftp_prefix_sensor")

    wait_for_prefix_file = SFTPSensor(
        task_id="wait_for_prefix_file",
        sftp_conn_id="sftp",   
        path=remote_dir,
        file_pattern=f"{prefix}*",
        poke_interval=30,
        timeout=600,
        mode="poke",
    )