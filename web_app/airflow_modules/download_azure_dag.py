from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.microsoft.azure.sensors.wasb import WasbPrefixSensor
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from utils.etl_tasks import custom_failure_email


default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": custom_failure_email(["amineelkpfe@gmail.com"]),
    "on_retry_callback": custom_failure_email(["amineelkpfe@gmail.com"]),
}


with DAG(
    dag_id="wait_and_download_files_from_azure_blob",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["azure", "blob", "sensor", "download"],
    default_args=default_args,
) as dag:

    wait_for_blob_prefix = WasbPrefixSensor(
        task_id="wait_for_blob_prefix",
        wasb_conn_id="wasb_default",
        container_name=Variable.get("azure_container_name"),
        prefix=Variable.get("azure_blob_prefix"),
        poke_interval=30,
        timeout=600,
        mode="poke",
    )

    @task()
    def download_blob_files():
        container_name = Variable.get("azure_container_name")
        blob_prefix = Variable.get("azure_blob_prefix")
        blob_extension = Variable.get("azure_blob_extension", default_var=".csv")
        local_download_dir = Variable.get(
            "azure_local_download_dir",
            default_var="/opt/airflow/dags/downloads",
        )

        hook = WasbHook(wasb_conn_id="wasb_default")
        os.makedirs(local_download_dir, exist_ok=True)

        blobs = hook.get_blobs_list(
            container_name=container_name,
            prefix=blob_prefix,
        )

        matching_blobs = [
            blob_name
            for blob_name in blobs
            if blob_name.lower().endswith(blob_extension.lower())
        ]

        if not matching_blobs:
            raise AirflowException(
                f"Prefix detected, but no blobs matched extension '{blob_extension}'"
            )

        downloaded_files = []

        for blob_name in matching_blobs:
            filename = os.path.basename(blob_name.rstrip("/"))
            if not filename:
                continue

            local_file_path = os.path.join(local_download_dir, filename)

            print(f"Downloading {blob_name} -> {local_file_path}")

            hook.get_file(
                file_path=local_file_path,
                container_name=container_name,
                blob_name=blob_name,
            )

            if not os.path.exists(local_file_path):
                raise AirflowException(f"Download failed for blob: {blob_name}")

            downloaded_files.append(
                {
                    "blob_name": blob_name,
                    "local_file_path": local_file_path,
                    "size_bytes": os.path.getsize(local_file_path),
                }
            )

        print(f"Downloaded {len(downloaded_files)} file(s).")
        return downloaded_files

    wait_for_blob_prefix >> download_blob_files()