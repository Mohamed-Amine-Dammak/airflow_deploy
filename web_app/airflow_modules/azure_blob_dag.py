from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator
from utils.etl_tasks import custom_failure_email


default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": custom_failure_email(["amineelkpfe@gmail.com"]),
    "on_retry_callback": custom_failure_email(["amineelkpfe@gmail.com"]),
}


@dag(
    dag_id="upload_file_to_azure_blob",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["azure", "blob", "upload"],
    default_args=default_args,
)
def upload_file_to_azure_blob_dag():

    @task
    def upload_to_blob():
        LocalFilesystemToWasbOperator(
            task_id="upload_to_blob_inner",
            wasb_conn_id="wasb_default",
            file_path="/mnt/cegid/CUSTOMER_CUS_TEST.csv",
            container_name="my-container",
            blob_name="uploads/CUSTOMER_CUS_TEST.csv",
            create_container=True,
        ).execute(context={})

    upload_to_blob()


dag = upload_file_to_azure_blob_dag()