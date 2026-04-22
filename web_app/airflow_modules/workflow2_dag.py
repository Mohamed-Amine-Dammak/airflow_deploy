from datetime import datetime, timedelta

from airflow.sdk import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="master_y2_sftp_parallel_watch",
    start_date=datetime(2026, 3, 12),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["master", "y2", "sftp", "n8n", "orchestration"],
)
def master_y2_sftp_parallel_watch():

    start = EmptyOperator(task_id="start")

    # Workflow A: producer flow
    trigger_y2_module = TriggerDagRunOperator(
        task_id="trigger_y2_module",
        trigger_dag_id="cegid_y2_scheduled_tasks_dynamic",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    trigger_azure_upload = TriggerDagRunOperator(
        task_id="trigger_azure_upload",
        trigger_dag_id="upload_file_to_azure_blob",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    trigger_azure_download = TriggerDagRunOperator(
        task_id="trigger_azure_download",
        trigger_dag_id="wait_and_download_files_from_azure_blob",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    trigger_sftp_upload = TriggerDagRunOperator(
        task_id="trigger_sftp_upload",
        trigger_dag_id="simple_sftp_upload",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    # Workflow B: watcher flow
    trigger_sftp_watch_workflow = TriggerDagRunOperator(
        task_id="trigger_sftp_watch_workflow",
        trigger_dag_id="sftp_watch_and_trigger_n8n",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    start >> [trigger_y2_module, trigger_sftp_watch_workflow]

    trigger_y2_module >> trigger_azure_upload >> trigger_azure_download >> trigger_sftp_upload


master_y2_sftp_parallel_watch()