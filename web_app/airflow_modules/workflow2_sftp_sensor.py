from datetime import datetime, timedelta

from airflow.sdk import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="sftp_watch_and_trigger_n8n",
    start_date=datetime(2026, 3, 12),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["watcher", "sftp", "n8n", "orchestration"],
)
def sftp_watch_and_trigger_n8n():

    # 1) Wait for file on SFTP
    trigger_sftp_sensor = TriggerDagRunOperator(
        task_id="trigger_sftp_sensor",
        trigger_dag_id="sftp_prefix_sensor",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    # 2) Trigger n8n workflow after file is detected
    trigger_n8n = TriggerDagRunOperator(
        task_id="trigger_n8n_workflow",
        trigger_dag_id="n8n_workflow_template",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    trigger_sftp_sensor >> trigger_n8n


sftp_watch_and_trigger_n8n()