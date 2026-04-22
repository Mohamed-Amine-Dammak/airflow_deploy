from datetime import datetime, timedelta

from airflow.sdk import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="master_boond_full_pipeline",
    start_date=datetime(2026, 2, 2),
    schedule="20 9 * * *",   # or whatever schedule you want for the full pipeline
    catchup=False,
    default_args=default_args,
    tags=["master", "boond", "orchestration"],
)
def master_boond_full_pipeline():

    # 1) Ingestion via n8n
    trigger_n8n = TriggerDagRunOperator(
        task_id="trigger_n8n_ingestion",
        trigger_dag_id="n8n_workflow_template",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    # 2) Processing via Cloud Run (CSV processing, cleaning, validation -> raw tables)
    trigger_cloudrun = TriggerDagRunOperator(
        task_id="trigger_cloudrun_processing",
        trigger_dag_id="cloudrun_trigger_template",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    # 3) Transformation via Dataform
    trigger_dataform = TriggerDagRunOperator(
        task_id="trigger_dataform_transformation",
        trigger_dag_id="trigger_dataform_template",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    # 4) Analytics refresh via Power BI
    trigger_powerbi = TriggerDagRunOperator(
        task_id="trigger_powerbi_refresh",
        trigger_dag_id="powerbi_dataset_refresh",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    trigger_n8n >> trigger_cloudrun >> trigger_dataform >> trigger_powerbi


master_boond_full_pipeline()