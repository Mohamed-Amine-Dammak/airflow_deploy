from datetime import datetime
import time

from airflow.sdk import dag, task


@dag(
    dag_id="hello_world_eval_v1",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "env:eval", "candidate:v1"],
)
def hello_world_eval_v1():
    @task
    def say_hello():
        time.sleep(2)  # faster candidate
        print("Hello from v1")

    say_hello()


hello_world_eval_v1()
