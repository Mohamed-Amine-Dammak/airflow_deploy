from datetime import datetime
import time

from airflow.sdk import dag, task


@dag(
    dag_id="hello_world_eval_v0",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "env:eval", "candidate:v0"],
)
def hello_world_eval_v0():
    @task
    def say_hello():
        time.sleep(8)  # slower baseline
        print("Hello from v0")

    say_hello()


hello_world_eval_v0()
