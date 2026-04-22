from datetime import datetime, timedelta
import json
import requests

from airflow.sdk import dag, task
from airflow.exceptions import AirflowException
from airflow.models import Variable

from google.oauth2 import service_account
from google.auth.transport.requests import Request


default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="cloudrun_trigger_template",
    start_date=datetime(2026, 2, 2),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["cloudrun", "template", "trigger"],
)
def cloudrun_trigger_template():

    @task()
    def trigger_cloud_run():
        # Airflow Variables
        cloud_run_url = Variable.get("cloudrun_url")
        service_account_file = Variable.get("cloudrun_service_account_file")
        payload_raw = Variable.get("cloudrun_payload")

        try:
            payload = json.loads(payload_raw)
        except json.JSONDecodeError as e:
            raise AirflowException(
                f"Invalid JSON in Airflow Variable 'cloudrun_payload': {e}"
            )

        # Create ID token credentials
        credentials = service_account.IDTokenCredentials.from_service_account_file(
            service_account_file,
            target_audience=cloud_run_url,
        )

        # Refresh to get token
        credentials.refresh(Request())

        headers = {
            "Authorization": f"Bearer {credentials.token}",
            "Content-Type": "application/json",
            "User-Agent": "Airflow",
        }

        response = requests.post(
            cloud_run_url,
            headers=headers,
            data=json.dumps(payload),
            timeout=300,
        )

        if response.status_code != 200:
            raise AirflowException(
                f"Cloud Run call failed. "
                f"status={response.status_code}, body={response.text}"
            )

        return {
            "cloud_run_url": cloud_run_url,
            "status_code": response.status_code,
            "response_text": response.text,
        }

    trigger_cloud_run()


cloudrun_trigger_template()