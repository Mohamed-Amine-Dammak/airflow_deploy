from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import json
from google.oauth2 import service_account
from google.auth.transport.requests import Request

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

CLOUD_RUN_URL = "https://ingestioncloudrun-72169857028.europe-west1.run.app/primary_keys"
SERVICE_ACCOUNT_FILE = "/opt/airflow/os-dpf-ariflow-prj-dev-3b50cbb16478.json"

@dag(
    dag_id="cloudrun_ingestion_boond",
    start_date=datetime(2026, 2, 2),
    schedule="20 9 * * *",   # 9:20 AM
    catchup=False,
    default_args=default_args,
    tags=["cloudrun", "boond", "ingestion"],
)
def cloudrun_ingestion_boond():

    @task()
    def trigger_cloud_run():

        # Create ID token credentials
        credentials = service_account.IDTokenCredentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            target_audience=CLOUD_RUN_URL,
        )

        # Refresh to get token
        credentials.refresh(Request())

        headers = {
            "Authorization": f"Bearer {credentials.token}",
            "Content-Type": "application/json",
            "User-Agent": "Airflow"
        }

        payload = {
            "dataset": "raw",
            "bucket_name": "os-dpf-ariflow-prj-dev",
            "integration_source": "n8n",
            "data_source": "boond",
            "tables": {
                "ABSENCES": {"separator": ";", "mode": "full"},
                "ADMINISTRATIVE": {"separator": ";", "mode": "delta"},
                "AGENCIES": {"separator": ";", "mode": "full"},
                "COMPANIES": {"separator": ";", "mode": "full"},
                "CONTACTS": {"separator": ";", "mode": "full"},
                "DAYS_OFF": {"separator": ";", "mode": "full"},
                "DELIVERIES": {"separator": ";", "mode": "full"},
                "DICTIONARY": {"separator": ";", "mode": "full"},
                "INVOICES": {"separator": ";", "mode": "full"},
                "POSTPRODUCTION_DETAILS": {"separator": ";", "mode": "delta"},
                "PREV_YEAR_LEAVE_BALANCE": {"separator": ";", "mode": "full"},
                "PROJECTS": {"separator": ";", "mode": "full"},
                "RESOURCES": {"separator": ";", "mode": "full"},
                "EXTERNAL_DELIVERIES": {"separator": "|", "mode": "full"},
                "RESOURCES_IMAGES": {"separator": ";", "mode": "delta"},
            },
        }

        response = requests.post(
            CLOUD_RUN_URL,
            headers=headers,
            data=json.dumps(payload),
            timeout=300
        )

        if response.status_code != 200:
            raise Exception(f"Cloud Run call failed: {response.text}")

        return response.text

    trigger_cloud_run()

cloudrun_ingestion_boond()