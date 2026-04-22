from airflow.sdk import dag, task
from datetime import datetime, timedelta
import requests
import json
from google.oauth2 import service_account
from google.auth.transport.requests import Request


# time par default 15 min research if query gourmand if there is a risque in dataform 
PROJECT_ID = "os-dpf-ariflow-prj-dev"
REGION = "europe-west1" #default
REPOSITORY = "airflow-dataform"
COMPILATION_ID = "9b22a514-c43e-4b07-b75d-f38d19ba0760"

SERVICE_ACCOUNT_FILE = "/opt/airflow/os-dpf-ariflow-prj-dev-3b50cbb16478.json"

DATAFORM_URL = f"https://dataform.googleapis.com/v1/projects/{PROJECT_ID}/locations/{REGION}/repositories/{REPOSITORY}/workflowInvocations"

@dag(
    dag_id="trigger_dataform_production",
    start_date=datetime(2026, 2, 2),
    schedule=None,
    catchup=False,
    tags=["dataform"],
)
def trigger_dataform_production():

    @task
    def run_release():

        # load service account
        with open(SERVICE_ACCOUNT_FILE) as f:
            sa_info = json.load(f)

        service_account_email = sa_info["client_email"]

        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

        credentials.refresh(Request())

        headers = {
            "Authorization": f"Bearer {credentials.token}",
            "Content-Type": "application/json",
        }

        body = {
            "compilationResult": f"projects/{PROJECT_ID}/locations/{REGION}/repositories/{REPOSITORY}/compilationResults/{COMPILATION_ID}",
            "invocationConfig": {
                "serviceAccount": service_account_email
            },
        }

        response = requests.post(DATAFORM_URL, headers=headers, json=body)

        if response.status_code != 200:
            raise Exception(response.text)

        print(response.json())
        return response.json()

    run_release()


dag = trigger_dataform_production()