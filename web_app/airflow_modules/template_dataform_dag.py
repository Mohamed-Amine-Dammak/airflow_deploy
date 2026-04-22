from airflow.sdk import dag, task
from airflow.models import Variable
from datetime import datetime
import requests
import json

from google.oauth2 import service_account
from google.auth.transport.requests import Request


# Airflow Variables
PROJECT_ID = Variable.get("dataform_project_id")
REGION = Variable.get("dataform_region")
REPOSITORY = Variable.get("dataform_repository")
COMPILATION_ID = Variable.get("dataform_compilation_id")
SERVICE_ACCOUNT_FILE = Variable.get("dataform_service_account_file")

DATAFORM_URL = (
    f"https://dataform.googleapis.com/v1/projects/{PROJECT_ID}"
    f"/locations/{REGION}/repositories/{REPOSITORY}/workflowInvocations"
)


@dag(
    dag_id="trigger_dataform_template",
    start_date=datetime(2026, 2, 2),
    schedule=None,
    catchup=False,
    tags=["dataform", "template"],
)
def trigger_dataform_template():

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
            "compilationResult": (
                f"projects/{PROJECT_ID}/locations/{REGION}/repositories/{REPOSITORY}"
                f"/compilationResults/{COMPILATION_ID}"
            ),
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


dag = trigger_dataform_template()