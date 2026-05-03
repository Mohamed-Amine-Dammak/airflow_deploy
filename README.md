# Airflow Orchestration Platform + DataOps Promotion

End-to-end project for building, generating, evaluating, and promoting Apache Airflow DAGs from a web UI.

## Repository Layout

- `docker-compose.yaml`, `Dockerfile`, `config/`, `dags/`, `logs/`, `plugins/`: Airflow stack at repository root.
- `web_app/`: Flask web application for DAG authoring, publishing, and operations.
- `scripts/`: helper scripts for validation, evaluation, and promotion metadata updates.

## Core Concepts

- DAG versions are tracked in `web_app/pipeline_versions_store.json`.
- Promotion lifecycle uses statuses like `draft`, `submitted`, `eval`, `challenger`, `champion`, `archived`.
- Scoring and promotion APIs are exposed by the web app.

## Run Locally

1. Start Airflow from repo root:

```powershell
docker compose up airflow-init
docker compose up -d
```

2. Start web app:

```powershell
cd .\web_app
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python .\app.py
```

## Main URLs

- Web app: `http://127.0.0.1:8000`
- Airflow UI/API: `http://localhost:8080`

## Scoring and Promotion APIs

- `POST /api/workflows/{workflow_id}/versions/{version_id}/score`
- `GET /api/workflows/{workflow_id}/champion`
- `GET /api/workflows/{workflow_id}/challengers`
- `POST /api/workflows/{workflow_id}/versions/{version_id}/promote`
- `POST /api/workflows/{workflow_id}/versions/{version_id}/rollback`

## Notes

- Keep secrets (`.env`, keys, tokens) out of Git history.
- If you use Git-backed DAG publishing, ensure connection/config matches your repo and branch strategy.
