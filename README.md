# Airflow Orchestration Platform + Web Builder

End-to-end project for building, validating, generating, and operating Apache Airflow DAGs from a web UI.

This repository contains:

- `(root)`: Docker Compose-based Airflow stack (CeleryExecutor, Postgres, Redis, API server, scheduler, worker, triggerer).
- `web_app/`: Flask + React-style frontend/backend application for visual pipeline authoring and operations.

## Key Features

- Visual DAG/pipeline builder (drag-and-drop nodes, connect, configure).
- JSON save/load for pipeline definitions.
- DAG generation from templates into Python files.
- DAG run trigger + run/task monitoring + logs from the web app.
- Airflow Connections management from UI.
- Admin dashboard (users, roles, requests, platform metrics/health).
- Requests workflow (submit and track requests).
- Theme support (light/dark) across pages.

## Airflow DAG Sources: Local + Git

This project is configured so Airflow can read DAGs from:

1. Local DAG folder (mounted volume):
   - `/opt/airflow/dags` (host: `dags`)
2. Git-based DAG bundle (`GitDagBundle`):
   - configured in `docker-compose.yaml` via:
     - `AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST`
     - bundle name: `github_dags`
     - `git_conn_id: git_default`
     - `subdir: dags`
     - `tracking_ref: main`

If you want Git DAG sync active, make sure Airflow has a valid `git_default` connection.  
If you only want local DAGs, you can remove/disable the Git bundle block in Compose config.

## Repository Structure

```text
application/
├── airflow/
│   ├── docker-compose.yaml
│   ├── Dockerfile
│   ├── .env
│   ├── dags/
│   ├── logs/
│   ├── config/
│   └── plugins/
└── web_app/
    ├── app.py
    ├── config.py
    ├── requirements.txt
    ├── .env
    ├── templates/
    ├── static/
    ├── dag_templates/
    ├── generated_dags/
    └── saved_pipelines/
```

## Prerequisites

- Git
- Docker Desktop + Docker Compose plugin
- Python 3.10+ (recommended)
- PowerShell (commands below use PowerShell syntax)

## Installation and Run

### 1. Clone

```powershell
git clone <your-repo-url>
cd web_app_airflow\application
```

### 2. Start Airflow with Docker Compose

```powershell
cd .\airflow
docker compose up airflow-init
docker compose up -d
```

Notes:

- Airflow API/UI is exposed on `http://localhost:8080`.
- Default bootstrap credentials (from compose defaults) are usually:
  - username: `airflow`
  - password: `airflow`

### 3. Set up Python virtual environment for Web App

```powershell
cd ..\web_app
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 4. Configure Web App environment

Edit `application/web_app/.env` (or set env vars in shell).  
Typical minimum values:

```env
APP_ADMIN_USERNAME=admin
APP_ADMIN_PASSWORD=admin123
FLASK_SECRET_KEY=change-me

AIRFLOW_API_BASE_URL=http://localhost:8080
AIRFLOW_API_USERNAME=airflow
AIRFLOW_API_PASSWORD=airflow
AIRFLOW_API_VERIFY_TLS=0

# Host folder where generated DAG files are copied
AIRFLOW_DAGS_DIR=D:\\dataops\\APACHE-AIRFLOW\\dags
```

Optional sync settings (already supported by app):

```env
AIRFLOW_DOCKER_SYNC_ENABLED=true
AIRFLOW_DOCKER_SYNC_BASE_DIR=/opt/airflow/local_uploads
AIRFLOW_DOCKER_SYNC_CONTAINERS=airflowgitfresh-airflow-scheduler-1,airflowgitfresh-airflow-dag-processor-1,airflowgitfresh-airflow-triggerer-1
```

### 5. Launch Web App

```powershell
python .\app.py
```

The app auto-selects an available port (starts from `8000`).  
Most common URL: `http://127.0.0.1:8000`.

## Main URLs

- Home: `http://127.0.0.1:8000/`
- Login: `http://127.0.0.1:8000/login`
- Builder: `http://127.0.0.1:8000/builder`
- Airflow Connections: `http://127.0.0.1:8000/airflow-connections`
- Requests: `http://127.0.0.1:8000/requests`
- Admin Dashboard: `http://127.0.0.1:8000/admin-dashboard`
- Airflow UI/API: `http://localhost:8080`

## Daily Developer Workflow

From repository root:

```powershell
docker compose up -d
```

From `application/web_app`:

```powershell
.\.venv\Scripts\Activate.ps1
python .\app.py
```

## Stop / Cleanup

Stop Airflow stack:

```powershell
cd .\application\airflow
docker compose down
```

Stop and remove volumes (full reset):

```powershell
docker compose down -v
```

## Troubleshooting

- Port already in use:
  - Web app auto-tries fallback ports.
  - Airflow port `8080` can be changed in compose if needed.
- DAG not visible in Airflow:
  - verify `AIRFLOW_DAGS_DIR` points to your mounted `dags`
  - check scheduler logs in `logs`
  - verify DAG file syntax.
- Git DAG bundle not loading:
  - ensure `git_default` connection exists and is valid.
- Permission issues on Linux:
  - set `AIRFLOW_UID` in `.env` as recommended by Airflow docs.

## Security Note

- Do not commit real secrets in `.env` files.
- Rotate credentials/tokens before sharing/deploying.


