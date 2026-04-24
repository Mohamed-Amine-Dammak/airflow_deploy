# Airflow Orchestration Web App (v1)

Flask + React (no heavy frontend framework tooling) web app for visually building a **sequential** orchestration pipeline and generating a final Airflow DAG Python file from module templates.

## Features

- Simple session-based authentication (single admin account)
- Interactive drag-and-drop canvas for module nodes
- Node configuration panel with dynamic schema-driven forms
- Sequential edge linking (v1: no branching)
- Strong backend validation:
  - pipeline config validation
  - graph structure validation
  - per-module config validation
- Jinja2 template-based DAG code generation
- Generated DAG saved locally in `generated_dags/`
- Generated DAG copied to Airflow folder: `D:\airflow\dags`
- Save/load pipeline JSON
- DAG preview in UI
- Run DAG directly from builder once Airflow confirms readiness
- Live run monitoring: node status coloring + per-task log viewer
- In-app Airflow Connections manager (HTTP + Azure Blob Storage)
- Admin Dashboard page with KPIs and full user management:
  - create/edit/delete users
  - assign predefined roles
  - create custom roles and assign them

## Project Structure

```text
web_app/
├── app.py
├── config.py
├── requirements.txt
├── README.md
├── airflow_modules/                  # Your existing example DAG/module files (reference)
├── dag_templates/
│   ├── base_dag.py.j2
│   ├── module_n8n.py.j2
│   ├── module_talend.py.j2
│   ├── module_dataform.py.j2
│   ├── module_dataform_tags.py.j2
│   ├── module_cloudrun.py.j2
│   ├── module_azure_download.py.j2
│   ├── module_azure_upload.py.j2
│   ├── module_powerbi.py.j2
│   ├── module_sftp_upload.py.j2
│   ├── module_sftp_sensor.py.j2
│   ├── module_y2.py.j2
│   └── pipeline_sequential.py.j2
├── generated_dags/
├── sample_pipelines/
│   └── demo_n8n_cloudrun_dataform_powerbi.json
├── saved_pipelines/
├── services/
│   ├── dag_generator.py
│   ├── file_writer.py
│   ├── pipeline_validator.py
│   ├── schema_registry.py
│   └── template_registry.py
├── models/
│   ├── module_models.py
│   └── pipeline_models.py
├── static/
│   ├── css/style.css
│   └── js/
│       ├── app.js
│       ├── canvas.js
│       ├── forms.js
│       └── modules.js
└── templates/
    ├── index.html
    ├── pipeline_builder.html
    └── partials/
```

## Setup

1. Clone the repository and go to the app folder:

```powershell
git clone https://github.com/Olive-Soft-Company/APACHE-AIRFLOW.git
cd APACHE-AIRFLOW\web_app
```

2. Create virtual environment and install dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

> Keep `.venv/` local only. Do not commit it to Git.

3. (Optional) Set custom Airflow DAG destination:

```powershell
$env:AIRFLOW_DAGS_DIR="D:\airflow\dags"
```

4. (Optional) Configure Airflow API for DAG readiness + run trigger:

```powershell
$env:AIRFLOW_API_BASE_URL="http://localhost:8080"
$env:AIRFLOW_API_USERNAME="airflow"
$env:AIRFLOW_API_PASSWORD="airflow"
$env:AIRFLOW_API_VERIFY_TLS="0"
$env:AIRFLOW_API_TIMEOUT_SECONDS="30"
$env:DAG_RUN_POLL_INTERVAL_SECONDS="5"
$env:DAG_RUN_POLL_TIMEOUT_SECONDS="1800"
```

5. (Optional) Configure admin login credentials:

```powershell
$env:APP_ADMIN_USERNAME="admin"
$env:APP_ADMIN_PASSWORD="admin123"
```

6. Run app:

```powershell
python app.py
```

7. Open:
   - `http://127.0.0.1:5000/`
   - Builder page: `http://127.0.0.1:5000/builder`
   - Airflow Connections page: `http://127.0.0.1:5000/airflow-connections`
   - Admin Dashboard page: `http://127.0.0.1:5000/admin-dashboard`
   - Admin User Creation page: `http://127.0.0.1:5000/admin/users/new`
   - Admin Role Creation page: `http://127.0.0.1:5000/admin/roles/new`
   - Login page: `http://127.0.0.1:5000/login`

## Quick Start (Run Every Time)

From `d:\web_app_airflow\web_app`:

```powershell
.\.venv\Scripts\Activate.ps1
python app.py
```

Then open `http://127.0.0.1:5000/login`.

## API Endpoints

- `GET /login`
- `POST /login`
- `GET /logout`
- `GET /admin-dashboard`
- `GET /api/admin/users`
- `POST /api/admin/users`
- `PUT /api/admin/users/<user_id>`
- `DELETE /api/admin/users/<user_id>`
- `POST /api/admin/roles`
- `GET /api/modules`
- `GET /api/module-schema/<module_type>`
- `POST /api/validate-pipeline`
- `POST /api/generate-dag`
- `POST /api/dag-readiness`
- `POST /api/run-dag`
- `GET /api/runs/<dag_id>/<dag_run_id>/status`
- `GET /api/runs/<dag_id>/<dag_run_id>/tasks`
- `GET /api/runs/<dag_id>/<dag_run_id>/tasks/<task_id>/logs?try_number=1`
- `GET /api/airflow/connections`
- `POST /api/airflow/connections`
- `PUT /api/airflow/connections/<connection_id>`
- `DELETE /api/airflow/connections/<connection_id>`
- `POST /api/save-pipeline`
- `GET /api/load-pipeline` (list saved JSON files)
- `POST /api/load-pipeline` (load one saved JSON file)
- `GET /api/generated-dag/<filename>`
- `POST /api/workflows/<workflow_id>/pull-requests`
- `POST /api/github/webhooks`

## GitHub App Publishing (PR Flow)

This platform supports publishing a selected DAG version to GitHub through an organization-owned GitHub App.

### Required GitHub App permissions

- Repository `Contents`: Read & write
- Repository `Pull requests`: Read & write
- Repository `Metadata`: Read-only

### Required webhook event

- `pull_request` (used to detect `closed` + `merged=true`)

### Environment variables

Use `.env.example` as reference and configure:

- `GITHUB_APP_ID`
- `GITHUB_APP_PRIVATE_KEY_PATH` (or `GITHUB_APP_PRIVATE_KEY`)
- `GITHUB_APP_INSTALLATION_ID`
- `GITHUB_OWNER`
- `GITHUB_REPO`
- `GITHUB_BASE_BRANCH` (default `main`)
- `GITHUB_DAGS_REPO_DIR` (default `airflow/dags`)
- `GITHUB_WEBHOOK_SECRET`
- `GITHUB_BRANCH_COLLISION_STRATEGY` (`suffix` or `reuse_or_suffix`)

### Publish behavior

- PR creation always targets an explicitly selected DAG version ID.
- The committed file path is `<GITHUB_DAGS_REPO_DIR>/<selected_output_filename>.py`.
- Local DAG file deletion happens only after merge confirmation from webhook processing.

## UI Behavior

- Left panel: drag module cards to canvas
  - each module displays its technology logo
- Canvas:
  - drag nodes to reposition
  - connect nodes from box edges:
    - click source node **right edge handle** (output)
    - click target node **left edge handle** (input)
  - edges are rendered with direction arrows
  - `Delete` button removes node and attached edges
  - Azure modules:
    - `Azure Download` supports one-file mode (`file_name` + extension) and multi-file mode (prefix + extension)
    - `Azure Blob Sensor` is a dedicated sensor node (`wasb_conn_id`, `container_name`, `prefix`, `poke_interval`, `timeout`)
- Right panel:
  - global pipeline settings
    - scheduling mode selector:
      - Manual / Preset / Cron / Interval / Event-Dataset / Custom
      - mode-specific inputs appear only after selecting a mode
  - selected node config form (changes by module type/mode/action)
  - Conditional Router:
    - `Input Source` includes an upstream suggestion dropdown
    - suggestions include `xcom_node:<node_id>` and common key variants such as `xcom_node:<node_id>.detected_file_name`
    - if the router is connected from a `Parallel Join`, suggestions expand to the join's incoming nodes (for example `Azure Download` and `SFTP Sensor`)
- Top actions:
  - New / Save / Load / Validate / Generate DAG / Run DAG / Preview

## Validation Rules (v1)

- `dag_id` required and Airflow/Python-safe (`[A-Za-z][A-Za-z0-9_]*`)
- output filename required and must end with `.py`
- retries and retry delay are integers `>= 0`
- scheduling mode is required and validates only the selected mode fields
- alert email format checked if provided
- graph must be a strict sequential chain:
  - one start node
  - one end node
  - no branching
  - no cycles
  - intermediate nodes have exactly one incoming and one outgoing edge

## Extending Modules Later

To add a new module:

1. Add module schema in `services/schema_registry.py`
2. Add template selection in `services/template_registry.py`
3. Add new Jinja file in `dag_templates/`
4. Add palette item in `static/js/modules.js`

No core DAG generation rewrite is needed.

