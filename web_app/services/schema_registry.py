from __future__ import annotations

import copy
import json
import re
from datetime import datetime
from typing import Any


EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
ALERT_MODES = {"on_failure", "on_retry", "both", "none"}
SCHEDULE_MODES = {"manual", "preset", "cron", "interval", "event", "custom"}
SCHEDULE_PRESETS = {
    "once": "@once",
    "hourly": "@hourly",
    "daily": "@daily",
    "weekly": "@weekly",
    "monthly": "@monthly",
    "yearly": "@yearly",
}
SCHEDULE_PRESETS_REVERSE = {value: key for key, value in SCHEDULE_PRESETS.items()}
INTERVAL_UNITS = {"minutes", "hours", "days"}
ROUTER_MATCH_TYPES = {"starts_with", "contains", "equals", "regex"}


MODULE_REGISTRY: dict[str, dict[str, Any]] = {
    "n8n": {
        "type": "n8n",
        "display_name": "n8n Workflow",
        "description": "Resolve workflow by name, trigger webhook, optionally monitor execution.",
        "template": "module_n8n.py.j2",
        "required_fields": ["workflow_name"],
        "fields": [
            {"name": "workflow_name", "label": "Workflow Name", "type": "text", "required": True},
            {
                "name": "http_method",
                "label": "HTTP Method",
                "type": "select",
                "required": False,
                "default": "GET",
                "options": [{"value": "GET", "label": "GET"}, {"value": "POST", "label": "POST"}],
            },
            {
                "name": "query_params",
                "label": "Query Params (JSON)",
                "type": "json",
                "required": False,
                "placeholder": '{"env":"prod"}',
            },
            {
                "name": "request_body",
                "label": "Request Body (JSON)",
                "type": "json",
                "required": False,
                "placeholder": '{"file":"input.csv"}',
            },
            {
                "name": "use_test_webhook",
                "label": "Use Test Webhook",
                "type": "boolean",
                "required": False,
                "default": True,
            },
            {
                "name": "monitor_poll_interval_seconds",
                "label": "Monitor Poll Interval (seconds)",
                "type": "number",
                "required": False,
                "default": 5,
            },
            {
                "name": "monitor_timeout_seconds",
                "label": "Monitor Timeout (seconds)",
                "type": "number",
                "required": False,
                "default": 300,
            },
        ],
    },
    "talend": {
        "type": "talend",
        "display_name": "Talend Cloud",
        "description": "Resolve executable id from job name, trigger execution, monitor status.",
        "template": "module_talend.py.j2",
        "required_fields": ["job_name"],
        "fields": [
            {"name": "job_name", "label": "Job Name", "type": "text", "required": True},
            {"name": "workspace_id", "label": "Workspace ID", "type": "text", "required": False},
        ],
    },
    "dataform": {
        "type": "dataform",
        "display_name": "Dataform",
        "description": "Run full Dataform invocation or tag-based sequential invocation.",
        "template": "module_dataform.py.j2",
        "required_fields": ["region", "repository"],
        "fields": [
            {
                "name": "project_mode",
                "label": "Project Scope",
                "type": "select",
                "required": True,
                "default": "single",
                "options": [
                    {"value": "single", "label": "One Project"},
                    {"value": "multiple", "label": "Multiple Projects"},
                ],
            },
            {
                "name": "project_id",
                "label": "Project ID",
                "type": "text",
                "required": False,
                "show_if": {"project_mode": "single"},
            },
            {
                "name": "project_count",
                "label": "Number of Projects",
                "type": "number",
                "required": False,
                "show_if": {"project_mode": "multiple"},
                "default": 2,
            },
            {
                "name": "project_ids",
                "label": "Project IDs",
                "type": "dynamic_project_ids",
                "required": False,
                "show_if": {"project_mode": "multiple"},
            },
            {"name": "region", "label": "Region", "type": "text", "required": True},
            {"name": "repository", "label": "Repository", "type": "text", "required": True},
            {
                "name": "gcp_connection_id",
                "label": "GCP Connection Name",
                "type": "text",
                "required": True,
                "placeholder": "my_gcp_conn",
                "description": "Use a Google Service Account connection created in Airflow Connections page.",
            },
            {"name": "compilation_id", "label": "Compilation ID", "type": "text", "required": False},
            {
                "name": "dataform_tags",
                "label": "Dataform Tags (comma-separated)",
                "type": "list",
                "required": False,
                "placeholder": "STG,DH,DW",
            },
        ],
    },
    "cloudrun": {
        "type": "cloudrun",
        "display_name": "Cloud Run",
        "description": "Authenticated Cloud Run call with service account ID token.",
        "template": "module_cloudrun.py.j2",
        "required_fields": ["url", "payload"],
        "fields": [
            {"name": "url", "label": "Cloud Run URL", "type": "text", "required": True},
            {
                "name": "gcp_connection_id",
                "label": "GCP Connection Name",
                "type": "text",
                "required": True,
                "placeholder": "my_gcp_conn",
            },
            {"name": "payload", "label": "Payload (JSON)", "type": "json", "required": True},
        ],
    },
    "gcs": {
        "type": "gcs",
        "display_name": "Google Cloud Storage",
        "description": "Upload, move, or delete files in GCS from local paths or Google Drive links.",
        "template": "module_gcs.py.j2",
        "required_fields": ["bucket_name", "action", "gcp_connection_id"],
        "fields": [
            {
                "name": "action",
                "label": "Action",
                "type": "select",
                "required": True,
                "default": "upload",
                "options": [
                    {"value": "upload", "label": "Upload"},
                    {"value": "move", "label": "Move"},
                    {"value": "delete", "label": "Delete"},
                ],
            },
            {
                "name": "bucket_name",
                "label": "Bucket Name",
                "type": "text",
                "required": True,
            },
            {
                "name": "gcp_connection_id",
                "label": "GCP Connection Name",
                "type": "text",
                "required": True,
                "placeholder": "my_gcp_conn",
                "description": "Use a Google Service Account connection created in Airflow Connections page.",
            },
            {
                "name": "destination_path",
                "label": "Destination Path Prefix",
                "type": "text",
                "required": False,
                "placeholder": "uploads/2026/04",
                "show_if": {"action": "upload"},
            },
            {
                "name": "upload_mode",
                "label": "Upload Mode",
                "type": "select",
                "required": False,
                "default": "single",
                "show_if": {"action": "upload"},
                "options": [
                    {"value": "single", "label": "One File"},
                    {"value": "multiple", "label": "Many Files"},
                ],
            },
            {
                "name": "source_type",
                "label": "Source",
                "type": "select",
                "required": False,
                "default": "local",
                "show_if_all": [
                    {"action": "upload"},
                    {"upload_mode": "single"},
                ],
                "options": [
                    {"value": "local", "label": "Local Computer"},
                    {"value": "drive", "label": "Google Drive"},
                ],
            },
            {
                "name": "multiple_source_type",
                "label": "Source",
                "type": "select",
                "required": False,
                "default": "local",
                "show_if_all": [
                    {"action": "upload"},
                    {"upload_mode": "multiple"},
                ],
                "options": [
                    {"value": "local", "label": "Local Computer"},
                    {"value": "drive", "label": "Google Drive"},
                ],
            },
            {
                "name": "local_file_path",
                "label": "Local File Path",
                "type": "text",
                "required": False,
                "show_if_all": [
                    {"action": "upload"},
                    {"upload_mode": "single"},
                    {"source_type": "local"},
                ],
            },
            {
                "name": "local_file_browser",
                "label": "Browse Local File",
                "type": "file_single",
                "required": False,
                "show_if_all": [
                    {"action": "upload"},
                    {"upload_mode": "single"},
                    {"source_type": "local"},
                ],
                "description": "Pick a file from your computer. The selected path is used when available.",
            },
            {
                "name": "local_directory_path",
                "label": "Local Directory Path",
                "type": "text",
                "required": False,
                "show_if_all": [
                    {"action": "upload"},
                    {"upload_mode": "multiple"},
                    {"multiple_source_type": "local"},
                ],
            },
            {
                "name": "local_file_paths",
                "label": "Browse Multiple Local Files",
                "type": "file_multiple",
                "required": False,
                "show_if_all": [
                    {"action": "upload"},
                    {"upload_mode": "multiple"},
                    {"multiple_source_type": "local"},
                ],
                "description": "Select one or many files.",
            },
            {
                "name": "drive_file_link",
                "label": "Google Drive File Link",
                "type": "text",
                "required": False,
                "show_if_all": [
                    {"action": "upload"},
                    {"upload_mode": "single"},
                    {"source_type": "drive"},
                ],
                "placeholder": "https://drive.google.com/file/d/<file_id>/view",
            },
            {
                "name": "drive_input_mode",
                "label": "Google Drive Input Mode",
                "type": "select",
                "required": False,
                "default": "directory",
                "show_if_all": [
                    {"action": "upload"},
                    {"upload_mode": "multiple"},
                    {"multiple_source_type": "drive"},
                ],
                "options": [
                    {"value": "directory", "label": "Directory"},
                    {"value": "links", "label": "Links"},
                ],
            },
            {
                "name": "drive_directory_link",
                "label": "Google Drive Directory Link",
                "type": "text",
                "required": False,
                "show_if_all": [
                    {"action": "upload"},
                    {"upload_mode": "multiple"},
                    {"multiple_source_type": "drive"},
                    {"drive_input_mode": "directory"},
                ],
                "placeholder": "https://drive.google.com/drive/folders/<folder_id>",
            },
            {
                "name": "drive_file_links",
                "label": "Google Drive File Links",
                "type": "link_list",
                "required": False,
                "show_if_all": [
                    {"action": "upload"},
                    {"upload_mode": "multiple"},
                    {"multiple_source_type": "drive"},
                    {"drive_input_mode": "links"},
                ],
            },
            {
                "name": "source_object",
                "label": "Source Object",
                "type": "text",
                "required": False,
                "show_if": {"action": "move"},
                "placeholder": "incoming/file.csv",
            },
            {
                "name": "destination_bucket_name",
                "label": "Destination Bucket",
                "type": "text",
                "required": False,
                "show_if": {"action": "move"},
                "placeholder": "my-target-bucket",
            },
            {
                "name": "destination_object",
                "label": "Destination Object",
                "type": "text",
                "required": False,
                "show_if": {"action": "move"},
                "placeholder": "processed/file.csv",
            },
            {
                "name": "delete_mode",
                "label": "Delete Mode",
                "type": "select",
                "required": False,
                "default": "single",
                "show_if": {"action": "delete"},
                "options": [
                    {"value": "single", "label": "One File"},
                    {"value": "multiple", "label": "Many Files"},
                ],
            },
            {
                "name": "object_name",
                "label": "Object Name",
                "type": "text",
                "required": False,
                "show_if_all": [
                    {"action": "delete"},
                    {"delete_mode": "single"},
                ],
                "placeholder": "incoming/file.csv",
            },
            {
                "name": "delete_object_prefix",
                "label": "Object Prefix",
                "type": "text",
                "required": False,
                "show_if_all": [
                    {"action": "delete"},
                    {"delete_mode": "multiple"},
                ],
                "placeholder": "incoming",
                "description": "Delete all objects that start with this prefix.",
            },
            {
                "name": "object_names",
                "label": "Object Names (comma-separated)",
                "type": "list",
                "required": False,
                "show_if_all": [
                    {"action": "delete"},
                    {"delete_mode": "multiple"},
                ],
                "placeholder": "incoming/a.csv,incoming/b.csv",
                "description": "Optional explicit object names to delete one by one.",
            },
        ],
    },
    "azure": {
        "type": "azure",
        "display_name": "Azure Blob",
        "description": "Upload, download, or delete blobs.",
        "template": "module_azure_upload.py.j2",
        "required_fields": ["wasb_conn_id", "container_name", "action"],
        "fields": [
            {
                "name": "wasb_conn_id",
                "label": "WASB Connection ID",
                "type": "text",
                "required": True,
                "default": "wasb_default",
                "placeholder": "wasb_default",
            },
            {
                "name": "action",
                "label": "Action",
                "type": "select",
                "required": True,
                "default": "upload",
                "options": [
                    {"value": "upload", "label": "Upload"},
                    {"value": "download", "label": "Download"},
                    {"value": "delete", "label": "Delete"},
                ],
            },
            {
                "name": "container_name",
                "label": "Container Name",
                "type": "text",
                "required": True,
            },
            {
                "name": "upload_mode",
                "label": "Upload Mode",
                "type": "select",
                "required": False,
                "default": "single",
                "show_if": {"action": "upload"},
                "options": [
                    {"value": "single", "label": "One File"},
                    {"value": "multiple", "label": "Many Files"},
                ],
            },
            {
                "name": "local_file_path",
                "label": "Local File Path",
                "type": "text",
                "required": False,
                "show_if_all": [
                    {"action": "upload"},
                    {"upload_mode": "single"},
                ],
            },
            {
                "name": "local_file_browser",
                "label": "Browse Local File",
                "type": "file_single",
                "required": False,
                "show_if_all": [
                    {"action": "upload"},
                    {"upload_mode": "single"},
                ],
                "description": "Select one local file.",
            },
            {
                "name": "local_file_paths",
                "label": "Browse Multiple Local Files",
                "type": "file_multiple",
                "required": False,
                "show_if_all": [
                    {"action": "upload"},
                    {"upload_mode": "multiple"},
                ],
                "description": "Select one or many local files.",
            },
            {
                "name": "blob_name",
                "label": "Blob Name",
                "type": "text",
                "required": False,
                "placeholder": "/path/yourfile.*",
                "show_if_all": [
                    {"action": "upload"},
                    {"upload_mode": "single"},
                ],
            },
            {
                "name": "blob_prefix",
                "label": "Blob Prefix",
                "type": "text",
                "required": False,
                "show_if_all": [
                    {"action": "upload"},
                    {"upload_mode": "multiple"},
                ],
                "placeholder": "uploads",
            },
            {
                "name": "create_container",
                "label": "Create Container",
                "type": "radio_boolean",
                "required": False,
                "default": True,
                "show_if": {"action": "upload"},
            },
            {
                "name": "download_mode",
                "label": "Download Mode",
                "type": "select",
                "required": False,
                "show_if": {"action": "download"},
                "options": [
                    {"value": "single", "label": "One File"},
                    {"value": "multiple", "label": "Multiple Files (Prefix)"},
                ],
            },
            {
                "name": "file_name",
                "label": "File Name (without extension)",
                "type": "text",
                "required": False,
                "show_if_all": [
                    {"action": "download"},
                    {"download_mode": "single"},
                ],
                "placeholder": "CUSTOMER_CUS_TEST",
                "description": "If file is inside a folder, you can use folder path too: uploads/CUSTOMER_CUS_TEST",
            },
            {
                "name": "file_prefix",
                "label": "File Prefix",
                "type": "text",
                "required": False,
                "show_if_all": [
                    {"action": "download"},
                    {"download_mode": "multiple"},
                ],
                "placeholder": "uploads/exp_",
            },
            {
                "name": "local_dir_download",
                "label": "Local Download Directory",
                "type": "text",
                "required": False,
                "show_if": {"action": "download"},
            },
            {
                "name": "delete_mode",
                "label": "Delete Mode",
                "type": "select",
                "required": False,
                "default": "single",
                "show_if": {"action": "delete"},
                "options": [
                    {"value": "single", "label": "One File"},
                    {"value": "multiple", "label": "Multiple Files (Prefix)"},
                ],
            },
            {
                "name": "delete_blob_name",
                "label": "Blob Name",
                "type": "text",
                "required": False,
                "show_if_all": [
                    {"action": "delete"},
                    {"delete_mode": "single"},
                ],
                "placeholder": "uploads/my_file.csv",
            },
            {
                "name": "delete_prefix",
                "label": "Blob Prefix",
                "type": "text",
                "required": False,
                "show_if_all": [
                    {"action": "delete"},
                    {"delete_mode": "multiple"},
                ],
                "placeholder": "uploads/exp_",
            },
        ],
    },
    "azure_blob_sensor": {
        "type": "azure_blob_sensor",
        "display_name": "Azure Blob Sensor",
        "description": "Wait for blob prefix in Azure Blob storage.",
        "template": "module_azure_blob_sensor.py.j2",
        "required_fields": ["wasb_conn_id", "container_name", "prefix", "poke_interval", "timeout"],
        "fields": [
            {
                "name": "wasb_conn_id",
                "label": "WASB Connection ID",
                "type": "text",
                "required": True,
                "default": "wasb_default",
            },
            {
                "name": "container_name",
                "label": "Container Name",
                "type": "text",
                "required": True,
            },
            {
                "name": "prefix",
                "label": "Blob Prefix",
                "type": "text",
                "required": True,
                "placeholder": "cus_",
                "description": "Use blob prefix without leading slash. Example: uploads/CUSTOMER_",
            },
            {
                "name": "poke_interval",
                "label": "Poke Interval (seconds)",
                "type": "number",
                "required": True,
                "default": 30,
            },
            {
                "name": "timeout",
                "label": "Timeout (seconds)",
                "type": "number",
                "required": True,
                "default": 600,
            },
        ],
    },
    "powerbi": {
        "type": "powerbi",
        "display_name": "Power BI",
        "description": "Refresh dataset by workspace and dataset id.",
        "template": "module_powerbi.py.j2",
        "required_fields": ["dataset_id", "workspace_id"],
        "fields": [
            {"name": "workspace_id", "label": "Workspace ID", "type": "text", "required": True},
            {"name": "dataset_id", "label": "Dataset ID", "type": "text", "required": True},
            {
                "name": "monitor_poll_interval_seconds",
                "label": "Monitor Poll Interval (seconds)",
                "type": "number",
                "required": False,
                "default": 10,
            },
            {
                "name": "monitor_timeout_seconds",
                "label": "Monitor Timeout (seconds)",
                "type": "number",
                "required": False,
                "default": 900,
            },
        ],
    },
    "sftp_upload": {
        "type": "sftp_upload",
        "display_name": "SFTP",
        "description": "Upload, download, or delete files on SFTP.",
        "template": "module_sftp_upload.py.j2",
        "required_fields": ["conn_name", "action", "remote_dir"],
        "fields": [
            {
                "name": "conn_name",
                "label": "SFTP Connection Name",
                "type": "text",
                "required": True,
                "default": "sftp",
                "placeholder": "sftp",
                "description": "Airflow connection ID to use for SFTP.",
            },
            {
                "name": "action",
                "label": "Action",
                "type": "select",
                "required": True,
                "default": "upload",
                "options": [
                    {"value": "upload", "label": "Upload"},
                    {"value": "download", "label": "Download"},
                    {"value": "delete", "label": "Delete"},
                ],
            },
            {
                "name": "remote_dir",
                "label": "Remote Directory",
                "type": "text",
                "required": True,
            },
            {
                "name": "upload_mode",
                "label": "Upload Mode",
                "type": "select",
                "required": False,
                "default": "single",
                "show_if": {"action": "upload"},
                "options": [
                    {"value": "single", "label": "One File"},
                    {"value": "multiple", "label": "Multiple Files (Prefix)"},
                ],
            },
            {
                "name": "local_file_browser",
                "label": "Browse Local File",
                "type": "file_single",
                "required": False,
                "show_if_all": [{"action": "upload"}, {"upload_mode": "single"}],
                "description": "Pick one local file from your computer.",
            },
            {
                "name": "local_file_path",
                "label": "Local File Path",
                "type": "text",
                "required": False,
                "show_if_all": [{"action": "upload"}, {"upload_mode": "single"}],
                "description": "Filled automatically after file selection. You can also paste a full path.",
            },
            {
                "name": "local_file_paths",
                "label": "Browse Multiple Local Files",
                "type": "file_multiple",
                "required": False,
                "show_if_all": [{"action": "upload"}, {"upload_mode": "multiple"}],
                "description": "Pick one or many local files from your computer.",
            },
            {
                "name": "local_directory_path",
                "label": "Local Directory Path",
                "type": "text",
                "required": False,
                "show_if_all": [{"action": "upload"}, {"upload_mode": "multiple"}],
                "description": "Optional fallback: directory to scan when explicit file list is not provided.",
            },
            {
                "name": "file_prefix",
                "label": "File Prefix",
                "type": "text",
                "required": False,
                "show_if_all": [{"action": "upload"}, {"upload_mode": "multiple"}],
                "placeholder": "CUSTOMER_",
            },
            {
                "name": "download_mode",
                "label": "Download Mode",
                "type": "select",
                "required": False,
                "default": "single",
                "show_if": {"action": "download"},
                "options": [
                    {"value": "single", "label": "One File"},
                    {"value": "multiple", "label": "Multiple Files (Prefix)"},
                ],
            },
            {
                "name": "remote_file_name",
                "label": "Remote File Name",
                "type": "text",
                "required": False,
                "show_if_all": [{"action": "download"}, {"download_mode": "single"}],
                "placeholder": "example.csv",
            },
            {
                "name": "remote_file_prefix",
                "label": "Remote File Prefix",
                "type": "text",
                "required": False,
                "show_if_all": [{"action": "download"}, {"download_mode": "multiple"}],
                "placeholder": "CUSTOMER_",
            },
            {
                "name": "local_dir_download",
                "label": "Local Download Directory",
                "type": "text",
                "required": False,
                "show_if": {"action": "download"},
                "default": "/opt/airflow/local_downloads/sftp",
            },
            {
                "name": "delete_mode",
                "label": "Delete Mode",
                "type": "select",
                "required": False,
                "default": "single",
                "show_if": {"action": "delete"},
                "options": [
                    {"value": "single", "label": "One File"},
                    {"value": "multiple", "label": "Multiple Files (Prefix)"},
                ],
            },
            {
                "name": "delete_file_name",
                "label": "Remote File Name",
                "type": "text",
                "required": False,
                "show_if_all": [{"action": "delete"}, {"delete_mode": "single"}],
                "placeholder": "example.csv",
            },
            {
                "name": "delete_prefix",
                "label": "Remote File Prefix",
                "type": "text",
                "required": False,
                "show_if_all": [{"action": "delete"}, {"delete_mode": "multiple"}],
                "placeholder": "CUSTOMER_",
            },
        ],
    },
    "sftp_sensor": {
        "type": "sftp_sensor",
        "display_name": "SFTP Sensor",
        "description": "Wait on remote prefix in SFTP directory.",
        "template": "module_sftp_sensor.py.j2",
        "required_fields": ["conn_name", "remote_dir", "sftp_prefix_sensor"],
        "fields": [
            {
                "name": "conn_name",
                "label": "SFTP Connection Name",
                "type": "text",
                "required": True,
                "default": "sftp",
                "placeholder": "sftp",
                "description": "Airflow connection ID to use for SFTP.",
            },
            {
                "name": "remote_dir",
                "label": "Remote Directory",
                "type": "text",
                "required": True,
            },
            {
                "name": "sftp_prefix_sensor",
                "label": "Remote Prefix",
                "type": "text",
                "required": True,
            },
        ],
    },
    "y2": {
        "type": "y2",
        "display_name": "Cegid Y2",
        "description": "Run Y2 scheduled tasks sequentially.",
        "template": "module_y2.py.j2",
        "required_fields": ["folder_id_value", "task_ids_value"],
        "fields": [
            {"name": "folder_id_value", "label": "Folder ID", "type": "text", "required": True},
            {
                "name": "task_ids_value",
                "label": "Task IDs (comma-separated)",
                "type": "list",
                "required": True,
            },
        ],
    },
    "delay": {
        "type": "delay",
        "display_name": "Delay",
        "description": "Pause flow for a fixed number of minutes.",
        "template": "module_delay.py.j2",
        "required_fields": ["delay_minutes"],
        "fields": [
            {
                "name": "delay_minutes",
                "label": "Delay Minutes",
                "type": "number",
                "required": True,
                "default": 5,
            },
        ],
    },
    "timeout_guard": {
        "type": "timeout_guard",
        "display_name": "Timeout Guard",
        "description": "Fail if downstream section exceeds timeout.",
        "template": "module_timeout_guard.py.j2",
        "required_fields": ["timeout_minutes"],
        "fields": [
            {
                "name": "timeout_minutes",
                "label": "Timeout Minutes",
                "type": "number",
                "required": True,
                "default": 30,
            },
        ],
    },
    "manual_approval": {
        "type": "manual_approval",
        "display_name": "Manual Approval",
        "description": "Gate flow until run conf contains approval key/value.",
        "template": "module_manual_approval.py.j2",
        "required_fields": ["approval_conf_key", "approval_expected_value"],
        "fields": [
            {
                "name": "approval_conf_key",
                "label": "Approval Conf Key",
                "type": "text",
                "required": True,
                "default": "manual_approved",
            },
            {
                "name": "approval_expected_value",
                "label": "Expected Value",
                "type": "text",
                "required": True,
                "default": "true",
            },
        ],
    },
    "if_else_router": {
        "type": "if_else_router",
        "display_name": "If/Else Router",
        "description": "Route to True branch or False branch from one condition.",
        "template": "module_if_else.py.j2",
        "required_fields": ["input_source", "match_type", "expected_value", "true_branch_node_id", "false_branch_node_id"],
        "fields": [
            {
                "name": "input_source",
                "label": "Input Source",
                "type": "text",
                "required": True,
                "placeholder": "xcom_node:node_3.detected_file_name",
            },
            {
                "name": "match_type",
                "label": "Match Type",
                "type": "select",
                "required": True,
                "default": "equals",
                "options": [
                    {"value": "starts_with", "label": "starts_with"},
                    {"value": "contains", "label": "contains"},
                    {"value": "equals", "label": "equals"},
                    {"value": "regex", "label": "regex"},
                ],
            },
            {
                "name": "expected_value",
                "label": "Expected Value",
                "type": "text",
                "required": True,
            },
            {
                "name": "true_branch_node_id",
                "label": "True Branch Target",
                "type": "branch_target_select",
                "required": True,
            },
            {
                "name": "false_branch_node_id",
                "label": "False Branch Target",
                "type": "branch_target_select",
                "required": True,
            },
        ],
    },
    "switch_router": {
        "type": "switch_router",
        "display_name": "Switch Router",
        "description": "Route to branch by exact value match with default branch fallback.",
        "template": "module_switch_router.py.j2",
        "required_fields": ["input_source", "cases", "default_branch_node_id"],
        "fields": [
            {
                "name": "input_source",
                "label": "Input Source",
                "type": "text",
                "required": True,
                "placeholder": "xcom_node:node_3.detected_file_name",
            },
            {
                "name": "cases",
                "label": "Switch Cases",
                "type": "router_rules",
                "required": True,
            },
            {
                "name": "default_branch_node_id",
                "label": "Default Branch Target",
                "type": "branch_target_select",
                "required": True,
            },
        ],
    },
    "retry_wrapper": {
        "type": "retry_wrapper",
        "display_name": "Retry Wrapper",
        "description": "Apply custom retry policy at this control point.",
        "template": "module_retry_wrapper.py.j2",
        "required_fields": ["retries", "retry_delay_minutes"],
        "fields": [
            {
                "name": "retries",
                "label": "Retries",
                "type": "number",
                "required": True,
                "default": 3,
            },
            {
                "name": "retry_delay_minutes",
                "label": "Retry Delay Minutes",
                "type": "number",
                "required": True,
                "default": 5,
            },
            {
                "name": "retry_exponential_backoff",
                "label": "Exponential Backoff",
                "type": "boolean",
                "required": False,
                "default": False,
            },
        ],
    },
    "try_catch": {
        "type": "try_catch",
        "display_name": "Try/Catch Router",
        "description": "Route to success or catch branch based on upstream task outcome.",
        "template": "module_try_catch.py.j2",
        "required_fields": ["success_branch_node_id", "catch_branch_node_id"],
        "fields": [
            {
                "name": "success_branch_node_id",
                "label": "Success Branch Target",
                "type": "branch_target_select",
                "required": True,
            },
            {
                "name": "catch_branch_node_id",
                "label": "Catch Branch Target",
                "type": "branch_target_select",
                "required": True,
            },
        ],
    },
    "quorum_join": {
        "type": "quorum_join",
        "display_name": "Quorum Join",
        "description": "Continue when minimum successful branches is met.",
        "template": "module_quorum_join.py.j2",
        "required_fields": ["min_success_count"],
        "fields": [
            {
                "name": "min_success_count",
                "label": "Minimum Success Branches",
                "type": "number",
                "required": True,
                "default": 1,
            },
        ],
    },
    "parallel_limit": {
        "type": "parallel_limit",
        "display_name": "Parallel Limit",
        "description": "Split flow into parallel branches with max branch count guard.",
        "template": "module_parallel_limit.py.j2",
        "required_fields": ["max_parallel_branches"],
        "fields": [
            {
                "name": "max_parallel_branches",
                "label": "Max Parallel Branches",
                "type": "number",
                "required": True,
                "default": 2,
            },
        ],
    },
    "semaphore_lock": {
        "type": "semaphore_lock",
        "display_name": "Semaphore Lock",
        "description": "Use Airflow pool and slots to limit concurrency for protected section.",
        "template": "module_semaphore_lock.py.j2",
        "required_fields": ["pool_name", "pool_slots"],
        "fields": [
            {
                "name": "pool_name",
                "label": "Pool Name",
                "type": "text",
                "required": True,
                "default": "orchestration_lock_pool",
            },
            {
                "name": "pool_slots",
                "label": "Pool Slots",
                "type": "number",
                "required": True,
                "default": 1,
            },
        ],
    },
    "parallel_fork": {
        "type": "parallel_fork",
        "display_name": "Parallel Fork",
        "description": "Split one flow into multiple parallel branches.",
        "template": "module_parallel_fork.py.j2",
        "required_fields": [],
        "fields": [],
    },
    "parallel_join": {
        "type": "parallel_join",
        "display_name": "Parallel Join",
        "description": "Synchronize parallel branches before continuing.",
        "template": "module_parallel_join.py.j2",
        "required_fields": [],
        "fields": [
            {
                "name": "trigger_rule",
                "label": "Trigger Rule",
                "type": "select",
                "required": False,
                "default": "all_success",
                "options": [
                    {"value": "all_success", "label": "All Success"},
                    {"value": "none_failed_min_one_success", "label": "None Failed, Min One Success"},
                    {"value": "all_done", "label": "All Done"},
                ],
            }
        ],
    },
    "conditional_router": {
        "type": "conditional_router",
        "display_name": "Conditional Router",
        "description": "Branch to exactly one downstream path from rules.",
        "template": "module_conditional_router.py.j2",
        "required_fields": ["input_source", "rules"],
        "fields": [
            {
                "name": "input_source",
                "label": "Input Source",
                "type": "text",
                "required": True,
                "placeholder": "xcom_node:node_3.detected_file_name",
                "description": (
                    "Use xcom_node:<node_id>.<key> (recommended), xcom:<task_id>.<key>, "
                    "or a dag_run.conf key."
                ),
            },
            {
                "name": "rules",
                "label": "Routing Rules",
                "type": "router_rules",
                "required": True,
            },
        ],
    },
}


def list_modules() -> list[dict[str, Any]]:
    return [copy.deepcopy(item) for item in MODULE_REGISTRY.values()]


def get_module_schema(module_type: str) -> dict[str, Any] | None:
    value = MODULE_REGISTRY.get(module_type.lower())
    if not value:
        return None
    return copy.deepcopy(value)


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        if not value.strip():
            return []
        return [part.strip() for part in value.split(",") if part.strip()]
    return [str(value).strip()]


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None or value == "":
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_json_maybe(value: Any, field_name: str, errors: list[str]) -> Any:
    if value is None or value == "":
        return None
    if isinstance(value, (dict, list)):
        return value
    if not isinstance(value, str):
        errors.append(f"Field '{field_name}' must be a JSON object/array or JSON string.")
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        errors.append(f"Field '{field_name}' must contain valid JSON.")
        return None


def _as_rule_list(value: Any, errors: list[str], field_name: str = "rules") -> list[dict[str, str]]:
    if value in (None, ""):
        return []

    raw = value
    if isinstance(value, str):
        try:
            raw = json.loads(value)
        except json.JSONDecodeError:
            errors.append(f"Field '{field_name}' must contain valid JSON array of rules.")
            return []

    if not isinstance(raw, list):
        errors.append(f"Field '{field_name}' must be a list.")
        return []

    normalized: list[dict[str, str]] = []
    for idx, item in enumerate(raw):
        if not isinstance(item, dict):
            errors.append(f"Field '{field_name}[{idx}]' must be an object.")
            continue
        normalized.append(
            {
                "match_type": str(item.get("match_type", "")).strip().lower(),
                "value": str(item.get("value", "")).strip(),
                "target_branch_node_id": str(item.get("target_branch_node_id", "")).strip(),
            }
        )
    return normalized


def normalize_module_config(module_type: str, config: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    module_type = module_type.lower()
    schema = MODULE_REGISTRY.get(module_type)
    if not schema:
        return config, [f"Unknown module type '{module_type}'."]

    errors: list[str] = []
    normalized = copy.deepcopy(config or {})

    for field in schema.get("fields", []):
        name = field["name"]
        field_type = field.get("type", "text")
        if name not in normalized and "default" in field:
            normalized[name] = field["default"]

        value = normalized.get(name)

        if field_type == "number":
            if value in (None, ""):
                continue
            try:
                normalized[name] = int(value)
            except (TypeError, ValueError):
                errors.append(f"Field '{name}' must be an integer.")
        elif field_type in {"boolean", "radio_boolean"}:
            normalized[name] = _as_bool(value, default=bool(field.get("default", False)))
        elif field_type == "list":
            normalized[name] = _as_list(value)
        elif field_type == "dynamic_project_ids":
            normalized[name] = _as_list(value)
        elif field_type in {"link_list", "file_multiple"}:
            normalized[name] = _as_list(value)
        elif field_type == "json":
            parsed = _parse_json_maybe(value, name, errors)
            if parsed is not None:
                normalized[name] = parsed
            else:
                normalized[name] = None
        elif field_type == "router_rules":
            normalized[name] = _as_rule_list(value, errors, field_name=name)
        elif field_type == "branch_target_select":
            normalized[name] = str(value or "").strip()
        elif value is not None:
            normalized[name] = str(value).strip()

    if module_type in {"dataform", "cloudrun", "gcs"}:
        normalized["gcp_connection_id"] = str(normalized.get("gcp_connection_id") or "").strip()

    if module_type == "dataform":
        mode = str(normalized.get("project_mode", "single") or "single").strip()
        normalized["project_mode"] = mode
        if mode == "multiple":
            project_ids = _as_list(normalized.get("project_ids", []))
            normalized["project_ids"] = project_ids
            normalized["project_id"] = normalized.get("project_id") or (project_ids[0] if project_ids else "")
        else:
            normalized["project_ids"] = _as_list(normalized.get("project_ids", []))
    elif module_type == "azure":
        normalized["wasb_conn_id"] = str(normalized.get("wasb_conn_id") or "wasb_default").strip() or "wasb_default"
        action = str(normalized.get("action") or "").strip().lower()
        if action not in {"upload", "download", "delete"}:
            has_upload_markers = bool(normalized.get("local_file_path") or normalized.get("blob_name"))
            has_download_markers = bool(
                normalized.get("local_dir_download") or normalized.get("file_prefix") or normalized.get("file_name")
            )
            has_delete_markers = bool(normalized.get("delete_blob_name") or normalized.get("delete_prefix"))
            if has_delete_markers and not has_upload_markers and not has_download_markers:
                normalized["action"] = "delete"
            else:
                normalized["action"] = "download" if (has_download_markers and not has_upload_markers) else "upload"
        if "create_container" not in normalized:
            normalized["create_container"] = True
        if normalized.get("action") == "upload":
            upload_mode = str(normalized.get("upload_mode") or "").strip().lower()
            if not upload_mode:
                upload_mode = "multiple" if normalized.get("local_file_paths") else "single"
            normalized["upload_mode"] = upload_mode if upload_mode in {"single", "multiple"} else "single"
            normalized["local_file_paths"] = _as_list(normalized.get("local_file_paths", []))
            local_file_path_container = str(normalized.get("local_file_path_container") or "").strip()
            if local_file_path_container:
                normalized["local_file_path_container"] = local_file_path_container
            browser_single = str(normalized.get("local_file_browser") or "").strip()
            if browser_single and not normalized.get("local_file_path_container"):
                normalized["local_file_path_container"] = browser_single
            local_file_path = str(normalized.get("local_file_path") or "").strip()
            if not local_file_path and normalized.get("local_file_path_container"):
                local_file_path = re.split(r"[\\/]", str(normalized.get("local_file_path_container")))[-1]
            elif local_file_path and not normalized.get("local_file_path_container") and (
                "/" in local_file_path or "\\" in local_file_path
            ):
                normalized["local_file_path_container"] = local_file_path
                local_file_path = re.split(r"[\\/]", local_file_path)[-1]
            elif (
                local_file_path
                and normalized.get("local_file_path_container")
                and local_file_path == str(normalized.get("local_file_path_container"))
            ):
                local_file_path = re.split(r"[\\/]", local_file_path)[-1]
            normalized["local_file_path"] = local_file_path
            if normalized.get("blob_prefix"):
                normalized["blob_prefix"] = str(normalized.get("blob_prefix")).lstrip("/")
            if normalized.get("blob_name"):
                normalized["blob_name"] = str(normalized.get("blob_name")).lstrip("/")
        if normalized.get("action") == "download":
            raw_mode = str(normalized.get("download_mode") or "").strip().lower()
            if not raw_mode:
                raw_mode = "multiple" if (normalized.get("file_prefix") and not normalized.get("file_name")) else "single"
            mode = raw_mode
            normalized["download_mode"] = mode if mode in {"single", "multiple"} else "single"
            local_dir_download = str(normalized.get("local_dir_download") or "").strip()
            if not local_dir_download:
                normalized["local_dir_download"] = "/opt/airflow/local_downloads/azure"
            if normalized.get("file_prefix"):
                normalized["file_prefix"] = str(normalized.get("file_prefix")).lstrip("/")
            if normalized.get("file_name"):
                normalized["file_name"] = str(normalized.get("file_name")).lstrip("/")
        if normalized.get("action") == "delete":
            delete_mode = str(normalized.get("delete_mode") or "").strip().lower()
            if not delete_mode:
                delete_mode = "multiple" if normalized.get("delete_prefix") else "single"
            normalized["delete_mode"] = delete_mode if delete_mode in {"single", "multiple"} else "single"
            if normalized.get("delete_blob_name"):
                normalized["delete_blob_name"] = str(normalized.get("delete_blob_name")).lstrip("/")
            if normalized.get("delete_prefix"):
                normalized["delete_prefix"] = str(normalized.get("delete_prefix")).lstrip("/")
    elif module_type == "gcs":
        action = str(normalized.get("action") or "upload").strip().lower()
        normalized["action"] = action if action in {"upload", "move", "delete"} else "upload"

        upload_mode = str(normalized.get("upload_mode") or "single").strip().lower()
        normalized["upload_mode"] = upload_mode if upload_mode in {"single", "multiple"} else "single"

        source_type = str(normalized.get("source_type") or "local").strip().lower()
        normalized["source_type"] = source_type if source_type in {"local", "drive"} else "local"
        multiple_source_type = str(normalized.get("multiple_source_type") or "").strip().lower()
        if not multiple_source_type:
            # Backward compatibility: reuse source_type if dedicated multiple selector is absent.
            multiple_source_type = normalized["source_type"]
        normalized["multiple_source_type"] = (
            multiple_source_type if multiple_source_type in {"local", "drive"} else "local"
        )

        delete_mode = str(normalized.get("delete_mode") or "single").strip().lower()
        normalized["delete_mode"] = delete_mode if delete_mode in {"single", "multiple"} else "single"

        drive_input_mode = str(normalized.get("drive_input_mode") or "directory").strip().lower()
        normalized["drive_input_mode"] = drive_input_mode if drive_input_mode in {"directory", "links"} else "directory"

        if normalized.get("local_file_path"):
            normalized["local_file_path"] = str(normalized.get("local_file_path")).strip()
        if normalized.get("local_directory_path"):
            normalized["local_directory_path"] = str(normalized.get("local_directory_path")).strip()

        browser_single = str(normalized.get("local_file_browser") or "").strip()
        if browser_single and not normalized.get("local_file_path"):
            normalized["local_file_path"] = browser_single

        normalized["local_file_paths"] = _as_list(normalized.get("local_file_paths", []))
        normalized["drive_file_links"] = _as_list(normalized.get("drive_file_links", []))
        normalized["object_names"] = _as_list(normalized.get("object_names", []))

        for key in {
            "source_object",
            "destination_object",
            "destination_bucket_name",
            "object_name",
            "drive_file_link",
            "drive_directory_link",
            "destination_path",
            "delete_object_prefix",
        }:
            if key in normalized:
                normalized[key] = str(normalized.get(key) or "").strip()

        # Backward compatibility: migrate old "multiple delete from local files" payloads
        # to direct object names when possible.
        if (
            normalized.get("action") == "delete"
            and normalized.get("delete_mode") == "multiple"
            and not normalized.get("object_names")
        ):
            legacy_delete_local_file_paths = _as_list(normalized.get("delete_local_file_paths", []))
            normalized["object_names"] = [
                str(item).replace("\\", "/").split("/")[-1].strip()
                for item in legacy_delete_local_file_paths
                if str(item).strip()
            ]
    elif module_type == "azure_blob_sensor":
        if normalized.get("prefix"):
            normalized["prefix"] = str(normalized.get("prefix")).lstrip("/")
    elif module_type == "sftp_upload":
        normalized["conn_name"] = str(normalized.get("conn_name") or "sftp").strip() or "sftp"
        action = str(normalized.get("action") or "upload").strip().lower()
        normalized["action"] = action if action in {"upload", "download", "delete"} else "upload"

        raw_mode = str(normalized.get("upload_mode") or "").strip().lower()
        if not raw_mode:
            raw_mode = "multiple" if normalized.get("file_prefix") else "single"
        normalized["upload_mode"] = raw_mode if raw_mode in {"single", "multiple"} else "single"

        download_mode = str(normalized.get("download_mode") or "single").strip().lower()
        normalized["download_mode"] = download_mode if download_mode in {"single", "multiple"} else "single"
        delete_mode = str(normalized.get("delete_mode") or "single").strip().lower()
        normalized["delete_mode"] = delete_mode if delete_mode in {"single", "multiple"} else "single"

        if normalized.get("local_file_path") is not None:
            normalized["local_file_path"] = str(normalized.get("local_file_path") or "").strip()
        browser_single = str(normalized.get("local_file_browser") or "").strip()
        if browser_single and not normalized.get("local_file_path"):
            normalized["local_file_path"] = browser_single
        normalized["local_file_paths"] = _as_list(normalized.get("local_file_paths", []))
        if normalized.get("local_directory_path") is not None:
            normalized["local_directory_path"] = str(normalized.get("local_directory_path") or "").strip()
        if normalized.get("file_prefix") is not None:
            normalized["file_prefix"] = str(normalized.get("file_prefix") or "").strip()
        if normalized.get("remote_file_name") is not None:
            normalized["remote_file_name"] = str(normalized.get("remote_file_name") or "").strip().lstrip("/")
        if normalized.get("remote_file_prefix") is not None:
            normalized["remote_file_prefix"] = str(normalized.get("remote_file_prefix") or "").strip().lstrip("/")
        if normalized.get("delete_file_name") is not None:
            normalized["delete_file_name"] = str(normalized.get("delete_file_name") or "").strip().lstrip("/")
        if normalized.get("delete_prefix") is not None:
            normalized["delete_prefix"] = str(normalized.get("delete_prefix") or "").strip().lstrip("/")
        if normalized.get("local_dir_download") is not None:
            normalized["local_dir_download"] = str(normalized.get("local_dir_download") or "").strip()
        if not normalized.get("local_dir_download"):
            normalized["local_dir_download"] = "/opt/airflow/local_downloads/sftp"
    elif module_type == "sftp_sensor":
        normalized["conn_name"] = str(normalized.get("conn_name") or "sftp").strip() or "sftp"
    elif module_type == "parallel_join":
        normalized["trigger_rule"] = str(normalized.get("trigger_rule") or "all_success").strip().lower()
    elif module_type == "conditional_router":
        normalized["input_source"] = str(normalized.get("input_source") or "").strip()
        normalized["rules"] = _as_rule_list(normalized.get("rules"), errors, field_name="rules")
    elif module_type == "if_else_router":
        normalized["input_source"] = str(normalized.get("input_source") or "").strip()
        normalized["match_type"] = str(normalized.get("match_type") or "equals").strip().lower()
        normalized["expected_value"] = str(normalized.get("expected_value") or "").strip()
        normalized["true_branch_node_id"] = str(normalized.get("true_branch_node_id") or "").strip()
        normalized["false_branch_node_id"] = str(normalized.get("false_branch_node_id") or "").strip()
    elif module_type == "switch_router":
        normalized["input_source"] = str(normalized.get("input_source") or "").strip()
        normalized["cases"] = _as_rule_list(normalized.get("cases"), errors, field_name="cases")
        # Switch cases use exact value matching.
        for case_item in normalized["cases"]:
            case_item["match_type"] = "equals"
        normalized["default_branch_node_id"] = str(normalized.get("default_branch_node_id") or "").strip()
    elif module_type == "try_catch":
        normalized["success_branch_node_id"] = str(normalized.get("success_branch_node_id") or "").strip()
        normalized["catch_branch_node_id"] = str(normalized.get("catch_branch_node_id") or "").strip()

    return normalized, errors


def validate_module_config(module_type: str, config: dict[str, Any]) -> list[str]:
    schema = MODULE_REGISTRY.get(module_type.lower())
    if not schema:
        return [f"Unknown module type '{module_type}'."]

    errors: list[str] = []

    for field_name in schema.get("required_fields", []):
        value = config.get(field_name)
        if value is None or value == "" or value == []:
            errors.append(f"Missing required field '{field_name}'.")

    if module_type == "n8n":
        method = str(config.get("http_method", "GET")).upper()
        if method not in {"GET", "POST"}:
            errors.append("n8n.http_method must be GET or POST.")
        monitor_poll_interval_seconds = config.get("monitor_poll_interval_seconds", 5)
        monitor_timeout_seconds = config.get("monitor_timeout_seconds", 300)
        if not isinstance(monitor_poll_interval_seconds, int) or monitor_poll_interval_seconds <= 0:
            errors.append("n8n.monitor_poll_interval_seconds must be an integer > 0.")
        if not isinstance(monitor_timeout_seconds, int) or monitor_timeout_seconds <= 0:
            errors.append("n8n.monitor_timeout_seconds must be an integer > 0.")
    elif module_type == "dataform":
        if not str(config.get("gcp_connection_id") or "").strip():
            errors.append("dataform.gcp_connection_id is required.")
        mode = str(config.get("project_mode", "single"))
        if mode not in {"single", "multiple"}:
            errors.append("dataform.project_mode must be 'single' or 'multiple'.")
        if mode == "single":
            if not config.get("project_id"):
                errors.append("dataform.project_id is required when using one project.")
        if mode == "multiple":
            project_count = config.get("project_count")
            if not isinstance(project_count, int) or project_count < 2:
                errors.append("dataform.project_count must be an integer >= 2 for multiple projects.")
            project_ids = config.get("project_ids")
            if not isinstance(project_ids, list) or len(project_ids) == 0:
                errors.append("dataform.project_ids must be provided for multiple projects.")
            elif isinstance(project_count, int) and len(project_ids) != project_count:
                errors.append("dataform.project_ids count must match dataform.project_count.")
            if project_ids and not config.get("project_id"):
                errors.append("dataform.project_id is required (v1 uses first project id).")
        if config.get("dataform_tags") and not isinstance(config.get("dataform_tags"), list):
            errors.append("dataform.dataform_tags must be a list.")
        if config.get("project_ids") and not isinstance(config.get("project_ids"), list):
            errors.append("dataform.project_ids must be a list.")
    elif module_type == "cloudrun":
        if not str(config.get("gcp_connection_id") or "").strip():
            errors.append("cloudrun.gcp_connection_id is required.")
        if not isinstance(config.get("payload"), (dict, list)):
            errors.append("cloudrun.payload must be a valid JSON object or array.")
    elif module_type == "powerbi":
        monitor_poll_interval_seconds = config.get("monitor_poll_interval_seconds", 10)
        monitor_timeout_seconds = config.get("monitor_timeout_seconds", 900)
        if not isinstance(monitor_poll_interval_seconds, int) or monitor_poll_interval_seconds <= 0:
            errors.append("powerbi.monitor_poll_interval_seconds must be an integer > 0.")
        if not isinstance(monitor_timeout_seconds, int) or monitor_timeout_seconds <= 0:
            errors.append("powerbi.monitor_timeout_seconds must be an integer > 0.")
    elif module_type == "azure":
        if not str(config.get("wasb_conn_id") or "").strip():
            errors.append("azure.wasb_conn_id is required.")
        action = config.get("action")
        if action not in {"upload", "download", "delete"}:
            errors.append("azure.action must be 'upload', 'download', or 'delete'.")
        if action == "upload":
            upload_mode = str(config.get("upload_mode", "single")).strip().lower()
            if upload_mode not in {"single", "multiple"}:
                errors.append("azure.upload_mode must be 'single' or 'multiple'.")
            elif upload_mode == "single":
                local_single_path = str(config.get("local_file_path_container") or config.get("local_file_path") or "").strip()
                if not local_single_path:
                    errors.append("azure.local_file_path_container (or azure.local_file_path) is required for single upload.")
                if not config.get("blob_name"):
                    errors.append("azure.blob_name is required for single upload.")
            else:
                if not isinstance(config.get("local_file_paths"), list) or len(config.get("local_file_paths")) == 0:
                    errors.append("azure.local_file_paths is required for multiple upload.")
        if action == "download":
            if not config.get("local_dir_download"):
                errors.append("azure.local_dir_download is required for download.")
            mode = str(config.get("download_mode", "single")).strip().lower()
            if mode not in {"single", "multiple"}:
                errors.append("azure.download_mode must be 'single' or 'multiple'.")
            elif mode == "single" and not config.get("file_name"):
                errors.append("azure.file_name is required when download_mode is single.")
            elif mode == "multiple" and not config.get("file_prefix"):
                errors.append("azure.file_prefix is required when download_mode is multiple.")
        if action == "delete":
            mode = str(config.get("delete_mode", "single")).strip().lower()
            if mode not in {"single", "multiple"}:
                errors.append("azure.delete_mode must be 'single' or 'multiple'.")
            elif mode == "single" and not config.get("delete_blob_name"):
                errors.append("azure.delete_blob_name is required when delete_mode is single.")
            elif mode == "multiple" and not config.get("delete_prefix"):
                errors.append("azure.delete_prefix is required when delete_mode is multiple.")
    elif module_type == "gcs":
        if not str(config.get("gcp_connection_id") or "").strip():
            errors.append("gcs.gcp_connection_id is required.")
        action = str(config.get("action") or "").strip().lower()
        if action not in {"upload", "move", "delete"}:
            errors.append("gcs.action must be 'upload', 'move', or 'delete'.")

        if not str(config.get("bucket_name") or "").strip():
            errors.append("gcs.bucket_name is required.")

        if action == "upload":
            upload_mode = str(config.get("upload_mode", "single")).strip().lower()
            if upload_mode not in {"single", "multiple"}:
                errors.append("gcs.upload_mode must be 'single' or 'multiple'.")
            if upload_mode == "single":
                source_type = str(config.get("source_type", "local")).strip().lower()
                if source_type not in {"local", "drive"}:
                    errors.append("gcs.source_type must be 'local' or 'drive'.")
            else:
                source_type = str(config.get("multiple_source_type", "local")).strip().lower()
                if source_type not in {"local", "drive"}:
                    errors.append("gcs.multiple_source_type must be 'local' or 'drive'.")

            if source_type == "local":
                if upload_mode == "single":
                    if not str(config.get("local_file_path") or "").strip():
                        errors.append("gcs.local_file_path is required for single local upload.")
                else:
                    local_dir = str(config.get("local_directory_path") or "").strip()
                    local_files = config.get("local_file_paths")
                    if not local_dir and (not isinstance(local_files, list) or len(local_files) == 0):
                        errors.append(
                            "gcs.local_directory_path or gcs.local_file_paths is required for multiple local upload."
                        )
            else:
                if upload_mode == "single":
                    if not str(config.get("drive_file_link") or "").strip():
                        errors.append("gcs.drive_file_link is required for single Drive upload.")
                else:
                    drive_input_mode = str(config.get("drive_input_mode", "directory")).strip().lower()
                    if drive_input_mode not in {"directory", "links"}:
                        errors.append("gcs.drive_input_mode must be 'directory' or 'links'.")
                    elif drive_input_mode == "directory":
                        if not str(config.get("drive_directory_link") or "").strip():
                            errors.append("gcs.drive_directory_link is required for Drive directory upload.")
                    elif not isinstance(config.get("drive_file_links"), list) or not config.get("drive_file_links"):
                        errors.append("gcs.drive_file_links is required when Drive input mode is links.")

        elif action == "move":
            if not str(config.get("source_object") or "").strip():
                errors.append("gcs.source_object is required for move.")
            if not str(config.get("destination_bucket_name") or "").strip():
                errors.append("gcs.destination_bucket_name is required for move.")
            if not str(config.get("destination_object") or "").strip():
                errors.append("gcs.destination_object is required for move.")
        elif action == "delete":
            delete_mode = str(config.get("delete_mode", "single")).strip().lower()
            if delete_mode not in {"single", "multiple"}:
                errors.append("gcs.delete_mode must be 'single' or 'multiple'.")
            elif delete_mode == "single":
                if not str(config.get("object_name") or "").strip():
                    errors.append("gcs.object_name is required when delete_mode is single.")
            else:
                object_names = config.get("object_names")
                has_object_names = isinstance(object_names, list) and any(str(item).strip() for item in object_names)
                has_prefix = bool(str(config.get("delete_object_prefix") or "").strip())
                if not has_object_names and not has_prefix:
                    errors.append(
                        "gcs.object_names or gcs.delete_object_prefix is required when delete_mode is multiple."
                    )
    elif module_type == "azure_blob_sensor":
        poke_interval = config.get("poke_interval")
        timeout = config.get("timeout")
        if not isinstance(poke_interval, int) or poke_interval <= 0:
            errors.append("azure_blob_sensor.poke_interval must be an integer > 0.")
        if not isinstance(timeout, int) or timeout <= 0:
            errors.append("azure_blob_sensor.timeout must be an integer > 0.")
    elif module_type == "sftp_upload":
        if not str(config.get("conn_name") or "").strip():
            errors.append("sftp_upload.conn_name is required.")
        if not config.get("remote_dir"):
            errors.append("sftp_upload.remote_dir is required.")
        action = str(config.get("action") or "upload").strip().lower()
        if action not in {"upload", "download", "delete"}:
            errors.append("sftp_upload.action must be 'upload', 'download', or 'delete'.")
        elif action == "upload":
            upload_mode = str(config.get("upload_mode", "single")).strip().lower()
            if upload_mode not in {"single", "multiple"}:
                errors.append("sftp_upload.upload_mode must be 'single' or 'multiple'.")
            elif upload_mode == "single":
                if not str(config.get("local_file_path") or "").strip():
                    errors.append("sftp_upload.local_file_path is required in single upload mode.")
            else:
                has_explicit_files = isinstance(config.get("local_file_paths"), list) and any(
                    str(item).strip() for item in (config.get("local_file_paths") or [])
                )
                has_directory = bool(str(config.get("local_directory_path") or "").strip())
                has_local_path = bool(str(config.get("local_file_path") or "").strip())
                has_prefix = bool(str(config.get("file_prefix") or "").strip())
                if not has_explicit_files and not has_directory and not has_local_path:
                    errors.append(
                        "sftp_upload.local_file_paths, sftp_upload.local_directory_path, or sftp_upload.local_file_path is required in multiple upload mode."
                    )
                if not has_explicit_files and not has_prefix:
                    errors.append("sftp_upload.file_prefix is required in multiple upload mode when local_file_paths is not provided.")
        elif action == "download":
            mode = str(config.get("download_mode", "single")).strip().lower()
            if mode not in {"single", "multiple"}:
                errors.append("sftp_upload.download_mode must be 'single' or 'multiple'.")
            elif mode == "single" and not str(config.get("remote_file_name") or "").strip():
                errors.append("sftp_upload.remote_file_name is required in single download mode.")
            elif mode == "multiple" and not str(config.get("remote_file_prefix") or "").strip():
                errors.append("sftp_upload.remote_file_prefix is required in multiple download mode.")
            if not str(config.get("local_dir_download") or "").strip():
                errors.append("sftp_upload.local_dir_download is required for download.")
        elif action == "delete":
            mode = str(config.get("delete_mode", "single")).strip().lower()
            if mode not in {"single", "multiple"}:
                errors.append("sftp_upload.delete_mode must be 'single' or 'multiple'.")
            elif mode == "single" and not str(config.get("delete_file_name") or "").strip():
                errors.append("sftp_upload.delete_file_name is required in single delete mode.")
            elif mode == "multiple" and not str(config.get("delete_prefix") or "").strip():
                errors.append("sftp_upload.delete_prefix is required in multiple delete mode.")
    elif module_type == "sftp_sensor":
        if not str(config.get("conn_name") or "").strip():
            errors.append("sftp_sensor.conn_name is required.")
        if not config.get("sftp_prefix_sensor"):
            errors.append("sftp_sensor.sftp_prefix_sensor is required.")
        if not config.get("remote_dir"):
            errors.append("sftp_sensor.remote_dir is required.")
    elif module_type == "y2":
        if not isinstance(config.get("task_ids_value"), list) or not config.get("task_ids_value"):
            errors.append("y2.task_ids_value must be a non-empty list.")
    elif module_type == "parallel_join":
        valid_rules = {"all_success", "none_failed_min_one_success", "all_done"}
        trigger_rule = str(config.get("trigger_rule") or "all_success").strip().lower()
        if trigger_rule not in valid_rules:
            errors.append(f"parallel_join.trigger_rule must be one of {sorted(valid_rules)}.")
    elif module_type == "conditional_router":
        input_source = str(config.get("input_source") or "").strip()
        if not input_source:
            errors.append("conditional_router.input_source is required.")
        rules = config.get("rules")
        if not isinstance(rules, list) or not rules:
            errors.append("conditional_router.rules must be a non-empty list.")
        else:
            for idx, rule in enumerate(rules):
                if not isinstance(rule, dict):
                    errors.append(f"conditional_router.rules[{idx}] must be an object.")
                    continue
                match_type = str(rule.get("match_type", "")).strip().lower()
                value = str(rule.get("value", "")).strip()
                target = str(rule.get("target_branch_node_id", "")).strip()
                if match_type not in ROUTER_MATCH_TYPES:
                    errors.append(
                        f"conditional_router.rules[{idx}].match_type must be one of {sorted(ROUTER_MATCH_TYPES)}."
                    )
                if not value:
                    errors.append(f"conditional_router.rules[{idx}].value is required.")
                elif match_type == "regex":
                    try:
                        re.compile(value)
                    except re.error as exc:
                        errors.append(f"conditional_router.rules[{idx}].value invalid regex: {exc}.")
                if not target:
                    errors.append(f"conditional_router.rules[{idx}].target_branch_node_id is required.")
    elif module_type == "if_else_router":
        input_source = str(config.get("input_source") or "").strip()
        if not input_source:
            errors.append("if_else_router.input_source is required.")
        match_type = str(config.get("match_type", "")).strip().lower()
        if match_type not in ROUTER_MATCH_TYPES:
            errors.append(f"if_else_router.match_type must be one of {sorted(ROUTER_MATCH_TYPES)}.")
        expected_value = str(config.get("expected_value") or "").strip()
        if not expected_value:
            errors.append("if_else_router.expected_value is required.")
        elif match_type == "regex":
            try:
                re.compile(expected_value)
            except re.error as exc:
                errors.append(f"if_else_router.expected_value invalid regex: {exc}.")
        if not str(config.get("true_branch_node_id") or "").strip():
            errors.append("if_else_router.true_branch_node_id is required.")
        if not str(config.get("false_branch_node_id") or "").strip():
            errors.append("if_else_router.false_branch_node_id is required.")
    elif module_type == "switch_router":
        input_source = str(config.get("input_source") or "").strip()
        if not input_source:
            errors.append("switch_router.input_source is required.")
        cases = config.get("cases")
        if not isinstance(cases, list) or not cases:
            errors.append("switch_router.cases must be a non-empty list.")
        else:
            for idx, case_item in enumerate(cases):
                if not isinstance(case_item, dict):
                    errors.append(f"switch_router.cases[{idx}] must be an object.")
                    continue
                value = str(case_item.get("value", "")).strip()
                target = str(case_item.get("target_branch_node_id", "")).strip()
                if not value:
                    errors.append(f"switch_router.cases[{idx}].value is required.")
                if not target:
                    errors.append(f"switch_router.cases[{idx}].target_branch_node_id is required.")
        if not str(config.get("default_branch_node_id") or "").strip():
            errors.append("switch_router.default_branch_node_id is required.")
    elif module_type == "delay":
        delay_minutes = config.get("delay_minutes")
        if not isinstance(delay_minutes, int) or delay_minutes <= 0:
            errors.append("delay.delay_minutes must be an integer > 0.")
    elif module_type == "timeout_guard":
        timeout_minutes = config.get("timeout_minutes")
        if not isinstance(timeout_minutes, int) or timeout_minutes <= 0:
            errors.append("timeout_guard.timeout_minutes must be an integer > 0.")
    elif module_type == "manual_approval":
        if not str(config.get("approval_conf_key") or "").strip():
            errors.append("manual_approval.approval_conf_key is required.")
        if not str(config.get("approval_expected_value") or "").strip():
            errors.append("manual_approval.approval_expected_value is required.")
    elif module_type == "retry_wrapper":
        retries = config.get("retries")
        retry_delay = config.get("retry_delay_minutes")
        if not isinstance(retries, int) or retries < 0:
            errors.append("retry_wrapper.retries must be an integer >= 0.")
        if not isinstance(retry_delay, int) or retry_delay < 0:
            errors.append("retry_wrapper.retry_delay_minutes must be an integer >= 0.")
    elif module_type == "try_catch":
        success_target = str(config.get("success_branch_node_id") or "").strip()
        catch_target = str(config.get("catch_branch_node_id") or "").strip()
        if not success_target:
            errors.append("try_catch.success_branch_node_id is required.")
        if not catch_target:
            errors.append("try_catch.catch_branch_node_id is required.")
        if success_target and catch_target and success_target == catch_target:
            errors.append("try_catch success and catch branch targets must be different.")
    elif module_type == "quorum_join":
        min_success_count = config.get("min_success_count")
        if not isinstance(min_success_count, int) or min_success_count <= 0:
            errors.append("quorum_join.min_success_count must be an integer > 0.")
    elif module_type == "parallel_limit":
        max_parallel = config.get("max_parallel_branches")
        if not isinstance(max_parallel, int) or max_parallel < 2:
            errors.append("parallel_limit.max_parallel_branches must be an integer >= 2.")
    elif module_type == "semaphore_lock":
        if not str(config.get("pool_name") or "").strip():
            errors.append("semaphore_lock.pool_name is required.")
        pool_slots = config.get("pool_slots")
        if not isinstance(pool_slots, int) or pool_slots <= 0:
            errors.append("semaphore_lock.pool_slots must be an integer > 0.")

    return errors


def normalize_pipeline_payload(raw_payload: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    errors: list[str] = []

    payload = copy.deepcopy(raw_payload or {})
    pipeline = payload.get("pipeline", {})
    nodes = payload.get("nodes", [])
    edges = payload.get("edges", [])
    group_boxes = payload.get("group_boxes", [])
    if not isinstance(group_boxes, list):
        group_boxes = payload.get("groupBoxes", [])
    if not isinstance(group_boxes, list):
        group_boxes = []

    normalized_pipeline = {
        "dag_id": str(pipeline.get("dag_id", "")).strip(),
        "description": str(pipeline.get("description", "")).strip(),
        "start_date": str(pipeline.get("start_date", "2026-01-01")).strip() or "2026-01-01",
        "schedule": pipeline.get("schedule"),
        "schedule_mode": str(pipeline.get("schedule_mode", "")).strip().lower(),
        "catchup": _as_bool(pipeline.get("catchup"), default=False),
        "schedule_preset": str(pipeline.get("schedule_preset", "")).strip().lower(),
        "schedule_cron": str(pipeline.get("schedule_cron", "")).strip(),
        "interval_value": pipeline.get("interval_value"),
        "interval_unit": str(pipeline.get("interval_unit", "minutes")).strip().lower(),
        "event_source": str(pipeline.get("event_source", "")).strip(),
        "custom_schedule_text": str(pipeline.get("custom_schedule_text", "")).strip(),
        "retries": pipeline.get("retries", 0),
        "retry_delay_minutes": pipeline.get("retry_delay_minutes", 0),
        "alert_emails": _as_list(pipeline.get("alert_emails", [])),
        "alert_mode": str(pipeline.get("alert_mode", "both")).strip() or "both",
        "tags": _as_list(pipeline.get("tags", [])),
        "output_filename": str(pipeline.get("output_filename", "")).strip(),
    }

    if not normalized_pipeline["output_filename"] and normalized_pipeline["dag_id"]:
        normalized_pipeline["output_filename"] = f"{normalized_pipeline['dag_id']}.py"

    try:
        normalized_pipeline["retries"] = int(normalized_pipeline["retries"])
    except (TypeError, ValueError):
        errors.append("pipeline.retries must be an integer.")

    try:
        normalized_pipeline["retry_delay_minutes"] = int(normalized_pipeline["retry_delay_minutes"])
    except (TypeError, ValueError):
        errors.append("pipeline.retry_delay_minutes must be an integer.")

    # Keep legacy schedule compatibility by inferring a mode when old payloads are loaded.
    if not normalized_pipeline["schedule_mode"]:
        legacy_schedule = normalized_pipeline.get("schedule")
        if isinstance(legacy_schedule, str) and legacy_schedule.strip():
            legacy_schedule = legacy_schedule.strip()
            if legacy_schedule in SCHEDULE_PRESETS_REVERSE:
                normalized_pipeline["schedule_mode"] = "preset"
                normalized_pipeline["schedule_preset"] = SCHEDULE_PRESETS_REVERSE[legacy_schedule]
            else:
                normalized_pipeline["schedule_mode"] = "cron"
                normalized_pipeline["schedule_cron"] = legacy_schedule
        else:
            normalized_pipeline["schedule_mode"] = "manual"

    # Normalize interval numeric value when possible.
    interval_value = normalized_pipeline.get("interval_value")
    if interval_value not in (None, ""):
        try:
            normalized_pipeline["interval_value"] = int(interval_value)
        except (TypeError, ValueError):
            errors.append("pipeline.interval_value must be an integer.")

    # Resolve final Airflow schedule string from selected mode.
    schedule_mode = normalized_pipeline.get("schedule_mode")
    if schedule_mode == "manual":
        normalized_pipeline["schedule"] = None
    elif schedule_mode == "preset":
        normalized_pipeline["schedule"] = SCHEDULE_PRESETS.get(normalized_pipeline.get("schedule_preset"))
    elif schedule_mode == "cron":
        normalized_pipeline["schedule"] = normalized_pipeline.get("schedule_cron") or None
    elif schedule_mode == "interval":
        interval = normalized_pipeline.get("interval_value")
        unit = normalized_pipeline.get("interval_unit")
        if isinstance(interval, int) and interval > 0:
            if unit == "minutes":
                normalized_pipeline["schedule"] = f"*/{interval} * * * *"
            elif unit == "hours":
                normalized_pipeline["schedule"] = f"0 */{interval} * * *"
            elif unit == "days":
                normalized_pipeline["schedule"] = f"0 0 */{interval} * *"
            else:
                normalized_pipeline["schedule"] = None
        else:
            normalized_pipeline["schedule"] = None
    elif schedule_mode in {"event", "custom"}:
        # Event/dataset and custom timetable logic are stored as metadata in v1;
        # DAG schedule stays manual until advanced schedulers are added.
        normalized_pipeline["schedule"] = None
    elif not schedule_mode:
        normalized_pipeline["schedule"] = None

    normalized_nodes: list[dict[str, Any]] = []
    for idx, node in enumerate(nodes):
        config = dict(node.get("config", {}))
        raw_node_type = str(node.get("type", "")).strip().lower()
        node_type = raw_node_type

        # Backward compatibility: old saved pipelines used `type: sftp` + `action`.
        if raw_node_type == "sftp":
            action = str(config.get("action", "upload")).strip().lower()
            node_type = "sftp_sensor" if action == "sensor" else "sftp_upload"
            config.pop("action", None)

        normalized_config, config_errors = normalize_module_config(node_type, config)
        if config_errors:
            for error in config_errors:
                errors.append(f"nodes[{idx}] ({node_type}): {error}")

        normalized_nodes.append(
            {
                "id": str(node.get("id", "")).strip(),
                "type": node_type,
                "label": str(node.get("label", "")).strip() or node_type.upper(),
                "x": node.get("x"),
                "y": node.get("y"),
                "config": normalized_config,
            }
        )

    normalized_edges = [
        {
            "source": str(edge.get("source", "")).strip(),
            "target": str(edge.get("target", "")).strip(),
        }
        for edge in edges
    ]

    normalized_group_boxes = []
    for idx, box in enumerate(group_boxes):
        if not isinstance(box, dict):
            continue
        normalized_group_boxes.append(
            {
                "id": str(box.get("id", "")).strip() or f"group_box_{idx + 1}",
                "title": str(box.get("title", "")).strip() or f"Group {idx + 1}",
                "x": box.get("x"),
                "y": box.get("y"),
                "width": box.get("width"),
                "height": box.get("height"),
                "color": str(box.get("color", "")).strip() or "#2f6ea2",
            }
        )

    return {
        "pipeline": normalized_pipeline,
        "nodes": normalized_nodes,
        "edges": normalized_edges,
        "group_boxes": normalized_group_boxes,
    }, errors


def validate_pipeline_settings(pipeline: dict[str, Any]) -> list[str]:
    errors: list[str] = []

    dag_id = str(pipeline.get("dag_id", "")).strip()
    if not dag_id:
        errors.append("pipeline.dag_id is required.")
    elif not re.match(r"^[A-Za-z][A-Za-z0-9_]*$", dag_id):
        errors.append("pipeline.dag_id must start with a letter and contain only letters, numbers, and underscores.")

    filename = str(pipeline.get("output_filename", "")).strip()
    if not filename:
        errors.append("pipeline.output_filename is required.")
    elif not filename.endswith(".py"):
        errors.append("pipeline.output_filename must end with .py")
    elif "/" in filename or "\\" in filename:
        errors.append("pipeline.output_filename must be a file name only, not a path.")

    retries = pipeline.get("retries")
    retry_delay = pipeline.get("retry_delay_minutes")
    start_date = str(pipeline.get("start_date", "")).strip()
    schedule_mode = str(pipeline.get("schedule_mode", "")).strip().lower()
    catchup = pipeline.get("catchup")
    schedule_preset = str(pipeline.get("schedule_preset", "")).strip().lower()
    schedule_cron = str(pipeline.get("schedule_cron", "")).strip()
    interval_value = pipeline.get("interval_value")
    interval_unit = str(pipeline.get("interval_unit", "")).strip().lower()
    event_source = str(pipeline.get("event_source", "")).strip()
    custom_schedule_text = str(pipeline.get("custom_schedule_text", "")).strip()

    if not isinstance(retries, int) or retries < 0:
        errors.append("pipeline.retries must be an integer >= 0.")

    if not isinstance(retry_delay, int) or retry_delay < 0:
        errors.append("pipeline.retry_delay_minutes must be an integer >= 0.")

    if not start_date:
        errors.append("pipeline.start_date is required.")
    elif not re.match(r"^\d{4}-\d{2}-\d{2}$", start_date):
        errors.append("pipeline.start_date must be in YYYY-MM-DD format.")
    else:
        try:
            datetime.strptime(start_date, "%Y-%m-%d")
        except ValueError:
            errors.append("pipeline.start_date must be a valid calendar date.")

    if not isinstance(catchup, bool):
        errors.append("pipeline.catchup must be a boolean.")

    alert_mode = str(pipeline.get("alert_mode", "both")).strip()
    if alert_mode not in ALERT_MODES:
        errors.append(f"pipeline.alert_mode must be one of {sorted(ALERT_MODES)}.")

    # Scheduling mode validation (mode is required in the new UX).
    if schedule_mode not in SCHEDULE_MODES:
        errors.append(f"pipeline.schedule_mode must be one of {sorted(SCHEDULE_MODES)}.")
    else:
        if schedule_mode == "manual":
            pass
        elif schedule_mode == "preset":
            if schedule_preset not in SCHEDULE_PRESETS:
                errors.append(f"pipeline.schedule_preset must be one of {sorted(SCHEDULE_PRESETS)} when mode is preset.")
        elif schedule_mode == "cron":
            if not schedule_cron:
                errors.append("pipeline.schedule_cron is required when mode is cron.")
            else:
                parts = [p for p in schedule_cron.split(" ") if p]
                if len(parts) not in (5, 6):
                    errors.append("pipeline.schedule_cron must contain 5 or 6 cron parts.")
        elif schedule_mode == "interval":
            if not isinstance(interval_value, int) or interval_value <= 0:
                errors.append("pipeline.interval_value must be an integer > 0 when mode is interval.")
            if interval_unit not in INTERVAL_UNITS:
                errors.append(f"pipeline.interval_unit must be one of {sorted(INTERVAL_UNITS)} when mode is interval.")
        elif schedule_mode == "event":
            if not event_source:
                errors.append("pipeline.event_source is required when mode is event.")
        elif schedule_mode == "custom":
            if not custom_schedule_text:
                errors.append("pipeline.custom_schedule_text is required when mode is custom.")

    for email in pipeline.get("alert_emails", []):
        if not EMAIL_RE.match(email):
            errors.append(f"Invalid alert email format: {email}")

    return errors
