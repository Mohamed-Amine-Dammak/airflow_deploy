from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta, timezone
import requests
import time
from utils.etl_tasks import custom_failure_email
from airflow.models import Variable


default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": custom_failure_email(["your_email@example.com"]),
    "on_retry_callback": custom_failure_email(["your_email@example.com"]),
}


@dag(
    dag_id="n8n_workflow_template",
    start_date=datetime(2026, 2, 2),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["n8n", "template", "orchestration"],
)
def n8n_workflow_template():

    def _api_headers(api_key: str) -> dict:
        return {
            "X-N8N-API-KEY": api_key,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _normalize_path(path: str) -> str:
        return path.strip().strip("/")

    def _pause(label: str, seconds: int = 10):
        print(f"{label} -> sleeping for {seconds} seconds...")
        time.sleep(seconds)
        print(f"{label} -> resumed.")

    def _extract_webhook_paths(workflow_obj: dict) -> list[dict]:
        """
        Return all webhook nodes found in a workflow definition.
        Each item contains node_name and webhook_path.
        """
        nodes = workflow_obj.get("nodes", []) or workflow_obj.get("data", {}).get("nodes", [])
        results = []

        for node in nodes:
            node_type = str(node.get("type", ""))
            if "webhook" not in node_type.lower():
                continue

            params = node.get("parameters", {}) or {}
            path = params.get("path")

            if isinstance(path, str) and path.strip():
                results.append(
                    {
                        "node_name": node.get("name"),
                        "webhook_path": _normalize_path(path),
                    }
                )

        return results

    def _find_workflow_by_name(conn, workflow_name: str) -> dict:
        """
        Find workflow by exact name match.
        """
        base_url = conn.host.rstrip("/")
        headers = _api_headers(conn.password)

        url = f"{base_url}/api/v1/workflows"
        resp = requests.get(url, headers=headers, params={"limit": 250}, timeout=30)
        resp.raise_for_status()

        payload = resp.json()
        workflows = payload.get("data", []) if isinstance(payload, dict) else payload

        exact_matches = [
            wf for wf in workflows
            if str(wf.get("name", "")).strip() == str(workflow_name).strip()
        ]

        if not exact_matches:
            sample = [wf.get("name") for wf in workflows[:20]]
            raise AirflowException(
                f"No workflow found with name='{workflow_name}'. Sample names: {sample}"
            )

        if len(exact_matches) > 1:
            ids = [wf.get("id") for wf in exact_matches]
            raise AirflowException(
                f"Multiple workflows found with name='{workflow_name}'. Matching IDs: {ids}"
            )

        return exact_matches[0]

    def _get_workflow_details(conn, workflow_id: str) -> dict:
        base_url = conn.host.rstrip("/")
        headers = _api_headers(conn.password)

        url = f"{base_url}/api/v1/workflows/{workflow_id}"
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.json()

    @task
    def resolve_workflow(
        workflow_name: str,
        conn_id: str = "n8n_local",
        sleep_after: int = 10,
    ) -> dict:
        """
        Resolve workflow name -> workflow_id + webhook_path dynamically.
        Assumes one target workflow for this DAG template.
        """
        conn = BaseHook.get_connection(conn_id)

        workflow = _find_workflow_by_name(conn, workflow_name)
        workflow_id = str(workflow.get("id"))

        if not workflow_id:
            raise AirflowException(f"Workflow '{workflow_name}' found but no id returned")

        details = _get_workflow_details(conn, workflow_id)
        webhooks = _extract_webhook_paths(details)

        if not webhooks:
            raise AirflowException(
                f"Workflow '{workflow_name}' (id={workflow_id}) has no Webhook node with a path"
            )

        if len(webhooks) > 1:
            raise AirflowException(
                f"Workflow '{workflow_name}' (id={workflow_id}) has multiple webhook paths: "
                f"{webhooks}. Add selection logic for the correct webhook."
            )

        webhook_path = webhooks[0]["webhook_path"]

        print(
            f"Resolved workflow_name='{workflow_name}' -> "
            f"workflow_id='{workflow_id}', webhook_path='{webhook_path}'"
        )

        _pause(f"After resolve_workflow [{workflow_name}]", sleep_after)

        return {
            "workflow_name": workflow_name,
            "workflow_id": workflow_id,
            "webhook_path": webhook_path,
        }

    @task
    def trigger_n8n_workflow(
        resolved: dict,
        query_params: dict | None = None,
        request_body: dict | None = None,
        http_method: str = "GET",
        conn_id: str = "n8n_local",
        use_test_webhook: bool = True,
        sleep_after: int = 10,
    ) -> dict:
        """
        Trigger n8n using the webhook path resolved from the workflow name.

        Notes:
        - query_params is optional.
        - request_body is optional.
        - Not all workflows need a file.
        - If a workflow needs input.csv, pass it inside query_params or request_body.
        """
        conn = BaseHook.get_connection(conn_id)
        base_url = conn.host.rstrip("/")

        workflow_name = resolved["workflow_name"]
        workflow_id = resolved["workflow_id"]
        webhook_path = resolved["webhook_path"]

        prefix = "webhook-test" if use_test_webhook else "webhook"
        webhook_url = f"{base_url}/{prefix}/{webhook_path}"

        query_params = query_params or {}
        request_body = request_body or {}
        method = http_method.upper().strip()

        try:
            if method == "GET":
                response = requests.get(
                    webhook_url,
                    params=query_params,
                    timeout=30,
                )
            elif method == "POST":
                response = requests.post(
                    webhook_url,
                    params=query_params,
                    json=request_body,
                    timeout=30,
                )
            else:
                raise AirflowException(
                    f"Unsupported http_method='{http_method}'. Use GET or POST."
                )

            response.raise_for_status()

        except requests.exceptions.RequestException as e:
            raise AirflowException(
                f"Failed to trigger workflow_name='{workflow_name}' "
                f"via webhook_path='{webhook_path}': {e}"
            )

        print(
            f"Triggered workflow_name='{workflow_name}' "
            f"(workflow_id={workflow_id}) via webhook_path='{webhook_path}' "
            f"using method={method}"
        )

        result = {
            "workflow_name": workflow_name,
            "workflow_id": workflow_id,
            "webhook_path": webhook_path,
            "triggered_at": datetime.utcnow().isoformat(),
            "http_method": method,
            "http_status": response.status_code,
            "query_params": query_params,
            "request_body": request_body,
            "response_text": response.text,
        }

        _pause(f"After trigger_n8n_workflow [{workflow_name}]", sleep_after)

        return result

    @task
    def monitor_n8n_workflow(
        trigger_result: dict,
        poll_interval: int = 5,
        timeout: int = 300,
        max_attempts: int = 60,
        conn_id: str = "n8n_local",
        sleep_after: int = 10,
    ) -> str:
        """
        Monitor the execution that matches the current trigger,
        then print detailed execution/node logs.
        """
        workflow_id = trigger_result["workflow_id"]
        workflow_name = trigger_result.get("workflow_name", "N/A")
        webhook_path = trigger_result.get("webhook_path", "N/A")
        triggered_at = trigger_result["triggered_at"]

        conn = BaseHook.get_connection(conn_id)
        api_key = conn.password

        headers = {
            "X-N8N-API-KEY": api_key,
            "Accept": "application/json",
        }

        executions_url = (
            f"{conn.host.rstrip('/')}/api/v1/executions"
            f"?workflowId={workflow_id}"
            f"&limit=10"
            f"&includeData=true"
        )

        print(
            f"→ Monitoring workflow_name='{workflow_name}', "
            f"workflow_id='{workflow_id}', webhook_path='{webhook_path}', "
            f"triggered_at='{triggered_at}'"
        )

        trigger_dt = datetime.fromisoformat(triggered_at.replace("Z", "+00:00"))
        if trigger_dt.tzinfo is None:
            trigger_dt = trigger_dt.replace(tzinfo=timezone.utc)

        start_time = time.time()
        attempt = 0
        matched_exec = None

        while attempt < max_attempts and (time.time() - start_time) < timeout:
            attempt += 1
            print(f"  attempt {attempt}/{max_attempts} ... ", end="")

            try:
                resp = requests.get(executions_url, headers=headers, timeout=12)
                resp.raise_for_status()
                data = resp.json()
                executions = data.get("data", [])

                candidate_execs = []

                for execution in executions:
                    started_at = execution.get("startedAt")
                    if not started_at:
                        continue

                    try:
                        exec_dt = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
                        if exec_dt.tzinfo is None:
                            exec_dt = exec_dt.replace(tzinfo=timezone.utc)

                        if exec_dt >= trigger_dt:
                            candidate_execs.append((exec_dt, execution))
                    except Exception:
                        continue

                if candidate_execs:
                    candidate_execs.sort(key=lambda x: x[0], reverse=True)
                    matched_exec = candidate_execs[0][1]
                    status = matched_exec.get("status", "unknown")
                    exec_id = matched_exec.get("id", "N/A")
                    print(f"FOUND → execution_id = {exec_id}, status = {status}")

                    if status in ["success", "error", "failed", "crashed"]:
                        break
                else:
                    print("still not visible")

            except requests.exceptions.RequestException as e:
                print(f"request error: {str(e)}")

            time.sleep(poll_interval)

        if not matched_exec:
            raise AirflowException(
                f"Could not find any execution started after trigger time "
                f"for workflow '{workflow_name}' ({workflow_id}) "
                f"after {attempt} attempts (~{int(time.time()-start_time)}s)"
            )

        _print_execution_details(
            exec_data=matched_exec,
            workflow_id=workflow_id,
            workflow_name=workflow_name,
            webhook_path=webhook_path,
        )

        final_status = matched_exec.get("status", "unknown")

        if final_status in ["error", "failed", "crashed"]:
            raise AirflowException(
                f"n8n workflow '{workflow_name}' ({workflow_id}) "
                f"finished with bad status: {final_status}"
            )

        print(
            f"Workflow '{workflow_name}' ({workflow_id}) completed "
            f"→ status = {final_status}"
        )

        _pause(f"After monitor_n8n_workflow [{workflow_name}]", sleep_after)

        return final_status

    def _print_execution_details(
        exec_data: dict,
        workflow_id: str,
        workflow_name: str,
        webhook_path: str,
    ):
        """Verbose printing of execution details + node-level data"""
        print("\n" + "═" * 80)
        print(f"EXECUTION DETAILS — Workflow {workflow_name}")
        print("═" * 80)

        fields = [
            ("Workflow ID", workflow_id),
            ("Webhook Path", webhook_path),
            ("Execution ID", exec_data.get("id", "N/A")),
            ("Status", exec_data.get("status", "N/A")),
            ("Mode", exec_data.get("mode", "N/A")),
            ("Started", exec_data.get("startedAt", "N/A")),
            ("Stopped", exec_data.get("stoppedAt", "N/A")),
            ("Finished", exec_data.get("finished", "N/A")),
            ("Retry of", exec_data.get("retryOf", "N/A")),
        ]

        for label, val in fields:
            if val is None:
                val = "None"
            print(f"{label:14} : {val}")

        print("\nNode execution data:")
        print("-" * 80)

        run_data = exec_data.get("data", {}).get("resultData", {}).get("runData", {})

        if not run_data:
            print("No node run data available.")
            print("-" * 80 + "\n")
            return

        for node_name, runs in run_data.items():
            print(f"• {node_name}  (runs: {len(runs)})")

            for i, run in enumerate(runs, 1):
                print(f"  ├─ Run #{i:2}  status: {run.get('status', '?')}")

                start_time = run.get("startTime") or run.get("startedAt")
                end_time = run.get("executionTime") or run.get("stoppedAt")
                if start_time:
                    print(f"  │   start: {start_time}")
                if end_time:
                    print(f"  │   end/executionTime: {end_time}")

                if run.get("error"):
                    err = run["error"]
                    print(f"  │   ERROR: {err.get('message', '<no message>')}")
                    if err.get("description"):
                        print(f"  │   DETAIL: {err.get('description')}")

                main_data = run.get("data", {}).get("main")
                if main_data:
                    print(f"  │   output branches: {len(main_data)}")
                    for branch_index, branch in enumerate(main_data):
                        item_count = len(branch) if isinstance(branch, list) else 0
                        print(f"  │   branch[{branch_index}] items: {item_count}")

                        if item_count > 0:
                            first_item = branch[0]
                            print(f"  │   sample item[0]: {first_item}")

            print("  └" + "─" * 50)

        print("-" * 80 + "\n")

    # ---------------------------------------------------------------------
    # TEMPLATE USAGE: ONE WORKFLOW
    # ---------------------------------------------------------------------
    workflow_name = Variable.get("n8n_workflow_name")
    resolved = resolve_workflow(
        workflow_name=workflow_name,
        sleep_after=10,
    )

    triggered = trigger_n8n_workflow(
        resolved=resolved,
        http_method="GET",           # change to POST if needed by your workflow
        query_params={
            "env": "dev",
            #"file": "input.csv",   # optional: only include if this workflow expects a file
        },
        request_body={
            # optional JSON payload for POST workflows
        },
        use_test_webhook=True,
        sleep_after=10,
    )

    monitor_n8n_workflow(
        trigger_result=triggered,
        sleep_after=10,
    )


n8n_template_dag = n8n_workflow_template()