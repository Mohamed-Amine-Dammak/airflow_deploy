from airflow.decorators import task
from airflow.hooks.base import BaseHook
import requests
import time
from airflow.exceptions import AirflowException
from datetime import datetime
from airflow.utils.email import send_email

def custom_failure_email(recipients):
    def _callback(context):
        dag_id = context["dag"].dag_id
        task_id = context["task_instance"].task_id
        run_id = context["run_id"]
        try_number = context["task_instance"].try_number
        exception = context.get("exception", "No exception details available")
        log_url = context["task_instance"].log_url
        execution_date = context.get("execution_date", "N/A")

        subject = f"❌ Airflow Alert | DAG '{dag_id}' Failed"

        body = f"""
        <html>
        <body style="margin:0; padding:0; background-color:#f4f6f8; font-family:Arial, sans-serif; color:#333;">
            <table width="100%" cellpadding="0" cellspacing="0" style="background-color:#f4f6f8; padding:30px 0;">
                <tr>
                    <td align="center">
                        <table width="700" cellpadding="0" cellspacing="0" style="background:#ffffff; border-radius:12px; overflow:hidden; box-shadow:0 4px 12px rgba(0,0,0,0.08);">
                            
                            <!-- Header -->
                            <tr>
                                <td style="background:#d32f2f; color:#ffffff; padding:24px 32px;">
                                    <h1 style="margin:0; font-size:24px;">❌ Task Failure Detected</h1>
                                    <p style="margin:8px 0 0; font-size:14px; opacity:0.95;">
                                        Airflow detected a task failure that requires attention.
                                    </p>
                                </td>
                            </tr>

                            <!-- Body -->
                            <tr>
                                <td style="padding:32px;">
                                    <p style="font-size:16px; margin-top:0;">
                                        Hello Team,
                                    </p>

                                    <p style="font-size:15px; line-height:1.6;">
                                        A task has failed during a DAG run. Below is a summary of what happened:
                                    </p>

                                    <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse; margin:24px 0; font-size:14px;">
                                        <tr>
                                            <td style="padding:12px; background:#f8f9fb; border:1px solid #e5e7eb; width:180px;"><strong>DAG ID</strong></td>
                                            <td style="padding:12px; border:1px solid #e5e7eb;">{dag_id}</td>
                                        </tr>
                                        <tr>
                                            <td style="padding:12px; background:#f8f9fb; border:1px solid #e5e7eb;"><strong>Task ID</strong></td>
                                            <td style="padding:12px; border:1px solid #e5e7eb;">{task_id}</td>
                                        </tr>
                                        <tr>
                                            <td style="padding:12px; background:#f8f9fb; border:1px solid #e5e7eb;"><strong>Run ID</strong></td>
                                            <td style="padding:12px; border:1px solid #e5e7eb;">{run_id}</td>
                                        </tr>
                                        <tr>
                                            <td style="padding:12px; background:#f8f9fb; border:1px solid #e5e7eb;"><strong>Execution Date</strong></td>
                                            <td style="padding:12px; border:1px solid #e5e7eb;">{execution_date}</td>
                                        </tr>
                                        <tr>
                                            <td style="padding:12px; background:#f8f9fb; border:1px solid #e5e7eb;"><strong>Try Number</strong></td>
                                            <td style="padding:12px; border:1px solid #e5e7eb;">{try_number}</td>
                                        </tr>
                                        <tr>
                                            <td style="padding:12px; background:#f8f9fb; border:1px solid #e5e7eb;"><strong>Exception</strong></td>
                                            <td style="padding:12px; border:1px solid #e5e7eb; color:#b42318;">{exception}</td>
                                        </tr>
                                    </table>

                                    <p style="font-size:15px; line-height:1.6;">
                                        You can review the task logs for more details by clicking the button below:
                                    </p>

                                    <p style="text-align:center; margin:30px 0;">
                                        <a href="{log_url}"
                                           style="background:#d32f2f; color:#ffffff; text-decoration:none; padding:14px 24px; border-radius:8px; font-size:15px; font-weight:bold; display:inline-block;">
                                            View Task Logs
                                        </a>
                                    </p>

                                    <p style="font-size:15px; line-height:1.6;">
                                        Please investigate this failure as soon as possible.
                                    </p>

                                    <p style="font-size:15px; margin-bottom:0;">
                                        Regards,<br>
                                        <strong>Airflow Monitoring</strong>
                                    </p>
                                </td>
                            </tr>

                            <!-- Footer -->
                            <tr>
                                <td style="background:#f8f9fb; padding:16px 32px; font-size:12px; color:#667085; text-align:center; border-top:1px solid #e5e7eb;">
                                    This is an automated notification from Apache Airflow.
                                </td>
                            </tr>

                        </table>
                    </td>
                </tr>
            </table>
        </body>
        </html>
        """

        send_email(to=recipients, subject=subject, html_content=body)

    return _callback