"""
Slack failure alerting for the retail pipeline.

Notify a Slack channel automatically whenever any task fails, with the
task name, DAG run, and a direct link to the failed task's logs.
"""

import os

from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

# Airflow connection ID configured for Slack Webhooks
SLACK_WEBHOOK_CONN_ID = "slack_webhook"


def task_failure_slack_alert(context):
    """Send a Slack alert when an individual task fails.
    
    Wire via default_args={'on_failure_callback': task_failure_slack_alert}.
    """
    # Extract execution context variables supplied by Airflow runtime
    ti = context["task_instance"]
    dag_id = ti.dag_id
    task_id = ti.task_id
    run_id = context["run_id"]
    log_url = ti.log_url
    exception = context.get("exception")

    # Format Slack message payload with markdown styling and error snippet
    message = (
        f":red_circle: *Airflow Task Failed*\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Task:* `{task_id}`\n"
        f"*Run:* `{run_id}`\n"
        f"*Error:* `{str(exception)[:300]}`\n"  # Truncate long tracebacks to prevent Slack payload size limits
        f"<{log_url}|View logs>"
    )

    # Route alert: Check environment variable first, fall back to Airflow Connection if not set
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if webhook_url:
        # Route 1: Post directly using raw Webhook URL from environment variables
        import requests
        requests.post(webhook_url, json={"text": message}, timeout=10)
    else:
        # Route 2: Post using standard Airflow Connection via SlackWebhookHook
        hook = SlackWebhookHook(slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID)
        hook.send(text=message)


def dag_failure_slack_alert(context):
    """Send a single summary alert when the entire DAG run fails (DAG-level callback)."""
    # Extract DAG run object from execution context
    dag_run = context["dag_run"]
    
    # Format DAG failure alert message
    message = (
        f":rotating_light: *Airflow DAG Run Failed*\n"
        f"*DAG:* `{dag_run.dag_id}`\n"
        f"*Run:* `{dag_run.run_id}`\n"
        f"*State:* `{dag_run.state}`"
    )

    # Route alert based on available environment variables or Airflow Connection
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if webhook_url:
        import requests
        requests.post(webhook_url, json={"text": message}, timeout=10)
    else:
        hook = SlackWebhookHook(slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID)
        hook.send(text=message)