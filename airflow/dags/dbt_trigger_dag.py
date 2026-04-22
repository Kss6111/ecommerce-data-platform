"""
DAG: dbt_trigger_dag
Day 11 — Waits for batch + streaming loads then triggers dbt

Flow:
    sense_batch_ingestion    — waits for today's batch_ingestion_dag to succeed
    sense_streaming_load     — waits for latest streaming_load_dag to succeed
    dbt_run_staging          — runs dbt staging models
    dbt_test                 — runs dbt tests
"""

from __future__ import annotations

import logging
import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "ecommerce",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

DBT_PROJECT_DIR = "/opt/dbt"
DBT_PROFILES_DIR = "/opt/dbt"


def run_dbt_command(command: list[str], **context) -> None:
    """
    Runs a dbt CLI command inside the Airflow container.
    dbt-snowflake is installed in the Airflow image.
    Passes Snowflake credentials via environment variables.
    """
    env = os.environ.copy()
    env.update({
        "SNOWFLAKE_ACCOUNT":   os.environ["SNOWFLAKE_ACCOUNT"],
        "SNOWFLAKE_USER":      os.environ["SNOWFLAKE_USER"],
        "SNOWFLAKE_PASSWORD":  os.environ["SNOWFLAKE_PASSWORD"],
        "SNOWFLAKE_WAREHOUSE": os.environ["SNOWFLAKE_WAREHOUSE"],
        "SNOWFLAKE_DATABASE":  os.environ["SNOWFLAKE_DATABASE"],
        "SNOWFLAKE_ROLE":      os.environ["SNOWFLAKE_ROLE"],
    })

    full_command = [
        "dbt", *command,
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROFILES_DIR,
    ]

    log.info("Running: %s", " ".join(full_command))

    result = subprocess.run(
        full_command,
        env=env,
        capture_output=True,
        text=True,
    )

    log.info("dbt stdout:\n%s", result.stdout)
    if result.returncode != 0:
        log.error("dbt stderr:\n%s", result.stderr)
        raise RuntimeError(f"dbt command failed with exit code {result.returncode}")

    log.info("dbt command completed successfully.")


with DAG(
    dag_id="dbt_trigger_dag",
    default_args=DEFAULT_ARGS,
    description="Waits for batch + streaming loads then runs dbt",
    schedule_interval="0 3 * * *",  # Daily at 03:00 UTC — after batch (02:00) completes
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["dbt", "orchestration"],
    max_active_runs=1,
) as dag:

    # ── Sensor: wait for batch ingestion to complete ──────────────────────────
    sense_batch = ExternalTaskSensor(
        task_id="sense_batch_ingestion",
        external_dag_id="batch_ingestion_dag",
        external_task_id="validate_row_counts",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        execution_delta=timedelta(hours=1),  # batch runs at 02:00, dbt at 03:00
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    # ── Sensor: wait for latest streaming load to complete ────────────────────
    sense_streaming = ExternalTaskSensor(
        task_id="sense_streaming_load",
        external_dag_id="streaming_load_dag",
        external_task_id="update_watermark_orders_stream",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        execution_delta=timedelta(hours=1),
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    # ── dbt debug — verifies connection before running models ─────────────────
    t_dbt_debug = PythonOperator(
        task_id="dbt_debug",
        python_callable=run_dbt_command,
        op_kwargs={"command": ["debug"]},
    )

    # ── dbt run — executes all staging models ─────────────────────────────────
    t_dbt_run = PythonOperator(
        task_id="dbt_run_staging",
        python_callable=run_dbt_command,
        op_kwargs={"command": ["run", "--select", "staging"]},
    )

    # ── dbt test — runs schema + data tests ───────────────────────────────────
    t_dbt_test = PythonOperator(
        task_id="dbt_test",
        python_callable=run_dbt_command,
        op_kwargs={"command": ["test", "--select", "staging"]},
    )

    # ── Dependency chain ──────────────────────────────────────────────────────
    [sense_batch, sense_streaming] >> t_dbt_debug >> t_dbt_run >> t_dbt_test