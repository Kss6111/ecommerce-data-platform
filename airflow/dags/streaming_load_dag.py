"""
DAG: streaming_load_dag
Day 11 — Streaming load: Delta Lake (S3) → Snowflake RAW every 30 minutes

Flow per topic:
    1. discover_new_files   — list S3 Parquet files not yet loaded
    2. copy_into_snowflake  — COPY INTO RAW table for each topic
    3. update_watermark     — save last loaded file key to Airflow Variable
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

log = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────
S3_BUCKET = os.environ["S3_BUCKET_NAME"]
SNOWFLAKE_CONN = "snowflake_default"
TOPICS = ["orders_stream", "clickstream", "inventory_updates"]

# Stage already points to s3://.../raw/ so path starts after raw/
TOPIC_CONFIG = {
    "orders_stream":     {"s3_prefix": "kafka/orders_stream/",     "table": "RAW.ORDERS_STREAM"},
    "clickstream":       {"s3_prefix": "kafka/clickstream/",       "table": "RAW.CLICKSTREAM"},
    "inventory_updates": {"s3_prefix": "kafka/inventory_updates/", "table": "RAW.INVENTORY_UPDATES"},
}

DEFAULT_ARGS = {
    "owner": "ecommerce",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


# ─── Task: Discover new Parquet files since last watermark ────────────────────
def discover_new_files(topic: str, **context) -> list[str]:
    """
    Lists all .parquet files under the topic prefix in S3.
    Filters out _delta_log/ entries and files already loaded
    (tracked via Airflow Variable as a watermark set).
    Returns list of stage-relative paths for new files only.
    """
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )

    prefix = f"raw/{TOPIC_CONFIG[topic]['s3_prefix']}"
    paginator = s3.get_paginator("list_objects_v2")
    all_files = []

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if "_delta_log" in key:
                continue
            if not key.endswith(".parquet"):
                continue
            all_files.append(key)

    # Load watermark — set of already-loaded file keys
    watermark_var = f"streaming_watermark_{topic}"
    try:
        loaded_files = set(json.loads(Variable.get(watermark_var)))
    except Exception:
        loaded_files = set()

    new_files = [f for f in all_files if f not in loaded_files]
    log.info("Topic %s: %d total files, %d new files to load", topic, len(all_files), len(new_files))

    context["ti"].xcom_push(key=f"new_files_{topic}", value=new_files)
    return new_files


# ─── Task: COPY INTO Snowflake for each new file ─────────────────────────────
def copy_streaming_to_snowflake(topic: str, **context) -> int:
    """
    Runs COPY INTO for each new Parquet file discovered.
    Uses _source_file metadata column to track which file each row came from.
    Skips if no new files found.
    """
    ti = context["ti"]
    new_files: list = ti.xcom_pull(
        task_ids=f"discover_new_files_{topic}",
        key=f"new_files_{topic}"
    )

    if not new_files:
        log.info("No new files for topic %s — skipping COPY INTO", topic)
        ti.xcom_push(key=f"loaded_files_{topic}", value=[])
        return 0

    table = TOPIC_CONFIG[topic]["table"]
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN)
    total_rows = 0
    successfully_loaded = []

    with hook.get_conn() as conn:
        cur = conn.cursor()

        for s3_key in new_files:
            # Strip leading raw/ — stage already points to .../raw/
            stage_path = s3_key.replace("raw/", "", 1)

            copy_sql = f"""
                COPY INTO {table}
                FROM @ECOMMERCE_DB.RAW.S3_RAW_STAGE/{stage_path}
                FILE_FORMAT = (
                    TYPE = 'PARQUET'
                    SNAPPY_COMPRESSION = TRUE
                )
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                PURGE = FALSE
                ON_ERROR = 'CONTINUE'
            """
            
            try:
                cur.execute(copy_sql)
                result = cur.fetchall()
                rows = sum(r[3] if len(r) > 3 else 0 for r in result) if result else 0
                total_rows += rows
                successfully_loaded.append(s3_key)
                log.info("Loaded %d rows from %s → %s", rows, stage_path, table)
            except Exception as e:
                log.error("Failed to load %s: %s", stage_path, str(e))

        conn.commit()

    ti.xcom_push(key=f"loaded_files_{topic}", value=successfully_loaded)
    log.info("Total rows loaded for %s: %d", topic, total_rows)
    return total_rows


# ─── Task: Update watermark ───────────────────────────────────────────────────
def update_watermark(topic: str, **context) -> None:
    """
    Appends successfully loaded file keys to the watermark Variable
    so they are not re-loaded on the next run.
    """
    ti = context["ti"]
    newly_loaded: list = ti.xcom_pull(
        task_ids=f"copy_streaming_{topic}",
        key=f"loaded_files_{topic}"
    )

    watermark_var = f"streaming_watermark_{topic}"
    try:
        existing = set(json.loads(Variable.get(watermark_var)))
    except Exception:
        existing = set()

    updated = existing | set(newly_loaded or [])
    Variable.set(watermark_var, json.dumps(list(updated)))
    log.info("Watermark updated for %s — %d total files tracked", topic, len(updated))


# ─── DAG Definition ───────────────────────────────────────────────────────────
with DAG(
    dag_id="streaming_load_dag",
    default_args=DEFAULT_ARGS,
    description="Delta Lake S3 → Snowflake RAW streaming load every 30 min",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["streaming", "kafka", "snowflake", "delta"],
    max_active_runs=1,
) as dag:

    for topic in TOPICS:
        t_discover = PythonOperator(
            task_id=f"discover_new_files_{topic}",
            python_callable=discover_new_files,
            op_kwargs={"topic": topic},
        )

        t_copy = PythonOperator(
            task_id=f"copy_streaming_{topic}",
            python_callable=copy_streaming_to_snowflake,
            op_kwargs={"topic": topic},
        )

        t_watermark = PythonOperator(
            task_id=f"update_watermark_{topic}",
            python_callable=update_watermark,
            op_kwargs={"topic": topic},
        )

        t_discover >> t_copy >> t_watermark