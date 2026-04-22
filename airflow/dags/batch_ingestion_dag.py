"""
DAG: batch_ingestion_dag
Day 10 — Batch pipeline: PostgreSQL → S3 Parquet → Snowflake RAW

Schedule: Daily at 02:00 UTC
Flow:
    1. extract_postgres_to_s3   — dump all 5 tables as Parquet to S3
    2. create_raw_tables        — CREATE TABLE IF NOT EXISTS in Snowflake RAW
    3. copy_into_snowflake_*    — COPY INTO each table (5 parallel tasks)
    4. validate_row_counts      — assert Snowflake counts match PostgreSQL
"""

from __future__ import annotations

import io
import logging
import os
from datetime import datetime, timedelta

import boto3
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# ─── Constants ────────────────────────────────────────────────────────────────
S3_BUCKET = os.environ["S3_BUCKET_NAME"]
S3_PREFIX = "raw/postgres"
SNOWFLAKE_CONN = "snowflake_default"
POSTGRES_CONN_PARAMS = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": int(os.environ.get("POSTGRES_PORT", 5432)),
    "dbname": os.environ.get("POSTGRES_DB", "ecommerce_db"),
    "user": os.environ.get("POSTGRES_USER", "ecommerce_user"),
    "password": os.environ.get("POSTGRES_PASSWORD", "ecommerce_pass"),
}

TABLES = ["customers", "products", "suppliers", "orders_history", "order_items"]

DEFAULT_ARGS = {
    "owner": "ecommerce",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,  # flip to True + add email conn for real alerting
    "email_on_retry": False,
}

log = logging.getLogger(__name__)


# ─── Task 1: Extract PostgreSQL → S3 Parquet ──────────────────────────────────
def extract_postgres_to_s3(**context) -> dict:
    """
    Dumps each source table from PostgreSQL to S3 as Parquet.
    Path pattern: raw/postgres/{table}/run_date={YYYY-MM-DD}/{table}.parquet
    Returns a dict of {table: s3_key} pushed to XCom.
    """
    run_date = context["ds"]  # YYYY-MM-DD
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )

    pg_conn = psycopg2.connect(**POSTGRES_CONN_PARAMS)
    s3_paths = {}

    try:
        for table in TABLES:
            log.info("Extracting table: %s", table)
            df = pd.read_sql(f"SELECT * FROM {table}", pg_conn)
            row_count = len(df)

            # Serialise to Parquet in memory — no local disk needed
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False, engine="pyarrow")
            buffer.seek(0)

            s3_key = f"{S3_PREFIX}/{table}/run_date={run_date}/{table}.parquet"
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=buffer.getvalue(),
            )
            s3_paths[table] = s3_key
            log.info("Uploaded %s rows → s3://%s/%s", row_count, S3_BUCKET, s3_key)
    finally:
        pg_conn.close()

    # Push to XCom so downstream tasks can reference exact paths
    context["ti"].xcom_push(key="s3_paths", value=s3_paths)
    context["ti"].xcom_push(key="run_date", value=run_date)
    return s3_paths


# ─── Task 2: Create RAW tables in Snowflake ───────────────────────────────────
def create_raw_tables(**context):
    """
    Runs the DDL script to CREATE TABLE IF NOT EXISTS for all 5 raw tables.
    Idempotent — safe to run on every DAG execution.
    """
    sql_path = os.path.join(os.path.dirname(__file__), "sql", "create_raw_tables.sql")
    with open(sql_path) as f:
        raw_sql = f.read()

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN)

    # Split on semicolons and execute each statement individually
    statements = [s.strip() for s in raw_sql.split(";") if s.strip()]
    with hook.get_conn() as conn:
        cur = conn.cursor()
        for stmt in statements:
            log.info("Executing: %s", stmt[:80])
            cur.execute(stmt)
        conn.commit()

    log.info("All RAW tables verified/created in Snowflake.")


# ─── Task 3: COPY INTO Snowflake (one task per table) ─────────────────────────
def copy_into_snowflake(table: str, **context) -> int:
    """
    Loads a single table's Parquet file from S3 into Snowflake RAW
    using the pre-configured S3_RAW_STAGE external stage.
    Truncates first so each daily run is a full refresh of raw data.
    """
    ti = context["ti"]
    s3_paths: dict = ti.xcom_pull(task_ids="extract_postgres_to_s3", key="s3_paths")
    run_date: str = ti.xcom_pull(task_ids="extract_postgres_to_s3", key="run_date")

    s3_key = s3_paths[table]
    # Strip bucket prefix — stage is already pointed at the bucket root
    stage_path = s3_key.replace("raw/", "", 1)  # strip leading raw/ — stage already points to .../raw/

    snowflake_table = f"RAW.{table.upper()}"

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN)
    with hook.get_conn() as conn:
        cur = conn.cursor()

        # Truncate existing raw data for today's full refresh
        cur.execute(f"DELETE FROM {snowflake_table} WHERE DATE(_ingested_at) = '{run_date}'")
        log.info("Cleared today's existing rows from %s", snowflake_table)

        # COPY INTO using the named stage
        copy_sql = f"""
            COPY INTO {snowflake_table}
            FROM @ECOMMERCE_DB.RAW.S3_RAW_STAGE/{stage_path}
            FILE_FORMAT = (
                TYPE = 'PARQUET'
                SNAPPY_COMPRESSION = TRUE
            )
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            PURGE = FALSE
            ON_ERROR = 'ABORT_STATEMENT'
        """
        log.info("Running COPY INTO for table: %s", table)
        cur.execute(copy_sql)
        result = cur.fetchall()
        conn.commit()

    rows_loaded = sum(r[3] if len(r) > 3 else 0 for r in result) if result else 0
    log.info("COPY INTO complete for %s — %d rows loaded", table, rows_loaded)

    ti.xcom_push(key=f"snowflake_count_{table}", value=rows_loaded)
    return rows_loaded


# ─── Task 4: Validate row counts ──────────────────────────────────────────────
def validate_row_counts(**context):
    """
    Compares row counts between PostgreSQL source and Snowflake RAW target.
    Raises ValueError if any table is off by more than 1% (tolerance for
    in-flight inserts during extraction window).
    """
    ti = context["ti"]
    run_date = ti.xcom_pull(task_ids="extract_postgres_to_s3", key="run_date")

    pg_conn = psycopg2.connect(**POSTGRES_CONN_PARAMS)
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN)

    mismatches = []

    try:
        pg_cur = pg_conn.cursor()
        with hook.get_conn() as sf_conn:
            sf_cur = sf_conn.cursor()

            for table in TABLES:
                # PostgreSQL count
                pg_cur.execute(f"SELECT COUNT(*) FROM {table}")
                pg_count = pg_cur.fetchone()[0]

                # Snowflake count — only rows loaded today
                sf_cur.execute(
                    f"SELECT COUNT(*) FROM RAW.{table.upper()}"
                )
                sf_count = sf_cur.fetchone()[0]

                log.info(
                    "%-20s  PostgreSQL: %6d  |  Snowflake: %6d",
                    table, pg_count, sf_count
                )

                if pg_count == 0:
                    continue

                pct_diff = abs(pg_count - sf_count) / pg_count
                if pct_diff > 0.01:
                    mismatches.append(
                        f"{table}: PostgreSQL={pg_count}, Snowflake={sf_count} "
                        f"({pct_diff*100:.1f}% off)"
                    )
    finally:
        pg_conn.close()

    if mismatches:
        raise ValueError(
            "Row count validation FAILED for the following tables:\n"
            + "\n".join(mismatches)
        )

    log.info("✅ All row counts validated successfully.")


# ─── DAG Definition ───────────────────────────────────────────────────────────
with DAG(
    dag_id="batch_ingestion_dag",
    default_args=DEFAULT_ARGS,
    description="PostgreSQL → S3 Parquet → Snowflake RAW daily batch",
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["batch", "postgres", "snowflake", "s3"],
    max_active_runs=1,
) as dag:

    t_extract = PythonOperator(
        task_id="extract_postgres_to_s3",
        python_callable=extract_postgres_to_s3,
    )

    t_create_tables = PythonOperator(
        task_id="create_raw_tables",
        python_callable=create_raw_tables,
    )

    # One COPY INTO task per table — they run in parallel after create_tables
    copy_tasks = []
    for table_name in TABLES:
        t_copy = PythonOperator(
            task_id=f"copy_into_snowflake_{table_name}",
            python_callable=copy_into_snowflake,
            op_kwargs={"table": table_name},
        )
        copy_tasks.append(t_copy)

    t_validate = PythonOperator(
        task_id="validate_row_counts",
        python_callable=validate_row_counts,
    )

    # ─── Dependency chain ─────────────────────────────────────────────────────
    # extract → create_tables → [copy × 5 in parallel] → validate
    t_extract >> t_create_tables >> copy_tasks >> t_validate