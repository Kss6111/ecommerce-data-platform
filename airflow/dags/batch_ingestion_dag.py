"""
DAG: batch_ingestion_dag
Batch pipeline: PostgreSQL -> S3 Parquet -> Snowflake RAW

Schedule: Daily at 02:00 UTC
Flow:
    1. extract_postgres_to_s3          - dump all 5 tables as Parquet to S3
    2. validate_customers_quality      - GE suite on raw customers (blocks on failure)
    2. validate_orders_history_quality - GE suite on raw orders_history (blocks on failure)
    3. create_raw_tables               - CREATE TABLE IF NOT EXISTS in Snowflake RAW
    4. copy_into_snowflake_*           - COPY INTO each table (5 parallel tasks)
    5. validate_row_counts             - assert Snowflake counts match PostgreSQL
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

# --- Constants ---------------------------------------------------------------
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

VALID_STATUSES = [
    "cancelled", "confirmed", "delivered",
    "pending", "processing", "refunded", "shipped",
]
VALID_PAYMENT_METHODS = [
    "bank_transfer", "credit_card", "crypto", "debit_card", "paypal",
]

DEFAULT_ARGS = {
    "owner": "ecommerce",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
    "email_on_retry": False,
}

log = logging.getLogger(__name__)


# --- Task 1: Extract PostgreSQL -> S3 Parquet --------------------------------
def extract_postgres_to_s3(**context) -> dict:
    run_date = context["ds"]
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

            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False, engine="pyarrow")
            buffer.seek(0)

            s3_key = f"{S3_PREFIX}/{table}/run_date={run_date}/{table}.parquet"
            s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buffer.getvalue())
            s3_paths[table] = s3_key
            log.info("Uploaded %s rows -> s3://%s/%s", row_count, S3_BUCKET, s3_key)
    finally:
        pg_conn.close()

    context["ti"].xcom_push(key="s3_paths", value=s3_paths)
    context["ti"].xcom_push(key="run_date", value=run_date)
    return s3_paths


# --- Task 2a: GE validation - customers --------------------------------------
def validate_customers_quality(**context):
    """
    Runs Great Expectations suite against the customers Parquet file that was
    just written to S3 by extract_postgres_to_s3. Raises AirflowException on
    any expectation failure, which blocks the Snowflake load.
    """
    import great_expectations as gx
    import great_expectations.expectations as gxe
    from airflow.exceptions import AirflowException

    ti = context["ti"]
    run_date = ti.xcom_pull(task_ids="extract_postgres_to_s3", key="run_date")
    s3_paths = ti.xcom_pull(task_ids="extract_postgres_to_s3", key="s3_paths")

    # Load the exact file that was just uploaded
    key = s3_paths["customers"]
    log.info("Validating s3://%s/%s", S3_BUCKET, key)
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    log.info("Loaded %d rows for GE validation", len(df))

    # GE context - ephemeral (no file system needed inside container)
    ctx = gx.get_context(mode="ephemeral")

    datasource = ctx.data_sources.add_pandas(name="customers_ds")
    asset = datasource.add_dataframe_asset(name="customers_asset")
    batch_def = asset.add_batch_definition_whole_dataframe("customers_batch")

    suite = ctx.suites.add(gx.ExpectationSuite(name="customers_airflow_suite"))

    # Column presence
    for col in ["customer_id", "first_name", "last_name", "email",
                "phone", "address", "city", "state", "country",
                "postal_code", "created_at", "updated_at"]:
        suite.add_expectation(gxe.ExpectColumnToExist(column=col))

    suite.add_expectation(gxe.ExpectTableColumnCountToEqual(value=12))

    # No nulls on critical fields
    for col in ["customer_id", "email", "first_name", "last_name", "created_at", "updated_at"]:
        suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=col))

    # Uniqueness
    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="customer_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="email"))

    # Email format
    suite.add_expectation(
        gxe.ExpectColumnValuesToMatchRegex(
            column="email",
            regex=r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$",
            mostly=0.99,
        )
    )

    # Row count guard
    suite.add_expectation(
        gxe.ExpectTableRowCountToBeBetween(min_value=800, max_value=50000)
    )

    # Timestamp plausibility
    for ts_col in ["created_at", "updated_at"]:
        suite.add_expectation(
            gxe.ExpectColumnValuesToBeBetween(
                column=ts_col,
                min_value="2020-01-01T00:00:00+00:00",
                max_value="2030-12-31T23:59:59+00:00",
            )
        )

    # Run validation
    vd = ctx.validation_definitions.add(
        gx.ValidationDefinition(name="customers_vd", data=batch_def, suite=suite)
    )
    checkpoint = ctx.checkpoints.add(
        gx.Checkpoint(
            name="customers_cp",
            validation_definitions=[vd],
            result_format={"result_format": "COMPLETE"},
        )
    )
    result = checkpoint.run(batch_parameters={"dataframe": df})

    if not result.success:
        failures = []
        for vr in result.run_results.values():
            for er in vr["validation_result"]["results"]:
                if not er["success"]:
                    failures.append(
                        f"  FAIL {er['expectation_config']['type']}  "
                        f"result={er.get('result', {})}"
                    )
        failure_msg = "\n".join(failures)
        log.error("Customers GE validation FAILED:\n%s", failure_msg)
        raise AirflowException(
            f"Data quality check FAILED for customers — Snowflake load blocked.\n{failure_msg}"
        )

    log.info("Customers GE validation PASSED (%d expectations)", len(suite.expectations))


# --- Task 2b: GE validation - orders_history ---------------------------------
def validate_orders_history_quality(**context):
    """
    Runs Great Expectations suite against orders_history Parquet on S3.
    Raises AirflowException on failure, blocking the Snowflake load.
    """
    import great_expectations as gx
    import great_expectations.expectations as gxe
    from airflow.exceptions import AirflowException

    ti = context["ti"]
    s3_paths = ti.xcom_pull(task_ids="extract_postgres_to_s3", key="s3_paths")

    key = s3_paths["orders_history"]
    log.info("Validating s3://%s/%s", S3_BUCKET, key)
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    log.info("Loaded %d rows for GE validation", len(df))

    ctx = gx.get_context(mode="ephemeral")

    datasource = ctx.data_sources.add_pandas(name="orders_ds")
    asset = datasource.add_dataframe_asset(name="orders_asset")
    batch_def = asset.add_batch_definition_whole_dataframe("orders_batch")

    suite = ctx.suites.add(gx.ExpectationSuite(name="orders_airflow_suite"))

    # Column presence
    for col in ["order_id", "customer_id", "order_status", "total_amount",
                "payment_method", "shipping_address", "order_date",
                "shipped_date", "delivered_date", "created_at"]:
        suite.add_expectation(gxe.ExpectColumnToExist(column=col))

    suite.add_expectation(gxe.ExpectTableColumnCountToEqual(value=10))

    # No nulls on critical fields (shipped/delivered intentionally nullable)
    for col in ["order_id", "customer_id", "order_status", "total_amount",
                "payment_method", "order_date", "created_at"]:
        suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=col))

    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="order_id"))

    # Valid enum values
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeInSet(column="order_status", value_set=VALID_STATUSES)
    )
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeInSet(column="payment_method", value_set=VALID_PAYMENT_METHODS)
    )

    # Amount range
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeBetween(
            column="total_amount", min_value=0.01, max_value=100000.00
        )
    )

    # Row count guard - alert if batch drops significantly below baseline
    suite.add_expectation(
        gxe.ExpectTableRowCountToBeBetween(min_value=4000, max_value=500000)
    )

    # Timestamp plausibility
    for ts_col in ["order_date", "created_at"]:
        suite.add_expectation(
            gxe.ExpectColumnValuesToBeBetween(
                column=ts_col,
                min_value="2020-01-01T00:00:00+00:00",
                max_value="2030-12-31T23:59:59+00:00",
            )
        )

    # UUID format
    for id_col in ["order_id", "customer_id"]:
        suite.add_expectation(
            gxe.ExpectColumnValuesToMatchRegex(
                column=id_col,
                regex=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
                mostly=1.0,
            )
        )

    vd = ctx.validation_definitions.add(
        gx.ValidationDefinition(name="orders_vd", data=batch_def, suite=suite)
    )
    checkpoint = ctx.checkpoints.add(
        gx.Checkpoint(
            name="orders_cp",
            validation_definitions=[vd],
            result_format={"result_format": "COMPLETE"},
        )
    )
    result = checkpoint.run(batch_parameters={"dataframe": df})

    if not result.success:
        failures = []
        for vr in result.run_results.values():
            for er in vr["validation_result"]["results"]:
                if not er["success"]:
                    failures.append(
                        f"  FAIL {er['expectation_config']['type']}  "
                        f"result={er.get('result', {})}"
                    )
        failure_msg = "\n".join(failures)
        log.error("Orders history GE validation FAILED:\n%s", failure_msg)
        raise AirflowException(
            f"Data quality check FAILED for orders_history — Snowflake load blocked.\n{failure_msg}"
        )

    log.info("Orders history GE validation PASSED (%d expectations)", len(suite.expectations))


# --- Task 3: Create RAW tables in Snowflake ----------------------------------
def create_raw_tables(**context):
    sql_path = os.path.join(os.path.dirname(__file__), "sql", "create_raw_tables.sql")
    with open(sql_path) as f:
        raw_sql = f.read()

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN)
    statements = [s.strip() for s in raw_sql.split(";") if s.strip()]
    with hook.get_conn() as conn:
        cur = conn.cursor()
        for stmt in statements:
            log.info("Executing: %s", stmt[:80])
            cur.execute(stmt)
        conn.commit()
    log.info("All RAW tables verified/created in Snowflake.")


# --- Task 4: COPY INTO Snowflake (one task per table) ------------------------
def copy_into_snowflake(table: str, **context) -> int:
    ti = context["ti"]
    s3_paths: dict = ti.xcom_pull(task_ids="extract_postgres_to_s3", key="s3_paths")
    run_date: str = ti.xcom_pull(task_ids="extract_postgres_to_s3", key="run_date")

    s3_key = s3_paths[table]
    stage_path = s3_key.replace("raw/", "", 1)
    snowflake_table = f"RAW.{table.upper()}"

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN)
    with hook.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {snowflake_table} WHERE DATE(_ingested_at) = '{run_date}'")
        log.info("Cleared today's existing rows from %s", snowflake_table)

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
    log.info("COPY INTO complete for %s - %d rows loaded", table, rows_loaded)
    ti.xcom_push(key=f"snowflake_count_{table}", value=rows_loaded)
    return rows_loaded


# --- Task 5: Validate row counts ---------------------------------------------
def validate_row_counts(**context):
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
                pg_cur.execute(f"SELECT COUNT(*) FROM {table}")
                pg_count = pg_cur.fetchone()[0]

                sf_cur.execute(f"SELECT COUNT(*) FROM RAW.{table.upper()}")
                sf_count = sf_cur.fetchone()[0]

                log.info("%-20s  PostgreSQL: %6d  |  Snowflake: %6d",
                         table, pg_count, sf_count)

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
            "Row count validation FAILED:\n" + "\n".join(mismatches)
        )
    log.info("All row counts validated successfully.")


# --- DAG Definition ----------------------------------------------------------
with DAG(
    dag_id="batch_ingestion_dag",
    default_args=DEFAULT_ARGS,
    description="PostgreSQL -> S3 Parquet -> [GE validation] -> Snowflake RAW daily batch",
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["batch", "postgres", "snowflake", "s3", "data_quality"],
    max_active_runs=1,
) as dag:

    t_extract = PythonOperator(
        task_id="extract_postgres_to_s3",
        python_callable=extract_postgres_to_s3,
    )

    t_validate_customers_quality = PythonOperator(
        task_id="validate_customers_quality",
        python_callable=validate_customers_quality,
    )

    t_validate_orders_quality = PythonOperator(
        task_id="validate_orders_history_quality",
        python_callable=validate_orders_history_quality,
    )

    t_create_tables = PythonOperator(
        task_id="create_raw_tables",
        python_callable=create_raw_tables,
    )

    copy_tasks = []
    for table_name in TABLES:
        t_copy = PythonOperator(
            task_id=f"copy_into_snowflake_{table_name}",
            python_callable=copy_into_snowflake,
            op_kwargs={"table": table_name},
        )
        copy_tasks.append(t_copy)

    t_validate_counts = PythonOperator(
        task_id="validate_row_counts",
        python_callable=validate_row_counts,
    )

    # --- Dependency chain ---
    # extract -> [validate_customers_quality, validate_orders_history_quality]
    #         -> create_tables -> [copy x 5 in parallel] -> validate_row_counts
    t_extract >> [t_validate_customers_quality, t_validate_orders_quality]
    [t_validate_customers_quality, t_validate_orders_quality] >> t_create_tables
    t_create_tables >> copy_tasks >> t_validate_counts
