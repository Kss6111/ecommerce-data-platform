import pathlib

streaming_dag = """\
\"\"\"
DAG: streaming_load_dag
Streaming load: Delta Lake (S3) -> Snowflake RAW every 30 minutes

Flow per topic:
    1. discover_new_files_{topic}   - list S3 Parquet files not yet loaded
    2. validate_{topic}_quality     - GE validation (blocks load on failure)
    3. copy_streaming_{topic}       - COPY INTO RAW table
    4. update_watermark_{topic}     - persist loaded file keys
\"\"\"

from __future__ import annotations

import io
import json
import logging
import os
from datetime import datetime, timedelta

import boto3
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

log = logging.getLogger(__name__)

S3_BUCKET      = os.environ["S3_BUCKET_NAME"]
SNOWFLAKE_CONN = "snowflake_default"
TOPICS         = ["orders_stream", "clickstream", "inventory_updates"]

TOPIC_CONFIG = {
    "orders_stream":     {"s3_prefix": "kafka/orders_stream/",     "table": "RAW.ORDERS_STREAM"},
    "clickstream":       {"s3_prefix": "kafka/clickstream/",       "table": "RAW.CLICKSTREAM"},
    "inventory_updates": {"s3_prefix": "kafka/inventory_updates/", "table": "RAW.INVENTORY_UPDATES"},
}

ORDERS_STREAM_EVENT_TYPES     = ["order_cancelled", "order_created", "order_updated"]
ORDERS_STREAM_STATUSES        = ["cancelled", "confirmed", "delivered", "pending", "processing", "refunded", "shipped"]
ORDERS_STREAM_PAYMENT_METHODS = ["bank_transfer", "credit_card", "crypto", "debit_card", "paypal"]
CLICKSTREAM_EVENT_TYPES       = ["add_to_cart", "checkout", "page_view", "product_view", "purchase"]
CLICKSTREAM_DEVICE_TYPES      = ["desktop", "mobile", "tablet"]
INVENTORY_EVENT_TYPES         = ["adjustment", "low_stock_alert", "restock", "sale_deduction"]
INVENTORY_REASONS = [
    "auto_threshold_trigger", "bulk_order", "damage_writeoff", "expiry_removal",
    "inventory_audit", "manual_review", "order_fulfilled", "return_processed",
    "subscription_fulfillment", "supplier_delivery", "transfer_in",
]
INVENTORY_TRIGGERED_BY = ["manual", "order_fulfillment", "system"]

DEFAULT_ARGS = {
    "owner": "ecommerce",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


def _s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )


def _load_s3_parquet_files(s3, keys: list) -> pd.DataFrame:
    frames = []
    for key in keys:
        raw = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
        frames.append(pd.read_parquet(io.BytesIO(raw)))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _load_latest_customer_ids(s3) -> set:
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="raw/postgres/customers/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append((obj["LastModified"], obj["Key"]))
    if not keys:
        return set()
    latest = sorted(keys, reverse=True)[0][1]
    raw = s3.get_object(Bucket=S3_BUCKET, Key=latest)["Body"].read()
    df = pd.read_parquet(io.BytesIO(raw))
    return set(df["customer_id"].tolist())


def discover_new_files(topic: str, **context) -> list:
    s3 = _s3_client()
    prefix = f"raw/{TOPIC_CONFIG[topic]['s3_prefix']}"
    paginator = s3.get_paginator("list_objects_v2")
    all_files = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if "_delta_log" in key or not key.endswith(".parquet"):
                continue
            all_files.append(key)
    watermark_var = f"streaming_watermark_{topic}"
    try:
        loaded_files = set(json.loads(Variable.get(watermark_var)))
    except Exception:
        loaded_files = set()
    new_files = [f for f in all_files if f not in loaded_files]
    log.info("Topic %s: %d total, %d new", topic, len(all_files), len(new_files))
    context["ti"].xcom_push(key=f"new_files_{topic}", value=new_files)
    return new_files


def validate_streaming_quality(topic: str, **context):
    import great_expectations as gx
    import great_expectations.expectations as gxe
    from airflow.exceptions import AirflowException

    ti = context["ti"]
    new_files = ti.xcom_pull(task_ids=f"discover_new_files_{topic}", key=f"new_files_{topic}")
    if not new_files:
        log.info("No new files for %s - skipping GE validation", topic)
        return

    s3 = _s3_client()
    df = _load_s3_parquet_files(s3, new_files)
    log.info("Loaded %d rows from %d files for %s", len(df), len(new_files), topic)

    ctx = gx.get_context(mode="ephemeral")
    ds = ctx.data_sources.add_pandas(name=f"{topic}_ds")
    asset = ds.add_dataframe_asset(name=f"{topic}_asset")
    batch_def = asset.add_batch_definition_whole_dataframe(f"{topic}_batch")
    suite = ctx.suites.add(gx.ExpectationSuite(name=f"{topic}_suite"))

    if topic == "orders_stream":
        for col in ["kafka_key", "kafka_offset", "kafka_partition", "kafka_timestamp",
                    "event_id", "event_type", "event_timestamp", "order_id",
                    "customer_id", "order_status", "payment_method", "total_amount",
                    "item_count", "shipping_city", "shipping_country", "source", "ingested_at"]:
            suite.add_expectation(gxe.ExpectColumnToExist(column=col))
        suite.add_expectation(gxe.ExpectTableColumnCountToEqual(value=17))
        for col in ["kafka_key", "event_id", "event_type", "order_id", "customer_id",
                    "order_status", "payment_method", "total_amount", "ingested_at"]:
            suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=col))
        suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="event_id"))
        suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="event_type",     value_set=ORDERS_STREAM_EVENT_TYPES))
        suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="order_status",   value_set=ORDERS_STREAM_STATUSES))
        suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="payment_method", value_set=ORDERS_STREAM_PAYMENT_METHODS))
        suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="source",         value_set=["postgres_cdc"]))
        suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="total_amount", min_value=0.01, max_value=100000.0))
        suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="item_count",   min_value=1,    max_value=1000))
        suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="kafka_offset", min_value=0,    max_value=None))
        suite.add_expectation(gxe.ExpectTableRowCountToBeBetween(min_value=1, max_value=10000000))
        for id_col in ["event_id", "order_id", "customer_id"]:
            suite.add_expectation(gxe.ExpectColumnValuesToMatchRegex(
                column=id_col,
                regex=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
                mostly=1.0,
            ))
        valid_cids = _load_latest_customer_ids(s3)
        if valid_cids:
            suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(
                column="customer_id", value_set=list(valid_cids), mostly=0.99
            ))
            log.info("Referential integrity: %d known customer_ids", len(valid_cids))

    elif topic == "clickstream":
        for col in ["kafka_key", "kafka_offset", "kafka_partition", "kafka_timestamp",
                    "event_id", "event_type", "event_timestamp", "session_id",
                    "user_id", "page_url", "product_id", "category", "device_type",
                    "browser", "referrer", "time_on_page_ms", "source", "ingested_at"]:
            suite.add_expectation(gxe.ExpectColumnToExist(column=col))
        suite.add_expectation(gxe.ExpectTableColumnCountToEqual(value=18))
        for col in ["kafka_key", "event_id", "event_type", "session_id",
                    "page_url", "device_type", "time_on_page_ms", "source", "ingested_at"]:
            suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=col))
        suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="event_id"))
        suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="event_type",  value_set=CLICKSTREAM_EVENT_TYPES))
        suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="device_type", value_set=CLICKSTREAM_DEVICE_TYPES))
        suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="source",      value_set=["web_frontend"]))
        suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="time_on_page_ms", min_value=0, max_value=3600000))
        suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="kafka_offset",    min_value=0, max_value=None))
        suite.add_expectation(gxe.ExpectTableRowCountToBeBetween(min_value=1, max_value=50000000))

    else:
        for col in ["kafka_key", "kafka_offset", "kafka_partition", "kafka_timestamp",
                    "event_id", "event_type", "event_timestamp", "product_id", "sku",
                    "warehouse_id", "quantity_before", "quantity_change", "quantity_after",
                    "reason", "triggered_by", "source", "ingested_at"]:
            suite.add_expectation(gxe.ExpectColumnToExist(column=col))
        suite.add_expectation(gxe.ExpectTableColumnCountToEqual(value=17))
        for col in ["kafka_key", "event_id", "event_type", "product_id", "sku",
                    "warehouse_id", "quantity_before", "quantity_change", "quantity_after",
                    "reason", "triggered_by", "source", "ingested_at"]:
            suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=col))
        suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="event_id"))
        suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="event_type",   value_set=INVENTORY_EVENT_TYPES))
        suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="reason",       value_set=INVENTORY_REASONS))
        suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="triggered_by", value_set=INVENTORY_TRIGGERED_BY))
        suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="source",       value_set=["inventory_service"]))
        suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="quantity_before", min_value=0,      max_value=None))
        suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="quantity_after",  min_value=0,      max_value=None))
        suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="quantity_change", min_value=-10000, max_value=100000))
        suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="kafka_offset",    min_value=0,      max_value=None))
        suite.add_expectation(gxe.ExpectTableRowCountToBeBetween(min_value=1, max_value=10000000))

    vd = ctx.validation_definitions.add(
        gx.ValidationDefinition(name=f"{topic}_vd", data=batch_def, suite=suite)
    )
    checkpoint = ctx.checkpoints.add(
        gx.Checkpoint(
            name=f"{topic}_cp",
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
                    failures.append(f"  FAIL {er['expectation_config']['type']}  result={er.get('result', {})}")
        msg = "\\n".join(failures)
        log.error("%s GE validation FAILED:\\n%s", topic, msg)
        raise AirflowException(f"Data quality FAILED for {topic} — load blocked.\\n{msg}")

    log.info("%s GE validation PASSED (%d expectations)", topic, len(suite.expectations))


def copy_streaming_to_snowflake(topic: str, **context) -> int:
    ti = context["ti"]
    new_files: list = ti.xcom_pull(task_ids=f"discover_new_files_{topic}", key=f"new_files_{topic}")
    if not new_files:
        log.info("No new files for topic %s - skipping COPY INTO", topic)
        ti.xcom_push(key=f"loaded_files_{topic}", value=[])
        return 0
    table = TOPIC_CONFIG[topic]["table"]
    hook  = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN)
    total_rows = 0
    successfully_loaded = []
    with hook.get_conn() as conn:
        cur = conn.cursor()
        for s3_key in new_files:
            stage_path = s3_key.replace("raw/", "", 1)
            copy_sql = f\"\"\"
                COPY INTO {table}
                FROM @ECOMMERCE_DB.RAW.S3_RAW_STAGE/{stage_path}
                FILE_FORMAT = (TYPE = 'PARQUET' SNAPPY_COMPRESSION = TRUE)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                PURGE = FALSE
                ON_ERROR = 'CONTINUE'
            \"\"\"
            try:
                cur.execute(copy_sql)
                result = cur.fetchall()
                rows = sum(r[3] if len(r) > 3 else 0 for r in result) if result else 0
                total_rows += rows
                successfully_loaded.append(s3_key)
                log.info("Loaded %d rows from %s -> %s", rows, stage_path, table)
            except Exception as e:
                log.error("Failed to load %s: %s", stage_path, str(e))
        conn.commit()
    ti.xcom_push(key=f"loaded_files_{topic}", value=successfully_loaded)
    log.info("Total rows loaded for %s: %d", topic, total_rows)
    return total_rows


def update_watermark(topic: str, **context) -> None:
    ti = context["ti"]
    newly_loaded: list = ti.xcom_pull(task_ids=f"copy_streaming_{topic}", key=f"loaded_files_{topic}")
    watermark_var = f"streaming_watermark_{topic}"
    try:
        existing = set(json.loads(Variable.get(watermark_var)))
    except Exception:
        existing = set()
    updated = existing | set(newly_loaded or [])
    Variable.set(watermark_var, json.dumps(list(updated)))
    log.info("Watermark updated for %s: %d total files tracked", topic, len(updated))


with DAG(
    dag_id="streaming_load_dag",
    default_args=DEFAULT_ARGS,
    description="Delta Lake S3 -> [GE validation] -> Snowflake RAW every 30 min",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["streaming", "kafka", "snowflake", "delta", "data_quality"],
    max_active_runs=1,
) as dag:

    for topic in TOPICS:
        t_discover = PythonOperator(
            task_id=f"discover_new_files_{topic}",
            python_callable=discover_new_files,
            op_kwargs={"topic": topic},
        )
        t_validate = PythonOperator(
            task_id=f"validate_{topic}_quality",
            python_callable=validate_streaming_quality,
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
        t_discover >> t_validate >> t_copy >> t_watermark
"""

pathlib.Path("airflow/dags").mkdir(parents=True, exist_ok=True)
with open("airflow/dags/streaming_load_dag.py", "w", encoding="utf-8", newline="\n") as f:
    f.write(streaming_dag)
print("Written: airflow/dags/streaming_load_dag.py")