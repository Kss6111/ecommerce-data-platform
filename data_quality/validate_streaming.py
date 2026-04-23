import io, logging, os, sys
from pathlib import Path
import boto3, pandas as pd
import great_expectations as gx
import great_expectations.expectations as gxe
from data_quality.ge_context import get_context

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BUCKET = os.environ.get("S3_BUCKET_NAME", "ecommerce-data-platform-krutarth-2025")

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


def load_delta_table(topic: str) -> pd.DataFrame:
    s3 = boto3.client("s3")
    prefix = f"raw/kafka/{topic}/"
    paginator = s3.get_paginator("list_objects_v2")
    frames = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if "_delta_log" in key or not key.endswith(".parquet"):
                continue
            raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
            frames.append(pd.read_parquet(io.BytesIO(raw)))
    if not frames:
        raise ValueError(f"No parquet files found for topic: {topic}")
    df = pd.concat(frames, ignore_index=True)
    logger.info("Loaded %d rows for topic %s", len(df), topic)
    return df


def load_latest_customers() -> pd.DataFrame:
    s3 = boto3.client("s3")
    prefix = "raw/postgres/customers/"
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append((obj["LastModified"], obj["Key"]))
    if not keys:
        raise ValueError("No customers parquet files found")
    latest_key = sorted(keys, reverse=True)[0][1]
    logger.info("Using customers file: %s", latest_key)
    raw = s3.get_object(Bucket=BUCKET, Key=latest_key)["Body"].read()
    return pd.read_parquet(io.BytesIO(raw))


def build_orders_stream_suite(context, df: pd.DataFrame) -> gx.ExpectationSuite:
    suite_name = "orders_stream_raw_suite"
    try:
        context.suites.delete(suite_name)
    except Exception:
        pass
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
    for col in ["kafka_key", "kafka_offset", "kafka_partition", "kafka_timestamp",
                "event_id", "event_type", "event_timestamp", "order_id",
                "customer_id", "order_status", "payment_method", "total_amount",
                "item_count", "shipping_city", "shipping_country", "source", "ingested_at"]:
        suite.add_expectation(gxe.ExpectColumnToExist(column=col))
    suite.add_expectation(gxe.ExpectTableColumnCountToEqual(value=17))
    for col in ["kafka_key", "event_id", "event_type", "order_id", "customer_id",
                "order_status", "payment_method", "total_amount", "ingested_at"]:
        suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=col, mostly=0.999))
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
            mostly=0.999,
        ))
    customers_df = load_latest_customers()
    valid_customer_ids = set(customers_df["customer_id"].tolist())
    logger.info("Referential integrity: %d known customer_ids loaded", len(valid_customer_ids))
    suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(
        column="customer_id", value_set=list(valid_customer_ids), mostly=0.99,
    ))
    logger.info("Built orders_stream suite with %d expectations", len(suite.expectations))
    return suite


def build_clickstream_suite(context) -> gx.ExpectationSuite:
    suite_name = "clickstream_raw_suite"
    try:
        context.suites.delete(suite_name)
    except Exception:
        pass
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
    for col in ["kafka_key", "kafka_offset", "kafka_partition", "kafka_timestamp",
                "event_id", "event_type", "event_timestamp", "session_id",
                "user_id", "page_url", "product_id", "category", "device_type",
                "browser", "referrer", "time_on_page_ms", "source", "ingested_at"]:
        suite.add_expectation(gxe.ExpectColumnToExist(column=col))
    suite.add_expectation(gxe.ExpectTableColumnCountToEqual(value=18))
    for col in ["kafka_key", "event_id", "event_type", "session_id",
                "page_url", "device_type", "time_on_page_ms", "source", "ingested_at"]:
        suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=col, mostly=0.999))
    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="event_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="event_type",  value_set=CLICKSTREAM_EVENT_TYPES))
    suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="device_type", value_set=CLICKSTREAM_DEVICE_TYPES))
    suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="source",      value_set=["web_frontend"]))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="time_on_page_ms", min_value=0, max_value=3600000))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="kafka_offset",    min_value=0, max_value=None))
    suite.add_expectation(gxe.ExpectTableRowCountToBeBetween(min_value=1, max_value=50000000))
    logger.info("Built clickstream suite with %d expectations", len(suite.expectations))
    return suite


def build_inventory_updates_suite(context) -> gx.ExpectationSuite:
    suite_name = "inventory_updates_raw_suite"
    try:
        context.suites.delete(suite_name)
    except Exception:
        pass
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
    for col in ["kafka_key", "kafka_offset", "kafka_partition", "kafka_timestamp",
                "event_id", "event_type", "event_timestamp", "product_id", "sku",
                "warehouse_id", "quantity_before", "quantity_change", "quantity_after",
                "reason", "triggered_by", "source", "ingested_at"]:
        suite.add_expectation(gxe.ExpectColumnToExist(column=col))
    suite.add_expectation(gxe.ExpectTableColumnCountToEqual(value=17))
    for col in ["kafka_key", "event_id", "event_type", "product_id", "sku",
                "warehouse_id", "quantity_before", "quantity_change", "quantity_after",
                "reason", "triggered_by", "source", "ingested_at"]:
        suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=col, mostly=0.999))
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
    logger.info("Built inventory_updates suite with %d expectations", len(suite.expectations))
    return suite


def run_validation(topic: str) -> bool:
    context = get_context()
    df = load_delta_table(topic)
    datasource = context.data_sources.add_or_update_pandas(name=f"pandas_{topic}")
    asset = datasource.add_dataframe_asset(name=f"{topic}_raw")
    batch_def = asset.add_batch_definition_whole_dataframe(f"{topic}_full_batch")
    if topic == "orders_stream":
        suite = build_orders_stream_suite(context, df)
    elif topic == "clickstream":
        suite = build_clickstream_suite(context)
    else:
        suite = build_inventory_updates_suite(context)
    vd_name = f"{topic}_validation_definition"
    try:
        context.validation_definitions.delete(vd_name)
    except Exception:
        pass
    validation_def = context.validation_definitions.add(
        gx.ValidationDefinition(name=vd_name, data=batch_def, suite=suite)
    )
    cp_name = f"{topic}_checkpoint"
    try:
        context.checkpoints.delete(cp_name)
    except Exception:
        pass
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name=cp_name,
            validation_definitions=[validation_def],
            actions=[gx.checkpoint.UpdateDataDocsAction(name="update_data_docs")],
            result_format={"result_format": "COMPLETE"},
        )
    )
    result = checkpoint.run(batch_parameters={"dataframe": df})
    if not result.success:
        logger.error("FAIL %s validation FAILED", topic)
        for vr in result.run_results.values():
            vr_results = getattr(vr, "results", None) or vr.get("results", [])
            for er in vr_results:
                try:
                    success = er["success"] if isinstance(er, dict) else er.success
                    if not success:
                        cfg = er["expectation_config"]["type"] if isinstance(er, dict) else er.expectation_config.type
                        res = er.get("result", {}) if isinstance(er, dict) else getattr(er, "result", {})
                        logger.error("  FAIL %s  result=%s", cfg, res)
                except Exception:
                    pass
    else:
        logger.info("PASS %s validation PASSED (%d expectations)", topic, len(suite.expectations))
    return result.success


if __name__ == "__main__":
    topic = sys.argv[1] if len(sys.argv) > 1 else "all"
    if topic not in ("orders_stream", "clickstream", "inventory_updates", "all"):
        print("Usage: python validate_streaming.py [orders_stream|clickstream|inventory_updates|all]")
        sys.exit(1)
    topics = ["orders_stream", "clickstream", "inventory_updates"] if topic == "all" else [topic]
    results = {t: run_validation(t) for t in topics}
    all_passed = all(results.values())
    for t, passed in results.items():
        logger.info("%s  %s", "PASS" if passed else "FAIL", t)
    sys.exit(0 if all_passed else 1)
