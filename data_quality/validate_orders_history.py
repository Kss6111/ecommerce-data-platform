import io, logging, os, sys
from datetime import date, timedelta
import boto3, pandas as pd
import great_expectations as gx
import great_expectations.expectations as gxe
from data_quality.ge_context import get_context

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BUCKET = os.environ.get("S3_BUCKET_NAME", "ecommerce-data-platform-krutarth-2025")
SUITE_NAME = "orders_history_raw_suite"
VALID_STATUSES = ["cancelled", "confirmed", "delivered", "pending", "processing", "refunded", "shipped"]
VALID_PAYMENT_METHODS = ["bank_transfer", "credit_card", "crypto", "debit_card", "paypal"]


def load_orders_history(run_date):
    key = f"raw/postgres/orders_history/run_date={run_date}/orders_history.parquet"
    logger.info("Reading s3://%s/%s", BUCKET, key)
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    logger.info("Loaded %d rows, %d columns", len(df), len(df.columns))
    return df


def build_orders_history_suite(context):
    try:
        context.suites.delete(SUITE_NAME)
    except Exception:
        pass
    suite = context.suites.add(gx.ExpectationSuite(name=SUITE_NAME))

    required_columns = [
        "order_id", "customer_id", "order_status", "total_amount",
        "payment_method", "shipping_address", "order_date",
        "shipped_date", "delivered_date", "created_at",
    ]
    for col in required_columns:
        suite.add_expectation(gxe.ExpectColumnToExist(column=col))

    suite.add_expectation(gxe.ExpectTableColumnCountToEqual(value=10))

    for col in ["order_id", "customer_id", "order_status", "total_amount",
                "payment_method", "order_date", "created_at"]:
        suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=col))

    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="order_id"))

    suite.add_expectation(
        gxe.ExpectColumnValuesToBeInSet(column="order_status", value_set=VALID_STATUSES)
    )
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeInSet(column="payment_method", value_set=VALID_PAYMENT_METHODS)
    )

    suite.add_expectation(
        gxe.ExpectColumnValuesToBeBetween(column="total_amount", min_value=0.01, max_value=100000.00)
    )

    suite.add_expectation(
        gxe.ExpectTableRowCountToBeBetween(min_value=4000, max_value=500000)
    )

    for ts_col in ["order_date", "created_at"]:
        suite.add_expectation(
            gxe.ExpectColumnValuesToBeBetween(
                column=ts_col,
                min_value="2020-01-01T00:00:00+00:00",
                max_value="2030-12-31T23:59:59+00:00",
            )
        )

    for id_col in ["order_id", "customer_id"]:
        suite.add_expectation(
            gxe.ExpectColumnValuesToMatchRegex(
                column=id_col,
                regex=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
                mostly=1.0,
            )
        )

    logger.info("Built suite '%s' with %d expectations", SUITE_NAME, len(suite.expectations))
    return suite


def run_validation(run_date):
    context = get_context()
    df = load_orders_history(run_date)

    datasource = context.data_sources.add_or_update_pandas(name="pandas_orders_history")
    asset = datasource.add_dataframe_asset(name="orders_history_raw")
    batch_def = asset.add_batch_definition_whole_dataframe("orders_history_full_batch")

    suite = build_orders_history_suite(context)

    vd_name = "orders_history_validation_definition"
    try:
        context.validation_definitions.delete(vd_name)
    except Exception:
        pass
    validation_def = context.validation_definitions.add(
        gx.ValidationDefinition(name=vd_name, data=batch_def, suite=suite)
    )

    cp_name = "orders_history_checkpoint"
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
        logger.error("FAIL Orders history validation FAILED")
        for vr in result.run_results.values():
            for er in vr["validation_result"]["results"]:
                if not er["success"]:
                    logger.error("  FAIL  %s  result=%s",
                        er["expectation_config"]["type"], er.get("result", {}))
    else:
        logger.info("PASS Orders history validation PASSED (%d expectations)", len(suite.expectations))

    return result.success


if __name__ == "__main__":
    run_date = sys.argv[1] if len(sys.argv) > 1 else str(date.today() - timedelta(days=1))
    logger.info("Running orders_history validation for run_date=%s", run_date)
    success = run_validation(run_date)
    sys.exit(0 if success else 1)
