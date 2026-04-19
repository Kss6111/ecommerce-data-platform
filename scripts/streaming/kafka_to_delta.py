"""
kafka_to_delta.py
PySpark Structured Streaming job that reads from all three Kafka topics
and writes to S3 in Delta Lake format under raw/kafka/<topic>/

Run via spark-submit inside the container — see run_streaming.sh
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp,
    year, month, dayofmonth, hour
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, BooleanType, LongType
)

# ── Schema definitions — must match schemas.py exactly ───────────────────────

ORDER_SCHEMA = StructType([
    StructField("event_id",          StringType(),  True),
    StructField("event_type",        StringType(),  True),
    StructField("event_timestamp",   StringType(),  True),
    StructField("order_id",          StringType(),  True),
    StructField("customer_id",       StringType(),  True),
    StructField("order_status",      StringType(),  True),
    StructField("payment_method",    StringType(),  True),
    StructField("total_amount",      DoubleType(),  True),
    StructField("item_count",        IntegerType(), True),
    StructField("shipping_city",     StringType(),  True),
    StructField("shipping_country",  StringType(),  True),
    StructField("source",            StringType(),  True),
])

CLICK_SCHEMA = StructType([
    StructField("event_id",          StringType(),  True),
    StructField("event_type",        StringType(),  True),
    StructField("event_timestamp",   StringType(),  True),
    StructField("session_id",        StringType(),  True),
    StructField("user_id",           StringType(),  True),
    StructField("page_url",          StringType(),  True),
    StructField("product_id",        StringType(),  True),
    StructField("category",          StringType(),  True),
    StructField("device_type",       StringType(),  True),
    StructField("browser",           StringType(),  True),
    StructField("referrer",          StringType(),  True),
    StructField("time_on_page_ms",   LongType(),    True),
    StructField("source",            StringType(),  True),
])

INVENTORY_SCHEMA = StructType([
    StructField("event_id",          StringType(),  True),
    StructField("event_type",        StringType(),  True),
    StructField("event_timestamp",   StringType(),  True),
    StructField("product_id",        StringType(),  True),
    StructField("sku",               StringType(),  True),
    StructField("warehouse_id",      StringType(),  True),
    StructField("quantity_before",   IntegerType(), True),
    StructField("quantity_change",   IntegerType(), True),
    StructField("quantity_after",    IntegerType(), True),
    StructField("reason",            StringType(),  True),
    StructField("triggered_by",      StringType(),  True),
    StructField("source",            StringType(),  True),
])

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "kafka:9092"          # internal Docker network
S3_BUCKET       = os.getenv("S3_BUCKET_NAME", "ecommerce-data-platform-krutarth-2025")
CHECKPOINT_BASE = f"s3a://{S3_BUCKET}/checkpoints"
DELTA_BASE      = f"s3a://{S3_BUCKET}/raw/kafka"

TOPICS = {
    "orders_stream":      ORDER_SCHEMA,
    "clickstream":        CLICK_SCHEMA,
    "inventory_updates":  INVENTORY_SCHEMA,
}


# ── Spark session ─────────────────────────────────────────────────────────────
def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("ecommerce-kafka-to-delta")
        .master("spark://spark-master:7077")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # S3 / Hadoop config
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.access.key",
                os.getenv("AWS_ACCESS_KEY_ID", ""))
        .config("spark.hadoop.fs.s3a.secret.key",
                os.getenv("AWS_SECRET_ACCESS_KEY", ""))
        .config("spark.hadoop.fs.s3a.endpoint",
                f"s3.{os.getenv('AWS_REGION', 'us-east-1')}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        # Delta Lake performance
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        # Streaming
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
    )
    return builder.getOrCreate()


# ── Stream builder ────────────────────────────────────────────────────────────
def read_kafka_topic(spark: SparkSession, topic: str):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 1000)
        .load()
    )


def parse_topic(raw_df, schema: StructType):
    return (
        raw_df
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("offset").alias("kafka_offset"),
            col("partition").alias("kafka_partition"),
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), schema).alias("data")
        )
        .select(
            "kafka_key",
            "kafka_offset",
            "kafka_partition",
            "kafka_timestamp",
            "data.*"
        )
        .withColumn("ingested_at", current_timestamp())
        .withColumn("year",  year(col("kafka_timestamp")))
        .withColumn("month", month(col("kafka_timestamp")))
        .withColumn("day",   dayofmonth(col("kafka_timestamp")))
        .withColumn("hour",  hour(col("kafka_timestamp")))
    )


def write_delta_stream(parsed_df, topic: str):
    return (
        parsed_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/{topic}")
        .partitionBy("year", "month", "day", "hour")
        .trigger(processingTime="30 seconds")
        .start(f"{DELTA_BASE}/{topic}")
    )


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("Building Spark session...")
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    print(f"Spark version: {spark.version}")

    queries = []
    for topic, schema in TOPICS.items():
        print(f"Starting stream: {topic}")
        raw   = read_kafka_topic(spark, topic)
        parsed = parse_topic(raw, schema)
        query  = write_delta_stream(parsed, topic)
        queries.append(query)
        print(f"  ✓ Stream started for {topic} → {DELTA_BASE}/{topic}")

    print(f"\nAll {len(queries)} streams running. Waiting for termination...")
    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()