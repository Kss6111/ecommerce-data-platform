"""
verify_delta.py
Verifies Delta Lake tables and demonstrates time travel on orders_stream.
"""

import os
from pyspark.sql import SparkSession

S3_BUCKET  = os.getenv("S3_BUCKET_NAME", "ecommerce-data-platform-krutarth-2025")
DELTA_BASE = f"s3a://{S3_BUCKET}/raw/kafka"

def build_spark():
    return (
        SparkSession.builder
        .appName("verify-delta")
        .master("spark://spark-master:7077")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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
        .getOrCreate()
    )

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    topics = ["orders_stream", "clickstream", "inventory_updates"]

    print("\n" + "="*60)
    print("DELTA LAKE VERIFICATION")
    print("="*60)

    # ── Row counts across all three tables ────────────────────────
    for topic in topics:
        path = f"{DELTA_BASE}/{topic}"
        df   = spark.read.format("delta").load(path)
        print(f"\n{topic}:")
        print(f"  Total rows      : {df.count():,}")
        print(f"  Columns         : {len(df.columns)}")
        print(f"  Partitions used : year, month, day, hour")
        df.printSchema()

    # ── Time travel on orders_stream ──────────────────────────────
    print("\n" + "="*60)
    print("TIME TRAVEL — orders_stream")
    print("="*60)

    orders_path = f"{DELTA_BASE}/orders_stream"

    # Current version
    df_current = spark.read.format("delta").load(orders_path)
    current_count = df_current.count()
    print(f"\nCurrent version  : {current_count:,} rows")

    # Version 0 — the very first batch ever written
    df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(orders_path)
    v0_count = df_v0.count()
    print(f"Version 0        : {v0_count:,} rows")

    # Version 1
    df_v1 = spark.read.format("delta").option("versionAsOf", 1).load(orders_path)
    v1_count = df_v1.count()
    print(f"Version 1        : {v1_count:,} rows")

    print(f"\nRows added after v0: {current_count - v0_count:,}")

    # ── Sample rows from current orders_stream ────────────────────
    print("\n" + "="*60)
    print("SAMPLE — orders_stream (5 rows, selected columns)")
    print("="*60)
    df_current.select(
        "event_id", "event_type", "order_status",
        "total_amount", "payment_method", "ingested_at"
    ).show(5, truncate=False)

    print("\n✅ Delta Lake verification complete.")
    spark.stop()

if __name__ == "__main__":
    main()