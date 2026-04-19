#!/bin/bash
# Submits kafka_to_delta.py to the Spark master inside the container.
# Called via: docker exec ecommerce_spark_master bash /opt/spark-apps/run_streaming.sh

set -e

PACKAGES=(
  "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
  "io.delta:delta-spark_2.12:3.0.0"
  "org.apache.hadoop:hadoop-aws:3.3.4"
  "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

PACKAGES_STR=$(IFS=,; echo "${PACKAGES[*]}")

/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages "${PACKAGES_STR}" \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID}" \
  --conf "spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY}" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
  --conf "spark.hadoop.fs.s3a.endpoint=s3.${AWS_REGION}.amazonaws.com" \
  /opt/spark-apps/kafka_to_delta.py