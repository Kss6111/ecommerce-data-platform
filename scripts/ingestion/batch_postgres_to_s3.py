"""
batch_postgres_to_s3.py
Reads all five PostgreSQL tables and uploads them as Parquet files to S3
under raw/postgres/<table>/<year>/<month>/<day>/<table>_<timestamp>.parquet

Incremental logic: tracks last_extracted_at per table in a local state file
(ingestion_state.json). On first run, does a full extract. On subsequent
runs, only pulls rows where created_at > last_extracted_at.

Tables without created_at (order_items) always do a full extract.
"""

import os
import json
import io
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg2
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5433)),
    "dbname":   os.getenv("POSTGRES_DB",   "ecommerce_db"),
    "user":     os.getenv("POSTGRES_USER", "ecommerce_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "ecommerce_pass"),
}

S3_BUCKET  = os.getenv("S3_BUCKET_NAME", "ecommerce-data-platform-krutarth-2025")
S3_PREFIX  = "raw/postgres"
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

STATE_FILE = Path(__file__).parent / "ingestion_state.json"

# Tables that support incremental extraction via created_at
INCREMENTAL_TABLES = {
    "customers":      "created_at",
    "suppliers":      "created_at",
    "products":       "created_at",
    "orders_history": "created_at",
}

# Tables that are always fully extracted
FULL_EXTRACT_TABLES = ["order_items"]


# ── State management ──────────────────────────────────────────────────────────
def load_state() -> dict:
    if STATE_FILE.exists():
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {}


def save_state(state: dict) -> None:
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2, default=str)
    log.info("State saved to %s", STATE_FILE)


# ── DB helpers ────────────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def extract_incremental(
    conn,
    table: str,
    ts_column: str,
    last_extracted_at: str | None,
) -> pd.DataFrame:
    if last_extracted_at:
        query = f"""
            SELECT * FROM {table}
            WHERE {ts_column} > %(last_ts)s
            ORDER BY {ts_column} ASC
        """
        log.info("  Incremental extract: %s where %s > %s", table, ts_column, last_extracted_at)
        df = pd.read_sql_query(query, conn, params={"last_ts": last_extracted_at})
    else:
        query = f"SELECT * FROM {table} ORDER BY {ts_column} ASC"
        log.info("  Full extract (first run): %s", table)
        df = pd.read_sql_query(query, conn)

    log.info("  Extracted %s rows from %s", f"{len(df):,}", table)
    return df


def extract_full(conn, table: str) -> pd.DataFrame:
    log.info("  Full extract: %s", table)
    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    log.info("  Extracted %s rows from %s", f"{len(df):,}", table)
    return df


# ── Parquet helpers ───────────────────────────────────────────────────────────
def df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    """Serialise DataFrame to Parquet in memory."""
    buffer = io.BytesIO()
    df.to_parquet(
        buffer,
        engine="pyarrow",
        index=False,
        compression="snappy",
    )
    return buffer.getvalue()


def serialize_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Snowflake can misread native Parquet timestamps produced from pandas.
    Persisting ISO-8601 strings keeps downstream COPY INTO deterministic.
    """
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            out[col] = out[col].str.replace(r"([+-]\d{2})(\d{2})$", r"\1:\2", regex=True)
    return out


def build_s3_key(table: str, run_ts: datetime) -> str:
    """
    Pattern: raw/postgres/<table>/year=YYYY/month=MM/day=DD/<table>_<timestamp>.parquet
    Hive-style partitioning so Spark/Athena can auto-discover partitions.
    """
    y  = run_ts.strftime("%Y")
    m  = run_ts.strftime("%m")
    d  = run_ts.strftime("%d")
    ts = run_ts.strftime("%Y%m%d_%H%M%S")
    return f"{S3_PREFIX}/{table}/year={y}/month={m}/day={d}/{table}_{ts}.parquet"


# ── S3 helpers ────────────────────────────────────────────────────────────────
def get_s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )


def upload_to_s3(s3_client, data: bytes, s3_key: str) -> None:
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=data,
            ContentType="application/octet-stream",
        )
        size_kb = len(data) / 1024
        log.info(f"  ✓ Uploaded s3://{S3_BUCKET}/{s3_key} ({size_kb:.1f} KB)")
    except ClientError as e:
        log.error(f"  ✗ S3 upload failed for {s3_key}: {e}")
        raise


def verify_s3_object(s3_client, s3_key: str) -> bool:
    """Confirm the object exists and return its size."""
    try:
        resp = s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
        size_kb = resp["ContentLength"] / 1024
        log.info(f"  ✓ Verified: {s3_key} ({size_kb:.1f} KB)")
        return True
    except ClientError:
        log.error(f"  ✗ Verification failed: {s3_key} not found in S3")
        return False


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    run_ts = datetime.now(timezone.utc)
    log.info("=" * 60)
    log.info(f"Batch ingestion started at {run_ts.isoformat()}")
    log.info(f"Target: s3://{S3_BUCKET}/{S3_PREFIX}/")
    log.info("=" * 60)

    state     = load_state()
    s3_client = get_s3_client()
    conn      = get_conn()

    results = {}

    # ── Incremental tables ────────────────────────────────────────
    for table, ts_col in INCREMENTAL_TABLES.items():
        log.info(f"\n[INCREMENTAL] {table}")
        last_ts = state.get(table, {}).get("last_extracted_at")

        df = extract_incremental(conn, table, ts_col, last_ts)

        if df.empty:
            log.info(f"  No new rows since last run — skipping upload")
            results[table] = {"rows": 0, "skipped": True}
            continue

        # Convert UUID and timestamp columns to strings for Parquet compatibility
        for col in df.columns:
            if str(df[col].dtype) == "object":
                df[col] = df[col].astype(str)

        parquet_bytes = df_to_parquet_bytes(serialize_datetime_columns(df))
        s3_key        = build_s3_key(table, run_ts)

        upload_to_s3(s3_client, parquet_bytes, s3_key)
        verify_s3_object(s3_client, s3_key)

        # Update state with latest timestamp from this batch
        max_ts = df[ts_col].max()
        state[table] = {
            "last_extracted_at": str(max_ts),
            "last_run_at":       run_ts.isoformat(),
            "last_row_count":    len(df),
            "last_s3_key":       s3_key,
        }
        results[table] = {"rows": len(df), "skipped": False, "s3_key": s3_key}

    # ── Full extract tables ───────────────────────────────────────
    for table in FULL_EXTRACT_TABLES:
        log.info(f"\n[FULL EXTRACT] {table}")

        df = extract_full(conn, table)

        if df.empty:
            log.info(f"  Table is empty — skipping upload")
            results[table] = {"rows": 0, "skipped": True}
            continue

        for col in df.columns:
            if str(df[col].dtype) == "object":
                df[col] = df[col].astype(str)

        parquet_bytes = df_to_parquet_bytes(serialize_datetime_columns(df))
        s3_key        = build_s3_key(table, run_ts)

        upload_to_s3(s3_client, parquet_bytes, s3_key)
        verify_s3_object(s3_client, s3_key)

        state[table] = {
            "last_run_at":    run_ts.isoformat(),
            "last_row_count": len(df),
            "last_s3_key":    s3_key,
        }
        results[table] = {"rows": len(df), "skipped": False, "s3_key": s3_key}

    conn.close()
    save_state(state)

    # ── Summary ───────────────────────────────────────────────────
    log.info("\n" + "=" * 60)
    log.info("INGESTION SUMMARY")
    log.info("=" * 60)
    for table, r in results.items():
        if r["skipped"]:
            log.info(f"  {table:<20} SKIPPED (no new rows)")
        else:
            log.info(f"  {table:<20} {r['rows']:>6} rows → {r['s3_key']}")
    log.info("=" * 60)
    log.info("✅ Batch ingestion complete.")


if __name__ == "__main__":
    main()
