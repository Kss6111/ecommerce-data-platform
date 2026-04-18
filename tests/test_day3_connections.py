import os
import boto3
import snowflake.connector
from dotenv import load_dotenv
from botocore.exceptions import ClientError

load_dotenv()

GREEN = "\033[92m"
RED   = "\033[91m"
CYAN  = "\033[96m"
RESET = "\033[0m"

def ok(msg):     print(f"  {GREEN}OK{RESET}  {msg}")
def fail(msg):   print(f"  {RED}FAIL{RESET}  {msg}"); exit(1)
def header(msg): print(f"\n{CYAN}{'='*50}\n  {msg}\n{'='*50}{RESET}")


# ── TEST 1: S3 ──────────────────────────────────────
header("TEST 1: AWS S3")
bucket = os.getenv("S3_BUCKET_NAME")
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)
try:
    s3.head_bucket(Bucket=bucket)
    ok(f"Bucket accessible: {bucket}")
except ClientError as e:
    fail(f"S3 access failed: {e}")

s3.put_object(Bucket=bucket, Key="logs/spark/connection_test.txt", Body=b"day3-test")
obj = s3.get_object(Bucket=bucket, Key="logs/spark/connection_test.txt")
assert obj["Body"].read() == b"day3-test"
s3.delete_object(Bucket=bucket, Key="logs/spark/connection_test.txt")
ok("S3 read/write/delete passed")

resp = s3.list_objects_v2(Bucket=bucket, Delimiter="/")
folders = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
ok(f"Top-level folders: {folders}")


# ── TEST 2: Snowflake connection ────────────────────
header("TEST 2: Snowflake Connection")
ctx = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    role=os.getenv("SNOWFLAKE_ROLE"),
)
cur = ctx.cursor()
ok("Snowflake connected")

cur.execute("SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_ROLE()")
wh, db, role = cur.fetchone()
ok(f"Warehouse: {wh}  DB: {db}  Role: {role}")


# ── TEST 3: Schemas ─────────────────────────────────
header("TEST 3: Snowflake Schemas")
cur.execute("SHOW SCHEMAS IN DATABASE ECOMMERCE_DB")
schemas = {row[1] for row in cur.fetchall()}
for s in ["RAW", "STAGING", "MARTS", "METADATA"]:
    if s in schemas:
        ok(f"Schema exists: {s}")
    else:
        fail(f"Schema MISSING: {s}")


# ── TEST 4: Stages ──────────────────────────────────
header("TEST 4: Snowflake Stages")
cur.execute("SHOW STAGES IN SCHEMA ECOMMERCE_DB.RAW")
stages = {row[1] for row in cur.fetchall()}
for stage in ["S3_RAW_STAGE", "S3_STAGING_STAGE"]:
    if stage in stages:
        ok(f"Stage exists: {stage}")
    else:
        fail(f"Stage MISSING: {stage}")

cur.execute("LIST @ECOMMERCE_DB.RAW.S3_RAW_STAGE")
rows = cur.fetchall()
ok(f"LIST @S3_RAW_STAGE returned {len(rows)} files — trust handshake working")


# ── TEST 5: Write test ──────────────────────────────
header("TEST 5: Snowflake Write")
cur.execute("USE SCHEMA ECOMMERCE_DB.METADATA")
cur.execute("""
    CREATE TABLE IF NOT EXISTS PIPELINE_RUNS (
        run_id    VARCHAR,
        pipeline  VARCHAR,
        status    VARCHAR,
        run_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
""")
cur.execute("INSERT INTO PIPELINE_RUNS (run_id, pipeline, status) VALUES ('day3-001', 'connection_test', 'SUCCESS')")
ctx.commit()
cur.execute("SELECT COUNT(*) FROM PIPELINE_RUNS")
ok(f"Write confirmed — row count: {cur.fetchone()[0]}")

cur.close()
ctx.close()

print(f"\n{GREEN}{'='*50}")
print("  ALL TESTS PASSED — Day 3 complete!")
print(f"{'='*50}{RESET}\n")