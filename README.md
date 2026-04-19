# E-Commerce Data Platform

An end-to-end enterprise data engineering platform that ingests real-time 
and batch data, processes it through a modern lakehouse architecture, 
transforms it using dbt, and serves business insights via a live dashboard.

---

## Architecture

Real-time order events, clickstream data, and inventory updates flow 
through Apache Kafka into Spark Structured Streaming, landing in Delta 
Lake format on AWS S3. Simultaneously, operational data from PostgreSQL 
is batch-extracted to S3 as partitioned Parquet files. Apache Airflow 
orchestrates both pipelines on schedule. dbt transforms raw Snowflake 
data through staging and intermediate layers into clean mart models. 
Great Expectations validates data quality at the raw layer. Metabase 
serves the business dashboard on top of Snowflake marts.

---

## Tech Stack

| Layer | Technology |
|-------|------------|
| Event streaming | Apache Kafka + Zookeeper |
| Stream processing | Spark Structured Streaming |
| Batch processing | PySpark |
| Orchestration | Apache Airflow |
| Data lake storage | AWS S3 + Delta Lake |
| Operational database | PostgreSQL |
| Data warehouse | Snowflake |
| Transformation | dbt Core |
| Data quality | Great Expectations + dbt tests |
| Infrastructure as code | Terraform |
| CI/CD | GitHub Actions |
| Visualization | Metabase |
| Data simulation | Python Faker |
| Containerization | Docker + Docker Compose |

---

## Architecture Diagram
[Python Faker]          [Python Faker]
|                       |
[PostgreSQL]           [Kafka Topics]
|                (orders_stream)
|               (clickstream)
|               (inventory_updates)
|                       |
[Batch Extract]     [Spark Structured Streaming]
(PySpark → Parquet) (PySpark → Delta Lake)
|                       |
└─────────┬─────────────┘
|
[AWS S3 Data Lake]
raw/postgres/     ← Parquet
raw/kafka/        ← Delta Lake
|
[Airflow DAGs]
(orchestrate loads)
|
[Snowflake RAW Schema]
|
[dbt Core]
staging → intermediate → marts
|
[Great Expectations]
(data quality gates)
|
[Snowflake MARTS Schema]
|
[Metabase]
(business dashboard)

---

## Local Services

| Service | Container | Port |
|---------|-----------|------|
| PostgreSQL | ecommerce_postgres | 5433 |
| Zookeeper | ecommerce_zookeeper | 2181 |
| Kafka | ecommerce_kafka | 9092 / 29092 |
| Kafka UI | ecommerce_kafka_ui | 8080 |
| Spark Master | ecommerce_spark_master | 7077 / 8181 |
| Spark Worker | ecommerce_spark_worker | 8182 |

---

## Cloud Infrastructure

| Resource | Details |
|----------|---------|
| AWS S3 Bucket | ecommerce-data-platform-krutarth-2025 |
| AWS Region | us-east-1 |
| Snowflake Database | ECOMMERCE_DB |
| Snowflake Schemas | RAW, STAGING, MARTS, METADATA |
| Snowflake Warehouse | ECOMMERCE_WH |
| Snowflake Stages | S3_RAW_STAGE, S3_STAGING_STAGE |

---

## Data Volumes (current)

| Source | Destination | Format | Rows |
|--------|-------------|--------|------|
| PostgreSQL customers | S3 raw/postgres/ | Parquet | 1,000 |
| PostgreSQL products | S3 raw/postgres/ | Parquet | 500 |
| PostgreSQL suppliers | S3 raw/postgres/ | Parquet | 100 |
| PostgreSQL orders_history | S3 raw/postgres/ | Parquet | 5,265 |
| PostgreSQL order_items | S3 raw/postgres/ | Parquet | ~19,800 |
| Kafka orders_stream | S3 raw/kafka/ | Delta Lake | 2,812 |
| Kafka clickstream | S3 raw/kafka/ | Delta Lake | 11,911 |
| Kafka inventory_updates | S3 raw/kafka/ | Delta Lake | 3,073 |

---

## Project Status

| Phase | Description | Status |
|-------|-------------|--------|
| Phase 1 | Environment setup | ✅ Complete |
| Phase 2 | Data generation and ingestion | ✅ Complete |
| Phase 3 | Airflow orchestration | 🔄 In Progress |
| Phase 4 | dbt transformations | ⏳ Pending |
| Phase 5 | Data quality | ⏳ Pending |
| Phase 6 | Terraform and infrastructure | ⏳ Pending |
| Phase 7 | CI/CD | ⏳ Pending |
| Phase 8 | Dashboard and launch | ⏳ Pending |

---

## What Is Built

**Phase 1 — Environment Setup**
Full local environment running six Docker services via Docker Compose. 
AWS S3 bucket with structured folder layout for raw, staging, marts, 
delta, checkpoints, logs, and terraform paths. Snowflake database with 
four schemas, dedicated warehouse, role-based access control, external 
stage, and IAM trust integration linking Snowflake to S3.

**Phase 2 — Data Generation and Ingestion**
Python Faker scripts seeding 1,000 customers, 500 products, 100 
suppliers, 5,265 historical orders, and ~19,800 order items into 
PostgreSQL. Three Kafka producers continuously publishing realistic 
order events, clickstream events, and inventory updates. Batch 
extraction pipeline reading all PostgreSQL tables incrementally and 
writing date-partitioned Parquet files to S3. Spark Structured 
Streaming jobs consuming all three Kafka topics in real time and 
writing to S3 in Delta Lake format with ACID transactions and time 
travel verified across 17 commits.

---

## Repository Structure
ecommerce-data-platform/
├── kafka/
│   ├── producers/          # Kafka producer scripts
│   ├── consumers/          # Kafka consumer scripts
│   └── config/             # Kafka configuration
├── spark/
│   ├── streaming/          # Spark Structured Streaming jobs
│   ├── batch/              # PySpark batch processing jobs
│   └── utils/              # Shared Spark utilities
├── airflow/
│   ├── dags/               # Airflow DAG definitions
│   └── plugins/            # Custom Airflow plugins
├── dbt/
│   ├── models/
│   │   ├── staging/        # Staging models (light cleaning)
│   │   ├── intermediate/   # Intermediate models (business logic)
│   │   └── marts/          # Mart models (serving layer)
│   ├── tests/              # Custom dbt tests
│   ├── macros/             # Reusable dbt macros
│   └── snapshots/          # SCD Type 2 snapshots
├── postgres/
│   └── init/               # PostgreSQL init SQL scripts
├── terraform/
│   └── modules/            # Terraform modules for AWS
├── scripts/                # Python ingestion and utility scripts
├── tests/                  # Connection and integration tests
├── docs/                   # Documentation and diagrams
├── .github/workflows/      # GitHub Actions CI/CD pipelines
├── docker-compose.yml      # All local services
├── .env.example            # Environment variable template
└── requirements.txt        # Python dependencies

---

## Getting Started

### Prerequisites
- Docker Desktop running
- Python 3.11
- AWS CLI configured
- Snowflake trial account

### Setup

```bash
# Clone the repository
git clone https://github.com/Kss6111/ecommerce-data-platform.git
cd ecommerce-data-platform

# Create virtual environment
python -m venv venv
venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Copy environment template and fill in credentials
copy .env.example .env

# Start all services
docker-compose up -d

# Verify everything is running
docker ps
```

---

## Key Design Decisions

**Why Delta Lake over plain Parquet** — ACID transactions, time travel 
for debugging, and schema enforcement on object storage. Essential for 
production streaming pipelines where bad writes need to be recoverable.

**Why S3 sits between sources and Snowflake** — S3 acts as an immutable 
raw archive. If anything goes wrong in Snowflake or dbt, data can be 
reprocessed from S3 without touching source systems again. This 
decouples PostgreSQL from Snowflake entirely.

**Why both batch and streaming** — Operational data in PostgreSQL 
changes slowly and suits batch extraction. Event data from Kafka is 
continuous and time-sensitive. A real enterprise platform handles both. 
dbt unifies them in Snowflake at the transformation layer.

**Why local Docker over managed cloud services** — AWS MSK costs 
~$150/month, MWAA costs ~$300/month. Running locally in Docker is free, 
teaches the same concepts, and the architecture is identical. Cloud 
deployment is the final phase.

---

*Built by Krutarth Shah — CMU Heinz College MISM*