# Data Quality Documentation

## Overview

This project uses [Great Expectations](https://greatexpectations.io/) to validate data at two points in the pipeline:

1. **Batch pipeline** (`batch_ingestion_dag`) — validates raw S3 Parquet files before loading into Snowflake RAW
2. **Streaming pipeline** (`streaming_load_dag`) — validates new Delta Lake Parquet files before each COPY INTO

A validation failure raises an `AirflowException` that blocks the downstream Snowflake load, preventing bad data from entering the warehouse.

---

## Batch Suites

### `customers_raw_suite`
**File:** `data_quality/validate_customers.py`
**Triggered by:** `validate_customers_quality` task in `batch_ingestion_dag`
**Data:** `s3://ecommerce-data-platform-krutarth-2025/raw/postgres/customers/run_date={date}/customers.parquet`

| Expectation | Purpose |
|---|---|
| All 12 columns present | Schema drift detection |
| Column count = 12 | No extra columns introduced |
| No nulls on `customer_id`, `email`, `first_name`, `last_name`, `created_at`, `updated_at` | Critical identity fields must be populated |
| `customer_id` unique | No duplicate customers |
| `email` unique | One account per email address |
| `email` matches regex | Valid email format (99% threshold) |
| Row count between 800–50,000 | Alerts if extraction drops significantly or explodes unexpectedly |
| `created_at`, `updated_at` between 2020–2030 | Timestamp plausibility — catches epoch errors and far-future dates |

---

### `orders_history_raw_suite`
**File:** `data_quality/validate_orders_history.py`
**Triggered by:** `validate_orders_history_quality` task in `batch_ingestion_dag`
**Data:** `s3://ecommerce-data-platform-krutarth-2025/raw/postgres/orders_history/run_date={date}/orders_history.parquet`

| Expectation | Purpose |
|---|---|
| All 10 columns present | Schema drift detection |
| Column count = 10 | No extra columns introduced |
| No nulls on `order_id`, `customer_id`, `order_status`, `total_amount`, `payment_method`, `order_date`, `created_at` | Core transactional fields must be populated |
| `shipped_date`, `delivered_date` nullable | Valid — pending/processing orders have no ship date yet |
| `order_id` unique | No duplicate orders |
| `order_status` in valid set | Catches invalid status values from upstream bugs |
| `payment_method` in valid set | Catches new payment methods not yet handled downstream |
| `total_amount` between 0.01–100,000 | No zero, negative, or implausibly large order amounts |
| Row count between 4,000–500,000 | Alerts if batch loses significant rows (baseline: 5,265) |
| `order_date`, `created_at` between 2020–2030 | Timestamp plausibility |
| `order_id`, `customer_id` match UUID regex | Catches non-UUID format IDs from data generation issues |

---

## Streaming Suites

### `orders_stream_raw_suite`
**File:** `data_quality/validate_streaming.py`
**Triggered by:** `validate_orders_stream_quality` task in `streaming_load_dag`
**Data:** `s3://ecommerce-data-platform-krutarth-2025/raw/kafka/orders_stream/` (Delta Lake)

| Expectation | Purpose |
|---|---|
| All 17 columns present | Schema drift detection |
| No nulls on key fields (mostly=0.999) | Tolerates 1 bootstrap sentinel record from Day 1 |
| `event_id` unique | No duplicate events |
| `event_type` in `[order_created, order_updated, order_cancelled]` | Catches unknown event types from producer bugs |
| `order_status` in valid set | Status must be a known value |
| `payment_method` in valid set | Payment method must be a known value |
| `source` = `postgres_cdc` | Events must come from the CDC pipeline only |
| `total_amount` between 0.01–100,000 | No invalid order amounts |
| `item_count` between 1–1,000 | No zero or implausibly large item counts |
| `event_id`, `order_id`, `customer_id` match UUID regex (mostly=0.999) | Tolerates 1 bootstrap sentinel record |
| `customer_id` in known customers set (mostly=0.99) | **Referential integrity** — every order must belong to a known customer |

#### Referential Integrity Check
The `customer_id` column in `orders_stream` is validated against the latest `customers` Parquet file from S3. This ensures every streaming order event references a real customer. A 99% threshold (`mostly=0.99`) is applied to tolerate edge cases where a customer was created within the same processing window.

---

### `clickstream_raw_suite`
**File:** `data_quality/validate_streaming.py`
**Triggered by:** `validate_clickstream_quality` task in `streaming_load_dag`
**Data:** `s3://ecommerce-data-platform-krutarth-2025/raw/kafka/clickstream/` (Delta Lake)

| Expectation | Purpose |
|---|---|
| All 18 columns present | Schema drift detection |
| No nulls on `event_id`, `event_type`, `session_id`, `page_url`, `device_type`, `time_on_page_ms`, `source`, `ingested_at` (mostly=0.999) | Core fields must be populated |
| `user_id`, `product_id`, `referrer` nullable | Valid — anonymous users and non-product pages have no user/product ID |
| `event_id` unique | No duplicate events |
| `event_type` in `[page_view, product_view, add_to_cart, checkout, purchase]` | Catches unknown event types |
| `device_type` in `[desktop, mobile, tablet]` | Catches invalid device classifications |
| `source` = `web_frontend` | Events must come from the web frontend only |
| `time_on_page_ms` between 0–3,600,000 | No negative times or sessions over 1 hour |

---

### `inventory_updates_raw_suite`
**File:** `data_quality/validate_streaming.py`
**Triggered by:** `validate_inventory_updates_quality` task in `streaming_load_dag`
**Data:** `s3://ecommerce-data-platform-krutarth-2025/raw/kafka/inventory_updates/` (Delta Lake)

| Expectation | Purpose |
|---|---|
| All 17 columns present | Schema drift detection |
| No nulls on all critical fields (mostly=0.999) | Core inventory fields must be populated |
| `event_id` unique | No duplicate events |
| `event_type` in `[sale_deduction, restock, adjustment, low_stock_alert]` | Catches unknown inventory event types |
| `reason` in valid set (11 values) | Reason must be a known business reason code |
| `triggered_by` in `[manual, order_fulfillment, system]` | Trigger source must be known |
| `source` = `inventory_service` | Events must come from the inventory service only |
| `quantity_before` >= 0 | No negative stock levels before update |
| `quantity_after` >= 0 | No negative stock levels after update |
| `quantity_change` between -10,000–100,000 | Catches wildly incorrect quantity deltas |

---

## Validation Thresholds

| Threshold | Meaning |
|---|---|
| `mostly=1.0` (default) | 100% of rows must pass |
| `mostly=0.999` | Tolerates up to 1 row in 1,000 failing — used for streaming data to handle bootstrap sentinel records |
| `mostly=0.99` | Tolerates up to 1% failure — used for referential integrity to handle same-window new customers |

---

## Running Validations Manually

```bash
# Batch validations
python data_quality/validate_customers.py 2026-04-21
python data_quality/validate_orders_history.py 2026-04-21

# Streaming validations
python data_quality/validate_streaming.py orders_stream
python data_quality/validate_streaming.py clickstream
python data_quality/validate_streaming.py inventory_updates
python data_quality/validate_streaming.py all
```

## HTML Reports

GE generates HTML data docs after each validation run: