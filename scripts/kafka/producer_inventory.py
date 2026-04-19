"""
producer_inventory.py
Generates synthetic inventory update events and publishes them to
the inventory_updates Kafka topic. Runs continuously until Ctrl+C.
"""

import json
import os
import random
import signal
import time
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import register_uuid
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

from schemas import InventoryEvent, now_iso, new_id

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC            = "inventory_updates"
PUBLISH_INTERVAL = 2.0
EVENTS_PER_TICK  = 8

DB_CONFIG = {
    "host":     os.getenv("PG_HOST", "localhost"),
    "port":     int(os.getenv("PG_PORT", 5433)),
    "dbname":   os.getenv("PG_DB",   "ecommerce_db"),
    "user":     os.getenv("PG_USER", "ecommerce_user"),
    "password": os.getenv("PG_PASSWORD", "ecommerce_pass"),
}

EVENT_TYPES = ["restock", "sale_deduction", "adjustment", "low_stock_alert"]
EVENT_WEIGHTS = [20, 50, 20, 10]

WAREHOUSES = ["WH-EAST-01", "WH-WEST-01", "WH-CENTRAL-01", "WH-SOUTH-01"]

REASONS = {
    "restock":         ["supplier_delivery", "return_processed", "transfer_in"],
    "sale_deduction":  ["order_fulfilled", "bulk_order", "subscription_fulfillment"],
    "adjustment":      ["inventory_audit", "damage_writeoff", "expiry_removal"],
    "low_stock_alert": ["auto_threshold_trigger", "manual_review"],
}

TRIGGERED_BY = {
    "restock":         "system",
    "sale_deduction":  "order_fulfillment",
    "adjustment":      "manual",
    "low_stock_alert": "system",
}

# ── State ─────────────────────────────────────────────────────────────────────
running = True
total_published = 0


def handle_sigint(sig, frame):
    global running
    print(f"\nStopping... Total published: {total_published} messages.")
    running = False


signal.signal(signal.SIGINT, handle_sigint)


# ── Kafka helpers ─────────────────────────────────────────────────────────────
def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP,
        "acks":               "all",
        "retries":            5,
        "retry.backoff.ms":   500,
        "enable.idempotence": True,
        "compression.type":   "snappy",
        "batch.size":         16384,
        "linger.ms":          10,
    })


def ensure_topic_exists():
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    metadata = admin.list_topics(timeout=5)
    if TOPIC not in metadata.topics:
        print(f"  Topic '{TOPIC}' not found — creating...")
        admin.create_topics([NewTopic(TOPIC, num_partitions=3, replication_factor=1)])
        time.sleep(2)
    else:
        print(f"  Topic '{TOPIC}' confirmed.")


def delivery_report(err, msg):
    if err:
        print(f"  ✗ Delivery failed: {err}")


# ── DB helpers ────────────────────────────────────────────────────────────────
def get_conn():
    conn = psycopg2.connect(**DB_CONFIG)
    register_uuid()
    return conn


def fetch_product_sample(cur, n: int = 100) -> list[dict]:
    cur.execute(
        """
        SELECT product_id::text, sku, stock_quantity
        FROM products
        WHERE is_active = true
        ORDER BY random()
        LIMIT %s
        """,
        (n,)
    )
    return [
        {"product_id": r[0], "sku": r[1], "stock_quantity": r[2]}
        for r in cur.fetchall()
    ]


# ── Event builder ─────────────────────────────────────────────────────────────
def build_inventory_event(product: dict) -> InventoryEvent:
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
    qty_before = max(0, product["stock_quantity"])

    if event_type == "restock":
        qty_change = random.randint(10, 200)
    elif event_type == "sale_deduction":
        qty_change = -random.randint(1, min(10, max(1, qty_before)))
    elif event_type == "adjustment":
        qty_change = random.randint(-20, 20)
    else:  # low_stock_alert — no actual quantity change
        qty_change = 0

    qty_after = max(0, qty_before + qty_change)

    return InventoryEvent(
        event_id        = new_id(),
        event_type      = event_type,
        event_timestamp = now_iso(),
        product_id      = product["product_id"],
        sku             = product["sku"],
        warehouse_id    = random.choice(WAREHOUSES),
        quantity_before = qty_before,
        quantity_change = qty_change,
        quantity_after  = qty_after,
        reason          = random.choice(REASONS[event_type]),
        triggered_by    = TRIGGERED_BY[event_type],
    )


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    global total_published

    print(f"Starting inventory_updates producer → topic: {TOPIC}")
    ensure_topic_exists()

    producer = make_producer()
    conn     = get_conn()

    print(f"Publishing {EVENTS_PER_TICK} events every {PUBLISH_INTERVAL}s. Press Ctrl+C to stop.\n")

    cache_refresh_at = 0
    products: list[dict] = []

    while running:
        try:
            now = time.time()
            if now >= cache_refresh_at:
                with conn.cursor() as cur:
                    products = fetch_product_sample(cur, 100)
                cache_refresh_at = now + 60

            for _ in range(EVENTS_PER_TICK):
                product = random.choice(products)
                event   = build_inventory_event(product)
                producer.produce(
                    topic    = TOPIC,
                    key      = product["product_id"],
                    value    = json.dumps(event.to_dict()),
                    callback = delivery_report,
                )

            producer.poll(0)
            producer.flush(5)

            total_published += EVENTS_PER_TICK
            ts = datetime.now().strftime("%H:%M:%S")
            print(f"[{ts}] Published {EVENTS_PER_TICK} inventory events | total: {total_published}")

        except psycopg2.OperationalError as e:
            print(f"  ✗ DB error: {e} — reconnecting...")
            try:
                conn.close()
            except Exception:
                pass
            time.sleep(3)
            conn = get_conn()
            cache_refresh_at = 0

        except Exception as e:
            print(f"  ✗ Error: {e}")

        time.sleep(PUBLISH_INTERVAL)

    producer.flush(10)
    conn.close()
    print("Inventory producer stopped cleanly.")


if __name__ == "__main__":
    main()