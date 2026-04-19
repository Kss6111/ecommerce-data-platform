"""
producer_orders.py
Reads recent orders from PostgreSQL and publishes them to
the orders_stream Kafka topic as OrderEvent JSON messages.
Runs continuously until Ctrl+C.
"""

import json
import os
import random
import signal
import time
import uuid
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import register_uuid
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

from schemas import OrderEvent, now_iso, new_id

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC           = "orders_stream"
POLL_INTERVAL   = 3.0        # seconds between polling PostgreSQL
BATCH_SIZE      = 10         # orders to publish per poll

DB_CONFIG = {
    "host":     os.getenv("PG_HOST", "localhost"),
    "port":     int(os.getenv("PG_PORT", 5433)),
    "dbname":   os.getenv("PG_DB",   "ecommerce_db"),
    "user":     os.getenv("PG_USER", "ecommerce_user"),
    "password": os.getenv("PG_PASSWORD", "ecommerce_pass"),
}

EVENT_TYPES = ["order_created", "order_updated", "order_cancelled"]

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
        "bootstrap.servers":       KAFKA_BOOTSTRAP,
        "acks":                    "all",           # wait for all replicas
        "retries":                 5,
        "retry.backoff.ms":        500,
        "delivery.timeout.ms":     10000,
        "enable.idempotence":      True,
        "compression.type":        "snappy",
        "batch.size":              16384,
        "linger.ms":               10,
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


def fetch_recent_orders(cur, n: int) -> list[dict]:
    cur.execute(
        """
        SELECT
            oh.order_id::text,
            oh.customer_id::text,
            oh.order_status,
            oh.payment_method,
            oh.total_amount,
            oh.shipping_address,
            COUNT(oi.order_item_id) AS item_count
        FROM orders_history oh
        LEFT JOIN order_items oi ON oh.order_id = oi.order_id
        GROUP BY oh.order_id, oh.customer_id, oh.order_status,
                 oh.payment_method, oh.total_amount, oh.shipping_address
        ORDER BY oh.order_date DESC
        LIMIT %s
        """,
        (n,)
    )
    cols = ["order_id", "customer_id", "order_status", "payment_method",
            "total_amount", "shipping_address", "item_count"]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def parse_city_country(shipping_address: str) -> tuple[str, str]:
    """Best-effort extraction from free-text address."""
    parts = shipping_address.split(",")
    city    = parts[1].strip() if len(parts) > 1 else "Unknown"
    country = parts[-1].strip() if len(parts) > 2 else "US"
    return city[:50], country[:50]


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    global total_published

    print(f"Starting orders_stream producer → topic: {TOPIC}")
    ensure_topic_exists()

    producer = make_producer()
    conn     = get_conn()

    print(f"Publishing {BATCH_SIZE} events every {POLL_INTERVAL}s. Press Ctrl+C to stop.\n")

    while running:
        try:
            with conn.cursor() as cur:
                orders = fetch_recent_orders(cur, BATCH_SIZE)

            for order in orders:
                city, country = parse_city_country(order["shipping_address"] or "")
                event = OrderEvent(
                    event_id        = new_id(),
                    event_type      = random.choice(EVENT_TYPES),
                    event_timestamp = now_iso(),
                    order_id        = order["order_id"],
                    customer_id     = order["customer_id"],
                    order_status    = order["order_status"],
                    payment_method  = order["payment_method"],
                    total_amount    = float(order["total_amount"]),
                    item_count      = int(order["item_count"]),
                    shipping_city   = city,
                    shipping_country= country,
                )

                producer.produce(
                    topic     = TOPIC,
                    key       = order["order_id"],
                    value     = json.dumps(event.to_dict()),
                    callback  = delivery_report,
                )

            producer.poll(0)      # trigger delivery callbacks non-blocking
            producer.flush(5)     # wait up to 5s for batch to land

            total_published += len(orders)
            ts = datetime.now().strftime("%H:%M:%S")
            print(f"[{ts}] Published {len(orders)} order events | total: {total_published}")

        except psycopg2.OperationalError as e:
            print(f"  ✗ DB error: {e} — reconnecting...")
            try:
                conn.close()
            except Exception:
                pass
            time.sleep(3)
            conn = get_conn()

        except Exception as e:
            print(f"  ✗ Error: {e}")

        time.sleep(POLL_INTERVAL)

    producer.flush(10)
    conn.close()
    print("Orders producer stopped cleanly.")


if __name__ == "__main__":
    main()