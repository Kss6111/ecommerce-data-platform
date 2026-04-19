"""
producer_clickstream.py
Generates synthetic clickstream events and publishes them to
the clickstream Kafka topic. Runs continuously until Ctrl+C.
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

from schemas import ClickEvent, now_iso, new_id

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC           = "clickstream"
PUBLISH_INTERVAL = 1.0
EVENTS_PER_TICK  = 15        # clicks are high-volume

DB_CONFIG = {
    "host":     os.getenv("PG_HOST", "localhost"),
    "port":     int(os.getenv("PG_PORT", 5433)),
    "dbname":   os.getenv("PG_DB",   "ecommerce_db"),
    "user":     os.getenv("PG_USER", "ecommerce_user"),
    "password": os.getenv("PG_PASSWORD", "ecommerce_pass"),
}

EVENT_TYPES  = ["page_view", "product_view", "add_to_cart", "checkout", "purchase"]
EVENT_WEIGHTS = [40, 30, 15, 10, 5]     # realistic funnel distribution
DEVICE_TYPES = ["desktop", "mobile", "tablet"]
DEVICE_WEIGHTS = [45, 45, 10]
BROWSERS     = ["Chrome", "Safari", "Firefox", "Edge", "Samsung Internet"]
REFERRERS    = [
    "https://google.com", "https://facebook.com", "https://instagram.com",
    "https://email.campaign.com", "https://affiliate.partner.com", None, None, None
]

PAGES = {
    "page_view":     ["/", "/about", "/deals", "/new-arrivals", "/sale"],
    "product_view":  ["/products/{id}", "/products/{id}?ref=search"],
    "add_to_cart":   ["/cart"],
    "checkout":      ["/checkout", "/checkout/shipping", "/checkout/payment"],
    "purchase":      ["/checkout/confirmation"],
}

CATEGORIES = [
    "Electronics", "Clothing", "Home & Garden", "Sports", "Books",
    "Toys", "Beauty", "Automotive", "Food & Grocery", "Office Supplies"
]

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
        "bootstrap.servers":   KAFKA_BOOTSTRAP,
        "acks":                "1",           # clickstream is high-volume, ack from leader only
        "retries":             3,
        "retry.backoff.ms":    200,
        "compression.type":    "snappy",
        "batch.size":          32768,
        "linger.ms":           20,
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
        "SELECT product_id::text, category FROM products WHERE is_active = true ORDER BY random() LIMIT %s",
        (n,)
    )
    return [{"product_id": r[0], "category": r[1]} for r in cur.fetchall()]


def fetch_customer_sample(cur, n: int = 100) -> list[str]:
    cur.execute("SELECT customer_id::text FROM customers ORDER BY random() LIMIT %s", (n,))
    return [r[0] for r in cur.fetchall()]


# ── Event builder ─────────────────────────────────────────────────────────────
def build_click_event(
    customer_ids: list[str],
    products: list[dict],
) -> ClickEvent:
    event_type  = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
    device_type = random.choices(DEVICE_TYPES, weights=DEVICE_WEIGHTS, k=1)[0]

    # 70% of clicks are from known users, 30% anonymous
    user_id = random.choice(customer_ids) if random.random() < 0.7 else None

    product     = random.choice(products) if products else None
    product_id  = product["product_id"] if product and event_type in ("product_view", "add_to_cart", "purchase") else None
    category    = product["category"]   if product and product_id else random.choice(CATEGORIES)

    page_template = random.choice(PAGES[event_type])
    page_url = page_template.replace("{id}", product_id or "unknown")

    return ClickEvent(
        event_id        = new_id(),
        event_type      = event_type,
        event_timestamp = now_iso(),
        session_id      = new_id(),
        user_id         = user_id,
        page_url        = page_url,
        product_id      = product_id,
        category        = category,
        device_type     = device_type,
        browser         = random.choice(BROWSERS),
        referrer        = random.choice(REFERRERS),
        time_on_page_ms = random.randint(500, 180000),
    )


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    global total_published

    print(f"Starting clickstream producer → topic: {TOPIC}")
    ensure_topic_exists()

    producer = make_producer()
    conn     = get_conn()

    print(f"Publishing {EVENTS_PER_TICK} events every {PUBLISH_INTERVAL}s. Press Ctrl+C to stop.\n")

    # cache product/customer lists, refresh every 60s
    cache_refresh_at = 0
    customer_ids: list[str] = []
    products: list[dict]    = []

    while running:
        try:
            now = time.time()
            if now >= cache_refresh_at:
                with conn.cursor() as cur:
                    customer_ids     = fetch_customer_sample(cur, 100)
                    products         = fetch_product_sample(cur, 100)
                cache_refresh_at = now + 60

            for _ in range(EVENTS_PER_TICK):
                event = build_click_event(customer_ids, products)
                producer.produce(
                    topic    = TOPIC,
                    key      = event.session_id,
                    value    = json.dumps(event.to_dict()),
                    callback = delivery_report,
                )

            producer.poll(0)
            producer.flush(5)

            total_published += EVENTS_PER_TICK
            ts = datetime.now().strftime("%H:%M:%S")
            print(f"[{ts}] Published {EVENTS_PER_TICK} click events | total: {total_published}")

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
    print("Clickstream producer stopped cleanly.")


if __name__ == "__main__":
    main()