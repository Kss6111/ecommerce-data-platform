"""
event_generator.py
Continuously generates new orders and order_items into PostgreSQL,
simulating live ecommerce activity. Runs until Ctrl+C.
"""

import uuid
import random
import time
import os
import signal
import sys
from datetime import datetime, timezone
from faker import Faker
import psycopg2
from psycopg2.extras import register_uuid
from dotenv import load_dotenv

load_dotenv()

fake = Faker()

# ── Config ────────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("PG_HOST", "localhost"),
    "port":     int(os.getenv("PG_PORT", 5433)),
    "dbname":   os.getenv("PG_DB",   "ecommerce_db"),
    "user":     os.getenv("PG_USER", "ecommerce_user"),
    "password": os.getenv("PG_PASSWORD", "ecommerce_pass"),
}

ORDERS_PER_BATCH    = 5       # orders inserted per tick
SLEEP_SECONDS       = 2.0     # seconds between ticks
MAX_ITEMS_PER_ORDER = 4

ORDER_STATUSES  = ["pending", "confirmed", "processing"]   # only fresh statuses for new orders
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "bank_transfer", "crypto"]

# ── State ─────────────────────────────────────────────────────────────────────
total_orders_inserted = 0
total_items_inserted  = 0
running = True


def handle_sigint(sig, frame):
    global running
    print(f"\n\nStopping... Total inserted: {total_orders_inserted} orders, {total_items_inserted} items.")
    running = False


signal.signal(signal.SIGINT, handle_sigint)


# ── DB ────────────────────────────────────────────────────────────────────────
def get_conn():
    conn = psycopg2.connect(**DB_CONFIG)
    register_uuid()
    return conn


def fetch_random_customer_ids(cur, n: int = 20) -> list[uuid.UUID]:
    cur.execute("SELECT customer_id FROM customers ORDER BY random() LIMIT %s", (n,))
    return [r[0] for r in cur.fetchall()]


def fetch_random_products(cur, n: int = 50) -> list[dict]:
    cur.execute(
        "SELECT product_id, unit_price FROM products WHERE is_active = true ORDER BY random() LIMIT %s",
        (n,)
    )
    return [{"product_id": r[0], "unit_price": float(r[1])} for r in cur.fetchall()]


def insert_order(cur, customer_id: uuid.UUID) -> uuid.UUID:
    order_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    cur.execute(
        """
        INSERT INTO orders_history
            (order_id, customer_id, order_status, total_amount,
             payment_method, shipping_address, order_date, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            order_id,
            customer_id,
            random.choice(ORDER_STATUSES),
            0.00,
            random.choice(PAYMENT_METHODS),
            fake.address().replace("\n", ", ")[:255],
            now,
            now,
        )
    )
    return order_id


def insert_order_items(cur, order_id: uuid.UUID, products: list[dict]) -> int:
    n_items   = random.randint(1, MAX_ITEMS_PER_ORDER)
    selection = random.sample(products, min(n_items, len(products)))
    for p in selection:
        cur.execute(
            """
            INSERT INTO order_items
                (order_item_id, order_id, product_id, quantity, unit_price, discount)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                uuid.uuid4(),
                order_id,
                p["product_id"],
                random.randint(1, 5),
                p["unit_price"],
                random.choice([0, 0, 0, 0.05, 0.10, 0.15]),
            )
        )
    return len(selection)


def update_order_total(cur, order_id: uuid.UUID) -> None:
    cur.execute(
        """
        UPDATE orders_history
        SET total_amount = (
            SELECT ROUND(SUM(subtotal)::numeric, 2)
            FROM order_items
            WHERE order_id = %s
        )
        WHERE order_id = %s
        """,
        (order_id, order_id)
    )


# ── Main loop ─────────────────────────────────────────────────────────────────
def main():
    global total_orders_inserted, total_items_inserted

    print("Starting continuous event generator...")
    print(f"Inserting {ORDERS_PER_BATCH} orders every {SLEEP_SECONDS}s. Press Ctrl+C to stop.\n")

    conn = get_conn()

    while running:
        try:
            with conn.cursor() as cur:
                customer_ids = fetch_random_customer_ids(cur, 20)
                products     = fetch_random_products(cur, 50)

                if not customer_ids or not products:
                    print("  ⚠ No customers or products found — has seed_postgres.py been run?")
                    time.sleep(5)
                    continue

                batch_orders = 0
                batch_items  = 0

                for _ in range(ORDERS_PER_BATCH):
                    customer_id = random.choice(customer_ids)
                    order_id    = insert_order(cur, customer_id)
                    n_items     = insert_order_items(cur, order_id, products)
                    update_order_total(cur, order_id)
                    batch_orders += 1
                    batch_items  += n_items

            conn.commit()

            total_orders_inserted += batch_orders
            total_items_inserted  += batch_items

            ts = datetime.now().strftime("%H:%M:%S")
            print(
                f"[{ts}] +{batch_orders} orders, +{batch_items} items "
                f"| total: {total_orders_inserted} orders, {total_items_inserted} items"
            )

        except psycopg2.OperationalError as e:
            print(f"  ✗ DB connection lost: {e}. Reconnecting...")
            try:
                conn.close()
            except Exception:
                pass
            time.sleep(3)
            conn = get_conn()

        except Exception as e:
            print(f"  ✗ Unexpected error: {e}")
            conn.rollback()

        time.sleep(SLEEP_SECONDS)

    conn.close()
    print("Event generator stopped cleanly.")


if __name__ == "__main__":
    main()