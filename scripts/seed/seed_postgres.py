"""
seed_postgres.py
Generates realistic fake data and seeds all five PostgreSQL tables.
Matches exact schema from postgres/init DDL.
Order: suppliers → products → customers → orders_history → order_items
"""

import uuid
import random
import os
from datetime import datetime, timedelta
from faker import Faker
import psycopg2
from psycopg2.extras import execute_batch, register_uuid
from dotenv import load_dotenv

load_dotenv()

fake = Faker()
Faker.seed(42)
random.seed(42)

# ── Config ────────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("PG_HOST", "localhost"),
    "port":     int(os.getenv("PG_PORT", 5433)),
    "dbname":   os.getenv("PG_DB",   "ecommerce_db"),
    "user":     os.getenv("PG_USER", "ecommerce_user"),
    "password": os.getenv("PG_PASSWORD", "ecommerce_pass"),
}

NUM_SUPPLIERS       = 50
NUM_PRODUCTS        = 500
NUM_CUSTOMERS       = 1000
NUM_ORDERS          = 5000
AVG_ITEMS_PER_ORDER = 3

CATEGORIES = {
    "Electronics":      ["Phones", "Laptops", "Tablets", "Audio", "Cameras"],
    "Clothing":         ["Men", "Women", "Kids", "Shoes", "Accessories"],
    "Home & Garden":    ["Furniture", "Decor", "Kitchen", "Garden", "Bedding"],
    "Sports":           ["Fitness", "Outdoor", "Team Sports", "Water Sports", "Cycling"],
    "Books":            ["Fiction", "Non-Fiction", "Academic", "Children", "Comics"],
    "Toys":             ["Action Figures", "Board Games", "Puzzles", "Educational", "Dolls"],
    "Beauty":           ["Skincare", "Haircare", "Makeup", "Fragrance", "Tools"],
    "Automotive":       ["Parts", "Accessories", "Tools", "Care", "Electronics"],
    "Food & Grocery":   ["Snacks", "Beverages", "Organic", "Frozen", "Condiments"],
    "Office Supplies":  ["Stationery", "Furniture", "Tech", "Paper", "Storage"],
}

ORDER_STATUSES   = ["pending", "confirmed", "processing", "shipped", "delivered", "cancelled", "refunded"]
PAYMENT_METHODS  = ["credit_card", "debit_card", "paypal", "bank_transfer", "crypto"]


# ── Helpers ───────────────────────────────────────────────────────────────────
def uid() -> uuid.UUID:
    return uuid.uuid4()


def random_date(start_days_ago: int, end_days_ago: int = 0) -> datetime:
    start = datetime.utcnow() - timedelta(days=start_days_ago)
    end   = datetime.utcnow() - timedelta(days=end_days_ago)
    delta = (end - start).total_seconds()
    return start + timedelta(seconds=random.randint(0, max(1, int(delta))))


def make_sku(category: str, index: int) -> str:
    prefix = "".join(w[0] for w in category.split()).upper()
    return f"{prefix}-{index:05d}-{random.randint(1000,9999)}"


# ── Generators ────────────────────────────────────────────────────────────────
def generate_suppliers(n: int) -> list[dict]:
    rows = []
    for _ in range(n):
        rows.append({
            "supplier_id":  uid(),
            "supplier_name": fake.company(),
            "contact_name":  fake.name(),
            "email":         fake.company_email(),
            "phone":         fake.phone_number()[:20],
            "address":       fake.street_address()[:255],
            "city":          fake.city(),
            "country":       fake.country()[:100],
            "created_at":    random_date(730, 365),
        })
    return rows


def generate_products(n: int, supplier_ids: list[uuid.UUID]) -> list[dict]:
    rows = []
    for i in range(n):
        category    = random.choice(list(CATEGORIES.keys()))
        subcategory = random.choice(CATEGORIES[category])
        rows.append({
            "product_id":     uid(),
            "supplier_id":    random.choice(supplier_ids),
            "product_name":   fake.catch_phrase()[:300],
            "category":       category,
            "subcategory":    subcategory,
            "unit_price":     round(random.uniform(2.0, 500.0), 2),
            "stock_quantity": random.randint(0, 1000),
            "sku":            make_sku(category, i),
            "is_active":      random.random() > 0.05,   # 95% active
            "created_at":     random_date(365, 30),
            "updated_at":     random_date(30, 0),
        })
    return rows


def generate_customers(n: int) -> list[dict]:
    rows = []
    seen_emails: set[str] = set()
    while len(rows) < n:
        email = fake.email()
        if email in seen_emails:
            continue
        seen_emails.add(email)
        rows.append({
            "customer_id": uid(),
            "first_name":  fake.first_name(),
            "last_name":   fake.last_name(),
            "email":       email,
            "phone":       fake.phone_number()[:20],
            "address":     fake.street_address()[:255],
            "city":        fake.city(),
            "state":       fake.state_abbr(),
            "country":     fake.country()[:100],
            "postal_code": fake.postcode()[:20],
            "created_at":  random_date(365, 0),
        })
    return rows


def generate_orders(n: int, customer_rows: list[dict]) -> list[dict]:
    rows = []
    for _ in range(n):
        customer     = random.choice(customer_rows)
        order_date   = random_date(365, 0)
        status       = random.choice(ORDER_STATUSES)

        shipped_date   = None
        delivered_date = None
        if status in ("shipped", "delivered"):
            shipped_date = order_date + timedelta(hours=random.randint(24, 72))
        if status == "delivered":
            delivered_date = shipped_date + timedelta(days=random.randint(1, 7))

        rows.append({
            "order_id":         uid(),
            "customer_id":      customer["customer_id"],
            "order_status":     status,
            "total_amount":     0.00,          # updated after items
            "payment_method":   random.choice(PAYMENT_METHODS),
            "shipping_address": f"{customer['address']}, {customer['city']}, {customer['state']} {customer['postal_code']}",
            "order_date":       order_date,
            "shipped_date":     shipped_date,
            "delivered_date":   delivered_date,
            "created_at":       order_date,
        })
    return rows


def generate_order_items(
    order_rows: list[dict],
    product_rows: list[dict],
    avg_items: int,
) -> list[dict]:
    rows = []
    for order in order_rows:
        n_items  = max(1, int(random.gauss(avg_items, 1)))
        products = random.sample(product_rows, min(n_items, len(product_rows)))
        for product in products:
            qty      = random.randint(1, 5)
            discount = round(random.choice([0, 0, 0, 0.05, 0.10, 0.15, 0.20]), 4)
            rows.append({
                "order_item_id": uid(),
                "order_id":      order["order_id"],
                "product_id":    product["product_id"],
                "quantity":      qty,
                "unit_price":    product["unit_price"],
                "discount":      discount,
                # subtotal is a generated column — do NOT insert it
            })
    return rows


# ── DB helpers ────────────────────────────────────────────────────────────────
def get_conn():
    conn = psycopg2.connect(**DB_CONFIG)
    register_uuid()          # lets psycopg2 handle uuid.UUID natively
    return conn


def seed_suppliers(conn, rows: list[dict]) -> None:
    sql = """
        INSERT INTO suppliers
            (supplier_id, supplier_name, contact_name, email, phone, address, city, country, created_at)
        VALUES
            (%(supplier_id)s, %(supplier_name)s, %(contact_name)s, %(email)s, %(phone)s,
             %(address)s, %(city)s, %(country)s, %(created_at)s)
        ON CONFLICT (supplier_id) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_batch(cur, sql, rows, page_size=200)
    conn.commit()
    print(f"  ✓ Suppliers seeded: {len(rows)}")


def seed_products(conn, rows: list[dict]) -> None:
    sql = """
        INSERT INTO products
            (product_id, supplier_id, product_name, category, subcategory,
             unit_price, stock_quantity, sku, is_active, created_at, updated_at)
        VALUES
            (%(product_id)s, %(supplier_id)s, %(product_name)s, %(category)s, %(subcategory)s,
             %(unit_price)s, %(stock_quantity)s, %(sku)s, %(is_active)s, %(created_at)s, %(updated_at)s)
        ON CONFLICT (product_id) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_batch(cur, sql, rows, page_size=200)
    conn.commit()
    print(f"  ✓ Products seeded: {len(rows)}")


def seed_customers(conn, rows: list[dict]) -> None:
    sql = """
        INSERT INTO customers
            (customer_id, first_name, last_name, email, phone,
             address, city, state, country, postal_code, created_at)
        VALUES
            (%(customer_id)s, %(first_name)s, %(last_name)s, %(email)s, %(phone)s,
             %(address)s, %(city)s, %(state)s, %(country)s, %(postal_code)s, %(created_at)s)
        ON CONFLICT (email) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_batch(cur, sql, rows, page_size=200)
    conn.commit()
    print(f"  ✓ Customers seeded: {len(rows)}")


def seed_orders(conn, rows: list[dict]) -> None:
    sql = """
        INSERT INTO orders_history
            (order_id, customer_id, order_status, total_amount, payment_method,
             shipping_address, order_date, shipped_date, delivered_date, created_at)
        VALUES
            (%(order_id)s, %(customer_id)s, %(order_status)s, %(total_amount)s, %(payment_method)s,
             %(shipping_address)s, %(order_date)s, %(shipped_date)s, %(delivered_date)s, %(created_at)s)
        ON CONFLICT (order_id) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_batch(cur, sql, rows, page_size=200)
    conn.commit()
    print(f"  ✓ Orders seeded: {len(rows)}")


def seed_order_items(conn, rows: list[dict]) -> None:
    sql = """
        INSERT INTO order_items
            (order_item_id, order_id, product_id, quantity, unit_price, discount)
        VALUES
            (%(order_item_id)s, %(order_id)s, %(product_id)s, %(quantity)s, %(unit_price)s, %(discount)s)
        ON CONFLICT (order_item_id) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_batch(cur, sql, rows, page_size=500)
    conn.commit()
    print(f"  ✓ Order items seeded: {len(rows)}")


def update_order_totals(conn) -> None:
    """Pull the generated subtotals back up into orders_history.total_amount."""
    sql = """
        UPDATE orders_history oh
        SET    total_amount = sub.total
        FROM (
            SELECT order_id, ROUND(SUM(subtotal)::numeric, 2) AS total
            FROM   order_items
            GROUP  BY order_id
        ) sub
        WHERE oh.order_id = sub.order_id
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    print("  ✓ Order totals recalculated from subtotals")


def verify_counts(conn) -> None:
    tables = ["suppliers", "products", "customers", "orders_history", "order_items"]
    print("\n── Row count verification ──────────────────────────")
    with conn.cursor() as cur:
        for t in tables:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            count = cur.fetchone()[0]
            print(f"  {t:<20} {count:>6} rows")
    print("────────────────────────────────────────────────────")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("Connecting to PostgreSQL...")
    conn = get_conn()
    print("Connected.\n")

    print(f"[1/5] Generating & seeding {NUM_SUPPLIERS} suppliers...")
    supplier_rows = generate_suppliers(NUM_SUPPLIERS)
    seed_suppliers(conn, supplier_rows)
    supplier_ids = [r["supplier_id"] for r in supplier_rows]

    print(f"\n[2/5] Generating & seeding {NUM_PRODUCTS} products...")
    product_rows = generate_products(NUM_PRODUCTS, supplier_ids)
    seed_products(conn, product_rows)

    print(f"\n[3/5] Generating & seeding {NUM_CUSTOMERS} customers...")
    customer_rows = generate_customers(NUM_CUSTOMERS)
    seed_customers(conn, customer_rows)

    print(f"\n[4/5] Generating & seeding {NUM_ORDERS} orders...")
    order_rows = generate_orders(NUM_ORDERS, customer_rows)
    seed_orders(conn, order_rows)

    print(f"\n[5/5] Generating & seeding order items (~{NUM_ORDERS * AVG_ITEMS_PER_ORDER} rows)...")
    item_rows = generate_order_items(order_rows, product_rows, AVG_ITEMS_PER_ORDER)
    seed_order_items(conn, item_rows)

    print("\nRecalculating order totals...")
    update_order_totals(conn)

    verify_counts(conn)
    conn.close()
    print("\n✅ Day 5 seeding complete.")


if __name__ == "__main__":
    main()