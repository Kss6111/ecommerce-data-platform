-- 02_create_tables.sql
-- Full schema: customers, suppliers, products, orders_history, order_items

-- ─────────────────────────────────────────────
-- CUSTOMERS
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS customers (
    customer_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    first_name      VARCHAR(100)        NOT NULL,
    last_name       VARCHAR(100)        NOT NULL,
    email           VARCHAR(255)        NOT NULL UNIQUE,
    phone           VARCHAR(20),
    address         VARCHAR(255),
    city            VARCHAR(100),
    state           VARCHAR(100),
    country         VARCHAR(100)        NOT NULL DEFAULT 'US',
    postal_code     VARCHAR(20),
    created_at      TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ         NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE customers IS 'Registered customers. Source of truth for the customer dimension in Snowflake.';
COMMENT ON COLUMN customers.email IS 'Unique business key used for deduplication in dbt silver layer.';


-- ─────────────────────────────────────────────
-- SUPPLIERS
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    supplier_name   VARCHAR(200)        NOT NULL,
    contact_name    VARCHAR(200),
    email           VARCHAR(255)        UNIQUE,
    phone           VARCHAR(20),
    address         VARCHAR(255),
    city            VARCHAR(100),
    country         VARCHAR(100)        NOT NULL DEFAULT 'US',
    created_at      TIMESTAMPTZ         NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE suppliers IS 'Product suppliers. Maps to product supply chain dimension.';


-- ─────────────────────────────────────────────
-- PRODUCTS
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS products (
    product_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    supplier_id     UUID                NOT NULL REFERENCES suppliers(supplier_id) ON DELETE RESTRICT,
    product_name    VARCHAR(300)        NOT NULL,
    category        VARCHAR(100)        NOT NULL,
    subcategory     VARCHAR(100),
    unit_price      NUMERIC(10, 2)      NOT NULL CHECK (unit_price >= 0),
    stock_quantity  INTEGER             NOT NULL DEFAULT 0 CHECK (stock_quantity >= 0),
    sku             VARCHAR(100)        NOT NULL UNIQUE,
    is_active       BOOLEAN             NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ         NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE products IS 'Product catalog. SKU is the business key used for deduplication in dbt.';
COMMENT ON COLUMN products.unit_price IS 'Current list price. Historical price captured in order_items.unit_price.';
COMMENT ON COLUMN products.is_active IS 'Soft-delete flag. Inactive products stay in history but cannot be ordered.';


-- ─────────────────────────────────────────────
-- ORDERS_HISTORY
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS orders_history (
    order_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id         UUID                NOT NULL REFERENCES customers(customer_id) ON DELETE RESTRICT,
    order_status        VARCHAR(50)         NOT NULL DEFAULT 'pending'
                            CHECK (order_status IN ('pending', 'confirmed', 'processing', 'shipped', 'delivered', 'cancelled', 'refunded')),
    total_amount        NUMERIC(12, 2)      NOT NULL CHECK (total_amount >= 0),
    payment_method      VARCHAR(50)         NOT NULL
                            CHECK (payment_method IN ('credit_card', 'debit_card', 'paypal', 'bank_transfer', 'crypto')),
    shipping_address    TEXT,
    order_date          TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
    shipped_date        TIMESTAMPTZ,
    delivered_date      TIMESTAMPTZ,
    created_at          TIMESTAMPTZ         NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE orders_history IS 'Append-only order log. Never update rows — new status changes are new rows or handled via status field.';
COMMENT ON COLUMN orders_history.order_status IS 'Enum-constrained. Kafka will stream status change events that update this field in OLTP.';


-- ─────────────────────────────────────────────
-- ORDER_ITEMS
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id        UUID                NOT NULL REFERENCES orders_history(order_id) ON DELETE CASCADE,
    product_id      UUID                NOT NULL REFERENCES products(product_id) ON DELETE RESTRICT,
    quantity        INTEGER             NOT NULL CHECK (quantity > 0),
    unit_price      NUMERIC(10, 2)      NOT NULL CHECK (unit_price >= 0),
    discount        NUMERIC(5, 4)       NOT NULL DEFAULT 0 CHECK (discount >= 0 AND discount <= 1),
    subtotal        NUMERIC(12, 2)      GENERATED ALWAYS AS
                        (ROUND(quantity * unit_price * (1 - discount), 2)) STORED,
    created_at      TIMESTAMPTZ         NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE order_items IS 'Line items per order. subtotal is a generated/computed column — never insert directly.';
COMMENT ON COLUMN order_items.unit_price IS 'Price at time of purchase. Intentionally denormalized from products.unit_price.';
COMMENT ON COLUMN order_items.discount IS 'Fractional discount (0.0 to 1.0). 0.15 = 15% off.';
COMMENT ON COLUMN order_items.subtotal IS 'Computed: quantity * unit_price * (1 - discount). Stored for query performance.';