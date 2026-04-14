-- 03_indexes.sql
-- Indexes for query performance and FK joins

-- customers
CREATE INDEX IF NOT EXISTS idx_customers_email         ON customers(email);
CREATE INDEX IF NOT EXISTS idx_customers_country       ON customers(country);
CREATE INDEX IF NOT EXISTS idx_customers_created_at    ON customers(created_at DESC);

-- suppliers
CREATE INDEX IF NOT EXISTS idx_suppliers_country       ON suppliers(country);

-- products
CREATE INDEX IF NOT EXISTS idx_products_supplier_id    ON products(supplier_id);
CREATE INDEX IF NOT EXISTS idx_products_category       ON products(category);
CREATE INDEX IF NOT EXISTS idx_products_sku            ON products(sku);
CREATE INDEX IF NOT EXISTS idx_products_is_active      ON products(is_active);

-- orders_history
CREATE INDEX IF NOT EXISTS idx_orders_customer_id      ON orders_history(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_status           ON orders_history(order_status);
CREATE INDEX IF NOT EXISTS idx_orders_order_date       ON orders_history(order_date DESC);
CREATE INDEX IF NOT EXISTS idx_orders_shipped_date     ON orders_history(shipped_date DESC);

-- order_items
CREATE INDEX IF NOT EXISTS idx_order_items_order_id    ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id  ON order_items(product_id);