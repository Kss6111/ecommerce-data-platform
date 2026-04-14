-- 04_seed_data.sql
-- Static seed data for smoke testing. Day 3 replaces with Faker bulk load.

-- Suppliers
INSERT INTO suppliers (supplier_id, supplier_name, contact_name, email, country) VALUES
    ('a1b2c3d4-0000-0000-0000-000000000001', 'TechSource Inc.',      'Alice Monroe',  'alice@techsource.com',   'US'),
    ('a1b2c3d4-0000-0000-0000-000000000002', 'Global Goods Ltd.',    'Raj Patel',     'raj@globalgoods.com',    'US'),
    ('a1b2c3d4-0000-0000-0000-000000000003', 'Prime Supplies Co.',   'Chen Wei',      'chen@primesupp.com',     'US')
ON CONFLICT DO NOTHING;

-- Products
INSERT INTO products (product_id, supplier_id, product_name, category, subcategory, unit_price, stock_quantity, sku) VALUES
    ('b1000000-0000-0000-0000-000000000001', 'a1b2c3d4-0000-0000-0000-000000000001', 'Wireless Headphones Pro',  'Electronics', 'Audio',       129.99, 250, 'ELEC-AUD-001'),
    ('b1000000-0000-0000-0000-000000000002', 'a1b2c3d4-0000-0000-0000-000000000001', 'USB-C Laptop Charger',     'Electronics', 'Accessories',  49.99, 500, 'ELEC-ACC-002'),
    ('b1000000-0000-0000-0000-000000000003', 'a1b2c3d4-0000-0000-0000-000000000002', 'Ergonomic Office Chair',   'Furniture',   'Seating',     299.99,  80, 'FURN-SEA-001'),
    ('b1000000-0000-0000-0000-000000000004', 'a1b2c3d4-0000-0000-0000-000000000003', 'Stainless Water Bottle',   'Sports',      'Hydration',    24.99, 900, 'SPRT-HYD-001'),
    ('b1000000-0000-0000-0000-000000000005', 'a1b2c3d4-0000-0000-0000-000000000002', 'Mechanical Keyboard TKL',  'Electronics', 'Peripherals',  89.99, 150, 'ELEC-PER-003')
ON CONFLICT DO NOTHING;

-- Customers
INSERT INTO customers (customer_id, first_name, last_name, email, city, state, country) VALUES
    ('c1000000-0000-0000-0000-000000000001', 'Jordan',  'Blake',   'jordan.blake@email.com',   'New York',    'NY', 'US'),
    ('c1000000-0000-0000-0000-000000000002', 'Priya',   'Sharma',  'priya.sharma@email.com',   'San Jose',    'CA', 'US'),
    ('c1000000-0000-0000-0000-000000000003', 'Marcus',  'Lee',     'marcus.lee@email.com',     'Chicago',     'IL', 'US'),
    ('c1000000-0000-0000-0000-000000000004', 'Sofia',   'Reyes',   'sofia.reyes@email.com',    'Austin',      'TX', 'US')
ON CONFLICT DO NOTHING;

-- Orders
INSERT INTO orders_history (order_id, customer_id, order_status, total_amount, payment_method, order_date) VALUES
    ('d1000000-0000-0000-0000-000000000001', 'c1000000-0000-0000-0000-000000000001', 'delivered',  179.98, 'credit_card',   NOW() - INTERVAL '10 days'),
    ('d1000000-0000-0000-0000-000000000002', 'c1000000-0000-0000-0000-000000000002', 'shipped',    299.99, 'paypal',        NOW() - INTERVAL '3 days'),
    ('d1000000-0000-0000-0000-000000000003', 'c1000000-0000-0000-0000-000000000003', 'processing',  74.98, 'debit_card',    NOW() - INTERVAL '1 day'),
    ('d1000000-0000-0000-0000-000000000004', 'c1000000-0000-0000-0000-000000000004', 'pending',     24.99, 'credit_card',   NOW())
ON CONFLICT DO NOTHING;

-- Order items (note: subtotal is generated, do not insert it)
INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount) VALUES
    ('d1000000-0000-0000-0000-000000000001', 'b1000000-0000-0000-0000-000000000001', 1, 129.99, 0.00),
    ('d1000000-0000-0000-0000-000000000001', 'b1000000-0000-0000-0000-000000000002', 1,  49.99, 0.00),
    ('d1000000-0000-0000-0000-000000000002', 'b1000000-0000-0000-0000-000000000003', 1, 299.99, 0.00),
    ('d1000000-0000-0000-0000-000000000003', 'b1000000-0000-0000-0000-000000000002', 1,  49.99, 0.10),
    ('d1000000-0000-0000-0000-000000000003', 'b1000000-0000-0000-0000-000000000004', 1,  24.99, 0.00),
    ('d1000000-0000-0000-0000-000000000004', 'b1000000-0000-0000-0000-000000000004', 1,  24.99, 0.00)
ON CONFLICT DO NOTHING;

-- Confirm row counts
SELECT 'customers'     AS tbl, COUNT(*) FROM customers
UNION ALL
SELECT 'suppliers',               COUNT(*) FROM suppliers
UNION ALL
SELECT 'products',                COUNT(*) FROM products
UNION ALL
SELECT 'orders_history',          COUNT(*) FROM orders_history
UNION ALL
SELECT 'order_items',             COUNT(*) FROM order_items;