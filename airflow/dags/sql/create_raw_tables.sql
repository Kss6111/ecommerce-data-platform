USE DATABASE ECOMMERCE_DB;
USE SCHEMA RAW;

CREATE TABLE IF NOT EXISTS RAW.CUSTOMERS (
    customer_id         VARCHAR(36),
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    email               VARCHAR(255),
    phone               VARCHAR(50),
    address             VARCHAR(500),
    city                VARCHAR(100),
    state               VARCHAR(50),
    country             VARCHAR(100),
    postal_code         VARCHAR(20),
    created_at          TIMESTAMP_TZ,
    updated_at          TIMESTAMP_TZ,
    _ingested_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW.PRODUCTS (
    product_id          VARCHAR(36),
    supplier_id         VARCHAR(36),
    product_name        VARCHAR(500),
    category            VARCHAR(100),
    subcategory         VARCHAR(100),
    unit_price          NUMBER(12,2),
    stock_quantity      NUMBER,
    sku                 VARCHAR(100),
    is_active           BOOLEAN,
    created_at          TIMESTAMP_TZ,
    updated_at          TIMESTAMP_TZ,
    _ingested_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW.SUPPLIERS (
    supplier_id         VARCHAR(36),
    supplier_name       VARCHAR(255),
    contact_name        VARCHAR(100),
    email               VARCHAR(255),
    phone               VARCHAR(50),
    address             VARCHAR(500),
    city                VARCHAR(100),
    country             VARCHAR(100),
    created_at          TIMESTAMP_TZ,
    _ingested_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW.ORDERS_HISTORY (
    order_id            VARCHAR(36),
    customer_id         VARCHAR(36),
    order_status        VARCHAR(50),
    total_amount        NUMBER(12,2),
    payment_method      VARCHAR(50),
    shipping_address    VARCHAR(1000),
    order_date          TIMESTAMP_TZ,
    shipped_date        TIMESTAMP_TZ,
    delivered_date      TIMESTAMP_TZ,
    created_at          TIMESTAMP_TZ,
    _ingested_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW.ORDER_ITEMS (
    order_item_id       VARCHAR(36),
    order_id            VARCHAR(36),
    product_id          VARCHAR(36),
    quantity            NUMBER,
    unit_price          NUMBER(12,2),
    discount            NUMBER(12,4),
    subtotal            NUMBER(12,2),
    created_at          TIMESTAMP_TZ,
    _ingested_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);