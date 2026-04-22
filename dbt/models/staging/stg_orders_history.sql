-- Staging model: stg_orders_history
-- Source: RAW.ORDERS_HISTORY (batch ingested via Airflow)

WITH source AS (
    SELECT * FROM {{ source('raw', 'orders_history') }}
),

cleaned AS (
    SELECT
        -- Primary key
        order_id,

        -- Foreign key
        customer_id,

        -- Order attributes — uppercase normalize status so downstream CASE logic is reliable
        UPPER(TRIM(order_status))                               AS order_status,
        UPPER(TRIM(payment_method))                             AS payment_method,
        TRIM(shipping_address)                                  AS shipping_address,

        -- Financials
        CAST(total_amount AS NUMERIC(12, 2))                    AS total_amount,

        -- Timestamps — SHIPPED and DELIVERED can be NULL for in-progress orders
        order_date,
        shipped_date,
        delivered_date,
        created_at,

        -- Derived: days to ship (NULL-safe — returns NULL if not yet shipped, which is correct)
        CAST(DATEDIFF('day', order_date, shipped_date) AS NUMBER(18,0)) AS days_to_ship,

        -- Metadata
        _ingested_at

    FROM source
    WHERE order_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _ingested_at DESC) = 1
)

SELECT * FROM cleaned