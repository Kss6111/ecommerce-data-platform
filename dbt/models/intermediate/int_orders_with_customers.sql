-- Intermediate model: int_orders_with_customers
-- Joins: stg_orders_history + stg_customers
-- Grain: one row per order
-- Purpose: order-level records enriched with customer demographics,
--          feeds mart_orders and mart_customers

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders_history') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

joined AS (
    SELECT
        -- Order identity
        o.order_id,
        o.customer_id,

        -- Customer demographics
        c.full_name                                             AS customer_full_name,
        c.email                                                 AS customer_email,
        c.city                                                  AS customer_city,
        c.state                                                 AS customer_state,
        c.country                                               AS customer_country,
        c.created_at                                            AS customer_since,

        -- Derived: customer tenure at time of order (days)
        CAST(DATEDIFF('day', c.created_at, o.order_date) AS NUMBER(18,0)) AS customer_tenure_days_at_order,

        -- Order attributes
        o.order_status,
        o.payment_method,
        o.total_amount,
        o.shipping_address,
        o.order_date,
        o.shipped_date,
        o.delivered_date,
        o.days_to_ship,

        -- Derived: fulfillment flags
        CASE WHEN o.order_status = 'DELIVERED' THEN TRUE ELSE FALSE END AS is_delivered,
        CASE WHEN o.order_status = 'CANCELLED' THEN TRUE ELSE FALSE END AS is_cancelled,
        CASE WHEN o.order_status = 'REFUNDED'  THEN TRUE ELSE FALSE END AS is_refunded,

        -- Derived: order value tier for segmentation
        CASE
            WHEN o.total_amount >= 500  THEN 'high'
            WHEN o.total_amount >= 100  THEN 'medium'
            ELSE 'low'
        END                                                     AS order_value_tier,

        -- Metadata
        o.created_at                                            AS order_created_at,
        o._ingested_at

    FROM orders o
    LEFT JOIN customers c
        ON o.customer_id = c.customer_id
)

SELECT * FROM joined