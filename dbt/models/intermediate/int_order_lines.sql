-- Intermediate model: int_order_lines
-- Joins: stg_order_items + stg_orders_history + stg_products
-- Grain: one row per order line item
-- Purpose: enriched line items with order context and product details,
--          ready for revenue and product performance marts

WITH order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders_history') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

joined AS (
    SELECT
        -- Line item identity
        oi.order_item_id,
        oi.order_id,
        oi.product_id,

        -- Order context
        o.customer_id,
        o.order_status,
        o.payment_method,
        o.order_date,
        o.shipped_date,
        o.delivered_date,
        o.days_to_ship,

        -- Product context
        p.product_name,
        p.category,
        p.subcategory,
        p.sku,
        p.supplier_id,
        p.is_active                                             AS product_is_active,

        -- Line item financials
        oi.quantity,
        oi.unit_price                                           AS listed_unit_price,
        oi.discount,
        oi.effective_unit_price,
        oi.subtotal,

        -- Derived: revenue contribution after discount
        ROUND(oi.effective_unit_price * oi.quantity, 2)         AS line_revenue,

        -- Derived: discount amount in dollars
        ROUND((oi.unit_price - oi.effective_unit_price) * oi.quantity, 2) AS discount_amount,

        -- Derived: is this a discounted line?
        CASE WHEN oi.discount > 0 THEN TRUE ELSE FALSE END      AS is_discounted,

        -- Derived: order fulfillment flags
        CASE WHEN o.order_status = 'DELIVERED' THEN TRUE ELSE FALSE END AS is_delivered,
        CASE WHEN o.order_status = 'CANCELLED' THEN TRUE ELSE FALSE END AS is_cancelled,
        CASE WHEN o.order_status = 'REFUNDED'  THEN TRUE ELSE FALSE END AS is_refunded,

        -- Metadata
        oi.created_at                                           AS item_created_at,
        oi._ingested_at

    FROM order_items oi
    INNER JOIN orders o
        ON oi.order_id = o.order_id
    LEFT JOIN products p
        ON oi.product_id = p.product_id
)

SELECT * FROM joined