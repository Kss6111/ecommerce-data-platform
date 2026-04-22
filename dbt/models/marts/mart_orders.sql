-- Mart model: mart_orders
-- Source: int_orders_with_customers + int_order_lines (aggregated)
-- Grain: one row per order
-- Materialization: table (queried heavily by dashboard)
-- Purpose: primary orders fact table for Metabase dashboards

WITH orders AS (
    SELECT * FROM {{ ref('int_orders_with_customers') }}
),

-- Aggregate line items to order level
order_line_summary AS (
    SELECT
        order_id,
        COUNT(*)                                                AS line_item_count,
        SUM(quantity)                                           AS total_units,
        SUM(line_revenue)                                       AS total_line_revenue,
        SUM(discount_amount)                                    AS total_discount_amount,
        AVG(discount)                                           AS avg_discount_rate,
        COUNT(DISTINCT product_id)                              AS distinct_products,
        COUNT(DISTINCT category)                                AS distinct_categories,
        BOOLOR_AGG(is_discounted)                               AS has_discounted_item,
        -- Most purchased category in the order
        MODE(category)                                          AS primary_category
    FROM {{ ref('int_order_lines') }}
    GROUP BY order_id
),

joined AS (
    SELECT
        -- Order identity
        o.order_id,
        o.customer_id,

        -- Customer demographics
        o.customer_full_name,
        o.customer_email,
        o.customer_city,
        o.customer_state,
        o.customer_country,
        o.customer_since,
        o.customer_tenure_days_at_order,

        -- Order attributes
        o.order_status,
        o.payment_method,
        o.order_value_tier,
        o.order_date,
        o.shipped_date,
        o.delivered_date,
        o.days_to_ship,

        -- Fulfillment flags
        o.is_delivered,
        o.is_cancelled,
        o.is_refunded,

        -- Financials
        o.total_amount,
        COALESCE(ol.total_line_revenue, 0)                      AS total_line_revenue,
        COALESCE(ol.total_discount_amount, 0)                   AS total_discount_amount,
        COALESCE(ol.avg_discount_rate, 0)                       AS avg_discount_rate,

        -- Line item summary
        COALESCE(ol.line_item_count, 0)                         AS line_item_count,
        COALESCE(ol.total_units, 0)                             AS total_units,
        COALESCE(ol.distinct_products, 0)                       AS distinct_products,
        COALESCE(ol.distinct_categories, 0)                     AS distinct_categories,
        ol.has_discounted_item,
        ol.primary_category,

        -- Date dimensions for easy filtering in dashboards
        DATE(o.order_date)                                      AS order_date_day,
        DATE_TRUNC('week', o.order_date)                        AS order_week,
        DATE_TRUNC('month', o.order_date)                       AS order_month,
        YEAR(o.order_date)                                      AS order_year,
        MONTH(o.order_date)                                     AS order_month_num,
        DAYOFWEEK(o.order_date)                                 AS order_day_of_week,

        -- Metadata
        o.order_created_at,
        o._ingested_at

    FROM orders o
    LEFT JOIN order_line_summary ol
        ON o.order_id = ol.order_id
)

SELECT * FROM joined