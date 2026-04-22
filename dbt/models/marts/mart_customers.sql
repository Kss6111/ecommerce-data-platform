-- Mart model: mart_customers
-- Source: stg_customers + mart_orders (aggregated)
-- Grain: one row per customer
-- Materialization: table
-- Purpose: customer 360 view with lifetime value and behavioral metrics

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

-- Aggregate orders to customer level
customer_orders AS (
    SELECT
        customer_id,
        COUNT(*)                                                AS total_orders,
        COUNT(CASE WHEN is_delivered THEN 1 END)                AS delivered_orders,
        COUNT(CASE WHEN is_cancelled THEN 1 END)                AS cancelled_orders,
        COUNT(CASE WHEN is_refunded  THEN 1 END)                AS refunded_orders,
        SUM(total_amount)                                       AS lifetime_value,
        AVG(total_amount)                                       AS avg_order_value,
        MAX(total_amount)                                       AS max_order_value,
        MIN(total_amount)                                       AS min_order_value,
        SUM(total_discount_amount)                              AS total_discounts_received,
        MIN(order_date)                                         AS first_order_date,
        MAX(order_date)                                         AS last_order_date,
        DATEDIFF('day', MIN(order_date), MAX(order_date))       AS days_between_first_last_order,
        MODE(payment_method)                                    AS preferred_payment_method,
        MODE(primary_category)                                  AS preferred_category,
        MODE(order_value_tier)                                  AS typical_order_tier
    FROM {{ ref('mart_orders') }}
    GROUP BY customer_id
),

joined AS (
    SELECT
        -- Customer identity
        c.customer_id,
        c.full_name,
        c.email,
        c.phone,
        c.city,
        c.state,
        c.country,
        c.postal_code,
        c.created_at                                            AS customer_since,

        -- Order metrics
        COALESCE(co.total_orders, 0)                            AS total_orders,
        COALESCE(co.delivered_orders, 0)                        AS delivered_orders,
        COALESCE(co.cancelled_orders, 0)                        AS cancelled_orders,
        COALESCE(co.refunded_orders, 0)                         AS refunded_orders,
        COALESCE(co.lifetime_value, 0)                          AS lifetime_value,
        COALESCE(co.avg_order_value, 0)                         AS avg_order_value,
        COALESCE(co.max_order_value, 0)                         AS max_order_value,
        COALESCE(co.total_discounts_received, 0)                AS total_discounts_received,

        -- Order history
        co.first_order_date,
        co.last_order_date,
        co.days_between_first_last_order,
        co.preferred_payment_method,
        co.preferred_category,
        co.typical_order_tier,

        -- Derived: days since last order
        CAST(DATEDIFF('day', co.last_order_date, CURRENT_TIMESTAMP()) AS NUMBER(18,0)) AS days_since_last_order,

        -- Derived: customer value segment
        CASE
            WHEN COALESCE(co.lifetime_value, 0) >= 2000 THEN 'platinum'
            WHEN COALESCE(co.lifetime_value, 0) >= 1000 THEN 'gold'
            WHEN COALESCE(co.lifetime_value, 0) >= 500  THEN 'silver'
            WHEN COALESCE(co.total_orders, 0) > 0       THEN 'bronze'
            ELSE 'prospect'
        END                                                     AS customer_segment,

        -- Derived: is this customer active (ordered in last 90 days)?
        CASE
            WHEN DATEDIFF('day', co.last_order_date, CURRENT_TIMESTAMP()) <= 90
            THEN TRUE ELSE FALSE
        END                                                     AS is_active_customer,

        -- Derived: delivery rate
        CASE
            WHEN COALESCE(co.total_orders, 0) > 0
            THEN ROUND(co.delivered_orders / co.total_orders, 4)
            ELSE NULL
        END                                                     AS delivery_rate,

        -- Derived: cancellation rate
        CASE
            WHEN COALESCE(co.total_orders, 0) > 0
            THEN ROUND(co.cancelled_orders / co.total_orders, 4)
            ELSE NULL
        END                                                     AS cancellation_rate,

        -- Metadata
        c._ingested_at

    FROM customers c
    LEFT JOIN customer_orders co
        ON c.customer_id = co.customer_id
)

SELECT * FROM joined