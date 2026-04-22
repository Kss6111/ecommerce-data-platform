-- Mart model: mart_revenue_daily
-- Source: mart_orders
-- Grain: one row per calendar day
-- Materialization: table
-- Purpose: time-series revenue metrics for trend charts in Metabase

WITH orders AS (
    SELECT * FROM {{ ref('mart_orders') }}
    WHERE NOT is_cancelled
      AND NOT is_refunded
),

daily AS (
    SELECT
        order_date_day                                          AS date_day,
        order_week,
        order_month,
        order_year,
        order_month_num,
        order_day_of_week,

        -- Volume
        COUNT(DISTINCT order_id)                                AS total_orders,
        COUNT(DISTINCT customer_id)                             AS unique_customers,
        SUM(total_units)                                        AS total_units_sold,

        -- Revenue
        SUM(total_amount)                                       AS gross_revenue,
        SUM(total_line_revenue)                                 AS net_revenue,
        SUM(total_discount_amount)                              AS total_discounts,
        AVG(total_amount)                                       AS avg_order_value,

        -- Order value distribution
        COUNT(CASE WHEN order_value_tier = 'high'   THEN 1 END) AS high_value_orders,
        COUNT(CASE WHEN order_value_tier = 'medium' THEN 1 END) AS medium_value_orders,
        COUNT(CASE WHEN order_value_tier = 'low'    THEN 1 END) AS low_value_orders,

        -- Payment methods
        COUNT(CASE WHEN payment_method = 'CREDIT_CARD'  THEN 1 END) AS credit_card_orders,
        COUNT(CASE WHEN payment_method = 'DEBIT_CARD'   THEN 1 END) AS debit_card_orders,
        COUNT(CASE WHEN payment_method = 'PAYPAL'       THEN 1 END) AS paypal_orders,
        COUNT(CASE WHEN payment_method = 'BANK_TRANSFER' THEN 1 END) AS bank_transfer_orders,

        -- Geography — top countries
        MODE(customer_country)                                  AS top_customer_country,

        -- Fulfillment
        COUNT(CASE WHEN is_delivered THEN 1 END)                AS delivered_orders,
        AVG(CASE WHEN days_to_ship IS NOT NULL 
                 THEN days_to_ship END)                         AS avg_days_to_ship

    FROM orders
    GROUP BY
        order_date_day,
        order_week,
        order_month,
        order_year,
        order_month_num,
        order_day_of_week
)

SELECT * FROM daily