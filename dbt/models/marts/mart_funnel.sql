-- Mart model: mart_funnel
-- Source: int_clickstream_sessionized + stg_customers
-- Grain: one row per session
-- Materialization: table
-- Purpose: full funnel analysis from browse to purchase for Metabase

WITH sessions AS (
    SELECT * FROM {{ ref('int_clickstream_sessionized') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

joined AS (
    SELECT
        -- Session identity
        s.session_id,
        s.user_id,

        -- Customer context (null for anonymous sessions)
        c.full_name                                             AS customer_full_name,
        c.email                                                 AS customer_email,
        c.country                                               AS customer_country,

        -- Authentication state
        s.has_authenticated_event,
        CASE WHEN c.customer_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_known_customer,

        -- Device / browser
        s.device_type,
        s.browser,

        -- Session timing
        s.session_start,
        s.session_end,
        s.session_duration_sec,
        DATE(s.session_start)                                   AS session_date,
        DATE_TRUNC('week', s.session_start)                     AS session_week,
        DATE_TRUNC('month', s.session_start)                    AS session_month,
        HOUR(s.session_start)                                   AS session_hour,

        -- Engagement
        s.total_events,
        s.unique_pages_viewed,
        s.unique_products_viewed,
        s.unique_categories_viewed,
        s.total_time_on_site_sec,
        s.avg_time_per_page_sec,
        s.entry_page,
        s.exit_page,
        s.referrer,

        -- Funnel event flags
        s.has_page_view,
        s.has_product_view,
        s.has_add_to_cart,
        s.has_checkout,
        s.has_purchase,

        -- Funnel stage and quality
        s.funnel_stage,
        s.session_quality,

        -- Derived: funnel stage number (for ordering in charts)
        CASE s.funnel_stage
            WHEN 'browse_only'   THEN 1
            WHEN 'product_view'  THEN 2
            WHEN 'add_to_cart'   THEN 3
            WHEN 'checkout'      THEN 4
            WHEN 'purchase'      THEN 5
        END                                                     AS funnel_stage_num,

        -- Derived: did session convert?
        CASE WHEN s.funnel_stage = 'purchase' THEN TRUE ELSE FALSE END AS converted,

        -- Derived: cart abandonment (added to cart but did not purchase)
        CASE
            WHEN s.has_add_to_cart AND NOT s.has_purchase
            THEN TRUE ELSE FALSE
        END                                                     AS cart_abandoned,

        -- Derived: checkout abandonment (reached checkout but did not purchase)
        CASE
            WHEN s.has_checkout AND NOT s.has_purchase
            THEN TRUE ELSE FALSE
        END                                                     AS checkout_abandoned

    FROM sessions s
    LEFT JOIN customers c
        ON s.user_id = c.customer_id
)

SELECT * FROM joined