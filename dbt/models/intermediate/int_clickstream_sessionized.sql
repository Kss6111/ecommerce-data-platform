-- Intermediate model: int_clickstream_sessionized
-- Source: stg_clickstream
-- Grain: one row per session
-- Purpose: collapse raw click events into session-level metrics
--          for funnel analysis and engagement scoring in mart_funnel

WITH clicks AS (
    SELECT * FROM {{ ref('stg_clickstream') }}
),

sessionized AS (
    SELECT
        session_id,

        -- Session identity
        -- Take the user_id that appears most in the session
        -- (handles anonymous → logged-in transitions mid-session)
        MODE(user_id)                                           AS user_id,
        BOOLOR_AGG(is_anonymous = FALSE)                        AS has_authenticated_event,

        -- Device / browser — take first value seen in session
        MIN_BY(device_type, event_timestamp)                    AS device_type,
        MIN_BY(browser, event_timestamp)                        AS browser,

        -- Session timing
        MIN(event_timestamp)                                    AS session_start,
        MAX(event_timestamp)                                    AS session_end,
        DATEDIFF('second', MIN(event_timestamp), MAX(event_timestamp)) AS session_duration_sec,

        -- Engagement metrics
        COUNT(*)                                                AS total_events,
        COUNT(DISTINCT page_url)                                AS unique_pages_viewed,
        COUNT(DISTINCT product_id)                              AS unique_products_viewed,
        COUNT(DISTINCT category)                                AS unique_categories_viewed,
        SUM(time_on_page_sec)                                   AS total_time_on_site_sec,
        AVG(time_on_page_sec)                                   AS avg_time_per_page_sec,

        -- Entry / exit points
        MIN_BY(page_url, event_timestamp)                       AS entry_page,
        MAX_BY(page_url, event_timestamp)                       AS exit_page,
        MIN_BY(referrer, event_timestamp)                       AS referrer,

        -- Event type flags — did this session contain these actions?
        BOOLOR_AGG(event_type = 'PAGE_VIEW')                    AS has_page_view,
        BOOLOR_AGG(event_type = 'PRODUCT_VIEW')                 AS has_product_view,
        BOOLOR_AGG(event_type = 'ADD_TO_CART')                  AS has_add_to_cart,
        BOOLOR_AGG(event_type = 'CHECKOUT')                     AS has_checkout,
        BOOLOR_AGG(event_type = 'PURCHASE')                     AS has_purchase,

        -- Derived: funnel stage reached (highest stage wins)
        CASE
            WHEN BOOLOR_AGG(event_type = 'PURCHASE')    THEN 'purchase'
            WHEN BOOLOR_AGG(event_type = 'CHECKOUT')    THEN 'checkout'
            WHEN BOOLOR_AGG(event_type = 'ADD_TO_CART') THEN 'add_to_cart'
            WHEN BOOLOR_AGG(event_type = 'PRODUCT_VIEW') THEN 'product_view'
            ELSE 'browse_only'
        END                                                     AS funnel_stage,

        -- Derived: session quality score (simple heuristic)
        CASE
            WHEN BOOLOR_AGG(event_type = 'PURCHASE')     THEN 'converted'
            WHEN BOOLOR_AGG(event_type = 'ADD_TO_CART')  THEN 'engaged'
            WHEN COUNT(*) >= 5                            THEN 'active'
            ELSE 'bounce'
        END                                                     AS session_quality

    FROM clicks
    GROUP BY session_id
)

SELECT * FROM sessionized