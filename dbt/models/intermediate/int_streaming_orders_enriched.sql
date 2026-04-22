-- Intermediate model: int_streaming_orders_enriched
-- Joins: stg_orders_stream + stg_customers
-- Grain: one row per streaming order event
-- Purpose: real-time order events enriched with customer profile data.
--          Left join because streaming events may arrive before customer
--          record is ingested in the batch pipeline.

WITH stream_orders AS (
    SELECT * FROM {{ ref('stg_orders_stream') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

joined AS (
    SELECT
        -- Event identity
        so.event_id,
        so.event_type,
        so.event_timestamp,
        so.event_source,

        -- Order attributes
        so.order_id,
        so.customer_id,
        so.order_status,
        so.payment_method,
        so.total_amount,
        so.item_count,
        so.shipping_city,
        so.shipping_country,

        -- Customer context (may be null if not yet batch-ingested)
        c.full_name                                             AS customer_full_name,
        c.email                                                 AS customer_email,
        c.city                                                  AS customer_city,
        c.state                                                 AS customer_state,
        c.country                                               AS customer_country,

        -- Derived: did we find a matching customer record?
        CASE WHEN c.customer_id IS NOT NULL THEN TRUE ELSE FALSE END AS customer_matched,

        -- Derived: order value tier
        CASE
            WHEN so.total_amount >= 500 THEN 'high'
            WHEN so.total_amount >= 100 THEN 'medium'
            ELSE 'low'
        END                                                     AS order_value_tier,

        -- Kafka metadata for debugging
        so.kafka_offset,
        so.kafka_partition,
        so.kafka_timestamp,

        -- Source tracing
        so.ingested_at,
        so._loaded_at

    FROM stream_orders so
    LEFT JOIN customers c
        ON so.customer_id = c.customer_id
)

SELECT * FROM joined