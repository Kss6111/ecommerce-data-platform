-- Staging model: stg_orders_stream
-- Source: RAW.ORDERS_STREAM (Kafka → S3 → Snowflake via Airflow)
-- Note: EVENT_TIMESTAMP is VARCHAR in raw — cast carefully with TRY_TO_TIMESTAMP

WITH source AS (
    SELECT * FROM {{ source('raw', 'orders_stream') }}
),

cleaned AS (
    SELECT
        -- Event identity
        event_id,
        UPPER(TRIM(event_type))                                 AS event_type,

        -- Cast VARCHAR timestamp — TRY_TO_TIMESTAMP returns NULL on bad values
        -- instead of hard-failing the entire model
        TRY_TO_TIMESTAMP(event_timestamp)                       AS event_timestamp,

        -- Order attributes
        order_id,
        customer_id,
        UPPER(TRIM(order_status))                               AS order_status,
        UPPER(TRIM(payment_method))                             AS payment_method,
        CAST(total_amount AS NUMERIC(12, 2))                    AS total_amount,
        CAST(item_count AS INT)                                 AS item_count,

        -- Shipping location
        TRIM(shipping_city)                                     AS shipping_city,
        TRIM(shipping_country)                                  AS shipping_country,

        -- Kafka metadata — keep for debugging / replay auditing
        kafka_offset,
        kafka_partition,
        kafka_timestamp,

        -- Source tracing
        source                                                  AS event_source,
        ingested_at,
        _loaded_at,
        _source_file

    FROM source
    WHERE event_id IS NOT NULL
      -- Filter out records where timestamp cast failed entirely
      AND TRY_TO_TIMESTAMP(event_timestamp) IS NOT NULL
)

SELECT * FROM cleaned