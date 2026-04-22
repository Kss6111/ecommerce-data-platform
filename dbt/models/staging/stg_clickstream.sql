-- Staging model: stg_clickstream
-- Source: RAW.CLICKSTREAM (Kafka → S3 → Snowflake via Airflow)
-- Note: EVENT_TIMESTAMP is VARCHAR in raw

WITH source AS (
    SELECT * FROM {{ source('raw', 'clickstream') }}
),

cleaned AS (
    SELECT
        -- Event identity
        event_id,
        session_id,
        UPPER(TRIM(event_type))                                 AS event_type,

        -- Cast VARCHAR timestamp
        TRY_TO_TIMESTAMP(event_timestamp)                       AS event_timestamp,

        -- User identity — user_id may be anonymous ('guest', null) for pre-login events
        user_id,
        CASE
            WHEN user_id IS NULL OR LOWER(TRIM(user_id)) IN ('null', 'guest', '')
            THEN TRUE
            ELSE FALSE
        END                                                     AS is_anonymous,

        -- Page / product context
        TRIM(page_url)                                          AS page_url,
        product_id,
        TRIM(category)                                          AS category,

        -- Device / browser
        LOWER(TRIM(device_type))                                AS device_type,
        LOWER(TRIM(browser))                                    AS browser,
        TRIM(referrer)                                          AS referrer,

        -- Engagement
        CAST(time_on_page_ms AS INT)                            AS time_on_page_ms,
        ROUND(CAST(time_on_page_ms AS FLOAT) / 1000.0, 2)      AS time_on_page_sec,

        -- Kafka metadata
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
      AND TRY_TO_TIMESTAMP(event_timestamp) IS NOT NULL
)

SELECT * FROM cleaned