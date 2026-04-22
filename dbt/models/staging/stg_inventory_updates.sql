-- Staging model: stg_inventory_updates
-- Source: RAW.INVENTORY_UPDATES (Kafka → S3 → Snowflake via Airflow)
-- Note: EVENT_TIMESTAMP is VARCHAR in raw

WITH source AS (
    SELECT * FROM {{ source('raw', 'inventory_updates') }}
),

cleaned AS (
    SELECT
        -- Event identity
        event_id,
        UPPER(TRIM(event_type))                                 AS event_type,

        -- Cast VARCHAR timestamp
        TRY_TO_TIMESTAMP(event_timestamp)                       AS event_timestamp,

        -- Product / warehouse identity
        product_id,
        TRIM(sku)                                               AS sku,
        TRIM(warehouse_id)                                      AS warehouse_id,

        -- Quantity movement — all three columns together tell the full story
        CAST(quantity_before AS INT)                            AS quantity_before,
        CAST(quantity_change AS INT)                            AS quantity_change,
        CAST(quantity_after AS INT)                             AS quantity_after,

        -- Derived: flag restock vs. depletion events for easy filtering downstream
        CASE
            WHEN CAST(quantity_change AS INT) > 0  THEN 'restock'
            WHEN CAST(quantity_change AS INT) < 0  THEN 'depletion'
            ELSE 'adjustment'
        END                                                     AS movement_type,

        -- Context
        TRIM(reason)                                            AS reason,
        TRIM(triggered_by)                                      AS triggered_by,

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