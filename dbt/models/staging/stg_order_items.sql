-- Staging model: stg_order_items
-- Source: RAW.ORDER_ITEMS (batch ingested via Airflow)

WITH source AS (
    SELECT * FROM {{ source('raw', 'order_items') }}
),

cleaned AS (
    SELECT
        -- Primary key
        order_item_id,

        -- Foreign keys
        order_id,
        product_id,

        -- Line item values — defend against nulls on numeric fields
        CAST(quantity AS INT)                                   AS quantity,
        CAST(unit_price AS NUMERIC(12, 2))                      AS unit_price,
        COALESCE(CAST(discount AS NUMERIC(12, 4)), 0.0)         AS discount,
        CAST(subtotal AS NUMERIC(12, 2))                        AS subtotal,

        -- Derived: effective unit price after discount (useful for mart revenue calcs)
        ROUND(
            CAST(unit_price AS NUMERIC(12, 4)) 
            * (1 - COALESCE(CAST(discount AS NUMERIC(12, 4)), 0.0)),
            2
        )                                                       AS effective_unit_price,

        -- Timestamps
        created_at,

        -- Metadata
        _ingested_at

    FROM source
    WHERE order_item_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_item_id ORDER BY _ingested_at DESC) = 1
)

SELECT * FROM cleaned