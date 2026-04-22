-- Staging model: stg_products
-- Source: RAW.PRODUCTS (batch ingested via Airflow)

WITH source AS (
    SELECT * FROM {{ source('raw', 'products') }}
),

cleaned AS (
    SELECT
        -- Primary key
        product_id,

        -- Foreign key
        supplier_id,

        -- Product identity
        TRIM(product_name)                                      AS product_name,
        TRIM(category)                                          AS category,
        TRIM(subcategory)                                       AS subcategory,
        TRIM(sku)                                               AS sku,

        -- Pricing & inventory — explicit cast to defend against upstream type drift
        CAST(unit_price AS NUMERIC(12, 2))                      AS unit_price,
        CAST(stock_quantity AS INT)                             AS stock_quantity,

        -- Status — already BOOLEAN in raw, keep as-is
        is_active,

        -- Timestamps
        created_at,
        updated_at,

        -- Metadata
        _ingested_at

    FROM source
    WHERE product_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _ingested_at DESC) = 1
)

SELECT * FROM cleaned

