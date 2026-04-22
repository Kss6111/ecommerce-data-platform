-- Staging model: stg_suppliers
-- Source: RAW.SUPPLIERS (batch ingested via Airflow)

WITH source AS (
    SELECT * FROM {{ source('raw', 'suppliers') }}
),

cleaned AS (
    SELECT
        -- Primary key
        supplier_id,

        -- Supplier identity
        TRIM(supplier_name)                                     AS supplier_name,
        TRIM(contact_name)                                      AS contact_name,

        -- Contact
        LOWER(TRIM(email))                                      AS email,
        TRIM(phone)                                             AS phone,

        -- Location
        TRIM(address)                                           AS address,
        TRIM(city)                                              AS city,
        TRIM(country)                                           AS country,

        -- Timestamps
        created_at,

        -- Metadata
        _ingested_at

    FROM source
    WHERE supplier_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY _ingested_at DESC) = 1
)

SELECT * FROM cleaned