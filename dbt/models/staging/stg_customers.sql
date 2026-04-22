-- Staging model: customers
-- Source: RAW.CUSTOMERS loaded by batch_ingestion_dag

WITH source AS (
    SELECT * FROM {{ source('raw', 'customers') }}
),

renamed AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        address,
        city,
        state,
        country,
        postal_code,
        created_at,
        updated_at,
        _ingested_at
    FROM source
)

SELECT * FROM renamed