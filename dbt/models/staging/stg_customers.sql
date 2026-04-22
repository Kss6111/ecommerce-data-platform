-- Staging model: stg_customers
-- Source: RAW.CUSTOMERS (batch ingested via Airflow)
-- Transformations: snake_case already correct, explicit casts, null guards on key fields

WITH source AS (
    SELECT * FROM {{ source('raw', 'customers') }}
),

cleaned AS (
    SELECT
        customer_id,
        TRIM(first_name)                                        AS first_name,
        TRIM(last_name)                                         AS last_name,
        COALESCE(TRIM(first_name), '') 
            || ' ' || 
        COALESCE(TRIM(last_name), '')                           AS full_name,
        LOWER(TRIM(email))                                      AS email,
        TRIM(phone)                                             AS phone,
        TRIM(address)                                           AS address,
        TRIM(city)                                              AS city,
        TRIM(state)                                             AS state,
        TRIM(country)                                           AS country,
        TRIM(postal_code)                                       AS postal_code,
        created_at,
        updated_at,
        _ingested_at
    FROM source
    WHERE customer_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _ingested_at DESC) = 1
)

SELECT * FROM cleaned