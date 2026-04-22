-- Intermediate model: int_inventory_enriched
-- Joins: stg_inventory_updates + stg_products + stg_suppliers
-- Grain: one row per inventory movement event
-- Purpose: inventory events with full product and supplier context,
--          feeds mart_products for stock level reporting

WITH inventory AS (
    SELECT * FROM {{ ref('stg_inventory_updates') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

suppliers AS (
    SELECT * FROM {{ ref('stg_suppliers') }}
),

joined AS (
    SELECT
        -- Event identity
        i.event_id,
        i.event_type,
        i.event_timestamp,
        i.movement_type,
        i.reason,
        i.triggered_by,
        i.warehouse_id,

        -- Product identity
        i.product_id,
        i.sku,
        p.product_name,
        p.category,
        p.subcategory,
        p.unit_price,
        p.is_active                                             AS product_is_active,

        -- Supplier context
        p.supplier_id,
        s.supplier_name,
        s.country                                               AS supplier_country,

        -- Inventory movement
        i.quantity_before,
        i.quantity_change,
        i.quantity_after,

        -- Derived: dollar value of the movement (unit_price * quantity changed)
        ROUND(p.unit_price * ABS(i.quantity_change), 2)         AS movement_value,

        -- Derived: stock health after this event
        CASE
            WHEN i.quantity_after = 0   THEN 'out_of_stock'
            WHEN i.quantity_after <= 10 THEN 'critical'
            WHEN i.quantity_after <= 50 THEN 'low'
            ELSE 'healthy'
        END                                                     AS stock_health,

        -- Derived: was this a large movement (>100 units)?
        CASE WHEN ABS(i.quantity_change) > 100 THEN TRUE ELSE FALSE END AS is_large_movement,

        -- Kafka / source metadata
        i.kafka_offset,
        i.kafka_partition,
        i.kafka_timestamp,
        i.ingested_at,
        i._loaded_at

    FROM inventory i
    LEFT JOIN products p
        ON i.product_id = p.product_id
    LEFT JOIN suppliers s
        ON p.supplier_id = s.supplier_id
)

SELECT * FROM joined