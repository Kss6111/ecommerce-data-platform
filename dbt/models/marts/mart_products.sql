-- Mart model: mart_products
-- Source: stg_products + stg_suppliers + int_order_lines + int_inventory_enriched
-- Grain: one row per product
-- Materialization: table
-- Purpose: product performance and inventory health for dashboards

WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

suppliers AS (
    SELECT * FROM {{ ref('stg_suppliers') }}
),

-- Sales metrics per product from order lines
product_sales AS (
    SELECT
        product_id,
        COUNT(DISTINCT order_id)                                AS total_orders,
        SUM(quantity)                                           AS total_units_sold,
        SUM(line_revenue)                                       AS total_revenue,
        AVG(line_revenue)                                       AS avg_revenue_per_order,
        SUM(discount_amount)                                    AS total_discount_given,
        AVG(discount)                                           AS avg_discount_rate,
        COUNT(DISTINCT customer_id)                             AS unique_customers,
        MIN(order_date)                                         AS first_sale_date,
        MAX(order_date)                                         AS last_sale_date
    FROM {{ ref('int_order_lines') }}
    WHERE NOT is_cancelled
      AND NOT is_refunded
    GROUP BY product_id
),

-- Latest inventory snapshot per product
latest_inventory AS (
    SELECT
        product_id,
        -- Most recent quantity_after is current stock level
        MAX_BY(quantity_after, event_timestamp)                 AS current_stock,
        MAX_BY(stock_health, event_timestamp)                   AS current_stock_health,
        COUNT(*)                                                AS total_inventory_events,
        SUM(CASE WHEN movement_type = 'restock'   THEN ABS(quantity_change) ELSE 0 END) AS total_units_restocked,
        SUM(CASE WHEN movement_type = 'depletion' THEN ABS(quantity_change) ELSE 0 END) AS total_units_depleted,
        MAX(event_timestamp)                                    AS last_inventory_update
    FROM {{ ref('int_inventory_enriched') }}
    GROUP BY product_id
),

joined AS (
    SELECT
        -- Product identity
        p.product_id,
        p.product_name,
        p.category,
        p.subcategory,
        p.sku,
        p.unit_price,
        p.is_active,

        -- Supplier context
        p.supplier_id,
        s.supplier_name,
        s.country                                               AS supplier_country,

        -- Sales performance
        COALESCE(ps.total_orders, 0)                            AS total_orders,
        COALESCE(ps.total_units_sold, 0)                        AS total_units_sold,
        COALESCE(ps.total_revenue, 0)                           AS total_revenue,
        COALESCE(ps.avg_revenue_per_order, 0)                   AS avg_revenue_per_order,
        COALESCE(ps.total_discount_given, 0)                    AS total_discount_given,
        COALESCE(ps.avg_discount_rate, 0)                       AS avg_discount_rate,
        COALESCE(ps.unique_customers, 0)                        AS unique_customers,
        ps.first_sale_date,
        ps.last_sale_date,

        -- Inventory status
        COALESCE(li.current_stock, p.stock_quantity)            AS current_stock,
        COALESCE(li.current_stock_health, 
            CASE
                WHEN p.stock_quantity = 0   THEN 'out_of_stock'
                WHEN p.stock_quantity <= 10 THEN 'critical'
                WHEN p.stock_quantity <= 50 THEN 'low'
                ELSE 'healthy'
            END
        )                                                       AS current_stock_health,
        COALESCE(li.total_inventory_events, 0)                  AS total_inventory_events,
        COALESCE(li.total_units_restocked, 0)                   AS total_units_restocked,
        COALESCE(li.total_units_depleted, 0)                    AS total_units_depleted,
        li.last_inventory_update,

        -- Derived: revenue per unit (sell-through efficiency)
        CASE
            WHEN COALESCE(ps.total_units_sold, 0) > 0
            THEN ROUND(ps.total_revenue / ps.total_units_sold, 2)
            ELSE 0
        END                                                     AS revenue_per_unit_sold,

        -- Derived: product performance tier
        CASE
            WHEN COALESCE(ps.total_revenue, 0) >= 10000 THEN 'top_performer'
            WHEN COALESCE(ps.total_revenue, 0) >= 5000  THEN 'strong'
            WHEN COALESCE(ps.total_revenue, 0) >= 1000  THEN 'moderate'
            WHEN COALESCE(ps.total_revenue, 0) > 0      THEN 'low_performer'
            ELSE 'no_sales'
        END                                                     AS performance_tier,

        -- Metadata
        p.created_at,
        p._ingested_at

    FROM products p
    LEFT JOIN suppliers s
        ON p.supplier_id = s.supplier_id
    LEFT JOIN product_sales ps
        ON p.product_id = ps.product_id
    LEFT JOIN latest_inventory li
        ON p.product_id = li.product_id
)

SELECT * FROM joined