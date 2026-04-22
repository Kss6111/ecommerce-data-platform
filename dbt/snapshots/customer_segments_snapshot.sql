{% snapshot customer_segments_snapshot %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='customer_id',
        strategy='check',
        check_cols=['customer_segment', 'lifetime_value', 'total_orders', 'is_active_customer'],
        invalidate_hard_deletes=True
    )
}}

SELECT
    customer_id,
    full_name,
    email,
    customer_segment,
    lifetime_value,
    total_orders,
    is_active_customer,
    days_since_last_order,
    preferred_category,
    CURRENT_TIMESTAMP()                                         AS snapshotted_at
FROM {{ ref('mart_customers') }}

{% endsnapshot %}