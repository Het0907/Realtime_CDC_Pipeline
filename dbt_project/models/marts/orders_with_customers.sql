-- orders_with_customers.sql
-- Gold mart: enriched orders — joins orders with customer and product dimensions.
-- This is the "wide" fact table powering most dashboard views.

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}

),

enriched AS (

    SELECT
        o.order_id,
        o.order_date,
        o.quantity,

        -- Customer dimension
        c.customer_id,
        c.full_name                                         AS customer_name,
        c.email                                             AS customer_email,

        -- Product dimension
        p.product_id,
        p.product_name,
        p.weight_kg,

        -- Derived metrics
        o.quantity                                          AS units_sold,

        -- CDC metadata (helpful for auditing)
        o.cdc_operation                                     AS order_cdc_op,
        o.event_timestamp                                   AS order_event_ts,
        o.ingested_at                                       AS order_ingested_at

    FROM orders o
    LEFT JOIN customers c ON o.customer_id = c.customer_id
    LEFT JOIN products  p ON o.product_id  = p.product_id

)

SELECT * FROM enriched
ORDER BY order_date DESC, order_id
