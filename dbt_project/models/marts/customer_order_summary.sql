-- customer_order_summary.sql
-- Gold mart: one row per customer with aggregated order metrics.
-- Used for customer analytics in the dashboard.

WITH enriched AS (
    SELECT * FROM {{ ref('orders_with_customers') }}
),

summary AS (

    SELECT
        customer_id,
        customer_name,
        customer_email,

        COUNT(DISTINCT order_id)                    AS total_orders,
        SUM(units_sold)                             AS total_units_purchased,
        MIN(order_date)                             AS first_order_date,
        MAX(order_date)                             AS last_order_date,
        -- Recency: days since last order
        DATEDIFF('day', MAX(order_date), CURRENT_DATE) AS days_since_last_order,

        -- Most purchased product
        MODE(product_name)                          AS favourite_product

    FROM enriched
    WHERE customer_id IS NOT NULL
    GROUP BY 1, 2, 3

)

SELECT
    *,
    CASE
        WHEN total_orders >= 5 THEN 'VIP'
        WHEN total_orders >= 2 THEN 'Returning'
        ELSE                        'New'
    END AS customer_segment

FROM summary
ORDER BY total_orders DESC
