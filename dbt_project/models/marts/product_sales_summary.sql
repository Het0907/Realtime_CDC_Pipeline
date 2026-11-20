-- product_sales_summary.sql
-- Gold mart: one row per product with sales aggregates.
-- Used for product analytics and top-sellers in the dashboard.

WITH enriched AS (
    SELECT * FROM {{ ref('orders_with_customers') }}
),

summary AS (

    SELECT
        product_id,
        product_name,
        weight_kg,

        COUNT(DISTINCT order_id)            AS total_orders,
        SUM(units_sold)                     AS total_units_sold,
        COUNT(DISTINCT customer_id)         AS unique_customers,
        MIN(order_date)                     AS first_sold_date,
        MAX(order_date)                     AS last_sold_date

    FROM enriched
    WHERE product_id IS NOT NULL
    GROUP BY 1, 2, 3

)

SELECT
    *,
    RANK() OVER (ORDER BY total_units_sold DESC) AS sales_rank

FROM summary
ORDER BY total_units_sold DESC
