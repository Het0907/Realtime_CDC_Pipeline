-- stg_orders.sql
-- Staging model: reads from Silver Delta table, decodes Debezium date encoding.

WITH source AS (

    SELECT *
    FROM delta_scan(
        '{{ env_var("SILVER_PATH", "../silver") }}/orders'
    )

),

cleaned AS (

    SELECT
        id                                              AS order_id,
        purchaser                                       AS customer_id,
        product_id,
        quantity,
        -- Debezium encodes DATE as days since epoch (1970-01-01) as integer
        -- After silver_processor converts it to string YYYY-MM-DD, cast back:
        TRY_CAST(order_date AS DATE)                    AS order_date,
        _cdc_op                                         AS cdc_operation,
        TO_TIMESTAMP(_event_ts / 1000.0)                AS event_timestamp,
        CAST(_ingested_at AS TIMESTAMP)                 AS ingested_at,
        _is_deleted                                     AS is_deleted

    FROM source
    WHERE _is_deleted = false

)

SELECT * FROM cleaned
