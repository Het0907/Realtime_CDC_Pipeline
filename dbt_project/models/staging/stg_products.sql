-- stg_products.sql
-- Staging model: reads from Silver Delta table for products.

WITH source AS (

    SELECT *
    FROM delta_scan(
        '{{ env_var("SILVER_PATH", "../silver") }}/products'
    )

),

cleaned AS (

    SELECT
        id                                          AS product_id,
        name                                        AS product_name,
        description                                 AS product_description,
        CAST(weight AS DOUBLE)                      AS weight_kg,
        _cdc_op                                     AS cdc_operation,
        TO_TIMESTAMP(_event_ts / 1000.0)            AS event_timestamp,
        CAST(_ingested_at AS TIMESTAMP)             AS ingested_at,
        _is_deleted                                 AS is_deleted

    FROM source
    WHERE _is_deleted = false

)

SELECT * FROM cleaned
