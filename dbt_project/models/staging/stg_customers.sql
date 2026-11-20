-- stg_customers.sql
-- Staging model: reads directly from Silver Delta Lake table (active rows only).
-- Renames internal CDC columns to clean business names.

WITH source AS (

    SELECT *
    FROM delta_scan(
        '{{ env_var("SILVER_PATH", "../silver") }}/customers'
    )

),

cleaned AS (

    SELECT
        id                                          AS customer_id,
        first_name,
        last_name,
        CONCAT(first_name, ' ', last_name)          AS full_name,
        email,
        _cdc_op                                     AS cdc_operation,
        -- Convert ms epoch to proper timestamp
        TO_TIMESTAMP(_event_ts / 1000.0)            AS event_timestamp,
        CAST(_ingested_at AS TIMESTAMP)             AS ingested_at,
        CAST(_processed_at AS TIMESTAMP)            AS processed_at,
        _is_deleted                                 AS is_deleted

    FROM source

    -- Only surface active (non-deleted) records in staging
    WHERE _is_deleted = false

)

SELECT * FROM cleaned
