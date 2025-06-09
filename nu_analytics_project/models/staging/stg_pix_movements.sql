{{ config(materialized='view') }}

SELECT
    -- Standardized column names and types
    id AS transaction_id,
    account_id,
    pix_amount AS transaction_amount, 
    TIMESTAMP_SECONDS(pix_requested_at) AS transaction_requested_at,
    TIMESTAMP_SECONDS(CAST(pix_completed_at AS INT64)) AS transaction_completed_at,
    EXTRACT(YEAR FROM TIMESTAMP_SECONDS(CAST(pix_completed_at AS INT64))) AS completed_year,
    EXTRACT(MONTH FROM TIMESTAMP_SECONDS(CAST(pix_completed_at AS INT64))) AS completed_month,
    status,
    in_or_out AS transaction_type  -- pix_in and pix_out as a transaction types
    LOWER(TRIM(status)) AS transaction_status,
    
    -- Standardize direction values
    CASE 
        WHEN LOWER(TRIM(in_or_out)) = 'in' THEN 'in'
        WHEN LOWER(TRIM(in_or_out)) = 'out' THEN 'out'
        ELSE 'unknown'
    END AS transaction_direction,
    
    -- Add metadata
    'pix_movements' AS source_table,
    CURRENT_TIMESTAMP() AS _loaded_at

FROM {{ source('nu_sources', 'pix_movements') }}
-- Filter out deleted/invalid records
WHERE status = 'completed'