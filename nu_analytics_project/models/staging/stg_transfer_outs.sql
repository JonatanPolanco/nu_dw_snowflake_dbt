{{ config(materialized='view') }}

SELECT 
    id AS transaction_id,
    account_id,
    amount AS transaction_amount,
    
    -- Handle string timestamps
    TIMESTAMP_SECONDS(transaction_requested_at) AS transaction_requested_at, 
    TIMESTAMP_SECONDS(CAST(transaction_completed_at AS INT64)) AS transaction_completed_at,
    EXTRACT(YEAR FROM TIMESTAMP_SECONDS(CAST(transaction_completed_at AS INT64))) AS completed_year,
    EXTRACT(MONTH FROM TIMESTAMP_SECONDS(CAST(transaction_completed_at AS INT64))) AS completed_month,
    
    LOWER(TRIM(status)) AS transaction_status,
    'out' AS transaction_direction,
    
    'transfer_out' AS source_table,
    CURRENT_TIMESTAMP() AS _loaded_at

FROM {{ source('nu_sources', 'transfer_out') }}
WHERE status = 'completed'
