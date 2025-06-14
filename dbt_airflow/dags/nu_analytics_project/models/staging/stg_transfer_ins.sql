{{ config(materialized='view') }}

SELECT 
    id AS transaction_id,
    account_id,
    amount AS transaction_amount,
    
    -- Handle string timestamps
    TO_TIMESTAMP(transaction_requested_at) AS transaction_requested_at, 
    TO_TIMESTAMP(CAST(transaction_completed_at AS BIGINT)) AS transaction_completed_at,
    EXTRACT(YEAR FROM TO_TIMESTAMP(CAST(transaction_completed_at AS BIGINT))) AS completed_year,
    EXTRACT(MONTH FROM TO_TIMESTAMP(CAST(transaction_completed_at AS BIGINT))) AS completed_month,
    
    LOWER(TRIM(status)) AS transaction_status,
    'in' AS transaction_direction,
    
    'transfer_ins' AS source_table,
    CURRENT_TIMESTAMP() AS _loaded_at

FROM {{ source('nu_sources', 'transfer_ins') }}
WHERE status = 'completed'
