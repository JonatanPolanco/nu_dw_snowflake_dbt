{{ config(materialized='table') }}

WITH pix_transactions AS (
    SELECT 
        transaction_id,
        account_id,
        transaction_amount,
        transaction_requested_at,
        transaction_completed_at,
        transaction_status,
        transaction_direction,
        'PIX' AS transaction_channel,
        CASE transaction_direction
            WHEN 'in' THEN 'PIX_IN'
            WHEN 'out' THEN 'PIX_OUT'
        END AS transaction_type,
        source_table
    FROM {{ ref('stg_pix_movements') }}
),

transfer_transactions AS (
    SELECT 
        transaction_id,
        account_id,
        transaction_amount,
        transaction_requested_at,
        transaction_completed_at,
        transaction_status,
        transaction_direction,
        'TRANSFER' AS transaction_channel,
        CASE transaction_direction
            WHEN 'in' THEN 'TRANSFER_IN'
            WHEN 'out' THEN 'TRANSFER_OUT'
        END AS transaction_type,
        source_table
    FROM {{ ref('stg_transfer_ins') }}
    
    UNION ALL
    
    SELECT 
        transaction_id,
        account_id,
        transaction_amount,
        transaction_requested_at,
        transaction_completed_at,
        transaction_status,
        transaction_direction,
        'TRANSFER' AS transaction_channel,
        CASE transaction_direction
            WHEN 'in' THEN 'TRANSFER_IN'
            WHEN 'out' THEN 'TRANSFER_OUT'
        END AS transaction_type,
        source_table
    FROM {{ ref('stg_transfer_outs') }}
)

SELECT * FROM pix_transactions
UNION ALL  
SELECT * FROM transfer_transactions