{{ config(
    materialized='table',
    cluster_by=['account_id', 'completed_date'],
    tags=['marts', 'fact', 'transactions']
) }}

WITH enriched_transactions AS (
    SELECT * FROM {{ ref('int_transactions_enriched') }}
),

final AS (
    SELECT 
        -- 1. IDENTIFIERS
        transaction_id,
        account_id,
        
        -- 2. DIMENSIONS
        transaction_channel,
        transaction_direction,
        transaction_size_category,
        source_table,
        
        -- Time dimensions
        completed_year,
        completed_month_number,
        completed_week,
        completed_weekday,
        completed_quarter,
        
        -- 3. MEASURES
        transaction_amount,
        signed_amount,
        
        -- 4. DATES/TIMESTAMPS
        transaction_completed_at,
        completed_date,
        completed_month_date,
        
        -- 5. BOOLEANS
        is_large_transaction,
        
        -- 6. DERIVED ATTRIBUTES
        currency_code,
        
        -- Direction flags for easier analysis
        CASE 
            WHEN transaction_direction = 'TRANSFER_IN' THEN transaction_amount
            ELSE 0 
        END AS inbound_amount,
        
        CASE 
            WHEN transaction_direction = 'TRANSFER_OUT' THEN transaction_amount
            ELSE 0 
        END AS outbound_amount,
        
        -- Channel flags
        CASE 
            WHEN transaction_channel = 'PIX' THEN transaction_amount
            ELSE 0 
        END AS pix_amount,
        
        CASE 
            WHEN transaction_channel = 'TRANSFER' THEN transaction_amount
            ELSE 0 
        END AS transfer_amount,
        
        CURRENT_TIMESTAMP() AS _processed_at
        
    FROM enriched_transactions
    WHERE transaction_id IS NOT NULL
      AND account_id IS NOT NULL
      AND transaction_amount > 0
      AND transaction_completed_at IS NOT NULL
)

SELECT * FROM final