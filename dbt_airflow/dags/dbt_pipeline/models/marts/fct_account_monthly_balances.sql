{{ config(
    materialized='table',
    cluster_by=['account_id', 'month_date'],
    tags=['marts', 'fact', 'balances']
) }}

WITH monthly_summary AS (
    SELECT * FROM {{ ref('int_monthly_transaction_summary') }}
),

final AS (
    SELECT 
        -- 1. IDENTIFIERS
        account_id,
        
        -- 2. DIMENSIONS
        year,
        month,
        
        -- 3. MEASURES - Volume metrics
        inbound_volume,
        outbound_volume,
        net_flow,
        
        -- Transaction counts
        total_transactions,
        inbound_transactions,
        outbound_transactions,
        
        -- Channel breakdown
        pix_net_flow,
        transfer_net_flow,
        pix_transactions,
        transfer_transactions,
        
        -- Average amounts (using existing columns)
        avg_inbound_transaction_amount,
        avg_outbound_transaction_amount,
        
        -- 4. DATES
        month_date,
        
        -- 5. BOOLEANS
        has_activity,
        
        -- Additional flags
        CASE 
            WHEN inbound_volume > outbound_volume THEN TRUE 
            ELSE FALSE 
        END AS is_net_positive,
        
        CASE 
            WHEN total_transactions >= 10 THEN TRUE 
            ELSE FALSE 
        END AS is_high_activity,
        
        CASE 
            WHEN pix_transactions > transfer_transactions THEN TRUE 
            ELSE FALSE 
        END AS prefers_pix,
        
        -- 6. DERIVED METRICS
        -- Balance ratios
        CASE 
            WHEN outbound_volume > 0 
            THEN ROUND(inbound_volume / outbound_volume, 2)
            ELSE NULL 
        END AS inbound_outbound_ratio,
        
        -- Activity intensity
        CASE 
            WHEN total_transactions = 0 THEN 'No Activity'
            WHEN total_transactions <= 5 THEN 'Low Activity'
            WHEN total_transactions <= 20 THEN 'Medium Activity'
            ELSE 'High Activity'
        END AS activity_level,
        
        -- Channel preference
        CASE 
            WHEN pix_transactions = 0 AND transfer_transactions = 0 THEN 'No Activity'
            WHEN pix_transactions > transfer_transactions THEN 'PIX Preferred'
            WHEN transfer_transactions > pix_transactions THEN 'Transfer Preferred'
            ELSE 'Mixed Usage'
        END AS channel_preference,
        
        -- 7. METADATA
        _loaded_at,
        CURRENT_TIMESTAMP() AS _processed_at

    FROM monthly_summary
    WHERE account_id IS NOT NULL
      AND month_date IS NOT NULL
)

SELECT * FROM final