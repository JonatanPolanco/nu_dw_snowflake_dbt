{{ config(
    materialized='table',
    cluster_by=['account_id', 'month_date'],
    post_hook="ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION;"
) }}

WITH balance_calculation AS (
    SELECT 
        account_id,
        month_date,
        year,
        month,
        inbound_volume,
        outbound_volume,
        net_flow,
        total_transactions,
        
        -- Running balance calculation
        SUM(net_flow) OVER (
            PARTITION BY account_id 
            ORDER BY year, month 
            ROWS UNBOUNDED PRECEDING
        ) AS monthly_balance,
        
        -- Channel metrics
        pix_net_flow,
        transfer_net_flow,
        pix_transactions,
        transfer_transactions,
        
        -- Transaction size distribution
        micro_transactions,
        small_transactions, 
        medium_transactions,
        large_transactions,
        
        -- Averages
        avg_inbound_amount,
        avg_outbound_amount
        
    FROM {{ ref('int_monthly_transaction_summary') }}
)

SELECT 
    *,
    
    -- Activity indicators
    CASE WHEN total_transactions > 0 THEN TRUE ELSE FALSE END AS has_activity,
    
    -- Balance categories
    CASE 
        WHEN monthly_balance < 0 THEN 'negative'
        WHEN monthly_balance = 0 THEN 'zero'
        WHEN monthly_balance BETWEEN 0 AND 1000 THEN 'low'
        WHEN monthly_balance BETWEEN 1000 AND 10000 THEN 'medium'
        WHEN monthly_balance BETWEEN 10000 AND 100000 THEN 'high'
        ELSE 'premium'
    END AS balance_category,
    
    -- Net flow direction
    CASE 
        WHEN net_flow > 0 THEN 'positive'
        WHEN net_flow < 0 THEN 'negative'
        ELSE 'neutral'
    END AS net_flow_direction,
    
    -- Month-over-month balance change
    monthly_balance - LAG(monthly_balance) OVER (
        PARTITION BY account_id 
        ORDER BY year, month
    ) AS balance_change_mom,
    
    -- Channel preference
    CASE 
        WHEN pix_transactions > transfer_transactions THEN 'pix_preferred'
        WHEN transfer_transactions > pix_transactions THEN 'transfer_preferred' 
        WHEN pix_transactions = transfer_transactions AND pix_transactions > 0 THEN 'balanced'
        ELSE 'inactive'
    END AS channel_preference,
    
    CURRENT_TIMESTAMP() AS _loaded_at

FROM balance_calculation