{{ config(materialized='table') }}

WITH account_metrics AS (
    SELECT 
        account_id,
        MIN(month_date) AS first_transaction_month,
        MAX(month_date) AS last_transaction_month,
        COUNT(*) AS total_active_months,
        SUM(total_transactions) AS lifetime_transactions,
        SUM(inbound_volume) AS lifetime_inbound,
        SUM(outbound_volume) AS lifetime_outbound,
        SUM(net_flow) AS lifetime_net_flow,
        AVG(monthly_balance) AS avg_monthly_balance,
        MAX(monthly_balance) AS max_monthly_balance,
        MIN(monthly_balance) AS min_monthly_balance,
        
        -- Get current balance (last month)
        LAST_VALUE(monthly_balance) OVER (
            PARTITION BY account_id 
            ORDER BY month_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS current_balance,
        
        -- Channel preferences
        SUM(pix_transactions) AS total_pix_transactions,
        SUM(transfer_transactions) AS total_transfer_transactions
        
    FROM {{ ref('fct_account_monthly_balances') }}
    GROUP BY account_id
)

SELECT 
    a.account_id,
    a.account_name,
    a.created_timestamp AS creation_utc_date,
    a.account_status AS status,
    
    -- Activity metrics
    m.first_transaction_month,
    m.last_transaction_month,
    m.total_active_months,
    m.lifetime_transactions,
    m.lifetime_inbound,
    m.lifetime_outbound,
    m.lifetime_net_flow,
    m.current_balance,
    m.avg_monthly_balance,
    m.max_monthly_balance,
    m.min_monthly_balance,
    
    -- Customer segmentation
    CASE 
        WHEN m.avg_monthly_balance >= 100000 THEN 'premium'
        WHEN m.avg_monthly_balance >= 10000 THEN 'high_value'
        WHEN m.avg_monthly_balance >= 1000 THEN 'standard'
        ELSE 'basic'
    END AS customer_tier,
    
    CASE 
        WHEN m.lifetime_transactions >= 200 THEN 'high_activity'
        WHEN m.lifetime_transactions >= 50 THEN 'medium_activity'
        WHEN m.lifetime_transactions >= 10 THEN 'low_activity'
        ELSE 'dormant'
    END AS activity_level,
    
    -- Channel preferences
    CASE 
        WHEN m.total_pix_transactions > m.total_transfer_transactions * 2 THEN 'pix_heavy'
        WHEN m.total_transfer_transactions > m.total_pix_transactions * 2 THEN 'transfer_heavy'
        ELSE 'balanced'
    END AS channel_preference,
    
    -- Account age in months
    DATEDIFF('month', a.created_timestamp, CURRENT_DATE()) AS account_age_months,
    
    CURRENT_TIMESTAMP() AS _loaded_at

FROM {{ ref('stg_accounts') }} a
LEFT JOIN account_metrics m ON a.account_id = m.account_id