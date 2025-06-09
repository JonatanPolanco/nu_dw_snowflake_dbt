{{ config(materialized='table') }}

SELECT 
    month_date,
    year,
    month,
    
    -- Account metrics
    COUNT(DISTINCT account_id) AS total_active_accounts,
    COUNT(DISTINCT CASE WHEN has_activity THEN account_id END) AS accounts_with_transactions,
    COUNT(DISTINCT CASE WHEN balance_category = 'negative' THEN account_id END) AS accounts_negative_balance,
    COUNT(DISTINCT CASE WHEN balance_category IN ('high', 'premium') THEN account_id END) AS high_value_accounts,
    
    -- Financial KPIs
    SUM(inbound_volume) AS total_inbound_volume,
    SUM(outbound_volume) AS total_outbound_volume,
    SUM(net_flow) AS total_net_flow,
    SUM(monthly_balance) AS total_system_balance,
    
    -- Transaction metrics
    SUM(total_transactions) AS total_transactions,
    SUM(pix_transactions) AS total_pix_transactions,
    SUM(transfer_transactions) AS total_transfer_transactions,
    
    -- Averages and distributions
    AVG(monthly_balance) AS avg_account_balance,
    MEDIAN(monthly_balance) AS median_account_balance,
    AVG(total_transactions) AS avg_transactions_per_account,
    
    -- Channel analysis
    ROUND(SUM(pix_transactions) * 100.0 / NULLIF(SUM(total_transactions), 0), 2) AS pix_transaction_share_pct,
    ROUND(SUM(pix_net_flow) * 100.0 / NULLIF(SUM(net_flow), 0), 2) AS pix_volume_share_pct,
    
    -- Growth metrics (vs previous month)
    LAG(SUM(total_transactions)) OVER (ORDER BY year, month) AS prev_month_transactions,
    ROUND(
        (SUM(total_transactions) - LAG(SUM(total_transactions)) OVER (ORDER BY year, month)) * 100.0 
        / NULLIF(LAG(SUM(total_transactions)) OVER (ORDER BY year, month), 0), 
        2
    ) AS transaction_growth_mom_pct,
    
    CURRENT_TIMESTAMP() AS _loaded_at

FROM {{ ref('fct_account_monthly_balances') }}
GROUP BY month_date, year, month
ORDER BY year, month
