{{ config(materialized='table') }}

WITH customer_summary AS (
    SELECT 
        da.customer_tier,
        da.activity_level,
        da.channel_preference,
        dc.country,
        dc.state,
        
        COUNT(*) AS account_count,
        SUM(da.current_balance) AS total_balance,
        SUM(da.lifetime_transactions) AS total_transactions,
        AVG(da.current_balance) AS avg_balance,
        AVG(da.lifetime_transactions) AS avg_transactions,
        AVG(da.account_age_months) AS avg_account_age_months
        
    FROM {{ ref('dim_account') }} da
    LEFT JOIN {{ ref('dim_customer') }} dc ON da.customer_id = dc.customer_id
    WHERE da.status = 'active'
    GROUP BY 1, 2, 3, 4, 5
)

SELECT 
    *,
    
    -- Segment sizing
    ROUND(account_count * 100.0 / SUM(account_count) OVER (), 2) AS account_share_pct,
    ROUND(total_balance * 100.0 / SUM(total_balance) OVER (), 2) AS balance_share_pct,