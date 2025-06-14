{{ config(
    materialized='table',
    cluster_by=['account_id'],
    tags=['marts', 'dimension', 'account']
) }}

WITH accounts AS (
    SELECT * FROM {{ ref('stg_accounts') }}
),

-- Get account activity summary
account_activity AS (
    SELECT 
        account_id,
        MIN(transaction_completed_at) AS first_transaction_date,
        MAX(transaction_completed_at) AS last_transaction_date,
        COUNT(*) AS total_lifetime_transactions,
        SUM(transaction_amount) AS total_lifetime_volume,
        COUNT(DISTINCT DATE(transaction_completed_at)) AS active_days
    FROM {{ ref('int_transactions_enriched') }}
    GROUP BY account_id
),

final AS (
    SELECT 
        -- 1. IDENTIFIERS
        a.account_id,
        
        -- 2. DIMENSIONS
        a.account_name,
        a.account_status,
        
        -- 3. DATES
        a.account_created_at,
        act.first_transaction_date,
        act.last_transaction_date,
        
        -- 4. MEASURES
        COALESCE(act.total_lifetime_transactions, 0) AS total_lifetime_transactions,
        COALESCE(act.total_lifetime_volume, 0) AS total_lifetime_volume,
        COALESCE(act.active_days, 0) AS active_days,
        
        -- 5. DERIVED ATTRIBUTES
        -- Account age
        DATEDIFF('day', a.account_created_at, CURRENT_DATE()) AS account_age_days,
        
        CASE 
            WHEN DATEDIFF('day', a.account_created_at, CURRENT_DATE()) < 30 THEN 'New (< 30 days)'
            WHEN DATEDIFF('day', a.account_created_at, CURRENT_DATE()) < 90 THEN 'Recent (30-90 days)'
            WHEN DATEDIFF('day', a.account_created_at, CURRENT_DATE()) < 365 THEN 'Established (3-12 months)'
            ELSE 'Mature (> 1 year)'
        END AS account_age_segment,
        
        -- Activity level
        CASE 
            WHEN COALESCE(act.total_lifetime_transactions, 0) = 0 THEN 'Inactive'
            WHEN COALESCE(act.total_lifetime_transactions, 0) <= 10 THEN 'Low Activity'
            WHEN COALESCE(act.total_lifetime_transactions, 0) <= 50 THEN 'Medium Activity'
            ELSE 'High Activity'
        END AS activity_segment,
        
        -- Value tier
        CASE 
            WHEN COALESCE(act.total_lifetime_volume, 0) = 0 THEN 'No Value'
            WHEN COALESCE(act.total_lifetime_volume, 0) < 1000 THEN 'Low Value'
            WHEN COALESCE(act.total_lifetime_volume, 0) < 10000 THEN 'Medium Value'
            WHEN COALESCE(act.total_lifetime_volume, 0) < 100000 THEN 'High Value'
            ELSE 'Premium Value'
        END AS value_tier,
        
        -- 6. BOOLEANS
        CASE 
            WHEN a.account_status = 'active' THEN TRUE 
            ELSE FALSE 
        END AS is_active,
        
        CASE 
            WHEN act.first_transaction_date IS NOT NULL THEN TRUE 
            ELSE FALSE 
        END AS has_transactions,
        
        CASE 
            WHEN act.last_transaction_date >= CURRENT_DATE() - INTERVAL '30 days' THEN TRUE 
            ELSE FALSE 
        END AS is_recently_active,
        
        CASE 
            WHEN DATEDIFF('day', a.account_created_at, CURRENT_DATE()) <= 30 THEN TRUE 
            ELSE FALSE 
        END AS is_new_account,
        
        -- 7. METADATA
        a.source_table,
        a._loaded_at,
        CURRENT_TIMESTAMP() AS _processed_at

    FROM accounts a
    LEFT JOIN account_activity act ON a.account_id = act.account_id
    WHERE a.account_id IS NOT NULL
)

SELECT * FROM final