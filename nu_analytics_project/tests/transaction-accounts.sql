SELECT 
    t.account_id,
    COUNT(*) as orphaned_transactions
FROM (
    SELECT account_id FROM {{ ref('stg_pix_movements') }}
    UNION ALL
    SELECT account_id FROM {{ ref('stg_transfer_ins') }}
    UNION ALL  
    SELECT account_id FROM {{ ref('stg_transfer_outs') }}
) t
LEFT JOIN {{ ref('stg_accounts') }} a ON t.account_id = a.account_id
WHERE a.account_id IS NULL
GROUP BY t.account_id