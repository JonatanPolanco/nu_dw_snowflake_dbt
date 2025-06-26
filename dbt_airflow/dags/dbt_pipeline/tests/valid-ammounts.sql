SELECT *
FROM (
    SELECT 'pix_movements' as source_table, transaction_id, transaction_amount
    FROM {{ ref('stg_pix_movements') }}
    WHERE amount <= 0

    UNION ALL

    SELECT 'transfer_ins' as source_table, transfer_id as transaction_id, amount  
    FROM {{ ref('stg_transfer_ins') }}
    WHERE amount <= 0

    UNION ALL

    SELECT 'transfer_outs' as source_table, transfer_id as transaction_id, amount
    FROM {{ ref('stg_transfer_outs') }}
    WHERE amount <= 0
) invalid_amounts