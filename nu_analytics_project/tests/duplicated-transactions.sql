WITH transaction_counts AS (
    SELECT 
        transaction_id,
        COUNT(*) as occurrence_count
    FROM {{ ref('int_unified_transactions') }}
    GROUP BY transaction_id
    HAVING COUNT(*) > 1
)

SELECT * FROM transaction_counts