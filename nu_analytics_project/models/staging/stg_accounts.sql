config{{ materialized='view' }}

SELECT
    account_id AS account_id,
    account_name AS account_name,
    created_at AS account_created_at,
    status AS account_status,
    
    -- Add metadata
    'accounts' AS source_table,
    CURRENT_TIMESTAMP() AS _loaded_at

FROM {{ source('nu_sources', 'accounts') }}
WHERE account_id IS NOT NULL