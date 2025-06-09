config{{ materialized='view' }}

SELECT
    customer_id,
    first_name,
    last_name,
    customer_city,
    cpf,
    country_name,
    -- Add metadata
    'customers' AS source_table,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM {{ source('nu_sources', 'customers') }}
WHERE customer_id IS NOT NULL
