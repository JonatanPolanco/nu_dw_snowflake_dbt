{{ config(
    materialized='table',
    cluster_by=['customer_id'],
    tags=['marts', 'dimension', 'customer']
) }}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

final AS (
    SELECT 
        -- 1. IDENTIFIERS
        customer_id,
        cpf,
        
        -- 2. DIMENSIONS
        first_name,
        last_name,
        CONCAT(first_name, ' ', last_name) AS full_name,
        customer_city,
        country_name,
        
        -- 3. DERIVED ATTRIBUTES
        -- Customer name analysis
        CASE 
            WHEN LENGTH(first_name) + LENGTH(last_name) > 30 THEN 'Long Name'
            WHEN LENGTH(first_name) + LENGTH(last_name) > 15 THEN 'Medium Name'
            ELSE 'Short Name'
        END AS name_length_category,
        
        -- Location standardization
        UPPER(TRIM(customer_city)) AS city_normalized,
        UPPER(TRIM(country_name)) AS country_normalized,
        
        -- 4. BOOLEANS
        CASE 
            WHEN first_name IS NOT NULL AND last_name IS NOT NULL THEN TRUE 
            ELSE FALSE 
        END AS has_complete_name,
        
        CASE 
            WHEN customer_city IS NOT NULL THEN TRUE 
            ELSE FALSE 
        END AS has_city_info,
        
        CASE 
            WHEN cpf IS NOT NULL AND LENGTH(cpf) = 11 THEN TRUE 
            ELSE FALSE 
        END AS has_valid_cpf,
        
        -- 5. METADATA
        source_table,
        _loaded_at,
        CURRENT_TIMESTAMP() AS _processed_at

    FROM customers
    WHERE customer_id IS NOT NULL
)

SELECT * FROM final