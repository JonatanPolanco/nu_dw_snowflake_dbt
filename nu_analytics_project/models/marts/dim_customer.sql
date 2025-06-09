{{ config(materialized='table') }}

SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    CONCAT(c.first_name, ' ', c.last_name) AS full_name,
    c.cpf_number,
    c.country_name,
    
    -- Location information
    c.location_id,
    l.city,
    l.state,
    l.country,
    l.country_iso2,
    l.full_location_path,
    
    CURRENT_TIMESTAMP() AS _loaded_at

FROM {{ ref('stg_customers') }} c
LEFT JOIN {{ ref('base_location_hierarchy') }} l 
    ON c.location_id = l.location_id
