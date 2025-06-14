{{ config(materialized='table') }}

SELECT 
    c.city_id AS location_id,
    c.city_name AS city,
    c.state_id,
    s.state AS state,
    s.country_id,
    co.country AS country,
    
    -- Add standardized country codes
    CASE co.country
        WHEN 'Brazil' THEN 'BR'
        WHEN 'Argentina' THEN 'AR' 
        WHEN 'Mexico' THEN 'MX'
        WHEN 'Colombia' THEN 'CO'
        ELSE NULL
    END AS country_iso2,
    
    -- Geographic hierarchy levels
    CONCAT(co.country, ' > ', s.state, ' > ', c.city_name) AS full_location_path

FROM {{ source('nu_sources', 'city') }} c
LEFT JOIN {{ source('nu_sources', 'state') }} s 
    ON c.state_id = s.state_id
LEFT JOIN {{ source('nu_sources', 'country') }} co 
    ON s.country_id = co.country_id