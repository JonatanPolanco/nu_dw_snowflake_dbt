{{ config(
    materialized='table',
    cluster_by=['customer_segment'],
    tags=['marts', 'reporting', 'segmentation']
) }}

WITH customers AS (
    SELECT * FROM {{ ref('dim_customer') }}
),


segmented_customers AS (
    SELECT
        customer_id,
        cpf,
        first_name,
        last_name,
        full_name,
        customer_city,
        country_name,
        city_normalized,
        country_normalized,
        name_length_category,
        has_complete_name,
        has_city_info,
        has_valid_cpf,
        
        -- Geographic segmentation
        CASE 
            WHEN country_normalized = 'BRAZIL' OR country_normalized = 'BRASIL' THEN 'Domestic'
            WHEN country_normalized IS NOT NULL THEN 'International'
            ELSE 'Unknown Location'
        END AS geographic_segment,
        
        -- Data completeness segmentation
        CASE 
            WHEN has_complete_name AND has_city_info AND has_valid_cpf THEN 'Complete Profile'
            WHEN has_complete_name AND has_valid_cpf THEN 'Partial Profile - Missing Location'
            WHEN has_valid_cpf THEN 'Minimal Profile - CPF Only'
            ELSE 'Incomplete Profile'
        END AS profile_completeness_segment,
        
        -- Main customer segment (combining multiple factors)
        CASE 
            WHEN has_complete_name AND has_city_info AND has_valid_cpf AND (country_normalized = 'BRAZIL' OR country_normalized = 'BRASIL') THEN 'Premium Domestic'
            WHEN has_complete_name AND has_city_info AND has_valid_cpf THEN 'Premium International'
            WHEN has_complete_name AND has_valid_cpf THEN 'Standard Customer'
            WHEN has_valid_cpf THEN 'Basic Customer'
            ELSE 'Incomplete Customer'
        END AS customer_segment,
        
        -- City tier (using existing customer_city field)
        CASE 
            WHEN UPPER(customer_city) IN ('SÃO PAULO', 'SAO PAULO', 'RIO DE JANEIRO', 'BRASÍLIA', 'BRASILIA', 'BELO HORIZONTE', 'SALVADOR', 'FORTALEZA', 'RECIFE', 'PORTO ALEGRE') THEN 'Tier 1 City'
            WHEN customer_city IS NOT NULL THEN 'Other City'
            ELSE 'Unknown City'
        END AS city_tier

    FROM customers
),

-- Final aggregation by segment using only existing fields
final AS (
    SELECT
        customer_segment,
        geographic_segment,
        profile_completeness_segment,
        name_length_category,
        city_tier,
        
        -- Customer counts
        COUNT(*) AS customers_count,
        COUNT(CASE WHEN has_complete_name THEN 1 END) AS customers_with_complete_name,
        COUNT(CASE WHEN has_city_info THEN 1 END) AS customers_with_city,
        COUNT(CASE WHEN has_valid_cpf THEN 1 END) AS customers_with_valid_cpf,
        
        -- Geographic distribution
        COUNT(CASE WHEN geographic_segment = 'Domestic' THEN 1 END) AS domestic_customers,
        COUNT(CASE WHEN geographic_segment = 'International' THEN 1 END) AS international_customers,
        
        -- Profile completeness
        COUNT(CASE WHEN profile_completeness_segment = 'Complete Profile' THEN 1 END) AS complete_profile_customers,
        COUNT(CASE WHEN profile_completeness_segment LIKE '%Partial%' THEN 1 END) AS partial_profile_customers,
        
        -- Name analysis
        COUNT(CASE WHEN name_length_category = 'Long Name' THEN 1 END) AS long_name_customers,
        COUNT(CASE WHEN name_length_category = 'Medium Name' THEN 1 END) AS medium_name_customers,
        COUNT(CASE WHEN name_length_category = 'Short Name' THEN 1 END) AS short_name_customers,
        
        -- Percentages
        ROUND(
            (COUNT(*)::FLOAT / SUM(COUNT(*)) OVER ()) * 100, 2
        ) AS segment_percentage,
        
        ROUND(
            (COUNT(CASE WHEN has_valid_cpf THEN 1 END)::FLOAT / COUNT(*)) * 100, 2
        ) AS cpf_completion_rate,
        
        ROUND(
            (COUNT(CASE WHEN has_complete_name THEN 1 END)::FLOAT / COUNT(*)) * 100, 2
        ) AS name_completion_rate,
        
        ROUND(
            (COUNT(CASE WHEN has_city_info THEN 1 END)::FLOAT / COUNT(*)) * 100, 2
        ) AS city_completion_rate,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS _processed_at

    FROM segmented_customers
    GROUP BY 
        customer_segment, 
        geographic_segment, 
        profile_completeness_segment, 
        name_length_category,
        city_tier
)

SELECT * FROM final
ORDER BY customers_count DESC