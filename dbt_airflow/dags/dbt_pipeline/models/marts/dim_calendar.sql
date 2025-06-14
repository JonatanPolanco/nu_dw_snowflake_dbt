{{ config(materialized='table') }}

SELECT 
    time_id,
    utc_timestamp,
    utc_date,
    
    -- Time hierarchy
    year_number AS year,
    month_number AS month,
    week_number AS week,
    weekday_name AS weekday,
    quarter_number AS quarter,
    
    -- Derived attributes
    month_start_date,
    week_start_date,
    
    -- Business attributes  
    CASE weekday_name
        WHEN 'Saturday' THEN FALSE
        WHEN 'Sunday' THEN FALSE
        ELSE TRUE
    END AS is_weekday,
    
    CASE 
        WHEN month_number IN (12, 1, 2) THEN 'Summer'
        WHEN month_number IN (3, 4, 5) THEN 'Autumn'
        WHEN month_number IN (6, 7, 8) THEN 'Winter'
        ELSE 'Spring'
    END AS season_southern_hemisphere,
    
    CURRENT_TIMESTAMP() AS _loaded_at

FROM {{ ref('base_time_dimension') }}