{{ config(
    materialized='table',
    cluster_by=['utc_date'],
    tags=['base', 'dimension', 'time']
) }}

WITH
-- Import CTEs 
d_time AS (
    SELECT * FROM {{ source('nu_sources', 'd_time') }}
),

d_week AS (
    SELECT * FROM {{ source('nu_sources', 'd_week') }}
),

d_month AS (
    SELECT * FROM {{ source('nu_sources', 'd_month') }}
),

d_year AS (
    SELECT * FROM {{ source('nu_sources', 'd_year') }}
),

d_weekday AS (
    SELECT * FROM {{ source('nu_sources', 'd_weekday') }}
),

-- Logical CTEs 
time_hierarchy AS (
    SELECT 
        dt.time_id,
        dt.week_id,
        dt.month_id,
        dt.year_id,
        dt.weekday_id,
        
        -- 2. DIMENSIONS
        -- From joined dimensions
        dw.action_week AS week_number,
        dm.action_month AS month_number,
        dy.action_year AS year_number,
        dwd.action_weekday AS weekday_name,
        
        -- 3. MEASURES
        -- (No measures in time dimension)
        
        -- 4. DATES/TIMESTAMPS
        dt.action_timestamp AS utc_timestamp,
        DATE(dt.action_timestamp) AS utc_date,
        
        -- Derived date attributes
        DATE_TRUNC('MONTH', dt.action_timestamp) AS month_start_date,
        DATE_TRUNC('WEEK', dt.action_timestamp) AS week_start_date,
        DATE_TRUNC('YEAR', dt.action_timestamp) AS year_start_date,
        DATE_TRUNC('QUARTER', dt.action_timestamp) AS quarter_start_date,
        
        -- Additional useful date parts
        EXTRACT(QUARTER FROM dt.action_timestamp) AS quarter_number,
        EXTRACT(DAY FROM dt.action_timestamp) AS day_of_month,
        EXTRACT(DAYOFYEAR FROM dt.action_timestamp) AS day_of_year,
        EXTRACT(WEEK FROM dt.action_timestamp) AS week_of_year,
        
        -- 5. BOOLEANS
        CASE 
            WHEN dwd.action_weekday IN ('Saturday', 'Sunday') THEN FALSE 
            ELSE TRUE 
        END AS is_weekday,
        
        CASE 
            WHEN dm.action_month IN (12, 1, 2) THEN TRUE 
            ELSE FALSE 
        END AS is_summer_brazil,  -- Southern hemisphere
        
        CASE 
            WHEN dm.action_month IN (6, 7, 8) THEN TRUE 
            ELSE FALSE 
        END AS is_winter_brazil,
        
        CASE 
            WHEN EXTRACT(DAY FROM dt.action_timestamp) = 1 THEN TRUE 
            ELSE FALSE 
        END AS is_month_start,
        
        CASE 
            WHEN EXTRACT(DAY FROM LAST_DAY(dt.action_timestamp)) = EXTRACT(DAY FROM dt.action_timestamp) THEN TRUE 
            ELSE FALSE 
        END AS is_month_end

    FROM d_time dt
    LEFT JOIN d_week dw ON dt.week_id = dw.week_id
    LEFT JOIN d_month dm ON dt.month_id = dm.month_id  
    LEFT JOIN d_year dy ON dt.year_id = dy.year_id
    LEFT JOIN d_weekday dwd ON dt.weekday_id = dwd.weekday_id
    
    -- Data quality filters
    WHERE dt.action_timestamp IS NOT NULL
      AND dt.time_id IS NOT NULL
),

-- Additional business logic
enhanced_time AS (
    SELECT 
        *,
        
        -- Business calendar attributes
        CASE 
            WHEN weekday_name = 'Monday' THEN 'week_start'
            WHEN weekday_name = 'Friday' THEN 'week_end'
            WHEN is_weekday = FALSE THEN 'weekend'
            ELSE 'weekday'
        END AS business_day_type,
        
        -- Relative time periods (useful for analysis)
        CASE 
            WHEN quarter_number = 1 THEN 'Q1'
            WHEN quarter_number = 2 THEN 'Q2'
            WHEN quarter_number = 3 THEN 'Q3'
            WHEN quarter_number = 4 THEN 'Q4'
        END AS quarter_name,
        
        -- Month name for readability
        CASE month_number
            WHEN 1 THEN 'January'
            WHEN 2 THEN 'February'
            WHEN 3 THEN 'March'
            WHEN 4 THEN 'April'
            WHEN 5 THEN 'May'
            WHEN 6 THEN 'June'
            WHEN 7 THEN 'July'
            WHEN 8 THEN 'August'
            WHEN 9 THEN 'September'
            WHEN 10 THEN 'October'
            WHEN 11 THEN 'November'
            WHEN 12 THEN 'December'
        END AS month_name,
        
        -- Financial year (if different from calendar year)
        -- Assuming Brazilian fiscal year (Jan-Dec), but can be adjusted
        year_number AS fiscal_year,
        
        -- Create display labels
        CONCAT(year_number, '-', LPAD(month_number, 2, '0')) AS year_month,
        CONCAT(year_number, '-Q', quarter_number) AS year_quarter,
        CONCAT(year_number, '-W', LPAD(week_number, 2, '0')) AS year_week

    FROM time_hierarchy
),

-- Final cleanup and organization
final AS (
    SELECT 
        -- 1. IDENTIFIERS
        time_id,
        week_id,
        month_id,
        year_id,
        weekday_id,
        
        -- 2. DIMENSIONS
        weekday_name,
        month_name,
        quarter_name,
        business_day_type,
        
        -- Numeric dimensions
        week_number,
        month_number,
        year_number,
        quarter_number,
        day_of_month,
        day_of_year,
        week_of_year,
        fiscal_year,
        
        -- Display labels
        year_month,
        year_quarter,
        year_week,
        
        -- 3. MEASURES
        -- (No measures in time dimension)
        
        -- 4. DATES/TIMESTAMPS
        utc_timestamp,
        utc_date,
        month_start_date,
        week_start_date,
        year_start_date,
        quarter_start_date,
        
        -- 5. BOOLEANS
        is_weekday,
        is_summer_brazil,
        is_winter_brazil,
        is_month_start,
        is_month_end,
        
        -- 6. METADATA
        CURRENT_TIMESTAMP() AS _loaded_at

    FROM enhanced_time
)

-- Simple select statement
SELECT * FROM final