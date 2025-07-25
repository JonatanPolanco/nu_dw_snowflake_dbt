{{ config(materialized='table') }}

WITH
-- Import CTEs
transactions AS (
    SELECT * FROM {{ ref('int_unified_transactions') }}
),

time_dimension AS (
    SELECT * FROM {{ ref('base_time_dimension') }}
),

-- Logical CTE - enrich with time dimension
enriched AS (
    SELECT 
        -- 1. IDENTIFIERS
        t.transaction_id,
        t.account_id,
        
        -- 2. DIMENSIONS
        t.transaction_channel,
        t.transaction_direction,
        t.source_table,
        
        -- Time dimension attributes
        td.year_number AS completed_year,
        td.month_number AS completed_month_number,
        td.week_number AS completed_week,
        td.weekday_name AS completed_weekday,
        td.quarter_number AS completed_quarter,
        
        -- 3. MEASURES
        t.transaction_amount,
        CASE 
            WHEN t.transaction_direction = 'in' THEN t.transaction_amount
            WHEN t.transaction_direction = 'out' THEN -t.transaction_amount
            ELSE 0
        END AS signed_amount,
        
        -- 4. DATES/TIMESTAMPS
        t.transaction_completed_at,
        td.utc_date AS completed_date,
        td.month_start_date AS completed_month_date,
        
        -- 5. BOOLEANS  
        CASE WHEN t.transaction_amount >= 1000 THEN TRUE ELSE FALSE END AS is_large_transaction,
        
        -- 6. DERIVED ATTRIBUTES
        'BRL' AS currency_code,
        
        -- Transaction size categorization
        CASE 
            WHEN t.transaction_amount < 100 THEN 'micro'
            WHEN t.transaction_amount < 1000 THEN 'small'
            WHEN t.transaction_amount < 10000 THEN 'medium' 
            WHEN t.transaction_amount < 100000 THEN 'large'
            ELSE 'enterprise'
        END AS transaction_size_category

    FROM transactions t
    LEFT JOIN time_dimension td 
        ON DATE(t.transaction_completed_at) = td.utc_date  
    
    WHERE t.transaction_completed_at IS NOT NULL  -- Solo transacciones con fecha
)

-- Simple select statement
SELECT * FROM enriched