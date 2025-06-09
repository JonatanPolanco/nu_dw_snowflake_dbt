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
        t.channel,
        t.direction,
        t.source_table,
        
        -- Time dimension attributes
        td.year_number AS completed_year,
        td.month_number AS completed_month_number,
        td.week_number AS completed_week,
        td.weekday_name AS completed_weekday,
        td.quarter_number AS completed_quarter,
        
        -- 3. MEASURES
        t.amount,
        CASE 
            WHEN t.direction = 'inbound' THEN t.amount
            WHEN t.direction = 'outbound' THEN -t.amount
            ELSE 0
        END AS signed_amount,
        
        -- 4. DATES/TIMESTAMPS
        t.completed_at,
        td.utc_date AS completed_date,
        td.month_start_date AS completed_month_date,
        
        -- 5. BOOLEANS  
        CASE WHEN t.amount >= 1000 THEN TRUE ELSE FALSE END AS is_large_transaction,
        
        -- 6. DERIVED ATTRIBUTES
        'BRL' AS currency_code,
        
        -- Transaction size categorization
        CASE 
            WHEN t.amount < 100 THEN 'micro'
            WHEN t.amount < 1000 THEN 'small'
            WHEN t.amount < 10000 THEN 'medium' 
            WHEN t.amount < 100000 THEN 'large'
            ELSE 'enterprise'
        END AS transaction_size_category

    FROM transactions t
    LEFT JOIN time_dimension td 
        ON DATE(t.completed_at) = td.utc_date  
    
    WHERE t.completed_at IS NOT NULL  -- Solo transacciones con fecha
)

-- Simple select statement
SELECT * FROM enriched