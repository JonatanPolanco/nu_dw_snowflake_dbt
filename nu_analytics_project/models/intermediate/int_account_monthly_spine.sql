{{ config(materialized='table') }}

WITH
-- Import CTEs - bringing in data
accounts AS (
    SELECT DISTINCT account_id 
    FROM {{ ref('stg_accounts') }}
    WHERE is_active = TRUE
),

time_dimension AS (
    SELECT 
        utc_date,
        month_start_date,
        year_number,
        month_number
    FROM {{ ref('base_time_dimension') }}
),

-- Logical CTEs - business logic
account_date_range AS (
    -- Get the first and last transaction date for each account
    SELECT 
        account_id,
        MIN(completed_date) AS first_transaction_date,
        MAX(completed_date) AS last_transaction_date
    FROM (
        SELECT 
            account_id,
            DATE(completed_at) AS completed_date
        FROM {{ ref('stg_pix_movements') }}
        WHERE is_completed = TRUE
        
        UNION ALL
        
        SELECT 
            account_id,
            DATE(completed_at) AS completed_date
        FROM {{ ref('stg_transfer_ins') }}
        WHERE is_completed = TRUE
        
        UNION ALL
        
        SELECT 
            account_id,
            DATE(completed_at) AS completed_date
        FROM {{ ref('stg_transfer_outs') }}
        WHERE is_completed = TRUE
    ) all_transactions
    GROUP BY account_id
),

monthly_calendar AS (
    -- Get distinct months from time dimension
    SELECT DISTINCT 
        month_start_date,
        year_number,
        month_number
    FROM time_dimension
    WHERE month_start_date IS NOT NULL
),

-- Create spine: every account for every month in their active period
account_month_spine AS (
    SELECT 
        a.account_id,
        c.month_start_date AS month_date,
        c.year_number AS year,
        c.month_number AS month
    FROM accounts a
    CROSS JOIN monthly_calendar c
    LEFT JOIN account_date_range adr ON a.account_id = adr.account_id
    WHERE c.month_start_date BETWEEN 
        COALESCE(DATE_TRUNC('MONTH', adr.first_transaction_date), '2020-01-01') 
        AND 
        COALESCE(DATE_TRUNC('MONTH', adr.last_transaction_date), CURRENT_DATE())
),

-- Final organization
final AS (
    SELECT 
        -- 1. IDENTIFIERS
        account_id,
        
        -- 2. DIMENSIONS
        year,
        month,
        
        -- 3. DATES
        month_date,
        
        -- 4. METADATA
        CURRENT_TIMESTAMP() AS _loaded_at

    FROM account_month_spine
)

-- Simple select statement
SELECT * FROM final