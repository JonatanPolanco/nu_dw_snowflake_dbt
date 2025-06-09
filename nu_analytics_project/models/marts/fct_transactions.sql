{{ config(
    materialized='table',
    cluster_by=['account_id', 'completed_date'],
    post_hook="ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(account_id, transaction_type);"
) }}

SELECT 
    -- Primary keys and identifiers
    transaction_id,
    account_id,
    
    -- Timestamps (all in UTC)
    requested_timestamp AS requested_utc_time,
    completed_timestamp AS completion_utc_time,
    completed_date AS completion_utc_date,
    
    -- Foreign keys to dimensions
    completed_time_id AS time_completion_id,
    
    -- Transaction details
    transaction_type,
    transaction_channel,
    transaction_amount AS amount,
    signed_amount,
    currency_code AS currency,
    transaction_status AS status,
    flow_direction AS transaction_direction,
    
    -- Business classifications
    transaction_size_category,
    
    -- Operational metrics
    days_to_complete,
    
    -- Time dimensions (denormalized for performance)
    completed_year,
    completed_month_number,
    completed_week,
    completed_weekday,
    completed_quarter,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS _loaded_at

FROM {{ ref('int_transactions_enriched') }}