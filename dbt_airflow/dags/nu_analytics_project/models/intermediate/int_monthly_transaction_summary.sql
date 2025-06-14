{{ config(materialized='table') }}

WITH
-- Import CTEs
spine AS (
    SELECT * FROM {{ ref('int_account_monthly_spine') }}
),

transactions AS (
    SELECT * FROM {{ ref('int_transactions_enriched') }}
),

-- Logical CTEs - aggregate transactions by account and month
monthly_aggregates AS (
    SELECT
        DATE_TRUNC('MONTH', transaction_completed_at) AS month_date,
        account_id,
        
        -- Volume metrics
        SUM(CASE WHEN transaction_direction = 'TRANSFER_IN' THEN transaction_amount ELSE 0 END) AS inbound_volume,
        SUM(CASE WHEN transaction_direction = 'TRANSFER_OUT' THEN transaction_amount ELSE 0 END) AS outbound_volume,
        SUM(signed_amount) AS net_flow,
        
        -- Transaction counts
        COUNT(*) AS total_transactions,
        COUNT(CASE WHEN transaction_direction = 'TRANSFER_IN' THEN 1 END) AS inbound_transactions,
        COUNT(CASE WHEN transaction_direction = 'TRANSFER_OUT' THEN 1 END) AS outbound_transactions,
        
        -- Channel breakdown
        SUM(CASE WHEN transaction_channel = 'PIX' THEN signed_amount ELSE 0 END) AS pix_net_flow,
        SUM(CASE WHEN transaction_channel = 'TRANSFER' THEN signed_amount ELSE 0 END) AS transfer_net_flow,
        
        COUNT(CASE WHEN transaction_channel = 'PIX' THEN 1 END) AS pix_transactions,
        COUNT(CASE WHEN transaction_channel = 'TRANSFER' THEN 1 END) AS transfer_transactions,
        
        -- Average transaction values
        AVG(CASE WHEN transaction_direction = 'TRANSFER_IN' THEN transaction_amount END) AS avg_inbound_transaction_amount,
        AVG(CASE WHEN transaction_direction = 'TRANSFER_OUT' THEN transaction_amount END) AS avg_outbound_transaction_amount

    FROM transactions
    WHERE transaction_completed_at IS NOT NULL
    GROUP BY 1, 2
),

-- Join spine with aggregates to ensure complete monthly coverage
complete_monthly_summary AS (
    SELECT
        s.account_id,
        s.month_date,
        s.year,
        s.month,
        
        -- Coalesce aggregates to 0 for months with no transactions
        COALESCE(a.inbound_volume, 0) AS inbound_volume,
        COALESCE(a.outbound_volume, 0) AS outbound_volume,
        COALESCE(a.net_flow, 0) AS net_flow,
        COALESCE(a.total_transactions, 0) AS total_transactions,
        COALESCE(a.inbound_transactions, 0) AS inbound_transactions,
        COALESCE(a.outbound_transactions, 0) AS outbound_transactions,
        COALESCE(a.pix_net_flow, 0) AS pix_net_flow,
        COALESCE(a.transfer_net_flow, 0) AS transfer_net_flow,
        COALESCE(a.pix_transactions, 0) AS pix_transactions,
        COALESCE(a.transfer_transactions, 0) AS transfer_transactions,
        a.avg_inbound_transaction_amount,
        a.avg_outbound_transaction_amount

    FROM spine s
    LEFT JOIN monthly_aggregates a 
        ON s.account_id = a.account_id 
        AND s.month_date = a.month_date
),

-- Final organization
final AS (
    SELECT 
        -- 1. IDENTIFIERS
        account_id,
        
        -- 2. DIMENSIONS
        year,
        month,
        
        -- 3. MEASURES
        inbound_volume,
        outbound_volume,
        net_flow,
        total_transactions,
        inbound_transactions,
        outbound_transactions,
        pix_net_flow,
        transfer_net_flow,
        pix_transactions,
        transfer_transactions,
        avg_inbound_transaction_amount,
        avg_outbound_transaction_amount,
        
        -- 4. DATES
        month_date,
        
        -- 5. BOOLEANS
        CASE WHEN total_transactions > 0 THEN TRUE ELSE FALSE END AS has_activity,
        
        -- 6. METADATA
        CURRENT_TIMESTAMP() AS _loaded_at

    FROM complete_monthly_summary
)

-- Simple select statement
SELECT * FROM final