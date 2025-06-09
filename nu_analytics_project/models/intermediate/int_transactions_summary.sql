{{ config(materialized='table') }}

SELECT
    spine.account_id,
    spine.month_date,
    spine.year,
    spine.month,
    
    -- Volume metrics
    COALESCE(SUM(CASE WHEN t.flow_direction = 'inbound' THEN t.transaction_amount END), 0) AS inbound_volume,
    COALESCE(SUM(CASE WHEN t.flow_direction = 'outbound' THEN t.transaction_amount END), 0) AS outbound_volume,
    COALESCE(SUM(t.signed_amount), 0) AS net_flow,
    
    -- Transaction counts
    COUNT(t.transaction_id) AS total_transactions,
    COUNT(CASE WHEN t.flow_direction = 'inbound' THEN 1 END) AS inbound_transactions,
    COUNT(CASE WHEN t.flow_direction = 'outbound' THEN 1 END) AS outbound_transactions,
    
    -- Channel breakdown
    COALESCE(SUM(CASE WHEN t.transaction_channel = 'PIX' THEN t.signed_amount END), 0) AS pix_net_flow,
    COALESCE(SUM(CASE WHEN t.transaction_channel = 'TRANSFER' THEN t.signed_amount END), 0) AS transfer_net_flow,
    
    COUNT(CASE WHEN t.transaction_channel = 'PIX' THEN 1 END) AS pix_transactions,
    COUNT(CASE WHEN t.transaction_channel = 'TRANSFER' THEN 1 END) AS transfer_transactions,
    
    -- Size category analysis
    COUNT(CASE WHEN t.transaction_size_category = 'micro' THEN 1 END) AS micro_transactions,
    COUNT(CASE WHEN t.transaction_size_category = 'small' THEN 1 END) AS small_transactions,
    COUNT(CASE WHEN t.transaction_size_category = 'medium' THEN 1 END) AS medium_transactions,
    COUNT(CASE WHEN t.transaction_size_category = 'large' THEN 1 END) AS large_transactions,
    
    -- Average transaction values
    AVG(CASE WHEN t.flow_direction = 'inbound' THEN t.transaction_amount END) AS avg_inbound_amount,
    AVG(CASE WHEN t.flow_direction = 'outbound' THEN t.transaction_amount END) AS avg_outbound_amount

FROM {{ ref('int_account_monthly_spine') }} spine
LEFT JOIN {{ ref('int_transactions_enriched') }} t 
    ON spine.account_id = t.account_id 
    AND spine.month_date = t.completed_month

GROUP BY 
    spine.account_id, 
    spine.month_date, 
    spine.year, 
    spine.month