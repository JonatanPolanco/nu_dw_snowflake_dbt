{{ config(
    materialized='table',
    cluster_by=['report_month'],
    tags=['marts', 'reporting', 'kpi']
) }}

WITH monthly_balances AS (
    SELECT * FROM {{ ref('fct_account_monthly_balances') }}
),

transactions AS (
    SELECT * FROM {{ ref('fct_transactions') }}
),

accounts AS (
    SELECT * FROM {{ ref('dim_account') }}
),

-- Monthly aggregations
monthly_kpis AS (
    SELECT
        mb.month_date AS report_month,
        mb.year,
        mb.month,
        
        -- Account metrics
        COUNT(DISTINCT mb.account_id) AS total_active_accounts,
        COUNT(DISTINCT CASE WHEN mb.has_activity THEN mb.account_id END) AS active_accounts,
        COUNT(DISTINCT CASE WHEN mb.is_high_activity THEN mb.account_id END) AS high_activity_accounts,
        
        -- Volume metrics
        SUM(mb.inbound_volume) AS total_inbound_volume,
        SUM(mb.outbound_volume) AS total_outbound_volume,
        SUM(mb.net_flow) AS total_net_flow,
        
        -- Transaction metrics
        SUM(mb.total_transactions) AS total_transactions,
        SUM(mb.pix_transactions) AS total_pix_transactions,
        SUM(mb.transfer_transactions) AS total_transfer_transactions,
        
        -- Average metrics
        AVG(mb.avg_inbound_transaction_amount) AS avg_inbound_amount_monthly,
        AVG(mb.avg_outbound_transaction_amount) AS avg_outbound_amount_monthly,
        
        -- Channel metrics
        SUM(mb.pix_net_flow) AS total_pix_flow,
        SUM(mb.transfer_net_flow) AS total_transfer_flow

    FROM monthly_balances mb
    WHERE mb.month_date IS NOT NULL
    GROUP BY mb.month_date, mb.year, mb.month
),

-- Add derived KPIs
final AS (
    SELECT
        report_month,
        year,
        month,
        
        -- Account KPIs
        total_active_accounts,
        active_accounts,
        high_activity_accounts,
        
        ROUND(
            CASE 
                WHEN total_active_accounts > 0 
                THEN (active_accounts::FLOAT / total_active_accounts) * 100 
                ELSE 0 
            END, 2
        ) AS account_activation_rate_pct,
        
        -- Volume KPIs
        total_inbound_volume,
        total_outbound_volume,
        total_net_flow,
        
        ROUND(
            CASE 
                WHEN total_outbound_volume > 0 
                THEN total_inbound_volume / total_outbound_volume 
                ELSE NULL 
            END, 2
        ) AS inflow_outflow_ratio,
        
        -- Transaction KPIs
        total_transactions,
        total_pix_transactions,
        total_transfer_transactions,
        
        ROUND(
            CASE 
                WHEN total_transactions > 0 
                THEN (total_pix_transactions::FLOAT / total_transactions) * 100 
                ELSE 0 
            END, 2
        ) AS pix_adoption_rate_pct,
        
        -- Average transaction values
        ROUND(avg_inbound_amount_monthly, 2) AS avg_inbound_amount,
        ROUND(avg_outbound_amount_monthly, 2) AS avg_outbound_amount,
        
        -- Channel performance
        total_pix_flow,
        total_transfer_flow,
        
        ROUND(
            CASE 
                WHEN (total_pix_flow + total_transfer_flow) != 0 
                THEN (total_pix_flow::FLOAT / (total_pix_flow + total_transfer_flow)) * 100 
                ELSE 0 
            END, 2
        ) AS pix_volume_share_pct,
        
        -- Growth indicators (month-over-month)
        LAG(total_inbound_volume) OVER (ORDER BY report_month) AS prev_month_inbound_volume,
        LAG(total_transactions) OVER (ORDER BY report_month) AS prev_month_transactions,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS _processed_at

    FROM monthly_kpis
)

SELECT * FROM final