{% macro create_external_tables() %}

  {% set sql %}
    USE DATABASE NU_DB;
    USE SCHEMA NU_RAW_SCHEMA;
    
    -- Crear stage si no existe
    CREATE STAGE IF NOT EXISTS nu_dataset_stage
        STORAGE_INTEGRATION = nu_dataset_integration
        URL = 'gcs://nu_dataset/Tables'
        FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);

    -- pix_movements
    CREATE OR REPLACE EXTERNAL TABLE pix_movements (
        id BIGINT AS (VALUE:c1::BIGINT),
        account_id BIGINT AS (VALUE:c2::BIGINT),
        pix_amount NUMBER(18,2) AS (VALUE:c3::NUMBER),
        pix_requested_at TIMESTAMP AS (TRY_TO_TIMESTAMP(NULLIF(VALUE:c4::STRING, 'None'))),
        pix_completed_at TIMESTAMP AS (TRY_TO_TIMESTAMP(NULLIF(VALUE:c5::STRING, 'None'))),
        status STRING AS (VALUE:c6::STRING),
        in_or_out STRING AS (VALUE:c7::STRING)
    )
    LOCATION = @nu_dataset_stage/pix_movements/
    PATTERN = '.*\.csv'
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
    AUTO_REFRESH = FALSE;

    -- accounts
    CREATE OR REPLACE EXTERNAL TABLE accounts (
        account_id BIGINT AS (VALUE:c1::BIGINT),
        account_name STRING AS (VALUE:c2::STRING),
        created_at TIMESTAMP AS (VALUE:c3::TIMESTAMP),
        status STRING AS (VALUE:c4::STRING)
    )
    LOCATION = @nu_dataset_stage/accounts/
    PATTERN = '.*\.csv'
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
    AUTO_REFRESH = FALSE;

    -- city
    CREATE OR REPLACE EXTERNAL TABLE city (
        city_name STRING AS (VALUE:c1::STRING),
        state_id BIGINT AS (VALUE:c2::BIGINT),
        city_id BIGINT AS (VALUE:c3::BIGINT)
    )
    LOCATION = @nu_dataset_stage/city/
    PATTERN = '.*\.csv'
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
    AUTO_REFRESH = FALSE;

    -- country
    CREATE OR REPLACE EXTERNAL TABLE country (
        country STRING AS (VALUE:c1::STRING),
        country_id BIGINT AS (VALUE:c2::BIGINT)
    )
    LOCATION = @nu_dataset_stage/country/
    PATTERN = '.*\.csv'
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
    AUTO_REFRESH = FALSE;

    -- customers
    CREATE OR REPLACE EXTERNAL TABLE customers (
        customer_id BIGINT AS (VALUE:c1::BIGINT),
        first_name STRING AS (VALUE:c2::STRING),
        last_name STRING AS (VALUE:c3::STRING),
        customer_city BIGINT AS (VALUE:c4::BIGINT),
        cpf BIGINT AS (VALUE:c5::BIGINT),
        country_name STRING AS (VALUE:c6::STRING)
    )
    LOCATION = @nu_dataset_stage/customers/
    PATTERN = '.*\.csv'
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
    AUTO_REFRESH = FALSE;

    -- d_month
    CREATE OR REPLACE EXTERNAL TABLE d_month (
        month_id INT AS (VALUE:c1::INT),
        action_month BIGINT AS (VALUE:c2::BIGINT)
    )
    LOCATION = @nu_dataset_stage/d_month/
    PATTERN = '.*\.csv'
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
    AUTO_REFRESH = FALSE;

    -- d_time (CORREGIDO)
    CREATE OR REPLACE EXTERNAL TABLE d_time (
        time_id BIGINT AS (VALUE:c1::BIGINT),
        action_timestamp STRING AS (VALUE:c2::STRING),
        week_id INT AS (VALUE:c3::INT),
        month_id INT AS (VALUE:c4::INT),
        year_id INT AS (VALUE:c5::INT),
        weekday_id INT AS (VALUE:c6::INT)
    )
    LOCATION = @nu_dataset_stage/d_time/
    PATTERN = '.*\.csv'
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
    AUTO_REFRESH = FALSE;

    -- d_week
    CREATE OR REPLACE EXTERNAL TABLE d_week (
        week_id INT AS (VALUE:c1::INT),
        action_week INT AS (VALUE:c2::INT)
    )
    LOCATION = @nu_dataset_stage/d_week/
    PATTERN = '.*\.csv'
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
    AUTO_REFRESH = FALSE;

    -- d_weekday
    CREATE OR REPLACE EXTERNAL TABLE d_weekday (
        weekday_id INT AS (VALUE:c1::INT),
        action_weekday STRING AS (VALUE:c2::STRING)
    )
    LOCATION = @nu_dataset_stage/d_weekday/
    PATTERN = '.*\.csv'
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
    AUTO_REFRESH = FALSE;

    -- d_year
    CREATE OR REPLACE EXTERNAL TABLE d_year (
        year_id INT AS (VALUE:c1::INT),
        action_year INT AS (VALUE:c2::INT)
    )
    LOCATION = @nu_dataset_stage/d_year/
    PATTERN = '.*\.csv'
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
    AUTO_REFRESH = FALSE;

    -- state
    CREATE OR REPLACE EXTERNAL TABLE state (
        state STRING AS (VALUE:c1::STRING),
        country_id INT AS (VALUE:c2::INT),
        state_id INT AS (VALUE:c3::INT)
    )
    LOCATION = @nu_dataset_stage/state/
    PATTERN = '.*\.csv'
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
    AUTO_REFRESH = FALSE;

    -- transfer_ins (CORREGIDO)
    CREATE OR REPLACE EXTERNAL TABLE transfer_ins (
        id BIGINT AS (VALUE:c1::BIGINT),
        account_id BIGINT AS (VALUE:c2::BIGINT),
        amount FLOAT AS (VALUE:c3::FLOAT),
        transaction_requested_at STRING AS (VALUE:c4::STRING),
        transaction_completed_at STRING AS (VALUE:c5::STRING),
        status STRING AS (VALUE:c6::STRING)
    )
    LOCATION = @nu_dataset_stage/transfer_ins/
    PATTERN = '.*\.csv'
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
    AUTO_REFRESH = FALSE;

    -- transfer_outs (CORREGIDO)
    CREATE OR REPLACE EXTERNAL TABLE transfer_outs (
        id BIGINT AS (VALUE:c1::BIGINT),
        account_id BIGINT AS (VALUE:c2::BIGINT),
        amount FLOAT AS (VALUE:c3::FLOAT),
        transaction_requested_at STRING AS (VALUE:c4::STRING),
        transaction_completed_at STRING AS (VALUE:c5::STRING),
        status STRING AS (VALUE:c6::STRING)
    )
    LOCATION = @nu_dataset_stage/transfer_outs/
    PATTERN = '.*\.csv'
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
    AUTO_REFRESH = FALSE;
  {% endset %}

  {% do run_query(sql) %}

{% endmacro %}