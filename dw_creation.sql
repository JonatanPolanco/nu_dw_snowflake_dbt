use role ACCOUNTADMIN;

CREATE WAREHOUSE nu_wh with WAREHOUSE_SIZE = 'x-small';

create database nu_db;
create role nu_role;

grant usage on WAREHOUSE  nu_wh to role nu_role;
show grants on WAREHOUSE  nu_wh;
grant role nu_role to user jpolanco;

grant all on database nu_db to role nu_role;
use role nu_role;
create schema nu_analytic_schema;
create schema nu_raw_schema;

GRANT ALL PRIVILEGES ON SCHEMA nu_db.nu_raw_schema TO ROLE ACCOUNTADMIN;
GRANT ALL PRIVILEGES ON SCHEMA nu_db.nu_raw_schema TO ROLE nu_role;


CREATE OR REPLACE STORAGE INTEGRATION nu_dataset_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'GCS'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('gcs://nu_dataset/Tables');


CREATE OR REPLACE STAGE nu_dataset_stage
    STORAGE_INTEGRATION = nu_dataset_integration
    URL = 'gcs://nu_dataset/Tables'
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);


-- ver las credenciales que necesito configurar en GCP
DESC STORAGE INTEGRATION nu_dataset_integration;



-- Probar que puedes ver el bucket
LIST @nu_dataset_stage;

-- Ver archivos en pix_movements
LIST @nu_dataset_stage/pix_movements/;

-- Ver todas las carpetas
LIST @nu_dataset_stage PATTERN='.*';


SELECT $1, $2, $3, $4, $5 
FROM @nu_dataset_stage/pix_movements/ 
LIMIT 5;


USE DATABASE NU_DB;
USE SCHEMA NU_RAW_SCHEMA;
-- creamos tabla pix_movements
-- Recrear Stage en NU_RAW_SCHEMA
USE SCHEMA NU_DB.NU_RAW_SCHEMA;

CREATE OR REPLACE STAGE nu_dataset_stage
    STORAGE_INTEGRATION = nu_dataset_integration
    URL = 'gcs://nu_dataset/Tables'
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);


-- Luego crear External Table

-- pix_movements
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
PATTERN = '.*\\.csv'
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
PATTERN = '.*\\.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
AUTO_REFRESH = FALSE;


-- city
CREATE OR REPLACE EXTERNAL TABLE city (
    city_name STRING AS (VALUE:c1::STRING),
    state_id BIGINT AS (VALUE:c2::BIGINT),
    city_id BIGINT AS (VALUE:c3::BIGINT)
)
LOCATION = @nu_dataset_stage/city/
PATTERN = '.*\\.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
AUTO_REFRESH = FALSE;


-- country
CREATE OR REPLACE EXTERNAL TABLE country (
    COUNTRY STRING AS (VALUE:c1::STRING),
    country_id BIGINT AS (VALUE:c2::BIGINT)
)
LOCATION = @nu_dataset_stage/country/
PATTERN = '.*\\.csv'
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
PATTERN = '.*\\.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
AUTO_REFRESH = FALSE;


-- d_month
CREATE OR REPLACE EXTERNAL TABLE d_month (
    month_id INT AS (VALUE:c1::INT),
    month_name STRING AS (VALUE:c2::STRING),
    month_number INT AS (VALUE:c3::INT)
)
LOCATION = @nu_dataset_stage/d_month/
PATTERN = '.*\\.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
AUTO_REFRESH = FALSE;


-- d_time
CREATE OR REPLACE EXTERNAL TABLE d_time (
    time_id INT AS (VALUE:c1::INT),
    hour INT AS (VALUE:c2::INT),
    minute INT AS (VALUE:c3::INT),
    second INT AS (VALUE:c4::INT)
)
LOCATION = @nu_dataset_stage/d_time/
PATTERN = '.*\\.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
AUTO_REFRESH = FALSE;


-- d_week
CREATE OR REPLACE EXTERNAL TABLE d_week (
    week_id INT AS (VALUE:c1::INT),
    week_number INT AS (VALUE:c2::INT),
    year INT AS (VALUE:c3::INT)
)
LOCATION = @nu_dataset_stage/d_week/
PATTERN = '.*\\.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
AUTO_REFRESH = FALSE;


-- d_weekday
CREATE OR REPLACE EXTERNAL TABLE d_weekday (
    weekday_id INT AS (VALUE:c1::INT),
    weekday_name STRING AS (VALUE:c2::STRING),
    weekday_number INT AS (VALUE:c3::INT)
)
LOCATION = @nu_dataset_stage/d_weekday/
PATTERN = '.*\\.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
AUTO_REFRESH = FALSE;


-- d_year
CREATE OR REPLACE EXTERNAL TABLE d_year (
    year_id INT AS (VALUE:c1::INT),
    year INT AS (VALUE:c2::INT)
)
LOCATION = @nu_dataset_stage/d_year/
PATTERN = '.*\\.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
AUTO_REFRESH = FALSE;


-- pix_movements
CREATE OR REPLACE EXTERNAL TABLE pix_movements (
    id BIGINT AS (VALUE:c1::BIGINT),
    account_id BIGINT AS (VALUE:c2::BIGINT),
    pix_amount NUMBER(18,2) AS (VALUE:c3::NUMBER),
    pix_requested_at TIMESTAMP AS (VALUE:c4::TIMESTAMP),
    pix_completed_at TIMESTAMP AS (VALUE:c5::TIMESTAMP),
    status STRING AS (VALUE:c6::STRING),
    in_or_out STRING AS (VALUE:c7::STRING)
)
LOCATION = @nu_dataset_stage/pix_movements/
PATTERN = '.*\\.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
AUTO_REFRESH = FALSE;


-- state
CREATE OR REPLACE EXTERNAL TABLE state (
    state_id INT AS (VALUE:c1::INT),
    state_name STRING AS (VALUE:c2::STRING),
    region STRING AS (VALUE:c3::STRING)
)
LOCATION = @nu_dataset_stage/state/
PATTERN = '.*\\.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
AUTO_REFRESH = FALSE;


-- transfer_ins
CREATE OR REPLACE EXTERNAL TABLE transfer_ins (
    transfer_id BIGINT AS (VALUE:c1::BIGINT),
    account_id BIGINT AS (VALUE:c2::BIGINT),
    amount NUMBER(18,2) AS (VALUE:c3::NUMBER),
    transfer_date TIMESTAMP AS (VALUE:c4::TIMESTAMP)
)
LOCATION = @nu_dataset_stage/transfer_ins/
PATTERN = '.*\\.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
AUTO_REFRESH = FALSE;


-- transfer_outs
CREATE OR REPLACE EXTERNAL TABLE transfer_outs (
    transfer_id BIGINT AS (VALUE:c1::BIGINT),
    account_id BIGINT AS (VALUE:c2::BIGINT),
    amount NUMBER(18,2) AS (VALUE:c3::NUMBER),
    transfer_date TIMESTAMP AS (VALUE:c4::TIMESTAMP)
)
LOCATION = @nu_dataset_stage/transfer_outs/
PATTERN = '.*\\.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
AUTO_REFRESH = FALSE;


