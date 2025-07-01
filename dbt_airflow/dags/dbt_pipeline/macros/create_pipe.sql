CREATE OR REPLACE STORAGE INTEGRATION nu_dataset_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'GCS'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('gcs://nu_dataset/Tables');


CREATE OR REPLACE NOTIFICATION INTEGRATION nu_notify_integration
    TYPE = QUEUE
    NOTIFICATION_PROVIDER = GCP_PUBSUB
    ENABLED = TRUE
    GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/start-up-ocai/subscriptions/nu_analytics_topic-sub';

    DESC NOTIFICATION INTEGRATION nu_notify_integration;



CREATE OR REPLACE STAGE nu_dataset_stage
    STORAGE_INTEGRATION = nu_dataset_integration
    URL = 'gcs://nu_dataset/Tables'
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);

--------------------------------------------------------------------------------------------
-- pix movements table    
use database nu_db;
use schema nu_raw_schema;

USE ROLE ACCOUNTADMIN;

GRANT USAGE ON INTEGRATION NU_NOTIFY_INTEGRATION TO ROLE NU_ROLE;
USE ROLE NU_ROLE;

CREATE OR REPLACE TABLE pix_movements (
    id BIGINT,
    account_id BIGINT,
    pix_amount NUMBER(18,2),
    pix_requested_at TIMESTAMP,
    pix_completed_at TIMESTAMP,
    status STRING,
    in_or_out STRING
);


--pix movements pipe
CREATE OR REPLACE PIPE nu_db.nu_raw_schema.pix_movements_pipe
  AUTO_INGEST = TRUE
  INTEGRATION = nu_notify_integration
  AS
  COPY INTO pix_movements
  FROM (
    SELECT
      $1::BIGINT,
      $2::BIGINT,
      TO_DECIMAL($3::STRING, 18, 2),
      TRY_TO_TIMESTAMP(NULLIF($4::STRING, 'None')),
      TRY_TO_TIMESTAMP(NULLIF($5::STRING, 'None')),
      $6::STRING,
      $7::STRING
    FROM @nu_dataset_stage/pix_movements/
  )
  FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);


--------------------------------------------------------------------------------------------
-- accounts table
CREATE OR REPLACE TABLE accounts (
    account_id BIGINT,
    account_name STRING,
    created_at TIMESTAMP,
    status STRING
);

CREATE OR REPLACE PIPE nu_db.nu_raw_schema.accounts_pipe 
AUTO_INGEST = TRUE 
INTEGRATION = nu_notify_integration
AS
COPY INTO accounts
FROM (
    SELECT
      $1::BIGINT,
      $2::STRING,
      $3::TIMESTAMP,
      $4::STRING
    FROM @nu_dataset_stage/accounts/
)
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);

-----------------------------------------------------------------
-- city
CREATE OR REPLACE TABLE city (
    city_name STRING,
    state_id BIGINT,
    city_id BIGINT
);

CREATE OR REPLACE PIPE nu_db.nu_raw_schema.city_pipe 
AUTO_INGEST = TRUE 
INTEGRATION = nu_notify_integration
AS
COPY INTO city
FROM (
    SELECT
      $1::STRING,
      $2::BIGINT,
      $3::BIGINT
    FROM @nu_dataset_stage/city/
)
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);


--------------------------------------------------------------------
-- country
CREATE OR REPLACE TABLE country (
    country STRING,
    country_id BIGINT
);

CREATE OR REPLACE PIPE country_pipe 
AUTO_INGEST = TRUE 
INTEGRATION = nu_notify_integration
AS
COPY INTO country
FROM (
    SELECT
      $1::STRING,
      $2::BIGINT
    FROM @nu_dataset_stage/country/
)
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);


-------------------------------------------------------
-- customers
CREATE OR REPLACE TABLE customers (
    customer_id BIGINT,
    first_name STRING,
    last_name STRING,
    customer_city BIGINT,
    cpf BIGINT,
    country_name STRING
);

CREATE OR REPLACE PIPE customers_pipe 
AUTO_INGEST = TRUE 
INTEGRATION = nu_notify_integration
AS
COPY INTO customers
FROM (
    SELECT
      $1::BIGINT,
      $2::STRING,
      $3::STRING,
      $4::BIGINT,
      $5::BIGINT,
      $6::STRING
    FROM @nu_dataset_stage/customers/
)
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);


---------------------------------------------------------------
-- d_month
CREATE OR REPLACE TABLE d_month (
    month_id INT,
    action_month BIGINT
);

CREATE OR REPLACE PIPE d_month_pipe 
AUTO_INGEST = TRUE 
INTEGRATION = nu_notify_integration
AS
COPY INTO d_month
FROM (
    SELECT
      $1::INT,
      $2::BIGINT
    FROM @nu_dataset_stage/d_month/
)
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);


--------------------------------------------------------------
-- d_time
CREATE OR REPLACE TABLE d_time (
    time_id BIGINT,
    action_timestamp STRING,
    week_id INT,
    month_id INT,
    year_id INT,
    weekday_id INT
);

CREATE OR REPLACE PIPE d_time_pipe 
AUTO_INGEST = TRUE 
INTEGRATION = nu_notify_integration
AS
COPY INTO d_time
FROM (
    SELECT
      $1::BIGINT,
      $2::STRING,
      $3::INT,
      $4::INT,
      $5::INT,
      $6::INT
    FROM @nu_dataset_stage/d_time/
)
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);


-----------------------------------------------------------------
-- d_week
CREATE OR REPLACE TABLE d_week (
    week_id INT,
    action_week INT
);

CREATE OR REPLACE PIPE d_week_pipe 
AUTO_INGEST = TRUE 
INTEGRATION = nu_notify_integration
AS
COPY INTO d_week
FROM (
    SELECT
      $1::INT,
      $2::INT
    FROM @nu_dataset_stage/d_week/
)
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);


-------------------------------------------------------------
-- d_weekday
CREATE OR REPLACE TABLE d_weekday (
    weekday_id INT,
    action_weekday STRING
);

CREATE OR REPLACE PIPE d_weekday_pipe 
AUTO_INGEST = TRUE 
INTEGRATION = nu_notify_integration
AS
COPY INTO d_weekday
FROM (
    SELECT
      $1::INT,
      $2::STRING
    FROM @nu_dataset_stage/d_weekday/
)
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);


------------------------------------------------------
-- d_year
CREATE OR REPLACE TABLE d_year (
    year_id INT,
    action_year INT
);

CREATE OR REPLACE PIPE d_year_pipe 
AUTO_INGEST = TRUE 
INTEGRATION = nu_notify_integration
AS
COPY INTO d_year
FROM (
    SELECT
      $1::INT,
      $2::INT
    FROM @nu_dataset_stage/d_year/
)
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);


------------------------------------------------
-- state
CREATE OR REPLACE TABLE state (
    state STRING,
    country_id INT,
    state_id INT
);

CREATE OR REPLACE PIPE state_pipe 
AUTO_INGEST = TRUE 
INTEGRATION = nu_notify_integration
AS
COPY INTO state
FROM (
    SELECT
      $1::STRING,
      $2::INT,
      $3::INT
    FROM @nu_dataset_stage/state/
)
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);


-------------------------------------------------------------------
-- transfer_ins
CREATE OR REPLACE TABLE transfer_ins (
    id BIGINT,
    account_id BIGINT,
    amount NUMBER(18,2),
    transaction_requested_at TIMESTAMP,
    transaction_completed_at TIMESTAMP,
    status STRING
);

CREATE OR REPLACE PIPE transfer_ins_pipe 
AUTO_INGEST = TRUE 
INTEGRATION = nu_notify_integration
AS
COPY INTO transfer_ins
FROM (
    SELECT
      $1::BIGINT,
      $2::BIGINT,
      TO_DECIMAL($3::STRING, 18, 2),
      TRY_TO_TIMESTAMP(NULLIF($4::STRING, 'None')),
      TRY_TO_TIMESTAMP(NULLIF($5::STRING, 'None')),
      $6::STRING
    FROM @nu_dataset_stage/transfer_ins/
)
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);


-----------------------------------------------------------------
-- transfer_outs
CREATE OR REPLACE TABLE transfer_outs (
    id BIGINT,
    account_id BIGINT,
    amount NUMBER(18,2),
    transaction_requested_at TIMESTAMP,
    transaction_completed_at TIMESTAMP,
    status STRING
);

CREATE OR REPLACE PIPE transfer_outs_pipe 
AUTO_INGEST = TRUE 
INTEGRATION = nu_notify_integration
AS
COPY INTO transfer_outs
FROM (
    SELECT
      $1::BIGINT,
      $2::BIGINT,
      TO_DECIMAL($3::STRING, 18, 2),
      TRY_TO_TIMESTAMP(NULLIF($4::STRING, 'None')),
      TRY_TO_TIMESTAMP(NULLIF($5::STRING, 'None')),
      $6::STRING
    FROM @nu_dataset_stage/transfer_outs/
)
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);
