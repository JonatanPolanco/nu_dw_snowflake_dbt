
"""
Autor: Jonatan Polanco
Date: 2025-07-05

Purpose: This DAG orchestrates the complete data pipeline for "Nu", from the
ingestion of files in Google Cloud Storage (GCS) to data transformation in
Snowflake using dbt.

Pipeline Flow (E-L-T):
1.  Wait for Data (Sensor): A sensor monitors a GCS bucket for the arrival
    of new data files.
2.  Extract & Load (Refresh External Tables): Once data is detected, commands
    are triggered in Snowflake to refresh the External Tables. These tables point
    directly to the files in GCS. This process is parallelized, prioritizing
    high-frequency data.
3.  Critical Data Validation: A post-refresh validation step ensures that the
    external tables have been updated correctly. This is a critical gatekeeper
    to prevent the propagation of stale data or running transformations on
    incomplete datasets. The DAG will fail if high-frequency tables have not
    been recently updated.
4.  Transform (dbt): With the raw data validated and updated in Snowflake,
    a dbt project (managed by Cosmos) is executed to perform all transformations,
    creating analytical models, fact, and dimension tables.
5.  Completion: A final task marks the successful completion of the pipeline.
"""

# =============================================================================
# IMPORTS
# =============================================================================
import logging
from datetime import datetime, timedelta

# Airflow Providers
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.utils.task_group import TaskGroup

# Astronomer Cosmos for dbt integration
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# =============================================================================
# CONSTANTS & CONFIGURATION
# =============================================================================

# --- General Configuration ---
GCP_CONN_ID = "gcp_default"
SNOWFLAKE_CONN_ID = "snowflake_conn"
GCS_BUCKET_NAME = "nu_dataset"
GCS_DATA_PREFIX = "Tables/"

# --- DAG Configuration ---
DAG_OWNER = "data_team"
DAG_EMAIL_ON_FAILURE = True

# --- Snowflake Configuration ---
SNOWFLAKE_DB = "NU_DB"
SNOWFLAKE_RAW_SCHEMA = "NU_RAW_SCHEMA"
SNOWFLAKE_ANALYTICS_SCHEMA = "NU_ANALYTIC_SCHEMA"
SNOWFLAKE_WAREHOUSE = "NU_WH"
SNOWFLAKE_ROLE = "NU_ROLE"

# --- dbt Configuration ---
DBT_PROJECT_PATH = "/usr/local/airflow/dags/dbt_pipeline"
DBT_EXECUTABLE_PATH = "/usr/local/airflow/dbt_venv/bin/dbt"

# --- External Tables Configuration by Update Frequency ---
# This configuration centralizes the refresh and validation logic.
# Adding or moving a table between categories will automatically adjust the DAG's behavior.
EXTERNAL_TABLES_CONFIG = {
    # Real-time/high frequency - Constantly updated. Business-critical.
    "high_frequency": ["pix_movements", "transfer_ins", "transfer_outs"],
    # Medium frequency - Daily/weekly changes. Important but less urgent.
    "medium_frequency": ["accounts", "customers"],
    # Low frequency - Static or rarely changing data.
    "low_frequency": [
        "city", "country", "state", "d_time", "d_month", "d_week", "d_weekday", "d_year"
    ],
}

# --- Airflow Default Arguments ---
default_args = {
    "owner": DAG_OWNER,
    "depends_on_past": False,
    "email_on_failure": DAG_EMAIL_ON_FAILURE,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# --- Cosmos Profile Configuration for Snowflake ---
# Defines how Cosmos will connect to Snowflake to run dbt jobs.
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id=SNOWFLAKE_CONN_ID,
        profile_args={
            "database": SNOWFLAKE_DB,
            "schema": SNOWFLAKE_ANALYTICS_SCHEMA,
            "warehouse": SNOWFLAKE_WAREHOUSE,
            "role": SNOWFLAKE_ROLE,
        },
    ),
)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def validate_external_table_refresh(**context):
    """
    Validates the state of external tables in Snowflake after a refresh attempt.

    This validation is a critical data quality gatekeeper for several reasons:
    1.  **Detects Silent Failures:** The `ALTER EXTERNAL TABLE REFRESH` command
        can "succeed" without error even if the underlying data in GCS is
        inaccessible (e.g., due to permissions changes), leading to stale data.
    2.  **Prevents Stale Data Propagation:** Ensures that downstream dbt models
        are built on fresh data, preventing incorrect analytics and reporting.
    3.  **Differentiated Logic:** Applies validation rules based on data criticality:
        -   **High/Medium Frequency:** Verifies that the `last_altered` timestamp in
            the information schema is recent. A failure to refresh a high-frequency
            table is a critical error and fails the DAG.
        -   **Low Frequency:** Verifies only that the tables contain data (are not
            empty), as they are not expected to change with every run.

    Args:
        context (dict): The Airflow task context, automatically injected.

    Returns:
        str: A summary message indicating the successful result of the validation.

    Raises:
        ValueError: If any of the `high_frequency` tables have not been refreshed
                    within the defined time threshold, halting the pipeline to
                    prevent processing of stale, critical data.
    """
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    logger = logging.getLogger(__name__)
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    validation_summary = {}
    critical_errors = []

    # Validate high and medium frequency tables by `last_altered` timestamp
    for category, threshold in [("high_frequency", "4 HOURS"), ("medium_frequency", "1 DAY")]:
        tables = EXTERNAL_TABLES_CONFIG.get(category)
        if not tables:
            continue

        logger.info(f"Validating {len(tables)} '{category}' tables with a {threshold} threshold.")

        # Use parameter binding for security and correctness
        placeholders = ", ".join(["%s"] * len(tables))
        sql = f"""
            SELECT table_name
            FROM {SNOWFLAKE_DB}.information_schema.tables
            WHERE table_schema = %s
              AND table_type = 'EXTERNAL TABLE'
              AND table_name IN ({placeholders})
              AND last_altered > CURRENT_TIMESTAMP - INTERVAL '{threshold}';
        """
        params = [SNOWFLAKE_RAW_SCHEMA.upper()] + [t.upper() for t in tables]

        refreshed_tables_rows = hook.get_records(sql, parameters=params)
        refreshed_names = {row[0].lower() for row in refreshed_tables_rows}
        expected_names = set(tables)
        missing_refresh = expected_names - refreshed_names

        if missing_refresh:
            error_msg = f"'{category}' tables not recently refreshed (> {threshold}): {sorted(list(missing_refresh))}"
            if category == "high_frequency":
                logger.error(f"CRITICAL: {error_msg}")
                critical_errors.append(error_msg)
            else:
                logger.warning(f"WARNING: {error_msg}")
        else:
            logger.info(f"All '{category}' tables were recently refreshed.")

        validation_summary[category] = {
            "refreshed_count": len(refreshed_names),
            "missing_count": len(missing_refresh),
            "missing_tables": sorted(list(missing_refresh)),
        }

    # Validate low frequency tables for data existence (non-empty)
    low_freq_tables = EXTERNAL_TABLES_CONFIG.get("low_frequency")
    if low_freq_tables:
        logger.info(f"Validating {len(low_freq_tables)} 'low_frequency' tables for data existence.")
        union_queries = [
            f"SELECT '{table}' AS table_name, COUNT(*) AS record_count FROM {SNOWFLAKE_DB}.{SNOWFLAKE_RAW_SCHEMA}.{table}"
            for table in low_freq_tables
        ]
        sql = " UNION ALL ".join(union_queries)

        data_results = hook.get_records(sql)
        empty_tables = {row[0] for row in data_results if row[1] == 0}
        tables_with_data = set(low_freq_tables) - empty_tables

        if empty_tables:
            logger.warning(f"'low_frequency' tables found empty (this may be expected): {sorted(list(empty_tables))}")
        else:
            logger.info("All 'low_frequency' tables contain data.")

        validation_summary["low_frequency"] = {
            "with_data_count": len(tables_with_data),
            "empty_count": len(empty_tables),
            "empty_tables": sorted(list(empty_tables)),
        }

    logger.info(f"--- Validation Summary --- \n{validation_summary}")
    context["ti"].xcom_push(key="validation_summary", value=validation_summary)

    if critical_errors:
        raise ValueError(f"Critical validation failed: {'; '.join(critical_errors)}")

    return "External table validation completed successfully."


# =============================================================================
# DAG DEFINITION
# =============================================================================

@dag(
    dag_id="nu_data_pipeline",
    default_args=default_args,
    description="Data pipeline from GCS to Snowflake with dbt transformations.",
    schedule=None,  # This DAG is event-driven or manually triggered
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["nu", "gcs", "snowflake", "dbt", "production"],
    max_active_runs=1,  # Prevent concurrent runs to maintain data consistency
)
def nu_data_pipeline():
    """
    Defines and orchestrates the tasks for the Nu data pipeline.
    """

    # Task 1: Wait for new file(s) in the GCS bucket
    wait_for_new_data = GCSObjectsWithPrefixExistenceSensor(
        task_id="wait_for_new_data_in_gcs",
        bucket=GCS_BUCKET_NAME,
        prefix=GCS_DATA_PREFIX,
        google_cloud_conn_id=GCP_CONN_ID,
        timeout=300,        # Max time to wait (in seconds)
        poke_interval=60,   # How often to check (in seconds)
        mode="poke",
    )

    # Task 2: TaskGroup to refresh and validate all external tables
    with TaskGroup(group_id="refresh_and_validate_external_tables") as refresh_and_validate_group:
        
        # Create a dictionary to hold lists of task objects for each category
        refresh_tasks_by_cat = {cat: [] for cat in EXTERNAL_TABLES_CONFIG}

        # Create one refresh task PER TABLE for security, granularity, and observability
        for category, tables in EXTERNAL_TABLES_CONFIG.items():
            for table in tables:
                # Sanity check on table names during DAG parsing
                if not table.replace("_", "").isalnum():
                    raise ValueError(f"Invalid table name '{table}'. Only alphanumeric and underscores are allowed.")

                refresh_task = SQLExecuteQueryOperator(
                    task_id=f"refresh_{table}",
                    conn_id=SNOWFLAKE_CONN_ID,
                    sql=f"ALTER EXTERNAL TABLE {SNOWFLAKE_DB}.{SNOWFLAKE_RAW_SCHEMA}.{table} REFRESH;",
                    autocommit=True,
                )
                refresh_tasks_by_cat[category].append(refresh_task)

        # This validation task runs after all refresh tasks are complete
        validate_refresh_status = PythonOperator(
            task_id="validate_refresh_status",
            python_callable=validate_external_table_refresh,
        )

        # --- Define explicit dependencies within the group ---
        # 1. High-frequency tasks run first.
        # 2. Once all high-frequency tasks succeed, medium and low-frequency tasks run in parallel.
        # 3. After all refresh tasks are done, run the final validation.
        if refresh_tasks_by_cat["high_frequency"]:
            high_freq_tasks = refresh_tasks_by_cat["high_frequency"]
            medium_low_freq_tasks = refresh_tasks_by_cat["medium_frequency"] + refresh_tasks_by_cat["low_frequency"]
            
            if medium_low_freq_tasks:
                high_freq_tasks >> medium_low_freq_tasks >> validate_refresh_status
            else:
                high_freq_tasks >> validate_refresh_status


    # Task 3: TaskGroup to run the dbt project using Cosmos
    dbt_transformation = DbtTaskGroup(
        group_id="dbt_transformation",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE_PATH),
        operator_args={
            "install_deps": True,   # Ensures dbt dependencies are installed
            "full_refresh": False,  # Prefer incremental runs in production by default
        },
    )

    # Task 4: Final endpoint to signify a successful pipeline run
    pipeline_success = EmptyOperator(
        task_id="pipeline_success",
        trigger_rule="all_success",
    )

    # =============================================================================
    # PIPELINE ORCHESTRATION
    # =============================================================================
    wait_for_new_data >> refresh_and_validate_group >> dbt_transformation >> pipeline_success

# Instantiate the DAG
nu_data_pipeline()