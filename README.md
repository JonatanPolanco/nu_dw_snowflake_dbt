# Datawarehouse full pipeline Architecture using:
### GCP: GCS
### Snowflake: Snowpipe, DW management
### DBT: Medallion Architecture for analytical data modeling, Tests.
### Airflow: Dags, Sensors, alerts and notifications.

Data model documentation: https://github.com/JonatanPolanco/nu_dw_snowflake_dbt/blob/main/nu_dw_proposal_documentation.pdf

Screenshot using external tables strategy. Takes too long, moving to real time with snowpipe and incremental tables.
<a href="https://drive.google.com/uc?export=view&id=1L2EwFw9Rm5Bq9eXvcgQvggQySnu8jzVI"><img src="https://drive.google.com/uc?export=view&id=1L2EwFw9Rm5Bq9eXvcgQvggQySnu8jzVI" style="width: 650px; max-width: 100%; height: auto" title="Click to enlarge picture" />
