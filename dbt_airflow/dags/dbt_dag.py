import os
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# Configuración de tablas externas por frecuencia de actualización
EXTERNAL_TABLES_CONFIG = {
    # Tiempo real/alta frecuencia - Se actualizan constantemente
    "high_frequency": [
        "pix_movements", 
        "transfer_ins", 
        "transfer_outs"
    ],
    
    # Frecuencia media - Cambios diarios/semanales
    "medium_frequency": [
        "accounts", 
        "customers"
    ],
    
    # Baja frecuencia - Cambios ocasionales o datos estáticos
    "low_frequency": [
        "city", 
        "country", 
        "state",
        "d_time", 
        "d_month", 
        "d_week", 
        "d_weekday", 
        "d_year"
    ]
}

# Configuración
default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Profile config para Snowflake
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn", 
        profile_args={
            "database": "NU_DB", 
            "schema": "NU_ANALYTIC_SCHEMA",
            "warehouse": "NU_WH",
            "role": "NU_ROLE"
        },
    )
)

def validate_external_table_refresh(**context):
    """
    Validar que las external tables se refrescaron correctamente.
    
    Esta validación es CRÍTICA porque:
    1. External tables pueden fallar silenciosamente (sin error visible)
    2. Los datos downstream (dbt) dependen de estos datos siendo actuales
    3. Un refresh fallido puede propagar datos obsoletos a toda la pipeline
    4. Detecta problemas de conectividad con el storage (GCS)
    
    Validación diferenciada por frecuencia:
    - High/Medium frequency: Verificar refresh reciente
    - Low frequency: Solo verificar existencia de datos
    """
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    
    logger = logging.getLogger(__name__)
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    
    validation_results = {}
    all_validation_errors = []
    
    # Validar cada categoría según su frecuencia
    for category, tables in EXTERNAL_TABLES_CONFIG.items():
        logger.info(f"🔍 Validando categoría: {category} ({len(tables)} tablas)")
        
        if category in ["high_frequency", "medium_frequency"]:
            # Para tablas que deben refrescarse: validar timestamp
            threshold = "4 HOURS" if category == "high_frequency" else "1 DAY"
            
            metadata_sql = f"""
            SELECT table_name, last_altered
            FROM information_schema.tables 
            WHERE table_schema = 'NU_RAW_SCHEMA'
            AND table_type = 'EXTERNAL TABLE'
            AND table_name IN ({', '.join([f"'{t.upper()}'" for t in tables])})
            AND last_altered > CURRENT_TIMESTAMP - INTERVAL '{threshold}'
            ORDER BY last_altered DESC
            """
            
            refreshed_tables = hook.get_records(metadata_sql)
            refreshed_names = [row[0].lower() for row in refreshed_tables]
            missing_refresh = set(tables) - set(refreshed_names)
            
            if missing_refresh:
                error_msg = f"Tablas {category} sin refresh reciente (>{threshold}): {missing_refresh}"
                logger.error(f"❌ {error_msg}")
                if category == "high_frequency":
                    # High frequency es crítico
                    all_validation_errors.append(f"CRÍTICO: {error_msg}")
                else:
                    # Medium frequency es warning
                    logger.warning(f"⚠️  WARNING: {error_msg}")
            else:
                logger.info(f"✅ Todas las tablas {category} refrescadas recientemente")
            
            validation_results[category] = {
                "refreshed": len(refreshed_names),
                "missing": len(missing_refresh),
                "missing_tables": list(missing_refresh)
            }
            
        else:  # low_frequency
            # Para tablas estáticas: solo verificar que tengan datos
            data_validation_queries = []
            for table in tables:
                data_validation_queries.append(
                    f"SELECT '{table}' AS table_name, COUNT(*) AS record_count "
                    f"FROM NU_DB.NU_RAW_SCHEMA.{table}"
                )
            
            data_validation_sql = " UNION ALL ".join(data_validation_queries)
            data_results = hook.get_records(data_validation_sql)
            
            empty_tables = [row[0] for row in data_results if row[1] == 0]
            tables_with_data = [row[0] for row in data_results if row[1] > 0]
            
            if empty_tables:
                logger.warning(f"⚠️  Tablas {category} vacías (puede ser normal): {empty_tables}")
            else:
                logger.info(f"✅ Todas las tablas {category} tienen datos")
            
            validation_results[category] = {
                "with_data": len(tables_with_data),
                "empty": len(empty_tables),
                "empty_tables": empty_tables
            }
    
    # Logging resumen
    logger.info("📊 Resumen de validación:")
    for category, results in validation_results.items():
        if category in ["high_frequency", "medium_frequency"]:
            logger.info(f"  {category}: {results['refreshed']} refrescadas, {results['missing']} faltantes")
        else:
            logger.info(f"  {category}: {results['with_data']} con datos, {results['empty']} vacías")
    
    # Fallar solo si hay errores críticos
    if all_validation_errors:
        raise ValueError(f"Validación crítica falló: {'; '.join(all_validation_errors)}")
    
    # Métricas para XCom
    context['ti'].xcom_push(key='validation_results', value=validation_results)
    context['ti'].xcom_push(key='critical_errors', value=len(all_validation_errors))
    
    return f"✅ Validación exitosa por frecuencias"

# DAG principal
with DAG(
    dag_id="nu_data_pipeline",
    default_args=default_args,
    description="Pipeline Nu mejorado: GCS → Snowflake → dbt con mejor control de errores",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nu", "gcs", "snowflake", "dbt", "production"],
    max_active_runs=1,  # Evitar ejecuciones concurrentes
) as dag:
    
    # Sensor para detectar nuevos datos en GCS (más específico)
    wait_for_new_data = GCSObjectsWithPrefixExistenceSensor(
        task_id="wait_for_new_data",
        bucket="nu_dataset",
        prefix="Tables/{{ ds }}/",  # Más específico por fecha
        google_cloud_conn_id="gcp_default",
        timeout=300,
        poke_interval=60,
        mode="poke"
    )
    
    # TaskGroup para refresh de External Tables con mejor control
    with TaskGroup(group_id="refresh_external_tables") as refresh_group:
        
        refresh_tasks = []
        
        # Crear tasks separados por categoría para mejor paralelización
        for category, tables in EXTERNAL_TABLES_CONFIG.items():
            
            # SQL para refrescar tablas de la categoría con manejo de errores
            # Escapar nombres de tablas para prevenir SQL injection
            safe_table_refreshes = []
            for table in tables:
                # Validar que el nombre de tabla solo contiene caracteres seguros
                if not table.replace('_', '').replace('-', '').isalnum():
                    raise ValueError(f"Nombre de tabla inseguro: {table}")
                safe_table_refreshes.append(f"""
                BEGIN
                    ALTER EXTERNAL TABLE NU_DB.NU_RAW_SCHEMA.{table} REFRESH;
                    LET success_count := success_count + 1;
                EXCEPTION
                    WHEN OTHER THEN
                        LET error_count := error_count + 1;
                        SYSTEM$LOG('ERROR', 'Failed to refresh {table}: ' || SQLERRM);
                END;""")
            
            refresh_sql = f"""
            BEGIN
                -- Variables para logging
                LET error_count INTEGER := 0;
                LET success_count INTEGER := 0;
                
                -- Refresh tablas de categoría: {category}
            """ + "\n".join(safe_table_refreshes) + f"""
                
                -- Log resultados
                SYSTEM$LOG('INFO', 'Category {category}: ' || success_count || ' successful, ' || error_count || ' errors');
                
                -- Fallar si todas las tablas de alta frecuencia fallaron (son las más críticas)
                IF ('{category}' = 'high_frequency' AND success_count = 0) THEN
                    RAISE EXCEPTION 'Critical high frequency tables failed to refresh';
                END IF;
            END;
            """
            
            refresh_task = SQLExecuteQueryOperator(
                task_id=f"refresh_{category}_tables",
                conn_id="snowflake_conn",
                sql=refresh_sql,
                autocommit=True
            )
            
            refresh_tasks.append(refresh_task)
        
        # Validación post-refresh
        validate_refresh = PythonOperator(
            task_id="validate_refresh",
            python_callable=validate_external_table_refresh
        )
        
        # Dependencias: high_frequency primero, luego medium y low en paralelo
        high_freq_task = None
        other_tasks = []
        
        for i, (category, _) in enumerate(EXTERNAL_TABLES_CONFIG.items()):
            if category == "high_frequency":
                high_freq_task = refresh_tasks[i]
            else:
                other_tasks.append(refresh_tasks[i])
        
        # Ejecutar high frequency primero, luego el resto en paralelo
        if high_freq_task:
            high_freq_task >> validate_refresh
        if other_tasks:
            other_tasks >> validate_refresh
    
    # Checkpoint mejorado con métricas por frecuencia
    def log_pipeline_ready(**context):
        validation_results = context['ti'].xcom_pull(key='validation_results', task_ids='refresh_external_tables.validate_refresh')
        logging.getLogger(__name__).info(f"✅ Pipeline ready! Validation results: {validation_results}")
        return "Pipeline ready"
    
    data_ready_checkpoint = PythonOperator(
        task_id="data_ready_checkpoint",
        python_callable=log_pipeline_ready
    )
    
    # dbt Task Group con configuración mejorada
    dbt_transformation = DbtTaskGroup(
        group_id="dbt_transformation",
        project_config=ProjectConfig("/usr/local/airflow/dags/dbt_pipeline"),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt"
        ),
        operator_args={
            "install_deps": True,
            "full_refresh": False,  # Para producción, evitar full refresh por defecto
        }
    )
    
    # Notificación final (opcional)
    pipeline_success = EmptyOperator(
        task_id="pipeline_success",
        trigger_rule="all_success"
    )
    
    # Pipeline dependencies mejoradas
    wait_for_new_data >> refresh_group >> data_ready_checkpoint >> dbt_transformation >> pipeline_success