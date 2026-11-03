from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# 1. Argumentos por defecto (Políticas de reintento y backoff exponencial)
default_args = {
    'owner': 'datamarca-team',
    'depends_on_past': False,
    'email_on_failure': False, 
    'email_on_retry': False,
    'retries': 3, # 3 intentos en caso de fallo
    # Backoff exponencial: 1 min, 2 min, 4 min...
    'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff': True, 
    'max_retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(minutes=30), # Timeout por tarea
}

with DAG(
    'datamarca_pipeline_orquestado',
    default_args=default_args,
    description='Orquestación del pipeline de DATAMARCA (U3)',
    schedule='@daily', # Se ejecuta diariamente
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=['datamarca', 'spark', 'batch'],
) as dag:

    # Tarea 0: Inicio
    inicio = EmptyOperator(task_id='inicio')

    # Tarea 1: Ingesta y Validación
    ingesta = BashOperator(
        task_id='ingesta_y_validacion',
        # La ruta /opt/airflow/datamarca viene del volumen en docker-compose.yaml
        bash_command='cd /opt/airflow/datamarca && python -m src.ingestion',
    )

    # Tarea 2: Transformaciones y Persistencia
    transformacion = BashOperator(
        task_id='transformaciones_y_persistencia',
        bash_command='cd /opt/airflow/datamarca && python -m src.transformations',
    )

    # Tarea 3: Notificación
    notificacion = BashOperator(
        task_id='notificacion',
        bash_command='echo "Pipeline de DATAMARCA completado exitosamente"',
    )

    # Grafo de dependencias
    inicio >> ingesta >> transformacion >> notificacion