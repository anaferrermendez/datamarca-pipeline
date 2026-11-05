from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
from airflow.utils.state import DagRunState

# 1. CALLBACKS: Logging y auditoría
def on_failure_callback(context):
    """Se ejecuta cuando una tarea falla."""
    ti = context["ti"]
    print("=" * 60)
    print(f"TAREA FALLIDA: {ti.task_id}")
    print(f"   Run ID: {context['run_id']}")
    print(f"   Intento: {ti.try_number} de {ti.max_tries}")
    print("=" * 60)

def on_retry_callback(context):
    """Se ejecuta en cada reintento (demuestra el backoff exponencial)."""
    ti = context["task_instance"]
    print(f"REINTENTO {ti.try_number}/{ti.max_tries} para: {ti.task_id}")

def notificar_resultado(**context):
    """Notificación final del pipeline."""
    print(f"Pipeline DATAMARCA finalizado. Run ID: {context['run_id']}")

def verificar_estado_final(**context):
    """Falla si el DagRun no terminó en éxito, para que el estado global sea 'failed'."""
    ti = context["ti"]
    dag_run_state = RuntimeTaskInstance.get_dagrun_state(ti.dag_id, ti.run_id)
    
    if dag_run_state == DagRunState.FAILED:
        raise AirflowException(
            "El DagRun se marcó como 'failed' debido a fallos en tareas anteriores."
        )
    print(" El DagRun terminó exitosamente.")

# 2. ARGUMENTOS POR DEFECTO (Retries y Backoff)
default_args = {
    "owner": "datamarca-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 3,                            # 3 intentos
    "retry_delay": timedelta(minutes=1),     # Espera 1 min
    "retry_exponential_backoff": True,       # 1min -> 2min -> 4min
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": on_failure_callback,
    "on_retry_callback": on_retry_callback,
}

# 3. DEFINICIÓN DEL DAG
with DAG(
    dag_id="datamarca_pipeline_orquestado",
    default_args=default_args,
    description="Pipeline batch de DATAMARCA (Ingesta -> Notificación)",
    schedule="@daily",
    start_date=datetime(2026, 11, 3),
    catchup=False,
    tags=["datamarca", "spark", "u4"],
) as dag:

    inicio = EmptyOperator(task_id="inicio")

    # Etapa 1: Ingesta y validación inicial
    ingesta = BashOperator(
        task_id="ingesta",
        bash_command="cd /opt/airflow/datamarca && python -m src.ingestion",
    )

    # Etapa 2: Validación (reporte de calidad sin escribir datos)
    validacion = BashOperator(
        task_id="validacion",
        bash_command="cd /opt/airflow/datamarca && python -m src.data_quality_report",
    )

    # Etapa 3: Transformaciones (Fallo simulado de API externa)
    transformacion = BashOperator(
        task_id="transformacion",
        bash_command="cd /opt/airflow/datamarca && python -m src.transformations",
    )

    # Etapa 4: Persistencia (verifica que las tablas se escribieron)
    persistencia = BashOperator(
        task_id="persistencia",
        bash_command="cd /opt/airflow/datamarca && test -d data/output/curated/ventas_tienda_mes && echo 'Tablas curadas OK'",
    )

    # Etapa 5: Notificación
    notificacion = PythonOperator(
        task_id="notificacion",
        python_callable=notificar_resultado,
    )

    # Alerta: Solo se dispara si algo falló antes
    alerta_fallo = BashOperator(
        task_id="alerta_fallo",
        bash_command='echo "ALERTA: El pipeline falló. Revisar logs."',
        trigger_rule=TriggerRule.ONE_FAILED,
        retries=0,
    )

    # Cierre: Se ejecuta SIEMPRE, pero falla si hubo errores aguas arriba
    fin = PythonOperator(
        task_id="fin",
        python_callable=verificar_estado_final,
        trigger_rule=TriggerRule.ALL_DONE,
        retries=0,
    )

    # 4. GRAFO DE DEPENDENCIAS
    # Flujo principal
    inicio >> ingesta >> validacion >> transformacion >> persistencia >> notificacion
    
    # La alerta se dispara si algo falla en el camino
    [ingesta, validacion, transformacion, persistencia] >> alerta_fallo
    
    # El fin depende SOLO de la última tarea exitosa, no de la alerta
    notificacion >> fin