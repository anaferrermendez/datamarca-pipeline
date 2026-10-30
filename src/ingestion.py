"""
Fase de ingesta batch del pipeline DATAMARCA.

Pasos:
  1. Lectura de transacciones (CSV) y logs (JSON) con esquemas explícitos.
  2. Validación: se aplican las reglas de calidad y se marcan los logs huérfanos.
  3. Escritura de la cuarentena en Parquet (siempre, para auditoría).
  4. Control de umbral: si la tasa de rechazo supera el máximo, se detiene aquí.
  5. Escritura de los registros válidos en Parquet particionado.

Cada paso se cronometra y las métricas se guardan en reports/ingesta_metricas.json.

Uso:
    python -m src.ingestion
"""

import json
import logging
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config import PipelineConfig, create_spark_session, load_config
from src.quality_rules import (
    LOG_RULES,
    ORPHAN_LOG_RULE,
    TRANSACTION_RULES,
    DataQualityError,
    apply_rules,
    enforce_threshold,
    flag_orphan_logs,
    log_coverage,
    quality_report,
    split_valid_quarantine,
)
from src.schemas import (
    LOGS_READ_OPTIONS,
    LOGS_SCHEMA,
    TRANSACTIONS_READ_OPTIONS,
    TRANSACTIONS_SCHEMA,
)

logger = logging.getLogger("datamarca.ingesta")


@dataclass
class IngestionResult:
    run_id: str
    status: str = "en_curso"
    timings: dict = field(default_factory=dict)
    reports: dict = field(default_factory=dict)
    outputs: dict = field(default_factory=dict)
    error: str | None = None


@contextmanager
def timed(result: IngestionResult, stage: str):
    """Mide el tiempo de una etapa y lo registra en el log y en el resultado."""
    logger.info("Inicio etapa: %s", stage)
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = round(time.perf_counter() - start, 3)
        result.timings[stage] = elapsed
        logger.info("Fin etapa: %s (%.3f s)", stage, elapsed)


# ---------------------------------------------------------------------------
# Lectura
# ---------------------------------------------------------------------------
def read_transactions(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    return (
        spark.read.options(**TRANSACTIONS_READ_OPTIONS)
        .schema(TRANSACTIONS_SCHEMA)
        .csv(config.transactions_path)
    )


def read_logs(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    return (
        spark.read.options(**LOGS_READ_OPTIONS)
        .schema(LOGS_SCHEMA)
        .json(config.logs_path)
    )


# ---------------------------------------------------------------------------
# Escritura
# ---------------------------------------------------------------------------
def with_ingestion_metadata(df: DataFrame, run_id: str) -> DataFrame:
    """Columnas de trazabilidad: qué corrida cargó cada registro y cuándo."""
    return df.withColumn("ingestion_run_id", F.lit(run_id)).withColumn(
        "ingested_at", F.current_timestamp()
    )


def add_time_partitions(df: DataFrame) -> DataFrame:
    """Columnas year/month derivadas del timestamp, usadas para particionar."""
    return df.withColumn("year", F.year("timestamp")).withColumn(
        "month", F.month("timestamp")
    )


def write_parquet(df: DataFrame, path: str, partition_cols: tuple = ()) -> None:
    """Escritura idempotente: cada corrida reemplaza la salida anterior.

    Antes de escribir se agrupan los datos por las columnas de partición
    (repartition), así cada carpeta year=/month= recibe un solo archivo en lugar
    de uno por tarea de Spark. Evita el problema de "muchos archivos pequeños",
    que hace lentas las lecturas posteriores, sobre todo en almacenamiento cloud.
    """
    if partition_cols:
        writer = df.repartition(*partition_cols).write.partitionBy(*partition_cols)
    else:
        writer = df.coalesce(1).write  # cuarentena: volumen pequeño por diseño (umbral)
    writer.mode("overwrite").parquet(path)


# ---------------------------------------------------------------------------
# Orquestación
# ---------------------------------------------------------------------------
def run_ingestion(spark: SparkSession, config: PipelineConfig) -> IngestionResult:
    result = IngestionResult(run_id=datetime.now().strftime("%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:6])
    logger.info("Corrida %s | entrada: %s | salida: %s",
                result.run_id, config.input_dir, config.output_dir)
    total_start = time.perf_counter()

    txn_flagged = logs_flagged = None
    try:
        # 1-2. Lectura y validación. Spark es perezoso: el trabajo real ocurre en las
        # acciones (los conteos del reporte). Se cachea para no releer los archivos
        # en las escrituras posteriores.
        with timed(result, "lectura_y_validacion"):
            transactions = read_transactions(spark, config)
            txn_flagged = apply_rules(transactions, TRANSACTION_RULES, config).cache()
            logs_flagged = flag_orphan_logs(
                apply_rules(read_logs(spark, config), LOG_RULES, config), transactions
            ).cache()

            txn_report = quality_report(txn_flagged, TRANSACTION_RULES)
            logs_report = quality_report(logs_flagged, LOG_RULES + [ORPHAN_LOG_RULE])
            result.reports["transacciones"] = txn_report
            result.reports["logs"] = logs_report

        logger.info("Transacciones: %s leídas, %s rechazadas (%.2f %%)",
                    txn_report["total"], txn_report["rechazados"], 100 * txn_report["tasa_rechazo"])
        logger.info("Logs: %s leídos, %s rechazados (%.2f %%)",
                    logs_report["total"], logs_report["rechazados"], 100 * logs_report["tasa_rechazo"])

        txn_valid, txn_quarantine = split_valid_quarantine(txn_flagged)
        logs_valid, logs_quarantine = split_valid_quarantine(logs_flagged)

        # 3. La cuarentena se escribe siempre, incluso si después se supera el umbral,
        # para poder auditar qué falló.
        with timed(result, "escritura_cuarentena"):
            for name, df in (("transacciones", txn_quarantine), ("logs", logs_quarantine)):
                path = config.quarantine_path(name)
                write_parquet(with_ingestion_metadata(df, result.run_id), path)
                result.outputs[f"cuarentena_{name}"] = path

        # 4. Freno: no se publican datos válidos si la calidad del lote es mala.
        enforce_threshold(txn_report, config, "transacciones")
        enforce_threshold(logs_report, config, "logs")

        # 5. Datos válidos en Parquet particionado.
        with timed(result, "escritura_validos"):
            txn_path = config.validated_path("transacciones")
            write_parquet(
                add_time_partitions(with_ingestion_metadata(txn_valid, result.run_id)),
                txn_path, config.transactions_partition_cols,
            )
            logs_path = config.validated_path("logs")
            write_parquet(
                with_ingestion_metadata(logs_valid, result.run_id),
                logs_path, config.events_partition_cols,
            )
            result.outputs["validos_transacciones"] = txn_path
            result.outputs["validos_logs"] = logs_path

        with timed(result, "metricas_cobertura"):
            result.reports["cobertura_logs"] = log_coverage(txn_valid, logs_valid)

        result.status = "exitosa"

    except DataQualityError as exc:
        result.status = "detenida_por_calidad"
        result.error = str(exc)
        logger.error("Ingesta detenida: %s", exc)
    finally:
        result.timings["total"] = round(time.perf_counter() - total_start, 3)
        for df in (txn_flagged, logs_flagged):
            if df is not None:
                df.unpersist()

    logger.info("Corrida %s terminó con estado '%s' en %.3f s",
                result.run_id, result.status, result.timings["total"])
    return result


def save_metrics(result: IngestionResult, config: PipelineConfig) -> Path:
    reports_dir = Path(config.reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    path = reports_dir / "ingesta_metricas.json"
    payload = {
        "run_id": result.run_id,
        "estado": result.status,
        "error": result.error,
        "spark_master": config.spark_master,
        "tiempos_segundos": result.timings,
        "salidas": result.outputs,
        "calidad": result.reports,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return path


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    config = load_config()
    spark = create_spark_session(config)
    try:
        result = run_ingestion(spark, config)
        metrics_path = save_metrics(result, config)
        logger.info("Métricas guardadas en %s", metrics_path)
        print("\nTiempos por etapa (s):")
        for stage, seconds in result.timings.items():
            print(f"  {stage:<24}{seconds:>8.3f}")
        return 0 if result.status == "exitosa" else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
