"""
Fase de transformaciones del pipeline DATAMARCA.

Parte de los datos validados por la ingesta (data/output/validated/) y produce
tablas de negocio listas para analizar (data/output/curated/):

  ventas_tienda_mes       Ingresos, unidades y ticket promedio por tienda y mes   (DataFrames)
  ventas_categoria_pais   Ingresos por categoría y país, con participación         (SparkSQL)
  ventas_canal            Tienda física vs. e-commerce, con participación          (DataFrames)
  embudo_conversion       Vistas, carritos y compras por dispositivo               (SparkSQL)

Pasos:
  1. Lectura de los Parquet validados (solo las columnas necesarias).
  2. Enriquecimiento: total por línea, ciudad/país (broadcast join), categoría
     (broadcast join) y canal de venta (cruce con los logs).
  3. Agregaciones de negocio y escritura en Parquet particionado.
  4. Conciliación: los ingresos agregados deben sumar lo mismo que el detalle.

Uso:
    python -m src.transformations
"""

import json
import logging
import time
import uuid
from contextlib import redirect_stdout
from dataclasses import dataclass, field
from datetime import datetime
from io import StringIO
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from src.config import PipelineConfig, create_spark_session, load_config
from src.ingestion import timed, write_parquet

logger = logging.getLogger("datamarca.transformaciones")

MONEY = DecimalType(18, 2)
TIME_PARTITIONS = ("year", "month")


@dataclass
class TransformationResult:
    run_id: str
    status: str = "en_curso"
    timings: dict = field(default_factory=dict)
    outputs: dict = field(default_factory=dict)
    row_counts: dict = field(default_factory=dict)
    reconciliation: dict = field(default_factory=dict)
    error: str | None = None


# ---------------------------------------------------------------------------
# Lectura y dimensiones
# ---------------------------------------------------------------------------
def read_validated(spark: SparkSession, config: PipelineConfig) -> tuple[DataFrame, DataFrame]:
    """Lee los Parquet validados seleccionando solo las columnas que se usan.

    El select explícito permite a Catalyst leer únicamente esas columnas del
    Parquet (column pruning): los metadatos de calidad y de ingesta no se cargan.
    """
    transactions = spark.read.parquet(config.validated_path("transacciones")).select(
        "transaction_id", "timestamp", "store_id", "product_id",
        "quantity", "unit_price", "year", "month",
    )
    logs = spark.read.parquet(config.validated_path("logs")).select(
        "event_id", "transaction_id", "event_type", "device",
    )
    return transactions, logs


def build_dimensions(spark: SparkSession, config: PipelineConfig) -> tuple[DataFrame, DataFrame]:
    """Tablas pequeñas de referencia construidas desde la configuración."""
    stores = spark.createDataFrame(
        [(sid, d["city"], d["country"]) for sid, d in config.store_dimension.items()],
        "store_id string, city string, country string",
    )
    categories = spark.createDataFrame(
        list(config.category_names.items()),
        "category_code string, category string",
    )
    return stores, categories


# ---------------------------------------------------------------------------
# Enriquecimiento
# ---------------------------------------------------------------------------
def enrich_transactions(
    transactions: DataFrame, logs: DataFrame, stores: DataFrame, categories: DataFrame
) -> DataFrame:
    """Agrega total por línea, ciudad, país, categoría y canal a cada venta.

    - Tiendas y categorías son tablas diminutas: F.broadcast las copia a cada
      trabajador y evita mover (shuffle) la tabla grande de ventas.
    - Canal: una venta con al menos un evento de e-commerce es "ecommerce";
      sin eventos, "tienda_fisica" (supuesto del caso: el POS no genera logs).
    """
    sales_with_logs = (
        logs.select("transaction_id").distinct().withColumn("_has_log", F.lit(True))
    )
    return (
        transactions
        .withColumn("line_total", (F.col("quantity") * F.col("unit_price")).cast(MONEY))
        .withColumn("category_code", F.substring_index("product_id", "-", 1))
        .join(F.broadcast(stores), on="store_id", how="left")
        .join(F.broadcast(categories), on="category_code", how="left")
        .join(sales_with_logs, on="transaction_id", how="left")
        .withColumn(
            "channel",
            F.when(F.col("_has_log"), F.lit("ecommerce")).otherwise(F.lit("tienda_fisica")),
        )
        .drop("_has_log", "category_code")
    )


# ---------------------------------------------------------------------------
# Agregaciones de negocio
# ---------------------------------------------------------------------------
def sales_by_store_month(sales: DataFrame) -> DataFrame:
    """Ingresos, unidades y ticket promedio por tienda y mes (API de DataFrames)."""
    return (
        sales.groupBy("year", "month", "country", "city", "store_id")
        .agg(
            F.count(F.lit(1)).alias("transacciones"),
            F.sum("quantity").alias("unidades"),
            F.sum("line_total").cast(MONEY).alias("ingresos"),
        )
        .withColumn("ticket_promedio", F.round(F.col("ingresos") / F.col("transacciones"), 2).cast(MONEY))
    )


def sales_by_category_country(spark: SparkSession, sales: DataFrame) -> DataFrame:
    """Ingresos por categoría y país, con su participación dentro del país (SparkSQL)."""
    sales.createOrReplaceTempView("ventas")
    return spark.sql("""
        SELECT
            year,
            month,
            country,
            category,
            SUM(quantity)                         AS unidades,
            CAST(SUM(line_total) AS DECIMAL(18,2)) AS ingresos,
            ROUND(
                100 * SUM(line_total)
                / SUM(SUM(line_total)) OVER (PARTITION BY year, month, country),
                2
            )                                     AS participacion_pct
        FROM ventas
        GROUP BY year, month, country, category
    """)


def sales_by_channel(sales: DataFrame) -> DataFrame:
    """Tienda física vs. e-commerce por mes, con participación en ingresos (DataFrames)."""
    month_window = Window.partitionBy("year", "month")
    return (
        sales.groupBy("year", "month", "channel")
        .agg(
            F.count(F.lit(1)).alias("transacciones"),
            F.sum("line_total").cast(MONEY).alias("ingresos"),
        )
        .withColumn(
            "participacion_pct",
            F.round(100 * F.col("ingresos") / F.sum("ingresos").over(month_window), 2),
        )
    )


def conversion_funnel(spark: SparkSession, logs: DataFrame) -> DataFrame:
    """Vistas, carritos y compras por dispositivo, con tasas de conversión (SparkSQL)."""
    logs.createOrReplaceTempView("eventos")
    return spark.sql("""
        SELECT
            device,
            vistas,
            carritos,
            compras,
            ROUND(100 * carritos / NULLIF(vistas, 0), 2)  AS tasa_vista_a_carrito_pct,
            ROUND(100 * compras / NULLIF(carritos, 0), 2) AS tasa_carrito_a_compra_pct,
            ROUND(100 * compras / NULLIF(vistas, 0), 2)   AS tasa_conversion_pct
        FROM (
            SELECT
                COALESCE(device, 'desconocido')                          AS device,
                SUM(CASE WHEN event_type = 'view'        THEN 1 ELSE 0 END) AS vistas,
                SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS carritos,
                SUM(CASE WHEN event_type = 'purchase'    THEN 1 ELSE 0 END) AS compras
            FROM eventos
            GROUP BY COALESCE(device, 'desconocido')
        )
    """)


# ---------------------------------------------------------------------------
# Evidencia de optimización
# ---------------------------------------------------------------------------
def explain_to_text(df: DataFrame) -> str:
    """Devuelve el plan físico que Catalyst eligió, como texto."""
    buffer = StringIO()
    with redirect_stdout(buffer):
        df.explain(mode="formatted")
    return buffer.getvalue()


def save_query_plans(spark: SparkSession, config: PipelineConfig, sales: DataFrame) -> Path:
    """Guarda dos planes de ejemplo como evidencia para el informe:

    1. Una consulta filtrada por periodo: el plan muestra PartitionFilters (solo se
       abren las carpetas de ese mes) y ReadSchema (solo las columnas pedidas).
    2. El enriquecimiento: el plan muestra BroadcastHashJoin con las dimensiones.
    """
    pruned = (
        spark.read.parquet(config.validated_path("transacciones"))
        .where((F.col("year") == 2026) & (F.col("month") == 1))
        .select("store_id", "unit_price")
    )
    path = Path(config.reports_dir) / "plan_catalyst.txt"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("=== 1. Consulta de enero 2026: partition pruning + column pruning ===\n\n")
        f.write(explain_to_text(pruned))
        f.write("\n\n=== 2. Enriquecimiento de ventas: broadcast joins ===\n\n")
        f.write(explain_to_text(sales))
    return path


# ---------------------------------------------------------------------------
# Orquestación
# ---------------------------------------------------------------------------
def run_transformations(spark: SparkSession, config: PipelineConfig) -> TransformationResult:
    result = TransformationResult(
        run_id=datetime.now().strftime("%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:6]
    )
    logger.info("Transformaciones %s | entrada: %s", result.run_id, config.output_dir)
    total_start = time.perf_counter()

    sales = None
    try:
        with timed(result, "lectura_y_enriquecimiento", logger):
            transactions, logs = read_validated(spark, config)
            stores, categories = build_dimensions(spark, config)
            # cache: las ventas enriquecidas alimentan tres agregaciones y la conciliación
            sales = enrich_transactions(transactions, logs, stores, categories).cache()
            result.row_counts["ventas_enriquecidas"] = sales.count()

        tables = {
            "ventas_tienda_mes": (sales_by_store_month(sales), TIME_PARTITIONS),
            "ventas_categoria_pais": (sales_by_category_country(spark, sales), TIME_PARTITIONS),
            "ventas_canal": (sales_by_channel(sales), TIME_PARTITIONS),
            "embudo_conversion": (conversion_funnel(spark, logs), ()),
        }

        with timed(result, "agregacion_y_escritura", logger):
            for name, (df, partitions) in tables.items():
                path = config.curated_path(name)
                write_parquet(df, path, partitions)
                result.outputs[name] = path
                result.row_counts[name] = spark.read.parquet(path).count()
                logger.info("Tabla %s: %s filas", name, result.row_counts[name])

        with timed(result, "conciliacion", logger):
            detail = sales.agg(F.sum("line_total")).first()[0]
            aggregated = (
                spark.read.parquet(config.curated_path("ventas_tienda_mes"))
                .agg(F.sum("ingresos")).first()[0]
            )
            result.reconciliation = {
                "ingresos_detalle": str(detail),
                "ingresos_agregados": str(aggregated),
                "cuadra": detail == aggregated,
            }
            if not result.reconciliation["cuadra"]:
                raise ValueError(f"Conciliación fallida: detalle {detail} vs. agregado {aggregated}")
            logger.info("Conciliación OK: ingresos totales %s", detail)

        with timed(result, "planes_catalyst", logger):
            result.outputs["plan_catalyst"] = str(save_query_plans(spark, config, sales))

        result.status = "exitosa"
    finally:
        result.timings["total"] = round(time.perf_counter() - total_start, 3)
        if sales is not None:
            sales.unpersist()

    logger.info("Transformaciones %s terminaron con estado '%s' en %.3f s",
                result.run_id, result.status, result.timings["total"])
    return result


def save_metrics(result: TransformationResult, config: PipelineConfig) -> Path:
    reports_dir = Path(config.reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    path = reports_dir / "transformaciones_metricas.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "run_id": result.run_id,
                "estado": result.status,
                "spark_master": config.spark_master,
                "tiempos_segundos": result.timings,
                "filas": result.row_counts,
                "conciliacion": result.reconciliation,
                "salidas": result.outputs,
            },
            f, ensure_ascii=False, indent=2,
        )
    return path


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main() -> int:
    configure_logging()
    config = load_config()
    spark = create_spark_session(config)
    try:
        result = run_transformations(spark, config)
        logger.info("Métricas guardadas en %s", save_metrics(result, config))
        print("\nTiempos por etapa (s):")
        for stage, seconds in result.timings.items():
            print(f"  {stage:<28}{seconds:>8.3f}")
        print("\nFilas por tabla:")
        for name, rows in result.row_counts.items():
            print(f"  {name:<28}{rows:>8,}")
        return 0 if result.status == "exitosa" else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
