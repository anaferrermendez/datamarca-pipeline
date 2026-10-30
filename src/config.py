"""
Configuración central del pipeline DATAMARCA.

Orden de precedencia (de menor a mayor):
  1. config/pipeline.json  -> valores por defecto versionados en Git
  2. Variables de entorno  -> permiten cambiar rutas o el master de Spark
                              (por ejemplo, en el entorno cloud) sin tocar código

Variables de entorno soportadas:
  DATAMARCA_CONFIG              Ruta alternativa al JSON de configuración
  DATAMARCA_INPUT_DIR           Carpeta (local o s3a://, gs://, abfss://) con los datos de entrada
  DATAMARCA_OUTPUT_DIR          Carpeta donde se escriben los resultados
  DATAMARCA_SPARK_MASTER        Master de Spark (local[*], yarn, ...)
  DATAMARCA_SHUFFLE_PARTITIONS  Número de particiones de shuffle
  DATAMARCA_REPORTS_DIR         Carpeta LOCAL donde se escriben métricas y planes
                                (en cloud, /tmp/...; luego se publican en el bucket)
  DATAMARCA_REPORTS_URI         Destino (gs://, s3a://, ...) al que se copian los reportes
                                al terminar la corrida (opcional)

  DATAMARCA_SPARK_MASTER=auto   No fija el master: lo define el servicio cloud
                                (Dataproc / EMR Serverless) al enviar el trabajo.
"""

import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "pipeline.json"


@dataclass(frozen=True)
class PipelineConfig:
    app_name: str
    spark_master: str
    shuffle_partitions: int
    timezone: str
    input_dir: str
    output_dir: str
    transactions_file: str
    logs_file: str
    validated_subdir: str
    quarantine_subdir: str
    curated_subdir: str
    reports_dir: str
    store_ids: tuple
    product_ids: tuple
    event_types: tuple
    devices: tuple
    valid_from: str
    valid_to: str
    max_quantity: int
    max_unit_price: float
    max_rejection_rate: float
    transactions_partition_cols: tuple
    events_partition_cols: tuple
    store_dimension: dict
    category_names: dict

    @property
    def transactions_path(self) -> str:
        return f"{self.input_dir.rstrip('/')}/{self.transactions_file}"

    @property
    def logs_path(self) -> str:
        return f"{self.input_dir.rstrip('/')}/{self.logs_file}"

    def validated_path(self, dataset: str) -> str:
        return f"{self.output_dir.rstrip('/')}/{self.validated_subdir}/{dataset}"

    def quarantine_path(self, dataset: str) -> str:
        return f"{self.output_dir.rstrip('/')}/{self.quarantine_subdir}/{dataset}"

    def curated_path(self, dataset: str) -> str:
        return f"{self.output_dir.rstrip('/')}/{self.curated_subdir}/{dataset}"


def _resolve_path(value: str) -> str:
    """Convierte rutas locales relativas en absolutas respecto a la raíz del proyecto.
    Las rutas cloud (s3a://, gs://, abfss://, ...) se dejan intactas."""
    if "://" in value:
        return value
    path = Path(value)
    return str(path if path.is_absolute() else PROJECT_ROOT / path)


def load_config(config_path: str | None = None) -> PipelineConfig:
    path = Path(config_path or os.getenv("DATAMARCA_CONFIG", DEFAULT_CONFIG_PATH))
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)

    spark, paths = raw["spark"], raw["paths"]
    catalogs, quality = raw["catalogs"], raw["quality"]
    partitioning = raw["partitioning"]

    return PipelineConfig(
        app_name=raw["app_name"],
        spark_master=os.getenv("DATAMARCA_SPARK_MASTER", spark["master"]),
        shuffle_partitions=int(
            os.getenv("DATAMARCA_SHUFFLE_PARTITIONS", spark["shuffle_partitions"])
        ),
        timezone=spark["timezone"],
        input_dir=_resolve_path(os.getenv("DATAMARCA_INPUT_DIR", paths["input_dir"])),
        output_dir=_resolve_path(os.getenv("DATAMARCA_OUTPUT_DIR", paths["output_dir"])),
        transactions_file=paths["transactions_file"],
        logs_file=paths["logs_file"],
        validated_subdir=paths["validated_subdir"],
        quarantine_subdir=paths["quarantine_subdir"],
        curated_subdir=paths["curated_subdir"],
        reports_dir=_resolve_path(os.getenv("DATAMARCA_REPORTS_DIR", paths["reports_dir"])),
        store_ids=tuple(catalogs["store_ids"]),
        product_ids=tuple(catalogs["product_ids"]),
        event_types=tuple(catalogs["event_types"]),
        devices=tuple(catalogs["devices"]),
        valid_from=quality["valid_from"],
        valid_to=quality["valid_to"],
        max_quantity=int(quality["max_quantity"]),
        max_unit_price=float(quality["max_unit_price"]),
        max_rejection_rate=float(quality["max_rejection_rate"]),
        transactions_partition_cols=tuple(partitioning["transactions"]),
        events_partition_cols=tuple(partitioning["events"]),
        store_dimension=raw["dimensions"]["stores"],
        category_names=raw["dimensions"]["categories"],
    )


def create_spark_session(config: PipelineConfig):
    """Crea la SparkSession usando el Python del entorno virtual activo.

    Fijar PYSPARK_PYTHON evita que, en Windows, Spark intente usar el alias de
    Python de Microsoft Store, y en macOS/Linux que use un Python distinto al del .venv.
    """
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    builder = SparkSession.builder.appName(config.app_name)
    if config.spark_master.lower() not in ("", "auto"):
        # En cloud se usa "auto": el servicio gestionado ya define el master del cluster
        builder = builder.master(config.spark_master)

    spark = (
        builder
        .config("spark.sql.shuffle.partitions", config.shuffle_partitions)
        .config("spark.sql.session.timeZone", config.timezone)
        # Adaptive Query Execution: Catalyst reoptimiza el plan en tiempo de ejecución
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
