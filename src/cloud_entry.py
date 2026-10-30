"""
Punto de entrada para ejecutar el pipeline en un servicio Spark gestionado
(Google Cloud Managed Service for Apache Spark / Dataproc Serverless, o equivalentes).

Los servicios serverless no pasan variables de entorno al driver de forma uniforme, así
que los parámetros llegan como argumentos y aquí se convierten en las variables DATAMARCA_*
que ya entiende src/config.py. El código del pipeline no cambia entre local y cloud.

Ejemplo (lo arma scripts/gcp_ejecutar.sh):
    cloud_entry.py --input-dir gs://B/raw --output-dir gs://B/out \\
        --reports-uri gs://B/reportes --config pipeline.json --shuffle-partitions 64
"""

import argparse
import os
import sys
import tempfile


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description="Pipeline DATAMARCA en cloud")
    ap.add_argument("--input-dir", required=True, help="Origen (gs://, s3a://, ...)")
    ap.add_argument("--output-dir", required=True, help="Destino de Parquet (gs://, s3a://, ...)")
    ap.add_argument("--reports-uri", default=None, help="Destino de métricas y planes")
    ap.add_argument("--config", default=None, help="Ruta local a pipeline.json (enviado con --files)")
    ap.add_argument("--shuffle-partitions", type=int, default=None)
    ap.add_argument("--spark-master", default="auto", help="'auto' deja el master al servicio cloud")
    return ap.parse_args(argv)


def apply_environment(args) -> None:
    os.environ["DATAMARCA_INPUT_DIR"] = args.input_dir
    os.environ["DATAMARCA_OUTPUT_DIR"] = args.output_dir
    os.environ["DATAMARCA_SPARK_MASTER"] = args.spark_master
    os.environ["DATAMARCA_REPORTS_DIR"] = os.path.join(tempfile.gettempdir(), "datamarca_reports")
    if args.reports_uri:
        os.environ["DATAMARCA_REPORTS_URI"] = args.reports_uri
    if args.config:
        if not os.path.exists(args.config):
            raise SystemExit(
                f"No se encontró {args.config} en el directorio de trabajo del driver. "
                "Envíelo con --files (ver scripts/gcp_ejecutar.sh)."
            )
        os.environ["DATAMARCA_CONFIG"] = args.config
    if args.shuffle_partitions:
        os.environ["DATAMARCA_SHUFFLE_PARTITIONS"] = str(args.shuffle_partitions)


def main(argv=None) -> int:
    args = parse_args(argv)
    apply_environment(args)
    from src import pipeline  # se importa después de fijar el entorno

    return pipeline.main()


if __name__ == "__main__":
    sys.exit(main())
