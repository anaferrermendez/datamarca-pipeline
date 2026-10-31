"""
Pipeline completo de DATAMARCA: ingesta + transformaciones en una sola corrida.

Usa una sola SparkSession para ambas fases y guarda un resumen con los tiempos
de cada etapa en reports/pipeline_metricas.json.

Uso:
    python -m src.pipeline
"""

import json
import logging
import time
from pathlib import Path

from src import ingestion, transformations
from src.cloud_utils import publish_reports
from src.config import create_spark_session, load_config

logger = logging.getLogger("datamarca.pipeline")


def main() -> int:
    transformations.configure_logging()
    config = load_config()
    spark = create_spark_session(config)
    start = time.perf_counter()
    summary = {"spark_master": config.spark_master, "input_dir": config.input_dir}

    try:
        ing = ingestion.run_ingestion(spark, config)
        ingestion.save_metrics(ing, config)
        summary["ingesta"] = {"run_id": ing.run_id, "estado": ing.status,
                              "tiempos_segundos": ing.timings, "error": ing.error}

        if ing.status != "exitosa":
            logger.error("Pipeline detenido en la ingesta: %s", ing.error)
            summary["estado"] = "detenido_en_ingesta"
            return 1

        tra = transformations.run_transformations(spark, config)
        transformations.save_metrics(tra, config)
        summary["transformaciones"] = {"run_id": tra.run_id, "estado": tra.status,
                                       "tiempos_segundos": tra.timings,
                                       "filas": tra.row_counts,
                                       "conciliacion": tra.reconciliation}
        summary["estado"] = "exitosa"
        return 0
    finally:
        summary["tiempo_total_segundos"] = round(time.perf_counter() - start, 3)
        path = Path(config.reports_dir) / "pipeline_metricas.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        print("\nResumen del pipeline (s):")
        for phase in ("ingesta", "transformaciones"):
            for stage, seconds in summary.get(phase, {}).get("tiempos_segundos", {}).items():
                print(f"  {phase:<17}{stage:<28}{seconds:>8.3f}")
        print(f"  {'TOTAL':<45}{summary['tiempo_total_segundos']:>8.3f}")
        print(f"Estado: {summary.get('estado', 'fallida')} | métricas en {path}")
        publish_reports(spark, config.reports_dir)
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
