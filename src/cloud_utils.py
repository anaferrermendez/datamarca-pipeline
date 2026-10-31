"""
Utilidades para ejecutar el pipeline en cloud.

En un servicio gestionado (Dataproc / EMR Serverless) el disco del driver desaparece al
terminar el trabajo. Si se define DATAMARCA_REPORTS_URI (por ejemplo gs://bucket/reportes),
las métricas y los planes de Catalyst escritos en la carpeta local de reportes se copian
a ese destino usando el sistema de archivos de Hadoop, que ya conoce gs://, s3a:// y file://.
"""

import logging
import os
from pathlib import Path

logger = logging.getLogger("datamarca.cloud")


def publish_reports(spark, local_dir: str, destination: str | None = None) -> str | None:
    """Copia los archivos de `local_dir` a `destination` (o a DATAMARCA_REPORTS_URI).

    Devuelve el destino usado, o None si no hay destino configurado o no hay nada que copiar.
    Un fallo al copiar se registra pero no hace fallar el pipeline.
    """
    destination = destination or os.getenv("DATAMARCA_REPORTS_URI")
    if not destination:
        return None
    files = [p for p in Path(local_dir).glob("*") if p.is_file()]
    if not files:
        return None
    try:
        jvm = spark._jvm
        hadoop_conf = spark._jsc.hadoopConfiguration()
        dest_path = jvm.org.apache.hadoop.fs.Path(destination)
        fs = dest_path.getFileSystem(hadoop_conf)
        fs.mkdirs(dest_path)
        for f in files:
            src = jvm.org.apache.hadoop.fs.Path(f.resolve().as_uri())
            fs.copyFromLocalFile(False, True, src, jvm.org.apache.hadoop.fs.Path(f"{destination.rstrip('/')}/{f.name}"))
        logger.info("Reportes publicados en %s (%d archivos)", destination, len(files))
        return destination
    except Exception as exc:  # noqa: BLE001 - la publicación es best-effort
        logger.warning("No se pudieron publicar los reportes en %s: %s", destination, exc)
        return None
