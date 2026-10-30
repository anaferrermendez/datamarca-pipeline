#!/usr/bin/env bash
# Ejecuta el pipeline DATAMARCA en Google Cloud (Managed Service for Apache Spark, modo serverless).
# Pensado para Google Cloud Shell (ya trae gcloud, git, python3 y zip).
#
# Uso:   PROJECT_ID=mi-proyecto BUCKET=datamarca-sofi-2026 ROWS=2000000 bash scripts/gcp_ejecutar.sh
# Opcionales: REGION (us-central1), EXECUTORS (2), SHUFFLE (64), SKIP_DATA=1 (reusar datos ya subidos)
#
# NOTA: este script no fue ejecutado por el autor del repo contra una cuenta real; si un comando
# falla por cambios de la CLI, el mensaje de gcloud indica el ajuste. Ver docs/despliegue_gcp.md.
set -euo pipefail

: "${PROJECT_ID:?Defina PROJECT_ID}"
: "${BUCKET:?Defina BUCKET (nombre global unico, sin gs://)}"
REGION="${REGION:-us-central1}"
ROWS="${ROWS:-2000000}"
EXECUTORS="${EXECUTORS:-2}"
SHUFFLE="${SHUFFLE:-64}"
BATCH="datamarca-${ROWS}-e${EXECUTORS}-$(date +%Y%m%d-%H%M%S)"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo ">> Proyecto $PROJECT_ID | region $REGION | bucket gs://$BUCKET | filas $ROWS | executors $EXECUTORS"
gcloud config set project "$PROJECT_ID" >/dev/null
gcloud services enable dataproc.googleapis.com storage.googleapis.com compute.googleapis.com

# Spark serverless necesita Private Google Access en la subred (la red 'default' lo trae desactivado)
gcloud compute networks subnets update default --region="$REGION" --enable-private-ip-google-access || true

# Bucket (idempotente)
gcloud storage buckets describe "gs://$BUCKET" >/dev/null 2>&1 || \
  gcloud storage buckets create "gs://$BUCKET" --location="$REGION" --uniform-bucket-level-access

# Datos de entrada
if [ -z "${SKIP_DATA:-}" ]; then
  echo ">> Generando $ROWS transacciones (semilla fija) y subiendolas"
  python3 "$ROOT/data/generate_sample.py" --rows "$ROWS" --seed 134340 --output-dir /tmp/datamarca_raw
  gcloud storage cp /tmp/datamarca_raw/transacciones.csv /tmp/datamarca_raw/logs_ecommerce.json "gs://$BUCKET/raw/$ROWS/"
fi

# Codigo: el paquete src va en un zip; la configuracion viaja como archivo aparte
echo ">> Subiendo codigo"
(cd "$ROOT" && rm -f /tmp/src.zip && zip -qr /tmp/src.zip src -x "*/__pycache__/*")
gcloud storage cp /tmp/src.zip "gs://$BUCKET/code/src.zip"
gcloud storage cp "$ROOT/src/cloud_entry.py" "gs://$BUCKET/code/cloud_entry.py"
gcloud storage cp "$ROOT/config/pipeline.json" "gs://$BUCKET/code/pipeline.json"

# Envio del trabajo (espera a que termine). Cada executor: 4 vCPU, como en el ejemplo de 12 DCU
echo ">> Enviando batch $BATCH"
gcloud dataproc batches submit pyspark "gs://$BUCKET/code/cloud_entry.py" \
  --project="$PROJECT_ID" --region="$REGION" --batch="$BATCH" \
  --py-files="gs://$BUCKET/code/src.zip" \
  --files="gs://$BUCKET/code/pipeline.json" \
  --properties="spark.executor.instances=${EXECUTORS},spark.driver.cores=4,spark.executor.cores=4" \
  -- --input-dir "gs://$BUCKET/raw/$ROWS" \
     --output-dir "gs://$BUCKET/out/$BATCH" \
     --reports-uri "gs://$BUCKET/reportes/$BATCH" \
     --config pipeline.json \
     --shuffle-partitions "$SHUFFLE"

echo ">> Resultado del batch (estado, tiempos y consumo aproximado en DCU):"
gcloud dataproc batches describe "$BATCH" --region="$REGION" --format=json | tee "/tmp/${BATCH}.json" | head -60
echo ">> Salida Parquet:   gs://$BUCKET/out/$BATCH"
echo ">> Reportes:         gs://$BUCKET/reportes/$BATCH"
echo ">> JSON del batch:   /tmp/${BATCH}.json  (guardelo en reports/evidencia/)"
