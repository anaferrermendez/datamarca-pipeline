#!/usr/bin/env bash
# Pruebas de rendimiento en Google Cloud: varios volumenes y varios numeros de executors.
# Uso: PROJECT_ID=... BUCKET=... bash scripts/gcp_benchmark.sh
# Genera reports/gcp_batches/*.json (uno por corrida); luego: python3 scripts/gcp_resumen.py
set -euo pipefail
: "${PROJECT_ID:?}"; : "${BUCKET:?}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
mkdir -p "$ROOT/reports/gcp_batches"
for ROWS in ${VOLUMES:-"500000 2000000 5000000"}; do
  FIRST=1
  for EXEC in ${EXECUTORS_LIST:-"2 4"}; do
    export ROWS EXECUTORS="$EXEC"
    [ "$FIRST" = "1" ] || export SKIP_DATA=1     # los datos de cada volumen se suben una sola vez
    FIRST=0
    bash "$ROOT/scripts/gcp_ejecutar.sh" | tee "/tmp/ultima_corrida.txt"
    JSON=$(grep "JSON del batch" /tmp/ultima_corrida.txt | awk '{print $NF}' | head -1)
    cp "$JSON" "$ROOT/reports/gcp_batches/$(basename "$JSON")"
    unset SKIP_DATA
  done
done
echo "Listo. Ejecute: python3 scripts/gcp_resumen.py"
