# Despliegue y pruebas en Google Cloud

Guía para ejecutar el pipeline en **Google Cloud Managed Service for Apache Spark** (antes Dataproc Serverless)
con datos en **Cloud Storage**. Se usa Cloud Shell, así que no hay que instalar nada en el computador.

## 1. Por qué Google Cloud (serverless)

| Criterio | Google Cloud, Spark serverless | AWS, EMR Serverless |
|---|---|---|
| Crédito para empezar | USD 300 de crédito para cuentas nuevas (según la documentación de Google Cloud) | Sin capa gratuita específica de EMR |
| Modelo de cobro | Por DCU-hora (1 vCPU = 0,6 DCU; 1 GiB de RAM = 0,1 DCU), por segundo, mínimo 1 minuto | Por vCPU-hora (USD 0,052624 en us-east-1) y GB-hora (USD 0,0057785), por segundo, mínimo 1 minuto |
| Cluster que administrar | Ninguno: se envía el trabajo y se paga solo lo que corre | Ninguno |
| Acceso a `gs://` / `s3a://` | `gs://` viene integrado | `s3a://` requiere rol de IAM y permisos adicionales |
| Prueba de varios tamaños de cluster | Un parámetro: `spark.executor.instances` | Parámetros de capacidad de la aplicación |
| Costo de referencia (12 vCPU, 48 GB) | 12 DCU × tarifa por DCU-hora (USD 0,06 de referencia, verificar) = 0,72 USD/h | 12 × 0,052624 + 48 × 0,0057785 = 0,91 USD/h |

Conclusión: para un trabajo batch corto, con un equipo estudiantil y presupuesto mínimo, Google Cloud serverless es la
opción con menos fricción y menor costo esperado, sobre todo por el crédito inicial. Las tarifas son de referencia y
cambian; verifíquelas en las páginas oficiales de precios antes de citarlas en el informe.

## 2. Antes de empezar (una sola vez)
1. Cree la cuenta en https://cloud.google.com/free y active el crédito. Pide una tarjeta para verificar identidad.
2. Cree un proyecto y anote su **ID**.
3. En *Facturación → Presupuestos y alertas* cree un presupuesto de USD 10 con alertas, para no llevarse sorpresas.
4. Abra **Cloud Shell** (icono `>_` arriba a la derecha).

## 3. Ejecución
En Cloud Shell:

```bash
git clone https://github.com/anaferrermendez/datamarca-pipeline.git
cd datamarca-pipeline
export PROJECT_ID=<id-de-su-proyecto>
export BUCKET=datamarca-<su-nombre>-2026      # debe ser único en todo Google Cloud
ROWS=500000 bash scripts/gcp_ejecutar.sh          # corrida completa de prueba
```

El script habilita las APIs, crea el bucket, genera y sube los datos, empaqueta el código, envía el batch y muestra el
estado final con el consumo aproximado en DCU. La salida queda en `gs://$BUCKET/out/<batch>/` y las métricas y planes
de Catalyst en `gs://$BUCKET/reportes/<batch>/`.

### Pruebas de rendimiento y escalamiento
```bash
bash scripts/gcp_benchmark.sh        # 500 mil, 2 millones y 5 millones de filas, con 2 y 4 executors
python3 scripts/gcp_resumen.py    # genera reports/gcp_resultados.md con tiempos, DCU-horas y costo
```
Se pueden cambiar los valores con `VOLUMES="1000000 10000000"` y `EXECUTORS_LIST="2 4 8"`.

## 4. Evidencia que hay que guardar para el informe
- Captura de **Dataproc → Batches** con los batches en estado *Succeeded* y sus duraciones.
- Captura del **bucket** con las carpetas `raw/`, `out/` (con `validated/`, `quarantine/`, `curated/` y las particiones
  `year=/month=`) y `reportes/`.
- Los archivos `reports/gcp_batches/*.json` y `reports/gcp_resultados.md`; cópielos a `reports/evidencia/`.
- Los reportes descargados desde `gs://$BUCKET/reportes/...` (métricas por etapa y plan de Catalyst).
- Captura del presupuesto o del costo acumulado en *Facturación*.

## 5. Limpieza (para no gastar crédito)
```bash
gcloud storage rm --recursive gs://$BUCKET
```

## 6. Qué puede fallar
- **Error de red o de subred:** el script activa *Private Google Access* en la subred `default`; si su organización usa
  otra red, active esa opción en la subred correspondiente.
- **Permiso denegado en el bucket:** la cuenta de servicio de cómputo necesita acceso a Cloud Storage; si el proyecto es
  nuevo suele tenerlo por defecto.
- **No se encuentra `pipeline.json`:** el archivo viaja con `--files` y el script de entrada lo busca en el directorio de
  trabajo del driver (error explícito en `cloud_entry.py`).
- **Versión del runtime:** el código solo usa API estables de DataFrames y SparkSQL, y es compatible con Spark 3.5 y 4.x.
- Los nombres de los campos del JSON de `batches describe` (`runtimeInfo.approximateUsage.milliDcuSeconds`) deben
  comprobarse la primera vez; `gcp_resumen.py` indica si falta alguno.

> Los scripts de esta guía y el código de entrada cloud (`src/cloud_entry.py`) se probaron en local con rutas `file://`;
> la ejecución contra Google Cloud real debe hacerla el equipo y documentarse en el informe.
