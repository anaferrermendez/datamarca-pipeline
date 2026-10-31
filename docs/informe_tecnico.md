# Informe técnico — Pipeline distribuido DATAMARCA S.A.

Curso: Big Data y Cloud Computing — Universidad de La Sabana · Unidad 3, Actividad 1
Repositorio: https://github.com/anaferrermendez/datamarca-pipeline

## 1. Resumen
Se implementó un pipeline **batch** en Apache Spark (PySpark) que ingesta transacciones CSV y logs JSON de e-commerce,
valida su calidad, los transforma con DataFrames y SparkSQL, y persiste los resultados en Parquet particionado. Se
ejecuta con `python -m src.pipeline` (local) o con `scripts/gcp_ejecutar.sh` (Google Cloud, serverless). Tiene 29 pruebas
unitarias, pruebas de rendimiento y un experimento que mide el efecto de cada optimización.

## 2. Arquitectura
![Arquitectura del pipeline](img/arquitectura.png)

La arquitectura de la Unidad 2 propone una zona de ingesta, almacenamiento distribuido y capas de procesamiento. Esta
implementación la concreta así: fuentes CSV y JSON, ingesta con calidad y cuarentena, capa de datos validados, capa de
datos curados para análisis y conciliación de cifras. Cada capa vive en una carpeta distinta del almacenamiento
(`validated/`, `quarantine/`, `curated/`), lo que permite auditar y reprocesar por separado.

## 3. Datos, esquema y particionamiento
- Fuentes: `transacciones.csv` y `logs_ecommerce.json` (JSON Lines), generadas con semilla fija (`134340`) y errores
  inyectados a propósito para que las reglas tengan qué detectar.
- Esquemas explícitos (sin `inferSchema`); `unit_price` es `Decimal(12,2)` para evitar errores de redondeo en dinero.
- Calidad: reglas como expresiones nativas de Spark (sin UDFs), severidad `error` (cuarentena) o `warning`, y umbral de
  rechazo del 15 % que detiene la corrida.
- Particionamiento: transacciones por `year`/`month` (partition pruning en consultas por periodo); logs por
  `event_type`. No se particiona por tienda para no generar archivos pequeños.

## 4. Modalidad elegida: batch frente a streaming
Se eligió batch porque las fuentes llegan como archivos completos, el negocio necesita reportes (tienda, categoría,
mes) y no reacción en segundos, y varias validaciones (unicidad, logs huérfanos, umbral de rechazo) requieren ver el
lote completo. Un cluster efímero también cuesta menos que uno permanente esperando eventos.
Con el modelo unificado de Apache Beam, batch es el caso acotado de streaming: la lógica de calidad (expresiones de
DataFrame) se reutiliza en Structured Streaming agregando `readStream`, ventanas sobre `timestamp` y `withWatermark`
para datos tardíos. Streaming **no se implementó**, por lo que no se presenta evidencia de ventanas ni de late data.

## 5. Transformaciones y Catalyst
Tablas curadas: `ventas_tienda_mes`, `ventas_categoria_pais`, `ventas_canal` (particionadas por año y mes) y
`embudo_conversion`. Se usan DataFrames y SparkSQL, y ambos pasan por Catalyst. El plan guardado en
`reports/evidencia/02_plan_catalyst_5000.txt` muestra partition pruning, column pruning, `BroadcastHashJoin` y caché.
Spark tiene activado Adaptive Query Execution. Una conciliación compara los ingresos agregados contra el detalle y hace
fallar la corrida si no coinciden.

## 6. Pruebas de rendimiento (entorno local)
Comando: `python -m src.benchmark --rows 5000 50000 100000 250000 500000 `. Datos: `reports/evidencia`.

Se realizaron pruebas de escalabilidad incrementando el volumen de transacciones desde 5.000 hasta 500.000 registros. El entorno de medición fue una máquina local con 20 vCPUs en modo `local[*]`, ejecutando el pipeline completo (ingesta, validación, transformaciones y persistencia). Los resultados se presentan en la Tabla 1.

**Tabla 1. Tiempos de ejecución y throughput por volumen de datos**

| Filas | Tamaño Entrada (MB) | Ingesta (s) | Transformación (s) | Total (s) | Throughput (filas/s) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 5.000 | 0,85 | 7,12 | 33,92 | 41,05 | 121,8 |
| 50.000 | 8,49 | 8,71 | 34,95 | 43,66 | 1.145,1 |
| 100.000 | 16,97 | 9,40 | 35,82 | 45,23 | 2.211,1 |
| 250.000 | 42,42 | 11,70 | 36,35 | 48,06 | 5.201,7 |
| **500.000** | **84,82** | **11,13** | **35,89** | **47,03** | **10.630,8** |

El tiempo total de ejecución se mantuvo prácticamente constante (entre 41 y 48 segundos) mientras el volumen de datos creció 100 veces (de 5k a 500k). Esto indica que el tiempo está dominado por un **overhead fijo de Apache Spark** (arranque de la JVM, inicialización del contexto y planificación de Catalyst) de aproximadamente 40 segundos, mientras que el procesamiento real de los datos agregó solo unos pocos segundos adicionales. El throughput escaló, alcanzando más de 10.000 filas por segundo en el volumen máximo. Los factores de escalamiento (1,06x al multiplicar por 10 el volumen, y 1,04x al multiplicar por 2) confirman que el pipeline no presenta cuellos de botella por volumen.

### Estimación de costos en el proveedor cloud

Como el pipeline se ejecutó únicamente en entorno local, el costo en la nube **no se midió: se estimó** a partir de los tiempos de la Tabla 1. La referencia es Google Cloud Serverless for Apache Spark, un servicio gestionado de Spark en el que solo se paga por los recursos consumidos durante la ejecución.

**Tarifa y configuración.** La tarifa del nivel estándar es de 0,06 USD por DCU-hora en la región `us-central1`, facturada por segundo con un mínimo de un minuto. Se asume la configuración por defecto, que consume un mínimo de 12 DCU: un driver de 4 vCPU y 16 GiB (4 DCU) y dos ejecutores de 4 vCPU y 16 GiB (8 DCU en total). Es decir, 8 vCPU procesan los datos y se facturan 12 DCU. El costo por ejecución es:

> costo (USD) = horas de ejecución × 12 DCU × 0,06

**Supuestos.**

1. **Tamaño por transacción:** 2 KB (supuesto del equipo para transacciones reales con sus logs asociados). Los datos sintéticos de las pruebas locales pesan ~0,17 KB por fila, por lo que la estimación se expresa en bytes y no en filas.
2. **Tasa de procesamiento medida:** en la Tabla 1, procesar 84,82 MB (500.000 filas) añadió ~7 s sobre el overhead fijo, es decir, ~12 MB/s. Se asume escalamiento lineal con el volumen en bytes.
3. **Overhead fijo:** ~40 s por ejecución, como en la Tabla 1.
4. **Núcleos:** la máquina local usó 20 vCPU y la configuración en la nube procesa con 8, por lo que en el escenario base el tiempo de procesamiento se multiplica por 20/8 = 2,5.
5. **Escenarios:** optimista (el procesamiento no está limitado por núcleos), base (ajuste por núcleos) y pesimista (el doble del procesamiento base, por shuffle y derrames a disco).

**Caso A. Ejecución diaria incremental.** Se procesan solo las transacciones nuevas del día: 1.000.000 de filas, equivalentes a ~2 GB.

**Tabla 2. Escenarios de costo para la ejecución diaria incremental (2 GB)**

| Escenario | Tiempo total | Costo por ejecución | Costo mensual (30 ejecuciones) |
| :--- | :--- | :--- | :--- |
| Optimista | 205 s | 0,041 USD | 1,23 USD |
| Base | 453 s | 0,091 USD | 2,72 USD |
| Pesimista | 865 s | 0,173 USD | 5,19 USD |

**Caso B. Recarga histórica completa (una sola vez).** Se procesan 50.000.000 de filas, equivalentes a ~100 GB.

**Tabla 3. Escenarios de costo para la recarga completa (100 GB)**

| Escenario | Tiempo total | Costo por ejecución |
| :--- | :--- | :--- |
| Optimista | 8.293 s (2,3 h) | 1,66 USD |
| Base | 20.672 s (5,7 h) | 4,13 USD |
| Pesimista | 41.303 s (11,5 h) | 8,26 USD |

Ejemplo de cálculo (Caso B, escenario base): 100.000 MB / 84,82 MB × 7 s = 8.253 s de procesamiento; × 2,5 + 40 s = 20.672 s = 5,74 h; 5,74 × 12 × 0,06 = 4,13 USD.

**Costos no incluidos.** El servicio factura aparte el almacenamiento de shuffle (0,000054795 USD por GiB-hora en el nivel estándar), además del almacenamiento en Cloud Storage y la salida de red. Por ejemplo, 100 GiB de shuffle durante 5,7 h costarían ~0,03 USD, y el almacenamiento de 100 GB de entrada en Cloud Storage sería del orden de unos pocos USD al mes (verificar la tarifa vigente). Estos rubros no cambian las conclusiones.

**Limitaciones.**

1. Las cifras son estimaciones, no mediciones en la nube.
2. La tasa de ~12 MB/s se midió con datos sintéticos de formato simple hasta 85 MB; con JSON real de 2 KB por transacción el procesamiento por byte podría ser más lento, y el escalamiento a 100 GB podría no ser lineal (shuffle, derrames a disco).
3. El costo real depende de la configuración de ejecutores elegida.

Como trabajo futuro, una corrida en la nube permitiría reemplazar la estimación: el comando `gcloud dataproc batches describe BATCH_ID --region REGION` entrega las métricas de uso de la carga (DCU-segundos) para calcular el costo exacto.

**Conclusión.** La operación diaria incremental cuesta entre 1 y 6 USD al mes, y una recarga histórica completa de 100 GB cuesta entre 2 y 8 USD por ejecución. En ambos casos el cómputo no es una barrera económica para la solución propuesta, y el costo se reduce directamente con las optimizaciones del pipeline (menos shuffle, menos particiones innecesarias, formato columnar).

## 7. Experimento de optimización
Comando: `python -m src.experimento_optimizacion --repeticiones 3`. Cada variante se ejecuta con una acción `noop`, tras
un calentamiento, y se reporta la mediana. Datos: 232.464 filas validadas, mismo entorno de 1 vCPU.

| Optimización | Con optimización (s) | Alternativa ingenua (s) | Mejora |
|---|---:|---:|---:|
| Función nativa en lugar de UDF de Python | 0,19 | 0,60 | x3,17 |
| Column pruning (leer solo columnas necesarias) | 0,24 | 0,52 | x2,15 |
| Broadcast join frente a sort-merge join | 1,16 | 1,96 | x1,70 |
| Partition pruning (filtro por `year`/`month`) | 0,19 | 0,32 | x1,68 |
| Caché de las ventas enriquecidas (3 agregaciones) | 4,26 | 3,42 | x0,80 |

Lectura honesta de los resultados:
- Las cuatro primeras optimizaciones reducen el tiempo, y la más fuerte es evitar UDFs, como recomienda la guía.
- **La caché empeoró el tiempo en este entorno** (x0,80). Con datos locales pequeños, en una sola CPU, volver a calcular
  el plan es más barato que materializar la caché. En un cluster con lectura desde Cloud Storage, releer es más caro, y
  la caché puede compensar; esto debe comprobarse con la corrida en la nube antes de afirmarlo.
- En el experimento de *column pruning*, la variante ingenua además calcula una agregación por columna, así que la
  diferencia mezcla lectura y cómputo.
- Las diferencias absolutas son de décimas de segundo; los factores son indicativos, no una garantía en producción.

## 8. Control de versiones y reproducibilidad
Commits semánticos (`feat`, `fix`, `test`, `docs`, `refactor`, `chore`), ramas `feat/*` integradas por Pull Request y
trabajo repartido entre varios integrantes. El README documenta las variables de entorno (sin credenciales), el comando
exacto de ejecución, los requisitos de Windows (`winutils`), el flujo de trabajo en Git y la ejecución en la nube.

## 9. Limitaciones
- Persistencia en Parquet, que el enunciado permite; no se usa Delta Lake.
- El embudo de conversión no es realista porque el generador asigna `event_type` al azar.
- Las mediciones locales vienen de una máquina de 20 vCPU; las tarifas son de referencia y deben verificarse.
