# Informe técnico — Pipeline distribuido DATAMARCA S.A.

Curso: Big Data y Cloud Computing — Universidad de La Sabana · Unidad 3, Actividad 1

## 1. Resumen

Se implementó un pipeline **batch** en Apache Spark (PySpark) que ingesta transacciones CSV y logs JSON de e-commerce,
valida su calidad, los transforma con DataFrames y SparkSQL, y persiste los resultados en Parquet particionado.
El pipeline completo se ejecuta con `python -m src.pipeline` y está cubierto por 25 pruebas unitarias (pytest).

## 2. Datos, esquema y particionamiento

- Fuentes: `transacciones.csv` y `logs_ecommerce.json` (JSON Lines), generadas con semilla fija (`134340`) para que
  cualquier integrante reproduzca exactamente los mismos datos. El generador inyecta errores controlados.
- Esquemas explícitos (sin `inferSchema`); `unit_price` es `Decimal(12,2)` para evitar errores de redondeo.
- Calidad: reglas como expresiones nativas de Spark (sin UDFs), severidad `error` (cuarentena) o `warning`, y umbral de
  rechazo del 15 % que detiene la corrida (`DataQualityError`).
- Particionamiento: transacciones por `year`/`month` (partition pruning en consultas por periodo); logs por
  `event_type`. No se particiona por tienda para evitar archivos pequeños.

## 3. Modalidad elegida: batch vs. streaming

Se eligió batch porque las fuentes llegan como archivos completos, el negocio necesita reportes (tienda, categoría,
mes) y no reacción en segundos, y varias validaciones (unicidad, logs huérfanos, umbral de rechazo) requieren ver el
lote completo. Además un cluster efímero cuesta menos que uno permanente esperando eventos.
Con el modelo unificado de Apache Beam, batch es el caso acotado de streaming: la lógica de calidad (expresiones de
DataFrame) se reutiliza en Structured Streaming agregando `readStream`, ventanas sobre `timestamp` y `withWatermark`
para datos tardíos. Streaming **no se implementó**, por lo que no se presenta evidencia de ventanas ni late data.

## 4. Transformaciones y optimización (Catalyst)

Tablas curadas: `ventas_tienda_mes`, `ventas_categoria_pais`, `ventas_canal` (particionadas por año/mes) y
`embudo_conversion`. Se usan DataFrames y SparkSQL (ambos pasan por Catalyst) y se verifican en
`reports/evidencia/02_plan_catalyst_5000.txt`: partition pruning, column pruning, `BroadcastHashJoin` para
dimensiones pequeñas y caché (`InMemoryRelation`) de las ventas enriquecidas reutilizadas por tres agregaciones.
Una conciliación compara ingresos agregados contra el detalle y falla la corrida si no coinciden.

## 5. Pruebas de rendimiento

Comando: `python -m src.benchmark --rows 5000 50000 250000`. Datos: `reports/evidencia/03_benchmark_rendimiento.*`.

**Entorno de medición (importante):** una sola máquina Linux con 1 vCPU y 3 GB de RAM, `local[*]`. **No es un cluster
cloud**; los tiempos sirven para ver la tendencia, no como rendimiento esperado en producción.

| Filas | Entrada (MB) | Ingesta (s) | Transformación (s) | Total (s) | Filas/s |
|---:|---:|---:|---:|---:|---:|
| 5.000 | 0,85 | 19,75 | 10,96 | 30,74 | 162,6 |
| 50.000 | 8,45 | 24,66 | 11,50 | 36,20 | 1.381 |
| 250.000 | 42,21 | 36,52 | 14,29 | 50,85 | 4.917 |

Lectura de los resultados:

- Con x10 en volumen (5.000 → 50.000) el tiempo crece x1,18, y con x5 más (→ 250.000) crece x1,40. El tiempo es
  dominado por costos fijos (arranque de la JVM, planificación, escritura de archivos), no por el volumen.
- El volumen máximo probado es pequeño (42 MB). Para afirmar escalabilidad real hay que repetir la prueba con
  volúmenes de varios GB en un cluster y comparar 1, 2 y 4 nodos.

## 6. Estimación de costo

Modelo: `(tiempo de ejecución + arranque del cluster) × nodos × tarifa nodo-hora`.
Supuestos: 3 nodos, USD 0,24 por nodo-hora (instancia tipo m5.xlarge más recargo EMR, bajo demanda; **supuesto sin
verificar contra la calculadora vigente**) y 300 s de arranque facturados.

| Filas | Costo estimado por corrida (USD) |
|---:|---:|
| 5.000 | 0,066 |
| 50.000 | 0,067 |
| 250.000 | 0,070 |

El costo está dominado por el arranque del cluster; a estos volúmenes procesar más datos casi no lo cambia. Se
recomienda cluster efímero (encender, procesar, apagar) y verificar tarifas reales antes de presentar la cifra.

## 7. Control de versiones y reproducibilidad

Commits semánticos (`feat`, `fix`, `test`, `docs`, `refactor`, `chore`), ramas `feat/*` integradas por Pull Request,
y trabajo repartido entre varios integrantes. El README documenta variables de entorno (sin credenciales), el comando
exacto de ejecución, requisitos de Windows (`winutils`) y supuestos de ejecución en cloud.

## 8. Limitaciones

- No hay corrida sobre infraestructura cloud real; el soporte existe vía `s3a://`, `gs://`, `abfss://` y variables de
  entorno, pero falta evidencia (captura del cluster, logs del proveedor, bucket con la salida).
- Persistencia en Parquet (permitido por el enunciado); no se usa Delta Lake.
- El embudo de conversión no es realista porque el generador asigna `event_type` al azar.
- Costos y tarifas son estimaciones con supuestos explícitos.
