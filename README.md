# DATAMARCA S.A. — Pipeline de procesamiento distribuido

Pipeline de datos sobre Apache Spark (PySpark) que ingesta, valida, transforma y
persiste transacciones de supermercado en formato CSV y logs de e-commerce en JSON,
con el objetivo de resolver los problemas de trazabilidad transaccional y tiempos
de análisis descritos en el caso DATAMARCA S.A.

Curso: Big Data y Cloud Computing — Universidad de La Sabana
Unidad: 3 — Implementación de pipeline batch o streaming con Spark en entorno cloud

## Estado del proyecto

| Etapa | Estado |
|---|---|
| Configuración del entorno (Java 17 + PySpark + `.venv`) | Completado |
| Generador de dataset sintético reproducible |  Completado |
| Reglas de calidad y configuración del pipeline |  Completado |
| Ingesta con validaciones |  Completado |
| Transformaciones (SparkSQL / DataFrames) |  Completado |
| Persistencia en Parquet particionado |  Completado |
| Pruebas de rendimiento y estimación de costos |  Pendiente |
| Informe técnico |  Pendiente |

## Requisitos del entorno

| Componente | Versión | Notas |
|---|---|---|
| Python | 3.11 o superior | Probado en Windows 11 |
| Java (JDK) | 17 | Requerido por PySpark |
| Apache Spark | vía `pyspark` (ver `requirements.txt`) | Se instala con pip |

### Variables de entorno requeridas

| Variable | Valor esperado | Propósito |
|---|---|---|
| `JAVA_HOME` | Ruta a JDK 17 (ej. `C:\Program Files\Java\jdk-17`) | Spark requiere JVM |
| `PYSPARK_PYTHON` | Ruta al Python del `.venv` | Fuerza a Spark a usar el intérprete del entorno virtual |
| `PYSPARK_DRIVER_PYTHON` | Igual que `PYSPARK_PYTHON` | Evita que Spark use el alias falso de Python de Microsoft Store |

`src/config.py` asigna automáticamente `PYSPARK_PYTHON` y `PYSPARK_DRIVER_PYTHON` al
Python del `.venv` activo si no están definidas.

### Variables de entorno opcionales del pipeline

Sobrescriben los valores de `config/pipeline.json` sin modificar código (por ejemplo,
para apuntar al almacenamiento cloud). Ninguna contiene credenciales.

| Variable | Default | Propósito |
|---|---|---|
| `DATAMARCA_CONFIG` | `config/pipeline.json` | Ruta a un archivo de configuración alternativo |
| `DATAMARCA_INPUT_DIR` | `data/sample` | Carpeta de entrada (local o `s3a://`, `gs://`, `abfss://`) |
| `DATAMARCA_OUTPUT_DIR` | `data/output` | Carpeta de salida |
| `DATAMARCA_SPARK_MASTER` | `local[*]` | Master de Spark |
| `DATAMARCA_SHUFFLE_PARTITIONS` | `8` | Particiones de shuffle (subir en cluster) |

### Windows: escritura de Parquet

En Windows, Spark necesita `winutils.exe` y `hadoop.dll` (Hadoop 3.x) para escribir
archivos. Copiarlos en una carpeta como `C:\hadoop\bin` y definir:

| Variable | Valor |
|---|---|
| `HADOOP_HOME` | `C:\hadoop` |
| `PATH` | agregar `C:\hadoop\bin` |

Sin esto, la ingesta falla al escribir con un error de `NativeIO` o `winutils`.
En macOS y Linux no hace falta.

### macOS

Instalar JDK 17 con `brew install openjdk@17` y definir en `~/.zshrc`:

```bash
export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
```

## Instalación reproducible desde cero

```bash
# 1. Clonar el repositorio
git clone <URL_DEL_REPO>
cd datamarca-pipeline

# 2. Crear y activar entorno virtual
python -m venv .venv

# Windows PowerShell
.\.venv\Scripts\Activate.ps1
# Si PowerShell bloquea scripts:
# Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned

# Linux / macOS
source .venv/bin/activate

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Verificar que Spark funciona
python src/smoke_test.py
# Debe imprimir una tabla de prueba
```
## Dataset

El dataset no se versiona en Git porque es reproducible a partir del generador y de una semilla fija. Cualquier integrante del equipo obtiene exactamente los mismos datos ejecutando el mismo comando.

### Generar el dataset

```bash
# Dataset base para desarrollo (5.000 transacciones)
python data/generate_sample.py --rows 5000 --seed 134340

# Dataset para pruebas de rendimiento (500.000 transacciones)
python data/generate_sample.py --rows 500000 --seed 134340
```

Parámetros disponibles:

| Parámetro | Default | Descripción |
|---|---|---|
| `--rows` | `5000` | Número de transacciones a generar |
| `--seed` | `134340` | Semilla para reproducibilidad |
| `--output-dir` | `data/sample` | Carpeta de salida |

### Fuentes generadas

| Archivo | Formato | Descripción |
|---|---|---|
| `transacciones.csv` | CSV | Transacciones de POS, e-commerce y sistemas internos |
| `logs_ecommerce.json` | JSON Lines | Eventos de e-commerce (view, add_to_cart, purchase) |

### Catálogos

- Tiendas (`store_id`): `<CIUDAD>-<NNN>`
  - Colombia: `BOG-001`, `BOG-002`, `MED-001`, `CAL-001`
  - México: `CDMX-001`, `GDL-001`, `MTY-001`
  - Perú: `LIM-001`, `LIM-002`
- Productos (`product_id`): `<CATEGORÍA>-<NNNN>`
  - `LAC` = Lácteos, `BEB` = Bebidas, `ABR` = Abarrotes,
    `LIM` = Limpieza, `FRU` = Frutas y verduras

### Rango temporal

Del 1 de octubre de 2025 al 31 de marzo de 2026 (6 meses cruzando el año
nuevo). Este rango justifica la partición de la salida por `year` y `month`.

### Errores inyectados intencionalmente

Para que las validaciones de calidad del pipeline tengan qué detectar, el generador introduce errores controlados:

| Error | Fuente | Frecuencia |
|---|---|---|
| `quantity <= 0` | Transacciones | ~2 % |
| `unit_price <= 0` | Transacciones | ~2 % |
| `transaction_id` vacío | Transacciones | ~1 % |
| `store_id` fuera de catálogo (`BOG-999`) | Transacciones | ~1 % |
| `timestamp` futuro (año 2027) | Transacciones | ~1 % |
| Log huérfano (`TXN-ORPHAN-...`) | Logs | ~10 % |
| Log sin `user_id` | Logs | ~2 % |
| Transacción sin log asociado | Logs | ~15 % |

### Verificar reproducibilidad

```bash
# PowerShell
Copy-Item data/sample/transacciones.csv data/sample/transacciones_copia.csv
python data/generate_sample.py --rows 100 --seed 134340
Compare-Object (Get-Content data/sample/transacciones.csv) `
               (Get-Content data/sample/transacciones_copia.csv)
# Sin salida = archivos idénticos
Remove-Item data/sample/transacciones_copia.csv
```

## Configuración y reglas de calidad

### Estructura

| Archivo | Contenido |
|---|---|
| `config/pipeline.json` | Catálogos, periodo válido, umbrales de calidad, particionamiento |
| `src/config.py` | Carga la configuración (JSON + variables de entorno) y crea la `SparkSession` |
| `src/schemas.py` | Esquemas explícitos de transacciones (CSV) y logs (JSON) |
| `src/quality_rules.py` | Catálogo de reglas y funciones para aplicarlas, separar cuarentena y reportar |
| `src/data_quality_report.py` | Perfil de calidad del dataset; guarda `reports/calidad_datos.json` |
| `tests/test_quality_rules.py` | Pruebas unitarias de cada regla (pytest) |

### Decisiones de diseño

- **Esquemas explícitos, sin `inferSchema`**: evita una lectura extra de los datos y
  garantiza tipos estables. `unit_price` es `Decimal(12,2)` para no acumular errores de
  redondeo en sumas de dinero.
- **Reglas como expresiones nativas de Spark, sin UDFs de Python**: Catalyst puede
  optimizar el plan completo (predicate pushdown, column pruning, generación de código).
- **Una sola pasada**: todas las reglas se evalúan juntas y sus nombres se guardan en las
  columnas `dq_errors` y `dq_warnings`.
- **Cuarentena en vez de descarte**: los registros con errores se conservan con el motivo
  del rechazo y la fecha de validación, para trazabilidad.
- **Periodo válido configurable** (`valid_from` / `valid_to`) en lugar de comparar con la
  fecha actual: así el resultado es reproducible sin importar cuándo se ejecute.
- **Umbral de rechazo** (`max_rejection_rate`, 15 %): si se supera, el pipeline se detiene
  con `DataQualityError` para no publicar resultados sobre datos degradados.

### Reglas implementadas

Severidad `error` envía el registro a cuarentena; `warning` lo deja pasar marcado.

| Regla | Fuente | Dimensión | Severidad |
|---|---|---|---|
| `txn_registro_corrupto` | Transacciones | Validez | error |
| `txn_id_faltante` | Transacciones | Completitud | error |
| `txn_id_formato_invalido` | Transacciones | Validez | error |
| `txn_id_duplicado` | Transacciones | Unicidad | error |
| `txn_timestamp_faltante` | Transacciones | Completitud | error |
| `txn_timestamp_fuera_de_rango` | Transacciones | Consistencia | error |
| `txn_tienda_faltante` / `txn_tienda_fuera_de_catalogo` | Transacciones | Completitud / Integridad | error |
| `txn_producto_faltante` / `txn_producto_fuera_de_catalogo` | Transacciones | Completitud / Integridad | error |
| `txn_cantidad_faltante` / `txn_cantidad_no_positiva` | Transacciones | Completitud / Validez | error |
| `txn_precio_faltante` / `txn_precio_no_positivo` | Transacciones | Completitud / Validez | error |
| `txn_cantidad_atipica` / `txn_precio_atipico` | Transacciones | Validez | warning |
| `log_registro_corrupto` | Logs | Validez | error |
| `log_event_id_faltante` / `log_event_id_duplicado` | Logs | Completitud / Unicidad | error |
| `log_txn_id_faltante` | Logs | Completitud | error |
| `log_tipo_evento_invalido` | Logs | Validez | error |
| `log_transaccion_huerfana` | Logs | Integridad referencial | error |
| `log_usuario_faltante` | Logs | Completitud | warning |
| `log_dispositivo_invalido` | Logs | Validez | warning |

Las transacciones sin log asociado no se consideran error (corresponden a ventas en POS
físico); se reportan como métrica informativa de cobertura.

### Estrategia de particionamiento

Las transacciones válidas se persistirán en Parquet particionado por `year` y `month`
(derivados de `timestamp`). Las consultas de negocio filtran por periodo, así que Spark
lee solo las carpetas necesarias (partition pruning), y el rango de 6 meses produce
particiones de tamaño razonable. No se particiona por `store_id` para evitar demasiados
archivos pequeños.

### Ejecutar

```bash
# Pruebas unitarias
python -m pytest

# Perfil de calidad del dataset (generarlo antes con data/generate_sample.py)
python -m src.data_quality_report
```

Resultado con `--rows 5000 --seed 134340`:

| Fuente | Registros | Rechazados | Advertencias |
|---|---|---|---|
| Transacciones | 5.000 | 356 (7,12 %) | 0 |
| Logs | 4.284 | 488 (11,39 %) | 102 |

Los logs huérfanos superan el 10 % inyectado porque también se marcan los logs cuya
transacción llegó con `transaction_id` vacío: sin identificador no hay forma de enlazarlos.

## Ingesta con validaciones

### Modalidad elegida: batch

El pipeline procesa los datos por lotes (batch) y no en streaming. Razones:

- **Naturaleza de las fuentes.** Las transacciones y los logs llegan como archivos
  completos (CSV y JSON), no como un flujo continuo de eventos.
- **Necesidad del negocio.** El caso de DATAMARCA pide trazabilidad y reducir tiempos de
  análisis (reportes por tienda, producto y mes), no reaccionar en segundos.
- **Validaciones que miran el lote completo.** Unicidad de `transaction_id`, logs huérfanos
  y el umbral de rechazo necesitan ver todos los registros; en streaming exigirían
  estado entre micro-lotes y marcas de agua (watermarks).
- **Costo.** Un cluster que se enciende, procesa y se apaga cuesta menos que uno encendido
  todo el día esperando eventos.

Siguiendo el modelo unificado de Apache Beam, batch es un caso particular de streaming con
datos acotados. Las reglas son expresiones de DataFrame que Structured Streaming también
acepta (`spark.readStream`), así que migrar implicaría cambiar la lectura y la escritura,
agregar ventanas temporales sobre `timestamp` y un `withWatermark` para datos tardíos,
pero no reescribir la lógica de calidad.

### Flujo

1. **Lectura** de ambas fuentes con esquemas explícitos.
2. **Validación**: reglas de calidad y detección de logs huérfanos, en una sola pasada.
   Los resultados se cachean para no releer los archivos.
3. **Escritura de la cuarentena**, siempre, para auditoría.
4. **Control de umbral**: si algún origen supera `max_rejection_rate`, la corrida termina
   con estado `detenida_por_calidad` y no publica datos válidos.
5. **Escritura de los válidos** en Parquet particionado.

Cada registro escrito lleva `ingestion_run_id` e `ingested_at` para trazabilidad.
Las escrituras usan modo `overwrite`, así que repetir la corrida da el mismo resultado
(idempotencia). Antes de escribir se hace `repartition` por las columnas de partición
para dejar un archivo por carpeta y evitar el problema de muchos archivos pequeños.

### Salidas

```
data/output/
├── validated/
│   ├── transacciones/year=2025/month=10/part-....parquet   (particionado por año y mes)
│   └── logs/event_type=purchase/part-....parquet          (particionado por tipo de evento)
└── quarantine/
    ├── transacciones/   (con dq_errors, dq_warnings y dq_checked_at)
    └── logs/
reports/ingesta_metricas.json   (tiempos por etapa, calidad y rutas de salida)
```

### Ejecutar

```bash
python -m src.ingestion
```

El comando termina con código 0 si la corrida es exitosa y 1 si se detuvo por calidad.
Los logs muestran el inicio y fin de cada etapa con su duración.

## Transformaciones

Parten de los datos validados por la ingesta y producen tablas de negocio en
`data/output/curated/`.

### Pipeline completo en un comando

```bash
python data/generate_sample.py --rows 5000 --seed 134340
python -m src.pipeline
```

`src.pipeline` ejecuta la ingesta y, si es exitosa, las transformaciones, con una sola
`SparkSession`. Los tiempos de todas las etapas quedan en `reports/pipeline_metricas.json`.
Cada fase también se puede ejecutar por separado con `python -m src.ingestion` y
`python -m src.transformations`.

### Flujo

1. **Lectura** de los Parquet validados, seleccionando solo las columnas necesarias.
2. **Enriquecimiento** de cada venta con:
   - `line_total` = `quantity` × `unit_price` (Decimal);
   - ciudad y país de la tienda, y nombre de la categoría del producto, mediante
     *broadcast joins* con dimensiones definidas en `config/pipeline.json`;
   - canal: `ecommerce` si la venta tiene al menos un evento de e-commerce,
     `tienda_fisica` si no (supuesto del caso: el POS no genera logs).
3. **Agregaciones** y escritura en Parquet.
4. **Conciliación**: la suma de ingresos agregados debe ser igual a la del detalle; si no,
   la corrida falla.
5. **Planes de Catalyst** guardados en `reports/plan_catalyst.txt` como evidencia.

### Tablas generadas

| Tabla | API | Particionado | Contenido |
|---|---|---|---|
| `ventas_tienda_mes` | DataFrames | `year`, `month` | Transacciones, unidades, ingresos y ticket promedio por tienda |
| `ventas_categoria_pais` | SparkSQL | `year`, `month` | Unidades, ingresos y participación de cada categoría dentro del país |
| `ventas_canal` | DataFrames | `year`, `month` | Transacciones, ingresos y participación de tienda física vs. e-commerce |
| `embudo_conversion` | SparkSQL | — (3 filas) | Vistas, carritos, compras y tasas de conversión por dispositivo |

Se usan las dos APIs a propósito: ambas pasan por el mismo optimizador Catalyst y
generan planes equivalentes.

### Optimizaciones verificables en `reports/plan_catalyst.txt`

| Optimización | Dónde se ve en el plan | Efecto |
|---|---|---|
| Partition pruning | `PartitionFilters: [(year = 2026), (month = 1)]` | Solo se abre la carpeta del mes consultado |
| Column pruning | `ReadSchema: struct<store_id, unit_price>` | Solo se leen las columnas usadas |
| Broadcast join | `BroadcastHashJoin` | Las dimensiones se copian a cada trabajador; la tabla de ventas no se mueve |
| Caché | `InMemoryRelation` | Las ventas enriquecidas se calculan una vez para tres agregaciones |

### Limitación conocida del dataset sintético

El generador asigna `event_type` al azar e independiente para cada evento, por lo que
el embudo no refleja un recorrido real del cliente: las tasas rondan el 100 % y pueden
superarlo. Con datos reales (secuencias de eventos por sesión), la misma consulta
mostraría la caída típica de vista → carrito → compra.

