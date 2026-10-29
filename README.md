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
| Reglas de calidad y configuración del pipeline |  En progreso |
| Ingesta con validaciones |  Pendiente |
| Transformaciones (SparkSQL / DataFrames) |  Pendiente |
| Persistencia en Parquet particionado |  Pendiente |
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
