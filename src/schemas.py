"""
Esquemas explícitos de las fuentes de DATAMARCA.

Se definen a mano en lugar de usar inferSchema porque:
  - inferSchema obliga a Spark a leer los datos dos veces (una para inferir, otra para cargar);
  - garantiza tipos estables entre corridas y entornos (local vs. cloud);
  - permite a Catalyst aplicar predicate pushdown y column pruning sobre tipos conocidos.

La columna _corrupt_record captura filas que no se pueden parsear con el esquema
(modo PERMISSIVE), para enviarlas a cuarentena en vez de perderlas en silencio.
"""

from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CORRUPT_RECORD_COL = "_corrupt_record"
TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"

# Transacciones (CSV). unit_price usa Decimal para no acumular errores de redondeo
# de punto flotante al sumar montos de dinero.
TRANSACTIONS_SCHEMA = StructType([
    StructField("transaction_id", StringType(), nullable=True),
    StructField("timestamp", TimestampType(), nullable=True),
    StructField("store_id", StringType(), nullable=True),
    StructField("product_id", StringType(), nullable=True),
    StructField("quantity", IntegerType(), nullable=True),
    StructField("unit_price", DecimalType(12, 2), nullable=True),
    StructField(CORRUPT_RECORD_COL, StringType(), nullable=True),
])

# Logs de e-commerce (JSON Lines)
LOGS_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=True),
    StructField("transaction_id", StringType(), nullable=True),
    StructField("event_type", StringType(), nullable=True),
    StructField("user_id", StringType(), nullable=True),
    StructField("device", StringType(), nullable=True),
    StructField(CORRUPT_RECORD_COL, StringType(), nullable=True),
])

# Opciones de lectura compartidas por la ingesta y las pruebas
TRANSACTIONS_READ_OPTIONS = {
    "header": "true",
    "mode": "PERMISSIVE",
    "columnNameOfCorruptRecord": CORRUPT_RECORD_COL,
    "timestampFormat": TIMESTAMP_FORMAT,
}

LOGS_READ_OPTIONS = {
    "mode": "PERMISSIVE",
    "columnNameOfCorruptRecord": CORRUPT_RECORD_COL,
}
