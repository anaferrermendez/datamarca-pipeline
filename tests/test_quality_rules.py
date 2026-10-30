"""Pruebas unitarias de las reglas de calidad: cada error inyectado debe detectarse."""

from datetime import datetime
from decimal import Decimal

import pytest

from src.quality_rules import (
    ERRORS_COL,
    LOG_RULES,
    TRANSACTION_RULES,
    WARNINGS_COL,
    DataQualityError,
    apply_rules,
    enforce_threshold,
    flag_orphan_logs,
    quality_report,
    split_valid_quarantine,
)
from src.schemas import LOGS_SCHEMA, TRANSACTIONS_SCHEMA

VALID_TXN = ("TXN-0000001", datetime(2025, 11, 15, 10, 0), "BOG-001", "LAC-1001", 2, Decimal("10.50"), None)


def txn_row(**overrides):
    fields = ["transaction_id", "timestamp", "store_id", "product_id", "quantity", "unit_price", "_corrupt_record"]
    row = dict(zip(fields, VALID_TXN))
    row.update(overrides)
    return tuple(row[f] for f in fields)


def errors_of(spark, config, rows):
    df = spark.createDataFrame(rows, TRANSACTIONS_SCHEMA)
    flagged = apply_rules(df, TRANSACTION_RULES, config)
    return [set(r[ERRORS_COL]) for r in flagged.orderBy("unit_price").collect()]


def test_transaccion_valida_no_tiene_errores(spark, config):
    assert errors_of(spark, config, [txn_row()]) == [set()]


@pytest.mark.parametrize("overrides, expected_rule", [
    ({"quantity": 0}, "txn_cantidad_no_positiva"),
    ({"quantity": -3}, "txn_cantidad_no_positiva"),
    ({"unit_price": Decimal("-4.20")}, "txn_precio_no_positivo"),
    ({"transaction_id": ""}, "txn_id_faltante"),
    ({"transaction_id": None}, "txn_id_faltante"),
    ({"transaction_id": "ABC-1"}, "txn_id_formato_invalido"),
    ({"store_id": "BOG-999"}, "txn_tienda_fuera_de_catalogo"),
    ({"product_id": "XXX-0000"}, "txn_producto_fuera_de_catalogo"),
    ({"timestamp": datetime(2027, 1, 5)}, "txn_timestamp_fuera_de_rango"),
    ({"timestamp": None}, "txn_timestamp_faltante"),
    ({"quantity": None}, "txn_cantidad_faltante"),
    ({"unit_price": None}, "txn_precio_faltante"),
    ({"_corrupt_record": "basura,,"}, "txn_registro_corrupto"),
])
def test_cada_error_se_detecta(spark, config, overrides, expected_rule):
    [errors] = errors_of(spark, config, [txn_row(**overrides)])
    assert expected_rule in errors


def test_duplicados_se_marcan_ambos(spark, config):
    rows = [txn_row(unit_price=Decimal("1.00")), txn_row(unit_price=Decimal("2.00"))]
    assert all("txn_id_duplicado" in e for e in errors_of(spark, config, rows))


def test_advertencia_no_envia_a_cuarentena(spark, config):
    df = spark.createDataFrame([txn_row(quantity=5000)], TRANSACTIONS_SCHEMA)
    flagged = apply_rules(df, TRANSACTION_RULES, config)
    valid, quarantine = split_valid_quarantine(flagged)
    assert valid.count() == 1 and quarantine.count() == 0
    assert "txn_cantidad_atipica" in valid.first()[WARNINGS_COL]


def test_split_separa_validos_y_cuarentena(spark, config):
    rows = [txn_row(), txn_row(transaction_id="TXN-0000002", quantity=-1)]
    flagged = apply_rules(spark.createDataFrame(rows, TRANSACTIONS_SCHEMA), TRANSACTION_RULES, config)
    valid, quarantine = split_valid_quarantine(flagged)
    assert valid.count() == 1
    assert quarantine.count() == 1
    assert "dq_checked_at" in quarantine.columns
    assert "_corrupt_record" not in valid.columns


def test_logs_huerfanos_y_sin_usuario(spark, config):
    txns = spark.createDataFrame([txn_row()], TRANSACTIONS_SCHEMA)
    logs = spark.createDataFrame([
        ("EVT-1", "TXN-0000001", "purchase", "USR-0001", "mobile", None),
        ("EVT-2", "TXN-ORPHAN-0000009", "view", "USR-0002", "desktop", None),
        ("EVT-3", "TXN-0000001", "view", None, "tablet", None),
        ("EVT-4", "TXN-0000001", "refund", "USR-0003", "mobile", None),
    ], LOGS_SCHEMA)
    flagged = flag_orphan_logs(apply_rules(logs, LOG_RULES, config), txns)
    result = {r["event_id"]: (set(r[ERRORS_COL]), set(r[WARNINGS_COL])) for r in flagged.collect()}

    assert result["EVT-1"] == (set(), set())
    assert "log_transaccion_huerfana" in result["EVT-2"][0]
    assert result["EVT-3"] == (set(), {"log_usuario_faltante"})
    assert "log_tipo_evento_invalido" in result["EVT-4"][0]


def test_reporte_y_umbral(spark, config):
    rows = [txn_row(transaction_id=f"TXN-000000{i}", quantity=-1 if i < 3 else 1) for i in range(1, 6)]
    flagged = apply_rules(spark.createDataFrame(rows, TRANSACTIONS_SCHEMA), TRANSACTION_RULES, config)
    report = quality_report(flagged, TRANSACTION_RULES)

    assert report["total"] == 5 and report["rechazados"] == 2
    regla = next(r for r in report["reglas"] if r["regla"] == "txn_cantidad_no_positiva")
    assert regla["violaciones"] == 2
    with pytest.raises(DataQualityError):  # 40 % > 15 % permitido
        enforce_threshold(report, config, "transacciones")
