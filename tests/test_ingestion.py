"""Pruebas de punta a punta de la ingesta sobre archivos pequeños en una carpeta temporal."""

import json
from dataclasses import replace
from pathlib import Path

from src.ingestion import run_ingestion

HEADER = "transaction_id,timestamp,store_id,product_id,quantity,unit_price\n"
GOOD_ROWS = [
    "TXN-0000001,2025-10-05 10:00:00,BOG-001,LAC-1001,2,10.50",
    "TXN-0000002,2025-11-20 12:30:00,MED-001,BEB-2001,1,8.00",
    "TXN-0000003,2026-01-15 09:15:00,LIM-001,ABR-3001,3,12.00",
    "TXN-0000004,2026-03-02 18:45:00,CDMX-001,FRU-5001,5,4.25",
]


def write_inputs(folder: Path, txn_rows, logs):
    folder.mkdir(parents=True)
    (folder / "transacciones.csv").write_text(HEADER + "\n".join(txn_rows) + "\n", encoding="utf-8")
    (folder / "logs_ecommerce.json").write_text(
        "".join(json.dumps(e) + "\n" for e in logs), encoding="utf-8"
    )


def make_config(config, tmp_path, **overrides):
    return replace(
        config,
        input_dir=str(tmp_path / "in"),
        output_dir=str(tmp_path / "out"),
        reports_dir=str(tmp_path / "reports"),
        **overrides,
    )


LOGS = [
    {"event_id": "EVT-1", "transaction_id": "TXN-0000001", "event_type": "purchase", "user_id": "USR-0001", "device": "mobile"},
    {"event_id": "EVT-2", "transaction_id": "TXN-0000002", "event_type": "view", "user_id": None, "device": "desktop"},
    {"event_id": "EVT-3", "transaction_id": "TXN-ORPHAN-0000099", "event_type": "view", "user_id": "USR-0002", "device": "tablet"},
]


def test_ingesta_exitosa_escribe_validos_y_cuarentena(spark, config, tmp_path):
    rows = GOOD_ROWS + ["TXN-0000005,2025-12-01 08:00:00,BOG-999,LAC-1001,1,5.00"]
    write_inputs(tmp_path / "in", rows, LOGS)
    cfg = make_config(config, tmp_path, max_rejection_rate=0.5)

    result = run_ingestion(spark, cfg)

    assert result.status == "exitosa"
    valid = spark.read.parquet(cfg.validated_path("transacciones"))
    quarantine = spark.read.parquet(cfg.quarantine_path("transacciones"))
    assert valid.count() == 4
    assert quarantine.count() == 1
    assert quarantine.first()["dq_errors"] == ["txn_tienda_fuera_de_catalogo"]

    # Particionado por year/month y metadatos de trazabilidad
    assert (Path(cfg.validated_path("transacciones")) / "year=2026" / "month=3").is_dir()
    assert {"year", "month", "ingestion_run_id", "ingested_at"} <= set(valid.columns)
    assert valid.select("ingestion_run_id").distinct().first()[0] == result.run_id

    logs_valid = spark.read.parquet(cfg.validated_path("logs"))
    assert logs_valid.count() == 2  # EVT-3 es huérfano
    assert {"lectura_y_validacion", "escritura_cuarentena", "escritura_validos", "total"} <= set(result.timings)


def test_ingesta_se_detiene_si_supera_umbral(spark, config, tmp_path):
    bad = [f"TXN-000001{i},2025-12-01 08:00:00,BOG-001,LAC-1001,-1,5.00" for i in range(3)]
    write_inputs(tmp_path / "in", GOOD_ROWS[:1] + bad, LOGS[:1])
    cfg = make_config(config, tmp_path)  # umbral 15 %, aquí se rechaza el 75 %

    result = run_ingestion(spark, cfg)

    assert result.status == "detenida_por_calidad"
    assert "transacciones" in result.error
    assert Path(cfg.quarantine_path("transacciones")).is_dir()      # se audita
    assert not Path(cfg.validated_path("transacciones")).exists()   # no se publica
