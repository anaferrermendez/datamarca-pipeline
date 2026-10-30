"""
Perfil de calidad del dataset de DATAMARCA.

Lee las dos fuentes con sus esquemas explícitos, aplica todas las reglas de calidad
e imprime cuántos registros viola cada una. El resultado también se guarda en
reports/calidad_datos.json como evidencia para el informe técnico.

Uso:
    python -m src.data_quality_report
"""

import json
import time
from pathlib import Path

from src.config import PROJECT_ROOT, create_spark_session, load_config
from src.quality_rules import (
    LOG_RULES,
    ORPHAN_LOG_RULE,
    TRANSACTION_RULES,
    apply_rules,
    flag_orphan_logs,
    log_coverage,
    quality_report,
    split_valid_quarantine,
)
from src.ingestion import read_logs, read_transactions


def print_report(title: str, report: dict) -> None:
    print(f"\n=== {title} ===")
    print(f"Total registros:   {report['total']:>10,}")
    print(f"Rechazados:        {report['rechazados']:>10,}  ({report['tasa_rechazo']:.2%})")
    print(f"Con advertencias:  {report['con_advertencias']:>10,}")
    print(f"\n{'Regla':<34}{'Dimensión':<14}{'Severidad':<11}{'Casos':>8}{'%':>8}")
    print("-" * 75)
    for r in report["reglas"]:
        print(f"{r['regla']:<34}{r['dimension']:<14}{r['severidad']:<11}"
              f"{r['violaciones']:>8,}{r['porcentaje']:>8.2f}")


def main() -> None:
    config = load_config()
    spark = create_spark_session(config)
    start = time.perf_counter()

    transactions = read_transactions(spark, config)
    logs = read_logs(spark, config)

    # cache: los DataFrames marcados se usan varias veces (reporte, split, cobertura)
    txn_flagged = apply_rules(transactions, TRANSACTION_RULES, config).cache()
    logs_flagged = flag_orphan_logs(
        apply_rules(logs, LOG_RULES, config), transactions
    ).cache()

    txn_report = quality_report(txn_flagged, TRANSACTION_RULES)
    logs_report = quality_report(logs_flagged, LOG_RULES + [ORPHAN_LOG_RULE])

    txn_valid, _ = split_valid_quarantine(txn_flagged)
    logs_valid, _ = split_valid_quarantine(logs_flagged)
    coverage = log_coverage(txn_valid, logs_valid)

    elapsed = time.perf_counter() - start

    print_report("Transacciones (CSV)", txn_report)
    print_report("Logs e-commerce (JSON)", logs_report)
    print("\n=== Cobertura de logs (informativo) ===")
    print(f"Transacciones válidas sin log: {coverage['sin_log']:,} "
          f"({coverage['porcentaje_sin_log']:.2f} %)")
    print(f"\nTiempo de evaluación de calidad: {elapsed:.2f} s")

    output = Path(config.reports_dir) / "calidad_datos.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w", encoding="utf-8") as f:
        json.dump(
            {
                "transacciones": txn_report,
                "logs": logs_report,
                "cobertura_logs": coverage,
                "tiempo_segundos": round(elapsed, 2),
            },
            f, ensure_ascii=False, indent=2,
        )
    print(f"Reporte guardado en {output.relative_to(PROJECT_ROOT)}")

    spark.stop()


if __name__ == "__main__":
    main()
