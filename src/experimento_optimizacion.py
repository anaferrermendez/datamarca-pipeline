"""
Experimento de optimización: compara cada buena práctica contra su alternativa "ingenua".

Cada variante ejecuta el mismo cálculo con una acción `noop` (ejecuta todo el plan sin escribir
a disco), se repite N veces y se reporta la mediana. Requiere datos validados:
    python -m src.pipeline            (o python -m src.ingestion)
    python -m src.experimento_optimizacion --repeticiones 3

Experimentos:
  1. broadcast_join      Dimensiones con F.broadcast  vs.  sort-merge join (sin broadcast)
  2. cache               .cache() en ventas enriquecidas (3 agregaciones)  vs.  recalcular
  3. partition_pruning   Filtro sobre year/month  vs.  filtro sobre una expresión de timestamp
  4. column_pruning      select de 2 columnas  vs.  select *
  5. nativo_vs_udf       Función nativa substring_index  vs.  UDF de Python
Salidas: reports/experimento_optimizacion.json y .md
"""

import argparse
import json
import statistics
import time
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from src.config import create_spark_session, load_config
from src.transformations import (
    build_dimensions, enrich_transactions, read_validated, sales_by_category_country,
    sales_by_channel, sales_by_store_month,
)


def run_noop(df) -> None:
    df.write.format("noop").mode("overwrite").save()


def measure(fn, repeats: int) -> dict:
    fn()  # calentamiento: descarta el costo de arranque de JIT y metadatos
    times = []
    for _ in range(repeats):
        start = time.perf_counter()
        fn()
        times.append(time.perf_counter() - start)
    return {"mediana_s": round(statistics.median(times), 3), "min_s": round(min(times), 3),
            "max_s": round(max(times), 3), "repeticiones": repeats}


def pair(spark, name, description, optimized, baseline, repeats, setup_baseline=None, teardown=None):
    result = {"experimento": name, "descripcion": description}
    result["optimizado"] = measure(optimized, repeats)
    if setup_baseline:
        setup_baseline()
    try:
        result["ingenuo"] = measure(baseline, repeats)
    finally:
        if teardown:
            teardown()
    o, b = result["optimizado"]["mediana_s"], result["ingenuo"]["mediana_s"]
    result["mejora_x"] = round(b / o, 2) if o else None
    print(f"  {name:<18} optimizado {o:>7.3f}s | ingenuo {b:>7.3f}s | x{result['mejora_x']}", flush=True)
    return result


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repeticiones", type=int, default=3)
    args = ap.parse_args(argv)

    config = load_config()
    spark = create_spark_session(config)
    results = []
    try:
        transactions, logs = read_validated(spark, config)
        stores, categories = build_dimensions(spark, config)
        n_rows = transactions.count()
        print(f"Filas validadas: {n_rows:,}")

        # 1. Broadcast vs sort-merge ------------------------------------------------
        def enrich_plain():
            sales = (
                transactions.withColumn("line_total", F.col("quantity") * F.col("unit_price"))
                .withColumn("category_code", F.substring_index("product_id", "-", 1))
                .join(stores, "store_id", "left").join(categories, "category_code", "left")
            )
            run_noop(sales)

        def enrich_broadcast():
            sales = (
                transactions.withColumn("line_total", F.col("quantity") * F.col("unit_price"))
                .withColumn("category_code", F.substring_index("product_id", "-", 1))
                .join(F.broadcast(stores), "store_id", "left").join(F.broadcast(categories), "category_code", "left")
            )
            run_noop(sales)

        def no_broadcast_on():
            spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
            spark.conf.set("spark.sql.adaptive.enabled", "false")

        def no_broadcast_off():
            spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
            spark.conf.set("spark.sql.adaptive.enabled", "true")

        results.append(pair(spark, "broadcast_join",
                            "Joins con dimensiones pequeñas: BroadcastHashJoin vs. SortMergeJoin",
                            enrich_broadcast, enrich_plain, args.repeticiones, no_broadcast_on, no_broadcast_off))

        # 2. Cache -------------------------------------------------------------------
        def aggregations(sales):
            run_noop(sales_by_store_month(sales))
            run_noop(sales_by_category_country(spark, sales))
            run_noop(sales_by_channel(sales))

        def with_cache():
            sales = enrich_transactions(transactions, logs, stores, categories).cache()
            sales.count()
            try:
                aggregations(sales)
            finally:
                sales.unpersist()

        def without_cache():
            aggregations(enrich_transactions(transactions, logs, stores, categories))

        results.append(pair(spark, "cache",
                            "Ventas enriquecidas reutilizadas por 3 agregaciones: con .cache() vs. recalcular",
                            with_cache, without_cache, args.repeticiones))

        # 3. Partition pruning ------------------------------------------------------
        base = spark.read.parquet(config.validated_path("transacciones"))

        def pruned():
            run_noop(base.where((F.col("year") == 2026) & (F.col("month") == 1))
                     .groupBy("store_id").agg(F.sum("unit_price")))

        def not_pruned():
            run_noop(base.where((F.year("timestamp") == 2026) & (F.month("timestamp") == 1))
                     .groupBy("store_id").agg(F.sum("unit_price")))

        results.append(pair(spark, "partition_pruning",
                            "Filtro sobre columnas de partición (year/month) vs. sobre función de timestamp",
                            pruned, not_pruned, args.repeticiones))

        # 4. Column pruning -----------------------------------------------------------
        def two_columns():
            run_noop(base.select("store_id", "unit_price").groupBy("store_id").agg(F.sum("unit_price")))

        def all_columns():
            run_noop(base.groupBy("store_id").agg(F.sum("unit_price"), *[F.max(c) for c in base.columns
                                                                        if c not in ("store_id", "unit_price")]))

        results.append(pair(spark, "column_pruning",
                            "Leer solo las columnas necesarias vs. tocar todas las columnas",
                            two_columns, all_columns, args.repeticiones))

        # 5. Nativo vs UDF ------------------------------------------------------------
        category_udf = F.udf(lambda p: p.split("-")[0] if p else None, StringType())

        def native():
            run_noop(transactions.withColumn("c", F.substring_index("product_id", "-", 1)).groupBy("c").count())

        def python_udf():
            run_noop(transactions.withColumn("c", category_udf("product_id")).groupBy("c").count())

        results.append(pair(spark, "nativo_vs_udf",
                            "Función nativa de Spark vs. UDF de Python (el optimizador no ve dentro de la UDF)",
                            native, python_udf, args.repeticiones))
    finally:
        spark.stop()

    out = {"filas_validadas": n_rows, "spark_master": config.spark_master, "resultados": results}
    reports = Path(config.reports_dir)
    reports.mkdir(parents=True, exist_ok=True)
    (reports / "experimento_optimizacion.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = ["# Experimento de optimización", "", f"Filas validadas: {n_rows:,}. Mediana de cada variante (s).", "",
             "| Experimento | Optimizado (s) | Ingenuo (s) | Mejora |", "|---|---:|---:|---:|"]
    for r in results:
        lines.append(f"| {r['experimento']} | {r['optimizado']['mediana_s']} | {r['ingenuo']['mediana_s']} | x{r['mejora_x']} |")
    lines += ["", *[f"- **{r['experimento']}**: {r['descripcion']}" for r in results]]
    (reports / "experimento_optimizacion.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("Reporte en reports/experimento_optimizacion.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
