"""Pruebas de las transformaciones: enriquecimiento, canal, agregaciones y conciliación."""

from decimal import Decimal
from pathlib import Path

from src.ingestion import run_ingestion
from src.transformations import run_transformations
from tests.test_ingestion import GOOD_ROWS, LOGS, make_config, write_inputs


def run_both(spark, config, tmp_path):
    write_inputs(tmp_path / "in", GOOD_ROWS, LOGS)
    cfg = make_config(config, tmp_path, max_rejection_rate=0.5)
    assert run_ingestion(spark, cfg).status == "exitosa"
    return cfg, run_transformations(spark, cfg)


def test_transformaciones_generan_tablas_y_concilian(spark, config, tmp_path):
    cfg, result = run_both(spark, config, tmp_path)

    assert result.status == "exitosa"
    assert result.reconciliation["cuadra"]
    # 2*10.50 + 1*8.00 + 3*12.00 + 5*4.25 = 86.25
    assert Decimal(result.reconciliation["ingresos_detalle"]) == Decimal("86.25")
    assert result.row_counts["ventas_enriquecidas"] == 4
    assert (Path(cfg.curated_path("ventas_tienda_mes")) / "year=2025" / "month=10").is_dir()
    assert Path(cfg.reports_dir, "plan_catalyst.txt").is_file()


def test_enriquecimiento_ciudad_pais_categoria(spark, config, tmp_path):
    cfg, _ = run_both(spark, config, tmp_path)
    rows = {r["store_id"]: r for r in spark.read.parquet(cfg.curated_path("ventas_tienda_mes")).collect()}

    assert rows["BOG-001"]["city"] == "Bogotá" and rows["BOG-001"]["country"] == "Colombia"
    assert rows["BOG-001"]["ingresos"] == Decimal("21.00")
    assert rows["CDMX-001"]["country"] == "México"

    categories = {r["category"] for r in spark.read.parquet(cfg.curated_path("ventas_categoria_pais")).collect()}
    assert categories == {"Lácteos", "Bebidas", "Abarrotes", "Frutas y verduras"}


def test_canal_segun_logs(spark, config, tmp_path):
    cfg, _ = run_both(spark, config, tmp_path)
    channels = spark.read.parquet(cfg.curated_path("ventas_canal"))

    # TXN-1 y TXN-2 tienen eventos -> ecommerce; TXN-3 y TXN-4 no -> tienda física
    totals = {r["channel"]: r["transacciones"] for r in
              channels.groupBy("channel").sum("transacciones")
              .withColumnRenamed("sum(transacciones)", "transacciones").collect()}
    assert totals == {"ecommerce": 2, "tienda_fisica": 2}

    # La participación de cada mes suma 100 %
    for r in channels.groupBy("year", "month").sum("participacion_pct").collect():
        assert abs(r["sum(participacion_pct)"] - 100) < 0.01


def test_embudo_por_dispositivo(spark, config, tmp_path):
    cfg, _ = run_both(spark, config, tmp_path)
    funnel = {r["device"]: r for r in spark.read.parquet(cfg.curated_path("embudo_conversion")).collect()}

    # EVT-3 es huérfano y no llega a los válidos
    assert set(funnel) == {"mobile", "desktop"}
    assert funnel["mobile"]["compras"] == 1
    assert funnel["desktop"]["vistas"] == 1
    assert funnel["desktop"]["tasa_carrito_a_compra_pct"] is None  # sin carritos: no divide por cero
