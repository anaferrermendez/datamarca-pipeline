"""
Pruebas de rendimiento del pipeline DATAMARCA.

Para cada volumen: genera el dataset (semilla fija), ejecuta `python -m src.pipeline`
en un proceso limpio, lee los tiempos por etapa y estima el costo en cloud.
Guarda reports/benchmark_rendimiento.json y reports/benchmark_rendimiento.md.

Uso:
    python -m src.benchmark                          # volumenes por defecto
    python -m src.benchmark --rows 5000 50000 500000
    python -m src.benchmark --nodes 3 --node-hourly-usd 0.24

Costo estimado = (tiempo de ejecucion + arranque del cluster) x nodos x tarifa por nodo-hora.
Las tarifas son SUPUESTOS ajustables por parametro (no se consultan a la API del proveedor);
el valor por defecto corresponde a una instancia tipo m5.xlarge + recargo de EMR en AWS
bajo demanda y debe verificarse contra la calculadora de precios vigente.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SEED = 134340


def estimate_cost(seconds: float, nodes: int, node_hourly_usd: float, startup_seconds: float) -> float:
    billed_hours = (seconds + startup_seconds) / 3600
    return round(billed_hours * nodes * node_hourly_usd, 6)


def run_volume(rows: int, work: Path) -> dict:
    data_dir, out_dir = work / f"data_{rows}", work / f"out_{rows}"
    shutil.rmtree(out_dir, ignore_errors=True)
    subprocess.run([sys.executable, str(PROJECT_ROOT / "data" / "generate_sample.py"),
                    "--rows", str(rows), "--seed", str(SEED), "--output-dir", str(data_dir)],
                   check=True, capture_output=True)
    size_mb = sum(p.stat().st_size for p in data_dir.glob("*") if p.is_file()) / 1e6

    env = {**os.environ, "DATAMARCA_INPUT_DIR": str(data_dir), "DATAMARCA_OUTPUT_DIR": str(out_dir)}
    start = time.perf_counter()
    proc = subprocess.run([sys.executable, "-m", "src.pipeline"], cwd=PROJECT_ROOT,
                          env=env, capture_output=True, text=True)
    wall = time.perf_counter() - start

    metrics = json.loads((PROJECT_ROOT / "reports" / "pipeline_metricas.json").read_text(encoding="utf-8"))
    ing, tra = metrics.get("ingesta", {}), metrics.get("transformaciones", {})
    output_mb = sum(p.stat().st_size for p in out_dir.rglob("*") if p.is_file()) / 1e6
    total = metrics.get("tiempo_total_segundos", wall)
    return {
        "filas_transacciones": rows,
        "tamano_entrada_mb": round(size_mb, 2),
        "tamano_salida_mb": round(output_mb, 2),
        "exit_code": proc.returncode,
        "estado": metrics.get("estado"),
        "tiempo_ingesta_s": ing.get("tiempos_segundos", {}).get("total"),
        "tiempo_transformacion_s": tra.get("tiempos_segundos", {}).get("total"),
        "tiempo_total_pipeline_s": total,
        "tiempo_proceso_completo_s": round(wall, 3),
        "throughput_filas_por_s": round(rows / total, 1) if total else None,
        "detalle_etapas": {"ingesta": ing.get("tiempos_segundos"),
                           "transformaciones": tra.get("tiempos_segundos")},
    }


def write_markdown(path: Path, result: dict) -> None:
    a = result["supuestos"]
    lines = ["# Resultados de pruebas de rendimiento", "",
             f"Entorno de medicion: {a['entorno_medicion']}", "",
             f"Supuestos de costo: {a['nodos']} nodos x USD {a['tarifa_nodo_hora_usd']}/h, "
             f"arranque del cluster {a['arranque_cluster_s']} s facturado.", "",
             "| Filas | Entrada (MB) | Ingesta (s) | Transf. (s) | Total (s) | Filas/s | Costo estimado (USD) |",
             "|---:|---:|---:|---:|---:|---:|---:|"]
    for r in result["corridas"]:
        lines.append(f"| {r['filas_transacciones']:,} | {r['tamano_entrada_mb']} | {r['tiempo_ingesta_s']} | "
                     f"{r['tiempo_transformacion_s']} | {r['tiempo_total_pipeline_s']} | "
                     f"{r['throughput_filas_por_s']} | {r['costo_estimado_usd']} |")
    lines += ["", "## Comportamiento ante incrementos de volumen", ""]
    for c in result["escalamiento"]:
        lines.append(f"- {c['de_filas']:,} a {c['a_filas']:,} filas (x{c['factor_volumen']}): "
                     f"el tiempo crece x{c['factor_tiempo']}.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rows", type=int, nargs="+", default=[5000, 50000, 250000])
    ap.add_argument("--nodes", type=int, default=3)
    ap.add_argument("--node-hourly-usd", type=float, default=0.24,
                    help="Tarifa por nodo-hora (supuesto; verificar con el proveedor)")
    ap.add_argument("--startup-seconds", type=float, default=300,
                    help="Segundos de arranque del cluster que tambien se facturan")
    ap.add_argument("--label", default=f"{os.cpu_count()} vCPU, local[*]")
    args = ap.parse_args()

    runs = []
    with tempfile.TemporaryDirectory() as tmp:
        for rows in sorted(args.rows):
            print(f"== Volumen {rows:,} filas ==", flush=True)
            r = run_volume(rows, Path(tmp))
            if r["exit_code"] != 0:
                print(f"   fallo (exit {r['exit_code']})")
            r["costo_estimado_usd"] = estimate_cost(r["tiempo_total_pipeline_s"] or 0, args.nodes,
                                                    args.node_hourly_usd, args.startup_seconds)
            runs.append(r)
            print(f"   total {r['tiempo_total_pipeline_s']} s | {r['throughput_filas_por_s']} filas/s", flush=True)

    scaling = []
    for prev, cur in zip(runs, runs[1:]):
        if prev["tiempo_total_pipeline_s"] and cur["tiempo_total_pipeline_s"]:
            scaling.append({"de_filas": prev["filas_transacciones"], "a_filas": cur["filas_transacciones"],
                            "factor_volumen": round(cur["filas_transacciones"] / prev["filas_transacciones"], 1),
                            "factor_tiempo": round(cur["tiempo_total_pipeline_s"] / prev["tiempo_total_pipeline_s"], 2)})

    result = {"supuestos": {"entorno_medicion": args.label, "nodos": args.nodes,
                            "tarifa_nodo_hora_usd": args.node_hourly_usd,
                            "arranque_cluster_s": args.startup_seconds, "semilla": SEED},
              "corridas": runs, "escalamiento": scaling}
    reports = PROJECT_ROOT / "reports"
    reports.mkdir(exist_ok=True)
    (reports / "benchmark_rendimiento.json").write_text(json.dumps(result, ensure_ascii=False, indent=2),
                                                        encoding="utf-8")
    write_markdown(reports / "benchmark_rendimiento.md", result)
    print("Reporte en reports/benchmark_rendimiento.md")
    return 0 if all(r["exit_code"] == 0 for r in runs) else 1


if __name__ == "__main__":
    raise SystemExit(main())
