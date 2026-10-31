"""
Resume las corridas de reports/gcp_batches/*.json (salida de `gcloud dataproc batches describe`)
en reports/gcp_resultados.md, con duracion y COSTO CALCULADO a partir del consumo real en DCU.

Costo = DCU-horas consumidas x tarifa por DCU-hora (parametro --dcu-hour-usd).
Verifique la tarifa vigente en la pagina de precios de Google Cloud antes de citarla.
"""
import argparse
import json
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def parse_time(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dcu-hour-usd", type=float, default=0.06,
                    help="USD por DCU-hora (Standard, us-central1; VERIFICAR)")
    args = ap.parse_args()
    rows = []
    for path in sorted((ROOT / "reports" / "gcp_batches").glob("*.json")):
        b = json.loads(path.read_text(encoding="utf-8"))
        usage = b.get("runtimeInfo", {}).get("approximateUsage", {})
        dcu_h = float(usage.get("milliDcuSeconds", 0)) / 1000 / 3600
        props = b.get("runtimeConfig", {}).get("properties", {})
        seconds = (parse_time(b["stateTime"]) - parse_time(b["createTime"])).total_seconds()
        name = b["name"].split("/")[-1]
        rows.append((name, b.get("state"), props.get("spark:spark.executor.instances", "?"),
                     round(seconds, 1), round(dcu_h, 4), round(dcu_h * args.dcu_hour_usd, 4)))
    lines = ["# Resultados en Google Cloud", "", f"Tarifa usada: USD {args.dcu_hour_usd} por DCU-hora.", "",
             "| Batch | Estado | Executors | Duracion total (s) | DCU-horas | Costo (USD) |", "|---|---|---:|---:|---:|---:|"]
    lines += [f"| {n} | {s} | {e} | {d} | {h} | {c} |" for n, s, e, d, h, c in rows]
    out = ROOT / "reports" / "gcp_resultados.md"
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(out.read_text(encoding="utf-8"))


if __name__ == "__main__":
    main()
