import argparse
import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

def parse_args():
    parser = argparse.ArgumentParser(description="Generador de dataset sintético DATAMARCA")
    parser.add_argument("--rows", type=int, default=5000,
                        help="Número de transacciones a generar (default: 5000)")
    parser.add_argument("--seed", type=int, default=134340,
                        help="Semilla para reproducibilidad (default: 134340)")
    parser.add_argument("--output-dir", type=str, default="data/sample",
                        help="Carpeta de salida (default: data/sample)")
    return parser.parse_args()

# Catálogos maestros: valores válidos del dominio
# Formato de store_id: <CIUDAD>-<NNN>  (ej. BOG-001 = Bogotá tienda 1)
TIENDAS = [
    "BOG-001", "BOG-002", "MED-001", "CAL-001",
    "CDMX-001", "GDL-001", "MTY-001",
    "LIM-001", "LIM-002",
]

# Formato de product_id: <CATEGORÍA>-<NNNN>
# LAC=Lácteos, BEB=Bebidas, ABR=Abarrotes, LIM=Limpieza, FRU=Frutas
PRODUCTOS = [
    "LAC-1001", "LAC-1002",
    "BEB-2001", "BEB-2002",
    "ABR-3001", "ABR-3002",
    "LIM-4001",
    "FRU-5001",
]

# Rango temporal del dataset: 6 meses cruzando año nuevo
FECHA_INICIO = datetime(2025, 10, 1)
FECHA_FIN = datetime(2026, 3, 31)

def generar_transacciones(num_rows, output_path):
    """
    Genera el CSV de transacciones con ~7% de errores controlados:
      - 2% quantity <= 0
      - 2% unit_price <= 0
      - 1% transaction_id vacío
      - 1% store_id fuera de catálogo (BOG-999)
      - 1% timestamp futuro
    """
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "transaction_id", "timestamp", "store_id",
            "product_id", "quantity", "unit_price"
        ])

        for i in range(1, num_rows + 1):
            txn_id = f"TXN-{i:07d}"
            fecha = FECHA_INICIO + timedelta(
                seconds=random.randint(
                    0, int((FECHA_FIN - FECHA_INICIO).total_seconds())
                )
            )
            store_id = random.choice(TIENDAS)
            product_id = random.choice(PRODUCTOS)
            quantity = random.randint(1, 5)
            unit_price = round(random.uniform(5.0, 50.0), 2)

            # Inyección controlada de errores
            dado = random.random()
            if dado < 0.02:
                quantity = random.choice([0, -1, -3])
            elif dado < 0.04:
                unit_price = round(random.uniform(-10.0, 0.0), 2)
            elif dado < 0.05:
                txn_id = ""
            elif dado < 0.06:
                store_id = "BOG-999"  # respeta formato pero no existe
            elif dado < 0.07:
                fecha = datetime(2027, 1, 1) + timedelta(
                    days=random.randint(0, 30)
                )

            writer.writerow([
                txn_id,
                fecha.strftime("%Y-%m-%d %H:%M:%S"),
                store_id,
                product_id,
                quantity,
                unit_price,
            ])

def generar_logs(num_tx, output_path):
    """
    Genera logs de e-commerce en JSON.
      - 15% de transacciones sin log (simula POS físicos)
      - 10% de logs huérfanos (transaction_id no existe)
      - 2% de logs sin user_id
    """
    with open(output_path, "w", encoding="utf-8") as f:
        for i in range(1, num_tx + 1):
            # 15% sin log (POS físico)
            if random.random() < 0.15:
                continue

            # 10% logs huérfanos
            txn_ref = (
                f"TXN-ORPHAN-{i:07d}"
                if random.random() < 0.10
                else f"TXN-{i:07d}"
            )

            # 2% sin user_id
            user_id = (
                None if random.random() < 0.02
                else f"USR-{random.randint(1, 1000):04d}"
            )

            event = {
                "event_id": f"EVT-{i:07d}",
                "transaction_id": txn_ref,
                "event_type": random.choice(
                    ["view", "add_to_cart", "purchase"]
                ),
                "user_id": user_id,
                "device": random.choice(["mobile", "desktop", "tablet"]),
            }
            f.write(json.dumps(event) + "\n")


def main():
    args = parse_args()
    random.seed(args.seed)  # reproducibilidad

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Generando {args.rows} transacciones con semilla={args.seed}...")
    generar_transacciones(args.rows, output_dir / "transacciones.csv")

    print("Generando logs de e-commerce...")
    generar_logs(args.rows, output_dir / "logs_ecommerce.json")

    print(f"Dataset generado en {output_dir}/")
    print("   - transacciones.csv")
    print("   - logs_ecommerce.json")


if __name__ == "__main__":
    main()