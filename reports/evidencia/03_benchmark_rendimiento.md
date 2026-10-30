# Resultados de pruebas de rendimiento

Entorno de medicion: sandbox Linux, 1 vCPU, 3 GB RAM, local[*]

Supuestos de costo: 3 nodos x USD 0.24/h, arranque del cluster 300 s facturado.

| Filas | Entrada (MB) | Ingesta (s) | Transf. (s) | Total (s) | Filas/s | Costo estimado (USD) |
|---:|---:|---:|---:|---:|---:|---:|
| 5,000 | 0.85 | 19.754 | 10.96 | 30.744 | 162.6 | 0.066149 |
| 50,000 | 8.45 | 24.659 | 11.501 | 36.196 | 1381.4 | 0.067239 |
| 250,000 | 42.21 | 36.522 | 14.294 | 50.847 | 4916.7 | 0.070169 |

## Comportamiento ante incrementos de volumen

- 5,000 a 50,000 filas (x10.0): el tiempo crece x1.18.
- 50,000 a 250,000 filas (x5.0): el tiempo crece x1.4.
