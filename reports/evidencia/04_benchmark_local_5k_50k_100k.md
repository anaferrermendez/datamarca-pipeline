# Resultados de pruebas de rendimiento

Entorno de medicion: 20 vCPU, local[*]

Supuestos de costo: 3 nodos x USD 0.24/h, arranque del cluster 300 s facturado.

| Filas | Entrada (MB) | Ingesta (s) | Transf. (s) | Total (s) | Filas/s | Costo estimado (USD) |
|---:|---:|---:|---:|---:|---:|---:|
| 5,000 | 0.85 | 7.123 | 33.917 | 41.047 | 121.8 | 0.068209 |
| 50,000 | 8.49 | 8.707 | 34.947 | 43.663 | 1145.1 | 0.068733 |
| 100,000 | 16.97 | 9.399 | 35.819 | 45.227 | 2211.1 | 0.069045 |

## Comportamiento ante incrementos de volumen

- 5,000 a 50,000 filas (x10.0): el tiempo crece x1.06.
- 50,000 a 100,000 filas (x2.0): el tiempo crece x1.04.
