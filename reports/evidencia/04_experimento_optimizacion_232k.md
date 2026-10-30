# Experimento de optimización

Filas validadas: 232,464. Mediana de cada variante (s).

| Experimento | Optimizado (s) | Ingenuo (s) | Mejora |
|---|---:|---:|---:|
| broadcast_join | 1.155 | 1.959 | x1.7 |
| cache | 4.26 | 3.421 | x0.8 |
| partition_pruning | 0.19 | 0.32 | x1.68 |
| column_pruning | 0.24 | 0.516 | x2.15 |
| nativo_vs_udf | 0.19 | 0.603 | x3.17 |

- **broadcast_join**: Joins con dimensiones pequeñas: BroadcastHashJoin vs. SortMergeJoin
- **cache**: Ventas enriquecidas reutilizadas por 3 agregaciones: con .cache() vs. recalcular
- **partition_pruning**: Filtro sobre columnas de partición (year/month) vs. sobre función de timestamp
- **column_pruning**: Leer solo las columnas necesarias vs. tocar todas las columnas
- **nativo_vs_udf**: Función nativa de Spark vs. UDF de Python (el optimizador no ve dentro de la UDF)
