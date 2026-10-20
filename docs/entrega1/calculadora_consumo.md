### Escenario base: lectura cada 30 min

| Capa | Servicio | Dimensionamiento | USD primer año |
|---|---|---|---:|
| Ingesta | Managed Service for Apache Kafka | 3 vCPU, RF=3, 7 días; 0.4 MB/s prom., 4 MB/s pico | 3,238 |
| Cómputo | Spark Structured Streaming (Dataproc, 24/7) | 3 nodos n2-standard-4 | 6,155 |
| Cómputo | Spark batch serverless (diario) | 384 M lecturas/día × 0.064 DCU-h/M | 701 |
| Almacenamiento | Cloud Storage (lake raw + curated dual-region) | 3.7 TB al cierre del año | 641 |
| Almacenamiento | BigQuery almacenamiento | detalle 30 min + 15 % de agregados, crecimiento lineal | 828 |
| Almacenamiento | Bigtable (serving app, 2 clústeres) | 2 nodos, 251 GiB SSD | 11,899 |
| Exposición | BigQuery consultas (OLAP / Looker Studio) | 10 TiB/mes on-demand (1 TiB gratis) | 675 |
| Exposición | Cloud Run (API REST/WebSocket) | 30 M requests/mes | 684 |
| Transversal | Managed Service for Apache Airflow (Composer 3) | entorno pequeño | 3,000 |
| Transversal | Logging, Monitoring y egreso | 200 GiB logs/mes, 150 GB egreso/mes | 1,416 |
| **Total** | | | **29,238** |

| Métrica unitaria | Valor |
|---|---:|
| Lecturas por día | 384 M |
| Costo por medidor al mes | US$0.0003 |
| Costo por millón de lecturas procesadas | US$0.208 |
| Costo de plataforma por MWh distribuido (30.1 TWh/año) | US$0.0010 |

### Escenario de crecimiento: lectura cada 1 min

| Capa | Servicio | Dimensionamiento | USD primer año |
|---|---|---|---:|
| Ingesta | Managed Service for Apache Kafka | 9 vCPU, RF=3, 7 días; 13.3 MB/s prom., 134 MB/s pico | 30,273 |
| Cómputo | Spark Structured Streaming (Dataproc, 24/7) | 10 nodos n2-standard-4 | 20,516 |
| Cómputo | Spark batch serverless (diario) | 11,534 M lecturas/día × 0.064 DCU-h/M | 21,017 |
| Almacenamiento | Cloud Storage (lake raw + curated dual-region) | 110.3 TB al cierre del año | 19,243 |
| Almacenamiento | BigQuery almacenamiento | detalle 1 min solo 90 días + agregados 30 min | 10,667 |
| Almacenamiento | Bigtable (serving app, 2 clústeres) | 6 nodos, 7,520 GiB SSD | 49,504 |
| Exposición | BigQuery consultas (OLAP / Looker Studio) | 10 TiB/mes on-demand (1 TiB gratis) | 675 |
| Exposición | Cloud Run (API REST/WebSocket) | 30 M requests/mes | 684 |
| Transversal | Managed Service for Apache Airflow (Composer 3) | entorno pequeño | 3,000 |
| Transversal | Logging, Monitoring y egreso | 200 GiB logs/mes, 150 GB egreso/mes | 1,416 |
| **Total** | | | **156,994** |

| Métrica unitaria | Valor |
|---|---:|
| Lecturas por día | 11,534 M |
| Costo por medidor al mes | US$0.0016 |
| Costo por millón de lecturas procesadas | US$0.037 |
| Costo de plataforma por MWh distribuido (30.1 TWh/año) | US$0.0052 |
