# Arquitectura técnica · qué script corre dónde y para qué

> Generado con asistencia de IA (Claude). Mantener sincronizado al agregar o mover scripts.

La arquitectura se documenta en tres niveles:

| Nivel | Para qué sirve | Diagrama |
|---|---|---|
| Servicios (taller) | Qué servicios de GCP usa el prototipo | [diagrama_arquitectura.png](diagrama_arquitectura.png) |
| Servicios (producción, 4 capas) | Blueprint de la Entrega 1: Ingesta, Cómputo, Almacenamiento, Exposición | [entrega1/diagrama_capas.png](entrega1/diagrama_capas.png) |
| **Técnica** | Qué archivo del repositorio corre en qué máquina o servicio | Los dos diagramas de abajo |

Hay **dos caminos independientes**:

- **Batch** (histórico, una vez al día): Kaggle → GCS → Spark → BigQuery. Lo orquesta **Airflow**, o lo corren a mano los scripts `infra/05` y `infra/06`.
- **Tiempo real** (cada lectura, en segundos): medidores → **Kafka** → **Spark Structured Streaming** → alertas. Airflow **no** participa; en el taller los medidores se simulan con un *replay* de los datos de 2013.

## Camino batch

![Arquitectura técnica · batch](diagrama_tecnico_batch.png)

## Camino tiempo real

![Arquitectura técnica · tiempo real](diagrama_tecnico_streaming.png)

## Referencia de scripts

| Archivo | Dónde corre | Cuándo | Qué hace | Lee | Escribe |
|---|---|---|---|---|---|
| `infra/01_apis_presupuesto.sh` | Laptop | Una vez | Habilita APIs; crea el presupuesto con alertas | — | Billing Budget |
| `infra/02_buckets_bigquery.sh` | Laptop | Una vez | Crea buckets y datasets; Private Google Access | — | GCS, BigQuery |
| `infra/03_secreto_kaggle.sh` | Laptop (lo corre el alumno) | Una vez | Sube el token de Kaggle | `infra/config.env` | Secret Manager |
| `infra/04_ingesta_kaggle.sh` | Laptop | Una vez | Crea la VM efímera, espera y la borra | — | VM `lumina-ingest` |
| `src/ingestion/kaggle_to_gcs.sh` | VM `lumina-ingest` (startup script) | Una vez | Descarga los datasets de Kaggle | Kaggle, Secret Manager | `raw/kaggle/` |
| `infra/05_spark_batch.sh` | Laptop | Manual | Sube el código y envía `silver` o `gold`; registra el costo | `src/batch/` | Spark serverless, `bitacora_costos.log` |
| `src/batch/silver.py` | Managed Spark serverless | Por corrida batch | Tipado, UTC, banderas de calidad | `raw/kaggle/` | `curated/silver/` |
| `src/batch/gold.py` | Managed Spark serverless | Por corrida batch | Modelo estrella, agregados, perfiles, `dq_medidor` | `curated/silver/` | `curated/gold/` |
| `src/batch/lumina_common.py` | Spark serverless (dependencia) | — | Sesión Spark, argumentos, escritura de Parquet | — | — |
| `infra/06_cargar_bigquery.sh` | Laptop | Manual | Carga gold en BigQuery y crea las tablas derivadas | `curated/gold/`, `src/sql/gold_derivadas.sql` | `lumina_curated` |
| `src/sql/gold_derivadas.sql` | BigQuery | Tras cada carga | NOAA GSOD, `consumo_dia_anidado` (STRUCT/ARRAY), `umbral_zona` | `lumina_curated`, NOAA público | `lumina_curated` |
| `src/dags/lumina_batch_diario.py` | VM `lumina-ops`, contenedor Airflow (perfil `airflow`) | Manual en el taller; diario en producción | Orquesta: sensor → silver → gold → 13 cargas → derivadas → 3 controles de calidad | GCS | Spark serverless, BigQuery |
| `infra/07_vm_ops.sh` | Laptop | Por demo | Crea, opera y destruye la VM `lumina-ops`; dispara el replay y el DAG | — | VM, metadata |
| `infra/ops/vm_startup.sh` | VM `lumina-ops` (cada arranque) | Al encender | Instala Docker, baja el código y levanta los perfiles | `curated/_ops/bundle.tgz` | Contenedores |
| `infra/ops/docker-compose.yml` | VM `lumina-ops` | Al encender | Define Kafka, Spark Streaming, replay, Airflow y Postgres | — | — |
| `src/streaming/replay_producer.py` | VM `lumina-ops`, contenedor `replay` | Por demo | Reproduce lecturas de 2013 como eventos; inyecta fallas | `lumina_curated` | Kafka `meter.readings`, `grid.demand` |
| `src/streaming/streaming_job.py` | VM `lumina-ops`, contenedor `spark-streaming` (perfil `streaming`) | Continuo mientras la VM está encendida | Anomalías, picos por zona, medidores silenciosos, demanda | Kafka, `lumina_curated` | `lumina_rt`, Kafka `alerts.*`, checkpoints en GCS |
| `src/sql/streaming_tablas.sql` | BigQuery | Una vez | Crea las tablas de salida del streaming | — | `lumina_rt` |
| `src/costos/calculadora_consumo.py` | Laptop | Al cambiar supuestos | Costo del primer año en producción | Supuestos del script | `docs/entrega1/calculadora_consumo.md` |
| `infra/shutdown.sh` / `infra/teardown.sh` | Laptop | Fin de sesión / fin de curso | Apaga lo que cobra por hora / borra todo | — | — |
