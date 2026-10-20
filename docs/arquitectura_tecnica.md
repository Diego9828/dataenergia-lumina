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

## Batch y streaming: arquitectura Lambda

Hay dos enfoques clásicos para combinar batch y streaming:

| | **Lambda** (el que usamos) | **Kappa** |
|---|---|---|
| Idea | Capa batch (verdad histórica) + capa de velocidad (lo reciente) + capa de servicio que las une | Un solo camino de streaming; la historia se reprocesa releyendo el log de Kafka |
| A favor | El batch recalcula todo con exactitud; el streaming da latencia de segundos | Un solo código, nada que sincronizar |
| En contra | Dos lógicas que mantener y sincronizar | Reprocesar 2 años desde el log es caro; entrenar perfiles y modelos sobre streaming es incómodo |

**Por qué Lambda en Lumina:** los perfiles por hogar, el pronóstico D+1 y los informes necesitan recalcular toda la historia, y eso es trabajo de batch. Para que las dos lógicas no se separen, ambas capas usan **el mismo motor (Spark)** y **una sola puerta de entrada (Kafka)**.

![Arquitectura Lambda y puntos de sincronización](diagrama_lambda.png)

**Sentido de los flujos:** los **datos** van del streaming al batch (cada lectura en vivo se copia a bronze y el batch la vuelve definitiva); del batch al streaming solo viajan **reglas** (perfiles y umbrales). El batch no escribe en `lumina_rt`: la precedencia del batch es una regla de las vistas.

### Puntos de sincronización

| # | Qué se sincroniza | Cómo | Dónde | Estado |
|---|---|---|---|---|
| 1 | Lectura en vivo → historia | Cada lectura que entra a Kafka se copia a bronze; el batch de la noche la procesa | Producción: Kafka Connect (sink a GCS). Taller: consulta `bronze` en `streaming_job.py` | ⬜ En el taller el replay lee de gold y no escribe bronze |
| 2 | Lo aprendido → streaming | `perfil_hogar` y `umbral_zona` (batch) son los umbrales del streaming | `streaming_job.py` (clase `Contexto`) | 🟡 Se cargan al arrancar el job; falta recargarlos cada día tras el batch |
| 3 | Historia + hoy → consumo | Vistas que unen `lumina_curated` (hasta ayer) con `lumina_rt` (hoy) | `lumina_olap` (Fase 4) | ⬜ |
| 4 | Dato provisional → definitivo | Las vistas toman de `lumina_rt` solo lo posterior a lo último que procesó el batch; **si hay diferencia, manda el batch** (incorpora lecturas atrasadas y correcciones) | Regla en las vistas `lumina_olap` | ⬜ |

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
