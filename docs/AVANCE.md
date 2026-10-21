# Avance del proyecto · DataEnergía / Lumina

Leyenda: ✅ hecho y verificado · 🟡 parcial o bloqueado · ⬜ pendiente

**Actualizado:** 2026-10-20 · **Costo acumulado:** ≈ US$1.3 de US$300 · **Recursos encendidos:** ninguno

## Resumen por entrega

| Entrega | Fecha | Estado | Falta |
|---|---|---|---|
| 1 · Sustentación + blueprint | 24-oct-2026 | 🟡 | Revisión y envío del documento (alumno) |
| 2 · Pipelines y modelado | 07-nov-2026 | 🟡 | Corrida del DAG de Airflow con Spark (bloqueada por cuota de CPU) |
| 3 · API, demo y sustentación | 20/21-nov-2026 | ⬜ | Fase 4, API, Looker Studio, slides, GitHub |

## Hitos

### Fase 0 · Plan
- ✅ Arquitectura propuesta, componentes y costo estimado → [00_plan_arquitectura.md](00_plan_arquitectura.md)
- ✅ Diagrama de servicios del taller → [diagrama_arquitectura.png](diagrama_arquitectura.png)

### Fase 1 · Setup en GCP
- ✅ APIs y presupuesto S/1,125 (≈ US$300) con alertas → `infra/01_apis_presupuesto.sh`
- ✅ Buckets raw/curated y datasets BigQuery → `infra/02_buckets_bigquery.sh`
- ✅ Token de Kaggle en Secret Manager → `infra/03_secreto_kaggle.sh`
- ✅ Ingesta Kaggle → raw (9.63 GiB) con VM efímera → `infra/04_ingesta_kaggle.sh`

### Fase 2 · Batch (camino batch)
- ✅ Limpieza bronze → silver (167.8 M lecturas) → `src/batch/silver.py`
- ✅ Modelo estrella, perfiles y calidad de medidores → `src/batch/gold.py`
- ✅ Carga a BigQuery (15 tablas) + tabla con STRUCT/ARRAY → `infra/06_cargar_bigquery.sh`
- ✅ Validación exacta contra Kaggle (3,510,403 medidor-día, 35.54 GWh) → [02_modelo_datos.md](02_modelo_datos.md)

### Entrega 1 · Documento
- ✅ Sustentación técnica, económica y DRP/HA → [entrega1/sustentacion_entrega1.docx](entrega1/sustentacion_entrega1.docx)
- ✅ Diagrama de producción en 4 capas, con capa batch, capa de velocidad y vistas unificadas → [entrega1/diagrama_capas.png](entrega1/diagrama_capas.png)
- ✅ Sección "Patrón Lambda" con los 4 puntos de sincronización (§ 2.1 del documento)
- ✅ Calculadora de consumo del primer año → `src/costos/calculadora_consumo.py`
- ⬜ **Revisar supuestos marcados y entregar (alumno, 24-oct)**

### Fase 3 · Streaming (camino tiempo real)
- ✅ Kafka + Spark Structured Streaming en la VM `lumina-ops` → `src/streaming/`, `infra/ops/`
- ✅ Replay de la ola de frío: 1.84 M lecturas, 20/20 anomalías y 52/52 medidores silenciosos detectados → [03_streaming.md](03_streaming.md)
- ✅ Umbrales calibrados offline (anomalías y picos por estación en hora punta)
- 🟡 Regla de picos por estación validada solo offline; falta verla en vivo en el próximo encendido

### Sincronización batch ↔ streaming (arquitectura Lambda)
- ✅ Patrón definido y documentado (Lambda, no Kappa), con diagrama → [arquitectura_tecnica.md](arquitectura_tecnica.md#batch-y-streaming-arquitectura-lambda)
- 🟡 1 · Lectura en vivo → bronze: consulta `bronze` en `streaming_job.py` y consolidación en `silver.py` implementadas; falta probarlas en vivo
- 🟡 2 · Perfiles y umbrales → streaming: recarga cada 24 h (`Contexto.refrescar()`) implementada; falta probarla en vivo
- ⬜ 3 · Vistas que unen historia (`curated`) y hoy (`rt`) en `lumina_olap` (Fase 4)
- ⬜ 4 · Reconciliación: de `rt` solo lo posterior al último batch; manda el batch (Fase 4)

### Orquestación · Airflow (camino batch, no tiempo real)
- ✅ DAG `lumina_batch_diario`: sensor → silver → gold → 13 cargas → derivadas → 3 controles → `src/dags/`
- ✅ Airflow en la VM, el DAG carga sin errores y el sensor pasa → [04_orquestacion_airflow.md](04_orquestacion_airflow.md)
- 🟡 **Bloqueado:** la cuota `CPUS_ALL_REGIONS` = 12 no permite VM (4) + Spark serverless (12). Decisión del alumno: subir la cuota a 16 o 24, o correr Airflow en la laptop

### Fase 4 · Analítica (para la Entrega 3)
- ⬜ Pronóstico de demanda D+1 por zona y segmento
- ⬜ Mantenimiento predictivo: ranking de medidores defectuosos y zonas con sobrecarga (base: `dq_medidor`)
- ⬜ Ahorro de tarifa dinámica vs. fija (calendario dToU 2013)
- ⬜ Vistas OLAP en `lumina_olap` + tablero en Looker Studio

### Entrega 3 · Exposición y demo
- ⬜ API REST en Cloud Run (consumo por hogar, alertas, pronóstico)
- ⬜ Guion de demo end-to-end
- ⬜ Slides con los 10 puntos del PDF (KPIs, calculadora, arquitectura, demo, conclusiones)
- ✅ Libro del proyecto (Quarto Book): 19 capítulos en 3 partes + apéndices, Parte I generada desde la sustentación, flujo interactivo incluido → `_quarto.yml`, `book/`
- ✅ Workflow de GitHub Pages → `.github/workflows/libro.yml`
- ✅ Enunciado del curso fuera del repo (copia en `bigdata utec/`)
- 🟡 **Pendiente decisión del alumno:** el enunciado sigue en el historial del primer commit; limpiar historial antes de publicar
- ⬜ Repositorio público en GitHub + libro en https://pipaber.github.io/dataenergia-lumina/

### Transversal
- ✅ Repositorio con commits por avance; secretos fuera de git
- ✅ Apagado: `infra/shutdown.sh`, `07_vm_ops.sh destruir`, horario 23:00, `teardown.sh`
- ✅ Bitácora de costos → [bitacora_costos.log](bitacora_costos.log)
- ✅ Arquitectura técnica (script → dónde corre → para qué) → [arquitectura_tecnica.md](arquitectura_tecnica.md)
- ✅ Ejemplo dummy del flujo de datos, ejecutable con pandas → `notebooks/flujo_dummy.py`
