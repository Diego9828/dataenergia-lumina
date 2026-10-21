# Avance del proyecto · DataEnergía / Lumina

Leyenda: ✅ hecho y verificado · 🟡 parcial o bloqueado · ⬜ pendiente

**Actualizado:** 2026-10-21 · **Costo acumulado:** ≈ US$1.7 de US$300 · **Recursos encendidos:** ninguno

## Resumen por entrega

| Entrega | Fecha | Estado | Falta |
|---|---|---|---|
| 1 · Sustentación + blueprint | 24-oct-2026 | 🟡 | Revisión y envío del documento (alumno) |
| 2 · Pipelines y modelado | 07-nov-2026 | 🟡 | Corrida del DAG de Airflow con Spark (cuota ya en 24 vCPU; falta encender la VM) |
| 3 · API, demo y sustentación | 20/21-nov-2026 | 🟡 | Despliegue de la API, mantenimiento predictivo, Looker Studio, demo y slides (vistas, tarifas y pronóstico listos) |

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
- ✅ 3 · Vistas que unen historia (`curated`) y hoy (`rt`) en `lumina_olap` → `src/sql/olap_vistas.sql`
- ✅ 4 · Reconciliación: de `rt` solo lo posterior al último batch; manda el batch (regla en `v_carga_zona_30min`)

### Orquestación · Airflow (camino batch, no tiempo real)
- ✅ DAG `lumina_batch_diario`: sensor → silver → gold → 13 cargas → derivadas → 3 controles → `src/dags/`
- ✅ Airflow en la VM, el DAG carga sin errores y el sensor pasa → [04_orquestacion_airflow.md](04_orquestacion_airflow.md)
- ✅ Cuota `CPUS_ALL_REGIONS` subida de 12 a 24 vCPU (alcanza para VM 4 + Spark serverless 16)
- ⬜ Corrida del DAG con Spark en el próximo encendido

### Fase 4 · Analítica (para la Entrega 3)
Detalle y resultados → [05_analitica.md](05_analitica.md)
- ✅ Calendario oficial dToU 2013 (17,520 medias horas) en `dim_tarifa_dtou` → `infra/08_tarifas_dtou.sh`
- ✅ Vistas OLAP en `lumina_olap` (5 vistas) → `infra/09_vistas_olap.sh`
- ✅ Tarifa dinámica vs. plana: −2.74 % de factura, 86 % de hogares ToU ahorran; −5.6 % de consumo en eventos Altos contra control → `infra/11_tarifas.sh`
- ✅ Pronóstico D+1 por zona (MAPE 4.4 % vs. 6.9 % ingenuo) y por hogar (WAPE 18.5 % vs. 21.3 %), ≤ US$0.42 → `infra/10_pronostico.sh`
- ⬜ Mantenimiento predictivo: ranking de medidores defectuosos y zonas con sobrecarga (base: `v_calidad_medidores`, `v_alertas`)
- ⬜ Tablero en Looker Studio sobre `lumina_olap`

### Entrega 3 · Exposición y demo
- 🟡 API REST (7 rutas: carga, pronóstico, consumo, factura, alertas, segmentos) → `src/api/`; falta desplegar en Cloud Run (requiere OK: endpoint público)
- ⬜ Guion de demo end-to-end
- ⬜ Slides con los 10 puntos del PDF (KPIs, calculadora, arquitectura, demo, conclusiones)
- ✅ Libro del proyecto (Quarto Book): 19 capítulos en 3 partes + apéndices, Parte I generada desde la sustentación, flujo interactivo incluido → `_quarto.yml`, `book/`
- ✅ Workflow de GitHub Pages → `.github/workflows/libro.yml`
- ✅ Enunciado del curso fuera del repo (copia en `bigdata utec/`)
- ✅ Historial limpio: el enunciado no está en ningún commit publicado (respaldo local en `backup/antes-de-quitar-enunciado`)
- ✅ Repositorio público → https://github.com/pipaber/dataenergia-lumina · libro → https://pipaber.github.io/dataenergia-lumina/ (se republica en cada push a `main`)

### Transversal
- ✅ Repositorio con commits por avance; secretos fuera de git
- ✅ Apagado: `infra/shutdown.sh`, `07_vm_ops.sh destruir`, horario 23:00, `teardown.sh`
- ✅ Bitácora de costos → [bitacora_costos.log](bitacora_costos.log)
- ✅ Arquitectura técnica (script → dónde corre → para qué) → [arquitectura_tecnica.md](arquitectura_tecnica.md)
- ✅ Ejemplo dummy del flujo de datos, ejecutable con pandas → `notebooks/flujo_dummy.py`
