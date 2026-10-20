#!/usr/bin/env bash
# Fase 2 · Carga gold (Parquet en GCS) -> BigQuery lumina_curated, más:
#   - clima_noaa_heathrow: cruce con el warehouse público NOAA GSOD
#   - consumo_dia_anidado: 1 fila por medidor-día con STRUCT (hogar) y ARRAY<STRUCT> (48 lecturas)
# Uso: bash infra/06_cargar_bigquery.sh [--muestra]
# Costo: las cargas desde GCS son gratis; las 2 consultas CTAS escanean ~6 GB (free tier: 1 TB/mes).
source "$(dirname "$0")/_common.sh"
require_config

DS="lumina_curated"
GOLD="gs://$BUCKET_CURATED/gold"   # con --muestra se carga la muestra en las mismas tablas (la carga completa la reemplaza)
if [[ "${1:-}" == "--muestra" ]]; then
  GOLD="gs://$BUCKET_CURATED/muestra/gold"
fi
BQ=(bq --project_id "$PROJECT_ID" --location "$BQ_LOCATION")

cargar() {  # $1 tabla, resto: opciones de particionado/clustering
  local tabla="$1"; shift
  "${BQ[@]}" load --replace --source_format PARQUET "$@" \
    "$DS.$tabla" "$GOLD/$tabla/*.parquet" >/dev/null
  echo "  $DS.$tabla: $("${BQ[@]}" query --nouse_legacy_sql --format csv \
    "SELECT COUNT(*) FROM \`$PROJECT_ID.$DS.$tabla\`" | tail -1) filas"
}

log "Cargando dimensiones y hechos desde $GOLD"
for t in dim_hogar dim_fecha dim_acorn acorn_atributos fact_clima_hora fact_clima_dia \
         fact_demanda_nacional_30min fact_carga_zona_30min perfil_hogar dq_medidor; do
  cargar "$t"
done
cargar fact_consumo_30min \
  --time_partitioning_field fecha --time_partitioning_type DAY --clustering_fields lclid
cargar fact_consumo_diario \
  --time_partitioning_field fecha --time_partitioning_type MONTH --clustering_fields lclid
cargar fact_carga_transformador_30min \
  --time_partitioning_field fecha --time_partitioning_type MONTH --clustering_fields zona,transformador

log "clima_noaa_heathrow (NOAA GSOD, estación 037720, 2011-2014; °F -> °C)"
"${BQ[@]}" query --nouse_legacy_sql >/dev/null <<SQL
CREATE OR REPLACE TABLE \`$PROJECT_ID.$DS.clima_noaa_heathrow\` AS
SELECT
  DATE(CAST(year AS INT64), CAST(mo AS INT64), CAST(da AS INT64)) AS fecha,
  ROUND((NULLIF(temp, 9999.9) - 32) * 5 / 9, 2) AS temp_media_c,
  ROUND((NULLIF(max, 9999.9) - 32) * 5 / 9, 2) AS temp_max_c,
  ROUND((NULLIF(min, 9999.9) - 32) * 5 / 9, 2) AS temp_min_c,
  ROUND(NULLIF(prcp, 99.99) * 25.4, 2) AS precipitacion_mm,
  ROUND(SAFE_CAST(NULLIF(wdsp, '999.9') AS FLOAT64) * 0.514444, 2) AS viento_ms
FROM \`bigquery-public-data.noaa_gsod.gsod*\`
WHERE _TABLE_SUFFIX BETWEEN '2011' AND '2014' AND stn = '037720'
SQL
echo "  $DS.clima_noaa_heathrow: $("${BQ[@]}" query --nouse_legacy_sql --format csv \
  "SELECT COUNT(*) FROM \`$PROJECT_ID.$DS.clima_noaa_heathrow\`" | tail -1) filas"

log "consumo_dia_anidado (STRUCT + ARRAY<STRUCT>)"
"${BQ[@]}" query --nouse_legacy_sql >/dev/null <<SQL
CREATE OR REPLACE TABLE \`$PROJECT_ID.$DS.consumo_dia_anidado\`
PARTITION BY DATE_TRUNC(fecha, MONTH)
CLUSTER BY lclid AS
SELECT
  c.lclid,
  c.fecha,
  STRUCT(h.tarifa, h.acorn, h.acorn_grupo, h.zona, h.transformador) AS hogar,
  ROUND(SUM(c.kwh), 4) AS kwh_total,
  ROUND(SUM(IF(c.es_punta, c.kwh, 0)), 4) AS kwh_punta,
  COUNT(c.kwh) AS n_lecturas,
  ARRAY_AGG(STRUCT(c.periodo_local, c.ts_utc, c.kwh, c.es_nulo, c.es_punta) ORDER BY c.ts_utc) AS lecturas
FROM \`$PROJECT_ID.$DS.fact_consumo_30min\` c
JOIN \`$PROJECT_ID.$DS.dim_hogar\` h USING (lclid)
WHERE c.fecha IS NOT NULL
GROUP BY c.lclid, c.fecha, h.tarifa, h.acorn, h.acorn_grupo, h.zona, h.transformador
SQL
echo "  $DS.consumo_dia_anidado: $("${BQ[@]}" query --nouse_legacy_sql --format csv \
  "SELECT COUNT(*) FROM \`$PROJECT_ID.$DS.consumo_dia_anidado\`" | tail -1) filas"

log "Almacenamiento por tabla"
"${BQ[@]}" query --nouse_legacy_sql --format pretty \
  "SELECT table_id, row_count, ROUND(size_bytes/POW(1024,3), 3) AS gib
   FROM \`$PROJECT_ID.$DS.__TABLES__\` ORDER BY size_bytes DESC"
