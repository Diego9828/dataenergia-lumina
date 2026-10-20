#!/usr/bin/env bash
# Fase 2 · Carga gold (Parquet en GCS) -> BigQuery lumina_curated, más:
#   - clima_noaa_heathrow: cruce con el warehouse público NOAA GSOD
#   - consumo_dia_anidado: 1 fila por medidor-día con STRUCT (hogar) y ARRAY<STRUCT> (48 lecturas)
#   - umbral_zona: umbrales de pico por zona para el streaming
# (SQL en src/sql/gold_derivadas.sql, compartido con el DAG de Airflow).
# Uso: bash infra/06_cargar_bigquery.sh [--muestra]   (--muestra carga en el dataset lumina_muestra)
# Costo: las cargas desde GCS son gratis; las 2 consultas CTAS escanean ~6 GB (free tier: 1 TB/mes).
source "$(dirname "$0")/_common.sh"
require_config

DS="lumina_curated"
GOLD="gs://$BUCKET_CURATED/gold"
if [[ "${1:-}" == "--muestra" ]]; then
  DS="lumina_muestra"
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

log "Tablas derivadas: clima_noaa_heathrow, consumo_dia_anidado (STRUCT/ARRAY), umbral_zona"
sed -e "s/\${P}/$PROJECT_ID/g" -e "s/\${DS}/$DS/g" "$REPO_DIR/src/sql/gold_derivadas.sql" |
  "${BQ[@]}" query --nouse_legacy_sql >/dev/null
for t in clima_noaa_heathrow consumo_dia_anidado umbral_zona; do
  echo "  $DS.$t: $("${BQ[@]}" query --nouse_legacy_sql --format csv     "SELECT COUNT(*) FROM \`$PROJECT_ID.$DS.$t\`" | tail -1) filas"
done

log "Almacenamiento por tabla"
"${BQ[@]}" query --nouse_legacy_sql --format pretty \
  "SELECT table_id, row_count, ROUND(size_bytes/POW(1024,3), 3) AS gib
   FROM \`$PROJECT_ID.$DS.__TABLES__\` ORDER BY size_bytes DESC"
