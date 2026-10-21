#!/usr/bin/env bash
# Fase 4 · Calendario oficial de la tarifa dinámica (dToU 2013) → bronze y BigQuery.
#   descarga Tariffs.xlsx (London Datastore, CC BY 4.0, ~240 KB)
#   → gs://<raw>/london_datastore/ (original + CSV) → lumina_curated.dim_tarifa_dtou
# Costo: US$0 (la carga a BigQuery es gratis; la tabla pesa < 1 MB).
source "$(dirname "$0")/_common.sh"
require_config

URL="https://data.london.gov.uk/download/vqm0d/14855047-44c2-4856-8a48-e5649200e6ce/Tariffs.xlsx"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

log "Descargando Tariffs.xlsx del London Datastore"
curl -sSfL -o "$TMP/Tariffs.xlsx" "$URL"
python "$REPO_DIR/src/ingestion/tarifas_dtou.py" "$TMP/Tariffs.xlsx" "$TMP/tarifas_dtou.csv"

log "Bronze: gs://$BUCKET_RAW/london_datastore/"
gcloud storage cp "$TMP/Tariffs.xlsx" "$TMP/tarifas_dtou.csv" "gs://$BUCKET_RAW/london_datastore/" --quiet

log "BigQuery: lumina_curated.dim_tarifa_dtou"
bq --project_id "$PROJECT_ID" --location "$BQ_LOCATION" load --replace --source_format CSV --skip_leading_rows 1 \
  lumina_curated.dim_tarifa_dtou "gs://$BUCKET_RAW/london_datastore/tarifas_dtou.csv" \
  ts_utc:TIMESTAMP,fecha:DATE,nivel:STRING,precio_p_kwh:FLOAT64,precio_plano_p_kwh:FLOAT64 >/dev/null
bq --project_id "$PROJECT_ID" --location "$BQ_LOCATION" query --nouse_legacy_sql --format pretty \
  "SELECT nivel, COUNT(*) medias_horas, ANY_VALUE(precio_p_kwh) precio_p_kwh, MIN(ts_utc) desde, MAX(ts_utc) hasta
   FROM \`$PROJECT_ID.lumina_curated.dim_tarifa_dtou\` GROUP BY 1 ORDER BY 3 DESC"
