#!/usr/bin/env bash
# Fase 4 · Tarifa dinámica vs. plana en 2013: factura por hogar y respuesta a los eventos de precio.
# Costo estimado: ~US$0 (escanea ~4 GB de 2013; dentro del TB gratis mensual de consultas).
source "$(dirname "$0")/_common.sh"
require_config
BQ=(bq --project_id "$PROJECT_ID" --location "$BQ_LOCATION" query --nouse_legacy_sql)
T="\`$PROJECT_ID.lumina_ml"

log "Calculando facturas y respuesta a eventos"
sed "s/\${P}/$PROJECT_ID/g" "$REPO_DIR/src/sql/tarifas.sql" | "${BQ[@]}" >/dev/null

log "Factura media 2013 por tipo de tarifa (mismo consumo, dos precios)"
"${BQ[@]}" --format pretty "
SELECT tarifa, COUNT(*) AS hogares, ROUND(AVG(kwh_2013)) AS kwh_medio,
       ROUND(AVG(costo_dtou_gbp), 2) AS factura_dtou_gbp, ROUND(AVG(costo_plano_gbp), 2) AS factura_plana_gbp,
       ROUND((1 - SAFE_DIVIDE(SUM(costo_dtou_gbp), SUM(costo_plano_gbp))) * 100, 2) AS ahorro_dtou_pct,
       ROUND(COUNTIF(costo_dtou_gbp < costo_plano_gbp) / COUNT(*) * 100, 1) AS pct_hogares_que_ahorran,
       ROUND(AVG(pct_kwh_en_high), 2) AS pct_kwh_en_high
FROM $T.factura_tarifas_2013\` GROUP BY tarifa ORDER BY tarifa"

log "Respuesta a la demanda (consumo en el evento / consumo normal a la misma hora)"
"${BQ[@]}" --format pretty "SELECT * FROM $T.respuesta_eventos_2013\` ORDER BY nivel, tarifa"
