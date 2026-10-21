#!/usr/bin/env bash
# Fase 4 · Crea las vistas de la capa de servicio (lumina_olap) y muestra un resumen.
# Costo: crear vistas es gratis; el resumen escanea unos pocos MB.
source "$(dirname "$0")/_common.sh"
require_config
BQ=(bq --project_id "$PROJECT_ID" --location "$BQ_LOCATION" query --nouse_legacy_sql)

log "Creando vistas lumina_olap"
sed "s/\${P}/$PROJECT_ID/g" "$REPO_DIR/src/sql/olap_vistas.sql" | "${BQ[@]}" >/dev/null

log "Resumen"
"${BQ[@]}" --format pretty "
SELECT 'v_carga_zona_30min' AS vista, COUNT(*) AS filas, COUNTIF(fuente = 'streaming') AS de_streaming,
       COUNTIF(es_pico) AS picos FROM \`$PROJECT_ID.lumina_olap.v_carga_zona_30min\`
UNION ALL SELECT 'v_consumo_segmento_diario', COUNT(*), NULL, NULL FROM \`$PROJECT_ID.lumina_olap.v_consumo_segmento_diario\`
UNION ALL SELECT 'v_alertas', COUNT(*), NULL, NULL FROM \`$PROJECT_ID.lumina_olap.v_alertas\`
UNION ALL SELECT 'v_calidad_medidores', COUNT(*), NULL, NULL FROM \`$PROJECT_ID.lumina_olap.v_calidad_medidores\`
UNION ALL SELECT 'v_red_vs_muestra', COUNT(*), NULL, NULL FROM \`$PROJECT_ID.lumina_olap.v_red_vs_muestra\`"
