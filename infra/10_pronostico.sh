#!/usr/bin/env bash
# Fase 4 · Pronóstico D+1 por zona y por hogar (BigQuery ML, boosted trees).
# Costo medido: ~US$0.42 (68 GiB facturados; los boosted trees releen los features en cada iteración,
# a la tarifa de consultas de US$6.25/TiB; entra en el TB gratis mensual si no se usó en otra cosa).
# Al final imprime las métricas fuera de muestra y el costo real según INFORMATION_SCHEMA.JOBS.
source "$(dirname "$0")/_common.sh"
require_config
BQ=(bq --project_id "$PROJECT_ID" --location "$BQ_LOCATION" query --nouse_legacy_sql)
INICIO="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

log "Entrenando y evaluando (puede tardar 10-20 min)"
sed "s/\${P}/$PROJECT_ID/g" "$REPO_DIR/src/sql/pronostico.sql" | "${BQ[@]}" >/dev/null

log "Métricas fuera de muestra (ene-feb 2014)"
"${BQ[@]}" --format pretty "SELECT * FROM \`$PROJECT_ID.lumina_ml.metricas_pronostico\` ORDER BY modelo, metodo"

log "Costo real de esta corrida"
"${BQ[@]}" --format pretty "
SELECT statement_type, COUNT(*) AS trabajos, ROUND(SUM(total_bytes_billed) / POW(1024, 3), 3) AS gib_facturados,
       ROUND(SUM(total_bytes_billed) / POW(1024, 4) * 6.25, 4) AS usd_aprox
FROM \`region-us\`.INFORMATION_SCHEMA.JOBS
WHERE creation_time >= TIMESTAMP('$INICIO') AND project_id = '$PROJECT_ID'
  AND statement_type != 'SCRIPT'   -- el SCRIPT padre repite los bytes de sus hijos
GROUP BY statement_type ORDER BY gib_facturados DESC"
