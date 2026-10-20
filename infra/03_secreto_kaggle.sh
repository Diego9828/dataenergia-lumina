#!/usr/bin/env bash
# Fase 1 · Paso 3: sube KAGGLE_API_TOKEN (de infra/config.env) a Secret Manager.
# El token viaja por stdin: no aparece en la línea de comandos, en logs ni en el repositorio.
# Lo ejecuta el dueño del token. Costo: US$0 (free tier: 6 versiones activas).
source "$(dirname "$0")/_common.sh"
require_config

[[ -n "${KAGGLE_API_TOKEN:-}" ]] || die "KAGGLE_API_TOKEN está vacío en infra/config.env."

if gcloud secrets describe "$KAGGLE_SECRET" --project "$PROJECT_ID" >/dev/null 2>&1; then
  log "El secreto $KAGGLE_SECRET ya existe: agregando una versión nueva"
  printf '%s' "$KAGGLE_API_TOKEN" | gcloud secrets versions add "$KAGGLE_SECRET" \
    --project "$PROJECT_ID" --data-file=-
else
  log "Creando el secreto $KAGGLE_SECRET en $REGION"
  printf '%s' "$KAGGLE_API_TOKEN" | gcloud secrets create "$KAGGLE_SECRET" \
    --project "$PROJECT_ID" \
    --replication-policy user-managed --locations "$REGION" \
    --labels "$LABELS" \
    --data-file=-
fi

gcloud secrets versions list "$KAGGLE_SECRET" --project "$PROJECT_ID" \
  --format "table(name,state,createTime)"
