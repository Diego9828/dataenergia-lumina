#!/usr/bin/env bash
# Fase 1 · Paso 2: data lake en GCS (raw / curated) y datasets de BigQuery.
# Costo: GCS US$0.020/GB-mes (~13 GB ≈ US$0.26/mes). BigQuery: primeros 10 GB gratis.
source "$(dirname "$0")/_common.sh"
require_config

create_bucket() {
  local bucket="$1"
  if gcloud storage buckets describe "gs://$bucket" --project "$PROJECT_ID" >/dev/null 2>&1; then
    echo "Ya existe gs://$bucket"
    return
  fi
  # Sin soft delete: al reprocesar no se paga por los objetos borrados durante 7 días.
  gcloud storage buckets create "gs://$bucket" \
    --project "$PROJECT_ID" \
    --location "$REGION" \
    --default-storage-class STANDARD \
    --uniform-bucket-level-access \
    --public-access-prevention \
    --soft-delete-duration 0
  # --labels no existe en create en SDKs antiguos; se agrega con update.
  gcloud storage buckets update "gs://$bucket" --project "$PROJECT_ID" \
    --update-labels "$LABELS" >/dev/null || echo "AVISO: no se pudieron poner labels a $bucket"
}

log "Creando buckets del data lake en $REGION"
create_bucket "$BUCKET_RAW"
create_bucket "$BUCKET_CURATED"

log "Creando datasets de BigQuery en $BQ_LOCATION"
for ds in $BQ_DATASETS; do
  if bq --project_id "$PROJECT_ID" show --dataset "$PROJECT_ID:$ds" >/dev/null 2>&1; then
    echo "Ya existe $ds"
  else
    bq --project_id "$PROJECT_ID" --location "$BQ_LOCATION" mk --dataset \
      --label proyecto:lumina --label curso:bigdata-utec \
      --description "Lumina Energía · capa ${ds#lumina_}" "$PROJECT_ID:$ds"
  fi
done

log "Habilitando Private Google Access en la subred default ($REGION) para Dataproc Serverless"
if gcloud compute networks subnets describe default --region "$REGION" --project "$PROJECT_ID" >/dev/null 2>&1; then
  gcloud compute networks subnets update default --region "$REGION" --project "$PROJECT_ID" \
    --enable-private-ip-google-access
else
  echo "AVISO: no existe la red 'default' en $REGION; se resolverá en la Fase 2."
fi

log "Resumen"
gcloud storage ls --project "$PROJECT_ID" | grep lumina || true
bq --project_id "$PROJECT_ID" ls --datasets | grep lumina || true
